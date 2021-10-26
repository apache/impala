// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <string>

#include <boost/thread/shared_mutex.hpp>
#include "common/atomic.h"
#include "common/status.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/io/file-writer.h"

#include "util/condition-variable.h"
#include "util/spinlock.h"

namespace impala {
class TmpFileMgr;
class TmpFileRemote;
class TmpFileMgrTest;
namespace io {
class RemoteOperRange;
class ScanRange;
class WriteRange;

static const int64_t DISK_FILE_INVALID_FILE_OFFSET = -1;

/// MemBlockStatus indicates the status of a MemBlock.
/// Normal status change should be: UNINIT -> RESERVED -> ALLOC -> WRITTEN -> DISABLED.
/// But all status can jump to DISABLED directly.
/// UNINIT is the default status, indicates the block is not initialized.
/// RESERVED indicates the memory required by the block is reserved.
/// ALLOC indicates the memory required by the block is allocated.
/// WRITTEN indicates the memory is allocated and content has been written to the memory.
/// DISABLED indicates the MemBlock is disabled, doesn't allow any writes to or reads from
/// the block. It is a final state, and no memory should be allocated or reserved.
enum class MemBlockStatus { UNINIT, RESERVED, ALLOC, WRITTEN, DISABLED };

/// Each MemBlock can contain multiple pages, and be used as the buffer to read multiple
/// pages at a time from the DiskFile.
/// The caller may need to maintain the status of the MemBlock and make sure the block is
/// used under the correct status.
class MemBlock {
 public:
  MemBlock(int block_id) : block_id_(block_id), status_(MemBlockStatus::UNINIT) {}
  virtual ~MemBlock() {
    // Must be MemBlockStatus::DISABLED before destruction.
    DCHECK_EQ(static_cast<int>(status_), static_cast<int>(MemBlockStatus::DISABLED));
    DCHECK(data_ == nullptr);
  }

  // Release the memory if it is allocated.
  // The MemBlock status will be set to MemBlockStatus::DISABLED after deletion.
  // Return to the caller whether the memory is reserved or allocated before deletion.
  // Must be called before the MemBlock destruction.
  void Delete(bool* reserved, bool* alloc);

  // Allocate the memory for the MemBlock.
  // Status must be MemBlockStatus::RESERVED before allocation.
  // If successfully allocated, the status will be set to MemBlockStatus::ALLOC.
  Status AllocLocked(const std::unique_lock<SpinLock>& lock, int64_t size) {
    DCHECK(lock.mutex() == &mem_block_lock_ && lock.owns_lock());
    DCHECK_EQ(static_cast<int>(status_), static_cast<int>(MemBlockStatus::RESERVED));
    // Use malloc, could be better to alloc from a buffer pool.
    data_ = static_cast<uint8_t*>(malloc(size));
    if (UNLIKELY(data_ == nullptr)) {
      return Status(strings::Substitute("Couldn't allocate memory for a memory block, "
                                        "block size: '$0' bytes",
          size));
    }
    SetStatusLocked(lock, MemBlockStatus::ALLOC);
    return Status::OK();
  }

  uint8_t* data() { return data_; }

  MemBlockStatus GetStatus() {
    std::unique_lock<SpinLock> l(mem_block_lock_);
    return status_;
  }

  bool IsStatus(MemBlockStatus status) {
    std::unique_lock<SpinLock> l(mem_block_lock_);
    return IsStatusLocked(l, status);
  }

  bool IsStatusLocked(const std::unique_lock<SpinLock>& lock, MemBlockStatus status) {
    DCHECK(lock.mutex() == &mem_block_lock_ && lock.owns_lock());
    return status_ == status;
  }

  void SetStatus(MemBlockStatus status) {
    std::unique_lock<SpinLock> l(mem_block_lock_);
    SetStatusLocked(l, status);
  }

  void SetStatusLocked(const std::unique_lock<SpinLock>& lock, MemBlockStatus status) {
    DCHECK(lock.mutex() == &mem_block_lock_ && lock.owns_lock());
    SetInternalStatus(status);
  }

  /// Return the lock of the memory block.
  SpinLock* GetLock() { return &mem_block_lock_; }

  /// Return the block id.
  int block_id() { return block_id_; }

 private:
  friend class TmpFileRemote;
  friend class RemoteOperRange;
  friend class DiskFileTest;

  /// Caller should hold the lock.
  void SetInternalStatus(MemBlockStatus new_status) {
    switch (new_status) {
      case MemBlockStatus::RESERVED: {
        DCHECK(status_ == MemBlockStatus::UNINIT);
        break;
      }
      case MemBlockStatus::ALLOC: {
        DCHECK(status_ == MemBlockStatus::RESERVED);
        break;
      }
      case MemBlockStatus::WRITTEN: {
        DCHECK(status_ == MemBlockStatus::ALLOC);
        break;
      }
      case MemBlockStatus::DISABLED: {
        break;
      }
      default:
        DCHECK(false) << "Invalid memory block status: " << static_cast<int>(new_status);
    }
    status_ = new_status;
  }

  /// The id of the memory block.
  const int block_id_;

  /// Protect the members below.
  SpinLock mem_block_lock_;

  /// The status of the memory block.
  MemBlockStatus status_;

  /// The data of the memory block, may contain multiple pages.
  uint8_t* data_ = nullptr;
};

/// DiskFileType indicates the type of the file handled by the DiskFile.
/// LOCAL indicates the file is in the local filesystem.
/// LOCAL_BUFFER indicates the file is used as a buffer in the local filesystem.
/// DFS indicates the file is an HDFS file.
/// S3 indicates the file is in the Amazon S3 filesystem.
/// DUMMY is a type for a fake file.
enum class DiskFileType { LOCAL, LOCAL_BUFFER, DFS, S3, DUMMY };

/// DiskFileStatus indicates the status of the file handled by the DiskFile.
/// INWRITING indicates the file is allowed to write, and it is the initial status.
/// PERSISTED indicates the file is closed with all of the content written.
/// DELETED indicates the file is deleted from the filesystem.
enum class DiskFileStatus { INWRITING, PERSISTED, DELETED };

/// DiskFile is a handle to a physical file, mainly contains the information
/// of the file, control the status changing, and provide the file writer for
/// writing the file.
///
/// Some methods of DiskFile are not thread-safe.
class DiskFile {
 public:
  /// Constructor for a LOCAL file only.
  DiskFile(const std::string& path, DiskIoMgr* io_mgr);

  /// Constructor for a file with detail settings.
  DiskFile(const std::string& path, DiskIoMgr* io_mgr, int64_t file_size,
      DiskFileType disk_type, const hdfsFS* hdfs_conn = nullptr);

  /// Constructor for a file with read buffers.
  DiskFile(const std::string& path, DiskIoMgr* io_mgr, int64_t file_size,
      DiskFileType disk_type, int64_t read_buffer_size, int num_read_buffer_blocks);

  virtual ~DiskFile() {}

  /// The ReadBuffer is designed for batch reading. Each ReadBuffer belongs to one
  /// DiskFile, and contains multiple read buffer blocks which are divided from the
  /// DiskFile by the block size. Each block contains multiple pages.
  /// When reading a page from the read buffer, we firstly use the offset of the page
  /// to calculate which block contains the page, then see whether the block is
  /// available or not. If it is available, the caller can read the page from the block.
  /// The block is only available after a fetch, which is triggered in TmpFileMgr.
  /// The default size of a read buffer block is fixed and the number of the block per
  /// disk file is the default file size divided by the default block size.
  struct ReadBuffer {
    ReadBuffer(int64_t read_buffer_block_size, int64_t num_read_buffer_blocks);

    /// The default read buffer block size.
    const int64_t read_buffer_block_size_;

    /// The number of read buffer blocks per disk file.
    const int64_t num_of_read_buffer_blocks_;

    /// Each read buffer is a memory block, therefore, the size of read_buffer_blocks_ is
    /// num_of_read_buffer_blocks_.
    std::vector<std::unique_ptr<MemBlock>> read_buffer_blocks_;

    /// Protect below members.
    SpinLock read_buffer_ctrl_lock_;

    /// The statistics for the page number for each read buffer block.
    /// The size of page_cnts_per_block_ is num_of_read_buffer_blocks_.
    std::unique_ptr<int64_t[]> page_cnts_per_block_;

    /// The start offsets of each read buffer block to the whole file.
    /// The size of read_buffer_block_offsets_ is num_of_read_buffer_blocks_.
    std::unique_ptr<int64_t[]> read_buffer_block_offsets_;
  };

  // Delete the physical file. Caller should hold the exclusive file lock.
  Status Delete(const std::unique_lock<boost::shared_mutex>& lock);

  /// Returns the path of the DiskFile.
  /// If it is a remote temporary file, the path should be a remote scratch space path.
  /// The same, if it is a local one, the path should be a local path either.
  const std::string& path() const { return path_; }

  /// Returns the disk type of the DiskFile.
  DiskFileType disk_type() { return disk_type_; }

  /// Return the default size of the file.
  int64_t file_size() const { return file_size_; }

  /// Return the actual size of the file.
  int64_t actual_file_size() { return actual_file_size_.Load(); }

  /// If return True, the file is persisted.
  /// The caller should hold the status lock.
  bool is_persisted(const std::unique_lock<SpinLock>& l) {
    return GetFileStatusLocked(l) == DiskFileStatus::PERSISTED;
  }

  /// If return True, the file is in writing.
  /// The caller should hold the status lock.
  bool is_writing(const std::unique_lock<SpinLock>& l) {
    return GetFileStatusLocked(l) == DiskFileStatus::INWRITING;
  }

  /// If return True, the file is deleted.
  /// The caller should hold the status lock.
  bool is_deleted(const std::unique_lock<SpinLock>& l) {
    return GetFileStatusLocked(l) == DiskFileStatus::DELETED;
  }

  /// If True, the file is to be deleted.
  bool is_to_delete() { return to_delete_.Load(); }

  /// Set the status of the DiskFile. Caller should not hold the status lock.
  void SetStatus(DiskFileStatus status) {
    std::unique_lock<SpinLock> l(status_lock_);
    SetStatusLocked(status, l);
  }

  /// Same as SetStatus, but the caller should hold the status lock.
  void SetStatusLocked(
      DiskFileStatus status, const std::unique_lock<SpinLock>& status_lock) {
    DCHECK(status_lock.mutex() == &status_lock_ && status_lock.owns_lock());
    SetInternalStatus(status);
  }

  /// Set the flag of to_delete.
  void SetToDeleteFlag(bool to_delete = true) { to_delete_.Store(to_delete); }

  /// Returns the status of the file.
  /// The caller should not hold the status lock.
  DiskFileStatus GetFileStatus() {
    std::unique_lock<SpinLock> l(status_lock_);
    return GetFileStatusLocked(l);
  }

  DiskFileStatus GetFileStatusLocked(const std::unique_lock<SpinLock>& status_lock) {
    DCHECK(status_lock.mutex() == &status_lock_ && status_lock.owns_lock());
    return file_status_;
  }

  /// Return the writer of the file.
  io::FileWriter* GetFileWriter() { return file_writer_.get(); }

  /// Getter and setter for space reserved flag.
  void SetSpaceReserved() { space_reserved_.Store(true); }
  bool IsSpaceReserved() { return space_reserved_.Load(); }

  /// Set actual file size.
  /// The function should only be called once during the lifetime of the DiskFile.
  void SetActualFileSize(int64_t size) {
    DCHECK_EQ(0, actual_file_size_.Load());
    DCHECK_LE(file_size_, size);
    actual_file_size_.Store(size);
  }

  // Update the metadata of read buffer if the file is batch read enabled.
  // The metadata of read buffer is set when the file is written, because each page may
  // have different sizes, so for each read buffer block, the number of pages and the
  // start offset of a block could be different. By updating the metadata, these
  // information would be recorded.
  void UpdateReadBufferMetaDataIfNeeded(int64_t offset) {
    if (!IsBatchReadEnabled()) return;
    int64_t par_idx = GetReadBufferIndex(offset);
    DCheckReadBufferIdx(par_idx);
    std::lock_guard<SpinLock> lock(read_buffer_->read_buffer_ctrl_lock_);
    read_buffer_->page_cnts_per_block_[par_idx]++;
    int64_t cur_offset = read_buffer_->read_buffer_block_offsets_[par_idx];
    if (cur_offset == DISK_FILE_INVALID_FILE_OFFSET || offset < cur_offset) {
      read_buffer_->read_buffer_block_offsets_[par_idx] = offset;
    }
  }

  // Return the index of the buffer block by the file offset.
  int GetReadBufferIndex(int64_t offset) {
    int read_buffer_idx = offset / read_buffer_block_size();
    if (read_buffer_idx >= num_of_read_buffers()) {
      // Because the offset could be a little over the default file size, the index
      // could equal to the max number of read buffers, but can't be more than it.
      DCHECK(read_buffer_idx == num_of_read_buffers());
      read_buffer_idx = num_of_read_buffers() - 1;
    }
    DCheckReadBufferIdx(read_buffer_idx);
    return read_buffer_idx;
  }

  // Return the start offset by the index of the buffer block.
  int64_t GetReadBuffStartOffset(int buffer_idx) {
    DCheckReadBufferIdx(buffer_idx);
    std::lock_guard<SpinLock> lock(read_buffer_->read_buffer_ctrl_lock_);
    int64_t offset = read_buffer_->read_buffer_block_offsets_[buffer_idx];
    DCHECK(offset != DISK_FILE_INVALID_FILE_OFFSET);
    return offset;
  }

  // Return the actual size of the specific read buffer block.
  int64_t GetReadBuffActualSize(int buffer_idx) {
    DCheckReadBufferIdx(buffer_idx);
    std::lock_guard<SpinLock> lock(read_buffer_->read_buffer_ctrl_lock_);
    int64_t cur_offset = read_buffer_->read_buffer_block_offsets_[buffer_idx];
    DCHECK(cur_offset != DISK_FILE_INVALID_FILE_OFFSET);
    while (buffer_idx != num_of_read_buffers() - 1) {
      DCHECK_LT(buffer_idx, num_of_read_buffers() - 1);
      int64_t nxt_offset = read_buffer_->read_buffer_block_offsets_[buffer_idx + 1];
      if (nxt_offset != DISK_FILE_INVALID_FILE_OFFSET) return nxt_offset - cur_offset;
      buffer_idx++;
    }
    int64_t actual_file_size = actual_file_size_.Load();
    DCHECK_GT(actual_file_size, 0);
    return actual_file_size - cur_offset;
  }

  // Return the number of the page count in the read buffer block.
  int64_t GetReadBuffPageCount(int buffer_idx) {
    DCheckReadBufferIdx(buffer_idx);
    std::lock_guard<SpinLock> lock(read_buffer_->read_buffer_ctrl_lock_);
    return read_buffer_->page_cnts_per_block_[buffer_idx];
  }

  // Return the read buffer block.
  MemBlock* GetBufferBlock(int index) {
    DCheckReadBufferIdx(index);
    return read_buffer_->read_buffer_blocks_[index].get();
  }

  // Return the lock of the read buffer block.
  SpinLock* GetBufferBlockLock(int index) {
    DCheckReadBufferIdx(index);
    return read_buffer_->read_buffer_blocks_[index]->GetLock();
  }

  // Check if there is an available local memory buffer for the specific offset.
  // Caller should hold the physical lock of the disk file in case the object is
  // destroyed. But the caller should not hold the lock of the memory block because the
  // IsStatus() would require the lock.
  bool CanReadFromReadBuffer(
      const boost::shared_lock<boost::shared_mutex>& lock, int64_t offset) {
    if (!IsBatchReadEnabled()) return false;
    DCHECK(lock.mutex() == &physical_file_lock_ && lock.owns_lock());
    MemBlock* read_buffer_block = GetBufferBlock(GetReadBufferIndex(offset));
    return read_buffer_block != nullptr
        && read_buffer_block->IsStatus(MemBlockStatus::WRITTEN);
  }

  // Return if batch reading is enabled.
  bool IsBatchReadEnabled() { return read_buffer_ != nullptr; }

  void DCheckMemBlock(const boost::shared_lock<boost::shared_mutex>& file_lock,
      MemBlock* read_buffer_block) {
    DCHECK(file_lock.mutex() == &physical_file_lock_ && file_lock.owns_lock());
    DCHECK(read_buffer_block != nullptr);
  }

  // Read the spilled data from the memory buffer.
  // Caller should hold the physical file lock of the disk file in case the object is
  // destroyed. Also, caller should guarantee the buffer won't be released during reading,
  // it is good for the caller to have the lock of the read buffer block.
  Status ReadFromMemBuffer(int64_t offset_to_file, int64_t len, uint8_t* dst,
      const boost::shared_lock<boost::shared_mutex>& file_lock) {
    DCHECK(file_lock.mutex() == &physical_file_lock_ && file_lock.owns_lock());
    int64_t idx = GetReadBufferIndex(offset_to_file);
    DCheckReadBufferIdx(idx);
    uint8_t* read_buffer_block = read_buffer_->read_buffer_blocks_[idx]->data();
    DCHECK(read_buffer_block != nullptr);
    int64_t offset_to_block = offset_to_file - GetReadBuffStartOffset(idx);
    DCHECK_GE(offset_to_block, 0);
    DCHECK_GE(GetReadBuffActualSize(idx), offset_to_block + len);
    memcpy(dst, read_buffer_block + offset_to_block, len);
    return Status::OK();
  }

  // Helper function to allocate the memory for a reading buffer block.
  // Caller should hold both physical_file_lock_ and the lock of the read buffer block.
  Status AllocReadBufferBlockLocked(MemBlock* read_buffer_block, int64_t size,
      const boost::shared_lock<boost::shared_mutex>& file_lock,
      const std::unique_lock<SpinLock>& block_lock) {
    DCheckMemBlock(file_lock, read_buffer_block);
    return read_buffer_block->AllocLocked(block_lock, size);
  }

  // Helper function to set the status of a memory block.
  // Caller should hold the physical_file_lock_.
  // If the caller holds the lock of the read buffer block, SetStatusLocked() will be
  // called.
  void SetReadBufferBlockStatus(MemBlock* read_buffer_block, MemBlockStatus status,
      const boost::shared_lock<boost::shared_mutex>& file_lock,
      const std::unique_lock<SpinLock>* block_lock = nullptr) {
    DCheckMemBlock(file_lock, read_buffer_block);
    if (block_lock == nullptr) {
      read_buffer_block->SetStatus(status);
    } else {
      read_buffer_block->SetStatusLocked(*block_lock, status);
    }
  }

  // Helper function to check the status of a read buffer block.
  // Caller should hold the physical_file_lock_.
  // If the caller holds the lock of the read buffer block, IsStatusLocked() will be
  // called.
  bool IsReadBufferBlockStatus(MemBlock* read_buffer_block, MemBlockStatus status,
      const boost::shared_lock<boost::shared_mutex>& file_lock,
      const std::unique_lock<SpinLock>* block_lock = nullptr) {
    DCheckMemBlock(file_lock, read_buffer_block);
    if (block_lock == nullptr) return read_buffer_block->IsStatus(status);
    return read_buffer_block->IsStatusLocked(*block_lock, status);
  }

  // Helper function to delete the read buffer block.
  // Caller should hold the physical_file_lock_, but should not hold the lock of the
  // read buffer block, because Delete() will hold the read buffer block's lock.
  template <typename T>
  void DeleteReadBuffer(
      MemBlock* read_buffer_block, bool* reserved, bool* alloc, const T& file_lock) {
    DCHECK(file_lock.mutex() == &physical_file_lock_ && file_lock.owns_lock());
    DCHECK(read_buffer_block != nullptr);
    return read_buffer_block->Delete(reserved, alloc);
  }

  // Return the number of read buffer blocks of this DiskFile.
  int64_t num_of_read_buffers() { return read_buffer_->num_of_read_buffer_blocks_; }

  // Return the default size of a read buffer.
  int64_t read_buffer_block_size() { return read_buffer_->read_buffer_block_size_; }

 private:
  friend class RemoteOperRange;
  friend class ScanRange;
  friend class WriteRange;
  friend class impala::TmpFileMgr;
  friend class impala::TmpFileRemote;
  friend class impala::TmpFileMgrTest;

  /// Path of the physical file in the filesystem.
  const std::string path_;

  /// The default file size of the disk file.
  const int64_t file_size_ = 0;

  /// Specify the type of the disk where the file locates.
  const DiskFileType disk_type_;

  /// The status of the file.
  /// It includes IN_WRITING/PERSISTED/DELETED.
  DiskFileStatus file_status_;

  /// The FileWriter of the DiskFile.
  /// Caller can use the writer to write a range to the disk file.
  std::unique_ptr<FileWriter> file_writer_;

  /// Every time to query or modify the status, or need to guarantee working under
  /// certain status, the caller should own the lock.
  /// If a thread needs to hold both status lock of two disk files, the sequence of
  /// acquiring locks must be from local file to remote file to avoid a deadlock.
  /// The lock order of file lock (below) and status lock should be file lock acquired
  /// first.
  SpinLock status_lock_;

  /// Protect the physical file from deleting. For removing the file, the caller should
  /// hold the unique lock of the mutex. In other cases, like writing to or reading from
  /// the file, a shared lock will be held.
  /// If a thread needs to hold the lock of two disk files for doing an operation like
  /// upload, the sequence of acquiring locks must be from local file to remote file to
  /// avoid a deadlock.
  /// The lock order of file lock and status lock (above) should be file lock acquired
  /// first.
  /// If the disk file has the memory blocks, the lock also protects them from
  /// destruction.
  boost::shared_mutex physical_file_lock_;

  /// The hdfs connection used to connect to the remote scratch path.
  hdfsFS hdfs_conn_;

  /// to_delete_ is set to true if the file is to be deleted.
  /// It is a flag for the deleting thread to fetch the unique file lock and
  /// ask the current lock holder to yield.
  AtomicBool to_delete_{false};

  /// Specify if the file's space is reserved to be allowed to write to the filesystem
  /// because the filesystem may reach the size limit and needs some time before it can
  /// release space for new writes to the filesystem, so the space reserved indicator is
  /// a way to gain the permission to write.
  /// It is mainly used for a LOCAL_BUFFER file to control multiple write ranges to
  /// guarantee sending them to the DiskQueues with space reserved.
  /// For simplicity, the space_reserved_ only allows one-way transition, from false to
  /// true, once the space_reserved_ is set true, won't be changed back.
  /// For a LOCAL_BUFFER file, space_reserved_ is primarily accessed in TmpFileBufferPool
  /// under the exclusive TmpFileBufferPool::lock_ to guarantee the correctness of
  /// accessing. One special case is that space_reserved_ can be set true if quick space
  /// reservation is successful in TmpFileGroup::AllocateRemoteSpace(), but because the
  /// "set" only works on the first range of the file, and the operation is before all the
  /// ranges of the same file arriving the TmpFileBufferPool (it is guaranteed by
  /// TmpFileGroup::lock_ gained in the TmpFileGroup::AllocateSpace()), the correctness is
  /// guaranteed. One other special "read" on the indicator is in TmpFileRemote::Remove(),
  /// when the function is called in TmpFileGroup::Close() and the order should be after
  /// all of the "set" of the indicator, so the correctness of the "read" is guaranteed.
  /// If any above condition is changed, we may need to reconsider the safety.
  /// For type LOCAL, the space reserved is always true, because the query would fail
  /// immediately before creating the DiskFile if it exceeds the total size limit.
  AtomicBool space_reserved_{false};

  /// The actual file size is mainly used for a LOCAL_BUFFER file, the value could be
  /// slightly over the default file size. Should only be set by the
  /// TmpFileRemote::AllocateSpace() right after the allocation is at capacity, and the
  /// value would be checked by the DiskQueues when writing the range to the file system.
  /// If the current bytes written is equals to the actual file size, it indicates the
  /// range is the last range of the file, the file can be closed and starts to upload.
  /// The reason of identifying the last range by the actual file size is that, the order
  /// of writing to the file system doesn't have to be the same as the order assigned to
  /// them in TmpFileMgr because the ranges of a file arrive in different disk queues and
  /// are written separately, and for efficiency consider, we do a sequential write for
  /// the same LOCAL_BUFFER file instead of seeking for a specific offset, so the last
  /// range being written into the filesystem is unknown util it is written. The safety is
  /// guaranteed because the actual_file_size_ is only set once when the allocation of a
  /// file is at capacity in TmpFileMgr, and it is guaranteed to be set before the last
  /// range arrives the DiskQueue. When actual_file_size_ is used for a file in remote FS,
  /// like S3, it is set after a successful upload.
  AtomicInt64 actual_file_size_{0};

  /// The read buffer for the disk file, would be a nullptr if batch reading is not
  /// enabled.
  std::unique_ptr<ReadBuffer> read_buffer_;

  /// Internal setter to set the status.
  /// The status is from INWRITING -> PERSISTED -> DELETED, which should not be a
  /// reverse transition.
  void SetInternalStatus(DiskFileStatus new_status) {
    DCHECK(disk_type_ != DiskFileType::LOCAL);
    switch (new_status) {
      case DiskFileStatus::INWRITING: {
        DCHECK(file_status_ != DiskFileStatus::DELETED
            && file_status_ != DiskFileStatus::PERSISTED);
        break;
      }
      case DiskFileStatus::PERSISTED: {
        DCHECK(file_status_ != DiskFileStatus::DELETED);
        break;
      }
      case DiskFileStatus::DELETED: {
        break;
      }
      default:
        DCHECK(false) << "Invalid disk file status: " << static_cast<int>(new_status);
    }
    file_status_ = new_status;
  }

  /// Return the lock of the file.
  boost::shared_mutex* GetFileLock() { return &physical_file_lock_; }

  /// Return the status lock of the file.
  SpinLock* GetStatusLock() { return &status_lock_; }

  /// Helper function to DCHECK if the read buffer control is not NULL and if the buffer
  /// index is valid.
  void DCheckReadBufferIdx(int buffer_idx) {
    DCHECK(read_buffer_ != nullptr);
    DCHECK_LT(buffer_idx, read_buffer_->num_of_read_buffer_blocks_);
    DCHECK_GE(buffer_idx, 0);
  }
};
} // namespace io
} // namespace impala
