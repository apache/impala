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

#ifndef IMPALA_RUNTIME_TMP_FILE_MGR_INTERNAL_H
#define IMPALA_RUNTIME_TMP_FILE_MGR_INTERNAL_H

#include <string>
#include <unordered_map>
#include <unordered_set>

#include <boost/thread/shared_mutex.hpp>
#include "common/atomic.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/io/file-writer.h"
#include "runtime/tmp-file-mgr.h"

namespace impala {

/// TmpFile is a handle to a physical file in a temporary directory. File space
/// can be allocated and files removed using AllocateSpace() and Remove(). Used
/// internally by TmpFileMgr.
///
/// Creation of the physical file in the file system is deferred until the file is
/// written by DiskIoMgr.
///
/// Methods of TmpFile are not thread-safe.
class TmpFile {
 public:
  TmpFile(TmpFileGroup* file_group, TmpFileMgr::DeviceId device_id,
      const std::string& path, bool expected_local = true);

  virtual ~TmpFile() {}

  /// Allocates 'num_bytes' bytes in this file for a new block of data if there is
  /// free capacity in this temporary directory. If there is insufficient capacity,
  /// return false. Otherwise, update state and return true.
  /// This function does not actually perform any file operations.
  /// On success, sets 'offset' to the file offset of the first byte in the allocated
  /// range on success.
  virtual bool AllocateSpace(int64_t num_bytes, int64_t* offset) = 0;

  /// Called when an IO error is encountered for this file. Logs the error and blacklists
  /// the file. Returns true if the file just became blacklisted.
  bool Blacklist(const ErrorMsg& msg);

  /// Delete the physical file including buffer on disk, if one was created.
  /// It is not valid to read or write to a file after calling Remove().
  virtual Status Remove() = 0;

  /// Get the disk ID that should be used for IO mgr queueing.
  int AssignDiskQueue(bool is_local_buffer = false) const;

  /// Try to punch a hole in the file of size 'len' at 'offset'.
  Status PunchHole(int64_t offset, int64_t len);

  /// Returns the exact file handle for writing. For Spilling to remote, the default
  /// is to return the local buffer file instead of the remote file.
  virtual io::DiskFile* GetWriteFile() = 0;

  /// Returns the path of the TmpFile.
  /// If it is a remote temporary file, the path should be a remote scratch space path.
  /// Similarly, if it is a local one, the path should be a local path.
  const std::string& path() const { return path_; }

  /// Caller must hold TmpFileMgr::FileGroup::lock_.
  bool is_blacklisted() const { return blacklisted_; }

  /// Returns the current length of the file.
  int64_t len() const { return allocation_offset_; }

  /// Returns the disk id of the temporary file.
  int disk_id() const { return disk_id_; }

  /// Returns if the temporary file is in local file system.
  bool is_local() { return expected_local_; }

  /// Returns the path of the local buffer of the TmpFile.
  const string& LocalBuffPath() { return local_buffer_path_; }

  std::string DebugString();

  /// Helper to get the TmpDir that this file is associated with.
  TmpFileMgr::TmpDir* GetDir();

  /// Helper to get the TmpFileGroup that this file is associated with.
  TmpFileGroup* FileGroup() const { return file_group_; }

  /// Return the pointer of the disk file.
  io::DiskFile* DiskFile() { return disk_file_.get(); }

  /// Return the disk type where the file locates.
  io::DiskFileType disk_type() { return disk_type_; }

 private:
  friend class TmpFileMgrTest;
  friend class TmpFileLocal;
  friend class TmpFileRemote;
  friend class TmpFileDummy;
  friend class TmpWriteHandle;

  /// The name of the sub-directory that Impala creates within each configured scratch
  /// directory.
  const static std::string TMP_SUB_DIR_NAME;

  /// Space (in MB) that must ideally be available for writing on a scratch
  /// directory. A warning is issued if available space is less than this threshold.
  const static uint64_t AVAILABLE_SPACE_THRESHOLD_MB;

  /// The TmpFileGroup this belongs to. Cannot be null.
  TmpFileGroup* const file_group_;

  /// Path of the physical file in the filesystem.
  const std::string path_;

  /// The temporary device this file is stored on.
  const TmpFileMgr::DeviceId device_id_;

  /// The id of the disk on which the physical file lies.
  int disk_id_;

  // If the file is expected to be in the local file system.
  bool expected_local_;

  /// Total bytes of the file that have been given out by AllocateSpace(). Note that
  /// these bytes may not be actually using space on the filesystem, either because the
  /// data hasn't been written or a hole has been punched. Modified by AllocateSpace().
  int64_t allocation_offset_ = 0;

  /// Bytes reclaimed through hole punching.
  AtomicInt64 bytes_reclaimed_{0};

  /// Set to true to indicate that we shouldn't allocate any more space in this file.
  /// Protected by TmpFileMgr::FileGroup::lock_.
  bool blacklisted_;

  /// Specify the type of the disk where the file locates.
  io::DiskFileType disk_type_;

  /// The file path of the local buffer of the TmpFile.
  string local_buffer_path_;

  /// The disk file of the temporary file which is a handle to manage the status and
  /// operate on the physical file.
  std::unique_ptr<io::DiskFile> disk_file_;
};

/// TmpFileLocal is a derived class of TmpFile to provide methods to handle a
/// physical file in a temporary directory of local filesystem.
class TmpFileLocal : public TmpFile {
 public:
  TmpFileLocal(TmpFileGroup* file_group, TmpFileMgr::DeviceId device_id,
      const std::string& path, bool expected_local = true);

  bool AllocateSpace(int64_t num_bytes, int64_t* offset);
  io::DiskFile* GetWriteFile();
  Status Remove();
};

/// TmpFileRemote is a derived class of TmpFile to provide methods to handle a
/// physical file in a temporary directory of remote filesystem.
///
/// Locking:
/// For remote temporary files, a locking mechanism is applied to guarantee a safe write,
/// read or upload on the file.A remote temporary file can have two DiskFiles, a local
/// buffer and a remote file.
/// Each DiskFile owns two type of locks, a file lock and a status lock.
/// DiskFile::lock_  -- file lock
/// DiskFile::status_lock_ -- status lock
/// For doing file deleting operation, a unique file lock is needed. For other types of
/// operations on the file, like reading or writing, a shared file lock is needed to
/// protect the file from deleting.
/// For status transition, the thread needs to hold the status lock, details can be found
/// in the header of TmpFileRemote.
/// If an operation requires locks from two DiskFiles, such as an upload operation, the
/// sequence of acquiring the lock must be from the local file to the remote file to
/// avoid deadlocks, and file locks need to be acquired before status locks.
///
/// By default, there are two DiskFiles, local buffer and remote file, which are
/// used to manage the status of the temporary file. For each DiskFile, there are
/// three status, InWriting/Persisted/Deleted, details can be found in the header of
/// DiskFile.
/// Assume that:
/// Local buffer: InWriting A, Persisted B, Deleted C
/// Remote file:  InWriting D, Persisted E, Deleted F
/// The normal status transition procedure of a remote temporary file should be:
/// AD ---> BD        Local Buffer File Closed
/// BD ---> BE        Upload Complete
/// BE ---> CE        Local Buffer Evicted
/// Any State ---> CF Temporary File Destoryed
/// Most of the state transitions are done in the DiskIoMgr when an IO operation is done.
/// For reading or changing the state, a status lock of the specific DiskFile should be
/// acquired.
class TmpFileRemote : public TmpFile {
 public:
  TmpFileRemote(TmpFileGroup* file_group, TmpFileMgr::DeviceId device_id,
      const std::string& path, const std::string& local_buffer_path,
      bool expected_local = false, const char* url = nullptr);
  ~TmpFileRemote();

  bool AllocateSpace(int64_t num_bytes, int64_t* offset);
  io::DiskFile* GetWriteFile();
  TmpFileMgr::TmpDir* GetLocalBufferDir() const;
  Status Remove();

  /// Returns the size of the file.
  int64_t file_size() const { return file_size_; }

  /// Returns if the disk buffer file pointer.
  io::DiskFile* DiskBufferFile() { return disk_buffer_file_.get(); }

  /// Set the at_capacity_ indicator to true.
  void SetAtCapacity() {
    DCHECK(!at_capacity_);
    at_capacity_ = true;
  }

  /// Set the file is enqueued or not.
  /// The function is thread-safe.
  void SetEnqueued(bool is_enqueued) {
    std::lock_guard<SpinLock> l(lock_);
    DCHECK(is_enqueued != enqueued_);
    enqueued_ = is_enqueued;
  }

  /// Set the buffer of the file is returned to the pool.
  /// The function is thread-safe.
  void SetBufferReturned() {
    std::lock_guard<SpinLock> l(lock_);
    DCHECK(!buffer_returned_);
    buffer_returned_ = true;
  }

  /// Returns if the file is enqueued to the tmp file available pool.
  /// The function is thread-safe.
  bool is_enqueued() {
    std::lock_guard<SpinLock> l(lock_);
    return enqueued_;
  }

  /// Returns if the buffer is returned to the tmp file available pool.
  /// The function is thread-safe.
  bool is_buffer_returned() {
    std::lock_guard<SpinLock> l(lock_);
    return buffer_returned_;
  }

 private:
  friend class TmpWriteHandle;
  friend class TmpFileMgr;
  friend class TmpFileGroup;
  friend class TmpFileBufferPool;

  /// The default file size of the temporary file, but the actual file size can be a
  /// little over it if the size of the last page written to the file is over the
  /// remaining space.
  int64_t file_size_ = 0;

  /// Bogus value of mtime for HDFS files.
  const int64_t mtime_{100000};

  /// The range for doing file uploading.
  std::unique_ptr<io::RemoteOperRange> upload_range_;

  // The pointer of the disk buffer file, which is the local buffer
  // of the disk file when disk file is a remote disk file.
  std::unique_ptr<io::DiskFile> disk_buffer_file_;

  /// The hdfs connection used to connect to the remote scratch path.
  hdfsFS hdfs_conn_;

  /// at_capacity_ is set to true if the file can't assign space anymore when the
  /// assigned space is equal to or just over the default file size.
  bool at_capacity_ = false;

  /// Protect below members.
  SpinLock lock_;

  /// Indicates if the file is enqueued to the pool. For debug use.
  bool enqueued_ = false;

  /// True if the buffer of the file is returned to the pool. We assume that the buffer
  /// only returns once and only needs to be returned when the buffer space is reserved.
  bool buffer_returned_ = false;
};

/// TmpFileDummy is a derived class of TmpFile for dummy allocation, used in
/// TmpFileBufferPool only.
class TmpFileDummy : public TmpFile {
 public:
  TmpFileDummy() : TmpFile(nullptr, -1, "") { disk_type_ = io::DiskFileType::DUMMY; }
  bool AllocateSpace(int64_t num_bytes, int64_t* offset) { return true; }
  io::DiskFile* GetWriteFile() { return nullptr; }
  Status Remove() { return Status::OK(); }
};

/// Temporary file buffer pool allows the temporary files to return their buffer to the
/// pool and can be evicted to make room for other files. The pool also provides an async
/// way for the write ranges to wait until there is an available space to reserve before
/// they are sent to the disk queues for writing.
class TmpFileBufferPool {
 public:
  TmpFileBufferPool(TmpFileMgr* tmp_file_mgr);
  ~TmpFileBufferPool();

  // Loop to reserve space for the ranges which are put into the queue write_ranges_, the
  // space is gained from tmp_files_avail_pool_ by calling DequeueTmpFilesPool(). Once
  // the space is reserved, the write ranges which are belong to the same file will be
  // sent to the disk queues to write. If an error happens during the sending to the disk
  // queue, the callback function of the write ranges would be called with the status.
  void TmpFileSpaceReserveThreadLoop();

  /// Enqueue the write ranges to wait for buffer space reservation. All ranges of the
  /// same file would be put into a map write_ranges_to_add_ to wait until the
  /// reservation is done and being sent to the disk queue after that. Specially, the
  /// first write range of a file (offset is 0) is used to doing the reservation.
  Status EnqueueWriteRange(io::WriteRange* range, TmpFile* file);

  /// Function is called to remove all enqueued write ranges belonging to the "io_ctx"
  /// when the TmpFileGroup which the "io_ctx" belongs to is closing.
  void RemoveWriteRanges(io::RequestContext* io_ctx);

  // Enqueue the temporary file whose buffer file is available to be evicted.
  void EnqueueTmpFilesPool(std::shared_ptr<TmpFile>& tmp_file, bool front);

  // Dequeue a temporary file, whose buffer is supposed to be available being evicted,
  // from the available pool and make room for other files' buffer.
  Status DequeueTmpFilesPool(std::shared_ptr<TmpFile>* tmp_file, bool quick_return);

  // Shut down the pool before destruction.
  void ShutDown();

 private:
  friend class TmpFileMgr;
  friend class TmpFileMgrTest;

  /// The TmpFileMgr the TmpFileBufferPool belongs to.
  TmpFileMgr* tmp_file_mgr_ = nullptr;

  /// A pool stores the pointer of remote temporary files whose buffer are available to be
  /// evicted.
  /// The file is enqueued when:
  /// 1. The file is uploaded to the remote directory.
  /// 2. The file is deleted without unloading.
  /// The file is enqueued by calling EnqueueTmpFilesPool(), and dequeued by
  /// DequeueTmpFilesPool().
  std::list<std::shared_ptr<TmpFile>> tmp_files_avail_pool_;

  /// Condition variable for tmp files available pool to wait for available
  /// tmp files to be evicted. Wait() in the DequeueTmpFilesPool() and NotifyOne()
  /// in the EnqueueTmpFilesPool().
  ConditionVariable tmp_files_available_cv_;

  /// Protects tmp files available pool members.
  std::mutex tmp_files_avail_pool_lock_;

  /// Lock that protects below members.
  std::mutex lock_;

  /// Condition variable to signal the space reservation thread that there is work to do
  /// or the thread should shut down.  The space reservation thread will be woken up when
  /// there is a write range added to the queue write_ranges_.
  ConditionVariable work_available_;

  /// The write ranges waiting for the buffer space reservation.
  /// The key is the pointer of the DiskFile which the ranges are writing into.
  std::unordered_map<io::DiskFile*, std::vector<io::WriteRange*>> write_ranges_to_add_;

  /// Records the relationship of io_ctx and disk files, data inserts when add write
  /// ranges for waiting the reservation, removes when the request context is
  /// deconstructing when the TmpFileGroup is Closing.
  std::unordered_map<io::RequestContext*, std::unordered_set<io::DiskFile*>>
      io_ctx_to_file_set_map_;

  /// The write ranges waiting for the buffer space reservation (only the first range of
  /// a file).
  std::list<io::WriteRange*> write_ranges_;

  /// An index to bind the write range to its iterator in write_ranges_ and its TmpFile.
  std::unordered_map<io::WriteRange*,
      std::pair<std::list<io::WriteRange*>::const_iterator, TmpFile*>>
      write_ranges_iterator_;

  /// The current write range which is waiting for the reservation.
  io::WriteRange* cur_write_range_ = nullptr;

  /// The temporary file which the current write range waiting for reservation is
  /// associated with.
  std::shared_ptr<TmpFile> cur_tmp_file_;

  /// Dummy TmpFile for the case that needs to return the space back to the pool without
  /// having a TmpFile.
  std::shared_ptr<TmpFile> tmp_file_dummy_;

  /// True if the TmpFileMgr is destroying.
  bool shut_down_ = false;

  /// A timer metric for recording the dequeue waiting time.
  HistogramMetric* dequeue_timer_metric_ = nullptr;

  /// A helper function to add or remove all of the write ranges related to the same disk
  /// file in the pool.
  /// When the function is called after the reservation of a buffer file is done, it adds
  /// all of the ranges of the file to the disk queue. If any adding fails, all other
  /// writeback function of the ranges would be put to the vector "write_callbacks" for
  /// the caller to callback with an error status. All ranges belonging to the file would
  /// be removed from the pool.
  /// When the function is called when the writing tasks are cancelled (happens when the
  /// TmpFileGroup is closing), then is_cancelled is set to true, the function is used to
  /// remove all the ranges belonging to the specific disk_file in the pool.
  /// Caller should hold the lock_.
  Status MoveWriteRangesHelper(io::DiskFile* disk_file,
      std::vector<TmpFileMgr::WriteDoneCallback>* write_callbacks, bool is_cancelled);

  /// Internal function for RemoveWriteRanges();
  void RemoveWriteRangesInternal(io::RequestContext* io_ctx,
      std::vector<TmpFileMgr::WriteDoneCallback>* write_callbacks);
};

} // namespace impala

#endif
