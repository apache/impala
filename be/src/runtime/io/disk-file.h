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

#include "util/spinlock.h"

namespace impala {
class TmpFileMgr;
class TmpFileRemote;
class TmpFileMgrTest;
namespace io {
class RemoteOperRange;
class ScanRange;
class WriteRange;

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

  virtual ~DiskFile() {}

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
  int64_t actual_file_size() const { return actual_file_size_.Load(); }

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

  /// Set actual file size. Should only be called by the TmpFileRemote::AllocateSpace()
  /// right after the allocation is at capacity, and the function should only be called
  /// once during the lifetime of the DiskFile.
  void SetActualFileSize(int64_t size) {
    DCHECK_EQ(0, actual_file_size_.Load());
    DCHECK_LE(file_size_, size);
    actual_file_size_.Store(size);
  }

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
  boost::shared_mutex physical_file_lock_;

  /// The hdfs connection used to connect to the remote scratch path.
  hdfsFS hdfs_conn_;

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
};
} // namespace io
} // namespace impala
