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

#include <functional>
#include <map>
#include <memory>
#include <unordered_map>
#include <utility>
#include <bits/stdc++.h>

#include <mutex>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <gtest/gtest_prod.h>

#include "common/atomic.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "gen-cpp/CatalogObjects_types.h" // for THdfsCompression
#include "gen-cpp/Types_types.h" // for TUniqueId
#include "hdfs-fs-cache.h"
#include "runtime/scoped-buffer.h"
#include "util/condition-variable.h"
#include "util/mem-range.h"
#include "util/metrics-fwd.h"
#include "util/openssl-util.h"
#include "util/runtime-profile.h"
#include "util/spinlock.h"
#include "util/thread.h"

namespace impala {
namespace io {
  class DiskIoMgr;
  class RequestContext;
  class ScanRange;
  class DiskFile;
  class WriteRange;
  class RemoteOperRange;
}
struct BufferPoolClientCounters;
class MemTracker;
class TmpDir;
class TmpFile;
class TmpFileRemote;
class TmpFileBufferPool;
class TmpFileGroup;
class TmpWriteHandle;

/// TmpFileMgr provides an abstraction for management of temporary (a.k.a. scratch) files
/// on the filesystem and I/O to and from them. TmpFileMgr manages multiple scratch
/// directories across multiple devices, configured via the --scratch_dirs option.
/// TmpFileMgr manages I/O to scratch files in order to abstract away details of which
/// files are allocated and recovery from certain I/O errors. I/O is done via DiskIoMgr.
/// TmpFileMgr encrypts data written to disk if enabled by the --disk_spill_encryption
/// command-line flag.
///
/// TmpFileGroups manage scratch space across multiple devices. To write to scratch space,
/// first a TmpFileGroup is created, then TmpFileGroup::Write() is called to
/// asynchronously write a memory buffer to one of the scratch files.
/// TmpFileGroup::Write() returns a TmpWriteHandle, which identifies that write operation.
/// The caller is notified when the asynchronous write completes via a callback, after
/// which the caller can use the TmpWriteHandle to read back the data.
///
/// Each TmpWriteHandle is backed by a range of data in a scratch file. The first call to
/// Write() will create files for the TmpFileGroup with unique filenames on the configured
/// temporary devices. At most one directory per device is used (unless overridden for
/// testing). The file range of a TmpWriteHandle can be replaced with a different one if
/// a write error is encountered and the data instead needs to be written to a different
/// disk.
///
/// Free Space Management:
/// Free space is managed within a TmpFileGroup: once a TmpWriteHandle is destroyed, the
/// file range backing it can be recycled for a different TmpWriteHandle. Scratch file
/// ranges are grouped into size classes, each for a power-of-two number of bytes. Free
/// file ranges of each size class are managed separately (i.e. there is no splitting or
/// coalescing of ranges).
///
/// Resource Management:
/// TmpFileMgr provides some basic support for managing local disk space consumption.
/// A TmpFileGroup can be created with a limit on the total number of bytes allocated
/// across all files. Writes that would exceed the limit fail with an error status.
/// The TmpFileBufferPool provides the ability for write ranges to wait for local
/// buffer space in a separate thread during spilling to remote file system, the purpose
/// of the pool is to handle the competitions for buffer files allocation with limited
/// local space.
///
/// Locking:
/// During spilling, multiple locks could be acquired, to avoid deadlocks, the order
/// of acquiring must be from lower-numbered locks to higher-numbered locks:
/// 1. BufferPool::Client::lock_
/// 2. BufferPool::Page::buffer_lock
/// 3. TmpFileGroup::lock_
/// 4. TmpFileBufferPool::lock_
/// 5. TmpWriteHandle::write_state_lock_
/// 6. RequestContext::lock_
/// 7. ScanRange::lock_
/// 8. DiskFile::physical_file_lock_
/// Specially, it may need to acquire both locks of two types of DiskFile during spilling
/// to remote filesystem, the order to acquire DiskFile's locks must be from local
/// DiskFile to remote DiskFile.
///
/// Ohter than the locks above, terminal locks, meaning no other locks should be acquired
/// while holding this one, could be used during spilling:
/// TmpFileBufferPool::tmp_files_avail_pool_lock_
/// TmpFileGroup::tmp_files_remote_ptrs_lock_
/// DiskQueue::lock_
/// DiskFile::status_lock_
///
/// TODO: IMPALA-4683: we could implement smarter handling of failures, e.g. to
/// temporarily blacklist devices that show I/O errors.
class TmpFileMgr {
 public:
  /// DeviceId is an internal unique identifier for a temporary device managed by
  /// TmpFileMgr. DeviceIds in the range [0, num tmp devices) are allocated arbitrarily.
  /// Needs to be public for TmpFileMgrTest.
  typedef int DeviceId;

  /// Same typedef as io::WriteRange::WriteDoneCallback.
  typedef std::function<void(const Status&)> WriteDoneCallback;

  /// A configuration for the control parameters of remote temporary directories.
  /// The struct is used by TmpFileMgr and has the same lifecycle as TmpFileMgr.
  struct TmpDirRemoteCtrl {
    /// Calculate the maximum allowed read buffer bytes for the remote spilling.
    int64_t CalcMaxReadBufferBytes();

    /// A helper function to set up the paremeters of read buffers for remote files.
    Status SetUpReadBufferParams() WARN_UNUSED_RESULT;

    /// The high water mark metric for local buffer directory.
    AtomicInt64 local_buff_dir_bytes_high_water_mark_{0};

    /// The default size of a remote temporary file.
    int64_t remote_tmp_file_size_;

    /// The default size of a read buffer block for a remote temporary file.
    int64_t read_buffer_block_size_;

    /// The number of read buffer blocks for a remote file, it is from
    /// remote_tmp_file_size_/read_buffer_block_size_.
    int num_read_buffer_blocks_per_file_;

    /// The default block size of a remote temporary file. The block is used as a buffer
    /// while doing upload and fetch a remote temporary file.
    int64_t remote_tmp_block_size_;

    /// The maximum total size of read buffer for remote spilling.
    int64_t max_read_buffer_size_;

    /// Specify the mode to enqueue the tmp file to the pool.
    /// If true, the file would be placed in the first to be poped out from the pool.
    /// If false, the file would be placed in the last of the pool.
    bool remote_tmp_files_avail_pool_lifo_;

    /// Indicates if batch reading is enabled for the remote temporary files.
    bool remote_batch_read_enabled_;

    /// Temporary file buffer pool managed by TmpFileMgr, is only activated when there is
    /// a remote scratch space is registered. So, if TmpFileMgr::HasRemoteDir() is true,
    /// the tmp_file_pool_ is non-null. Otherwise, it is null.
    std::unique_ptr<TmpFileBufferPool> tmp_file_pool_;

    /// Thread group containing threads created by the TmpFileMgr.
    ThreadGroup tmp_file_mgr_thread_group_;

    /// Timeout duration for waiting for the buffer (us).
    int64_t wait_for_spill_buffer_timeout_us_;
  };

  TmpFileMgr();

  ~TmpFileMgr();

  /// Creates the configured tmp directories. If multiple directories are specified per
  /// disk, only one is created and used. Must be called after DiskInfo::Init().
  Status Init(MetricGroup* metrics) WARN_UNUSED_RESULT;

  /// Custom initialization - initializes with the provided list of directories.
  /// If one_dir_per_device is true, only use one temporary directory per device.
  /// This interface is intended for testing purposes. 'tmp_dir_specifiers'
  /// use the command-line syntax, i.e. <path>[:<limit>]. The first variant takes
  /// a comma-separated list, the second takes a vector.
  Status InitCustom(const std::string& tmp_dirs_spec, bool one_dir_per_device,
      const std::string& compression_codec, bool punch_holes,
      MetricGroup* metrics) WARN_UNUSED_RESULT;
  Status InitCustom(const std::vector<std::string>& tmp_dir_specifiers,
      bool one_dir_per_device, const std::string& compression_codec, bool punch_holes,
      MetricGroup* metrics) WARN_UNUSED_RESULT;
  // Create the TmpFile buffer pool thread for async buffer file reservation.
  Status CreateTmpFileBufferPoolThread(MetricGroup* metrics) WARN_UNUSED_RESULT;

  /// Try to reserve space for the buffer file from local buffer directory.
  /// If quick_return is true, the function won't wait if there is no available space.
  Status ReserveLocalBufferSpace(bool quick_return) WARN_UNUSED_RESULT;

  /// Return the scratch directory path for the device.
  std::string GetTmpDirPath(DeviceId device_id) const;

  /// Return the remote temporary file size.
  int64_t GetRemoteTmpFileSize() const {
    return tmp_dirs_remote_ctrl_.remote_tmp_file_size_;
  }

  /// Return the read buffer block size for a remote temporary file.
  int64_t GetReadBufferBlockSize() const {
    return tmp_dirs_remote_ctrl_.read_buffer_block_size_;
  }

  /// Return the number of read buffer blocks for a remote temporary file.
  int GetNumReadBuffersPerFile() const {
    return tmp_dirs_remote_ctrl_.num_read_buffer_blocks_per_file_;
  }

  /// Return the maximum total size of all the read buffer blocks for remote spilling.
  int64_t GetRemoteMaxTotalReadBufferSize() const {
    return tmp_dirs_remote_ctrl_.max_read_buffer_size_;
  }

  /// Return the remote temporary block size.
  int64_t GetRemoteTmpBlockSize() const {
    return tmp_dirs_remote_ctrl_.remote_tmp_block_size_;
  }

  /// Return the timeout duration of waiting for spill buffer.
  int64_t GetSpillBufferWaitTimeout() const {
    return tmp_dirs_remote_ctrl_.wait_for_spill_buffer_timeout_us_;
  }

  /// Return if return the remote temporary file to the pool by LIFO.
  /// If false is returned, it is set by FIFO.
  bool GetRemoteTmpFileBufferPoolLifo() {
    return tmp_dirs_remote_ctrl_.remote_tmp_files_avail_pool_lifo_;
  }

  // Return if batch reading for remote temporary files is enabled.
  bool IsRemoteBatchReadingEnabled() {
    return tmp_dirs_remote_ctrl_.remote_batch_read_enabled_;
  }

  /// Return the local buffer dir for remote spilling.
  TmpDir* GetLocalBufferDir() const;

  /// Total number of devices with tmp directories that are active in local file system.
  /// There is one tmp directory per device.
  int NumActiveTmpDevicesLocal();

  /// Total number of devices with tmp directories that are active. There is one tmp
  /// directory per device.
  int NumActiveTmpDevices();

  /// Return vector with device ids of all tmp devices being actively used.
  /// I.e. those that haven't been blacklisted.
  std::vector<DeviceId> ActiveTmpDevices();

  /// Add the write range to the TmpFileBufferPool for waiting the reservation of the
  /// buffer of the TmpFile the range writes into. The pool would send the range to the
  /// disk queue once reservation is done. If the space of the buffer is already reserved,
  /// an error would return, and the caller should enqueue the range to the disk queue by
  /// itself.
  Status AsyncWriteRange(io::WriteRange* write_range, TmpFile* file);

  /// A helper function to enqueue a dummy temporary file to the TmpFileBufferPool.
  void EnqueueTmpFilesPoolDummyFile();

  /// Enqueue the temporary files into the pool when the local buffer of the file is ready
  /// to be deleted to release buffer space.
  /// If front set to true, the file is enqueued to the front of the pool and is to be
  /// popped first.
  void EnqueueTmpFilesPool(std::shared_ptr<TmpFile>& tmp_file, bool front);

  /// Dequeue the temporary files from the pool. The caller which successfully
  /// dequeues a file from the pool has the right to delete the local buffer of the file
  /// and create its buffer.
  /// The function is called by TryEvictFile() to gain space from the pool if the
  /// local buffer directory reaches byte limit. If there is no file in the pool, the
  /// caller needs to wait until a file is enqueued (often after a file is uploaded).
  /// It could be possible a long wait if no file is uploaded due a jammed disk queue
  /// or a slow network, so it is not recommended to hold an exclusive lock while calling
  /// the function.
  /// If quick_return is true, one won't wait if no file is in the pool. Otherwise,
  /// one would wait util a file is dequeued.
  Status DequeueTmpFilesPool(std::shared_ptr<TmpFile>* tmp_file, bool quick_return);

  /// The function releases all the memory of read buffer in a temporary file.
  /// Caller needs to hold the unique lock of the buffer file.
  void ReleaseTmpFileReadBuffer(
      const std::unique_lock<boost::shared_mutex>& lock, TmpFile* tmp_file);

  /// Try to delete the buffer of a TmpFile to make some space for other buffers.
  /// May return an error status if error happens during deletion of the buffer.
  Status TryEvictFile(TmpFile* tmp_file);

  /// If a remote scratch space is registered in the TmpFileMgr.
  bool HasRemoteDir() { return tmp_dirs_remote_ != nullptr; }

  /// Return default S3 options for spilling.
  const vector<std::pair<string, string>>* s3a_options() { return &s3a_options_; }

  MemTracker* compressed_buffer_tracker() const {
    return compressed_buffer_tracker_.get();
  }

  /// The type of spill-to-disk compression in use for spilling.
  THdfsCompression::type compression_codec() const { return compression_codec_; }
  bool compression_enabled() const {
    return compression_codec_ != THdfsCompression::NONE;
  }
  int compression_level() const { return compression_level_; }
  bool punch_holes() const { return punch_holes_; }

  /// The minimum size of hole that we will try to punch in a scratch file.
  /// This avoids ineffective hole-punching where we only punch a hole in
  /// part of a block and can't reclaim space. 4kb is chosen based on Linux
  /// filesystem typically using 4kb or smaller blocks
  static constexpr int64_t HOLE_PUNCH_BLOCK_SIZE_BYTES = 4096;

 private:
  friend class TmpFile;
  friend class TmpFileRemote;
  friend class TmpFileGroup;
  friend class TmpFileMgrTest;
  friend class TmpDirLocal;
  friend class TmpDirHdfs;
  friend class TmpDirS3;

  /// Return a new TmpFile handle with a path based on file_group->unique_id. The file is
  /// associated with the 'file_group' and the file path is within the (single) scratch
  /// directory on the specified device id. The caller owns the returned handle and is
  /// responsible for deleting it. The file is not created - creation is deferred until
  /// the file is written.
  void NewFile(TmpFileGroup* file_group, DeviceId device_id,
    std::unique_ptr<TmpFile>* new_file);

  /// Remove the remote directory which stores tmp files of the tmp file group.
  void RemoveRemoteDir(TmpFileGroup* file_group, DeviceId device_id);

  bool initialized_ = false;

  /// The type of spill-to-disk compression in use for spilling. NONE means no
  /// compression is used.
  THdfsCompression::type compression_codec_ = THdfsCompression::NONE;

  /// The compression level, which is used for certain compression codecs like ZSTD
  /// and ignored otherwise. -1 means not set/invalid.
  int compression_level_ = -1;

  /// Whether hole punching is enabled.
  bool punch_holes_ = false;

  /// Whether one local scratch directory per device.
  bool one_dir_per_device_ = false;

  /// The paths of the created tmp directories, which are used for spilling to local
  /// filesystem.
  std::vector<std::unique_ptr<TmpDir>> tmp_dirs_;

  /// The paths of remote directories, which are used for spilling to remote filesystem.
  std::unique_ptr<TmpDir> tmp_dirs_remote_;

  /// The control parameters for remote temporary directories.
  TmpDirRemoteCtrl tmp_dirs_remote_ctrl_;

  /// The path of the directory to store local buffers for remote temporary files.
  std::unique_ptr<TmpDir> local_buff_dir_;

  /// Default S3 options for spilling to S3.
  HdfsFsCache::HdfsConnOptions s3a_options_;

  /// Local cache for HDFS connection handle.
  HdfsFsCache::HdfsFsMap hdfs_conns_;

  /// Memory tracker to track compressed buffers. Set up in InitCustom() if disk spill
  /// compression is enabled
  std::unique_ptr<MemTracker> compressed_buffer_tracker_;

  /// Metrics to track active scratch directories.
  IntGauge* num_active_scratch_dirs_metric_ = nullptr;
  SetMetric<std::string>* active_scratch_dirs_metric_ = nullptr;

  /// Metrics to track the scratch space HWM.
  AtomicHighWaterMarkGauge* scratch_bytes_used_metric_ = nullptr;

  /// Metrics to track the read memory buffer HWM.
  AtomicHighWaterMarkGauge* scratch_read_memory_buffer_used_metric_ = nullptr;
};

/// Represents a group of temporary files - one per disk with a scratch directory. The
/// total allocated bytes of the group can be bound by setting the space allocation
/// limit. The owner of the TmpFileGroup object is responsible for calling the Close()
/// method to delete all the files in the group.
///
/// Public methods of TmpFileGroup and TmpWriteHandle are safe to call concurrently from
/// multiple threads as long as different TmpWriteHandle arguments are provided.
class TmpFileGroup {
 public:
  /// Initialize a new file group, which will create files using 'tmp_file_mgr'
  /// and perform I/O using 'io_mgr'. Adds counters to 'profile' to track scratch
  /// space used. 'unique_id' is a unique ID that is used to prefix any scratch file
  /// names. It is an error to create multiple TmpFileGroups with the same 'unique_id'.
  /// 'bytes_limit' is the limit on the total file space to allocate.
  TmpFileGroup(TmpFileMgr* tmp_file_mgr, io::DiskIoMgr* io_mgr, RuntimeProfile* profile,
      const TUniqueId& unique_id, int64_t bytes_limit = -1);

  ~TmpFileGroup();

  /// Asynchronously writes 'buffer' to a temporary file of this file group. If there
  /// are multiple scratch files, this can write to any of them, and will attempt to
  /// recover from I/O errors on one file by writing to a different file. The memory
  /// referenced by 'buffer' must remain valid until the write completes. The callee
  /// may rewrite the data in 'buffer' in-place (e.g. to do in-place encryption or
  /// compression). The caller should not modify the data in 'buffer' until the write
  /// completes or is cancelled, otherwise invalid data may be written to disk.
  ///
  /// The write may take some time to complete. It may be queued behind other I/O
  /// operations. If remote scratch is enabled, it may also need to wait for other queries
  /// to make progress and release space in the local buffer directory.
  ///
  /// Returns an error if the scratch space cannot be allocated or the write cannot
  /// be started. Otherwise 'handle' is set and 'cb' will be called asynchronously from
  /// a different thread when the write completes successfully or unsuccessfully or is
  /// cancelled. If non-null, the counters in 'counters' are updated with information
  /// about the write.
  ///
  /// 'handle' must be destroyed by passing the DestroyWriteHandle() or RestoreData().
  Status Write(MemRange buffer, TmpFileMgr::WriteDoneCallback cb,
      std::unique_ptr<TmpWriteHandle>* handle,
      const BufferPoolClientCounters* counters = nullptr);

  /// Synchronously read the data referenced by 'handle' from the temporary file into
  /// 'buffer'. buffer.len() must be the same as handle->len(). Can only be called
  /// after a write successfully completes. Should not be called while an async read
  /// is in flight. Equivalent to calling ReadAsync() then WaitForAsyncRead().
  Status Read(TmpWriteHandle* handle, MemRange buffer) WARN_UNUSED_RESULT;

  /// Asynchronously read the data referenced by 'handle' from the temporary file into
  /// 'buffer'. buffer.len() must be the same as handle->len(). Can only be called
  /// after a write successfully completes. WaitForAsyncRead() must be called before the
  /// data in the buffer is valid. Should not be called while an async read
  /// is already in flight.
  Status ReadAsync(TmpWriteHandle* handle, MemRange buffer) WARN_UNUSED_RESULT;

  /// Wait until the read started for 'handle' by ReadAsync() completes. 'buffer'
  /// should be the same buffer passed into ReadAsync(). Returns an error if the
  /// read fails. Retrying a failed read by calling ReadAsync() again is allowed.
  /// If non-null, the counters in 'counters' are updated with information about the read.
  Status WaitForAsyncRead(TmpWriteHandle* handle, MemRange buffer,
      const BufferPoolClientCounters* counters = nullptr) WARN_UNUSED_RESULT;

  /// Restore the original data in the 'buffer' passed to Write(), decrypting as
  /// necessary. Returns an error if restoring the data fails. The write must not be
  /// in-flight - the caller is responsible for waiting for the write to complete.
  /// If non-null, the counters in 'counters' are updated with information about the read.
  Status RestoreData(std::unique_ptr<TmpWriteHandle> handle, MemRange buffer,
      const BufferPoolClientCounters* counters = nullptr) WARN_UNUSED_RESULT;

  /// Wait for the in-flight I/Os to complete and destroy resources associated with
  /// 'handle'.
  void DestroyWriteHandle(std::unique_ptr<TmpWriteHandle> handle);

  /// Calls Remove() on all the files in the group and deletes them.
  void Close();

  /// A function template to close files from local or remote scratch space.
  template <typename T>
  void CloseInternal(vector<T>& tmp_files);

  /// Update the corresponding metrics of scratch space after new scratch space
  /// is allocated.
  void UpdateScratchSpaceMetrics(int64_t num_bytes, bool is_remote = false);

  /// Assemble and return a new path.
  std::string GenerateNewPath(const string& dir, const string& unique_name);

  std::string DebugString();

  const TUniqueId& unique_id() const { return unique_id_; }

  TmpFileMgr* tmp_file_mgr() const { return tmp_file_mgr_; }

  void SetDebugAction(const std::string& debug_action) { debug_action_ = debug_action; }

  /// Return true if spill-to-disk failed due to local faulty disk.
  bool IsSpillingDiskFaulty();

  /// Return the shared_ptr of a remote TmpFile.
  std::shared_ptr<TmpFile>& FindTmpFileSharedPtr(TmpFile* tmp_file);

 private:
  friend class TmpFile;
  friend class TmpFileLocal;
  friend class TmpFileRemote;
  friend class TmpFileMgrTest;
  friend class TmpWriteHandle;
  friend class io::WriteRange;
  friend class io::ScanRange;
  friend class io::RemoteOperRange;

  /// Initializes the file group with one temporary file per disk with a scratch
  /// directory. Returns OK if at least one temporary file could be created.
  /// Returns an error if no temporary files were successfully created. Must only be
  /// called once. Must be called with 'lock_' held.
  Status CreateFiles() WARN_UNUSED_RESULT;

  /// Allocate 'num_bytes' bytes in a temporary file. Try multiple disks if error
  /// occurs. Returns an error only if no temporary files are usable or the scratch
  /// limit is exceeded. Must be called without 'lock_' held.
  Status AllocateSpace(
      int64_t num_bytes, TmpFile** tmp_file, int64_t* file_offset) WARN_UNUSED_RESULT;

  /// Try to allocate 'num_bytes' bytes from local scratch space. Called by the
  /// AllocateSpace().
  /// The alloc_full returns true, if all of the directories are at capacity.
  Status AllocateLocalSpace(int64_t num_bytes, TmpFile** tmp_file, int64_t* file_offset,
      vector<int>* at_capacity_dirs, bool* alloc_full) WARN_UNUSED_RESULT;

  /// Try to allocate 'num_bytes' bytes from remote scratch space when there is no
  /// space left in the local scratch space. Called by the AllocateSpace().
  Status AllocateRemoteSpace(int64_t num_bytes, TmpFile** tmp_file, int64_t* file_offset,
      vector<int>* at_capacity_dirs) WARN_UNUSED_RESULT;

  /// Recycle the range of bytes in a scratch file and destroy 'handle'. Called when the
  /// range is no longer in use for 'handle'. The disk space associated with the file can
  /// be reclaimed once this function, either by adding it to 'free_ranges_' for
  /// recycling, or punching a hole in the file. Must be called without 'lock_' held.
  void RecycleFileRange(std::unique_ptr<TmpWriteHandle> handle);

  /// Called when the DiskIoMgr write completes for 'handle'. On error, will attempt
  /// to retry the write. On success or if the write can't be retried, calls
  /// handle->WriteComplete().
  void WriteComplete(TmpWriteHandle* handle, const Status& write_status);

  /// Handles a write error. Logs the write error and blacklists the device for this
  /// file group if the cause was an I/O error. Blacklisting limits the number of times
  /// a write is retried because each device will only be tried once. Returns OK if it
  /// successfully reissued the write. Returns an error status if the original error
  /// was unrecoverable or an unrecoverable error is encountered when reissuing the
  /// write. The error status will include all previous I/O errors in its details.
  Status RecoverWriteError(
      TmpWriteHandle* handle, const Status& write_status) WARN_UNUSED_RESULT;

  /// Return a SCRATCH_ALLOCATION_FAILED error with the appropriate information,
  /// including scratch directories, the amount of scratch allocated and previous
  /// errors that caused this failure. If some directories were at capacity,
  /// but had not encountered an error, the indices of these directories in
  /// tmp_file_mgr_->tmp_dir_ should be included in 'at_capacity_dirs'.
  /// 'lock_' must be held by caller.
  Status ScratchAllocationFailedStatus(const std::vector<int>& at_capacity_dirs);

  /// Debug action of the query.
  std::string debug_action_;

  /// The TmpFileMgr it is associated with.
  TmpFileMgr* const tmp_file_mgr_;

  /// DiskIoMgr used for all I/O to temporary files.
  io::DiskIoMgr* const io_mgr_;

  /// I/O context used for all reads and writes. Registered in constructor.
  std::unique_ptr<io::RequestContext> io_ctx_;

  /// Stores scan ranges allocated in Read(). Needed because ScanRange objects may be
  /// touched by DiskIoMgr even after the scan is finished.
  /// TODO: IMPALA-4249: remove once lifetime of ScanRange objects is better defined.
  ObjectPool scan_range_pool_;

  /// Unique across all TmpFileGroups. Used to prefix file names.
  const TUniqueId unique_id_;

  /// Max write space allowed (-1 means no limit).
  const int64_t bytes_limit_;

  /// Number of write operations (includes writes started but not yet complete).
  RuntimeProfile::Counter* const write_counter_;

  /// Number of bytes written to disk (includes writes started but not yet complete).
  RuntimeProfile::Counter* const bytes_written_counter_;

  /// Number of bytes written to disk before compression (includes writes started but
  /// not yet complete).
  RuntimeProfile::Counter* const uncompressed_bytes_written_counter_;

  /// Number of read operations (includes reads started but not yet complete).
  RuntimeProfile::Counter* const read_counter_;

  /// Number of bytes read from disk (includes reads started but not yet complete).
  RuntimeProfile::Counter* const bytes_read_counter_;

  /// Number of read operations that use mem buffer.
  RuntimeProfile::Counter* const read_use_mem_counter_;

  /// Number of bytes read from mem buffer.
  RuntimeProfile::Counter* const bytes_read_use_mem_counter_;

  /// Number of read operations that use local disk buffer.
  RuntimeProfile::Counter* const read_use_local_disk_counter_;

  /// Number of bytes read from local disk buffer.
  RuntimeProfile::Counter* const bytes_read_use_local_disk_counter_;

  /// Amount of scratch space allocated in bytes.
  RuntimeProfile::Counter* const scratch_space_bytes_used_counter_;

  /// Time spent waiting for disk reads.
  RuntimeProfile::Counter* const disk_read_timer_;

  /// Time spent in disk spill encryption, decryption, and integrity checking.
  RuntimeProfile::Counter* encryption_timer_;

  /// Time spent in disk spill compression and decompression. nullptr if compression
  /// is not enabled.
  RuntimeProfile::Counter* compression_timer_;

  /// Protects tmp_files_remote_ptrs_.
  SpinLock tmp_files_remote_ptrs_lock_;

  /// A map of raw pointer and its shared_ptr of remote TmpFiles.
  std::unordered_map<TmpFile*, std::shared_ptr<TmpFile>> tmp_files_remote_ptrs_;

  /// Protects below members.
  SpinLock lock_;

  /// List of files representing the TmpFileGroup. Files are ordered by the priority of
  /// the related TmpDir.
  std::vector<std::unique_ptr<TmpFile>> tmp_files_;

  /// Number of files in TmpFileGroup which have been blacklisted.
  int num_blacklisted_files_;

  /// Set to true to indicate spill-to-disk failure caused by faulty disks. It is set as
  /// true if all temporary (a.k.a. scratch) files in this TmpFileGroup are blacklisted,
  /// or getting disk error when reading temporary file back.
  bool spilling_disk_faulty_;

  /// List of remote files representing the TmpFileGroup.
  std::vector<std::shared_ptr<TmpFile>> tmp_files_remote_;

  /// Index Range in the 'tmp_files'. Used to keep track of index range
  /// corresponding to a given priority.
  struct TmpFileIndexRange {
    TmpFileIndexRange(int start, int end)
      : start(start), end(end) {}
    // Start index of the range.
    const int start;
    // End index of the range.
    const int end;
  };
  /// Map storing the index range in the 'tmp_files', corresponding to scratch dirs's
  /// priority.
  std::map<int, TmpFileIndexRange> tmp_files_index_range_;

  /// Total space allocated in this group's files.
  AtomicInt64 current_bytes_allocated_;

  /// Total space allocated remotely in this group's files.
  AtomicInt64 current_bytes_allocated_remote_;

  /// Index into 'tmp_files' denoting the file to which the next temporary file range
  /// should be allocated from, for a given priority. Used to implement round-robin
  /// allocation from temporary files.
  std::unordered_map<int, int> next_allocation_index_;

  /// Each vector in free_ranges_[i] is a vector of File/offset pairs for free scratch
  /// ranges of length 2^i bytes. Has 64 entries so that every int64_t length has a
  /// valid list associated with it.
  /// Only used if --disk_spill_punch_holes is false.
  std::vector<std::vector<std::pair<TmpFile*, int64_t>>> free_ranges_;

  /// Errors encountered when creating/writing scratch files. We store the history so
  /// that we can report the original cause of the scratch errors if we run out of
  /// devices to write to.
  std::vector<Status> scratch_errors_;
};

/// A handle to a write operation, backed by a range of a temporary file. The operation
/// is either in-flight or has completed. If it completed with no error and wasn't
/// cancelled then the data is in the file and can be read back.
///
/// TmpWriteHandle is returned from TmpFileGroup::Write(). After the write completes, the
/// handle can be passed to TmpFileGroup::Read() to read back the data zero or more
/// times. TmpFileGroup::DestroyWriteHandle() can be called at any time to destroy the
/// handle and allow reuse of the scratch file range written to. Alternatively,
/// TmpFileGroup::RestoreData() can be called to reverse the effects of
/// TmpFileGroup::Write() by destroying the handle and restoring the original data to
/// the buffer, so long as the data in the buffer was not modified by the caller.
///
/// Public methods of TmpWriteHandle are safe to call concurrently from multiple threads.
class TmpWriteHandle {
 public:
  /// The write must be destroyed by passing it to TmpFileGroup - destroying it before
  /// the write completes is an error.
  ~TmpWriteHandle();

  /// Cancel any in-flight read synchronously.
  void CancelRead();

  /// Path of temporary file backing the block. Intended for use in testing.
  /// Returns empty string if no backing file allocated.
  std::string TmpFilePath() const;

  /// Buffer path of temporary file backing the block. Intended for use in testing.
  /// Returns empty string if no backing file allocated or the temporary file is not in
  /// remote.
  std::string TmpFileBufferPath() const;

  /// The length of the in-memory data written to disk in bytes, before any compression.
  int64_t data_len() const { return data_len_; }

  /// The size of the data on disk (after compression) in bytes. Only valid to call if
  /// Write() succeeds.
  int64_t on_disk_len() const;

  bool is_compressed() const { return compressed_len_ >= 0; }

  std::string DebugString();

 private:
  friend class TmpFileGroup;
  friend class TmpFileMgrTest;

  TmpWriteHandle(TmpFileGroup* const parent, TmpFileMgr::WriteDoneCallback cb);

  /// Starts a write. This method allocates space in the file, compresses (if needed) and
  /// encrypts (if needed). 'write_in_flight_' must be false before calling. After
  /// returning, 'write_in_flight_' is true on success or false on failure and
  /// 'is_cancelled_' is set to true on failure. If the data was compressed,
  /// 'compressed_len_' will be non-negative and 'compressed_' will be the temporary
  /// buffer used to hold the compressed data.
  /// If non-null, the counters in 'counters' are updated with information about the read.
  Status Write(io::RequestContext* io_ctx, MemRange buffer,
      TmpFileMgr::WriteDoneCallback callback,
      const BufferPoolClientCounters* counters = nullptr);

  /// Try to compress 'buffer'. On success, returns true and 'compressed_' and
  /// 'compressed_len_' contain the buffer used (with the length reflecting the
  /// allocated size) and the length of the compressed data, respectively. On failure,
  /// returns false and 'compressed_' will be an empty buffer and 'compressed_len_'
  /// will be -1. The reason for the failure to compress may be logged.
  /// If non-null, the counters in 'counters' are updated with compression time.
  bool TryCompress(MemRange buffer, const BufferPoolClientCounters* counters);

  /// Retry the write after the initial write failed with an error, instead writing to
  /// 'offset' of 'file'. 'write_in_flight_' must be true before calling.
  /// After returning, 'write_in_flight_' is true on success or false on failure.
  Status RetryWrite(io::RequestContext* io_ctx, TmpFile* file,
      int64_t offset) WARN_UNUSED_RESULT;

  /// Called when the write has completed successfully or not. Sets 'write_in_flight_'
  /// then calls 'cb_'.
  void WriteComplete(const Status& write_status);

  /// Called when the upload has completed successfully or not.
  /// Enqueue the file into the available pool if the upload succeeds.
  void UploadComplete(TmpFile* file, const Status& write_status);

  /// Cancels any in-flight writes or reads. Reads are cancelled synchronously and
  /// writes are cancelled asynchronously. After Cancel() is called, writes are not
  /// retried. The write callback may be called with a CANCELLED_INTERNALLY status
  /// (unless it succeeded or encountered a different error first).
  void Cancel();

  /// Blocks until the write completes either successfully or unsuccessfully.
  /// May return before the write callback has been called.
  void WaitForWrite();

  /// Encrypts the data in 'buffer' in-place and computes 'hash_'.
  /// If non-null, the counters in 'counters' are updated with compression time.
  Status EncryptAndHash(MemRange buffer, const BufferPoolClientCounters* counters);

  /// Verifies the integrity hash and decrypts the contents of 'buffer' in place.
  /// If non-null, the counters in 'counters' are updated with compression time.
  Status CheckHashAndDecrypt(MemRange buffer, const BufferPoolClientCounters* counters);

  /// Free 'compressed_' and update memory accounting. No-op if 'compressed_' is empty.
  void FreeCompressedBuffer();

  TmpFileGroup* const parent_;

  /// Callback to be called when the write completes.
  TmpFileMgr::WriteDoneCallback cb_;

  /// Length of the in-memory data buffer that was written to disk. If compression
  /// is in use, this is the uncompressed size. Set in Write().
  int64_t data_len_ = -1;

  /// The DiskIoMgr write range for this write.
  boost::scoped_ptr<io::WriteRange> write_range_;

  /// The temporary file being written to.
  TmpFile* file_ = nullptr;

  /// If --disk_spill_encryption is on, a AES 256-bit key and initialization vector.
  /// Regenerated for each write.
  EncryptionKey key_;

  /// If --disk_spill_encryption is on, our hash of the data being written. Filled in
  /// on writes; verified on reads. This is calculated _after_ encryption.
  IntegrityHash hash_;

  /// The scan range for the read that is currently in flight. NULL when no read is in
  /// flight.
  io::ScanRange* read_range_ = nullptr;

  /// Protects all fields below while 'write_in_flight_' is true. At other times, it is
  /// invalid to call WriteRange/TmpFileGroup methods concurrently from multiple
  /// threads, so no locking is required.
  /// The lock should not be held while invoking 'cb_' to avoid a deadlock.
  std::mutex write_state_lock_;

  /// True if the the write has been cancelled (but is not necessarily complete).
  bool is_cancelled_ = false;

  /// True if a write is in flight.
  bool write_in_flight_ = false;

  /// The buffer used to store compressed data. Buffer is allocated while reading or
  /// writing a compressed range.
  /// TODO: ScopedBuffer is a suboptimal memory allocation approach. We would be better
  /// off integrating more directly with the buffer pool to use its buffer allocator and
  /// making the compression buffers somehow evictable.
  ScopedBuffer compressed_;

  /// Set to non-negative if the data in this range was compressed. In that case,
  /// 'compressed_' is the buffer used to store the data and 'compressed_len_' is the
  /// amount of valid data in the buffer.
  int64_t compressed_len_ = -1;

  /// Signalled when the write completes and 'write_in_flight_' becomes false, before
  /// 'cb_' is invoked.
  ConditionVariable write_complete_cv_;
};
}
