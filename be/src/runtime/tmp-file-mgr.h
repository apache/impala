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

#include <mutex>
#include <boost/scoped_ptr.hpp>
#include <gtest/gtest_prod.h>

#include "common/object-pool.h"
#include "common/status.h"
#include "gen-cpp/CatalogObjects_types.h" // for THdfsCompression
#include "gen-cpp/Types_types.h" // for TUniqueId
#include "runtime/scoped-buffer.h"
#include "util/condition-variable.h"
#include "util/mem-range.h"
#include "util/metrics-fwd.h"
#include "util/openssl-util.h"
#include "util/runtime-profile.h"
#include "util/spinlock.h"

namespace impala {
namespace io {
  class DiskIoMgr;
  class RequestContext;
  class ScanRange;
  class WriteRange;
}
struct BufferPoolClientCounters;
class MemTracker;
class TmpFile;
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

  /// A configured temporary directory that TmpFileMgr allocates files in.
  struct TmpDir {
    TmpDir(const std::string& path, int64_t bytes_limit, int priority,
      IntGauge* bytes_used_metric)
      : path(path), bytes_limit(bytes_limit), priority(priority),
        bytes_used_metric(bytes_used_metric) {}

    /// Path to the temporary directory.
    const std::string path;

    /// Limit on bytes that should be written to this path. Set to maximum value
    /// of int64_t if there is no limit.
    const int64_t bytes_limit;

    /// Scratch directory priority.
    const int priority;

    /// The current bytes of scratch used for this temporary directory.
    IntGauge* const bytes_used_metric;
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

  /// Return the scratch directory path for the device.
  std::string GetTmpDirPath(DeviceId device_id) const;

  /// Total number of devices with tmp directories that are active. There is one tmp
  /// directory per device.
  int NumActiveTmpDevices();

  /// Return vector with device ids of all tmp devices being actively used.
  /// I.e. those that haven't been blacklisted.
  std::vector<DeviceId> ActiveTmpDevices();

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
  friend class TmpFileMgrTest;
  friend class TmpFile;
  friend class TmpFileGroup;

  /// Return a new TmpFile handle with a path based on file_group->unique_id. The file is
  /// associated with the 'file_group' and the file path is within the (single) scratch
  /// directory on the specified device id. The caller owns the returned handle and is
  /// responsible for deleting it. The file is not created - creation is deferred until
  /// the file is written.
  void NewFile(TmpFileGroup* file_group, DeviceId device_id,
    std::unique_ptr<TmpFile>* new_file);

  bool initialized_ = false;

  /// The type of spill-to-disk compression in use for spilling. NONE means no
  /// compression is used.
  THdfsCompression::type compression_codec_ = THdfsCompression::NONE;

  /// The compression level, which is used for certain compression codecs like ZSTD
  /// and ignored otherwise. -1 means not set/invalid.
  int compression_level_ = -1;

  /// Whether hole punching is enabled.
  bool punch_holes_ = false;

  /// The paths of the created tmp directories.
  std::vector<TmpDir> tmp_dirs_;

  /// Memory tracker to track compressed buffers. Set up in InitCustom() if disk spill
  /// compression is enabled
  std::unique_ptr<MemTracker> compressed_buffer_tracker_;

  /// Metrics to track active scratch directories.
  IntGauge* num_active_scratch_dirs_metric_ = nullptr;
  SetMetric<std::string>* active_scratch_dirs_metric_ = nullptr;

  /// Metrics to track the scratch space HWM.
  AtomicHighWaterMarkGauge* scratch_bytes_used_metric_ = nullptr;
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

  std::string DebugString();

  const TUniqueId& unique_id() const { return unique_id_; }

  TmpFileMgr* tmp_file_mgr() const { return tmp_file_mgr_; }

 private:
  friend class TmpFile;
  friend class TmpFileMgrTest;
  friend class TmpWriteHandle;

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

  /// Amount of scratch space allocated in bytes.
  RuntimeProfile::Counter* const scratch_space_bytes_used_counter_;

  /// Time spent waiting for disk reads.
  RuntimeProfile::Counter* const disk_read_timer_;

  /// Time spent in disk spill encryption, decryption, and integrity checking.
  RuntimeProfile::Counter* encryption_timer_;

  /// Time spent in disk spill compression and decompression. nullptr if compression
  /// is not enabled.
  RuntimeProfile::Counter* compression_timer_;

  /// Protects below members.
  SpinLock lock_;

  /// List of files representing the TmpFileGroup. Files are ordered by the priority of
  /// the related TmpDir.
  std::vector<std::unique_ptr<TmpFile>> tmp_files_;

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
  int64_t current_bytes_allocated_;

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
  /// threads,
  /// so no locking is required. This is a terminal lock and should not be held while
  /// acquiring other locks or invoking 'cb_'.
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
