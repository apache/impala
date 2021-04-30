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

#include <algorithm>
#include <vector>

#include "util/condition-variable.h"
#include "util/bit-util.h"

namespace impala {
namespace io {
// Forward Declaration
class ScanRange;
class BufferDescriptor;
class ScanRangeLockStore;

// Implementation of the buffer management for ScanRange. Each ScanRange contains a queue
// of ready buffers and a queue of unused buffers. For each ScanRange, there is only a
// single producer and consumer thread, i.e. only one disk thread will push to a scan
// range at any time and only one thread will remove from the queue. This is to guarantee
// that buffers are queued and read in file order.
//
// BUFFER LIFECYCLE:
// Disk thread will use the buffer from unused buffer queue to read the data. Once data
// is read into buffer it will be put into ready buffers and consumer thread will read it
// using ScanRange::GetNext(). Once read consumer will return that buffer for reuse using
// ScanRange::ReturnBuffer() which gets added to unused buffers again. Once data read is
// completed by ScanRange or when it gets cancelled, all the remaining buffers in both
// queues get freed.
class ScanBufferManager {
 public:

  /// Tag for the buffer associated with scan range using this buffer manager.
  /// There are 3 tags and each identify different types of buffer used for read:
  /// a) CLIENT_BUFFER: A client allocated buffer, large enough to fit the whole scan
  ///    range's data, that is provided by the caller when constructing the scan range.
  ///    This buffer is external to this buffer manager and is not managed by it
  ///    i.e., not allocated, freed or maintained in internal queues.
  /// b) CACHED_BUFFER: Cached HDFS buffers if the scan range was read from the HDFS
  ///    cache. Again like CLIENT_BUFFER this is external to this buffer manager.
  /// c) INTERNAL_BUFFER: It represents buffers allocated and managed by this buffer
  ///    manager. IoMgr allocates the buffer via AllocateBuffersForRange() and this
  ///    manager maintains them in their internal queues.
  enum class BufferTag { CLIENT_BUFFER, CACHED_BUFFER, INTERNAL_BUFFER };

  /// Creates ScanBufferManager for a ScanRange. It will take care of buffer
  /// management for the respective range.
  ScanBufferManager(ScanRange* range);

  ~ScanBufferManager();

  void Init();

  /// Add 'buffers' to read data into. Buffer is added to 'unused_iomgr_buffers_' .
  /// Need to take lock on scan range using this buffer manager before invoking this
  /// function.
  /// If 'returned' is true, the buffers returned from ScanRange::GetNext() that are
  /// being recycled via ScanRange::ReturnBuffer(). Otherwise the buffers are newly
  /// allocated buffers to be added.
  /// Returns 'true' if at least one buffer gets added to 'unused_iomgr_buffers_'.
  bool AddUnusedBuffers(const std::unique_lock<std::mutex>& scan_range_lock,
    std::vector<std::unique_ptr<BufferDescriptor>>&& buffers, bool returned);

  /// Remove a buffer from 'unused_iomgr_buffers_' and update
  /// 'unused_iomgr_buffer_bytes_'. If 'unused_iomgr_buffers_' is empty, return nullptr.
  /// 'scan_range_->lock_' must be held by the caller via 'scan_range_lock'.
  std::unique_ptr<BufferDescriptor> GetUnusedBuffer(
      const std::unique_lock<std::mutex>& scan_range_lock);

  /// Enqueues into 'ready_buffer_' with valid data.
  /// The caller passes ownership of buffer to buffer manager and it is not valid
  /// to access buffer after this call. It needs lock taken upon ScanRange using
  /// this buffer manager before invoking this. If 'scan_range_' is already cancelled,
  /// 'buffer' will be cleaned up instead of enqueing into 'ready_buffer_'.
  void EnqueueReadyBuffer(const std::unique_lock<std::mutex>& scan_range_lock,
      std::unique_ptr<BufferDescriptor> buffer);

  /// Allocates up to 'max_bytes' buffers and adds it to 'buffers'.
  /// Called from ScanRange::AllocateBuffersForRange
  ///
  /// The buffer sizes are chosen based on 'scan_range_->len()'. 'max_bytes' must be >=
  /// 'min_buffer_size' so that at least one buffer can be allocated. The caller
  /// must ensure that 'bp_client' has at least 'max_bytes' unused reservation.
  /// Returns ok if the buffers were successfully allocated.
  Status AllocateBuffersForRange(
      BufferPool::ClientHandle* bp_client, int64_t max_bytes,
      std::vector<std::unique_ptr<BufferDescriptor>>& buffers, int64_t min_buffer_size,
      int64_t max_buffer_size);

  /// Cleans up a buffer that is not being recycled or returned by client.
  /// The caller must hold 'scan_range_->lock_' via 'scan_range_lock'.
  /// This function may acquire 'scan_range_->file_reader_->lock()'
  void CleanUpBuffer(const std::unique_lock<std::mutex>& scan_range_lock,
      const std::unique_ptr<BufferDescriptor> buffer);

  /// Same as CleanUpBuffer() except cleans up multiple buffers. Caller must
  /// hold 'scan_range_->lock_' via 'scan_range_lock'.
  void CleanUpBuffers(const std::unique_lock<std::mutex>& scan_range_lock,
      std::vector<std::unique_ptr<BufferDescriptor>>&& buffers);

  /// Clean up all buffers in 'unused_iomgr_buffers_'. Only valid to call when the scan
  /// range is cancelled or at eos. The caller must hold 'scan_range_->lock_' via
  /// 'scan_range_lock'.
  void CleanUpUnusedBuffers(const std::unique_lock<std::mutex>& scan_range_lock);

  /// Clean up all buffers in 'ready_buffer_'. Caller must hold 'scan_range_->lock_' via
  /// 'scan_range_lock'. Only valid to call when scan range using this buffer manager
  /// is cancelled.
  void CleanUpReadyBuffers(const std::unique_lock<std::mutex>& scan_range_lock);

  std::string DebugString() const {
    std::stringstream ss;
    ss << " buffer_queue=" << ready_buffers_.size()
       << " num_buffers_in_readers=" << num_buffers_in_reader_.Load()
       << " unused_iomgr_buffers=" << unused_iomgr_buffers_.size()
       << " unused_iomgr_buffer_bytes=" << unused_iomgr_buffer_bytes_;
    return ss.str();
  }

  /// Remove the first buffer from 'ready_buffer_' and assign it to '*buffer'.
  /// Returns 'false', if 'ready_buffer_' is empty and '*buffer' cannot be assigned,
  /// 'false' otherwise.
  /// 'scan_range_->lock_' needs to be held before invoking this method.
  bool PopFirstReadyBuffer(const std::unique_lock<std::mutex>& scan_range_lock,
      std::unique_ptr<BufferDescriptor>* buffer);

  /// Validates the buffer state. Validation is done based on the state of
  /// ScanRange using this manager.
  /// 'scan_range_->lock_' needs to be held via 'scan_range_lock' before invoking
  /// this method.
  bool Validate(const std::unique_lock<std::mutex>& scan_range_lock);

  void set_cached_buffer() {
    buffer_tag_ = BufferTag::CACHED_BUFFER;
  }

  void set_client_buffer() {
    buffer_tag_ = BufferTag::CLIENT_BUFFER;
  }

  void set_internal_buffer() {
    buffer_tag_ = BufferTag::INTERNAL_BUFFER;
  }

  bool is_cached() const {
    return buffer_tag_ == BufferTag::CACHED_BUFFER;
  }

  bool is_client_buffer() const {
    return buffer_tag_ == BufferTag::CLIENT_BUFFER;
  }

  bool is_internal_buffer() const {
    return buffer_tag_ == BufferTag::INTERNAL_BUFFER;
  }

  BufferTag buffer_tag() const { return buffer_tag_; }

  bool is_readybuffer_empty() const { return ready_buffers_.empty(); }

  int num_buffers_in_reader() const { return num_buffers_in_reader_.Load(); }

  void add_buffers_in_reader(int inc) { num_buffers_in_reader_.Add(inc); }

  void add_iomgr_buffer_cumulative_bytes_used(int inc) {
    iomgr_buffer_cumulative_bytes_used_ += inc;
  }

 private:

  /// Scan range that uses this buffer manager.
  ScanRange* const scan_range_;

  /// The number of buffers that have been returned to a client via GetNext() that have
  /// not yet been returned.
  AtomicInt32 num_buffers_in_reader_{0};

  /// Buffers to read into, used if the 'buffer_tag_' is INTERNAL_BUFFER.
  /// These are initially populated when the client calls AllocateBuffersForRange()
  /// and are used to read scanned data into. Buffers are taken from this vector for
  /// every read and added back after use.
  std::vector<std::unique_ptr<BufferDescriptor>> unused_iomgr_buffers_;

  /// Total number of bytes of buffers in 'unused_iomgr_buffers_'.
  int64_t unused_iomgr_buffer_bytes_ = 0;

  /// Cumulative bytes of I/O mgr buffers taken from 'unused_iomgr_buffers_' by DoRead().
  /// Used to infer how many bytes of buffers need to be held onto to read the rest of
  /// the scan range.
  int64_t iomgr_buffer_cumulative_bytes_used_ = 0;

  /// IO buffers that are queued for this scan range. When Cancel() is called
  /// this is drained by the cancelling thread. I.e. this is always empty if
  /// 'cancel_status_' is not OK.
  std::deque<std::unique_ptr<BufferDescriptor>> ready_buffers_;

  /// Tag that represents the kind of buffers used to read scan data.
  /// Please see comments for enum 'BufferTag' for more details.
  BufferTag buffer_tag_;

  /// Choose buffer sizes to read scan range's data of length 'scan_range_len'.
  /// 'min_buffer_size' and 'max_buffer_size' are minimum and maximum buffer size
  /// that can be allocated. Additionally, cumulative sum of buffer sizes should not
  /// exceed 'max_bytes'.
  static std::vector<int64_t> ChooseBufferSizes(int64_t scan_range_len,
      int64_t max_bytes, int64_t min_buffer_size, int64_t max_buffer_size) {
    DCHECK_GE(max_bytes, min_buffer_size);
    std::vector<int64_t> buffer_sizes;
    int64_t bytes_allocated = 0;
    while (bytes_allocated < scan_range_len) {
      int64_t bytes_remaining = scan_range_len - bytes_allocated;
      // Either allocate a max-sized buffer or a smaller buffer to fit the rest of the
      // range.
      int64_t next_buffer_size;
      if (bytes_remaining >= max_buffer_size) {
        next_buffer_size = max_buffer_size;
      } else {
        next_buffer_size =
            std::max(min_buffer_size, BitUtil::RoundUpToPowerOfTwo(bytes_remaining));
      }
      if (next_buffer_size + bytes_allocated > max_bytes) {
        // Can't allocate the desired buffer size. Make sure to allocate at least one
        // buffer.
        if (bytes_allocated > 0) break;
        next_buffer_size = BitUtil::RoundDownToPowerOfTwo(max_bytes);
      }
      DCHECK(BitUtil::IsPowerOf2(next_buffer_size)) << next_buffer_size;
      buffer_sizes.push_back(next_buffer_size);
      bytes_allocated += next_buffer_size;
    }
    return buffer_sizes;
  }
};
}
}