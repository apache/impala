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

#include "runtime/exec-env.h"
#include "runtime/io/file-reader.h"
#include "runtime/io/request-context.h"
#include "runtime/io/scan-buffer-manager.h"

using namespace impala;
using namespace impala::io;
using namespace std;

// Implementation of the buffer management for ScanRange.
ScanBufferManager::ScanBufferManager(ScanRange* range): scan_range_(range),
    buffer_tag_(BufferTag::INTERNAL_BUFFER) {
  DCHECK(range != nullptr);
}

ScanBufferManager::~ScanBufferManager() {
  DCHECK_EQ(0, ready_buffers_.size());
  DCHECK_EQ(0, num_buffers_in_reader_.Load());
}

void ScanBufferManager::Init() {
  unused_iomgr_buffer_bytes_ = 0;
  iomgr_buffer_cumulative_bytes_used_ = 0;
}

bool ScanBufferManager::AddUnusedBuffers(const unique_lock<mutex>& scan_range_lock,
    vector<unique_ptr<BufferDescriptor>>&& buffers, bool returned) {
  DCHECK(scan_range_->is_locked(scan_range_lock));

  if (returned) {
    // Buffers were in reader but now aren't.
    num_buffers_in_reader_.Add(-buffers.size());
  }
  bool buffer_added = false;
  for (unique_ptr<BufferDescriptor>& buffer : buffers) {
    // We should not hold onto the buffers in the following cases:
    // 1. the scan range is using external buffers, e.g. cached buffers.
    // 2. the scan range is cancelled
    // 3. the scan range already hit eosr
    // 4. we already have enough buffers to read the remainder of the scan range.
    if (buffer_tag_ != BufferTag::INTERNAL_BUFFER
        || scan_range_->is_cancelled()
        || scan_range_->is_eosr_queued()
        || unused_iomgr_buffer_bytes_ >=
           scan_range_->len() - iomgr_buffer_cumulative_bytes_used_) {
      CleanUpBuffer(scan_range_lock, move(buffer));
    } else {
      unused_iomgr_buffer_bytes_ += buffer->buffer_len();
      unused_iomgr_buffers_.emplace_back(move(buffer));
      buffer_added = true;
    }
  }
  return buffer_added;
}

unique_ptr<BufferDescriptor> ScanBufferManager::GetUnusedBuffer(
    const unique_lock<mutex>& scan_range_lock) {
  DCHECK(scan_range_->is_locked(scan_range_lock));
  if (unused_iomgr_buffers_.empty()) return nullptr;
  unique_ptr<BufferDescriptor> result = move(unused_iomgr_buffers_.back());
  unused_iomgr_buffers_.pop_back();
  unused_iomgr_buffer_bytes_ -= result->buffer_len();
  return result;
}

void ScanBufferManager::EnqueueReadyBuffer(
    const std::unique_lock<std::mutex>& scan_range_lock,
    unique_ptr<BufferDescriptor> buffer) {
  DCHECK(scan_range_->is_locked(scan_range_lock));
  DCHECK(buffer->buffer_ != nullptr) << "Cannot enqueue freed buffer";
  if (scan_range_->is_cancelled()) {
    // For scan range cancelled, no need to enqueue the buffer.
    CleanUpBuffer(scan_range_lock, move(buffer));
  } else {
    // Clean up any surplus buffers if eosr is hit. E.g. we may have allocated too many
    // if the file was shorter than expected.
    if (buffer->eosr()) CleanUpUnusedBuffers(scan_range_lock);
    ready_buffers_.emplace_back(move(buffer));
  }
}

Status ScanBufferManager::AllocateBuffersForRange(
    BufferPool::ClientHandle* bp_client, int64_t max_bytes,
    vector<unique_ptr<BufferDescriptor>>& buffers,
    int64_t min_buffer_size, int64_t max_buffer_size) {
  DCHECK_GE(max_bytes, min_buffer_size);
  DCHECK(buffers.empty());
  DCHECK(buffer_tag_ == BufferTag::INTERNAL_BUFFER)
      << static_cast<int>(buffer_tag_) << " invalid to allocate buffers "
      << "when already reading into an external buffer";
  BufferPool* bp = ExecEnv::GetInstance()->buffer_pool();
  Status status;
  vector<int64_t> buffer_sizes = ChooseBufferSizes(scan_range_->bytes_to_read(),
      max_bytes, min_buffer_size, max_buffer_size);
  for (int64_t buffer_size : buffer_sizes) {
    BufferPool::BufferHandle handle;
    status = bp->AllocateBuffer(bp_client, buffer_size, &handle);
    if (!status.ok()) {
      return status;
    }
    buffers.emplace_back(new BufferDescriptor(scan_range_, bp_client, move(handle)));
  }
  return Status::OK();
}

void ScanBufferManager::CleanUpBuffers(const unique_lock<mutex>& scan_range_lock,
    vector<unique_ptr<BufferDescriptor>>&& buffers) {
  for (unique_ptr<BufferDescriptor>& buffer : buffers) {
    CleanUpBuffer(scan_range_lock, move(buffer));
  }
}

void ScanBufferManager::CleanUpBuffer(const unique_lock<mutex>& scan_range_lock,
    const unique_ptr<BufferDescriptor> buffer_desc) {
  DCHECK(scan_range_->is_locked(scan_range_lock));
  DCHECK(buffer_desc != nullptr);
  DCHECK_EQ(buffer_desc->scan_range(), scan_range_);
  buffer_desc->Free();
  // Close the reader if there are no buffer in the reader or if no buffers will be
  // returned from the range in future.
  scan_range_->CloseReader(scan_range_lock);
}

void ScanBufferManager::CleanUpUnusedBuffers(const unique_lock<mutex>& scan_range_lock) {
  DCHECK(scan_range_->is_locked(scan_range_lock));
  while (!unused_iomgr_buffers_.empty()) {
    CleanUpBuffer(scan_range_lock, GetUnusedBuffer(scan_range_lock));
  }
}

void ScanBufferManager::CleanUpReadyBuffers(const unique_lock<mutex>& scan_range_lock) {
  DCHECK(scan_range_->is_locked(scan_range_lock));
  while (!ready_buffers_.empty()) {
    CleanUpBuffer(scan_range_lock, move(ready_buffers_.front()));
    ready_buffers_.pop_front();
  }
}

bool ScanBufferManager::PopFirstReadyBuffer(const unique_lock<mutex>& scan_range_lock,
    unique_ptr<BufferDescriptor>* buffer) {
  DCHECK(scan_range_->is_locked(scan_range_lock));
  if(ready_buffers_.empty()) {
    return false;
  }
  *buffer = move(ready_buffers_.front());
  ready_buffers_.pop_front();
  // If eosr is seen, then unused buffer should be empty.
  DCHECK(!(*buffer)->eosr() || unused_iomgr_buffers_.empty()) << DebugString();
  return true;
}

bool ScanBufferManager::Validate(const std::unique_lock<std::mutex>& scan_range_lock) {
  DCHECK(scan_range_->is_locked(scan_range_lock));
  // State of 'scan_range_' to validate against.
  bool range_cancelled = scan_range_->is_cancelled();
  bool eosr_queued = scan_range_->is_eosr_queued();
  bool blocked_on_buffer = scan_range_->is_blocked_on_buffer();

  if (range_cancelled && !ready_buffers_.empty()) {
    LOG(ERROR) << "Cancelled range should not have queued buffers";
    return false;
  }
  int64_t unused_iomgr_buffer_bytes = 0;
  for (auto& buffer : unused_iomgr_buffers_) {
    unused_iomgr_buffer_bytes += buffer->buffer_len();
  }
  if (unused_iomgr_buffer_bytes != unused_iomgr_buffer_bytes_) {
    LOG(ERROR) << "unused_iomgr_buffer_bytes_ incorrect actual: "
               << unused_iomgr_buffer_bytes_
               << " vs. expected: " << unused_iomgr_buffer_bytes;
    return false;
  }
  bool is_finished = range_cancelled || eosr_queued;
  if (is_finished && !unused_iomgr_buffers_.empty()) {
    LOG(ERROR) << "Held onto too many buffers "
               << unused_iomgr_buffers_.size()
               << " bytes: " << unused_iomgr_buffer_bytes_
               << " cancel_status: " << range_cancelled
               << " eosr_queued: " << eosr_queued;
    return false;
  }

  if (!is_finished && blocked_on_buffer &&
      !unused_iomgr_buffers_.empty()) {
    LOG(ERROR) << "ScanRange is Blocked despite having buffers";
    return false;
  }
  return true;
}
