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

#include "exec/scanner-context.inline.h"

#include <gutil/strings/substitute.h>

#include "exec/hdfs-scan-node-base.h"
#include "exec/hdfs-scan-node.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/mem-pool.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-buffer.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using namespace impala;
using namespace impala::io;
using namespace strings;

static const int64_t INIT_READ_PAST_SIZE_BYTES = 64 * 1024;

const int64_t ScannerContext::Stream::OUTPUT_BUFFER_BYTES_LEFT_INIT;

ScannerContext::ScannerContext(RuntimeState* state, HdfsScanNodeBase* scan_node,
    HdfsPartitionDescriptor* partition_desc, ScanRange* scan_range,
    const vector<FilterContext>& filter_ctxs, MemPool* expr_results_pool)
  : state_(state),
    scan_node_(scan_node),
    partition_desc_(partition_desc),
    filter_ctxs_(filter_ctxs),
    expr_results_pool_(expr_results_pool) {
  AddStream(scan_range);
}

ScannerContext::~ScannerContext() {
  DCHECK(streams_.empty());
}

void ScannerContext::ReleaseCompletedResources(bool done) {
  for (int i = 0; i < streams_.size(); ++i) {
    streams_[i]->ReleaseCompletedResources(done);
  }
}

void ScannerContext::ClearStreams() {
  streams_.clear();
}

ScannerContext::Stream::Stream(ScannerContext* parent, ScanRange* scan_range,
    const HdfsFileDesc* file_desc)
  : parent_(parent),
    scan_range_(scan_range),
    file_desc_(file_desc),
    file_len_(file_desc->file_length),
    next_read_past_size_bytes_(INIT_READ_PAST_SIZE_BYTES),
    boundary_pool_(new MemPool(parent->scan_node_->mem_tracker())),
    boundary_buffer_(new StringBuffer(boundary_pool_.get())) {
}

ScannerContext::Stream* ScannerContext::AddStream(ScanRange* range) {
  streams_.emplace_back(new Stream(
      this, range, scan_node_->GetFileDesc(partition_desc_->id(), range->file())));
  return streams_.back().get();
}

void ScannerContext::Stream::ReleaseCompletedResources(bool done) {
  if (done) {
    // Cancel the underlying scan range to clean up any queued buffers there
    scan_range_->Cancel(Status::CANCELLED);
    boundary_pool_->FreeAll();

    // Reset variables - the stream is no longer valid.
    io_buffer_pos_ = nullptr;
    io_buffer_bytes_left_ = 0;
    boundary_buffer_pos_ = nullptr;
    boundary_buffer_bytes_left_ = 0;
    boundary_buffer_->Reset();
  }
  // Check if we're done with the current I/O buffer.
  if (io_buffer_ != nullptr && io_buffer_bytes_left_ == 0) ReturnIoBuffer();
}

Status ScannerContext::Stream::GetNextBuffer(int64_t read_past_size) {
  DCHECK_EQ(0, io_buffer_bytes_left_);
  if (UNLIKELY(parent_->cancelled())) return Status::CANCELLED;
  if (io_buffer_ != nullptr) ReturnIoBuffer();

  // Nothing to do if we're at the end of the file - return leaving io_buffer_ == nullptr.
  int64_t offset = file_offset() + boundary_buffer_bytes_left_;
  int64_t file_bytes_remaining = file_desc()->file_length - offset;
  if (file_bytes_remaining == 0) return Status::OK();

  if (!scan_range_eosr_) {
    // Get the next buffer from 'scan_range_'.
    SCOPED_TIMER(parent_->state_->total_storage_wait_timer());
    Status status = scan_range_->GetNext(&io_buffer_);
    DCHECK(!status.ok() || io_buffer_ != nullptr);
    RETURN_IF_ERROR(status);
    scan_range_eosr_ = io_buffer_->eosr();
  } else {
    // Already got all buffers from 'scan_range_' - reading past end.
    SCOPED_TIMER(parent_->state_->total_storage_wait_timer());

    int64_t read_past_buffer_size = 0;
    int64_t max_buffer_size = parent_->state_->io_mgr()->max_read_buffer_size();
    if (!read_past_size_cb_.empty()) read_past_buffer_size = read_past_size_cb_(offset);
    if (read_past_buffer_size <= 0) {
      // Either no callback was set or the callback did not return an estimate. Use
      // the default doubling strategy.
      read_past_buffer_size = next_read_past_size_bytes_;
      next_read_past_size_bytes_ =
          min<int64_t>(next_read_past_size_bytes_ * 2, max_buffer_size);
    }
    read_past_buffer_size = ::max(read_past_buffer_size, read_past_size);
    read_past_buffer_size = ::min(read_past_buffer_size, file_bytes_remaining);
    read_past_buffer_size = ::min(read_past_buffer_size, max_buffer_size);
    // We're reading past the scan range. Be careful not to read past the end of file.
    DCHECK_GE(read_past_buffer_size, 0);
    if (read_past_buffer_size == 0) {
      io_buffer_bytes_left_ = 0;
      return Status::OK();
    }
    int64_t partition_id = parent_->partition_descriptor()->id();
    ScanRange* range = parent_->scan_node_->AllocateScanRange(
        scan_range_->fs(), filename(), read_past_buffer_size, offset, partition_id,
        scan_range_->disk_id(), false, BufferOpts::Uncached());
    RETURN_IF_ERROR(parent_->state_->io_mgr()->Read(
        parent_->scan_node_->reader_context(), range, &io_buffer_));
  }

  DCHECK(io_buffer_ != nullptr);
  if (UNLIKELY(io_buffer_ == nullptr)) {
    // This has bitten us before, so we defend against NULL in release builds here. It
    // indicates an error in the IoMgr, which did not return a valid buffer.
    // TODO(IMPALA-5914): Remove this check once we're confident we're not hitting it.
    return Status(TErrorCode::INTERNAL_ERROR, Substitute("Internal error: "
        "Failed to receive buffer from scan range for file $0 at offset $1",
        filename(), offset));
  }

  io_buffer_pos_ = reinterpret_cast<uint8_t*>(io_buffer_->buffer());
  io_buffer_bytes_left_ = io_buffer_->len();
  if (io_buffer_->len() == 0) {
    file_len_ = file_offset() + boundary_buffer_bytes_left_;
    VLOG_FILE << "Unexpectedly read 0 bytes from file=" << filename() << " table="
              << parent_->scan_node_->hdfs_table()->name()
              << ". Setting expected file length=" << file_len_;
  }
  return Status::OK();
}

Status ScannerContext::Stream::GetBuffer(bool peek, uint8_t** out_buffer, int64_t* len) {
  *out_buffer = nullptr;
  *len = 0;
  if (eosr()) return Status::OK();

  if (UNLIKELY(parent_->cancelled())) {
    DCHECK(*out_buffer == nullptr);
    return Status::CANCELLED;
  }

  if (boundary_buffer_bytes_left_ > 0) {
    DCHECK(ValidateBufferPointers());
    DCHECK_EQ(output_buffer_bytes_left_, &boundary_buffer_bytes_left_);
    *out_buffer = boundary_buffer_pos_;
    // Don't return more bytes past eosr
    *len = min(boundary_buffer_bytes_left_, bytes_left());
    DCHECK_GE(*len, 0);
    if (!peek) {
      AdvanceBufferPos(*len, &boundary_buffer_pos_, &boundary_buffer_bytes_left_);
      total_bytes_returned_ += *len;
    }
    return Status::OK();
  }

  if (io_buffer_bytes_left_ == 0) {
    // We're at the end of the boundary buffer and the current IO buffer. Get a new IO
    // buffer and set the current buffer to it.
    RETURN_IF_ERROR(GetNextBuffer());
    // Check that we're not pointing to the IO buffer if there are bytes left in the
    // boundary buffer.
    DCHECK_EQ(boundary_buffer_bytes_left_, 0);
    output_buffer_pos_ = &io_buffer_pos_;
    output_buffer_bytes_left_ = &io_buffer_bytes_left_;
  }
  DCHECK(io_buffer_ != nullptr);

  *out_buffer = io_buffer_pos_;
  *len = io_buffer_bytes_left_;
  if (!peek) {
    AdvanceBufferPos(*len, &io_buffer_pos_, &io_buffer_bytes_left_);
    total_bytes_returned_ += *len;
  }
  DCHECK_GE(bytes_left(), 0);
  DCHECK(ValidateBufferPointers());
  return Status::OK();
}

Status ScannerContext::Stream::GetBytesInternal(int64_t requested_len,
    uint8_t** out_buffer, bool peek, int64_t* out_len) {
  DCHECK_GT(requested_len, boundary_buffer_bytes_left_);
  DCHECK(output_buffer_bytes_left_ != &io_buffer_bytes_left_
      || requested_len > io_buffer_bytes_left_) << "All bytes in output buffer "
      << requested_len << " " << io_buffer_bytes_left_;
  *out_buffer = nullptr;

  if (boundary_buffer_bytes_left_ == 0) boundary_buffer_->Clear();
  DCHECK(ValidateBufferPointers());

  // First this loop ensures, by reading I/O buffers one-by-one, that we've got all of
  // the requested bytes in 'boundary_buffer_', 'io_buffer_', or split between the two.
  // We may not be able to get all of the bytes if we hit eof.
  while (boundary_buffer_bytes_left_ + io_buffer_bytes_left_ < requested_len) {
    if (io_buffer_bytes_left_ > 0) {
      // Copy the remainder of 'io_buffer_' to 'boundary_buffer_' before getting the next
      // 'io_buffer_'. Preallocate 'boundary_buffer_' to avoid unnecessary resizes for
      // large reads.
      RETURN_IF_ERROR(boundary_buffer_->GrowBuffer(requested_len));
      RETURN_IF_ERROR(CopyIoToBoundary(io_buffer_bytes_left_));
    }
    int64_t remaining_requested_len = requested_len - boundary_buffer_bytes_left_;
    RETURN_IF_ERROR(GetNextBuffer(remaining_requested_len));
    if (UNLIKELY(parent_->cancelled())) return Status::CANCELLED;
    // No more bytes (i.e. EOF).
    if (io_buffer_bytes_left_ == 0) break;
  }

  // We have read the full 'requested_len' bytes or hit eof.
  // We can assemble the contiguous bytes in two ways:
  // 1. if the the read range falls entirely with an I/O buffer, we return a pointer into
  //    that I/O buffer.
  // 2. if the read straddles I/O buffers, we append the data to 'boundary_buffer_'.
  //    'boundary_buffer_' may already contain some of the data that we need if we did a
  //    "peek" earlier.
  int64_t requested_bytes_left = requested_len - boundary_buffer_bytes_left_;
  DCHECK_GE(requested_bytes_left, 0);
  int64_t num_bytes_left_to_copy = min(io_buffer_bytes_left_, requested_bytes_left);
  *out_len = boundary_buffer_bytes_left_ + num_bytes_left_to_copy;
  DCHECK_LE(*out_len, requested_len);
  if (boundary_buffer_bytes_left_ == 0) {
    // Case 1: return a pointer into the I/O buffer.
    output_buffer_pos_ = &io_buffer_pos_;
    output_buffer_bytes_left_ = &io_buffer_bytes_left_;
  } else {
    // Case 2: return a pointer into the boundary buffer, after copying any required
    // data from the I/O buffer.
    DCHECK_EQ(output_buffer_pos_, &boundary_buffer_pos_);
    DCHECK_EQ(output_buffer_bytes_left_, &boundary_buffer_bytes_left_);
    if (io_buffer_bytes_left_ > 0) {
      RETURN_IF_ERROR(CopyIoToBoundary(num_bytes_left_to_copy));
    }
  }
  *out_buffer = *output_buffer_pos_;
  if (!peek) {
    total_bytes_returned_ += *out_len;
    AdvanceBufferPos(*out_len, output_buffer_pos_, output_buffer_bytes_left_);
  }
  DCHECK(ValidateBufferPointers());
  return Status::OK();
}

bool ScannerContext::Stream::SkipBytesInternal(
    int64_t length, int64_t bytes_left, Status* status) {
  DCHECK_GT(bytes_left, 0);
  DCHECK_EQ(0, boundary_buffer_bytes_left_);
  DCHECK_EQ(0, io_buffer_bytes_left_);
  // Skip data in subsequent buffers by simply fetching them.
  // TODO: consider adding support to skip ahead in a ScanRange so we can avoid doing
  // actual I/O in some cases.
  while (bytes_left > 0) {
    *status = GetNextBuffer(bytes_left);
    if (!status->ok()) return false;
    if (io_buffer_ == nullptr) {
      // Hit end of file before reading the requested bytes.
      DCHECK_GT(bytes_left, 0);
      *status = ReportIncompleteRead(length, length - bytes_left);
      return false;
    }
    int64_t io_buffer_bytes_to_skip = std::min(bytes_left, io_buffer_bytes_left_);
    AdvanceBufferPos(io_buffer_bytes_to_skip, &io_buffer_pos_, &io_buffer_bytes_left_);
    // Check if we skipped all data in this I/O buffer.
    if (io_buffer_bytes_left_ == 0) ReturnIoBuffer();
    bytes_left -= io_buffer_bytes_to_skip;
    total_bytes_returned_ += io_buffer_bytes_to_skip;
  }
  return true;
}

Status ScannerContext::Stream::CopyIoToBoundary(int64_t num_bytes) {
  DCHECK(io_buffer_ != nullptr);
  DCHECK_GT(io_buffer_bytes_left_, 0);
  DCHECK_GE(io_buffer_bytes_left_, num_bytes);
  RETURN_IF_ERROR(boundary_buffer_->Append(io_buffer_pos_, num_bytes));
  boundary_buffer_bytes_left_ += num_bytes;
  boundary_buffer_pos_ = reinterpret_cast<uint8_t*>(boundary_buffer_->buffer()) +
      boundary_buffer_->len() - boundary_buffer_bytes_left_;
  AdvanceBufferPos(num_bytes, &io_buffer_pos_, &io_buffer_bytes_left_);
  // If all data from I/O buffer was returned or copied to boundary buffer, we don't need
  // I/O buffer.
  if (io_buffer_bytes_left_ == 0) ReturnIoBuffer();
  output_buffer_pos_ = &boundary_buffer_pos_;
  output_buffer_bytes_left_ = &boundary_buffer_bytes_left_;
  return Status::OK();
}

void ScannerContext::Stream::ReturnIoBuffer() {
  DCHECK(io_buffer_ != nullptr);
  ExecEnv::GetInstance()->disk_io_mgr()->ReturnBuffer(move(io_buffer_));
  io_buffer_pos_ = nullptr;
  io_buffer_bytes_left_ = 0;
}

bool ScannerContext::cancelled() const {
  if (state_->is_cancelled()) return true;
  if (!scan_node_->HasRowBatchQueue()) return false;
  return static_cast<HdfsScanNode*>(scan_node_)->done();
}

bool ScannerContext::Stream::ValidateBufferPointers() const {
  // If there are bytes left in the boundary buffer, the output buffer pointers must point
  // to it.
  return boundary_buffer_bytes_left_ == 0 ||
      (output_buffer_pos_ == &boundary_buffer_pos_ &&
      output_buffer_bytes_left_ == &boundary_buffer_bytes_left_);
}

Status ScannerContext::Stream::ReportIncompleteRead(int64_t length, int64_t bytes_read) {
  return Status(TErrorCode::SCANNER_INCOMPLETE_READ, length, bytes_read,
      filename(), file_offset());
}

Status ScannerContext::Stream::ReportInvalidRead(int64_t length) {
  return Status(TErrorCode::SCANNER_INVALID_READ, length, filename(), file_offset());
}

Status ScannerContext::Stream::ReportInvalidInt() {
  return Status(TErrorCode::SCANNER_INVALID_INT, filename(), file_offset());
}
