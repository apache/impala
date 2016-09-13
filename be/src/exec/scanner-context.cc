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

#include "exec/scanner-context.h"

#include <gutil/strings/substitute.h>

#include "exec/hdfs-scan-node-base.h"
#include "exec/hdfs-scan-node.h"
#include "runtime/row-batch.h"
#include "runtime/mem-pool.h"
#include "runtime/runtime-state.h"
#include "runtime/string-buffer.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using namespace impala;
using namespace strings;

static const int64_t INIT_READ_PAST_SIZE_BYTES = 64 * 1024;

// We always want output_buffer_bytes_left_ to be non-NULL, so we can avoid a NULL check
// in GetBytes(). We use this variable, which is set to 0, to initialize
// output_buffer_bytes_left_. After the first successful call to GetBytes(),
// output_buffer_bytes_left_ will be set to something else.
static const int64_t OUTPUT_BUFFER_BYTES_LEFT_INIT = 0;

ScannerContext::ScannerContext(RuntimeState* state, HdfsScanNodeBase* scan_node,
    HdfsPartitionDescriptor* partition_desc, DiskIoMgr::ScanRange* scan_range,
    const vector<FilterContext>& filter_ctxs)
  : state_(state),
    scan_node_(scan_node),
    partition_desc_(partition_desc),
    num_completed_io_buffers_(0),
    filter_ctxs_(filter_ctxs) {
  AddStream(scan_range);
}

ScannerContext::~ScannerContext() {
  DCHECK(streams_.empty());
}

void ScannerContext::ReleaseCompletedResources(RowBatch* batch, bool done) {
  for (int i = 0; i < streams_.size(); ++i) {
    streams_[i]->ReleaseCompletedResources(batch, done);
  }
}

void ScannerContext::ClearStreams() {
  streams_.clear();
}

ScannerContext::Stream::Stream(ScannerContext* parent)
  : parent_(parent),
    next_read_past_size_bytes_(INIT_READ_PAST_SIZE_BYTES),
    boundary_pool_(new MemPool(parent->scan_node_->mem_tracker())),
    boundary_buffer_(new StringBuffer(boundary_pool_.get())) {
}

ScannerContext::Stream* ScannerContext::AddStream(DiskIoMgr::ScanRange* range) {
  std::unique_ptr<Stream> stream(new Stream(this));
  stream->scan_range_ = range;
  stream->file_desc_ = scan_node_->GetFileDesc(stream->filename());
  stream->file_len_ = stream->file_desc_->file_length;
  stream->total_bytes_returned_ = 0;
  stream->io_buffer_pos_ = NULL;
  stream->io_buffer_ = NULL;
  stream->io_buffer_bytes_left_ = 0;
  stream->boundary_buffer_bytes_left_ = 0;
  stream->output_buffer_pos_ = NULL;
  stream->output_buffer_bytes_left_ =
      const_cast<int64_t*>(&OUTPUT_BUFFER_BYTES_LEFT_INIT);
  stream->contains_tuple_data_ = scan_node_->tuple_desc()->ContainsStringData();
  streams_.push_back(std::move(stream));
  return streams_.back().get();
}

void ScannerContext::Stream::ReleaseCompletedResources(RowBatch* batch, bool done) {
  DCHECK(batch != nullptr || done || !contains_tuple_data_);
  if (done) {
    // Mark any pending resources as completed
    if (io_buffer_ != nullptr) {
      ++parent_->num_completed_io_buffers_;
      completed_io_buffers_.push_back(io_buffer_);
    }
    // Set variables to nullptr to make sure streams are not used again
    io_buffer_ = nullptr;
    io_buffer_pos_ = nullptr;
    io_buffer_bytes_left_ = 0;
    // Cancel the underlying scan range to clean up any queued buffers there
    scan_range_->Cancel(Status::CANCELLED);
  }

  for (DiskIoMgr::BufferDescriptor* buffer: completed_io_buffers_) {
    if (contains_tuple_data_ && batch != nullptr) {
      batch->AddIoBuffer(buffer);
      // TODO: We can do row batch compaction here.  This is the only place io buffers are
      // queued.  A good heuristic is to check the number of io buffers queued and if
      // there are too many, we should compact.
    } else {
      buffer->Return();
      parent_->scan_node_->num_owned_io_buffers_.Add(-1);
    }
  }
  parent_->num_completed_io_buffers_ -= completed_io_buffers_.size();
  completed_io_buffers_.clear();

  if (contains_tuple_data_ && batch != nullptr) {
    // If we're not done, keep using the last chunk allocated in boundary_pool_ so we
    // don't have to reallocate. If we are done, transfer it to the row batch.
    batch->tuple_data_pool()->AcquireData(boundary_pool_.get(), /* keep_current */ !done);
  }
  if (done) boundary_pool_->FreeAll();
}

Status ScannerContext::Stream::GetNextBuffer(int64_t read_past_size) {
  if (UNLIKELY(parent_->cancelled())) return Status::CANCELLED;

  // Nothing to do if we've already processed all data in the file
  int64_t offset = file_offset() + boundary_buffer_bytes_left_;
  int64_t file_bytes_remaining = file_desc()->file_length - offset;
  if (io_buffer_ == NULL && file_bytes_remaining == 0) return Status::OK();

  // Otherwise, io_buffer_ should only be null the first time this is called
  DCHECK(io_buffer_ != NULL ||
         (total_bytes_returned_ == 0 && completed_io_buffers_.empty()));

  // We can't use the eosr() function because it reflects how many bytes have been
  // returned, not if we're fetched all the buffers in the scan range
  bool eosr = false;
  if (io_buffer_ != NULL) {
    eosr = io_buffer_->eosr();
    ++parent_->num_completed_io_buffers_;
    completed_io_buffers_.push_back(io_buffer_);
    io_buffer_ = NULL;
  }

  if (!eosr) {
    SCOPED_TIMER(parent_->state_->total_storage_wait_timer());
    RETURN_IF_ERROR(scan_range_->GetNext(&io_buffer_));
  } else {
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
      // TODO: We are leaving io_buffer_ = NULL, revisit.
      return Status::OK();
    }
    DiskIoMgr::ScanRange* range = parent_->scan_node_->AllocateScanRange(
        scan_range_->fs(), filename(), read_past_buffer_size, offset, -1,
        scan_range_->disk_id(), false, DiskIoMgr::BufferOpts::Uncached());
    RETURN_IF_ERROR(parent_->state_->io_mgr()->Read(
        parent_->scan_node_->reader_context(), range, &io_buffer_));
  }

  DCHECK(io_buffer_ != NULL);
  parent_->scan_node_->num_owned_io_buffers_.Add(1);
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
  *out_buffer = NULL;
  *len = 0;
  if (eosr()) return Status::OK();

  if (UNLIKELY(parent_->cancelled())) {
    DCHECK(*out_buffer == NULL);
    return Status::CANCELLED;
  }

  if (boundary_buffer_bytes_left_ > 0) {
    DCHECK_EQ(output_buffer_pos_, &boundary_buffer_pos_);
    DCHECK_EQ(output_buffer_bytes_left_, &boundary_buffer_bytes_left_);
    *out_buffer = boundary_buffer_pos_;
    // Don't return more bytes past eosr
    *len = min(boundary_buffer_bytes_left_, bytes_left());
    DCHECK_GE(*len, 0);
    if (!peek) {
      boundary_buffer_pos_ += *len;
      boundary_buffer_bytes_left_ -= *len;
      total_bytes_returned_ += *len;
    }
    return Status::OK();
  }

  if (io_buffer_bytes_left_ == 0) {
    // We're at the end of the boundary buffer and the current IO buffer. Get a new IO
    // buffer and set the current buffer to it.
    RETURN_IF_ERROR(GetNextBuffer());
    output_buffer_pos_ = &io_buffer_pos_;
    output_buffer_bytes_left_ = &io_buffer_bytes_left_;
  }
  DCHECK(io_buffer_ != NULL);

  *out_buffer = io_buffer_pos_;
  *len = io_buffer_bytes_left_;
  if (!peek) {
    io_buffer_bytes_left_ = 0;
    io_buffer_pos_ += *len;
    total_bytes_returned_ += *len;
  }
  DCHECK_GE(bytes_left(), 0);
  return Status::OK();
}

Status ScannerContext::Stream::GetBytesInternal(int64_t requested_len,
    uint8_t** out_buffer, bool peek, int64_t* out_len) {
  DCHECK_GT(requested_len, boundary_buffer_bytes_left_);
  *out_buffer = NULL;

  if (boundary_buffer_bytes_left_ == 0) {
    if (contains_tuple_data_) {
      boundary_buffer_->Reset();
    } else {
      boundary_buffer_->Clear();
    }
  }

  while (requested_len > boundary_buffer_bytes_left_ + io_buffer_bytes_left_) {
    // We must copy the remainder of 'io_buffer_' to 'boundary_buffer_' before advancing
    // to handle the case when the read straddles a block boundary. Preallocate
    // 'boundary_buffer_' to avoid unnecessary resizes for large reads.
    if (io_buffer_bytes_left_ > 0) {
      RETURN_IF_ERROR(boundary_buffer_->GrowBuffer(requested_len));
      RETURN_IF_ERROR(boundary_buffer_->Append(io_buffer_pos_, io_buffer_bytes_left_));
      boundary_buffer_bytes_left_ += io_buffer_bytes_left_;
    }

    int64_t remaining_requested_len = requested_len - boundary_buffer_->len();
    RETURN_IF_ERROR(GetNextBuffer(remaining_requested_len));
    if (UNLIKELY(parent_->cancelled())) return Status::CANCELLED;
    // No more bytes (i.e. EOF).
    if (io_buffer_bytes_left_ == 0) break;
  }

  // We have read the full 'requested_len' bytes or couldn't read more bytes.
  int64_t requested_bytes_left = requested_len - boundary_buffer_bytes_left_;
  DCHECK_GE(requested_bytes_left, 0);
  int64_t num_bytes = min(io_buffer_bytes_left_, requested_bytes_left);
  *out_len = boundary_buffer_bytes_left_ + num_bytes;
  DCHECK_LE(*out_len, requested_len);

  if (boundary_buffer_bytes_left_ == 0) {
    // No stitching, just return the memory
    output_buffer_pos_ = &io_buffer_pos_;
    output_buffer_bytes_left_ = &io_buffer_bytes_left_;
  } else {
    RETURN_IF_ERROR(boundary_buffer_->Append(io_buffer_pos_, num_bytes));
    boundary_buffer_bytes_left_ += num_bytes;
    boundary_buffer_pos_ = reinterpret_cast<uint8_t*>(boundary_buffer_->buffer()) +
        boundary_buffer_->len() - boundary_buffer_bytes_left_;
    io_buffer_bytes_left_ -= num_bytes;
    io_buffer_pos_ += num_bytes;

    output_buffer_pos_ = &boundary_buffer_pos_;
    output_buffer_bytes_left_ = &boundary_buffer_bytes_left_;
  }
  *out_buffer = *output_buffer_pos_;

  if (!peek) {
    total_bytes_returned_ += *out_len;
    if (boundary_buffer_bytes_left_ == 0) {
      io_buffer_bytes_left_ -= num_bytes;
      io_buffer_pos_ += num_bytes;
    } else {
      DCHECK_EQ(boundary_buffer_bytes_left_, *out_len);
      boundary_buffer_bytes_left_ = 0;
    }
  }

  return Status::OK();
}

bool ScannerContext::cancelled() const {
  if (!scan_node_->HasRowBatchQueue()) return false;
  return static_cast<HdfsScanNode*>(scan_node_)->done();
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
