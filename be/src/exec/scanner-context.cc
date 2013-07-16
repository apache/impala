// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/scanner-context.h"

#include "exec/hdfs-scan-node.h"
#include "runtime/row-batch.h"
#include "runtime/mem-pool.h"
#include "runtime/runtime-state.h"
#include "runtime/string-buffer.h"
#include "util/debug-util.h"

using namespace boost;
using namespace impala;
using namespace std;

static const int DEFAULT_READ_PAST_SIZE = 10 * 1024;  // In Bytes

ScannerContext::ScannerContext(RuntimeState* state, HdfsScanNode* scan_node, 
    HdfsPartitionDescriptor* partition_desc, DiskIoMgr::ScanRange* scan_range)
  : state_(state),
    scan_node_(scan_node),
    partition_desc_(partition_desc) {
  Stream* stream = AddStream(scan_range);
  stream->compact_data_ = scan_node_->compact_data();
}

void ScannerContext::CloseStreams() {
  // Return all resources for the current streams
  for (int i = 0; i < streams_.size(); ++i) {
    streams_[i]->ReturnAllBuffers();
  }
  streams_.clear();
}

void ScannerContext::AttachCompletedResources(RowBatch* batch, bool done) {
  DCHECK(batch != NULL);
  for (int i = 0; i < streams_.size(); ++i) {
    streams_[i]->AttachCompletedResources(batch, done);
  }
}

ScannerContext::Stream::Stream(ScannerContext* parent) 
  : parent_(parent), total_len_(0), 
    boundary_pool_(new MemPool(parent->state_->mem_limits())),
    boundary_buffer_(new StringBuffer(boundary_pool_.get())) {
}

ScannerContext::Stream* ScannerContext::AddStream(DiskIoMgr::ScanRange* range) {
  Stream* stream = state_->obj_pool()->Add(new Stream(this));
  stream->scan_range_ = range;
  stream->file_desc_ = scan_node_->GetFileDesc(stream->filename());
  stream->scan_range_start_ = range->offset();
  stream->total_bytes_returned_ = 0;
  stream->current_buffer_pos_ = NULL;
  stream->read_past_buffer_size_ = DEFAULT_READ_PAST_SIZE;
  stream->total_len_ = range->len();
  stream->read_eosr_ = false;
  stream->current_buffer_ = NULL;
  stream->current_buffer_bytes_left_ = 0;
  streams_.push_back(stream);
  return stream;
}

void ScannerContext::Stream::ReturnAllBuffers() {
  if (current_buffer_ != NULL) completed_buffers_.push_back(current_buffer_);
  for (list<DiskIoMgr::BufferDescriptor*>::iterator it = completed_buffers_.begin();
      it != completed_buffers_.end(); ++it) {
    (*it)->Return();
    --parent_->scan_node_->num_owned_io_buffers_;
  }
  current_buffer_ = NULL;
  current_buffer_pos_ = NULL;
  current_buffer_bytes_left_ = 0;

  // Cancel the underlying scan range to clean up any queued buffers there
  if (scan_range_ != NULL) scan_range_->Cancel();
}

void ScannerContext::Stream::AttachCompletedResources(RowBatch* batch, bool done) {
  DCHECK(batch != NULL);
  if (done) {
    // Mark any pending resources as completed
    if (current_buffer_ != NULL) completed_buffers_.push_back(current_buffer_);
    current_buffer_ = NULL;
    // Cancel the underlying scan range to clean up any queued buffers there
    scan_range_->Cancel();
  }

  for (list<DiskIoMgr::BufferDescriptor*>::iterator it = completed_buffers_.begin();
       it != completed_buffers_.end(); ++it) {
    if (compact_data_) {
      (*it)->Return();
      --parent_->scan_node_->num_owned_io_buffers_;
    } else {
      batch->AddIoBuffer(*it);
      // TODO: We can do row batch compaction here.  This is the only place io buffers are
      // queued.  A good heuristic is to check the number of io buffers queued and if
      // there are too many, we should compact.
    }
  }
  completed_buffers_.clear();
  
  if (!compact_data_) {
    // If we're not done, keep using the last chunk allocated in boundary_pool_ so we
    // don't have to reallocate. If we are done, transfer it to the row batch.
    batch->tuple_data_pool()->AcquireData(boundary_pool_.get(), /* keep_current */ !done);
  }
}

Status ScannerContext::Stream::GetNextBuffer() {
  if (current_buffer_ != NULL) {
    read_eosr_ = current_buffer_->eosr();
    completed_buffers_.push_back(current_buffer_);
    current_buffer_ = NULL;
  }

  if (!read_eosr_) RETURN_IF_ERROR(scan_range_->GetNext(&current_buffer_));
  
  if (current_buffer_ == NULL) {
    // NULL indicates eosr
    current_buffer_pos_ = NULL;
    current_buffer_bytes_left_ = 0;
  } else {
    ++parent_->scan_node_->num_owned_io_buffers_;
    current_buffer_pos_ = reinterpret_cast<uint8_t*>(current_buffer_->buffer());
    current_buffer_bytes_left_ = current_buffer_->len();
  } 
  return Status::OK;
}

Status ScannerContext::Stream::GetRawBytes(uint8_t** out_buffer, int* len, bool* eos) {
  *out_buffer = NULL;
  *len = 0;

  if (parent_->cancelled()) {
    DCHECK(*out_buffer == NULL);
    return Status::CANCELLED;
  }

  // If there is no current data, fetch the first available buffer.
  if (current_buffer_bytes_left_ == 0) {
    return GetBytesInternal(0, out_buffer, true, len, eos);
  } 

  *out_buffer = current_buffer_pos_;
  *len = current_buffer_bytes_left_;
  *eos = current_buffer_->eosr();
  return Status::OK;
}

Status ScannerContext::Stream::GetBytesInternal(int requested_len,
    uint8_t** out_buffer, bool peek, int* out_len, bool* eos) {
  *out_len = 0;
  *out_buffer = NULL;
  *eos = true;

  if (current_buffer_bytes_left_ == 0) RETURN_IF_ERROR(GetNextBuffer());

  // The previous boundary buffer must have been processed by the scanner.
  if (compact_data()) {
    boundary_buffer_->Clear();
  } else {
    boundary_buffer_->Reset();
  }

  // The caller requested a complete buffer but there are no more bytes
  if (requested_len == 0 && eosr()) return Status::OK;

  // Loop and wait for the next buffer
  while (true) {
    if (parent_->cancelled()) return Status::CANCELLED;
    if (current_buffer_bytes_left_ == 0) RETURN_IF_ERROR(GetNextBuffer());

    if (requested_len == 0) {
      DCHECK(current_buffer_ != NULL);
      DCHECK(*out_len == 0);
      requested_len = current_buffer_bytes_left_;
    }

    // Not enough bytes, copy the end of this buffer and combine it with the next one
    if (requested_len > current_buffer_bytes_left_) {
      if (current_buffer_ != NULL) {
        boundary_buffer_->Append(current_buffer_pos_, current_buffer_bytes_left_);
        *out_len += current_buffer_bytes_left_;
        requested_len -= current_buffer_bytes_left_;
        total_bytes_returned_ += current_buffer_bytes_left_;
        RETURN_IF_ERROR(GetNextBuffer());
      }

      if (!eosr()) continue;

      // We are at the end of the scan range and there are still not enough bytes
      // to satisfy the request.  Issue a sync read to the io mgr and keep going
      DCHECK(current_buffer_ == NULL);
      DCHECK_EQ(current_buffer_bytes_left_, 0);

      // TODO: we're reading past this scan range so this is likely a remote read.
      // Update when the IoMgr has better support for remote reads.
      DiskIoMgr::ScanRange* range = parent_->scan_node_->AllocateScanRange(
          filename(), read_past_buffer_size_, file_offset(), -1, scan_range_->disk_id());

      DiskIoMgr::BufferDescriptor* buffer_desc;
      Status status = parent_->state_->io_mgr()->Read(
          parent_->scan_node_->reader_context(), range, &buffer_desc);
      if (!status.ok()) {
        if (buffer_desc != NULL) buffer_desc->Return();
        return status;
      }

      ++parent_->scan_node_->num_owned_io_buffers_;
      
      DCHECK(!peek);
      current_buffer_ = buffer_desc;
      current_buffer_bytes_left_ = current_buffer_->len();
      current_buffer_pos_ = reinterpret_cast<uint8_t*>(current_buffer_->buffer());

      if (current_buffer_bytes_left_ == 0) {
        // Tried to read past but there were no more bytes (i.e. EOF)
        *out_buffer = reinterpret_cast<uint8_t*>(boundary_buffer_->str().ptr);
        *eos = true;
        return Status::OK;
      }
      continue;
    }

    // We have enough bytes
    int num_bytes = min(current_buffer_bytes_left_, requested_len);
    *out_len += num_bytes;
    if (peek) {
      *out_buffer = current_buffer_pos_;
    } else {
      DCHECK(!peek);
      current_buffer_bytes_left_ -= num_bytes;
      total_bytes_returned_ += num_bytes;
      DCHECK_GE(current_buffer_bytes_left_, 0);

      if (boundary_buffer_->Empty()) {
        // No stitching, just return the memory
        *out_buffer = current_buffer_pos_;
      } else {
        boundary_buffer_->Append(current_buffer_pos_, num_bytes);
        *out_buffer = reinterpret_cast<uint8_t*>(boundary_buffer_->str().ptr);
      }
      current_buffer_pos_ += num_bytes;
    }

    *eos = (current_buffer_bytes_left_ == 0) && current_buffer_->eosr();
    return Status::OK;
  }
}

void ScannerContext::Close() {
  // Set variables to NULL to make sure this object is not being used after Close()
  for (int i = 0; i < streams_.size(); ++i) {
    streams_[i]->read_eosr_ = false;
    streams_[i]->current_buffer_ = NULL;
    streams_[i]->current_buffer_pos_ = NULL;
  }

  for (int i = 0; i < streams_.size(); ++i) {
    DCHECK(streams_[i]->completed_buffers_.empty());
  }
}

bool ScannerContext::cancelled() const { 
  return scan_node_->done_; 
}

bool ScannerContext::Stream::eof() {
  return file_offset() == file_desc_->file_length;
}

Status ScannerContext::Stream::ReportIncompleteRead(int length, int bytes_read) {
  stringstream ss;
  ss << "Tried to read " << length << " bytes but could only read "
     << bytes_read << " bytes. This may indicate data file corruption. "
     << "(file: " << filename() << ", byte offset: " << file_offset() << ")";
  return Status(ss.str());
}
