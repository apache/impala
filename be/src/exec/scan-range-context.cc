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

#include "exec/scan-range-context.h"

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

ScanRangeContext::ScanRangeContext(RuntimeState* state, HdfsScanNode* scan_node, 
    HdfsPartitionDescriptor* partition_desc, DiskIoMgr::BufferDescriptor* initial_buffer)
  : state_(state),
    scan_node_(scan_node),
    tuple_byte_size_(scan_node_->tuple_desc()->byte_size()),
    partition_desc_(partition_desc),
    current_buffer_pos_(NULL),
    total_bytes_returned_(0),
    read_past_buffer_size_(DEFAULT_READ_PAST_SIZE),
    boundary_pool_(new MemPool()),
    boundary_buffer_(new StringBuffer(boundary_pool_.get())),
    cancelled_(false),
    done_(false),
    read_eosr_(false),
    current_buffer_(NULL) {

  compact_data_ = scan_node->compact_data() || 
      scan_node->tuple_desc()->string_slots().empty();

  scan_range_start_ = initial_buffer->scan_range()->offset();
  scan_range_ = initial_buffer->scan_range();

  AddBuffer(initial_buffer);

  NewRowBatch();
  
  template_tuple_ = 
      scan_node_->InitTemplateTuple(state, partition_desc_->partition_key_values());
}

ScanRangeContext::~ScanRangeContext() {
  DCHECK(current_row_batch_ == NULL);
}

void ScanRangeContext::NewRowBatch() {
  current_row_batch_ = new RowBatch(scan_node_->row_desc(), state_->batch_size());
  tuple_mem_ = current_row_batch_->tuple_data_pool()->Allocate(
      state_->batch_size() * tuple_byte_size_);
}

void ScanRangeContext::AttachCompletedResources(bool done) {
  DCHECK(current_row_batch_ != NULL);
  for (list<DiskIoMgr::BufferDescriptor*>::iterator it = completed_buffers_.begin();
      it != completed_buffers_.end(); ++it) {
    if (compact_data_) {
      (*it)->Return();
      __sync_fetch_and_add(&scan_node_->num_owned_io_buffers_, -1);
    } else {
      current_row_batch_->AddIoBuffer(*it);
    } 
  }
  completed_buffers_.clear();
  
  // If this scan range is done, attach the boundary mem pool to the current row batch.
  if (done) {
    current_row_batch_->tuple_data_pool()->AcquireData(boundary_pool_.get(), false);
  }

  // If there are any rows, any io buffers or the context is done, pass 
  // the current batch to the scan node.
  if (current_row_batch_->num_io_buffers() > 0 || 
      current_row_batch_->num_rows() > 0 ||
      done) {
    scan_node_->AddMaterializedRowBatch(current_row_batch_);
    current_row_batch_ = NULL;
    if (!done) NewRowBatch();
  }
}

void ScanRangeContext::RemoveFirstBuffer() {
  DCHECK(current_buffer_ != NULL);
  DCHECK(!buffers_.empty());
  buffers_.pop_front();

  completed_buffers_.push_back(current_buffer_);
  
  if (!buffers_.empty()) {
    current_buffer_ = buffers_.front();
    current_buffer_pos_ = reinterpret_cast<uint8_t*>(current_buffer_->buffer());
    current_buffer_bytes_left_ = current_buffer_->len();
  } else {
    current_buffer_ = NULL;
    current_buffer_pos_ = NULL;
    current_buffer_bytes_left_ = 0;
  }
}

Status ScanRangeContext::GetRawBytes(uint8_t** out_buffer, int* len, bool* eos) {
  *out_buffer = NULL;
  *len = 0;

  // Wait for first buffer
  {
    unique_lock<mutex> l(lock_);
    while (!cancelled_ && buffers_.empty()) {
      read_ready_cv_.wait(l);
    }

    if (cancelled_) {
      DCHECK(*out_buffer == NULL);
      return Status::CANCELLED;
    }
    
    DCHECK(current_buffer_ != NULL);
    DCHECK(!buffers_.empty());
  }

  // If there is no current data, fetch the first available buffer.
  if (current_buffer_bytes_left_ == 0) {
    return GetBytesInternal(out_buffer, 0, true, len, eos);
  } 

  *out_buffer = current_buffer_pos_;
  *len = current_buffer_bytes_left_;
  *eos = current_buffer_->eosr();
  return Status::OK;
}

Status ScanRangeContext::GetBytesInternal(uint8_t** out_buffer, int requested_len, 
    bool peek, int* out_len, bool* eos) {
  *out_len = 0;
  *out_buffer = NULL;
  *eos = true;

  boundary_buffer_->Clear();

  // Any previously allocated boundary buffers must have been processed by the
  // scanner. Attach the boundary pool and io buffers to the current row batch.
  {
    unique_lock<mutex> l(lock_);
    if (current_buffer_bytes_left_ == 0 && current_buffer_ != NULL) {
      RemoveFirstBuffer();
    }
    AttachCompletedResources(false);
    
    // The caller requested a complete buffer but there are no more bytes
    if (requested_len == 0 && eosr()) return Status::OK;
  }

  // Loop and wait for the next buffer
  while (true) {
    unique_lock<mutex> l(lock_);
   
    while (!cancelled_ && buffers_.empty() && !eosr()) {
      read_ready_cv_.wait(l);
    }

    if (cancelled_) return Status::CANCELLED;

    if (requested_len == 0) {
      DCHECK(current_buffer_ != NULL);
      DCHECK(*out_len == 0);
      requested_len = current_buffer_bytes_left_;
    }

    // Not enough bytes, copy the end of this buffer and combine it wit the next one
    if (requested_len > current_buffer_bytes_left_) {
      if (current_buffer_ != NULL) {
        read_eosr_ = current_buffer_->eosr();
        boundary_buffer_->Append(current_buffer_pos_, current_buffer_bytes_left_);
        *out_len += current_buffer_bytes_left_;
        requested_len -= current_buffer_bytes_left_;
        total_bytes_returned_ += current_buffer_bytes_left_;
        RemoveFirstBuffer();
        AttachCompletedResources(false);
      }

      if (!eosr()) continue;

      // We are at the end of the scan range and there are still not enough bytes
      // to satisfy the request.  Issue a sync read to the io mgr and keep going
      DCHECK(current_buffer_ == NULL);
      DCHECK_EQ(current_buffer_bytes_left_, 0);

      DiskIoMgr::ScanRange range;
      // TODO: this should pick the remote read "disk id" when the io mgr supports that
      range.Reset(filename(), read_past_buffer_size_, 
          file_offset(), scan_range_->disk_id(), NULL);

      DiskIoMgr::BufferDescriptor* buffer_desc;
      Status status = state_->io_mgr()->Read(scan_node_->hdfs_connection(),
          &range, &buffer_desc);
      if (!status.ok()) {
        if (buffer_desc != NULL) buffer_desc->Return();
        return status;
      }

      __sync_fetch_and_add(&scan_node_->num_owned_io_buffers_, 1);
      
      DCHECK(!peek);
      current_buffer_ = buffer_desc;
      current_buffer_bytes_left_ = current_buffer_->len();
      current_buffer_pos_ = reinterpret_cast<uint8_t*>(current_buffer_->buffer());
      buffers_.push_back(current_buffer_);

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
        // No stiching, just return the memory
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

int ScanRangeContext::GetMemory(MemPool** pool, Tuple** tuple_mem, 
    TupleRow** tuple_row_mem) {
  DCHECK(!current_row_batch_->IsFull());
  *pool = current_row_batch_->tuple_data_pool();
  *tuple_mem = reinterpret_cast<Tuple*>(tuple_mem_);
  *tuple_row_mem = current_row_batch_->GetRow(current_row_batch_->AddRow());
  return current_row_batch_->capacity() - current_row_batch_->num_rows();
}

void ScanRangeContext::CommitRows(int num_rows) {
  DCHECK_LE(num_rows, current_row_batch_->capacity() - current_row_batch_->num_rows());
  current_row_batch_->CommitRows(num_rows);
  tuple_mem_ += scan_node_->tuple_desc()->byte_size() * num_rows;

  if (current_row_batch_->IsFull()) {
    scan_node_->AddMaterializedRowBatch(current_row_batch_);
    current_row_batch_ = NULL;
    NewRowBatch();
  }
}

void ScanRangeContext::AddBuffer(DiskIoMgr::BufferDescriptor* buffer) {
  {
    unique_lock<mutex> l(lock_);
    if (done_) {
      // The context is done (e.g. limit reached) so this buffer can be just
      // returned.
      buffer->Return();
      __sync_fetch_and_add(&scan_node_->num_owned_io_buffers_, -1);
      return;
    }
    buffers_.push_back(buffer);

    // These variables are read without a lock in GetBytes.  There is a race in 
    // reading/writing these variables when buffers_ is empty and this function
    // adds the first buffer.  This is the only case where the read does not take
    // locks.
    // current_buffer_bytes_left_ serves as a flag to indicate that the current
    // buffer has been queued.  We need to make sure its value is written *after*
    // the other members.  To do this, we will put a full memory barrier before
    // updating current_buffer_bytes_left_.
    if (current_buffer_ == NULL) {
      current_buffer_ = buffer;
      current_buffer_pos_ = reinterpret_cast<uint8_t*>(current_buffer_->buffer());
      __sync_synchronize();
      current_buffer_bytes_left_ = buffer->len();
      __sync_synchronize();
    }
  }
  read_ready_cv_.notify_one();
}

void ScanRangeContext::Flush() {
  {
    unique_lock<mutex> l(lock_);
    
    if (current_buffer_ != NULL) RemoveFirstBuffer();

    completed_buffers_.insert(
        completed_buffers_.end(), buffers_.begin(), buffers_.end());
    buffers_.clear();
    AttachCompletedResources(true);
    DCHECK(current_row_batch_ == NULL);

    // Set variables to NULL to make sure this object is not being used after Complete()
    current_buffer_ = NULL;
    current_buffer_pos_ = NULL;
    read_eosr_ = false;
    done_ = true;
  }
  
  DCHECK(completed_buffers_.empty());
  DCHECK(buffers_.empty());
}

void ScanRangeContext::Cancel() {
  {
    unique_lock<mutex> l(lock_);
    cancelled_ = true;
  }
  // Wake up any reading threads.
  read_ready_cv_.notify_one();
}
