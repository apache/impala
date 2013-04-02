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
    HdfsPartitionDescriptor* partition_desc, DiskIoMgr::BufferDescriptor* initial_buffer)
  : state_(state),
    scan_node_(scan_node),
    tuple_byte_size_(scan_node_->tuple_desc()->byte_size()),
    partition_desc_(partition_desc),
    done_(false),
    is_columnar_(false),
    cancelled_(false) {
  template_tuple_ = 
      scan_node_->InitTemplateTuple(state, partition_desc_->partition_key_values());

  streams_.push_back(state->obj_pool()->Add(new Stream(this)));
  streams_[0]->AddBuffer(initial_buffer);
  streams_[0]->compact_data_ = scan_node_->compact_data();

  NewRowBatch();
}

ScannerContext::~ScannerContext() {
  DCHECK(current_row_batch_ == NULL);
}

void ScannerContext::NewRowBatch() {
  current_row_batch_ = new RowBatch(scan_node_->row_desc(), state_->batch_size());
  current_row_batch_->tuple_data_pool()->set_limits(*state_->mem_limits());
  tuple_mem_ = current_row_batch_->tuple_data_pool()->Allocate(
      state_->batch_size() * tuple_byte_size_);
}

void ScannerContext::CreateStreams(int num_streams) {
  DCHECK_GT(num_streams, 0);
  unique_lock<mutex> l(lock_);
  DCHECK_EQ(streams_.size(), 1);

  // Return all resources for the current streams
  for (int i = 0; i < streams_.size(); ++i) {
    streams_[i]->ReturnAllBuffers();
  }

  // Create the new streams
  streams_.clear();
  for (int i = 0; i < num_streams; ++i) {
    streams_.push_back(state_->obj_pool()->Add(new Stream(this)));
  }
  is_columnar_ = true;
}

void ScannerContext::AttachCompletedResources(bool done) {
  DCHECK(current_row_batch_ != NULL);
  for (int i = 0; i < streams_.size(); ++i) {
    streams_[i]->AttachCompletedResources(done);
  }
  // If there are any rows or any io buffers, pass this batch to the scan node.
  if (current_row_batch_->num_io_buffers() > 0 || current_row_batch_->num_rows() > 0 
      || done) {
    scan_node_->AddMaterializedRowBatch(current_row_batch_);
    current_row_batch_ = NULL;
    if (!done) NewRowBatch();
  }
}

ScannerContext::Stream::Stream(ScannerContext* parent) 
  : parent_(parent), total_len_(0) { 
}

void ScannerContext::Stream::SetInitialBuffer(DiskIoMgr::BufferDescriptor* buffer) {
  scan_range_ = buffer->scan_range();
  scan_range_start_ = scan_range_->offset();
  total_bytes_returned_ = 0;
  current_buffer_pos_ = NULL;
  read_past_buffer_size_ = DEFAULT_READ_PAST_SIZE;
  total_len_ = scan_range_->len();
  read_eosr_ = false;
  boundary_pool_.reset(new MemPool());
  boundary_pool_->set_limits(*parent_->state_->mem_limits());
  boundary_buffer_.reset(new StringBuffer(boundary_pool_.get()));
  current_buffer_ = NULL;
}

void ScannerContext::Stream::ReturnAllBuffers() {
  completed_buffers_.insert(completed_buffers_.end(), buffers_.begin(), buffers_.end());
  buffers_.clear();
  for (list<DiskIoMgr::BufferDescriptor*>::iterator it = completed_buffers_.begin();
      it != completed_buffers_.end(); ++it) {
    (*it)->Return();
    __sync_fetch_and_add(&parent_->scan_node_->num_owned_io_buffers_, -1);
  }
}

void ScannerContext::Stream::AttachCompletedResources(bool done) {
  DCHECK(parent_->current_row_batch_ != NULL);
  for (list<DiskIoMgr::BufferDescriptor*>::iterator it = completed_buffers_.begin();
      it != completed_buffers_.end(); ++it) {
    if (compact_data_) {
      (*it)->Return();
      __sync_fetch_and_add(&parent_->scan_node_->num_owned_io_buffers_, -1);
    } else {
      parent_->current_row_batch_->AddIoBuffer(*it);
    } 
  }
  completed_buffers_.clear();
  
  // If this scan range is done, attach the boundary mem pool to the current row batch.
  if (done) {
    parent_->current_row_batch_->tuple_data_pool()->AcquireData(
        boundary_pool_.get(), false);
  }
}

void ScannerContext::Stream::RemoveFirstBuffer() {
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

Status ScannerContext::Stream::GetRawBytes(uint8_t** out_buffer, int* len, bool* eos) {
  *out_buffer = NULL;
  *len = 0;

  // Wait for first buffer
  {
    unique_lock<mutex> l(parent_->lock_);
    while (!parent_->cancelled_ && buffers_.empty()) {
      read_ready_cv_.wait(l);
    }

    if (parent_->cancelled_) {
      DCHECK(*out_buffer == NULL);
      return Status::CANCELLED;
    }
    
    DCHECK(current_buffer_ != NULL);
    DCHECK(!buffers_.empty());
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

  // Any previously allocated boundary buffers must have been processed by the
  // scanner. Attach the boundary pool to the current row batch.
  if (compact_data()) {
    boundary_buffer_->Clear();
  } else {
    parent_->current_row_batch_->tuple_data_pool()->AcquireData(
        boundary_pool_.get(), false);
    boundary_buffer_->Reset();
  }

  {
    // Any previously allocated boundary buffers must have been processed by the
    // scanner. Attach the boundary pool and io buffers to the current row batch.
    unique_lock<mutex> l(parent_->lock_);
    if (current_buffer_bytes_left_ == 0 && current_buffer_ != NULL) {
      RemoveFirstBuffer();
    }
    parent_->AttachCompletedResources(false);
    
    // The caller requested a complete buffer but there are no more bytes
    if (requested_len == 0 && eosr()) return Status::OK;
  }

  // Loop and wait for the next buffer
  while (true) {
    unique_lock<mutex> l(parent_->lock_);
   
    while (!parent_->cancelled_ && buffers_.empty() && !eosr()) {
      read_ready_cv_.wait(l);
    }

    if (parent_->cancelled_) return Status::CANCELLED;

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
        parent_->AttachCompletedResources(false);
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
      Status status = parent_->state_->io_mgr()->Read(
          parent_->scan_node_->hdfs_connection(), &range, &buffer_desc);
      if (!status.ok()) {
        if (buffer_desc != NULL) buffer_desc->Return();
        return status;
      }

      __sync_fetch_and_add(&parent_->scan_node_->num_owned_io_buffers_, 1);
      
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

int ScannerContext::GetMemory(MemPool** pool, Tuple** tuple_mem, 
    TupleRow** tuple_row_mem) {
  DCHECK(!current_row_batch_->IsFull());
  *pool = current_row_batch_->tuple_data_pool();
  *tuple_mem = reinterpret_cast<Tuple*>(tuple_mem_);
  *tuple_row_mem = current_row_batch_->GetRow(current_row_batch_->AddRow());
  return current_row_batch_->capacity() - current_row_batch_->num_rows();
}

void ScannerContext::CommitRows(int num_rows) {
  DCHECK_LE(num_rows, current_row_batch_->capacity() - current_row_batch_->num_rows());
  current_row_batch_->CommitRows(num_rows);
  tuple_mem_ += scan_node_->tuple_desc()->byte_size() * num_rows;

  if (current_row_batch_->IsFull()) {
    scan_node_->AddMaterializedRowBatch(current_row_batch_);
    current_row_batch_ = NULL;
    NewRowBatch();
  }
}

void ScannerContext::Stream::AddBuffer(DiskIoMgr::BufferDescriptor* buffer) {
  {
    unique_lock<mutex> l(parent_->lock_);
    if (parent_->done_) {
      // The context is done (e.g. limit reached) so this buffer can be just
      // returned.
      buffer->Return();
      __sync_fetch_and_add(&parent_->scan_node_->num_owned_io_buffers_, -1);
      return;
    }
        
    if (total_len_ == 0) {
      // First buffer for this stream, initialize the state
      SetInitialBuffer(buffer);
      DCHECK_GT(total_len_, 0);
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

void ScannerContext::Close() {
  {
    unique_lock<mutex> l(lock_);
    
    for (int i = 0; i < streams_.size(); ++i) {
      if (streams_[i]->current_buffer_ != NULL) streams_[i]->RemoveFirstBuffer();

      streams_[i]->completed_buffers_.insert(streams_[i]->completed_buffers_.end(), 
          streams_[i]->buffers_.begin(), streams_[i]->buffers_.end());
      streams_[i]->buffers_.clear();
    }
    AttachCompletedResources(true);
    DCHECK(current_row_batch_ == NULL);

    // Set variables to NULL to make sure this object is not being used after Complete()
    done_ = true;
    for (int i = 0; i < streams_.size(); ++i) {
      streams_[i]->read_eosr_ = false;
      streams_[i]->current_buffer_ = NULL;
      streams_[i]->current_buffer_pos_ = NULL;
    }
  }
  
  for (int i = 0; i < streams_.size(); ++i) {
    DCHECK(streams_[i]->completed_buffers_.empty());
    DCHECK(streams_[i]->buffers_.empty());
  }
}

void ScannerContext::Cancel() {
  {
    unique_lock<mutex> l(lock_);
    cancelled_ = true;
  }
  // Wake up any reading threads.
  for (int i = 0; i < streams_.size(); ++i) {
    streams_[i]->read_ready_cv_.notify_one();
  }
}
