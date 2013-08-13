// Copyright 2013 Cloudera Inc.
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


#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>
#include <fcntl.h>
#include <vector>

#include "buffer-pool.h"
#include "disk-structs.h"
#include "disk-writer.h"

#include "util/blocking-queue.h"

using namespace std;
using namespace boost;

namespace impala {

static const char* temp_filename_ = "/tmp/impala-temp";

void DiskWriter::Init() {
  shutdown_ = false;
  my_file_ = fopen(temp_filename_, "w");
  DCHECK(my_file_ != NULL);
  workers_.add_thread(new boost::thread(
      boost::bind(&DiskWriter::WriterThread, this, 0 /* disk id */)));
}

void DiskWriter::Cancel() {
  boost::lock_guard<boost::mutex> guard(cancel_lock_);
  if (shutdown_) return;
  queued_writes_.Shutdown();
  workers_.join_all();
  fclose(my_file_);
  shutdown_ = true;
}

void DiskWriter::WriterThread(int disk_id) {
  while (true) {
    EnqueuedBlock enqueuedBlock;
    bool ok = queued_writes_.BlockingGet(&enqueuedBlock);
    Block* block = enqueuedBlock.block;
    if (!ok) return;

    int64_t offset = ftell(my_file_);
    FilePosition& file_pos = block->file_pos;
    file_pos.file = temp_filename_;
    file_pos.offset = offset;
    file_pos.disk_id = disk_id;

    block->Pin();
    if (block->in_mem() && !block->should_not_persist()) {
      DCHECK_GT(block->len(), 0);
      int bytes_written = fwrite(block->buf_desc()->buffer, 1, block->len(), my_file_);
      block->SetPersisted(true);
      block->Unpin();
      DCHECK_EQ(block->len(), bytes_written);
      fflush(my_file_);
//    fdatasync(fileno(my_file_));
//    posix_fadvise(fileno(my_file_), offset, block->len, POSIX_FADV_DONTNEED);

      DCHECK(enqueuedBlock.buffer_manager != NULL);
    } else {
      block->Unpin();
    }

    enqueuedBlock.buffer_manager->BlockWritten(block);
  }
}

DiskWriter::BufferManager::BufferManager(BufferPool* buffer_pool, DiskWriter* writer)
    : buffer_pool_(buffer_pool), writer_(writer), stop_disk_flush_(false),
      num_blocks_managed_(0), num_blocks_disk_queue_(0), num_freed_buffers_requested_(0) {
  function<bool ()> get_buffer_callback(
      bind(&BufferManager::FreeBufferRequested, this));
  buffer_pool->SetTryFreeBufferCallback(get_buffer_callback);
}

DiskWriter::BufferManager::~BufferManager() {
}

void DiskWriter::BufferManager::EnqueueBlock(Block* block, int priority) {
  {
   lock_guard<mutex> lock(disk_lock_);
   ++num_blocks_managed_;
   priority_blocks_heap_.push_back(PriorityBlock(block, priority));
   push_heap(priority_blocks_heap_.begin(), priority_blocks_heap_.end());

   if (num_blocks_disk_queue_ == 0) FlushNextBlock();
  }
}

// Hold disk_lock_ while calling
void DiskWriter::BufferManager::FlushNextBlock() {
  if (priority_blocks_heap_.empty()) return;
  if (stop_disk_flush_) return;
  pop_heap(priority_blocks_heap_.begin(), priority_blocks_heap_.end());
  Block* block = priority_blocks_heap_.back().block;
  priority_blocks_heap_.pop_back();
  --num_blocks_managed_;

  ++num_blocks_disk_queue_;
  writer_->EnqueueBlock(block, this);
}

// Hold persisted_blocks_lock_ before calling.
// Returns true if we successfully returned a buffer.
bool DiskWriter::BufferManager::ReturnPersistedBuffer() {
  if (num_freed_buffers_requested_ == 0) return false;

  vector<Block*>::iterator it = persisted_blocks_.begin();
  for (; it != persisted_blocks_.end(); ++it) {
    Block* block = *it;
    if (block->ReleaseBufferIfUnpinned()) break;
  }

  if (it == persisted_blocks_.end()) return false;
  persisted_blocks_.erase(it);
  --num_freed_buffers_requested_;
  return true;
}

bool DiskWriter::BufferManager::FreeBufferRequested() {
  lock_guard<mutex> lock(persisted_blocks_lock_);
  ++num_freed_buffers_requested_;
  return ReturnPersistedBuffer();
}

void DiskWriter::BufferManager::BlockWritten(Block* block) {
  {
    lock_guard<mutex> lock(disk_lock_);
    DCHECK_GT(num_blocks_disk_queue_, 0);
    --num_blocks_disk_queue_;
    if (num_blocks_disk_queue_ == 0) FlushNextBlock();
  }

  if (!block->persisted()) return;

  {
    lock_guard<mutex> lock(persisted_blocks_lock_);
    persisted_blocks_.push_back(block);
  }

  if (num_freed_buffers_requested_ > 0) {
    // Grab the BufferPool lock first to avoid deadlock.
    lock_guard<recursive_mutex> buffer_pool_lock(buffer_pool_->lock());
    lock_guard<mutex> persisted_blocks_lock(persisted_blocks_lock_);
    ReturnPersistedBuffer();
  }
}

}
