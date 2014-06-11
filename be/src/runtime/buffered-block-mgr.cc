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

#include "runtime/runtime-state.h"
#include "runtime/mem-tracker.h"
#include "runtime/buffered-block-mgr.h"
#include "runtime/tmp-file-mgr.h"
#include "util/runtime-profile.h"
#include "util/disk-info.h"
#include "util/filesystem-util.h"

using namespace std;
using namespace boost;

namespace impala {

// BufferedBlockMgr::Block methods.
BufferedBlockMgr::Block::Block(BufferedBlockMgr* block_mgr)
  : buffer_desc_(NULL),
    block_mgr_(block_mgr),
    write_range_(NULL),
    valid_data_len_(0),
    unpinned_blocks_it_(block_mgr->unpinned_blocks_.end()) {
}

Status BufferedBlockMgr::Block::Pin() {
  block_mgr_->pin_counter_->Update(1);
  RETURN_IF_ERROR(block_mgr_->PinBlock(this));
  DCHECK(Validate()) << endl << DebugString();
  return Status::OK;
}

Status BufferedBlockMgr::Block::Unpin() {
  block_mgr_->unpin_counter_->Update(1);
  RETURN_IF_ERROR(block_mgr_->UnpinBlock(this));
  DCHECK(Validate()) << endl << DebugString();
  return Status::OK;
}

Status BufferedBlockMgr::Block::Delete() {
  block_mgr_->delete_counter_->Update(1);
  RETURN_IF_ERROR(block_mgr_->DeleteBlock(this));
  DCHECK(Validate()) << endl << DebugString();
  return Status::OK;
}

void BufferedBlockMgr::Block::Init() {
  // No locks are taken because the block is new or has previously been deleted.
  is_pinned_ = false;
  in_write_ = false;
  is_deleted_ = false;
  valid_data_len_ = 0;
}

bool BufferedBlockMgr::Block::Validate() const {
  if (is_deleted_ && (is_pinned_ || (!in_write_ && buffer_desc_ != NULL))) {
    LOG (WARNING) << "Deleted block in use - " << DebugString();
    return false;
  }

  if (buffer_desc_ == NULL && (is_pinned_ || in_write_)) {
    LOG (WARNING) << "Block without buffer in use - " << DebugString();
    return false;
  }

  if (buffer_desc_ == NULL &&
      (unpinned_blocks_it_ != block_mgr_->unpinned_blocks_.end())) {
    LOG (WARNING) << "Unpersisted block without buffer - " << DebugString();
    return false;
  }

  if (buffer_desc_ != NULL && (buffer_desc_->block != this)) {
    LOG (WARNING) << "Block buffer inconsistency - " << DebugString();
    return false;
  }

  return true;
}

string BufferedBlockMgr::Block::DebugString() const {
  stringstream ss;
  ss << "Block: " << this
     << " Buffer Desc: " << buffer_desc_
     << " Deleted: " << is_deleted_
     << " Pinned: " << is_pinned_
     << " Write Issued: " << in_write_;

  return ss.str();
}

// BufferedBlockMgr methods.
BufferedBlockMgr* BufferedBlockMgr::Create(RuntimeState* state, MemTracker* parent,
      RuntimeProfile* profile, int64_t mem_limit, int64_t block_size) {
  BufferedBlockMgr* block_mgr = new BufferedBlockMgr(state->io_mgr(), parent, mem_limit,
      block_size);

  // Initialize the tmp files and the initial file to use.
  int num_tmp_devices = TmpFileMgr::num_tmp_devices();
  block_mgr->tmp_files_.reserve(num_tmp_devices);
  for (int i = 0; i < num_tmp_devices; ++i) {
    TmpFileMgr::File* tmp_file;
    TmpFileMgr::GetFile(i, state->query_id(), state->fragment_instance_id(), &tmp_file);
    block_mgr->tmp_files_.push_back(tmp_file);
  }
  block_mgr->next_block_index_ = rand() % num_tmp_devices;
  block_mgr->InitCounters(profile);
  return block_mgr;
}

Status BufferedBlockMgr::GetFreeBlock(Block** block) {
  Block* new_block;
  if (unused_blocks_.empty()) {
    new_block = obj_pool_.Add(new Block(this));
    new_block->Init();
    created_block_counter_->Update(1);
  } else {
    new_block = unused_blocks_.Dequeue();
    recycled_blocks_counter_->Update(1);
  }

  DCHECK_NOTNULL(new_block);
  DCHECK(new_block->Validate()) << endl << new_block->DebugString();
  bool in_mem;
  RETURN_IF_ERROR(FindBufferForBlock(new_block, &in_mem));
  DCHECK(!in_mem);

  *block = new_block;
  return Status::OK;
}

void BufferedBlockMgr::Close() {
  if (closed_) return;
  closed_ = true;
  io_mgr_->UnregisterContext(io_request_context_);
  // Delete tmp files.
  BOOST_FOREACH(TmpFileMgr::File& file, tmp_files_) {
    file.Remove();
  }
  tmp_files_.clear();

  // Free memory resources.
  buffer_pool_->FreeAll();
  buffer_pool_.reset();
  DCHECK_EQ(mem_tracker_->consumption(), 0);
  mem_tracker_->UnregisterFromParent();
  mem_tracker_.reset();
}

BufferedBlockMgr::BufferedBlockMgr(DiskIoMgr* io_mgr, MemTracker* parent,
    int64_t mem_limit, int64_t block_size)
  : block_size_(block_size),
    block_write_threshold_(TmpFileMgr::num_tmp_devices()),
    num_outstanding_writes_(0),
    io_mgr_(io_mgr),
    closed_(false) {
  // Create a new mem_tracker and allocate buffers.
  mem_tracker_.reset(new MemTracker(mem_limit, "Block Manager", parent));
  buffer_pool_.reset(new MemPool(mem_tracker_.get(), block_size));
  // Use SpareCapacity() to determine the actual memory limit in case the parent has
  // a limit lower than mem_limit.
  int64_t memory_available = mem_tracker_->SpareCapacity();
  int64_t total_buffers = memory_available / block_size_;
  for (int i = 0; i < total_buffers; ++i) {
    uint8_t* buffer = buffer_pool_->Allocate(block_size_);
    BufferDescriptor* buffer_desc = obj_pool_.Add(new BufferDescriptor(buffer));
    free_buffers_.Enqueue(buffer_desc);
    all_buffers_.push_back(buffer_desc);
  }

  io_mgr->RegisterContext(NULL, &io_request_context_);
}

Status BufferedBlockMgr::PinBlock(Block* block) {
  bool in_mem;
  RETURN_IF_ERROR(FindBufferForBlock(block, &in_mem));

  if (!in_mem) {
    // Read the block from disk if it was not in memory.
    DCHECK_NOTNULL(block->write_range_);
    SCOPED_TIMER(disk_read_timer_);
    // Create a ScanRange to perform the read.
    DiskIoMgr::ScanRange* scan_range = obj_pool_.Add(new DiskIoMgr::ScanRange());
    scan_range->Reset(block->write_range_->file(), block->write_range_->len(),
        block->write_range_->offset(), block->write_range_->disk_id(), false, block);
    vector<DiskIoMgr::ScanRange*> ranges(1, scan_range);
    RETURN_IF_ERROR(io_mgr_->AddScanRanges(io_request_context_, ranges, true));

    // Read from the io mgr buffer into the block's assigned buffer.
    int64_t offset = 0;
    DiskIoMgr::BufferDescriptor* io_mgr_buffer;
    do {
      RETURN_IF_ERROR(scan_range->GetNext(&io_mgr_buffer));
      memcpy(block->buffer() + offset, io_mgr_buffer->buffer(), io_mgr_buffer->len());
      offset += io_mgr_buffer->len();
      io_mgr_buffer->Return();
    } while (!io_mgr_buffer->eosr());
    DCHECK_EQ(offset, block->write_range_->len());
  }

  return Status::OK;
}

Status BufferedBlockMgr::UnpinBlock(Block* block) {
  lock_guard<mutex> unpinned_lock(lock_);
  if (closed_) return Status::CANCELLED;
  DCHECK(block->is_pinned_) << "Unpin for unpinned block.";
  DCHECK(!block->is_deleted_) << "Unpin for deleted block.";
  DCHECK(Validate()) << endl << DebugString();
  // Add 'block' to the list of unpinned blocks and set is_pinned_ to false.
  // Cache its position in the list for later removal.
  block->is_pinned_ = false;
  DCHECK(block->unpinned_blocks_it_ == unpinned_blocks_.end())
      << " Unpin for block in unpinned list";
  if (!block->in_write_) {
    block->unpinned_blocks_it_ = unpinned_blocks_.insert(unpinned_blocks_.end(), block);
  }
  RETURN_IF_ERROR(WriteUnpinnedBlocks());
  DCHECK(Validate()) << endl << DebugString();
  return Status::OK;
}

Status BufferedBlockMgr::WriteUnpinnedBlocks() {
  // Assumes block manager lock is already taken.
  while (!unpinned_blocks_.empty() &&
      num_outstanding_writes_ < (block_write_threshold_ - free_buffers_.size())) {
    // Pop a block from the back of the list (LIFO)
    Block* block_to_write = unpinned_blocks_.back();
    unpinned_blocks_.pop_back();
    block_to_write->unpinned_blocks_it_ = unpinned_blocks_.end();

    if (block_to_write->write_range_ == NULL) {
      // First time the block is being persisted. Find the next physical file in
      // round-robin order and create a write range for it.
      TmpFileMgr::File& tmp_file = tmp_files_[next_block_index_];
      next_block_index_ = (next_block_index_ + 1) % tmp_files_.size();
      int64_t file_offset;
      RETURN_IF_ERROR(tmp_file.AllocateSpace(block_size_, &file_offset));
      // Assign a valid disk id to the write range if the tmp file was not assigned one.
      int disk_id = tmp_file.disk_id();
      if (disk_id < 0) {
        static unsigned int next_disk_id = 0;
        disk_id = (++next_disk_id) % io_mgr_->num_disks();
      }
      disk_id %= io_mgr_->num_disks();
      DiskIoMgr::WriteRange::WriteDoneCallback callback =
          bind(mem_fn(&BufferedBlockMgr::WriteComplete), this, block_to_write, _1);
      block_to_write->write_range_ = obj_pool_.Add(new DiskIoMgr::WriteRange(
          tmp_file.path(), file_offset, disk_id, callback));
    }

    block_to_write->write_range_->SetData(block_to_write->buffer(),
        block_to_write->valid_data_len_);

    // Issue write through DiskIoMgr.
    RETURN_IF_ERROR(
        io_mgr_->AddWriteRange(io_request_context_, block_to_write->write_range_));
    block_to_write->in_write_ = true;
    DCHECK(block_to_write->Validate()) << endl << block_to_write->DebugString();
    ++num_outstanding_writes_;
    outstanding_writes_counter_->Update(1);
    writes_issued_counter_->Update(1);
  }
  DCHECK(Validate()) << endl << DebugString();
  return Status::OK;
}

void BufferedBlockMgr::WriteComplete(Block* block, const Status& write_status) {
  outstanding_writes_counter_->Update(-1);
  lock_guard<mutex> lock(lock_);
  DCHECK(Validate()) << endl << DebugString();
  --num_outstanding_writes_;
  if (closed_) return;
  DCHECK(block->in_write_) << "WriteComplete() for block not in write."
                           << endl << block->DebugString();
  block->in_write_ = false;
  // If the block was re-pinned when it was in the IOMgr queue, don't free it.
  if (block->is_pinned_) {
    // The number of outstanding writes has decreased but the number of free buffers
    // hasn't.
    WriteUnpinnedBlocks();
    DCHECK(Validate()) << endl << DebugString();
    return;
  }
  free_buffers_.Enqueue(block->buffer_desc_);
  if (block->is_deleted_) ReturnUnusedBlock(block);
  DCHECK(Validate()) << endl << DebugString();
  if (write_status.ok()) {
    buffer_available_cv_.notify_one();
  } else {
    Close();
    buffer_available_cv_.notify_all();
  }
}

Status BufferedBlockMgr::DeleteBlock(Block* block) {
  lock_guard<mutex> lock(lock_);
  if (closed_) return Status::CANCELLED;
  // For now, only pinned blocks can be deleted.
  DCHECK(block->is_pinned_);

  block->is_deleted_ = true;
  block->is_pinned_ = false;

  // If a write is still pending, return. Cleanup will be done in WriteComplete().
  if (block->in_write_) return Status::OK;

  free_buffers_.Enqueue(block->buffer_desc_);
  ReturnUnusedBlock(block);
  DCHECK(Validate()) << endl << DebugString();
  buffer_available_cv_.notify_one();
  return Status::OK;
}

void BufferedBlockMgr::ReturnUnusedBlock(Block* block) {
  // Assumes the lock is already taken.
  DCHECK(block->is_deleted_);
  block->buffer_desc_->block = NULL;
  block->buffer_desc_ = NULL;
  unused_blocks_.Enqueue(block);
  block->Init();
}

Status BufferedBlockMgr::FindBufferForBlock(Block* block, bool* in_mem) {
  unique_lock<mutex> l(lock_);
  if (closed_) return Status::CANCELLED;
  DCHECK(!block->is_pinned_ && !block->is_deleted_) << "GetBufferForBlock() "
                                                    << endl << block->DebugString();
  DCHECK(Validate()) << endl << DebugString();
  *in_mem = false;
  if (block->buffer_desc_ != NULL) {
    // The block is in memory. It may be in 3 states
    // 1) In the unpinned list. The buffer will not be in the free list.
    // 2) Or, in_write_ = true. The buffer will not be in the free list.
    // 3) Or, the buffer is free, but hasn;t yet been reassigned to a different block.
    DCHECK((block->unpinned_blocks_it_ != unpinned_blocks_.end()) ||
        free_buffers_.Contains(block->buffer_desc_) || block->in_write_);
    if (block->unpinned_blocks_it_ != unpinned_blocks_.end()) {
      unpinned_blocks_.erase(block->unpinned_blocks_it_);
      block->unpinned_blocks_it_ = unpinned_blocks_.end();
      DCHECK(!free_buffers_.Contains(block->buffer_desc_));
    } else if (!block->in_write_) {
      free_buffers_.Remove(block->buffer_desc_);
    } else {
      DCHECK(block->in_write_ && !free_buffers_.Contains(block->buffer_desc_));
    }

    buffered_pin_counter_->Update(1);
    *in_mem = true;
  } else {
    // At this point, we know the block is not associated with any buffer.
    // Get a free buffer from the front of the queue and assign it to the block.
    // Wait if there are no free buffers available.
    while (free_buffers_.empty()) {
      SCOPED_TIMER(buffer_wait_timer_);
      buffer_available_cv_.wait(l);
      if (closed_) return Status::CANCELLED;
    }
    BufferDescriptor* buffer = free_buffers_.Dequeue();
    // Check if the buffer was assigned to a different block
    if (buffer->block != NULL) {
      DCHECK(buffer->block->Validate()) << endl << buffer->block->DebugString();
      buffer->block->buffer_desc_ = NULL;
    }

    buffer->block = block;
    block->buffer_desc_ = buffer;
  }

  DCHECK_NOTNULL(block->buffer_desc_);
  block->is_pinned_ = true;
  DCHECK(block->Validate()) << endl << block->DebugString();
  // The number of free buffers has decreased. Write unpinned blocks if the number
  // of free buffers below the threshold is reached.
  RETURN_IF_ERROR(WriteUnpinnedBlocks());
  DCHECK(Validate()) << endl << DebugString();
  return Status::OK;
}

bool BufferedBlockMgr::Validate() const {
  int num_free_buffers = 0;
  BOOST_FOREACH(BufferDescriptor* buffer, all_buffers_) {
    bool is_free = free_buffers_.Contains(buffer);
    num_free_buffers += is_free;
    if (buffer->block == NULL && !is_free) {
      LOG(WARNING) << "Buffer with no block not in free list." << endl << DebugString();
      return false;
    }

    if (buffer->block != NULL) {
      if (!buffer->block->Validate()) {
        LOG (WARNING) << "buffer->block inconsistent."
                      << endl << buffer->block->DebugString();
        return false;
      }

      if (is_free && (buffer->block->is_pinned_ || buffer->block->in_write_ ||
          buffer->block->unpinned_blocks_it_ != unpinned_blocks_.end())) {
        LOG (WARNING) << "Block with buffer in free list and"
                      << " is_pinned_ = " << buffer->block->is_pinned_
                      << " in_write_ = " << buffer->block->in_write_
                      << " Unpinned_blocks_.Contains = "
                      << (buffer->block->unpinned_blocks_it_ != unpinned_blocks_.end())
                      << endl << buffer->block->DebugString();
        return false;
      }
    }
  }

  if (free_buffers_.size() != num_free_buffers) {
    LOG (WARNING) << "free_buffer_list_ inconsistency."
                  << " num_free_buffers = " << num_free_buffers
                  << " free_buffer_list_.size() = " << free_buffers_.size()
                  << endl << DebugString();
    return false;
  }

  BOOST_FOREACH(Block* block, unpinned_blocks_) {
    if (block->unpinned_blocks_it_ == unpinned_blocks_.end()) {
      LOG (WARNING) << "Block in unpinned list with no pointer to list "
                    << endl << block->DebugString();
    }
    if (!block->Validate()) {
      LOG (WARNING) << "Block inconsistent in unpinned list."
                    << endl << block->DebugString();
      return false;
    }

    if (block->in_write_ || free_buffers_.Contains(block->buffer_desc_)) {
      LOG (WARNING) << "Block in unpinned list with"
                    << " in_write_ = " << block->in_write_
                    << " free_buffers_.Contains = "
                    << free_buffers_.Contains(block->buffer_desc_)
                    << endl << block->DebugString();
      return false;
    }
  }

  if (!unpinned_blocks_.empty() &&
      (free_buffers_.size() + num_outstanding_writes_ < block_write_threshold_)) {
    LOG (WARNING) << "Missed writing unpinned blocks";
    return false;
  }
  return true;
}

string BufferedBlockMgr::DebugString() const {
  stringstream ss;
  ss << " Num blocks unpinned " << unpin_counter_->value()
     << " Num blocks pinned " << pin_counter_->value()
     << " Num blocks deleted " << delete_counter_->value()
     << " Num writes outstanding " << outstanding_writes_counter_->value()
     << " Num free buffers " << free_buffers_.size()
     << " Block write threshold " << block_write_threshold_;

  return ss.str();
}

void BufferedBlockMgr::InitCounters(RuntimeProfile* profile) {
  mem_limit_counter_ = ADD_COUNTER(profile, "MemoryLimit", TCounterType::UNIT);
  mem_limit_counter_->Set(mem_tracker_->SpareCapacity());
  block_size_counter_ = ADD_COUNTER(profile, "BlockSize", TCounterType::UNIT);
  block_size_counter_->Set(block_size_);
  created_block_counter_ = ADD_COUNTER(profile, "NumCreatedBlocks", TCounterType::UNIT);
  recycled_blocks_counter_ =
      ADD_COUNTER(profile, "RecycledBlocks", TCounterType::UNIT);
  writes_issued_counter_ = ADD_COUNTER(profile, "WritesIssued", TCounterType::UNIT);
  outstanding_writes_counter_ =
      ADD_COUNTER(profile, "WritesOutstanding", TCounterType::UNIT);

  pin_counter_ = ADD_COUNTER(profile, "NumPinned", TCounterType::UNIT);
  unpin_counter_ = ADD_COUNTER(profile, "NumUnpinned", TCounterType::UNIT);
  delete_counter_ = ADD_COUNTER(profile, "NumDeleted", TCounterType::UNIT);

  buffered_pin_counter_ = ADD_COUNTER(profile, "BufferedPins", TCounterType::UNIT);
  disk_read_timer_ = ADD_TIMER(profile, "TotalReadBlockTime");
  buffer_wait_timer_ = ADD_TIMER(profile, "TotalBufferWaitTime");
}

} // namespace impala
