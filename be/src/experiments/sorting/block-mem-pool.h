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


#ifndef IMPALA_UTIL_BLOCK_MEM_POOL_H_
#define IMPALA_UTIL_BLOCK_MEM_POOL_H_

#include <boost/shared_ptr.hpp>

#include "buffer-pool.h"
#include "disk-structs.h"
#include "codegen/impala-ir.h"
#include "common/compiler-util.h"
#include "common/object-pool.h"
#include "util/bit-util.h"

namespace impala {

// Provides an Allocator interface on top of a BufferPool.
class BlockMemPool {
 public:
  // Blocks are allocated within the given object pool.
  // Buffer size is determined by the BufferPool.
  BlockMemPool(ObjectPool* obj_pool, BufferPool* buffer_pool)
      : obj_pool_(obj_pool),
        buffer_pool_(buffer_pool),
        buffer_size_(buffer_pool->buffer_size()),
        cur_block_(NULL),
        cur_block_index_(-1),
        total_bytes_allocated_(0),
        reservation_context_(NULL) {
  }

  // Causes the BlockMemPool to reserve a certain number of buffers from the
  // BufferPool, creating a ReservationContext.
  // TODO: Avoid using ReservationContexts -- perhaps have a hierarchical BufferPool
  // structure where each Allocator interacts directly with its own.
  void ReserveBuffers(int num_buffers) {
    DCHECK(reservation_context_ == NULL);
    buffer_pool_->Reserve(num_buffers, &reservation_context_);
  }

  // Allocates a slot for a new element, and returns a pointer to it.
  uint8_t* Allocate(int size) {
    DCHECK_LE(size, buffer_pool_->buffer_size());
    if (WouldExpand(size)) cur_block_ = GetNextBlock(&cur_block_index_);
    return cur_block_->Allocate(size);
  }

  // Inserts an element, and copies the element into it.
  void Insert(void* element, int size) {
    uint8_t* next_ptr = Allocate(size);
    memcpy(next_ptr, element, size);
  }

  // Returns true if allocating the given bytes would cause us to get a new Block.
  bool WouldExpand(int size) const {
    return cur_block_ == NULL || !cur_block_->HasRoom(size);
  }

  // Returns the total number of bytes allocated within this pool.
  uint64_t bytes_allocated() const { return total_bytes_allocated_; }

  // Returns the size of each block in this pool.
  int64_t buffer_size() const { return buffer_size_; }

  // The number of blocks we've allocated so far.
  int32_t num_blocks() const { return blocks_.size(); }

  // Returns the underlying array of Blocks that have been allocated by this pool.
  const std::vector<Block*>& blocks() const { return blocks_; }

  // Returns a pointer to the first byte in the given block.
  uint8_t* BlockBegin(uint32_t index) const { return block_infos_[index]->data_; }

  // Returns a pointer to the first byte after the end of the given block.
  uint8_t* BlockEnd(uint32_t index) const { return block_infos_[index]->end_; }

  // Returns a pointer to the next byte that would be returned by Allocate(0).
  // Note that there are no guarantees that this will actually be the location of the next
  // allocation, in particular if there's insufficient room.
  uint8_t* BlockNext(uint32_t index) const { return block_infos_[index]->next_; }

  // Deallocates the blocks and returns their buffers, if we own our blocks.
  ~BlockMemPool() {
    DCHECK_EQ(blocks_.size(), block_infos_.size());
    for (int i = 0; i < blocks_.size(); ++i) {
      delete block_infos_[i];
    }

    if (reservation_context_ != NULL) {
      buffer_pool_->CancelReservation(reservation_context_);
    }
  }

 private:
  // Extra information related to allocations on a single Block.
  struct BlockInfo {
    BlockInfo(Block* block, uint64_t buffer_size) : block_(block) {
      DCHECK(block->buf_desc() != NULL);
      DCHECK(block->buf_desc()->buffer != NULL);
      data_ = block->buf_desc()->buffer;
      next_ = data_;
      end_ = data_ + buffer_size;
    }

    // Returns true if we have enough room to store the given number of bytes.
    bool HasRoom(int size) const {
      return next_ + size <= end_;
    }

    // Allocates a slot for a new element, and returns a pointer to it.
    // Increments the Block's len.
    uint8_t* Allocate(int size) {
      DCHECK(HasRoom(size));
      DCHECK(block_->in_mem());
      uint8_t* element_addr = next_;
      next_ += size;
      block_->IncrementLength(size);
      return element_addr;
    }

    Block* block_;

    // The beginning of the block's actual pool of elements.
    uint8_t* data_;

    // Address to store the next element.
    uint8_t* next_;

    // Address of the byte after our pool of elements.
    uint8_t* end_;
  };

  // Gets a block with index greater than the given one.
  // If such a block has already been allocated, this will return it, otherwise we'll
  // allocate a new one. Increments block_index by 1.
  BlockInfo* GetNextBlock(int* block_index) {
    ++*block_index;
    if (*block_index < block_infos_.size()) {
      return block_infos_[*block_index];
    } else {
      return AllocateBlock();
    }
  }

  // Allocates a new Block.
  BlockInfo* AllocateBlock() {
    Block* block = obj_pool_->Add(new Block());
    block->SetBuffer(buffer_pool_->GetBuffer(reservation_context_));
    blocks_.push_back(block);

    BlockInfo* block_info = new BlockInfo(block, buffer_size_);
    block_infos_.push_back(block_info);
    total_bytes_allocated_ += buffer_size_;
    return block_info;
  }

  // ObjectPool which contains the actual Blocks.
  // Owned by user of the BlockMemPool.
  ObjectPool* obj_pool_;

  // BufferPool out of which buffers are allocated for Blocks.
  BufferPool* buffer_pool_;

  // Size of each block in bytes.
  int64_t buffer_size_;

  // The set of blocks containing the actual data for our vector.
  std::vector<Block*> blocks_;

  // Auxiliary info for each block.
  std::vector<BlockInfo*> block_infos_;

  // The first block that has space remaining.
  BlockInfo* cur_block_;

  // The index of our cur_block_ in our blocks_ array.
  int cur_block_index_;

  // Total number of bytes allocated, a multiple of the block size.
  uint64_t total_bytes_allocated_;

  // Current BufferPool ReservationContext, created from Reserve().
  BufferPool::ReservationContext* reservation_context_;
};

}

#endif
