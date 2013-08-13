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


#ifndef IMPALA_UTIL_BLOCKED_VECTOR_H_
#define IMPALA_UTIL_BLOCKED_VECTOR_H_

#include <boost/shared_ptr.hpp>

#include "block-mem-pool.h"
#include "buffer-pool.h"
#include "codegen/impala-ir.h"
#include "common/compiler-util.h"
#include "common/object-pool.h"
#include "util/bit-util.h"

namespace impala {

// Provides a similar interface to std::vector. Like std::vector, random access is O(1)
// and sequential scans are O(n). While a BlockedVector also provides dynamic resizing,
// it does not copy all data; instead, it allocates a new chunk which is linked to the
// last.
// Also unlike std::vector, the BlockedVector is given the size of elements at runtime,
// making it usable for types like Tuples which are fixed-size but not known at
// compile time.
//
// Internally, the BlockedVector gets memory from a BlockMemPool. Absolute indexes are
// converted to a block index and a relative index into that block.
template <typename T>
class BlockedVector {
 public:
  BlockedVector(BlockMemPool* block_mem_pool, int element_size)
      : block_mem_pool_(block_mem_pool),
        element_size_(element_size),
        block_capacity_(block_mem_pool->buffer_size() / element_size),
        unused_block_bytes_(block_mem_pool->buffer_size() % element_size),
        total_num_elements_(0) {
  }

  BlockedVector() {
  }

  // Allocates a slot for a new element, and returns a pointer to it.
  T* AllocateElement() {
    ++total_num_elements_;
    return reinterpret_cast<T*>(block_mem_pool_->Allocate(element_size_));
  }

  // Allocates a slot for a new element, and copies data into it.
  void Insert(T* element) {
    ++total_num_elements_;
    block_mem_pool_->Insert(element, element_size_);
  }

  // Returns the number of elements in this vector.
  uint64_t size() const {
    return total_num_elements_;
  }

  // Total number of bytes allocated for this vector so far, a multiple of the buffer size
  uint64_t bytes_allocated() const {
    return block_mem_pool_->bytes_allocated();
  }

  int element_size() const { return element_size_; }

  BlockMemPool* pool() const { return block_mem_pool_; }

  T* operator[] (uint64_t index) const {
    uint32_t block_index;
    uint64_t relative_index;
    ResolveIndex(index, &block_index, &relative_index);
    DCHECK(block_mem_pool_->blocks()[block_index]->in_mem());
    return reinterpret_cast<T*>(
        block_mem_pool_->BlockBegin(block_index) + relative_index * element_size_);
  }

  // A "mutable iterator", allows standard iterator operations, plus Swap() and SetValue()
  class Iterator {
    friend class BlockedVector;

    Iterator(const BlockedVector<T>* bv,
             uint64_t abs_index, int block_index, uint8_t* block_ptr)
        : bv_(bv), abs_index_(abs_index),
          block_index_(block_index), cur_block_ptr_(block_ptr) {
    }

   public:
    // Dummy constructor for automatic stack/heap allocations.
    Iterator() { }

    T* operator*() const {
      VerifyInMem();
      return reinterpret_cast<T*>(cur_block_ptr_);
    }

    // Only support prefix increment/decrement, for efficiency purposes
    Iterator& operator++() {
      cur_block_ptr_ += bv_->element_size_;
      ++abs_index_;
      // Special case for when we try to ++ to the last+1 element, which is
      // on the next block: keep cur_block_ptr_ here, pointing to invalid data.
      // This is consistent with End().
      if (UNLIKELY(cur_block_ptr_ >= bv_->BlockEnd(block_index_))
          && abs_index_ != bv_->total_num_elements_) {
        ++block_index_;
        DCHECK_LT(block_index_, bv_->block_mem_pool_->num_blocks());
        cur_block_ptr_ = bv_->BlockBegin(block_index_);
      }
      return *this;
    }

    Iterator& operator--() {
      if (UNLIKELY(cur_block_ptr_ <= bv_->BlockBegin(block_index_))) {
        --block_index_;
        cur_block_ptr_ = bv_->BlockEnd(block_index_);
      }
      cur_block_ptr_ -= bv_->element_size_;
      --abs_index_;
      return *this;
    }
    // Returns a child iterator at the given index
    Iterator IteratorAtIndex(uint64_t abs_index) const {
      uint32_t block_index;
      uint64_t relative_index;
      bv_->ResolveIndex(abs_index, &block_index, &relative_index, true);
      uint8_t* block_ptr = bv_->BlockBegin(block_index)
                           + relative_index * bv_->element_size_;
      return Iterator(bv_, abs_index, block_index, block_ptr);
    }

    Iterator operator+(uint32_t increment) const {
      return IteratorAtIndex(abs_index_ + increment);
    }

    Iterator operator-(uint32_t decrement) const {
      return IteratorAtIndex(abs_index_ - decrement);
    }

    uint32_t operator-(const Iterator& rhs) const { return abs_index_ - rhs.abs_index_; }

    bool operator==(const Iterator& rhs) const { return abs_index_ == rhs.abs_index_; }
    bool operator!=(const Iterator& rhs) const { return abs_index_ != rhs.abs_index_; }
    bool operator<(const Iterator& rhs)  const { return abs_index_ <  rhs.abs_index_; }
    bool operator<=(const Iterator& rhs) const { return abs_index_ <= rhs.abs_index_; }
    bool operator>(const Iterator& rhs)  const { return abs_index_ >  rhs.abs_index_; }
    bool operator>=(const Iterator& rhs) const { return abs_index_ >= rhs.abs_index_; }

    // Const because it changes the value of the iterator, but not the
    // iterator's positional state.
    void SetValue(void* value) const {
      VerifyInMem();
      memcpy(**this, value, bv_->element_size_);
    }

    // Const because it changes the value of the iterator, but not either
    // iterators' positional state.
    void Swap(Iterator& rhs, void* swap_buffer) const {
      VerifyInMem();
      rhs.VerifyInMem();
      memcpy(swap_buffer, *rhs, bv_->element_size_);
      memcpy(*rhs, **this, bv_->element_size_);
      memcpy(**this, swap_buffer, bv_->element_size_);
    }

    uint64_t block_index() const { return block_index_; }
    uint64_t absolute_index() const { return abs_index_; }

   private:
    inline void VerifyInMem() const {
      DCHECK(bv_->block_mem_pool_->blocks()[block_index_]->in_mem());
    }

    // Parent BlockedVector
    const BlockedVector<T>* bv_;

    // Absolute index of the current element
    uint64_t abs_index_;

    // Index into blocks_ of the current block
    uint64_t block_index_;

    // Pointer into the current block
    uint8_t* cur_block_ptr_;
  };

  // Returns an iterator starting at the first element.
  Iterator Begin() const {
    if (UNLIKELY(size() == 0)) return NullIterator();
    return Iterator(this, 0, 0, block_mem_pool_->BlockBegin(0));
  }

  // Returns an iterator starting at one element after the last element.
  Iterator End() const {
    if (UNLIKELY(size() == 0)) return NullIterator();
    // The pointer index is the "next" usable one for the last block, which means
    // that the block index will still be the last block (even though if the element
    // did exist, it would be in a newly allocated block).
    int last_index = block_mem_pool_->blocks().size() - 1;
    return Iterator(this, total_num_elements_, last_index,
        block_mem_pool_->BlockNext(last_index));
  }

 private:
  // Resolves an absolute 'index' into a block index and relative index, allowing
  // the index to overflow by 1 for iterator purposes.
  inline void ResolveIndex(uint64_t index, uint32_t* block_index,
      uint64_t* relative_index, bool allow_one_over=false) const {
    DCHECK_GE(index, 0);
    DCHECK_LT(index, total_num_elements_ + (allow_one_over ? 1 : 0));
    *block_index = index / block_capacity_;
    *relative_index = index % block_capacity_;
  }

  Iterator NullIterator() const {
    return Iterator(this, 0, 0, NULL);
  }

  // Returns a pointer to the beginning of the given block.
  uint8_t* BlockBegin(uint32_t block_index) const {
    return block_mem_pool_->BlockBegin(block_index);
  }

  // Returns a pointer to the end of the given block (i.e., right after the last element).
  // This method accounts for the extra wiggle room that might exist if the buffer size
  // is not a multiple of the element size.
  uint8_t* BlockEnd(uint32_t block_index) const {
    return block_mem_pool_->BlockEnd(block_index) - unused_block_bytes_;
  }

  // Pool from which we allocate memory.
  BlockMemPool* block_mem_pool_;

  // Size in bytes of each element.
  int element_size_;

  // Number of elements a block can hold.
  int64_t block_capacity_;

  // Number of bytes at the end of a block that are unused, if the block size is not
  // a multiple of the element size.
  int64_t unused_block_bytes_;

  // The total number of elements held by this vector.
  uint64_t total_num_elements_;
};

}

#endif
