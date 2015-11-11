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


#ifndef IMPALA_RUNTIME_FREE_POOL_H
#define IMPALA_RUNTIME_FREE_POOL_H

#include <stdio.h>
#include <string.h>
#include <string>
#include "common/logging.h"
#include "runtime/mem-pool.h"
#include "util/bit-util.h"

DECLARE_int32(stress_free_pool_alloc);
DECLARE_bool(disable_mem_pools);

namespace impala {

/// Implementation of a free pool to recycle allocations. The pool is broken
/// up into 64 lists, one for each power of 2. Each allocation is rounded up
/// to the next power of 2. When the allocation is freed, it is added to the
/// corresponding free list.
/// Each allocation has an 8 byte header that immediately precedes the actual
/// allocation. If the allocation is owned by the user, the header contains
/// the ptr to the list that it should be added to on Free().
/// When the allocation is in the pool (i.e. available to be handed out), it
/// contains the link to the next allocation.
/// This has O(1) Allocate() and Free().
/// This is not thread safe.
/// TODO: consider integrating this with MemPool.
/// TODO: consider changing to something more granular than doubling.
class FreePool {
 public:
  /// C'tor, initializes the FreePool to be empty. All allocations come from the
  /// 'mem_pool'.
  FreePool(MemPool* mem_pool)
    : mem_pool_(mem_pool),
      net_allocations_(0) {
    memset(&lists_, 0, sizeof(lists_));
  }

  /// Allocates a buffer of size.
  uint8_t* Allocate(int size) {
#ifndef NDEBUG
    static int32_t alloc_counts = 0;
    if (FLAGS_stress_free_pool_alloc > 0 &&
        (++alloc_counts % FLAGS_stress_free_pool_alloc) == 0) {
      return NULL;
    }
#endif
    ++net_allocations_;
    if (FLAGS_disable_mem_pools) return reinterpret_cast<uint8_t*>(malloc(size));

    /// This is the typical malloc behavior. NULL is reserved for failures.
    if (size == 0) return reinterpret_cast<uint8_t*>(0x1);

    /// Do ceil(log_2(size))
    int free_list_idx = BitUtil::Log2(size);
    DCHECK_LT(free_list_idx, NUM_LISTS);

    FreeListNode* allocation = lists_[free_list_idx].next;
    if (allocation == NULL) {
      // There wasn't an existing allocation of the right size, allocate a new one.
      size = 1 << free_list_idx;
      allocation = reinterpret_cast<FreeListNode*>(
          mem_pool_->Allocate(size + sizeof(FreeListNode)));
      if (UNLIKELY(allocation == NULL)) {
        --net_allocations_;
        return NULL;
      }
    } else {
      // Remove this allocation from the list.
      lists_[free_list_idx].next = allocation->next;
    }
    DCHECK(allocation != NULL);
    // Set the back node to point back to the list it came from so know where
    // to add it on Free().
    allocation->list = &lists_[free_list_idx];
    return reinterpret_cast<uint8_t*>(allocation) + sizeof(FreeListNode);
  }

  void Free(uint8_t* ptr) {
    --net_allocations_;
    if (FLAGS_disable_mem_pools) {
      free(ptr);
      return;
    }
    if (ptr == NULL || reinterpret_cast<int64_t>(ptr) == 0x1) return;
    FreeListNode* node = reinterpret_cast<FreeListNode*>(ptr - sizeof(FreeListNode));
    FreeListNode* list = node->list;
#ifndef NDEBUG
    CheckValidAllocation(list, ptr);
#endif
    // Add node to front of list.
    node->next = list->next;
    list->next = node;
  }

  /// Returns an allocation that is at least 'size'. If the current allocation backing
  /// 'ptr' is big enough, 'ptr' is returned. Otherwise a new one is made and the contents
  /// of ptr are copied into it.
  ///
  /// NULL will be returned on allocation failure. It's the caller's responsibility to
  /// free the memory buffer pointed to by "ptr" in this case.
  uint8_t* Reallocate(uint8_t* ptr, int size) {
#ifndef NDEBUG
    static int32_t alloc_counts = 0;
    if (FLAGS_stress_free_pool_alloc > 0 &&
        (++alloc_counts % FLAGS_stress_free_pool_alloc) == 0) {
      return NULL;
    }
#endif
    if (FLAGS_disable_mem_pools) {
      return reinterpret_cast<uint8_t*>(realloc(reinterpret_cast<void*>(ptr), size));
    }
    if (ptr == NULL || reinterpret_cast<int64_t>(ptr) == 0x1) return Allocate(size);
    FreeListNode* node = reinterpret_cast<FreeListNode*>(ptr - sizeof(FreeListNode));
    FreeListNode* list = node->list;
#ifndef NDEBUG
    CheckValidAllocation(list, ptr);
#endif
    int bucket_idx = (list - &lists_[0]);
    // This is the actual size of ptr.
    int allocation_size = 1 << bucket_idx;

    // If it's already big enough, just return the ptr.
    if (allocation_size >= size) return ptr;

    // Make a new one. Since Allocate() already rounds up to powers of 2, this effectively
    // doubles for the caller.
    uint8_t* new_ptr = Allocate(size);
    if (LIKELY(new_ptr != NULL)) {
      memcpy(new_ptr, ptr, allocation_size);
      Free(ptr);
    }
    return new_ptr;
  }

  MemTracker* mem_tracker() { return mem_pool_->mem_tracker(); }
  int64_t net_allocations() const { return net_allocations_; }

 private:
  static const int NUM_LISTS = 64;

  struct FreeListNode {
    /// Union for clarity when manipulating the node.
    union {
      FreeListNode* next; // Used when it is in the free list
      FreeListNode* list; // Used when it is being used by the caller.
    };
  };

  void CheckValidAllocation(FreeListNode* computed_list_ptr, uint8_t* allocation) const {
    // On debug, check that list is valid.
    bool found = false;
    for (int i = 0; i < NUM_LISTS && !found; ++i) {
      if (computed_list_ptr == &lists_[i]) found = true;
    }
    DCHECK(found) << "Could not find list for ptr: "
                  << reinterpret_cast<void*>(allocation)
                  << ". Allocation could have already been freed." << std::endl
                  << DebugString();
  }

  std::string DebugString() const {
    std::stringstream ss;
    ss << "FreePool: " << this << std::endl;
    for (int i = 0; i < NUM_LISTS; ++i) {
      FreeListNode* n = lists_[i].next;
      if (n == NULL) continue;
      ss << i << ": ";
      while (n != NULL) {
        uint8_t* ptr = reinterpret_cast<uint8_t*>(n);
        ptr += sizeof(FreeListNode);
        ss << reinterpret_cast<void*>(ptr);
        n = n->next;
        if (n != NULL) ss << "->";
      }
      ss << std::endl;
    }
    return ss.str();
  }

  /// MemPool to allocate from. Unowned.
  MemPool* mem_pool_;

  /// One list head for each allocation size indexed by the LOG_2 of the allocation size.
  /// While it doesn't make too much sense to use this for very small (e.g. 8 byte)
  /// allocations, it makes the indexing easy.
  FreeListNode lists_[NUM_LISTS];

  /// Diagnostic counter that tracks (# Allocates - # Frees)
  int64_t net_allocations_;
};

}

#endif
