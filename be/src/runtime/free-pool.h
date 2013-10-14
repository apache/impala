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

namespace impala {

// Implementation of a free pool to recycle allocations. The pool is broken
// up into 64 lists, one for each power of 2. Each allocation is rounded up
// to the next power of 2. When the allocation is freed, it is added to the
// corresponding free list.
// Each allocation has an 8 byte header that immediately precedes the actual
// allocation. If the allocation is owned by the user, the header contains
// the ptr to the list that it should be added to on Free().
// When the allocation is in the pool (i.e. available to be handed out), it
// contains the link to the next allocation.
// This has O(1) Allocate() and Free().
// This is not thread safe.
// TODO: consider integrating this with MemPool.
class FreePool {
 public:
  // C'tor, initializes the FreePool to be empty. All allocations come from the
  // 'mem_pool'.
  FreePool(MemPool* mem_pool) : mem_pool_(mem_pool) {
    memset(&lists_, 0, sizeof(lists_));
  }

  // Allocates a buffer of size.
  uint8_t* Allocate(int size) {
    // This is the typical malloc behavior. NULL is reserved for failures.
    if (size == 0) return reinterpret_cast<uint8_t*>(0x1);

    // Do ceil(log_2(size))
    int free_list_idx = BitUtil::Log2(size);
    DCHECK_GT(free_list_idx, 0);
    DCHECK_LT(free_list_idx, NUM_LISTS);

    FreeListNode* allocation = lists_[free_list_idx].next;
    if (allocation == NULL) {
      // There wasn't an existing allocation of the right size, allocate a new one.
      size = BitUtil::RoundUp(size, 2);
      allocation = reinterpret_cast<FreeListNode*>(
          mem_pool_->Allocate(size + sizeof(FreeListNode)));
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
    if (ptr == NULL || reinterpret_cast<int64_t>(ptr) == 0x1) return;
    FreeListNode* node = reinterpret_cast<FreeListNode*>(ptr - sizeof(FreeListNode));
    FreeListNode* list = node->list;
#ifndef NDEBUG
    // On debug, check that list is valid.
    bool found = false;
    for (int i = 0; i < NUM_LISTS && !found; ++i) {
      if (list == &lists_[i]) found = true;
    }
    DCHECK(found);
#endif
    // Add node to front of list.
    node->next = list->next;
    list->next = node;
  }

 private:
  static const int NUM_LISTS = 64;

  struct FreeListNode {
    // Union for clarity when manipulating the node.
    union {
      FreeListNode* next; // Used when it is in the free list
      FreeListNode* list; // Used when it is being used by the caller.
    };
  };

  // MemPool to allocate from. Unowned.
  MemPool* mem_pool_;

  // One list head for each allocation size indexed by the LOG_2 of the allocation size.
  // While it doesn't make too much sense to use this for very small (e.g. 8 byte)
  // allocations, it makes the indexing easy.
  FreeListNode lists_[NUM_LISTS];
};

}

#endif

