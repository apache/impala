// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_FREE_LIST_H
#define IMPALA_RUNTIME_FREE_LIST_H

#include <stdio.h>
#include <algorithm>
#include <vector>
#include <string>
#include <glog/logging.h>
 
namespace impala {

// Implementation of a free list:
// The free list is made up of nodes which contain a pointer to the next node
// and the size of the block.  The free list does not allocate any memory and instead
// overlays the node data over the caller-provided memory.
//
// Since the free list does not allocate or acquire any allocations, it needs to have
// the same lifetime as whereever the allocations came from (i.e MemPool).  If, for
// example, the underlying MemPool is deallocated, if there are any blocks in the 
// free list from that pool, the free list is corrupt.

class FreeList {
 public:
  // Returns the minimum allocation size that is compatible with
  // the free list.  The free list uses the allocations to maintain
  // its own internal linked list structure.
  static int MinSize() {      
    return sizeof(FreeListNode);
  }
    
  // C'tor, initializes the free list to be empty
  FreeList() {
    Reset();
  }

  // Attempts to allocate a block that is at least the input size
  // from the free list.  
  // Returns the size of the entire buffer in *buffer_size.
  // Returns NULL if there is no matching free list size.
  uint8_t* Allocate(int size, int* buffer_size) {
    DCHECK(buffer_size != NULL);
    FreeListNode* prev = &head_;
    FreeListNode* node = head_.next;
    while (node != NULL) {
      if (node->size >= size) {
        prev->next = node->next; 
        *buffer_size = node->size;
        return reinterpret_cast<uint8_t*>(node);
      }
      prev = node;
      node = node->next;
    }
    *buffer_size = 0;
    return NULL;
  }

  // Add a block to the free list.  The caller can no longer touch
  // the memory.  If the size is too small, the free list ignores
  // the memory.
  void Add(uint8_t* memory, int size) {
    if (size < FreeList::MinSize()) return;
    FreeListNode* node = reinterpret_cast<FreeListNode*>(memory);
    node->next = head_.next;
    node->size = size;
    head_.next = node;
  }

  // Empties the free list
  void Reset() {
    bzero(&head_, sizeof(FreeListNode));
  }

 private:
  struct FreeListNode {
    FreeListNode* next;
    int size;
  };

  FreeListNode head_;
};

}

#endif

