// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_MEM_POOL_H
#define IMPALA_RUNTIME_MEM_POOL_H

namespace impala {

// A MemPool allows allocation of small pieces of memory
// from a contiguous chunk.
class MemPool {
 public:
  // Allocates mempool with an initial contiguous block of memory of
  // 'initial_size' bytes. Initial_size must be greather than 0.
  MemPool(int initial_size);

  // Frees all allocated chunks of memory.
  ~MemPool();

  // Allocates 8-byte aligned section of memory of 'size' bytes at the end
  // of the the current chunk. Creates a new chunk if current chunk does not
  // have enough capacity.
  void* Allocate(int size);
};

}

#endif

