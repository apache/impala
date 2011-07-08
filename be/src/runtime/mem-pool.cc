// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/mem-pool.h"

using namespace impala;

MemPool::MemPool()
  : last_chunk_(0),
    free_offset_(0),
    chunk_size_(0) {
  AllocChunk(DEFAULT_INITIAL_CHUNK_SIZE);
}

MemPool::MemPool(int chunk_size)
  : last_chunk_(0),
    free_offset_(0),
    chunk_size_((chunk_size + 7) / 8 * 8) {
  // assert(chunk_size_ > 0);
  AllocChunk(chunk_size_);
}

MemPool::~MemPool() {
  for (int i = 0; i < mem_chunks_.size(); ++i) {
    delete [] mem_chunks_[i];
  }
}

void MemPool::AllocChunk(int chunk_size) {
  mem_chunks_.push_back(new char[chunk_size]);
  chunk_sizes_.push_back(chunk_size);
  free_offset_ = 0;
  // assert(mem_chunks_.size() == chunk_sizes_.size());
  // assert(last_chunk_ < mem_chunks_.size());
}
