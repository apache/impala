// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/mem-pool.h"

#include <glog/logging.h>
#include <stdio.h>

using namespace impala;

const int MemPool::DEFAULT_INITIAL_CHUNK_SIZE;
const int MemPool::MAX_CHUNK_SIZE;

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
  DCHECK_GT(chunk_size_, 0);
  AllocChunk(chunk_size_);
}

MemPool::~MemPool() {
  for (size_t i = 0; i < mem_chunks_.size(); ++i) {
    delete [] mem_chunks_[i];
  }
}

void MemPool::AllocChunk(int chunk_size) {
  mem_chunks_.push_back(new char[chunk_size]);
  chunk_sizes_.push_back(chunk_size);
  free_offset_ = 0;
  DCHECK_EQ(mem_chunks_.size(), chunk_sizes_.size());
  DCHECK_LT(last_chunk_, mem_chunks_.size());
}
