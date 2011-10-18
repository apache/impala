// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/mem-pool.h"

#include <glog/logging.h>
#include <stdio.h>
#include <sstream>

using namespace std;
using namespace impala;

const int MemPool::DEFAULT_INITIAL_CHUNK_SIZE;
const int MemPool::MAX_CHUNK_SIZE;

MemPool::MemPool()
  : last_allocated_chunk_idx_(-1),
    free_offset_(0),
    chunk_size_(0),
    allocated_bytes_(0),
    acquired_allocated_bytes_(0) {
}

MemPool::MemPool(int chunk_size)
  : last_allocated_chunk_idx_(-1),
    free_offset_(0),
    // round up chunk size to nearest 8 bytes
    chunk_size_(((chunk_size + 7) / 8) * 8),
    allocated_bytes_(0),
    acquired_allocated_bytes_(0) {
  DCHECK_GT(chunk_size_, 0);
}

MemPool::~MemPool() {
  for (size_t i = 0; i < allocated_chunks_.size(); ++i) {
    delete [] allocated_chunks_[i];
  }
  for (size_t i = 0; i < acquired_chunks_.size(); ++i) {
    delete [] acquired_chunks_[i];
  }
}

void MemPool::AllocChunk(int min_size) {
  // cast size() to signed int in order to avoid -1 (last_chunk) to be cast
  // to unsigned long
  if (!allocated_chunk_sizes_.empty()
      && last_allocated_chunk_idx_ < static_cast<int>(allocated_chunk_sizes_.size() - 1)) {
    // try to re-use existing unoccupied chunk
    DCHECK_GE(last_allocated_chunk_idx_, -1);
    ++last_allocated_chunk_idx_;
    free_offset_ = 0;
    if (allocated_chunk_sizes_[last_allocated_chunk_idx_] < min_size) {
      // insert a new min_size chunk
      allocated_chunks_.insert(allocated_chunks_.begin() + last_allocated_chunk_idx_, new char[min_size]);
      allocated_chunk_sizes_.insert(allocated_chunk_sizes_.begin() + last_allocated_chunk_idx_, min_size);
    }
    return;
  }

  int chunk_size = chunk_size_;
  if (chunk_size == 0) {
    if (last_allocated_chunk_idx_ == -1) {
      chunk_size = DEFAULT_INITIAL_CHUNK_SIZE;
    } else {
      chunk_size = std::min(allocated_chunk_sizes_[last_allocated_chunk_idx_] * 2, MAX_CHUNK_SIZE);
    }
  }
  chunk_size = max(min_size, chunk_size);

  allocated_chunks_.push_back(new char[chunk_size]);
  allocated_chunk_sizes_.push_back(chunk_size);
  free_offset_ = 0;
  ++last_allocated_chunk_idx_;
}

void MemPool::AcquireData(MemPool* src, bool keep_current) {
  string before_this_str = DebugString();
  string before_src_str = src->DebugString();

  // pass on data sitting in allocated_chunks_
  int num_released_chunks;
  if (keep_current) {
    num_released_chunks = src->last_allocated_chunk_idx_;
  } else {
    if (src->free_offset_ == 0) {
      // nothing in the last chunk
      num_released_chunks = src->last_allocated_chunk_idx_;
    } else {
      num_released_chunks = src->last_allocated_chunk_idx_ + 1;
    }
  }

  if (num_released_chunks > 0) {
    vector<char*>::iterator end_chunk = src->allocated_chunks_.begin() + num_released_chunks;
    acquired_chunks_.insert(acquired_chunks_.end(), src->allocated_chunks_.begin(), end_chunk);
    src->allocated_chunks_.erase(
        src->allocated_chunks_.begin(), src->allocated_chunks_.begin() + num_released_chunks);
    src->allocated_chunk_sizes_.erase(
        src->allocated_chunk_sizes_.begin(),
        src->allocated_chunk_sizes_.begin() + num_released_chunks);
    if (keep_current) {
      src->last_allocated_chunk_idx_ = 0;
      acquired_allocated_bytes_ += src->allocated_bytes_ - src->free_offset_ ;
      src->allocated_bytes_ = src->free_offset_;
    } else {
      src->last_allocated_chunk_idx_ = -1;
      src->free_offset_ = 0;
      acquired_allocated_bytes_ += src->allocated_bytes_;
      src->allocated_bytes_ = 0;
    }
  }

  bool acquired_chunks_empty = src->acquired_chunks_.empty();

  // pass on acquired_chunks_
  acquired_chunks_.insert(
      acquired_chunks_.end(), src->acquired_chunks_.begin(), src->acquired_chunks_.end());
  src->acquired_chunks_.clear();
  acquired_allocated_bytes_ += src->acquired_allocated_bytes_;
  src->acquired_allocated_bytes_ = 0;

  if (num_released_chunks > 0 || !acquired_chunks_empty) {
    VLOG(1) << "before this " << before_this_str;
    VLOG(1) << "before src " << before_src_str;
    VLOG(1) << "after this " << DebugString();
    VLOG(1) << "after src " << src->DebugString();
  }
}

std::string MemPool::DebugString() {
  stringstream out;
  out << "MemPool(#chunks=" << allocated_chunks_.size()
      << " [";
  for (int i = 0; i < allocated_chunk_sizes_.size(); ++i) {
    out << (i > 0 ? " " : "") << allocated_chunk_sizes_[i];
  }
  out << "] #acquired=" << acquired_chunks_.size()
      << " last=" << last_allocated_chunk_idx_
      << " free_offset=" << free_offset_
      << " total_sizes=" << GetTotalChunkSizes()
      << " alloc=" << allocated_bytes_
      << " acq_alloc=" << acquired_allocated_bytes_
      << ")";
  return out.str();
}

int64_t MemPool::GetTotalChunkSizes() const {
  int64_t result = 0;
  for (int i = 0; i < allocated_chunk_sizes_.size(); ++i) {
    result += allocated_chunk_sizes_[i];
  }
  return result;
}
