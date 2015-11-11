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

#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "util/impalad-metrics.h"

#include <algorithm>
#include <stdio.h>
#include <sstream>

#include "common/names.h"

using namespace impala;

DECLARE_bool(disable_mem_pools);

const int MemPool::DEFAULT_INITIAL_CHUNK_SIZE;

const char* MemPool::LLVM_CLASS_NAME = "class.impala::MemPool";

MemPool::MemPool(MemTracker* mem_tracker, int chunk_size)
  : current_chunk_idx_(-1),
    last_offset_conversion_chunk_idx_(-1),
    // round up chunk size to nearest 8 bytes
    chunk_size_(chunk_size == 0
      ? 0
      : ((chunk_size + 7) / 8) * 8),
    total_allocated_bytes_(0),
    peak_allocated_bytes_(0),
    total_reserved_bytes_(0),
    mem_tracker_(mem_tracker) {
  DCHECK_GE(chunk_size_, 0);
  DCHECK(mem_tracker != NULL);
}

MemPool::ChunkInfo::ChunkInfo(int64_t size, uint8_t* buf)
  : data(buf),
    size(size),
    cumulative_allocated_bytes(0),
    allocated_bytes(0) {
  if (ImpaladMetrics::MEM_POOL_TOTAL_BYTES != NULL) {
    ImpaladMetrics::MEM_POOL_TOTAL_BYTES->Increment(size);
  }
}

MemPool::~MemPool() {
  int64_t total_bytes_released = 0;
  for (size_t i = 0; i < chunks_.size(); ++i) {
    total_bytes_released += chunks_[i].size;
    free(chunks_[i].data);
  }

  DCHECK(chunks_.empty()) << "Must call FreeAll() or AcquireData() for this pool";
  if (ImpaladMetrics::MEM_POOL_TOTAL_BYTES != NULL) {
    ImpaladMetrics::MEM_POOL_TOTAL_BYTES->Increment(-total_bytes_released);
  }
}

void MemPool::FreeAll() {
  int64_t total_bytes_released = 0;
  for (size_t i = 0; i < chunks_.size(); ++i) {
    total_bytes_released += chunks_[i].size;
    free(chunks_[i].data);
  }
  chunks_.clear();
  current_chunk_idx_ = -1;
  last_offset_conversion_chunk_idx_ = -1;
  total_allocated_bytes_ = 0;
  total_reserved_bytes_ = 0;

  mem_tracker_->Release(total_bytes_released);
  if (ImpaladMetrics::MEM_POOL_TOTAL_BYTES != NULL) {
    ImpaladMetrics::MEM_POOL_TOTAL_BYTES->Increment(-total_bytes_released);
  }
}

bool MemPool::FindChunk(int64_t min_size, bool check_limits) {
  // Try to allocate from a free chunk. The first free chunk, if any, will be immediately
  // after the current chunk.
  int first_free_idx = current_chunk_idx_ + 1;
  // (cast size() to signed int in order to avoid everything else being cast to
  // unsigned long, in particular -1)
  while (++current_chunk_idx_  < static_cast<int>(chunks_.size())) {
    // we found a free chunk
    DCHECK_EQ(chunks_[current_chunk_idx_].allocated_bytes, 0);

    if (chunks_[current_chunk_idx_].size >= min_size) {
      // This chunk is big enough.  Move it before the other free chunks.
      if (current_chunk_idx_ != first_free_idx) {
        std::swap(chunks_[current_chunk_idx_], chunks_[first_free_idx]);
        current_chunk_idx_ = first_free_idx;
      }
      break;
    }
  }

  if (current_chunk_idx_ == static_cast<int>(chunks_.size())) {
    // need to allocate new chunk.
    int64_t chunk_size = chunk_size_;
    if (chunk_size == 0) {
      if (current_chunk_idx_ == 0) {
        chunk_size = DEFAULT_INITIAL_CHUNK_SIZE;
      } else {
        // double the size of the last chunk in the list
        chunk_size = chunks_[current_chunk_idx_ - 1].size * 2;
      }
    }
    chunk_size = ::max(min_size, chunk_size);

    if (FLAGS_disable_mem_pools) chunk_size = min_size;

    if (check_limits) {
      if (!mem_tracker_->TryConsume(chunk_size)) {
        // We couldn't allocate a new chunk so current_chunk_idx_ is now be past the
        // end of chunks_.
        DCHECK_EQ(current_chunk_idx_, static_cast<int>(chunks_.size()));
        current_chunk_idx_ = static_cast<int>(chunks_.size()) - 1;
        return false;
      }
    } else {
      mem_tracker_->Consume(chunk_size);
    }

    // Allocate a new chunk. Return early if malloc fails.
    uint8_t* buf = reinterpret_cast<uint8_t*>(malloc(chunk_size));
    if (UNLIKELY(buf == NULL)) {
      mem_tracker_->Release(chunk_size);
      DCHECK_EQ(current_chunk_idx_, static_cast<int>(chunks_.size()));
      current_chunk_idx_ = static_cast<int>(chunks_.size()) - 1;
      return false;
    }

    // If there are no free chunks put it at the end, otherwise before the first free.
    if (first_free_idx == static_cast<int>(chunks_.size())) {
      chunks_.push_back(ChunkInfo(chunk_size, buf));
    } else {
      current_chunk_idx_ = first_free_idx;
      vector<ChunkInfo>::iterator insert_chunk = chunks_.begin() + current_chunk_idx_;
      chunks_.insert(insert_chunk, ChunkInfo(chunk_size, buf));
    }
    total_reserved_bytes_ += chunk_size;
  }

  if (current_chunk_idx_ > 0) {
    ChunkInfo& prev_chunk = chunks_[current_chunk_idx_ - 1];
    chunks_[current_chunk_idx_].cumulative_allocated_bytes =
        prev_chunk.cumulative_allocated_bytes + prev_chunk.allocated_bytes;
  }

  DCHECK_LT(current_chunk_idx_, static_cast<int>(chunks_.size()));
  DCHECK(CheckIntegrity(true));
  return true;
}

void MemPool::AcquireData(MemPool* src, bool keep_current) {
  DCHECK(src->CheckIntegrity(false));
  int num_acquired_chunks;
  if (keep_current) {
    num_acquired_chunks = src->current_chunk_idx_;
  } else if (src->GetFreeOffset() == 0) {
    // nothing in the last chunk
    num_acquired_chunks = src->current_chunk_idx_;
  } else {
    num_acquired_chunks = src->current_chunk_idx_ + 1;
  }

  if (num_acquired_chunks <= 0) {
    if (!keep_current) src->FreeAll();
    return;
  }

  vector<ChunkInfo>::iterator end_chunk = src->chunks_.begin() + num_acquired_chunks;
  int64_t total_transfered_bytes = 0;
  for (vector<ChunkInfo>::iterator i = src->chunks_.begin(); i != end_chunk; ++i) {
    total_transfered_bytes += i->size;
  }
  src->total_reserved_bytes_ -= total_transfered_bytes;
  total_reserved_bytes_ += total_transfered_bytes;

  src->mem_tracker_->Release(total_transfered_bytes);
  mem_tracker_->Consume(total_transfered_bytes);

  // insert new chunks after current_chunk_idx_
  vector<ChunkInfo>::iterator insert_chunk = chunks_.begin() + current_chunk_idx_ + 1;
  chunks_.insert(insert_chunk, src->chunks_.begin(), end_chunk);
  src->chunks_.erase(src->chunks_.begin(), end_chunk);
  current_chunk_idx_ += num_acquired_chunks;

  if (keep_current) {
    src->current_chunk_idx_ = 0;
    DCHECK(src->chunks_.size() == 1 || src->chunks_[1].allocated_bytes == 0);
    total_allocated_bytes_ += src->total_allocated_bytes_ - src->GetFreeOffset();
    src->chunks_[0].cumulative_allocated_bytes = 0;
    src->total_allocated_bytes_ = src->GetFreeOffset();
  } else {
    src->current_chunk_idx_ = -1;
    total_allocated_bytes_ += src->total_allocated_bytes_;
    src->total_allocated_bytes_ = 0;
  }
  peak_allocated_bytes_ = std::max(total_allocated_bytes_, peak_allocated_bytes_);

  // recompute cumulative_allocated_bytes
  int start_idx = chunks_.size() - num_acquired_chunks;
  int cumulative_bytes = (start_idx == 0
      ? 0
      : chunks_[start_idx - 1].cumulative_allocated_bytes
        + chunks_[start_idx - 1].allocated_bytes);
  for (int i = start_idx; i <= current_chunk_idx_; ++i) {
    chunks_[i].cumulative_allocated_bytes = cumulative_bytes;
    cumulative_bytes += chunks_[i].allocated_bytes;
  }

  if (!keep_current) src->FreeAll();
  DCHECK(CheckIntegrity(false));
}

bool MemPool::Contains(uint8_t* ptr, int size) {
  for (int i = 0; i < chunks_.size(); ++i) {
    const ChunkInfo& info = chunks_[i];
    if (ptr >= info.data && ptr < info.data + info.allocated_bytes) {
      if (ptr + size > info.data + info.allocated_bytes) {
        DCHECK_LE(reinterpret_cast<size_t>(ptr + size),
                  reinterpret_cast<size_t>(info.data + info.allocated_bytes));
        return false;
      }
      return true;
    }
  }
  return false;
}

string MemPool::DebugString() {
  stringstream out;
  char str[16];
  out << "MemPool(#chunks=" << chunks_.size() << " [";
  for (int i = 0; i < chunks_.size(); ++i) {
    sprintf(str, "0x%lx=", reinterpret_cast<size_t>(chunks_[i].data));
    out << (i > 0 ? " " : "")
        << str
        << chunks_[i].size
        << "/" << chunks_[i].cumulative_allocated_bytes
        << "/" << chunks_[i].allocated_bytes;
  }
  out << "] current_chunk=" << current_chunk_idx_
      << " total_sizes=" << GetTotalChunkSizes()
      << " total_alloc=" << total_allocated_bytes_
      << ")";
  return out.str();
}

int64_t MemPool::GetTotalChunkSizes() const {
  int64_t result = 0;
  for (int i = 0; i < chunks_.size(); ++i) {
    result += chunks_[i].size;
  }
  return result;
}

bool MemPool::CheckIntegrity(bool current_chunk_empty) {
  // Without pooling, there are way too many chunks and this takes too long.
  if (FLAGS_disable_mem_pools) return true;

  // check that current_chunk_idx_ points to the last chunk with allocated data
  DCHECK_LT(current_chunk_idx_, static_cast<int>(chunks_.size()));
  int64_t total_allocated = 0;
  for (int i = 0; i < chunks_.size(); ++i) {
    DCHECK_GT(chunks_[i].size, 0);
    if (i < current_chunk_idx_) {
      DCHECK_GT(chunks_[i].allocated_bytes, 0);
    } else if (i == current_chunk_idx_) {
      if (current_chunk_empty) {
        DCHECK_EQ(chunks_[i].allocated_bytes, 0);
      } else {
        DCHECK_GT(chunks_[i].allocated_bytes, 0);
      }
    } else {
      DCHECK_EQ(chunks_[i].allocated_bytes, 0);
    }
    if (i > 0 && i <= current_chunk_idx_) {
      DCHECK_EQ(chunks_[i-1].cumulative_allocated_bytes + chunks_[i-1].allocated_bytes,
                chunks_[i].cumulative_allocated_bytes);
    }
    if (chunk_size_ != 0) DCHECK_GE(chunks_[i].size, chunk_size_);
    total_allocated += chunks_[i].allocated_bytes;
  }
  DCHECK_EQ(total_allocated, total_allocated_bytes_);
  return true;
}

int MemPool::GetOffsetHelper(uint8_t* data) {
  if (chunks_.empty()) return -1;
  // try to locate chunk containing 'data', starting with chunk following
  // the last one we looked at
  for (int i = 0; i < chunks_.size(); ++i) {
    int idx = (last_offset_conversion_chunk_idx_ + i + 1) % chunks_.size();
    const ChunkInfo& info = chunks_[idx];
    if (info.data <= data && info.data + info.allocated_bytes > data) {
      last_offset_conversion_chunk_idx_ = idx;
      return info.cumulative_allocated_bytes + data - info.data;
    }
  }
  return -1;
}

uint8_t* MemPool::GetDataPtrHelper(int offset) {
  if (offset > total_allocated_bytes_) return NULL;
  for (int i = 0; i < chunks_.size(); ++i) {
    int idx = (last_offset_conversion_chunk_idx_ + i + 1) % chunks_.size();
    const ChunkInfo& info = chunks_[idx];
    if (info.cumulative_allocated_bytes <= offset
        && info.cumulative_allocated_bytes + info.allocated_bytes > offset) {
      last_offset_conversion_chunk_idx_ = idx;
      return info.data + offset - info.cumulative_allocated_bytes;
    }
  }
  return NULL;
}

void MemPool::GetChunkInfo(vector<pair<uint8_t*, int> >* chunk_info) {
  chunk_info->clear();
  for (vector<ChunkInfo>::iterator info = chunks_.begin(); info != chunks_.end(); ++info) {
    chunk_info->push_back(make_pair(info->data, info->allocated_bytes));
  }
}

string MemPool::DebugPrint() {
  char str[3];
  stringstream out;
  for (int i = 0; i < chunks_.size(); ++i) {
    ChunkInfo& info = chunks_[i];
    if (info.allocated_bytes == 0) return out.str();

    for (int j = 0; j < info.allocated_bytes; ++j) {
      sprintf(str, "%x ", info.data[j]);
      out << str;
    }
  }
  return out.str();
}
