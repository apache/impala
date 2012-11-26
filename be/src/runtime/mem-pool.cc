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
#include "util/impalad-metrics.h"

#include <algorithm>
#include <stdio.h>
#include <sstream>

using namespace std;
using namespace impala;

const int MemPool::DEFAULT_INITIAL_CHUNK_SIZE;
const int MemPool::MAX_CHUNK_SIZE;

const char* MemPool::LLVM_CLASS_NAME = "class.impala::MemPool";

MemPool::MemPool()
  : current_chunk_idx_(-1),
    last_offset_conversion_chunk_idx_(-1),
    chunk_size_(0),
    total_allocated_bytes_(0),
    peak_allocated_bytes_(0) {
}

MemPool::MemPool(int chunk_size)
  : current_chunk_idx_(-1),
    last_offset_conversion_chunk_idx_(-1),
    // round up chunk size to nearest 8 bytes
    chunk_size_(((chunk_size + 7) / 8) * 8),
    total_allocated_bytes_(0),
    peak_allocated_bytes_(0) {
  DCHECK_GT(chunk_size_, 0);
}

MemPool::MemPool(const vector<string>& chunks)
  : current_chunk_idx_(-1),
    last_offset_conversion_chunk_idx_(-1),
    chunk_size_(0),
    total_allocated_bytes_(0),
    peak_allocated_bytes_(0) {
  if (chunks.empty()) return;
  chunks_.reserve(chunks.size());
  int64_t total_bytes_allocated = 0;
  for (int i = 0; i < chunks.size(); ++i) {
    chunks_.push_back(ChunkInfo());
    ChunkInfo& chunk = chunks_.back();
    chunk.owns_data = true;
    chunk.data = new uint8_t[chunks[i].size()];
    total_bytes_allocated += chunks[i].size();
    memcpy(chunk.data, chunks[i].data(), chunks[i].size());
    chunk.size = chunks[i].size();
    chunk.allocated_bytes = chunk.size;
    chunk.cumulative_allocated_bytes = total_allocated_bytes_;
    total_allocated_bytes_ += chunk.size;
  }
  current_chunk_idx_ = chunks_.size() - 1;

  if (ImpaladMetrics::MEM_POOL_TOTAL_BYTES != NULL) {
    ImpaladMetrics::MEM_POOL_TOTAL_BYTES->Increment(total_bytes_allocated);
  }
}

MemPool::ChunkInfo::ChunkInfo(int size)
  : owns_data(true),
    data(new uint8_t[size]),
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
    if (!chunks_[i].owns_data) continue;
    total_bytes_released += chunks_[i].size;
    delete [] chunks_[i].data;
  }
  if (ImpaladMetrics::MEM_POOL_TOTAL_BYTES != NULL) {
    ImpaladMetrics::MEM_POOL_TOTAL_BYTES->Increment(-total_bytes_released);
  }
}

void MemPool::FindChunk(int min_size) {
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
    int chunk_size = chunk_size_;
    if (chunk_size == 0) {
      if (current_chunk_idx_ == 0) {
        chunk_size = DEFAULT_INITIAL_CHUNK_SIZE;
      } else {
        // double the size of the last chunk in the list, up to a maximum
        // TODO: stick with constant sizes throughout?
        chunk_size = ::min(chunks_[current_chunk_idx_ - 1].size * 2, MAX_CHUNK_SIZE);
      }
    }
    chunk_size = ::max(min_size, chunk_size);
    // If there are no free chunks put it at the end, otherwise before the first free.
    if (first_free_idx == static_cast<int>(chunks_.size())) {
      chunks_.push_back(ChunkInfo(chunk_size));
    } else {
      current_chunk_idx_ = first_free_idx;
      vector<ChunkInfo>::iterator insert_chunk = chunks_.begin() + current_chunk_idx_;
      chunks_.insert(insert_chunk, ChunkInfo(chunk_size));
    }
  }

  if (current_chunk_idx_ > 0) {
    ChunkInfo& prev_chunk = chunks_[current_chunk_idx_ - 1];
    chunks_[current_chunk_idx_].cumulative_allocated_bytes =
        prev_chunk.cumulative_allocated_bytes + prev_chunk.allocated_bytes;
  }

  DCHECK_LT(current_chunk_idx_, static_cast<int>(chunks_.size()));
  DCHECK(CheckIntegrity(true));
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

  if (num_acquired_chunks <= 0) return;

  vector<ChunkInfo>::iterator end_chunk = src->chunks_.begin() + num_acquired_chunks;
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

  DCHECK(CheckIntegrity(false));
}

bool MemPool::Contains(uint8_t* ptr, int size) {
  for (int i = 0; i < chunks_.size(); ++i) {
    const ChunkInfo& info = chunks_[i];
    if (ptr >= info.data && ptr < info.data + info.allocated_bytes) {
      if (ptr + size > info.data + info.allocated_bytes) {
        LOG(ERROR) << DebugString();
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
  // check that current_chunk_idx_ points to the last chunk with allocated data
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
    if (chunk_size_ != 0) DCHECK_EQ(chunks_[i].size, chunk_size_);
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
