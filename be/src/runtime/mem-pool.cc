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
  : current_chunk_idx_(-1),
    last_offset_conversion_chunk_idx_(-1),
    chunk_size_(0),
    total_allocated_bytes_(0) {
}

MemPool::MemPool(int chunk_size)
  : current_chunk_idx_(-1),
    last_offset_conversion_chunk_idx_(-1),
    // round up chunk size to nearest 8 bytes
    chunk_size_(((chunk_size + 7) / 8) * 8),
    total_allocated_bytes_(0) {
  DCHECK_GT(chunk_size_, 0);
}

MemPool::MemPool(vector<string>* chunks)
  : current_chunk_idx_(-1),
    last_offset_conversion_chunk_idx_(-1),
    chunk_size_(0),
    total_allocated_bytes_(0) {
  if (chunks->empty()) return;
  chunks_.reserve(chunks->size());
  for (int i = 0; i < chunks->size(); ++i) {
    chunks_.push_back(ChunkInfo());
    ChunkInfo& chunk = chunks_.back();
    chunk.owns_data = false;
    chunk.data = const_cast<char*>((*chunks)[i].data());
    chunk.size = (*chunks)[i].size();
    VLOG(1) << "chunk: data=" << (void*)chunk.data << " size=" << chunk.size;
    chunk.allocated_bytes = chunk.size;
    chunk.cumulative_allocated_bytes = total_allocated_bytes_;
    total_allocated_bytes_ += chunk.size;
  }
  current_chunk_idx_ = chunks_.size() - 1;
}

MemPool::~MemPool() {
  VLOG(1) << this << " ~mempool: deleting " << chunks_.size() << " chunks";
  for (size_t i = 0; i < chunks_.size(); ++i) {
    if (!chunks_[i].owns_data) continue;
    VLOG(1) << "deleting " << (void*)chunks_[i].data;
    delete [] chunks_[i].data;
  }
}

void MemPool::FindChunk(int min_size) {
  // skip past the last occupied chunk
  // (cast size() to signed int in order to avoid everything else being cast to
  // unsigned long, in particular -1)
  while (current_chunk_idx_ < static_cast<int>(chunks_.size())
      && (current_chunk_idx_ == -1 || chunks_[current_chunk_idx_].allocated_bytes > 0)) {
    ++current_chunk_idx_;
  }

  if (current_chunk_idx_ < static_cast<int>(chunks_.size())) {
    // we found a free chunk
    DCHECK_EQ(chunks_[current_chunk_idx_].allocated_bytes, 0);
    if (chunks_[current_chunk_idx_].size < min_size) {
      // still not big enough, insert a new min_size chunk
      chunks_.insert(chunks_.begin() + current_chunk_idx_, ChunkInfo(min_size));
    }
  } else {
    // need to allocate new chunk; we append it to the existing list
    DCHECK_EQ(current_chunk_idx_, static_cast<int>(chunks_.size()));
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
    chunks_.push_back(ChunkInfo(chunk_size));
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
  string before_this_str = DebugString();
  string before_src_str = src->DebugString();

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
    DCHECK_EQ(src->chunks_.size(), 1);
    total_allocated_bytes_ += src->total_allocated_bytes_ - src->GetFreeOffset();
    src->chunks_[0].cumulative_allocated_bytes = 0;
    src->total_allocated_bytes_ = src->GetFreeOffset();
  } else {
    src->current_chunk_idx_ = -1;
    total_allocated_bytes_ += src->total_allocated_bytes_;
    src->total_allocated_bytes_ = 0;
  }

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

  VLOG(1) << "before this " << before_this_str;
  VLOG(1) << "before src " << before_src_str;
  VLOG(1) << "after this " << DebugString();
  VLOG(1) << "after src " << src->DebugString();
  DCHECK(CheckIntegrity(false));
}

string MemPool::DebugString() {
  stringstream out;
  out << "MemPool(#chunks=" << chunks_.size() << " [";
  for (int i = 0; i < chunks_.size(); ++i) {
    out << (i > 0 ? " " : "")
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
  int total_allocated = 0;
  for (int i = 0; i < chunks_.size(); ++i) {
    DCHECK_GT(chunks_[i].size, 0);
    if (i < current_chunk_idx_) {
      DCHECK_GT(chunks_[i].allocated_bytes, 0);
    } else if (i == current_chunk_idx_ && !current_chunk_empty) {
      DCHECK_GT(chunks_[i].allocated_bytes, 0);
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

int MemPool::GetOffsetHelper(char* data) {
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

char* MemPool::GetDataPtrHelper(int offset) {
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

void MemPool::GetChunkInfo(vector<pair<char*, int> >* chunk_info) {
  chunk_info->clear();
  for (vector<ChunkInfo>::iterator info = chunks_.begin(); info != chunks_.end(); ++info) {
    chunk_info->push_back(make_pair(info->data, info->allocated_bytes));
  }
}

std::string MemPool::DebugPrint() {
  char str[3];
  stringstream out;
  for (int i = 0; i < chunks_.size(); ++i) {
    ChunkInfo& info = chunks_[i];
    if (info.allocated_bytes == 0) return out.str();

    for (int j = 0; j < info.allocated_bytes; ++j) {
      sprintf(str, "%x ", (unsigned char)info.data[j]);
      out << str;
    }
  }
  return out.str();
}
