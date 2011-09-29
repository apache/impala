// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_MEM_POOL_H
#define IMPALA_RUNTIME_MEM_POOL_H

#include <stdio.h>
#include <algorithm>
#include <vector>
#include <string>
#include <glog/logging.h>

namespace impala {

// A MemPool maintains two lists of memory chunks:
// - one from which it allocates memory in response to Allocate() calls;
// - another one containing chunks which were passed on to it via Release() calls.
// Chunks stay around for the lifetime of the mempool or until they are passed on to
// another mempool.
// An Allocate() call will attempt to allocate memory from the chunk that was most
// recently added (to the first list); if that chunk doesn't have enough memory to
// satisfy the allocation request, a new chunk is added to the list.
// In order to keep allocation overhead low, chunk sizes double with each new one
// added, until they hit a maximum size.
// 
//     Example:
//     MemPool* p = new MemPool();
//     for (int i = 0; i < 1024; ++i) {
// returns 8-byte aligned memory (effectively 24 bytes):
//       .. = p->Allocate(17);
//     }
// at this point, 17K have been handed out in response to Allocate() calls and
// 28K of chunks have been allocated (chunk sizes: 4K, 8K, 16K)
//     p->Clear();
// the entire 1st chunk is returned:
//     .. = p->Allocate(4 * 1024);
// 4K of the 2nd chunk are returned:
//     .. = p->Allocate(4 * 1024);
// a new 20K chunk is created
//     .. = p->Allocate(20 * 1024);
//
//      MemPool* p2 = new MemPool();
// the new mempool receives all chunks containing data from p
//      p->Release(p2, false);
// the one remaining (empty) chunk is released:
//    delete p;

class MemPool {
 public:
  MemPool();

  // Allocates mempool with fixed-size chunks of size 'chunk_size'.
  // Chunk_size must be > 0.
  MemPool(int chunk_size);

  // Frees all allocated chunks of memory.
  ~MemPool();

  // Allocates 8-byte aligned section of memory of 'size' bytes at the end
  // of the the current chunk. Creates a new chunk if current chunk does not
  // have enough capacity or there aren't any.
  char* Allocate(int size) {
    int num_bytes = (size + 7) / 8 * 8;  // round up to nearest 8 bytes
    if (last_allocated_chunk_idx_ == -1 || num_bytes + free_offset_ > allocated_chunk_sizes_[last_allocated_chunk_idx_]) {
      AllocChunk(num_bytes);
    }
    DCHECK_EQ(allocated_chunks_.size(), allocated_chunk_sizes_.size());
    DCHECK_LT(last_allocated_chunk_idx_, allocated_chunks_.size());
    DCHECK_GE(last_allocated_chunk_idx_, 0);
    char* result = allocated_chunks_[last_allocated_chunk_idx_] + free_offset_;
    DCHECK_LE(free_offset_ + num_bytes, allocated_chunk_sizes_[last_allocated_chunk_idx_]);
    free_offset_ += num_bytes;
    allocated_bytes_ += num_bytes;
    return result;
  }

  // Makes all allocated chunks available for re-use, but doesn't delete any chunks.
  void Clear() {
    last_allocated_chunk_idx_ = -1;
    free_offset_ = 0;
    allocated_bytes_ = 0;
  }

  // Absorb all chunks, allocated and acquired, that hold data from src. If keep_current is true,
  // let src hold on to its last allocated chunk that contains data.
  void AcquireData(MemPool* src, bool keep_current);

  std::string DebugString();

  int64_t allocated_bytes() const { return allocated_bytes_; }
  int64_t acquired_allocated_bytes() const { return acquired_allocated_bytes_; }

  // Return sum of allocated_chunk_sizes_.
  int64_t GetTotalChunkSizes() const;

 private:
  static const int DEFAULT_INITIAL_CHUNK_SIZE = 4 * 1024;
  static const int MAX_CHUNK_SIZE = 512 * 1024;

  std::vector<char*> allocated_chunks_;
  std::vector<int> allocated_chunk_sizes_;  // sizes in bytes
  std::vector<char*> acquired_chunks_;
  int last_allocated_chunk_idx_;  // chunks 0-last_allocated_chunk_idx_ contain data
  int free_offset_;  // offset of unallocated memory in last chunk
  int chunk_size_;  // if != 0, use this size for new chunks

  // number of bytes of data sitting in allocated_chunks_ that we handed back
  // via Allocate() (which is not the number of requested bytes, because we pad
  // to 8-byte boundaries)
  int64_t allocated_bytes_;

  int64_t acquired_allocated_bytes_;  // number of bytes of data sitting in acquired_chunks_

  // Create new chunk of at least min_size and update allocated_chunks_ and allocated_chunk_sizes_
  void AllocChunk(int min_size);
};

}

#endif

