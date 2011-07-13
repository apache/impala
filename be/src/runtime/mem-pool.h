// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_MEM_POOL_H
#define IMPALA_RUNTIME_MEM_POOL_H

#include <algorithm>
#include <vector>

namespace impala {

// A MemPool maintains a list of memory chunks from which it allocates
// memory in response to Allocate() calls. Chunks stay around for the lifetime
// of the mempool, and an Allocate() calls will attempt to allocate memory
// from the most recently added chunk; if that chunk doesn't have enough
// memory to satisfy the allocation request, a new chunk is added to the list.
// In order to keep allocation overhead low, chunk sizes double with each new one
// added, until they hit a maximum size.
// 
//     Example:
//     MemPool* p = new MemPool();
//     for (int i = 0; i < 1024; ++i) {
// returns 8-byte aligned memory (effectively 24 bytes):
//       .. = p->Allocate(17);
//     }
// at this point, 28K have been allocated (chunk sizes: 4K, 8K, 16K)
//     p->Clear();
// the entire 2nd chunk is returned:
//     .. = p->Allocate(8 * 1024);
// 4K of the 16K chunk are returned:
//     .. = p->Allocate(4 * 1024);
// all chunks are released:
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
  // have enough capacity.
  void* Allocate(int size) {
    int num_bytes = (size + 7) / 8 * 8;  // round up to nearest 8 bytes
    if (num_bytes + free_offset_ > chunk_sizes_[last_chunk_]) {
      int chunk_size = std::min(chunk_sizes_[last_chunk_] * 2, MAX_CHUNK_SIZE);
      AllocChunk(std::max(num_bytes, chunk_size));
      ++last_chunk_;
    }
    void* result = mem_chunks_[last_chunk_] + free_offset_;
    free_offset_ += num_bytes;
    return result;
  }

  // Makes all allocated chunks available for re-use, but doesn't delete any chunks.
  void Clear() {
    last_chunk_ = 0;
    free_offset_ = 0;
  }

 private:
  std::vector<char*> mem_chunks_;
  std::vector<int> chunk_sizes_;  // sizes in bytes
  int last_chunk_;  // chunks 0-last_chunk_-1 contain data
  int free_offset_;  // offset of unallocated memory in last chunk
  int chunk_size_;  // if != 0, use this size for new chunks

  static const int DEFAULT_INITIAL_CHUNK_SIZE = 4 * 1024;
  static const int MAX_CHUNK_SIZE = 512 * 1024;

  // Create new chunk and update mem_chunks_ and chunk_sizes_
  void AllocChunk(int size);
};

}

#endif

