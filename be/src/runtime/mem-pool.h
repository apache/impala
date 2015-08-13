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


#ifndef IMPALA_RUNTIME_MEM_POOL_H
#define IMPALA_RUNTIME_MEM_POOL_H

#include <stdio.h>
#include <algorithm>
#include <vector>
#include <string>

#include "common/logging.h"
#include "util/runtime-profile.h"

namespace impala {

class MemTracker;

/// A MemPool maintains a list of memory chunks from which it allocates memory in
/// response to Allocate() calls;
/// Chunks stay around for the lifetime of the mempool or until they are passed on to
/// another mempool.
//
/// The caller registers a MemTracker with the pool; chunk allocations are counted
/// against that tracker and all of its ancestors. If chunks get moved between pools
/// during AcquireData() calls, the respective MemTrackers are updated accordingly.
/// Chunks freed up in the d'tor are subtracted from the registered trackers.
//
/// An Allocate() call will attempt to allocate memory from the chunk that was most
/// recently added; if that chunk doesn't have enough memory to
/// satisfy the allocation request, the free chunks are searched for one that is
/// big enough otherwise a new chunk is added to the list.
/// The current_chunk_idx_ always points to the last chunk with allocated memory.
/// In order to keep allocation overhead low, chunk sizes double with each new one
/// added, until they hit a maximum size.
//
///     Example:
///     MemPool* p = new MemPool();
///     for (int i = 0; i < 1024; ++i) {
/// returns 8-byte aligned memory (effectively 24 bytes):
///       .. = p->Allocate(17);
///     }
/// at this point, 17K have been handed out in response to Allocate() calls and
/// 28K of chunks have been allocated (chunk sizes: 4K, 8K, 16K)
/// We track total and peak allocated bytes. At this point they would be the same:
/// 28k bytes.  A call to Clear will return the allocated memory so
/// total_allocate_bytes_
/// becomes 0 while peak_allocate_bytes_ remains at 28k.
///     p->Clear();
/// the entire 1st chunk is returned:
///     .. = p->Allocate(4 * 1024);
/// 4K of the 2nd chunk are returned:
///     .. = p->Allocate(4 * 1024);
/// a new 20K chunk is created
///     .. = p->Allocate(20 * 1024);
//
///      MemPool* p2 = new MemPool();
/// the new mempool receives all chunks containing data from p
///      p2->AcquireData(p, false);
/// At this point p.total_allocated_bytes_ would be 0 while p.peak_allocated_bytes_
/// remains unchanged.
/// The one remaining (empty) chunk is released:
///    delete p;

class MemPool {
 public:
  /// Allocates mempool with fixed-size chunks of size 'chunk_size'.
  /// Chunk_size must be >= 0; 0 requests automatic doubling of chunk sizes,
  /// up to a limit.
  /// 'tracker' tracks the amount of memory allocated by this pool. Must not be NULL.
  MemPool(MemTracker* mem_tracker, int chunk_size = 0);

  /// Frees all chunks of memory and subtracts the total allocated bytes
  /// from the registered limits.
  ~MemPool();

  /// Allocates 8-byte aligned section of memory of 'size' bytes at the end
  /// of the the current chunk. Creates a new chunk if there aren't any chunks
  /// with enough capacity.
  uint8_t* Allocate(int size) {
    return Allocate<false>(size);
  }

  /// Same as Allocate() except the mem limit is checked before the allocation and
  /// this call will fail (returns NULL) if it does.
  /// The caller must handle the NULL case. This should be used for allocations
  /// where the size can be very big to bound the amount by which we exceed mem limits.
  uint8_t* TryAllocate(int size) {
    return Allocate<true>(size);
  }

  /// Returns 'byte_size' to the current chunk back to the mem pool. This can
  /// only be used to return either all or part of the previous allocation returned
  /// by Allocate().
  void ReturnPartialAllocation(int byte_size) {
    DCHECK_GE(byte_size, 0);
    DCHECK(current_chunk_idx_ != -1);
    ChunkInfo& info = chunks_[current_chunk_idx_];
    DCHECK(info.owns_data);
    DCHECK_GE(info.allocated_bytes, byte_size);
    info.allocated_bytes -= byte_size;
    total_allocated_bytes_ -= byte_size;
  }

  /// Makes all allocated chunks available for re-use, but doesn't delete any chunks.
  void Clear() {
    current_chunk_idx_ = -1;
    for (std::vector<ChunkInfo>::iterator chunk = chunks_.begin();
         chunk != chunks_.end(); ++chunk) {
      chunk->cumulative_allocated_bytes = 0;
      chunk->allocated_bytes = 0;
    }
    total_allocated_bytes_ = 0;
    DCHECK(CheckIntegrity(false));
  }

  /// Deletes all allocated chunks. FreeAll() or AcquireData() must be called for
  /// each mem pool
  void FreeAll();

  /// Absorb all chunks that hold data from src. If keep_current is true, let src hold on
  /// to its last allocated chunk that contains data.
  /// All offsets handed out by calls to GetCurrentOffset() for 'src' become invalid.
  void AcquireData(MemPool* src, bool keep_current);

  /// Diagnostic to check if memory is allocated from this mempool.
  /// Inputs:
  ///   ptr: start of memory block.
  ///   size: size of memory block.
  /// Returns true if memory block is in one of the chunks in this mempool.
  bool Contains(uint8_t* ptr, int size);

  std::string DebugString();

  int64_t total_allocated_bytes() const { return total_allocated_bytes_; }
  int64_t peak_allocated_bytes() const { return peak_allocated_bytes_; }
  int64_t total_reserved_bytes() const { return total_reserved_bytes_; }
  MemTracker* mem_tracker() { return mem_tracker_; }

  /// Return sum of chunk_sizes_.
  int64_t GetTotalChunkSizes() const;

  /// Return (data ptr, allocated bytes) pairs for all chunks owned by this mempool.
  void GetChunkInfo(std::vector<std::pair<uint8_t*, int> >* chunk_info);

  /// Print allocated bytes from all chunks.
  std::string DebugPrint();

  /// TODO: make a macro for doing this
  /// For C++/IR interop, we need to be able to look up types by name.
  static const char* LLVM_CLASS_NAME;

 private:
  friend class MemPoolTest;
  static const int DEFAULT_INITIAL_CHUNK_SIZE = 4 * 1024;

  struct ChunkInfo {
    bool owns_data;  // true if we eventually need to dealloc data
    uint8_t* data;
    int size;  // in bytes

    /// number of bytes allocated via Allocate() up to but excluding this chunk;
    /// *not* valid for chunks > current_chunk_idx_ (because that would create too
    /// much maintenance work if we have trailing unoccupied chunks)
    int cumulative_allocated_bytes;

    /// bytes allocated via Allocate() in this chunk
    int allocated_bytes;

    explicit ChunkInfo(int size);

    ChunkInfo()
      : owns_data(true),
        data(NULL),
        size(0),
        cumulative_allocated_bytes(0),
        allocated_bytes(0) {}
  };

  /// chunk from which we served the last Allocate() call;
  /// always points to the last chunk that contains allocated data;
  /// chunks 0..current_chunk_idx_ are guaranteed to contain data
  /// (chunks_[i].allocated_bytes > 0 for i: 0..current_chunk_idx_);
  /// -1 if no chunks present
  int current_chunk_idx_;

  /// chunk where last offset conversion (GetOffset() or GetDataPtr()) took place;
  /// -1 if those functions have never been called
  int last_offset_conversion_chunk_idx_;

  int chunk_size_;  // if != 0, use this size for new chunks

  /// sum of allocated_bytes_
  int64_t total_allocated_bytes_;

  /// Maximum number of bytes allocated from this pool at one time.
  int64_t peak_allocated_bytes_;

  /// sum of all bytes allocated in chunks_
  int64_t total_reserved_bytes_;

  std::vector<ChunkInfo> chunks_;

  /// The current and peak memory footprint of this pool. This is different from
  /// total allocated_bytes_ since it includes bytes in chunks that are not used.
  MemTracker* mem_tracker_;

  /// Find or allocated a chunk with at least min_size spare capacity and update
  /// current_chunk_idx_. Also updates chunks_, chunk_sizes_ and allocated_bytes_
  /// if a new chunk needs to be created.
  /// If check_limits is true, this call can fail (returns false) if adding a
  /// new chunk exceeds the mem limits.
  bool FindChunk(int min_size, bool check_limits);

  /// Check integrity of the supporting data structures; always returns true but DCHECKs
  /// all invariants.
  /// If 'current_chunk_empty' is false, checks that the current chunk contains data.
  bool CheckIntegrity(bool current_chunk_empty);

  int GetOffsetHelper(uint8_t* data);
  uint8_t* GetDataPtrHelper(int offset);

  /// Return offset to unoccpied space in current chunk.
  int GetFreeOffset() const {
    if (current_chunk_idx_ == -1) return 0;
    return chunks_[current_chunk_idx_].allocated_bytes;
  }

  template <bool CHECK_LIMIT_FIRST>
  uint8_t* Allocate(int size) {
    if (size == 0) return NULL;

    int num_bytes = ((size + 7) / 8) * 8;  // round up to nearest 8 bytes
    if (current_chunk_idx_ == -1
        || num_bytes + chunks_[current_chunk_idx_].allocated_bytes
          > chunks_[current_chunk_idx_].size) {
      if (CHECK_LIMIT_FIRST) {
        // If we couldn't allocate a new chunk, return NULL.
        if (!FindChunk(num_bytes, true)) return NULL;
      } else {
        FindChunk(num_bytes, false);
      }
    }
    ChunkInfo& info = chunks_[current_chunk_idx_];
    DCHECK(info.owns_data);
    uint8_t* result = info.data + info.allocated_bytes;
    DCHECK_LE(info.allocated_bytes + num_bytes, info.size);
    info.allocated_bytes += num_bytes;
    total_allocated_bytes_ += num_bytes;
    DCHECK_LE(current_chunk_idx_, chunks_.size() - 1);
    peak_allocated_bytes_ = std::max(total_allocated_bytes_, peak_allocated_bytes_);
    return result;
  }
};

}

#endif
