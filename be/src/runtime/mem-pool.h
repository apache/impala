// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_RUNTIME_MEM_POOL_H
#define IMPALA_RUNTIME_MEM_POOL_H

#include <stdio.h>

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

#include "common/logging.h"
#include "util/bit-util.h"

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
/// In order to keep allocation overhead low, chunk sizes double with each new one
/// added, until they hit a maximum size.
///
/// Allocated chunks can be reused for new allocations if Clear() is called to free
/// all allocations or ReturnPartialAllocation() is called to return part of the last
/// allocation.
///
/// All chunks before 'current_chunk_idx_' have allocated memory, while all chunks
/// after 'current_chunk_idx_' are free. The chunk at 'current_chunk_idx_' may or may
/// not have allocated memory.
///
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
  /// 'tracker' tracks the amount of memory allocated by this pool. Must not be NULL.
  MemPool(MemTracker* mem_tracker);

  /// Frees all chunks of memory and subtracts the total allocated bytes
  /// from the registered limits.
  ~MemPool();

  /// Allocates a section of memory of 'size' bytes with DEFAULT_ALIGNMENT at the end
  /// of the the current chunk. Creates a new chunk if there aren't any chunks
  /// with enough capacity.
  uint8_t* Allocate(int64_t size) noexcept {
    return Allocate<false>(size, DEFAULT_ALIGNMENT);
  }

  /// Same as Allocate() except the mem limit is checked before the allocation and
  /// this call will fail (returns NULL) if it does.
  /// The caller must handle the NULL case. This should be used for allocations
  /// where the size can be very big to bound the amount by which we exceed mem limits.
  uint8_t* TryAllocate(int64_t size) noexcept {
    return Allocate<true>(size, DEFAULT_ALIGNMENT);
  }

  /// Same as TryAllocate() except a non-default alignment can be specified. It
  /// should be a power-of-two in [1, alignof(std::max_align_t)].
  uint8_t* TryAllocateAligned(int64_t size, int alignment) noexcept {
    DCHECK_GE(alignment, 1);
    DCHECK_LE(alignment, alignof(std::max_align_t));
    DCHECK_EQ(BitUtil::RoundUpToPowerOfTwo(alignment), alignment);
    return Allocate<true>(size, alignment);
  }

  /// Returns 'byte_size' to the current chunk back to the mem pool. This can
  /// only be used to return either all or part of the previous allocation returned
  /// by Allocate().
  void ReturnPartialAllocation(int64_t byte_size) {
    DCHECK_GE(byte_size, 0);
    DCHECK(current_chunk_idx_ != -1);
    ChunkInfo& info = chunks_[current_chunk_idx_];
    DCHECK_GE(info.allocated_bytes, byte_size);
    info.allocated_bytes -= byte_size;
    total_allocated_bytes_ -= byte_size;
  }

  /// Return a dummy pointer for zero-length allocations.
  static uint8_t* EmptyAllocPtr() {
    return reinterpret_cast<uint8_t*>(&zero_length_region_);
  }

  /// Makes all allocated chunks available for re-use, but doesn't delete any chunks.
  void Clear();

  /// Deletes all allocated chunks. FreeAll() or AcquireData() must be called for
  /// each mem pool
  void FreeAll();

  /// Absorb all chunks that hold data from src. If keep_current is true, let src hold on
  /// to its last allocated chunk that contains data.
  /// All offsets handed out by calls to GetCurrentOffset() for 'src' become invalid.
  void AcquireData(MemPool* src, bool keep_current);

  std::string DebugString();

  int64_t total_allocated_bytes() const { return total_allocated_bytes_; }
  int64_t peak_allocated_bytes() const { return peak_allocated_bytes_; }
  int64_t total_reserved_bytes() const { return total_reserved_bytes_; }
  MemTracker* mem_tracker() { return mem_tracker_; }

  /// Return sum of chunk_sizes_.
  int64_t GetTotalChunkSizes() const;

  /// TODO: make a macro for doing this
  /// For C++/IR interop, we need to be able to look up types by name.
  static const char* LLVM_CLASS_NAME;

  static const int DEFAULT_ALIGNMENT = 8;

 private:
  friend class MemPoolTest;
  static const int INITIAL_CHUNK_SIZE = 4 * 1024;

  /// The maximum size of chunk that should be allocated. Allocations larger than this
  /// size will get their own individual chunk.
  static const int MAX_CHUNK_SIZE = 1024 * 1024;

  struct ChunkInfo {
    uint8_t* data; // Owned by the ChunkInfo.
    int64_t size;  // in bytes

    /// bytes allocated via Allocate() in this chunk
    int64_t allocated_bytes;

    explicit ChunkInfo(int64_t size, uint8_t* buf);

    ChunkInfo()
      : data(NULL),
        size(0),
        allocated_bytes(0) {}
  };

  /// A static field used as non-NULL pointer for zero length allocations. NULL is
  /// reserved for allocation failures. It must be as aligned as max_align_t for
  /// TryAllocateAligned().
  static uint32_t zero_length_region_ alignas(std::max_align_t);

  /// chunk from which we served the last Allocate() call;
  /// always points to the last chunk that contains allocated data;
  /// chunks 0..current_chunk_idx_ - 1 are guaranteed to contain data
  /// (chunks_[i].allocated_bytes > 0 for i: 0..current_chunk_idx_ - 1);
  /// chunks after 'current_chunk_idx_' are "free chunks" that contain no data.
  /// -1 if no chunks present
  int current_chunk_idx_;

  /// The size of the next chunk to allocate.
  int next_chunk_size_;

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
  bool FindChunk(int64_t min_size, bool check_limits) noexcept;

  /// Check integrity of the supporting data structures; always returns true but DCHECKs
  /// all invariants.
  /// If 'check_current_chunk_empty' is true, checks that the current chunk contains no
  /// data. Otherwise the current chunk can be either empty or full.
  bool CheckIntegrity(bool check_current_chunk_empty);

  /// Return offset to unoccupied space in current chunk.
  int64_t GetFreeOffset() const {
    if (current_chunk_idx_ == -1) return 0;
    return chunks_[current_chunk_idx_].allocated_bytes;
  }

  template <bool CHECK_LIMIT_FIRST>
  uint8_t* Allocate(int64_t size, int alignment) noexcept {
    DCHECK_GE(size, 0);
    if (UNLIKELY(size == 0)) return reinterpret_cast<uint8_t*>(&zero_length_region_);

    bool fits_in_chunk = false;
    if (current_chunk_idx_ != -1) {
      int64_t aligned_allocated_bytes = BitUtil::RoundUpToPowerOf2(
          chunks_[current_chunk_idx_].allocated_bytes, alignment);
      if (aligned_allocated_bytes + size <= chunks_[current_chunk_idx_].size) {
        // Ensure the requested alignment is respected.
        total_allocated_bytes_ +=
            aligned_allocated_bytes - chunks_[current_chunk_idx_].allocated_bytes;
        chunks_[current_chunk_idx_].allocated_bytes = aligned_allocated_bytes;
        fits_in_chunk = true;
      }
    }

    if (!fits_in_chunk) {
      // If we couldn't allocate a new chunk, return NULL. malloc() guarantees alignment
      // of alignof(std::max_align_t), so we do not need to do anything additional to
      // guarantee alignment.
      static_assert(
          INITIAL_CHUNK_SIZE >= alignof(std::max_align_t), "Min chunk size too low");
      if (UNLIKELY(!FindChunk(size, CHECK_LIMIT_FIRST))) return NULL;
    }

    ChunkInfo& info = chunks_[current_chunk_idx_];
    uint8_t* result = info.data + info.allocated_bytes;
    DCHECK_LE(info.allocated_bytes + size, info.size);
    info.allocated_bytes += size;
    total_allocated_bytes_ += size;
    DCHECK_LE(current_chunk_idx_, chunks_.size() - 1);
    peak_allocated_bytes_ = std::max(total_allocated_bytes_, peak_allocated_bytes_);
    return result;
  }
};

// Stamp out templated implementations here so they're included in IR module
template uint8_t* MemPool::Allocate<false>(int64_t size, int alignment) noexcept;
template uint8_t* MemPool::Allocate<true>(int64_t size, int alignment) noexcept;
}

#endif
