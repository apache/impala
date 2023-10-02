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

#pragma once

#include <stdint.h>

#include <algorithm>
#include <cstring>
#include <limits>

#ifdef __x86_64__
#include <immintrin.h>
#endif

#include "codegen/impala-ir.h"
#include "common/compiler-util.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/macros.h"
#include "kudu/util/block_bloom_filter.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "util/cpu-info.h"
#include "util/hash-util.h"
#include "util/impala-bloom-filter-buffer-allocator.h"

namespace kudu {
namespace rpc {
class RpcController;
} // namespace rpc
} // namespace kudu

namespace impala {
class BloomFilter;
class BloomFilterPB;
} // namespace impala

// Need this forward declaration since we make bloom_filter_test_util::BfUnion() a friend
// function.
namespace bloom_filter_test_util {
void BfUnion(const impala::BloomFilter& x, const impala::BloomFilter& y,
    int64_t directory_size, bool* success, impala::BloomFilterPB* protobuf,
    std::string* directory);
} // namespace bloom_filter_test_util

// Need this forward declaration since we make either::TestData a friend struct.
namespace either {
struct TestData;
} // namespace either

namespace impala {

/// A BloomFilter stores sets of items and offers a query operation indicating whether
/// or not that item is in the set. The BloomFilter functionality is implemented in
/// kudu::BlockBloomFilter class (see source at be/src/kudu/util/block_bloom_filter.h),
/// which using block Bloom filters from Putze et al.'s "Cache-, Hash- and
/// Space-Efficient Bloom Filters".
/// This class is defined as thin wrapper around kudu::BlockBloomFilter.
/// Note: Kudu only support FastHash for BlockBloomFilter.
///       Since Fasthash is strictly better than Murmur Hash2, we do not
///       support Murmur Hash2 algorithm for Bloom filter.
class BloomFilter {
 public:
  /// Consumes at most (1 << log_bufferpool_space) bytes from the buffer pool client.
  /// 'client' should be a valid registered BufferPool Client and should have enough
  /// reservation to fulfill allocation for 'directory_'.
  explicit BloomFilter(BufferPool::ClientHandle* client);
  ~BloomFilter();

  /// Reset the filter state, allocate/reallocate and initialize the 'directory_'. All
  /// calls to Insert() and Find() should only be done between the calls to Init() and
  /// Close(). Init and Close are safe to call multiple times.
  Status Init(const int log_bufferpool_space, uint32_t hash_seed);
  Status Init(const BloomFilterPB& protobuf, const uint8_t* directory_in,
      size_t directory_in_size, uint32_t hash_seed);
  void Close();

  /// Representation of a filter which allows all elements to pass.
  static constexpr BloomFilter* const ALWAYS_TRUE_FILTER = NULL;

  /// Converts 'filter' to its corresponding Protobuf representation.
  /// If the first argument is NULL, it is interpreted as a complete filter which
  /// contains all elements.
  /// Also sets a sidecar on 'controller' containing the Bloom filter's directory.
  static void ToProtobuf(const BloomFilter* filter, kudu::rpc::RpcController* controller,
      BloomFilterPB* protobuf);

  bool AlwaysFalse() const {
    return block_bloom_filter_.always_false() && !not_always_false_;
  }

  void MarkNotAlwaysFalse() { not_always_false_ = true; }

  /// Adds an element to the BloomFilter. The function used to generate 'hash' need not
  /// have good uniformity, but it should have low collision probability. For instance, if
  /// the set of values is 32-bit ints, the identity function is a valid hash function for
  /// this Bloom filter, since the collision probability (the probability that two
  /// non-equal values will have the same hash value) is 0.
  void Insert(const uint32_t hash) noexcept;
  // Same as above for codegen
  void IR_ALWAYS_INLINE IrInsert(const uint32_t hash) noexcept;

  /// Finds an element in the BloomFilter, returning true if it is found and false (with
  /// high probabilty) if it is not.
  bool Find(const uint32_t hash) const noexcept;

  /// Computes the logical OR of this filter with 'other' and stores the result in this
  /// filter.
  void Or(const BloomFilter& other);

  /// Computes the logical OR of this filter with 'other' and stores the result in this
  /// filter. Different from Or(), the operation happen straight in the raw bytes rather
  /// than goes through kudu::BlockBloomFilter::Or() method. The logical OR operation
  /// still happen even if other.AlwaysFalse() is true.
  void RawOr(const BloomFilter& other);

  /// Computes the logical OR of this filter with 'in' and its corresponding
  /// 'input_slice' and stores the result in this filter.
  void Or(const BloomFilterPB& in, const kudu::Slice& input_slice);

  /// This function computes the logical OR of 'directory_in' with 'directory_out'
  /// and stores the result in 'directory_out'. 'in' must be a valid filter object
  /// (i.e. not ALWAYS_TRUE_FILTER).
  /// Additional checks are also performed to make sure the related fields of
  /// 'in' and 'out' are well-defined.
  static void Or(const BloomFilterPB& in, const uint8_t* directory_in, BloomFilterPB* out,
      uint8_t* directory_out, size_t directory_size);

  /// As more distinct items are inserted into a BloomFilter, the false positive rate
  /// rises. MaxNdv() returns the NDV (number of distinct values) at which a BloomFilter
  /// constructed with (1 << log_bufferpool_space) bytes of heap space hits false positive
  /// probabilty fpp.
  static size_t MaxNdv(const int log_bufferpool_space, const double fpp) {
    return kudu::BlockBloomFilter::MaxNdv(log_bufferpool_space, fpp);
  }

  /// If we expect to fill a Bloom filter with 'ndv' different unique elements and we
  /// want a false positive probabilty of less than 'fpp', then this is the log (base 2)
  /// of the minimum number of bytes we need.
  static int MinLogSpace(const size_t ndv, const double fpp) {
    return kudu::BlockBloomFilter::MinLogSpace(ndv, fpp);
  }

  /// Returns the expected false positive rate for the given ndv and log_bufferpool_space
  static double FalsePositiveProb(const size_t ndv, const int log_bufferpool_space) {
    return kudu::BlockBloomFilter::FalsePositiveProb(ndv, log_bufferpool_space);
  }

  /// Returns the amount of buffer pool space used (in bytes). A value of -1 means that
  /// 'directory_' has not been allocated which can happen if the object was just created
  /// and Init() hasn't been called or Init() failed or Close() was called on the object.
  int64_t GetBufferPoolSpaceUsed();

  static int64_t GetExpectedMemoryUsed(int log_heap_size) {
    return kudu::BlockBloomFilter::GetExpectedMemoryUsed(log_heap_size);
  }

  /// The following two functions set a sidecar on 'controller' containing the Bloom
  /// filter's directory. Two interfaces are provided because this function may be called
  /// in different contexts depending on whether or not the caller has access to an
  /// instantiated BloomFilter. It is also required that 'rpc_params' is neither an
  /// always false nor an always true Bloom filter when calling this function. Moreover,
  /// since we directly pass the reference to Bloom filter's directory when instantiating
  /// the corresponding RpcSidecar, we have to make sure that 'directory' is alive until
  /// the RPC is done.
  static void AddDirectorySidecar(BloomFilterPB* rpc_params,
      kudu::rpc::RpcController* controller, const char* directory,
      unsigned long directory_size);
  static void AddDirectorySidecar(BloomFilterPB* rpc_params,
      kudu::rpc::RpcController* controller, const string& directory);

  kudu::BlockBloomFilter* GetBlockBloomFilter() { return &block_bloom_filter_; }

 private:
  /// Buffer allocator is used by Kudu::BlockBloomFilter to allocate memory for
  /// Kudu::BlockBloomFilter.directory_.
  ImpalaBloomFilterBufferAllocator buffer_allocator_;

  /// Embedded Kudu BlockBloomFilter object
  kudu::BlockBloomFilter block_bloom_filter_;

  /// Flag to override block_bloom_filter_.always_false() in AlwaysFalse() method.
  bool not_always_false_ = false;

  /// Serializes this filter as Protobuf.
  void ToProtobuf(BloomFilterPB* protobuf, kudu::rpc::RpcController* controller) const;

  DISALLOW_COPY_AND_ASSIGN(BloomFilter);

  /// List 'BloomFilterTest_Protobuf_Test' as a friend class to run the backend
  /// test in 'bloom-filter-test.cc' since it has to access the private field of
  /// 'directory_' in BloomFilter.
  friend class BloomFilterTest_Protobuf_Test;

  /// List 'bloom_filter_test_util::BfUnion()' as a friend function to run the backend
  /// test in 'bloom-filter-test.cc' since it has to access the private field of
  /// 'directory_' in BloomFilter.
  friend void bloom_filter_test_util::BfUnion(const impala::BloomFilter& x,
      const impala::BloomFilter& y, int64_t directory_size, bool* success,
      impala::BloomFilterPB* protobuf, std::string* directory);

  /// List 'either::Test' as a friend struct to run the benchmark in
  /// 'bloom-filter-benchmark.cc' since it has to access the private field of
  /// 'directory_' in BloomFilter.
  friend struct either::TestData;
};

inline void ALWAYS_INLINE BloomFilter::Insert(const uint32_t hash) noexcept {
  block_bloom_filter_.Insert(hash);
}

inline bool ALWAYS_INLINE BloomFilter::Find(const uint32_t hash) const noexcept {
  return block_bloom_filter_.Find(hash);
}

} // namespace impala
