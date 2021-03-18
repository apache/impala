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

// This file is partially a copy of Kudu BlockBloomFilter code. We wanted to reuse the
// existing implementation but also extend/modify some parts. This would not have been
// possible without modifying the Kudu source code in Impala
// (be/src/kudu/util/block_bloom_filter*). On the other hand, we have to maintain binary
// compatibility between the the Kudu code in Impala and actual Kudu code, so we decided
// against modifying the code in be/src/kudu/util/block_bloom_filter*.

#pragma once

#include "common/status.h"

namespace impala {

/// A Bloom filter implementation for Parquet Bloom filters. See
/// https://github.com/apache/parquet-format/blob/master/BloomFilter.md.
class ParquetBloomFilter {
 public:
  ParquetBloomFilter();
  ~ParquetBloomFilter();

  /// Initialises the directory (bitset) of the Bloom filter. The data is not copied and
  /// is not owned by this object. The buffer must be valid as long as this object uses
  /// it.
  /// If 'always_false_' is true, the implementation assumes that the directory is empty.
  /// If the directory contains any bytes other than zero, 'always_false_' should be
  /// false.
  Status Init(uint8_t* directory, size_t dir_size, bool always_false);

  void Insert(const uint64_t hash) noexcept;
  void HashAndInsert(const uint8_t* input, size_t size) noexcept;

  /// Finds an element in the BloomFilter, returning true if it is found and false (with
  /// high probabilty) if it is not.
  bool Find(const uint64_t hash) const noexcept;
  bool HashAndFind(const uint8_t* input, size_t size) const noexcept;

  const uint8_t* directory() const {
    return reinterpret_cast<const uint8_t*>(directory_);
  }

  // Size of the internal directory structure in bytes.
  int64_t directory_size() const {
    return 1ULL << log_space_bytes();
  }

  bool AlwaysFalse() const {
    return always_false_;
  }

  static int OptimalByteSize(const size_t ndv, const double fpp);

  // If we expect to fill a Bloom filter with 'ndv' different unique elements and we
  // want a false positive probability of less than 'fpp', then this is the log (base 2)
  // of the minimum number of bytes we need.
  static int MinLogSpace(size_t ndv, double fpp);

  // Returns the expected false positive rate for the given ndv and log_space_bytes.
  static double FalsePositiveProb(size_t ndv, int log_space_bytes);

  /// Hash the given bytes by the hashing algorithm of this ParquetBloomFilter.
  static uint64_t Hash(const uint8_t* input, size_t size);

  /// The maximum and minimum size of the Bloom filter in bytes. Constants taken from
  /// parquet-mr.
  static constexpr uint64_t MAX_BYTES = 128 * 1024 * 1024;
  static constexpr uint64_t MIN_BYTES = 64;

 private:
  // The BloomFilter is divided up into Buckets and each Bucket comprises of 8 BucketWords
  // of 4 bytes each.
  static constexpr uint64_t kBucketWords = 8;
  typedef uint32_t BucketWord;
  typedef BucketWord Bucket[kBucketWords];

  // log2(number of bits in a BucketWord)
  static constexpr int kLogBucketWordBits = 5;
  static constexpr BucketWord kBucketWordMask = (1 << kLogBucketWordBits) - 1;

  // log2(number of bytes in a bucket)
  static constexpr int kLogBucketByteSize = 5;
  // Bucket size in bytes.
  static constexpr size_t kBucketByteSize = 1UL << kLogBucketByteSize;

  static_assert((1 << kLogBucketWordBits) == std::numeric_limits<BucketWord>::digits,
      "BucketWord must have a bit-width that is be a power of 2, like 64 for uint64_t.");

  // Some constants used in hashing. #defined for efficiency reasons.
#define BLOOM_HASH_CONSTANTS                                             \
  0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU, 0x705495c7U, 0x2df1424bU, \
      0x9efc4947U, 0x5c6bfb31U

  // The block-based algorithm needs 8 odd SALT values to calculate eight indexes of bits
  // to set, one per 32-bit word.
  static constexpr uint32_t SALT[8]
      __attribute__((aligned(32))) = {BLOOM_HASH_CONSTANTS};

  // Detect at run-time whether CPU supports AVX2
  static bool has_avx2();

  // log_num_buckets_ is the log (base 2) of the number of buckets in the directory.
  int log_num_buckets_;

  // directory_mask_ is (1 << log_num_buckets_) - 1. It is precomputed for
  // efficiency reasons.
  uint32_t directory_mask_;

  Bucket* directory_;

  // Indicates whether the Bloom filter is empty and therefore all *Find* calls will
  // return false without further checks.
  bool always_false_;

  // Does the actual work of Insert(). bucket_idx is the index of the bucket to insert
  // into and 'hash' is the value passed to Insert().
  void BucketInsert(uint32_t bucket_idx, uint32_t hash) noexcept;

  bool BucketFind(uint32_t bucket_idx, uint32_t hash) const noexcept;

#ifdef USE_AVX2
  // A faster SIMD version of BucketInsert().
  void BucketInsertAVX2(const uint32_t bucket_idx, const uint32_t hash) noexcept
      __attribute__((__target__("avx2")));

  // A faster SIMD version of BucketFind().
  bool BucketFindAVX2(const uint32_t bucket_idx, const uint32_t hash) const noexcept
      __attribute__((__target__("avx2")));
#endif

  // Function pointers initialized in the constructor to avoid run-time cost in hot-path
  // of Find and Insert operations.
  decltype(&ParquetBloomFilter::BucketInsert) bucket_insert_func_ptr_;
  decltype(&ParquetBloomFilter::BucketFind) bucket_find_func_ptr_;

  // Returns amount of space used in log2 bytes.
  int log_space_bytes() const {
    return log_num_buckets_ + kLogBucketByteSize;
  }

  uint32_t DetermineBucketIdx(const uint64_t hash) const noexcept {
    const uint64_t hash_top_bits = hash >> 32;
    const uint64_t num_buckets = 1ULL << log_num_buckets_;
    const uint32_t i = (hash_top_bits * num_buckets) >> 32;
    return i;
  }

  DISALLOW_COPY_AND_ASSIGN(ParquetBloomFilter);
};

} // namespace impala
