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

#ifndef IMPALA_UTIL_BLOOM_H
#define IMPALA_UTIL_BLOOM_H

#include <math.h>
#include <stdint.h>

#include <limits>

#include <immintrin.h>

#include "gutil/macros.h"

#include "gen-cpp/ImpalaInternalService_types.h"
#include "runtime/buffered-block-mgr.h"

namespace impala {

/// A BloomFilter stores sets of items and offers a query operation indicating whether or
/// not that item is in the set.  BloomFilters use much less space than other compact data
/// structures, but they are less accurate: for a small percentage of elements, the query
/// operation incorrectly returns true even when the item is not in the set.
///
/// When talking about Bloom filter size, rather than talking about 'size', which might be
/// ambiguous, we distinguish two different quantities:
///
/// 1. Space: the amount of heap memory used
///
/// 2. NDV: the number of unique items that have been inserted
///
/// BloomFilter is implemented using block Bloom filters from Putze et al.'s "Cache-,
/// Hash- and Space-Efficient Bloom Filters". The basic idea is to hash the item to a tiny
/// Bloom filter the size of a single cache line or smaller. This implementation sets 8
/// bits in each tiny Bloom filter. This provides a false positive rate near optimal for
/// between 5 and 15 bits per distinct value, which corresponds to false positive
/// probabilities between 0.1% (for 15 bits) and 10% (for 5 bits).
///
/// Our tiny BloomFilters are 32 bytes to take advantage of 32-byte SIMD in newer Intel
/// machines.
class BloomFilter {
 public:
  /// Consumes at most (1 << log_heap_space) bytes on the heap.
  explicit BloomFilter(const int log_heap_space);
  explicit BloomFilter(const TBloomFilter& thrift);
  ~BloomFilter();

  /// Representation of a filter which allows all elements to pass.
  static BloomFilter* const ALWAYS_TRUE_FILTER;

  /// Converts 'filter' to its corresponding Thrift representation. If the first argument
  /// is NULL, it is interpreted as a complete filter which contains all elements.
  static void ToThrift(const BloomFilter* filter, TBloomFilter* thrift);

  /// Adds an element to the BloomFilter. The function used to generate 'hash' need not
  /// have good uniformity, but it should have low collision probability. For instance, if
  /// the set of values is 32-bit ints, the identity function is a valid hash function for
  /// this Bloom filter, since the collision probability (the probability that two
  /// non-equal values will have the same hash value) is 0.
  void Insert(const uint32_t hash);

  /// Finds an element in the BloomFilter, returning true if it is found and false (with
  /// high probabilty) if it is not.
  bool Find(const uint32_t hash) const;

  /// Computes the logical OR of this filter with 'other' and stores the result in 'this'.
  void Or(const BloomFilter& other);

  /// As more distinct items are inserted into a BloomFilter, the false positive rate
  /// rises. MaxNdv() returns the NDV (number of distinct values) at which a BloomFilter
  /// constructed with (1 << log_heap_space) bytes of heap space hits false positive
  /// probabilty fpp.
  static size_t MaxNdv(const int log_heap_space, const double fpp);

  /// If we expect to fill a Bloom filter with 'ndv' different unique elements and we
  /// want a false positive probabilty of less than 'fpp', then this is the log (base 2)
  /// of the minimum number of bytes we need.
  static int MinLogSpace(const size_t ndv, const double fpp);

  /// Returns the expected false positive rate for the given ndv and log_heap_space
  static double FalsePositiveProb(const size_t ndv, const int log_heap_space);

  /// Returns amount of heap space used, in bytes
  int64_t GetHeapSpaceUsed() const { return sizeof(Bucket) * (1LL << log_num_buckets_); }

  static int64_t GetExpectedHeapSpaceUsed(uint32_t log_heap_size) {
    DCHECK_GE(log_heap_size, LOG_BUCKET_WORD_BITS);
    return sizeof(Bucket) * (1LL << (log_heap_size - LOG_BUCKET_WORD_BITS));
  }

 private:
  /// The BloomFilter is divided up into Buckets
  static const uint64_t BUCKET_WORDS = 8;
  typedef uint32_t BucketWord;

  // log2(number of bits in a BucketWord)
  static const int LOG_BUCKET_WORD_BITS = 5;
  static const BucketWord BUCKET_WORD_MASK = (1 << LOG_BUCKET_WORD_BITS) - 1;

  /// log2(number of bytes in a bucket)
  static const int LOG_BUCKET_BYTE_SIZE = 5;

  static_assert((1 << LOG_BUCKET_WORD_BITS) == std::numeric_limits<BucketWord>::digits,
      "BucketWord must have a bit-width that is be a power of 2, like 64 for uint64_t.");

  typedef BucketWord Bucket[BUCKET_WORDS];

  /// log_num_buckets_ is the log (base 2) of the number of buckets in the directory.
  const int log_num_buckets_;

  /// directory_mask_ is (1 << log_num_buckets_) - 1. It is precomputed for
  /// efficiency reasons.
  const uint32_t directory_mask_;

  Bucket* directory_;

  /// Does the actual work of Insert(). bucket_idx is the index of the bucket to insert
  /// into and 'hash' is the value passed to Insert().
  void BucketInsert(const uint32_t bucket_idx, const uint32_t hash);

  /// A faster SIMD version of BucketInsert().
  void BucketInsertAVX2(const uint32_t bucket_idx, const uint32_t hash)
      __attribute__((__target__("avx2")));

  /// BucketFind() and BucketFindAVX2() are just like BucketInsert() and
  /// BucketInsertAVX2(), but for Find().
  bool BucketFind(const uint32_t bucket_idx, const uint32_t hash) const;
  bool BucketFindAVX2(const uint32_t bucket_idx, const uint32_t hash) const
      __attribute__((__target__("avx2")));

  /// A helper function for the AVX2 methods. Turns a 32-bit hash into a 256-bit Bucket
  /// with 1 single 1-bit set in each 32-bit lane.
  static __m256i MakeMask(const uint32_t hash) __attribute__((__target__("avx2")));

  int64_t directory_size() const {
    return 1uLL << (log_num_buckets_ + LOG_BUCKET_BYTE_SIZE);
  }

  /// Serializes this filter as Thrift.
  void ToThrift(TBloomFilter* thrift) const;

  /// Some constants used in hashing. #defined for efficiency reasons.
#define IMPALA_BLOOM_HASH_CONSTANTS                                             \
  0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU, 0x705495c7U, 0x2df1424bU, \
      0x9efc4947U, 0x5c6bfb31U

  /// REHASH is used as 8 odd 32-bit unsigned ints.  See Dietzfelbinger et al.'s "A
  /// reliable randomized algorithm for the closest-pair problem".
  static constexpr uint32_t REHASH[8]
      __attribute__((aligned(32))) = {IMPALA_BLOOM_HASH_CONSTANTS};

  DISALLOW_COPY_AND_ASSIGN(BloomFilter);
};

// To set 8 bits in an 32-byte Bloom filter, we set one bit in each 32-bit uint32_t. This
// is a "split Bloom filter", and it has approximately the same false positive probability
// as standard a Bloom filter; See Mitzenmacher's "Bloom Filters and Such". It also has
// the advantage of requiring fewer random bits: log2(32) * 8 = 5 * 8 = 40 random bits for
// a split Bloom filter, but log2(256) * 8 = 64 random bits for a standard Bloom filter.

inline void BloomFilter::Insert(const uint32_t hash) {
  const uint32_t bucket_idx = HashUtil::Rehash32to32(hash) & directory_mask_;
  if (CpuInfo::IsSupported(CpuInfo::AVX2)) {
    BucketInsertAVX2(bucket_idx, hash);
  } else {
    BucketInsert(bucket_idx, hash);
  }
}

inline bool BloomFilter::Find(const uint32_t hash) const {
  const uint32_t bucket_idx = HashUtil::Rehash32to32(hash) & directory_mask_;
  if (CpuInfo::IsSupported(CpuInfo::AVX2)) {
    return BucketFindAVX2(bucket_idx, hash);
  } else {
    return BucketFind(bucket_idx, hash);
  }
}

// The SIMD reinterpret_casts technically violate C++'s strict aliasing rules. However, we
// compile with -fno-strict-aliasing.

inline void BloomFilter::BucketInsert(const uint32_t bucket_idx, const uint32_t hash) {
  // new_bucket will be all zeros except for eight 1-bits, one in each 32-bit word. It is
  // 16-byte aligned so it can be read as a __m128i using aligned SIMD loads in the second
  // part of this method.
  uint32_t new_bucket[8] __attribute__((aligned(16)));
  for (int i = 0; i < 8; ++i) {
    // Rehash 'hash' and use the top LOG_BUCKET_WORD_BITS bits, following Dietzfelbinger.
    new_bucket[i] =
        (REHASH[i] * hash) >> ((1 << LOG_BUCKET_WORD_BITS) - LOG_BUCKET_WORD_BITS);
    new_bucket[i] = 1U << new_bucket[i];
  }
  for (int i = 0; i < 2; ++i) {
    __m128i new_bucket_sse =
        _mm_load_si128(reinterpret_cast<__m128i*>(new_bucket + 4 * i));
    __m128i* existing_bucket = reinterpret_cast<__m128i*>(&directory_[bucket_idx][4 * i]);
    *existing_bucket = _mm_or_si128(*existing_bucket, new_bucket_sse);
  }
}

inline __m256i BloomFilter::MakeMask(const uint32_t hash) {
   const __m256i ones = _mm256_set1_epi32(1);
   const __m256i rehash = _mm256_setr_epi32(IMPALA_BLOOM_HASH_CONSTANTS);
  // Load hash into a YMM register, repeated eight times
  __m256i hash_data = _mm256_set1_epi32(hash);
  // Multiply-shift hashing ala Dietzfelbinger et al.: multiply 'hash' by eight different
  // odd constants, then keep the 5 most significant bits from each product.
  hash_data = _mm256_mullo_epi32(rehash, hash_data);
  hash_data = _mm256_srli_epi32(hash_data, 27);
  // Use these 5 bits to shift a single bit to a location in each 32-bit lane
  return _mm256_sllv_epi32(ones, hash_data);
}

inline void BloomFilter::BucketInsertAVX2(
    const uint32_t bucket_idx, const uint32_t hash) {
  const __m256i mask = MakeMask(hash);
  __m256i* const bucket = &reinterpret_cast<__m256i*>(directory_)[bucket_idx];
  _mm256_store_si256(bucket, _mm256_or_si256(*bucket, mask));
  // For SSE compatibility, unset the high bits of each YMM register so SSE instructions
  // dont have to save them off before using XMM registers.
  _mm256_zeroupper();
}

inline bool BloomFilter::BucketFindAVX2(
    const uint32_t bucket_idx, const uint32_t hash) const {
  const __m256i mask = MakeMask(hash);
  const __m256i bucket = reinterpret_cast<__m256i*>(directory_)[bucket_idx];
  // We should return true if 'bucket' has a one wherever 'mask' does. _mm256_testc_si256
  // takes the negation of its first argument and ands that with its second argument. In
  // our case, the result is zero everywhere iff there is a one in 'bucket' wherever
  // 'mask' is one. testc returns 1 if the result is 0 everywhere and returns 0 otherwise.
  const bool result = _mm256_testc_si256(bucket, mask);
  _mm256_zeroupper();
  return result;
}

inline bool BloomFilter::BucketFind(
    const uint32_t bucket_idx, const uint32_t hash) const {
  for (int i = 0; i < BUCKET_WORDS; ++i) {
    BucketWord hval =
        (REHASH[i] * hash) >> ((1 << LOG_BUCKET_WORD_BITS) - LOG_BUCKET_WORD_BITS);
    hval = 1U << hval;
    if (!(directory_[bucket_idx][i] & hval)) {
      return false;
    }
  }
  return true;
}

}  // namespace impala

#undef IMPALA_BLOOM_HASH_CONSTANTS
#endif  // IMPALA_UTIL_BLOOM_H
