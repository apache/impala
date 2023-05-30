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

#include "parquet-bloom-filter.h"

#ifdef __aarch64__
  #include "arm_neon.h"
#else
  #include <immintrin.h>
  #include <mm_malloc.h>
#endif

#include <climits>
#include <cmath>
#include <cstdint>

#include "gutil/strings/substitute.h"
#include "util/cpu-info.h"
#include "thirdparty/xxhash/xxhash.h"

using namespace std;
using strings::Substitute;

// TODO: Reconcile with legacy AVX support.
DEFINE_bool(disable_parquetbloomfilter_avx2, false,
    "Disable AVX2 operations in ParquetBloomFilter. This flag has no effect if the "
    "target CPU doesn't support AVX2 at run-time or ParquetBloomFilter was built with "
    "a compiler that doesn't support AVX2.");
DECLARE_bool(enable_legacy_avx_support);

namespace impala {

// This is needed to avoid undefined reference errors.
constexpr uint64_t ParquetBloomFilter::MAX_BYTES;
constexpr uint64_t ParquetBloomFilter::MIN_BYTES;
constexpr uint32_t ParquetBloomFilter::SALT[8] __attribute__((aligned(32)));

ParquetBloomFilter::ParquetBloomFilter() :
  log_num_buckets_(0),
  directory_mask_(0),
  directory_(nullptr),
  always_false_(false) {
#ifdef USE_AVX2
  if (has_avx2()) {
    bucket_insert_func_ptr_ = &ParquetBloomFilter::BucketInsertAVX2;
    bucket_find_func_ptr_ = &ParquetBloomFilter::BucketFindAVX2;
  } else {
    bucket_insert_func_ptr_ = &ParquetBloomFilter::BucketInsert;
    bucket_find_func_ptr_ = &ParquetBloomFilter::BucketFind;
  }
#else
  bucket_insert_func_ptr_ = &ParquetBloomFilter::BucketInsert;
  bucket_find_func_ptr_ = &ParquetBloomFilter::BucketFind;
#endif

  DCHECK(bucket_insert_func_ptr_);
  DCHECK(bucket_find_func_ptr_);
}

ParquetBloomFilter::~ParquetBloomFilter() {}

Status ParquetBloomFilter::Init(uint8_t* directory, size_t dir_size, bool always_false) {
  const int log_space_bytes = std::log2(dir_size);
  DCHECK_EQ(1ULL << log_space_bytes, dir_size);

  // Since log_space_bytes is in bytes, we need to convert it to the number of tiny
  // Bloom filters we will use.
  log_num_buckets_ = std::max(1, log_space_bytes - kLogBucketByteSize);
  // Since we use 32 bits in the arguments of Insert() and Find(), log_num_buckets_
  // must be limited.
  if (log_num_buckets_ > 32) {
    return Status(Substitute("Parquet Bloom filter too large. log_space_bytes: $0",
          log_space_bytes));
  }
  DCHECK_EQ(directory_size(), dir_size);
  DCHECK(directory != nullptr);
  directory_ = reinterpret_cast<Bucket*>(directory);

  if (always_false) {
    // Check the assumption that the directory is empty.
    DCHECK(std::all_of(directory, directory + dir_size,
          [](uint8_t byte) { return byte == 0; }));
    always_false_ = true;
  }

  // Don't use log_num_buckets_ if it will lead to undefined behavior by a shift
  // that is too large.
  directory_mask_ = (1ULL << log_num_buckets_) - 1;
  return Status::OK();
}

void ParquetBloomFilter::Insert(const uint64_t hash) noexcept {
  always_false_ = false;
  uint32_t idx = DetermineBucketIdx(hash);
  uint32_t hash_lower = hash;
  DCHECK(bucket_insert_func_ptr_);
  (this->*bucket_insert_func_ptr_)(idx, hash_lower);
}

void ParquetBloomFilter::HashAndInsert(const uint8_t* input, size_t size) noexcept {
  const uint64_t hash = Hash(input, size);
  Insert(hash);
}

bool ParquetBloomFilter::Find(const uint64_t hash) const noexcept {
  if (always_false_) return false;
  uint32_t idx = DetermineBucketIdx(hash);
  uint32_t hash_lower = hash;
  DCHECK(bucket_find_func_ptr_);
  return (this->*bucket_find_func_ptr_)(idx, hash_lower);
}

bool ParquetBloomFilter::HashAndFind(const uint8_t* input, size_t size) const noexcept {
  const uint64_t hash = Hash(input, size);
  return Find(hash);
}

int ParquetBloomFilter::OptimalByteSize(const size_t ndv, const double fpp) {
  DCHECK(fpp > 0.0 && fpp < 1.0)
      << "False positive probability should be less than 1.0 and greater than 0.0";
  const int min_log_space = MinLogSpace(ndv, fpp);
  const int min_space = std::pow(2, min_log_space);

  if (min_space < MIN_BYTES) return MIN_BYTES;
  if (min_space > MAX_BYTES) return MAX_BYTES;
  return min_space;
}

// This implements the false positive probability in Putze et al.'s "Cache-, hash-and
// space-efficient bloom filters", equation 3.
double ParquetBloomFilter::FalsePositiveProb(const size_t ndv, const int log_space_bytes) {
  static constexpr double kWordBits = 1 << kLogBucketWordBits;
  const double bytes = static_cast<double>(1L << log_space_bytes);
  if (ndv == 0) return 0.0;
  if (bytes <= 0) return 1.0;
  // This short-cuts a slowly-converging sum for very dense filters
  if (ndv / (bytes * CHAR_BIT) > 2) return 1.0;

  double result = 0;
  // lam is the usual parameter to the Poisson's PMF. Following the notation in the paper,
  // lam is B/c, where B is the number of bits in a bucket and c is the number of bits per
  // distinct value
  const double lam = kBucketWords * kWordBits / ((bytes * CHAR_BIT) / ndv);
  // Some of the calculations are done in log-space to increase numerical stability
  const double loglam = log(lam);

  // 750 iterations are sufficient to cause the sum to converge in all of the tests. In
  // other words, setting the iterations higher than 750 will give the same result as
  // leaving it at 750.
  static constexpr uint64_t kBloomFppIters = 750;
  for (uint64_t j = 0; j < kBloomFppIters; ++j) {
    // We start with the highest value of i, since the values we're adding to result are
    // mostly smaller at high i, and this increases accuracy to sum from the smallest
    // values up.
    const double i = static_cast<double>(kBloomFppIters - 1 - j);
    // The PMF of the Poisson distribution is lam^i * exp(-lam) / i!. In logspace, using
    // lgamma for the log of the factorial function:
    double logp = i * loglam - lam - lgamma(i + 1);
    // The f_inner part of the equation in the paper is the probability of a single
    // collision in the bucket. Since there are kBucketWords non-overlapping lanes in each
    // bucket, the log of this probability is:
    const double logfinner = kBucketWords * log(1.0 - pow(1.0 - 1.0 / kWordBits, i));
    // Here we are forced out of log-space calculations
    result += exp(logp + logfinner);
  }
  return (result > 1.0) ? 1.0 : result;
}

int ParquetBloomFilter::MinLogSpace(const size_t ndv, const double fpp) {
  int low = 0;
  int high = 64;
  while (high > low + 1) {
    int mid = (high + low) / 2;
    const double candidate = FalsePositiveProb(ndv, mid);
    if (candidate <= fpp) {
      high = mid;
    } else {
      low = mid;
    }
  }
  return high;
}

uint64_t ParquetBloomFilter::Hash(const uint8_t* input, size_t size) {
  static_assert(std::is_same<XXH64_hash_t, uint64_t>::value,
      "XXHash should return a 64 bit integer.");
  XXH64_hash_t hash = XXH64(input, size, 0 /* seed */);
  return hash;
}

#ifdef __aarch64__
// A static helper function for the arm64 methods. Turns a 32-bit hash into a 256-bit
// Bucket with 1 single 1-bit set in each 32-bit lane.
static inline ATTRIBUTE_ALWAYS_INLINE uint32x4x2_t MakeMask(const uint32_t hash) {
  const uint32x4_t ones = vdupq_n_u32(1);
  constexpr uint32_t c[8] = {BLOOM_HASH_CONSTANTS};
  const uint32x4x2_t rehash = vld1q_u32_x2(c);
  // Load hash, repeated 4 times.
  uint32x4_t hash_data = vdupq_n_u32(hash);

  // Multiply-shift hashing ala Dietzfelbinger et al.: multiply 'hash' by eight different
  // odd constants, then keep the 5 most significant bits from each product.
  int32x4x2_t t;
  t.val[0] = vreinterpretq_s32_u32(vshrq_n_u32(vmulq_u32(rehash.val[0], hash_data), 27));
  t.val[1] = vreinterpretq_s32_u32(vshrq_n_u32(vmulq_u32(rehash.val[1], hash_data), 27));

  // Use these 5 bits to shift a single bit to a location in each 32-bit lane
  uint32x4x2_t res;
  res.val[0] = vshlq_u32(ones, t.val[0]);
  res.val[1] = vshlq_u32(ones, t.val[1]);
  return res;
}

ATTRIBUTE_NO_SANITIZE_INTEGER
void ParquetBloomFilter::BucketInsert(const uint32_t bucket_idx,
    const uint32_t hash) noexcept {
  const uint32x4x2_t mask = MakeMask(hash);
  uint32x4x2_t* addr = &(reinterpret_cast<uint32x4x2_t*>(directory_)[bucket_idx]);
  uint32_t* bucket = reinterpret_cast<uint32_t*>(addr);
  uint32x4x2_t data = vld1q_u32_x2(bucket);
  data.val[0] = vorrq_u32(data.val[0], mask.val[0]);
  data.val[1] = vorrq_u32(data.val[1], mask.val[1]);
  vst1q_u32_x2(bucket, data);
}

ATTRIBUTE_NO_SANITIZE_INTEGER
bool ParquetBloomFilter::BucketFind(
    const uint32_t bucket_idx, const uint32_t hash) const noexcept {
  const uint32x4x2_t mask = MakeMask(hash);
  uint32x4x2_t* addr = &(reinterpret_cast<uint32x4x2_t*>(directory_)[bucket_idx]);
  uint32_t* bucket = reinterpret_cast<uint32_t*>(addr);
  uint32x4x2_t data = vld1q_u32_x2(bucket);
  // We should return true if 'bucket' has a one wherever 'mask' does.
  uint32x4_t t0 = vtstq_u32(data.val[0], mask.val[0]);
  uint32x4_t t1 = vtstq_u32(data.val[1], mask.val[1]);
  int64x2_t t = vreinterpretq_s64_u32(vandq_u32(t0, t1));
  int64_t a = vgetq_lane_s64(t, 0) & vgetq_lane_s64(t, 1);
  return a == -1;
}

#elif defined(__x86_64__)
ATTRIBUTE_NO_SANITIZE_INTEGER
void ParquetBloomFilter::BucketInsert(const uint32_t bucket_idx,
    const uint32_t hash) noexcept {
  // new_bucket will be all zeros except for eight 1-bits, one in each 32-bit word. It is
  // 16-byte aligned so it can be read as a __m128i using aligned SIMD loads in the second
  // part of this method.
  uint32_t new_bucket[kBucketWords] __attribute__((aligned(16)));
  for (int i = 0; i < kBucketWords; ++i) {
    // Rehash 'hash' and use the top kLogBucketWordBits bits, following Dietzfelbinger.
    new_bucket[i] = (SALT[i] * hash) >> ((1 << kLogBucketWordBits) - kLogBucketWordBits);
    new_bucket[i] = 1U << new_bucket[i];
  }
  for (int i = 0; i < 2; ++i) {
    __m128i new_bucket_sse = _mm_load_si128(
        reinterpret_cast<__m128i*>(new_bucket + 4 * i));
    __m128i* existing_bucket = reinterpret_cast<__m128i*>(
        &DCHECK_NOTNULL(directory_)[bucket_idx][4 * i]);
    *existing_bucket = _mm_or_si128(*existing_bucket, new_bucket_sse);
  }
}

ATTRIBUTE_NO_SANITIZE_INTEGER
bool ParquetBloomFilter::BucketFind(
    const uint32_t bucket_idx, const uint32_t hash) const noexcept {
  for (int i = 0; i < kBucketWords; ++i) {
    BucketWord hval = (SALT[i] * hash) >> (
        (1 << kLogBucketWordBits) - kLogBucketWordBits);
    hval = 1U << hval;
    if (!(DCHECK_NOTNULL(directory_)[bucket_idx][i] & hval)) {
      return false;
    }
  }
  return true;
}
#endif

bool ParquetBloomFilter::has_avx2() {
  return !FLAGS_disable_parquetbloomfilter_avx2 && !FLAGS_enable_legacy_avx_support
      && CpuInfo::IsSupported(CpuInfo::AVX2);
}

} // namespace impala
