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

#include "util/bloom-filter.h"

#include "runtime/exec-env.h"
#include "runtime/runtime-state.h"

using namespace std;

namespace impala {

constexpr uint32_t BloomFilter::REHASH[8] __attribute__((aligned(32)));

BloomFilter::BloomFilter(BufferPool::ClientHandle* client)
  : buffer_pool_client_(client) {}

BloomFilter::~BloomFilter() {
  DCHECK(directory_ == nullptr)
      << "Close() should have been called before the object is destroyed.";
}

Status BloomFilter::Init(const int log_bufferpool_space) {
  // Since log_bufferpool_space is in bytes, we need to convert it to the number of tiny
  // Bloom filters we will use.
  log_num_buckets_ = std::max(1, log_bufferpool_space - LOG_BUCKET_BYTE_SIZE);
  // Don't use log_num_buckets_ if it will lead to undefined behavior by a shift
  // that is too large.
  directory_mask_ = (1ull << std::min(63, log_num_buckets_)) - 1;
  // Since we use 32 bits in the arguments of Insert() and Find(), log_num_buckets_
  // must be limited.
  DCHECK(log_num_buckets_ <= 32) << "Bloom filter too large. log_bufferpool_space: "
                                 << log_bufferpool_space;
  const size_t alloc_size = directory_size();
  BufferPool* buffer_pool_ = ExecEnv::GetInstance()->buffer_pool();
  Close(); // Ensure that any previously allocated memory for directory_ is released.
  RETURN_IF_ERROR(
      buffer_pool_->AllocateBuffer(buffer_pool_client_, alloc_size, &buffer_handle_));
  directory_ = reinterpret_cast<Bucket*>(buffer_handle_.data());
  memset(directory_, 0, alloc_size);
  return Status::OK();
}

Status BloomFilter::Init(const TBloomFilter& thrift) {
  RETURN_IF_ERROR(Init(thrift.log_bufferpool_space));
  if (directory_ != nullptr && !thrift.always_false) {
    always_false_ = false;
    DCHECK_EQ(thrift.directory.size(), directory_size());
    memcpy(directory_, &thrift.directory[0], thrift.directory.size());
  }
  return Status::OK();
}

void BloomFilter::Close() {
  if (directory_ != nullptr) {
    BufferPool* buffer_pool_ = ExecEnv::GetInstance()->buffer_pool();
    buffer_pool_->FreeBuffer(buffer_pool_client_, &buffer_handle_);
    directory_ = nullptr;
  }
}

void BloomFilter::ToThrift(TBloomFilter* thrift) const {
  thrift->log_bufferpool_space = log_num_buckets_ + LOG_BUCKET_BYTE_SIZE;
  if (always_false_) {
    thrift->always_false = true;
    thrift->always_true = false;
    return;
  }
  thrift->directory.assign(reinterpret_cast<const char*>(directory_),
      static_cast<unsigned long>(directory_size()));
  thrift->always_false = false;
  thrift->always_true = false;
}

void BloomFilter::ToThrift(const BloomFilter* filter, TBloomFilter* thrift) {
  DCHECK(thrift != nullptr);
  if (filter == nullptr) {
    thrift->always_true = true;
    DCHECK_EQ(thrift->always_false, false);
    return;
  }
  filter->ToThrift(thrift);
}

// The SIMD reinterpret_casts technically violate C++'s strict aliasing rules. However, we
// compile with -fno-strict-aliasing.

void BloomFilter::BucketInsert(const uint32_t bucket_idx, const uint32_t hash) noexcept {
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

__m256i BloomFilter::MakeMask(const uint32_t hash) {
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

void BloomFilter::BucketInsertAVX2(
    const uint32_t bucket_idx, const uint32_t hash) noexcept {
  const __m256i mask = MakeMask(hash);
  __m256i* const bucket = &reinterpret_cast<__m256i*>(directory_)[bucket_idx];
  _mm256_store_si256(bucket, _mm256_or_si256(*bucket, mask));
  // For SSE compatibility, unset the high bits of each YMM register so SSE instructions
  // dont have to save them off before using XMM registers.
  _mm256_zeroupper();
}

bool BloomFilter::BucketFindAVX2(
    const uint32_t bucket_idx, const uint32_t hash) const noexcept {
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

bool BloomFilter::BucketFind(
    const uint32_t bucket_idx, const uint32_t hash) const noexcept {
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

namespace {
// Computes out[i] |= in[i] for the arrays 'in' and 'out' of length 'n' using AVX
// instructions. 'n' must be a multiple of 32.
void __attribute__((target("avx")))
OrEqualArrayAvx(size_t n, const char* __restrict__ in, char* __restrict__ out) {
  constexpr size_t AVX_REGISTER_BYTES = sizeof(__m256d);
  DCHECK_EQ(n % AVX_REGISTER_BYTES, 0) << "Invalid Bloom Filter directory size";
  const char* const in_end = in + n;
  for (; in != in_end; (in += AVX_REGISTER_BYTES), (out += AVX_REGISTER_BYTES)) {
    const double* double_in = reinterpret_cast<const double*>(in);
    double* double_out = reinterpret_cast<double*>(out);
    _mm256_storeu_pd(double_out,
        _mm256_or_pd(_mm256_loadu_pd(double_out), _mm256_loadu_pd(double_in)));
  }
}
} //namespace

void BloomFilter::Or(const TBloomFilter& in, TBloomFilter* out) {
  DCHECK(out != nullptr);
  DCHECK(&in != out);
  // These cases are impossible in current code. If they become possible in the future,
  // memory usage should be tracked accordingly.
  DCHECK(!out->always_false);
  DCHECK(!out->always_true);
  DCHECK(!in.always_true);
  if (in.always_false) return;
  DCHECK_EQ(in.log_bufferpool_space, out->log_bufferpool_space);
  DCHECK_EQ(in.directory.size(), out->directory.size())
      << "Equal log heap space " << in.log_bufferpool_space
      << ", but different directory sizes: " << in.directory.size() << ", "
      << out->directory.size();
  // The trivial loop out[i] |= in[i] should auto-vectorize with gcc at -O3, but it is not
  // written in a way that is very friendly to auto-vectorization. Instead, we manually
  // vectorize, increasing the speed by up to 56x.
  //
  // TODO: Tune gcc flags to auto-vectorize the trivial loop instead of hand-vectorizing
  // it. This might not be possible.
  if (CpuInfo::IsSupported(CpuInfo::AVX)) {
    OrEqualArrayAvx(in.directory.size(), &in.directory[0], &out->directory[0]);
  } else {
    const __m128i* simd_in = reinterpret_cast<const __m128i*>(&in.directory[0]);
    const __m128i* const simd_in_end =
        reinterpret_cast<const __m128i*>(&in.directory[0] + in.directory.size());
    __m128i* simd_out = reinterpret_cast<__m128i*>(&out->directory[0]);
    // in.directory has a size (in bytes) that is a multiple of 32. Since sizeof(__m128i)
    // == 16, we can do two _mm_or_si128's in each iteration without checking array
    // bounds.
    while (simd_in != simd_in_end) {
      for (int i = 0; i < 2; ++i, ++simd_in, ++simd_out) {
        _mm_storeu_si128(
            simd_out, _mm_or_si128(_mm_loadu_si128(simd_out), _mm_loadu_si128(simd_in)));
      }
    }
  }
}

// The following three methods are derived from
//
// fpp = (1 - exp(-BUCKET_WORDS * ndv/space))^BUCKET_WORDS
//
// where space is in bits.

size_t BloomFilter::MaxNdv(const int log_bufferpool_space, const double fpp) {
  DCHECK(log_bufferpool_space > 0 && log_bufferpool_space < 61);
  DCHECK(0 < fpp && fpp < 1);
  static const double ik = 1.0 / BUCKET_WORDS;
  return -1 * ik * (1ull << (log_bufferpool_space + 3)) * log(1 - pow(fpp, ik));
}

int BloomFilter::MinLogSpace(const size_t ndv, const double fpp) {
  static const double k = BUCKET_WORDS;
  if (0 == ndv) return 0;
  // m is the number of bits we would need to get the fpp specified
  const double m = -k * ndv / log(1 - pow(fpp, 1.0 / k));

  // Handle case where ndv == 1 => ceil(log2(m/8)) < 0.
  return max(0, static_cast<int>(ceil(log2(m / 8))));
}

double BloomFilter::FalsePositiveProb(const size_t ndv, const int log_bufferpool_space) {
  return pow(1 - exp((-1.0 * static_cast<double>(BUCKET_WORDS) * static_cast<double>(ndv))
                     / static_cast<double>(1ull << (log_bufferpool_space + 3))),
      BUCKET_WORDS);
}

} // namespace impala
