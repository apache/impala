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

#include <stdint.h>

#include <iostream>
#include <limits>
#include <memory>
#include <vector>

#ifdef __aarch64__
#include <arm_neon.h>
#else
#include <immintrin.h>
#endif

#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/hash-util.h"
#include "util/sse-util.h"

using namespace std;
using namespace impala;

// Test hash functions that take integers as arguments and produce integers as the result.
//
// Machine Info: Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz
// 32 -> 32:                  Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                                 FNV               25.7       26     26.4         1X         1X         1X
//                             Zobrist               30.2     30.7     30.9      1.18X      1.18X      1.17X
//                             MultRot               42.6     43.4     43.4      1.66X      1.67X      1.64X
//                    MultiplyAddShift               42.4     43.2     43.2      1.65X      1.66X      1.64X
//                            Jenkins1               51.8       54       54      2.02X      2.08X      2.05X
//                            Jenkins2               66.2     67.4     67.5      2.58X      2.59X      2.56X
//                                 CRC               98.6      100      101      3.84X      3.85X      3.84X
//                       MultiplyShift                150      152      153      5.84X      5.86X      5.79X
//
// 32 x 4 -> 32 x 4:          Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//              (Multiple<Zobrist, 4>)               30.8     31.2     31.4         1X         1X         1X
//     (Multiple<MultiplyAddShift, 4>)               44.2       45       45      1.43X      1.44X      1.43X
//                  (Multiple<CRC, 4>)                118      120      121      3.84X      3.86X      3.85X
//        (Multiple<MultiplyShift, 4>)                156      159      159      5.07X       5.1X      5.08X
//                 MultiplyAddShift128               75.7     77.2     77.2      2.46X      2.48X      2.46X
//                    MultiplyShift128                128      131      133      4.16X      4.21X      4.23X
//
// 32 x 8 -> 32 x 8:          Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//              (Multiple<Zobrist, 8>)                 31     31.5     31.8         1X         1X         1X
//     (Multiple<MultiplyAddShift, 8>)               44.3     44.5     45.2      1.43X      1.41X      1.42X
//                  (Multiple<CRC, 8>)                121      123      123       3.9X       3.9X      3.88X
//        (Multiple<MultiplyShift, 8>)                158      159      160      5.11X      5.05X      5.04X
//                    Zobrist256simple               16.5     16.5     16.6     0.533X     0.524X     0.522X
//                    Zobrist256gather               18.8     19.2     19.4     0.608X     0.608X      0.61X
//                 MultiplyAddShift256                151      154      154      4.88X      4.88X      4.84X
//                    MultiplyShift256                209      212      212      6.73X      6.71X      6.67X

// Rotate 32 bits right by 'shift'. This is _rotr in the intel instrinsics, but that isn't
// usable on Clang yet. Fortunately, both GCC and Clang can optimize this to use the 'ror'
// instruction.
template<int SHIFT>
inline uint32_t RotateRight(uint32_t x) {
  static_assert(SHIFT > 0, "Only positive shifts are defined behavior and useful");
  static_assert(
      SHIFT < std::numeric_limits<decltype(x)>::digits, "This much shift is just 0");
  return (x << (std::numeric_limits<decltype(x)>::digits - SHIFT)) | (x >> SHIFT);
}

// Make a random uint32_t, avoiding the absent high bit and the low-entropy low bits
// produced by rand().
uint32_t MakeRandU32() {
  uint32_t result = (rand() >> 8) & 0xffff;
  result <<= 16;
  result |= (rand() >> 8) & 0xffff;
  return result;
}

// Almost universal hashing, M. Dietzfelbinger, T. Hagerup, J. Katajainen, and M.
// Penttonen, "A reliable randomized algorithm for the closest-pair problem".
inline void MultiplyShift(uint32_t* x) {
  static const uint32_t m = 0x61eaf8e9u;
  *x = *x * m;
}

// Like MultiplyShift, but using SSE's 128-bit SIMD registers to do 4 at once.
//
// TODO: Add the Intel intrinsics used in this function and the other functions in this
// file to sse-util.h so that these functions can be inlined.
#ifdef __aarch64__
inline void MultiplyShift128(uint32x4_t* x) {
  uint32_t* xx = (uint32_t *)x;
  const uint32x4_t m = vdupq_n_u32(0x61eaf8e9);
  uint32x4_t a = vld1q_u32(xx);
  a = vmulq_u32(a, m);
  vst1q_u32(xx, a);
}
#elif defined(__x86_64__)
inline void MultiplyShift128(__m128i* x) __attribute__((__target__("sse4.1")));
inline void MultiplyShift128(__m128i* x) {
  const __m128i m = _mm_set1_epi32(0x61eaf8e9);
  _mm_storeu_si128(x, _mm_mullo_epi32(_mm_loadu_si128(x), m));
}
#endif

// Like MultiplyShift, but using 256-bit SIMD registers to do 8 at once.
// Not inline, because it degrades the performance for unknown reasons.
#ifdef __aarch64__
void MultiplyShift256(uint32x4x2_t* x) {
  uint32_t* xx = (uint32_t*) x;
  const uint32x4_t m = vdupq_n_u32(0x61eaf8e9);
  uint32x4x2_t a = vld1q_u32_x2(xx);
  a.val[0] = vmulq_u32(a.val[0], m);
  a.val[1] = vmulq_u32(a.val[1], m);
  vst1q_u32_x2(xx, a);
}
#elif defined(__x86_64__)
void MultiplyShift256(__m256i* x) __attribute__((__target__("avx2")));
void MultiplyShift256(__m256i* x) {
  const __m256i m = _mm256_set1_epi32(0x61eaf8e9);
  _mm256_storeu_si256(x, _mm256_mullo_epi32(_mm256_loadu_si256(x), m));
}
#endif

// 2-independent hashing. M. Dietzfelbinger, "Universal hashing and k-wise independent
// random variables via integer arithmetic without primes"
inline void MultiplyAddShift(uint32_t* x) {
  static const uint64_t m = 0xa1f1bd3e020b4be0ull, a = 0x86b0426193d86e66ull;
  *x = (static_cast<uint64_t>(*x) * m + a) >> 32;
}

// Like MultiplyAddShift, but using 128-bit SIMD registers to do 4 at once.
#ifdef __aarch64__
inline void MultiplyAddShift128(uint32x4_t* x) {
  uint32_t* xx = (uint32_t*) x;
  const uint32x2_t mlo = vdup_n_u32(0x020b4be0);
  const uint32x4_t mhi = vdupq_n_u32(0xa1f1bd3e);
  const uint64x2_t a = vdupq_n_u64(0x86b0426193d86e66ull);
  uint32x4_t input = vld1q_u32(xx);
  uint32x4_t prod32easy = vmulq_u32(input, mhi);
  uint32x2_t input_lo = vget_low_u32 (input);
  uint32x2_t input_hi = vget_high_u32 (input);
  uint32x2_t input_even = vuzp1_u32(input_lo, input_hi);
  uint32x4_t input_odds = vcombine_u32(vuzp2_u32(input_lo, input_hi), input_even);
  uint64x2_t prod64_evens = vmull_u32(input_even, mlo);
  uint64x2_t prod64_odds = vmull_high_u32(input_odds, mhi);
  prod64_evens = vaddq_u64(a, prod64_evens);
  prod64_odds = vaddq_u64(a, prod64_odds);
  uint32x4_t prod64_evens_1 = vreinterpretq_u32_u64(prod64_evens);
  uint32x4_t prod64_odds_1 = vreinterpretq_u32_u64(prod64_odds);
  uint32x4_t prod32hard = vzip2q_u32(prod64_evens_1, prod64_odds_1);
  vst1q_u32(xx, vaddq_u32(prod32easy, prod32hard));
}
#else
inline void MultiplyAddShift128(__m128i* x) __attribute__((__target__("sse4.1")));
inline void MultiplyAddShift128(__m128i* x) {
  const auto m = _mm_set1_epi64x(0xa1f1bd3e020b4be0ull),
                mhi = _mm_set1_epi32(0xa1f1bd3e),
                a = _mm_set1_epi64x(0x86b0426193d86e66ull);
  auto input = _mm_loadu_si128(x);
  auto prod32easy = _mm_mullo_epi32(input, mhi);
  auto input_odds = _mm_srli_epi64(input, 32);
  auto prod64_evens = _mm_mul_epu32(input, m),
          prod64_odds = _mm_mul_epu32(input_odds, m);
  prod64_evens = _mm_add_epi64(a, prod64_evens);
  prod64_odds = _mm_add_epi64(a, prod64_odds);
  auto prod32hard = _mm_unpackhi_epi32(prod64_evens, prod64_odds);
  _mm_storeu_si128(x, _mm_add_epi32(prod32easy, prod32hard));
}
#endif

#ifndef __aarch64__
// Like MultiplyAddShift, but using AVX2's 256-bit SIMD registers to do 8 at once.
inline void MultiplyAddShift256(__m256i* x) __attribute__((__target__("avx2")));
inline void MultiplyAddShift256(__m256i* x) {
  const __m256i m = _mm256_set1_epi64x(0xa1f1bd3e020b4be0ull),
                mhi = _mm256_set1_epi32(0xa1f1bd3e),
                a = _mm256_set1_epi64x(0x86b0426193d86e66ull);
  __m256i input = _mm256_loadu_si256(x);
  __m256i prod32easy = _mm256_mullo_epi32(input, mhi);
  __m256i input_odds = _mm256_srli_epi64(input, 32);
  __m256i prod64_evens = _mm256_mul_epu32(input, m),
          prod64_odds = _mm256_mul_epu32(input_odds, m);
  prod64_evens = _mm256_add_epi64(a, prod64_evens);
  prod64_odds = _mm256_add_epi64(a, prod64_odds);
  __m256i prod32hard = _mm256_unpackhi_epi32(prod64_evens, prod64_odds);
  _mm256_storeu_si256(x, _mm256_add_epi32(prod32easy, prod32hard));
}
#endif

// From http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm:
inline void Jenkins1(int32_t* x) {
  *x = ~*x + (*x << 15); // x = (x << 15) - x - 1;
  *x = *x ^ RotateRight<12>(*x);
  *x = *x + (*x << 2);
  *x = *x ^ RotateRight<4>(*x);
  *x = *x * 2057; // x = (x + (x << 3)) + (x << 11);
  *x = *x ^ RotateRight<16>(*x);
}

// From http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm:
inline void Jenkins2(uint32_t* a) {
  *a = (*a + 0x7ed55d16) + (*a << 12);
  *a = (*a ^ 0xc761c23c) ^ (*a >> 19);
  *a = (*a + 0x165667b1) + (*a << 5);
  *a = (*a + 0xd3a2646c) ^ (*a << 9);
  *a = (*a + 0xfd7046c5) + (*a << 3);
  *a = (*a ^ 0xb55a4f09) ^ (*a >> 16);
}

// From http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm:
inline void MultRot(int32_t* key) {
  static const int32_t c2 = 0x27d4eb2d;  // a prime or an odd constant
  *key = (*key ^ 61) ^ RotateRight<16>(*key);
  *key = *key + (*key << 3);
  *key = *key ^ RotateRight<4>(*key);
  *key = *key * c2;
  *key = *key ^ RotateRight<15>(*key);
}

inline void CRC(uint32_t* x) {
  *x = SSE4_crc32_u32(*x, 0xab8ce2abu);
}

inline void FNV(uint32_t* key) {
  *key = HashUtil::FnvHash64to32(key, sizeof(*key), HashUtil::FNV_SEED);
}

// Zobrist hashing, also known as tabulation hashing or simple tabulation hashing, is an
// old technique that has been recently analyzed and found to be very good for a number of
// applications. See "The Power of Simple Tabulation Hashing", by Mihai Patrascu and
// Mikkel Thorup.

uint32_t ZOBRIST_DATA[4][256];

inline void Zobrist(uint32_t* key) {
  const uint8_t* key_chars = reinterpret_cast<const uint8_t*>(key);
  *key = ZOBRIST_DATA[0][key_chars[0]] ^ ZOBRIST_DATA[1][key_chars[1]]
      ^ ZOBRIST_DATA[2][key_chars[2]] ^ ZOBRIST_DATA[3][key_chars[3]];
}

#ifndef __aarch64__
// Like Zobrist, but uses AVX2's "gather" primatives to hash 8 values at once.
inline void Zobrist256gather(__m256i* key) __attribute__((__target__("avx2")));
inline void Zobrist256gather(__m256i* key) {
  const auto k = _mm256_loadu_si256(key);
  const auto low_mask = _mm256_set1_epi32(0xff);
  auto k0 = _mm256_and_si256(low_mask, k),
       k1 = _mm256_and_si256(low_mask, _mm256_srli_epi32(k, 8)),
       k2 = _mm256_and_si256(low_mask, _mm256_srli_epi32(k, 16)),
       k3 = _mm256_and_si256(low_mask, _mm256_srli_epi32(k, 24));
  k0 = _mm256_i32gather_epi32(reinterpret_cast<const int*>(ZOBRIST_DATA[0]), k0, 1);
  k1 = _mm256_i32gather_epi32(reinterpret_cast<const int*>(ZOBRIST_DATA[1]), k1, 1);
  k2 = _mm256_i32gather_epi32(reinterpret_cast<const int*>(ZOBRIST_DATA[2]), k2, 1);
  k3 = _mm256_i32gather_epi32(reinterpret_cast<const int*>(ZOBRIST_DATA[3]), k3, 1);
  auto k01 = _mm256_xor_si256(k0, k1), k23 = _mm256_xor_si256(k2, k3);
  _mm256_storeu_si256(key, _mm256_xor_si256(k01, k23));
}

// Like Zobrist256gather, but only uses AVX2's SIMD xor, not its gather.
inline void Zobrist256simple(uint32_t (*key)[8]) __attribute__((__target__("avx2")));
inline void Zobrist256simple(uint32_t (*key)[8]) {
  uint32_t row[4][8];
  const uint8_t (*key_chars)[8][4] = reinterpret_cast<const uint8_t (*)[8][4]>(key);
  for (int i = 0; i < 4; ++i) {
    for (int j = 0; j < 8; ++j) {
      row[i][j] = ZOBRIST_DATA[i][(*key_chars)[j][i]];
    }
  }
  auto result0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row[0])),
       result1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row[1])),
       result2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row[2])),
       result3 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row[3]));
  auto k01 = _mm256_xor_si256(result0, result1), k23 = _mm256_xor_si256(result2, result3);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(*key), _mm256_xor_si256(k01, k23));
}
#endif

// Perform one hash function the given number of times. This can sometimes auto-vectorize.
//
// TODO: We could also test the costs of running on non-contiguous uint32_t's. For
// instance, ExprValuesCache.expr_values_array_ might have multiple values to hash per
// input row.
template <void (*F)(uint32_t*), size_t N>
inline void Multiple(uint32_t (*x)[N]) {
  for (int i = 0; i < N; ++i) {
    F((*x) + i);
  }
}

// The size of the test data we run each hash function on:
static const size_t DATA_SIZE = 1 << 15;

template<typename T, void (*HASH)(T*)>
void Run(int batch_size, void* data) {
  T* d = reinterpret_cast<T*>(data);
  for (int i = 0; i < batch_size; ++i) {
    for (int j = 0; j < ((sizeof(uint32_t)) * DATA_SIZE) / sizeof(T); ++j) {
      HASH(&d[j]);
    }
  }
}

int main() {
  CpuInfo::Init();
  cout << endl
       << Benchmark::GetMachineInfo() << endl;

  unique_ptr<uint32_t[]> ud(new uint32_t[DATA_SIZE]);
  for (size_t i = 0; i < (DATA_SIZE); ++i) {
    ud[i] = MakeRandU32();
  }

  for (size_t i = 0; i < 4; ++i) {
    for (size_t j = 0; j < 256; ++j) {
      ZOBRIST_DATA[i][j] = MakeRandU32();
    }
  }

  Benchmark suite32("32 -> 32");

#define BENCH(T,x) AddBenchmark(#x, Run<T, x>, ud.get())

  suite32.BENCH(uint32_t, FNV);
  suite32.BENCH(uint32_t, Zobrist);
  suite32.BENCH(int32_t, MultRot);
  suite32.BENCH(uint32_t, MultiplyAddShift);
  suite32.BENCH(int32_t, Jenkins1);
  suite32.BENCH(uint32_t, Jenkins2);
  #ifdef __aarch64__
  suite32.BENCH(uint32_t, CRC);
  #else
  if (CpuInfo::IsSupported(CpuInfo::SSE4_2)) suite32.BENCH(uint32_t, CRC);
  #endif
  suite32.BENCH(uint32_t, MultiplyShift);

  cout << suite32.Measure() << endl;

  Benchmark suite32x4("32 x 4 -> 32 x 4");

  suite32x4.BENCH(uint32_t[4], (Multiple<Zobrist, 4>));
  suite32x4.BENCH(uint32_t[4], (Multiple<MultiplyAddShift, 4>));
  #ifdef __aarch64__
  suite32x4.BENCH(uint32_t[4], (Multiple<CRC, 4>));
  #else
  if (CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
    suite32x4.BENCH(uint32_t[4], (Multiple<CRC, 4>));
  }
  #endif

  suite32x4.BENCH(uint32_t[4], (Multiple<MultiplyShift, 4>));
  #ifdef __aarch64__
  suite32x4.BENCH(uint32x4_t, MultiplyAddShift128);
  suite32x4.BENCH(uint32x4_t, MultiplyShift128);
  #else
  if (CpuInfo::IsSupported(CpuInfo::SSE4_1)) {
    suite32x4.BENCH(__m128i, MultiplyAddShift128);
    suite32x4.BENCH(__m128i, MultiplyShift128);
  }
  #endif

  cout << suite32x4.Measure() << endl;

  Benchmark suite32x8("32 x 8 -> 32 x 8");

  suite32x8.BENCH(uint32_t[8], (Multiple<Zobrist, 8>));
  suite32x8.BENCH(uint32_t[8], (Multiple<MultiplyAddShift, 8>));
  suite32x8.BENCH(uint32_t[8], (Multiple<CRC, 8>));
  suite32x8.BENCH(uint32_t[8], (Multiple<MultiplyShift, 8>));
  #ifdef __aarch64__
  suite32x8.BENCH(uint32x4x2_t, MultiplyShift256);
  #else
  if (CpuInfo::IsSupported(CpuInfo::AVX2)) {
    suite32x8.BENCH(uint32_t[8], Zobrist256simple);
    suite32x8.BENCH(__m256i, Zobrist256gather);
    suite32x8.BENCH(__m256i, MultiplyAddShift256);
    suite32x8.BENCH(__m256i, MultiplyShift256);
  }
  #endif

  cout << suite32x8.Measure() << endl;

#undef BENCH
}
