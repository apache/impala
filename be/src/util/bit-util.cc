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

#include "util/bit-util.h"

#ifdef __aarch64__
  #include "arm_neon.h"
#else
  #include <emmintrin.h>
  #include <immintrin.h>
#endif

#include <ostream>

namespace {
// ByteSwapScalarLoop is only used in bit-util.cc, so put it in this anonymous
// namespace
inline static void ByteSwapScalarLoop(const void* src, int len, void* dst) {
  //TODO: improve the performance of following code further using BSWAP intrinsic
  uint8_t* d = reinterpret_cast<uint8_t*>(dst);
  const uint8_t* s = reinterpret_cast<const uint8_t*>(src);
  for (int i = 0; i < len; ++i) d[i] = s[len - i - 1];
}
}

namespace impala {
void SimdByteSwap::ByteSwapScalar(const void* source, int len, void* dest) {
  uint8_t* dst = reinterpret_cast<uint8_t*>(dest);
  const uint8_t* src = reinterpret_cast<const uint8_t*>(source);
  switch (len) {
    case 1:
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src);
      return;
    case 2:
      *reinterpret_cast<uint16_t*>(dst) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint16_t*>(src));
      return;
    case 3:
      *reinterpret_cast<uint16_t*>(dst + 1) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint16_t*>(src));
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src + 2);
      return;
    case 4:
      *reinterpret_cast<uint32_t*>(dst) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint32_t*>(src));
      return;
    case 5:
      *reinterpret_cast<uint32_t*>(dst + 1) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint32_t*>(src));
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src + 4);
      return;
    case 6:
      *reinterpret_cast<uint32_t*>(dst + 2) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint32_t*>(src));
      *reinterpret_cast<uint16_t*>(dst) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint16_t*>(src + 4));
      return;
    case 7:
      *reinterpret_cast<uint32_t*>(dst + 3) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint32_t*>(src));
      *reinterpret_cast<uint16_t*>(dst + 1) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint16_t*>(src + 4));
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src + 6);
      return;
    case 8:
      *reinterpret_cast<uint64_t*>(dst) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      return;
    case 9:
      *reinterpret_cast<uint64_t*>(dst + 1) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src + 8);
      return;
    case 10:
      *reinterpret_cast<uint64_t*>(dst + 2) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint16_t*>(dst) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint16_t*>(src + 8));
      return;
    case 11:
      *reinterpret_cast<uint64_t*>(dst + 3) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint16_t*>(dst + 1) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint16_t*>(src + 8));
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src + 10);
      return;
    case 12:
      *reinterpret_cast<uint64_t*>(dst + 4) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint32_t*>(dst) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint32_t*>(src + 8));
      return;
    case 13:
      *reinterpret_cast<uint64_t*>(dst + 5) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint32_t*>(dst + 1) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint32_t*>(src + 8));
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src + 12);
      return;
    case 14:
      *reinterpret_cast<uint64_t*>(dst + 6) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint32_t*>(dst + 2) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint32_t*>(src + 8));
      *reinterpret_cast<uint16_t*>(dst) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint16_t*>(src + 12));
      return;
    case 15:
      *reinterpret_cast<uint64_t*>(dst + 7) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint32_t*>(dst + 3) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint32_t*>(src + 8));
      *reinterpret_cast<uint16_t*>(dst + 1) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint16_t*>(src + 12));
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src + 14);
      return;
    case 16:
      *reinterpret_cast<uint64_t*>(dst + 8) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint64_t*>(dst) =
          BitUtil::ByteSwap(*reinterpret_cast<const uint64_t*>(src + 8));
      return;
    default:
      // Revert to slow loop-based swap.
      ByteSwapScalarLoop(source, len, dest);
      return;
  }
}

#ifdef __aarch64__
const uint8x16_t mask128i = {15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
void SimdByteSwap::ByteSwap128(const uint8_t* src, uint8_t* dst) {
  vst1q_u8(dst, vqtbl1q_u8(vld1q_u8(src), mask128i));
}

void SimdByteSwap::ByteSwap256(const uint8_t* src, uint8_t* dst) {
  uint8x16x2_t a = vld1q_u8_x2(src);
  uint8x16x2_t res;
  res.val[1] = vqtbl1q_u8(a.val[0], mask128i);
  res.val[0] = vqtbl1q_u8(a.val[1], mask128i);
  vst1q_u8_x2(dst, res);
}

#else
// This constant is concluded from the definition of _mm_set_epi8;
// Refer this link for more details:
// https://software.intel.com/sites/landingpage/IntrinsicsGuide/
const __m128i mask128i = _mm_set_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
    13, 14, 15);
// ByteSwap 16 bytes using SSSE3 instructions.
__attribute__((target("ssse3")))
inline void SimdByteSwap::ByteSwap128(const uint8_t* src, uint8_t* dst) {
  _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), _mm_shuffle_epi8(
      _mm_loadu_si128(reinterpret_cast<const __m128i*>(src)), mask128i));
}

// ByteSwap 32 bytes using AVX2 instructions.
__attribute__((target("avx2")))
inline void SimdByteSwap::ByteSwap256(const uint8_t* src, uint8_t* dst) {
  // This constant is concluded from the definition of _mm256_set_epi8;
  // Refer this link for more details:
  // https://software.intel.com/sites/landingpage/IntrinsicsGuide/
  const __m256i mask256i = _mm256_set_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
    11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), _mm256_shuffle_epi8(
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src)), mask256i));
  const __m128i part1 = _mm_loadu_si128(reinterpret_cast<__m128i*>(dst));
  const __m128i part2 = _mm_loadu_si128(reinterpret_cast<__m128i*>(dst + 16));
  _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), part2);
  _mm_storeu_si128(reinterpret_cast<__m128i*>(dst + 16), part1);
  _mm256_zeroupper();
}
#endif

// Internal implementation of ByteSwapSimd
// TEMPLATE_DATA_WIDTH: 16byte or 32byte, corresponding to SSSE3 or AVX2 routine
// SIMD_FUNC: function pointer to ByteSwapSSE_Unit(16byte) or ByteSwapAVX_Unit(32byte)
// dest: the memory address of destination
// source: the memory address of source
// len: the number of bytes of input data
template <int TEMPLATE_DATA_WIDTH>
inline void SimdByteSwap::ByteSwapSimd(const void* source, const int len, void* dest) {
  DCHECK(TEMPLATE_DATA_WIDTH == 16 || TEMPLATE_DATA_WIDTH == 32)
    << "Only 16 or 32 are valid for TEMPLATE_DATA_WIDTH now.";
  /// Function pointer to SIMD ByteSwap functions
  void (*bswap_fptr)(const uint8_t* src, uint8_t* dst) = NULL;
  if (TEMPLATE_DATA_WIDTH == 16) {
    bswap_fptr = SimdByteSwap::ByteSwap128;
  } else if (TEMPLATE_DATA_WIDTH == 32) {
    bswap_fptr = SimdByteSwap::ByteSwap256;
  }

  const uint8_t* src = reinterpret_cast<const uint8_t*>(source);
  uint8_t* dst = reinterpret_cast<uint8_t*>(dest);
  src += len - TEMPLATE_DATA_WIDTH;
  int i = len - TEMPLATE_DATA_WIDTH;
  while (true) {
    bswap_fptr(src, dst);
    dst += TEMPLATE_DATA_WIDTH;
    if (i < TEMPLATE_DATA_WIDTH) break;
    i -= TEMPLATE_DATA_WIDTH;
    src -= TEMPLATE_DATA_WIDTH;
  }
  if (TEMPLATE_DATA_WIDTH > 16 && i >= 16) {
    src -= 16;
    SimdByteSwap::ByteSwap128(src, dst);
    i -= 16;
    dst += 16;
  }
  // Remaining bytes(<16) are dealt with scalar routine
  // TODO: improve the performance of following code further using pshufb intrinsic
  src -= i;
  SimdByteSwap::ByteSwapScalar(src, i, dst);
}

// Explicit instantiations for ByteSwapSSE_Unit and ByteSwapAVX2_Unit
template void SimdByteSwap::ByteSwapSimd<16>(const void* source, const int len, void* dest);
template void SimdByteSwap::ByteSwapSimd<32>(const void* source, const int len, void* dest);

void BitUtil::ByteSwap(void* dest, const void* source, int len) {
  // Branch selection according to current CPU capacity and input data length
  if (LIKELY(len < 16)) {
    SimdByteSwap::ByteSwapScalar(source, len, dest);
  }
#ifdef __aarch64__
  else if (len >= 32) {
    SimdByteSwap::ByteSwapSimd<32>(source, len, dest);
  }
  else {
    SimdByteSwap::ByteSwapSimd<16>(source, len, dest);
  }
#elif defined(__x86_64__)
  else if (len >= 32) {
    // AVX2 can only be used to process data whose size >= 32byte
    if (CpuInfo::IsSupported(CpuInfo::AVX2)) {
      SimdByteSwap::ByteSwapSimd<32>(source, len, dest);
    } else if (LIKELY(CpuInfo::IsSupported(CpuInfo::SSSE3))) {
      // SSSE3 support is more popular than AVX2.
      SimdByteSwap::ByteSwapSimd<16>(source, len, dest);
    } else {
      SimdByteSwap::ByteSwapScalar(source, len, dest);
    }
  }
  else {
    // SSSE3 can only be used to process data whose size >= 16byte
    // 16 <= len < 32
    if (LIKELY(CpuInfo::IsSupported(CpuInfo::SSSE3))) {
      SimdByteSwap::ByteSwapSimd<16>(source, len, dest);
    } else {
      SimdByteSwap::ByteSwapScalar(source, len, dest);
    }
  }
#endif
}

}
