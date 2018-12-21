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

#include <stdlib.h>
#include <stdio.h>
#include <limits.h>

#include <algorithm>
#include <iostream>
#include <numeric>

#include <boost/utility.hpp>

#include "testutil/gtest-util.h"
#include "util/bit-util.h"
#include "util/cpu-info.h"

#include "common/names.h"
#include "runtime/multi-precision.h"

namespace impala {

TEST(BitUtil, UnsignedWidth) {
  EXPECT_EQ(BitUtil::UnsignedWidth<signed char>(), 7);
  EXPECT_EQ(BitUtil::UnsignedWidth<unsigned char>(), 8);
  EXPECT_EQ(BitUtil::UnsignedWidth<volatile long long>(), 63);
  EXPECT_EQ(BitUtil::UnsignedWidth<unsigned long long&>(), 64);
  EXPECT_EQ(BitUtil::UnsignedWidth<const int128_t&>(), 127);
  EXPECT_EQ(BitUtil::UnsignedWidth<const volatile unsigned __int128&>(), 128);
}

TEST(BitUtil, Sign) {
  EXPECT_EQ(BitUtil::Sign<int>(0), 1);
  EXPECT_EQ(BitUtil::Sign<int>(1), 1);
  EXPECT_EQ(BitUtil::Sign<int>(-1), -1);
  EXPECT_EQ(BitUtil::Sign<int>(200), 1);
  EXPECT_EQ(BitUtil::Sign<int>(-200), -1);
  EXPECT_EQ(BitUtil::Sign<unsigned int>(0), 1);
  EXPECT_EQ(BitUtil::Sign<unsigned int>(1), 1);
  EXPECT_EQ(BitUtil::Sign<unsigned int>(-1U), 1);
  EXPECT_EQ(BitUtil::Sign<unsigned int>(200), 1);
  EXPECT_EQ(BitUtil::Sign<unsigned int>(-200), 1);
  EXPECT_EQ(BitUtil::Sign<int128_t>(0), 1);
  EXPECT_EQ(BitUtil::Sign<int128_t>(1), 1);
  EXPECT_EQ(BitUtil::Sign<int128_t>(-1), -1);
  EXPECT_EQ(BitUtil::Sign<int128_t>(200), 1);
  EXPECT_EQ(BitUtil::Sign<int128_t>(-200), -1);
}

TEST(BitUtil, Ceil) {
  EXPECT_EQ(BitUtil::Ceil(0, 1), 0);
  EXPECT_EQ(BitUtil::Ceil(1, 1), 1);
  EXPECT_EQ(BitUtil::Ceil(1, 2), 1);
  EXPECT_EQ(BitUtil::Ceil(1, 8), 1);
  EXPECT_EQ(BitUtil::Ceil(7, 8), 1);
  EXPECT_EQ(BitUtil::Ceil(8, 8), 1);
  EXPECT_EQ(BitUtil::Ceil(9, 8), 2);
  EXPECT_EQ(BitUtil::Ceil(9, 9), 1);
  EXPECT_EQ(BitUtil::Ceil(10000000000, 10), 1000000000);
  EXPECT_EQ(BitUtil::Ceil(10, 10000000000), 1);
  EXPECT_EQ(BitUtil::Ceil(100000000000, 10000000000), 10);
}

TEST(BitUtil, RoundUp) {
  EXPECT_EQ(BitUtil::RoundUp(0, 1), 0);
  EXPECT_EQ(BitUtil::RoundUp(1, 1), 1);
  EXPECT_EQ(BitUtil::RoundUp(1, 2), 2);
  EXPECT_EQ(BitUtil::RoundUp(6, 2), 6);
  EXPECT_EQ(BitUtil::RoundUp(7, 3), 9);
  EXPECT_EQ(BitUtil::RoundUp(9, 9), 9);
  EXPECT_EQ(BitUtil::RoundUp(10000000001, 10), 10000000010);
  EXPECT_EQ(BitUtil::RoundUp(10, 10000000000), 10000000000);
  EXPECT_EQ(BitUtil::RoundUp(100000000000, 10000000000), 100000000000);
}

TEST(BitUtil, RoundDown) {
  EXPECT_EQ(BitUtil::RoundDown(0, 1), 0);
  EXPECT_EQ(BitUtil::RoundDown(1, 1), 1);
  EXPECT_EQ(BitUtil::RoundDown(1, 2), 0);
  EXPECT_EQ(BitUtil::RoundDown(6, 2), 6);
  EXPECT_EQ(BitUtil::RoundDown(7, 3), 6);
  EXPECT_EQ(BitUtil::RoundDown(9, 9), 9);
  EXPECT_EQ(BitUtil::RoundDown(10000000001, 10), 10000000000);
  EXPECT_EQ(BitUtil::RoundDown(10, 10000000000), 0);
  EXPECT_EQ(BitUtil::RoundDown(100000000000, 10000000000), 100000000000);
}

TEST(BitUtil, Popcount) {
  EXPECT_EQ(BitUtil::Popcount(BOOST_BINARY(0 1 0 1 0 1 0 1)), 4);
  EXPECT_EQ(BitUtil::PopcountNoHw(BOOST_BINARY(0 1 0 1 0 1 0 1)), 4);
  EXPECT_EQ(BitUtil::Popcount(BOOST_BINARY(1 1 1 1 0 1 0 1)), 6);
  EXPECT_EQ(BitUtil::PopcountNoHw(BOOST_BINARY(1 1 1 1 0 1 0 1)), 6);
  EXPECT_EQ(BitUtil::Popcount(BOOST_BINARY(1 1 1 1 1 1 1 1)), 8);
  EXPECT_EQ(BitUtil::PopcountNoHw(BOOST_BINARY(1 1 1 1 1 1 1 1)), 8);
  EXPECT_EQ(BitUtil::Popcount(0), 0);
  EXPECT_EQ(BitUtil::PopcountNoHw(0), 0);
}

TEST(BitUtil, TrailingBits) {
  EXPECT_EQ(BitUtil::TrailingBits(BOOST_BINARY(1 1 1 1 1 1 1 1), 0), 0);
  EXPECT_EQ(BitUtil::TrailingBits(BOOST_BINARY(1 1 1 1 1 1 1 1), 1), 1);
  EXPECT_EQ(BitUtil::TrailingBits(BOOST_BINARY(1 1 1 1 1 1 1 1), 64),
            BOOST_BINARY(1 1 1 1 1 1 1 1));
  EXPECT_EQ(BitUtil::TrailingBits(BOOST_BINARY(1 1 1 1 1 1 1 1), 100),
            BOOST_BINARY(1 1 1 1 1 1 1 1));
  EXPECT_EQ(BitUtil::TrailingBits(0, 1), 0);
  EXPECT_EQ(BitUtil::TrailingBits(0, 64), 0);
  EXPECT_EQ(BitUtil::TrailingBits(1ULL << 63, 0), 0);
  EXPECT_EQ(BitUtil::TrailingBits(1ULL << 63, 63), 0);
  EXPECT_EQ(BitUtil::TrailingBits(1ULL << 63, 64), 1ULL << 63);
}

// Test different SIMD functionality units with an input/output buffer.
// CpuFlag parameter indicates SIMD routine to be tested:
//   CpuInfo::SSSE3 for ByteSwapSSE_Unit;
//   CpuInfo::AVX2 for ByteSwapAVX2_Unit;
void TestByteSwapSimd_Unit(const int64_t CpuFlag) {
  void (*bswap_fptr)(const uint8_t* src, uint8_t* dst) = NULL;
  int buf_size = 0;
  if (CpuFlag == CpuInfo::SSSE3) {
    buf_size = 16;
    bswap_fptr = SimdByteSwap::ByteSwap128;
  } else {
    static const int64_t AVX2_MASK = CpuInfo::AVX2;
    ASSERT_EQ(CpuFlag, AVX2_MASK);
    buf_size = 32;
    bswap_fptr = SimdByteSwap::ByteSwap256;
  }

  DCHECK(bswap_fptr != NULL);
  // IMPALA-4058: test that bswap_fptr works when it reads to o writes from memory with
  // any alignment.
  static const size_t MAX_ALIGNMENT = 64;
  uint8_t src_buf[buf_size + MAX_ALIGNMENT];
  uint8_t dst_buf[buf_size + MAX_ALIGNMENT];
  std::iota(src_buf, src_buf + buf_size + MAX_ALIGNMENT, 1);
  for (size_t i = 0; i < MAX_ALIGNMENT; ++i) {
    for (size_t j = 0; j < MAX_ALIGNMENT; ++j) {
      std::fill(dst_buf, dst_buf + buf_size + MAX_ALIGNMENT, 0);
      bswap_fptr(src_buf + i, dst_buf + j);

      // Validate the swap results.
      for (int k = 0; k < buf_size; ++k) {
        EXPECT_EQ(dst_buf[k + j], 1 + i + buf_size - k - 1);
        EXPECT_EQ(dst_buf[k + j], src_buf[i + buf_size - k - 1]);
      }
    }
  }
}

// Test the logic of ByteSwapSimd control flow using specified SIMD routine with an
// input/output buffer.
// CpuFlag parameter indicates SIMD routine to be tested:
//   CpuInfo::SSSE3 for SimdByteSwap::ByteSwapSSE_Unit;
//   CpuInfo::AVX2 for SimdByteSwap::ByteSwapAVX2_Unit;
//   CpuFlag == 0 for BitUtil::ByteSwap;
// buf_size parameter indicates the size of input/output buffer.
void TestByteSwapSimd(const int64_t CpuFlag, const int buf_size) {
  if (buf_size <= 0) return;
  uint8_t src_buf[buf_size];
  uint8_t dst_buf[buf_size];
  std::iota(src_buf, src_buf + buf_size, 0);

  int start_size = 0;
  if (CpuFlag == CpuInfo::SSSE3) {
    start_size = 16;
  } else if (CpuFlag == CpuInfo::AVX2) {
    start_size = 32;
  }

  for (int i = start_size; i < buf_size; ++i) {
    // Initialize dst buffer and swap i bytes.
    memset(dst_buf, 0, buf_size);
    if (CpuFlag == CpuInfo::SSSE3) {
      SimdByteSwap::ByteSwapSimd<16>(src_buf, i, dst_buf);
    } else if (CpuFlag == CpuInfo::AVX2) {
      SimdByteSwap::ByteSwapSimd<32>(src_buf, i, dst_buf);
    } else {
      // CpuFlag == 0: test the internal logic of BitUtil::ByteSwap
      ASSERT_EQ(CpuFlag, 0);
      BitUtil::ByteSwap(dst_buf, src_buf, i);
    }

    // Validate the swap results.
    for (int j = 0; j < i; ++j) {
      EXPECT_EQ(dst_buf[j], i - j - 1);
      EXPECT_EQ(dst_buf[j], src_buf[i - j - 1]);
    }
    // Check that the dst buffer is otherwise unmodified.
    for (int j = i; j < buf_size; ++j) {
      EXPECT_EQ(dst_buf[j], 0);
    }
  }
}

TEST(BitUtil, ByteSwap) {
  EXPECT_EQ(BitUtil::ByteSwap(static_cast<uint32_t>(0)), 0);
  EXPECT_EQ(BitUtil::ByteSwap(static_cast<uint32_t>(0x11223344)), 0x44332211);

  EXPECT_EQ(BitUtil::ByteSwap(static_cast<int32_t>(0)), 0);
  EXPECT_EQ(BitUtil::ByteSwap(static_cast<int32_t>(0x11223344)), 0x44332211);

  EXPECT_EQ(BitUtil::ByteSwap(static_cast<uint64_t>(0)), 0);
  EXPECT_EQ(BitUtil::ByteSwap(
      static_cast<uint64_t>(0x1122334455667788)), 0x8877665544332211);

  EXPECT_EQ(BitUtil::ByteSwap(static_cast<int64_t>(0)), 0);
  EXPECT_EQ(BitUtil::ByteSwap(
      static_cast<int64_t>(0x1122334455667788)), 0x8877665544332211);

  EXPECT_EQ(BitUtil::ByteSwap(static_cast<int16_t>(0)), 0);
  EXPECT_EQ(BitUtil::ByteSwap(static_cast<int16_t>(0x1122)), 0x2211);

  EXPECT_EQ(BitUtil::ByteSwap(static_cast<uint16_t>(0)), 0);
  EXPECT_EQ(BitUtil::ByteSwap(static_cast<uint16_t>(0x1122)), 0x2211);

  // Tests for ByteSwap SIMD functions
  if (CpuInfo::IsSupported(CpuInfo::SSSE3)) {
    // Test SSSE3 functionality unit
    TestByteSwapSimd_Unit(CpuInfo::SSSE3);
    // Test ByteSwapSimd() using SSSE3;
    TestByteSwapSimd(CpuInfo::SSSE3, 64);
  }

  if (CpuInfo::IsSupported(CpuInfo::AVX2)) {
    // Test AVX2 functionality unit
    TestByteSwapSimd_Unit(CpuInfo::AVX2);
    // Test ByteSwapSimd() using AVX2;
    TestByteSwapSimd(CpuInfo::AVX2, 64);
  }

  // Test BitUtil::ByteSwap(Black Box Testing)
  for (int i = 0; i <= 32; ++i) TestByteSwapSimd(0, i);
}

TEST(BitUtil, Log2) {
  EXPECT_EQ(BitUtil::Log2CeilingNonZero64(1), 0);
  EXPECT_EQ(BitUtil::Log2CeilingNonZero64(2), 1);
  EXPECT_EQ(BitUtil::Log2CeilingNonZero64(3), 2);
  EXPECT_EQ(BitUtil::Log2CeilingNonZero64(4), 2);
  EXPECT_EQ(BitUtil::Log2CeilingNonZero64(5), 3);
  EXPECT_EQ(BitUtil::Log2CeilingNonZero64(INT_MAX), 31);
  EXPECT_EQ(BitUtil::Log2CeilingNonZero64(UINT_MAX), 32);
  EXPECT_EQ(BitUtil::Log2CeilingNonZero64(ULLONG_MAX), 64);
}

TEST(BitUtil, RoundToPowerOfTwo) {
  EXPECT_EQ(16, BitUtil::RoundUpToPowerOfTwo(9));
  EXPECT_EQ(16, BitUtil::RoundUpToPowerOfTwo(15));
  EXPECT_EQ(16, BitUtil::RoundUpToPowerOfTwo(16));
  EXPECT_EQ(32, BitUtil::RoundUpToPowerOfTwo(17));
  EXPECT_EQ(8, BitUtil::RoundDownToPowerOfTwo(9));
  EXPECT_EQ(8, BitUtil::RoundDownToPowerOfTwo(15));
  EXPECT_EQ(16, BitUtil::RoundDownToPowerOfTwo(16));
  EXPECT_EQ(16, BitUtil::RoundDownToPowerOfTwo(17));
}

TEST(BitUtil, RoundUpToPowerOf2) {
  EXPECT_EQ(BitUtil::RoundUpToPowerOf2(7, 8), 8);
  EXPECT_EQ(BitUtil::RoundUpToPowerOf2(8, 8), 8);
  EXPECT_EQ(BitUtil::RoundUpToPowerOf2(9, 8), 16);
}

TEST(BitUtil, RoundDownToPowerOf2) {
  EXPECT_EQ(BitUtil::RoundDownToPowerOf2(7, 8), 0);
  EXPECT_EQ(BitUtil::RoundDownToPowerOf2(8, 8), 8);
  EXPECT_EQ(BitUtil::RoundDownToPowerOf2(9, 8), 8);
}

TEST(BitUtil, RoundUpDown) {
  EXPECT_EQ(BitUtil::RoundUpNumBytes(7), 1);
  EXPECT_EQ(BitUtil::RoundUpNumBytes(8), 1);
  EXPECT_EQ(BitUtil::RoundUpNumBytes(9), 2);
  EXPECT_EQ(BitUtil::RoundDownNumBytes(7), 0);
  EXPECT_EQ(BitUtil::RoundDownNumBytes(8), 1);
  EXPECT_EQ(BitUtil::RoundDownNumBytes(9), 1);

  EXPECT_EQ(BitUtil::RoundUpNumi32(31), 1);
  EXPECT_EQ(BitUtil::RoundUpNumi32(32), 1);
  EXPECT_EQ(BitUtil::RoundUpNumi32(33), 2);
  EXPECT_EQ(BitUtil::RoundDownNumi32(31), 0);
  EXPECT_EQ(BitUtil::RoundDownNumi32(32), 1);
  EXPECT_EQ(BitUtil::RoundDownNumi32(33), 1);

  EXPECT_EQ(BitUtil::RoundUpNumi64(63), 1);
  EXPECT_EQ(BitUtil::RoundUpNumi64(64), 1);
  EXPECT_EQ(BitUtil::RoundUpNumi64(65), 2);
  EXPECT_EQ(BitUtil::RoundDownNumi64(63), 0);
  EXPECT_EQ(BitUtil::RoundDownNumi64(64), 1);
  EXPECT_EQ(BitUtil::RoundDownNumi64(65), 1);
}

// Prevent inlining so that the compiler can't optimize out the check.
__attribute__((noinline))
int CpuInfoIsSupportedHoistHelper(int64_t cpu_info_flag, int arg) {
  if (CpuInfo::IsSupported(cpu_info_flag)) {
    // Assembly follows similar pattern to popcnt instruction but executes
    // illegal instruction.
    int64_t result;
    __asm__ __volatile__("ud2" : "=a"(result): "mr"(arg): "cc");
    return result;
  } else {
    return 12345;
  }
}

// Regression test for IMPALA-6882 - make sure illegal instruction isn't hoisted out of
// CpuInfo::IsSupported() checks. This doesn't test the bug precisely but is a canary for
// this kind of optimization happening.
TEST(BitUtil, CpuInfoIsSupportedHoist) {
  constexpr int64_t CPU_INFO_FLAG = CpuInfo::SSSE3;
  CpuInfo::TempDisable disable_sssse3(CPU_INFO_FLAG);
  EXPECT_EQ(12345, CpuInfoIsSupportedHoistHelper(CPU_INFO_FLAG, 0));
}

}

