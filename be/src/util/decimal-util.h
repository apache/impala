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

#include <endian.h>

#include <cstdint>
#include <functional>

#include "common/compiler-util.h"
#include "common/logging.h"
#include "runtime/multi-precision.h"
#include "runtime/types.h"
#include "util/arithmetic-util.h"
#include "util/bit-util.h"
#include "util/decimal-constants.h"

namespace impala {

class DecimalUtil {
 public:
  // The scale's exclusive upper bound for GetScaleMultiplier<int32_t>()
  static constexpr int INT32_SCALE_UPPER_BOUND = ColumnType::MAX_DECIMAL4_PRECISION + 1;
  // The scale's exclusive upper bound for GetScaleMultiplier<int64_t>()
  static constexpr int INT64_SCALE_UPPER_BOUND = ColumnType::MAX_DECIMAL8_PRECISION + 1;
  // The scale's exclusive upper bound for GetScaleMultiplier<int128_t>()
  static constexpr int INT128_SCALE_UPPER_BOUND = ColumnType::MAX_PRECISION + 1;
  // The scale's exclusive upper bound for GetScaleMultiplier<int256_t>()
  static constexpr int INT256_SCALE_UPPER_BOUND = 77;

  // Helper function that checks for multiplication overflow. We only check for overflow
  // if may_overflow is false.
  template <typename T>
  static T SafeMultiply(T a, T b, bool may_overflow) {
    T result = ArithmeticUtil::AsUnsigned<std::multiplies>(a, b);
    DCHECK(may_overflow || a == 0 || result / a == b);
    return result;
  }

  static int256_t SafeMultiply(int256_t a, int256_t b, bool may_overflow) {
    int256_t result = a * b;
    DCHECK(may_overflow || a == 0 || result / a == b);
    return result;
  }

  template<typename T>
  static T MultiplyByScale(const T& v, int scale, bool may_overflow) {
    T multiplier = GetScaleMultiplier<T>(scale);
    DCHECK(multiplier > 0);
    return SafeMultiply(v, multiplier, may_overflow);
  }

  template<typename T>
  static T GetScaleMultiplier(int scale) {
    DCHECK_GE(scale, 0);
    T result = 1;
    for (int i = 0; i < scale; ++i) {
      // Verify that the result of multiplication does not overflow.
      // TODO: This is not an ideal way to check for overflow because if T is signed, the
      // behavior is undefined in case of overflow. Replace this with a better overflow
      // check.
      DCHECK_GE(result * 10, result);
      result *= 10;
    }
    return result;
  }

  /// Helper function to scale down values by 10^delta_scale, truncating if
  /// round is false or rounding otherwise.
  template<typename T>
  static inline T ScaleDownAndRound(T value, int delta_scale, bool round) {
    DCHECK_GT(delta_scale, 0);
    T divisor = DecimalUtil::GetScaleMultiplier<T>(delta_scale);
    if (divisor > 0) {
      DCHECK(divisor % 2 == 0);
      T result = value / divisor;
      if (round) {
        T remainder = value % divisor;
        // In general, shifting down the multiplier is not safe, but we know
        // here that it is a multiple of two.
        if (abs(remainder) >= (divisor >> 1)) {
          // Bias at zero must be corrected by sign of dividend.
          result += Sign(value);
        }
      }
      return result;
    } else {
      DCHECK(divisor == -1);
      return 0;
    }
  }

  /// Write decimals as big endian (byte comparable) in fixed_len_size bytes.
  template<typename T>
  static inline void EncodeToFixedLenByteArray(
      uint8_t* buffer, int fixed_len_size, const T& v) {
    DCHECK_GT(fixed_len_size, 0);
    DCHECK_LE(fixed_len_size, sizeof(T));

#if __BYTE_ORDER == __LITTLE_ENDIAN
    BitUtil::ByteSwap(buffer, &v, fixed_len_size);
#else
    memcpy(buffer, &v + sizeof(T) - fixed_len_size, fixed_len_size);
#endif

#ifndef NDEBUG
#if __BYTE_ORDER == __LITTLE_ENDIAN
    const int8_t* skipped_bytes_start = reinterpret_cast<const int8_t*>(&v) +
        fixed_len_size;
#else
    const int8_t* skipped_bytes_start = reinterpret_cast<const int8_t*>(&v);
#endif
    // On debug, verify that the skipped bytes are what we expect.
    for (int i = 0; i < sizeof(T) - fixed_len_size; ++i) {
      DCHECK_EQ(skipped_bytes_start[i], v.value() < 0 ? -1 : 0);
    }
#endif
  }

  template <typename T>
  static inline void DecodeFromFixedLenByteArray(
      const uint8_t* RESTRICT buffer, int fixed_len_size, T* RESTRICT v) {
    DCHECK_GT(fixed_len_size, 0);
#if __BYTE_ORDER != __LITTLE_ENDIAN
    static_assert(false, "Byte order must be little-endian");
#endif
    if (LIKELY(sizeof(T) >= fixed_len_size)) {
      // We need to sign extend val. For example, if the original value was
      // -1, the original bytes were -1,-1,-1,-1. If we only wrote out 1 byte, ByteSwap()
      // only fills in the first byte of 'v' - the least significant byte. We initialize
      // the value to either 0 or -1 to ensure that the result is correctly sign-extended.

      // GCC can optimize this code to an instruction pair of movsbq and sarq with no
      // branches.
      int8_t most_significant_byte = reinterpret_cast<const int8_t*>(buffer)[0];
      v->set_value(most_significant_byte >= 0 ? 0 : -1);
      BitUtil::ByteSwap(reinterpret_cast<int8_t*>(v), buffer, fixed_len_size);
    } else {
      // If the destination 'v' is smaller than the input, discard the upper bytes.
      // These bytes should only contain the sign-extended value if they were encoded
      // correctly, so are not required.
      int bytes_to_discard = fixed_len_size - sizeof(T);
      BitUtil::ByteSwap(reinterpret_cast<int8_t*>(v),
          buffer + bytes_to_discard, sizeof(T));
    }
  }

  // Used to skip checking overflow in multiply of decimal values.
  static inline int Clz(const int128_t& v) {
    // GCC leaves __builtin_clz undefined for zero.
    uint64_t high = static_cast<uint64_t>(v >> 64);
    if (high != 0) return __builtin_clzll(high);
    uint64_t low = static_cast<uint64_t>(v);
    if (low != 0) return 64 + __builtin_clzll(low);
    return 128;
  }

};

template <>
inline int32_t DecimalUtil::GetScaleMultiplier<int32_t>(int scale) {
  DCHECK_GE(scale, 0);
  static constexpr int32_t values[INT32_SCALE_UPPER_BOUND] = {
      1,
      10,
      100,
      1000,
      10000,
      100000,
      1000000,
      10000000,
      100000000,
      1000000000};
  if (LIKELY(scale < INT32_SCALE_UPPER_BOUND)) return values[scale];
  return -1;  // Overflow
}

template <>
inline int64_t DecimalUtil::GetScaleMultiplier<int64_t>(int scale) {
  DCHECK_GE(scale, 0);
  static constexpr int64_t values[INT64_SCALE_UPPER_BOUND] = {
      1ll,
      10ll,
      100ll,
      1000ll,
      10000ll,
      100000ll,
      1000000ll,
      10000000ll,
      100000000ll,
      1000000000ll,
      10000000000ll,
      100000000000ll,
      1000000000000ll,
      10000000000000ll,
      100000000000000ll,
      1000000000000000ll,
      10000000000000000ll,
      100000000000000000ll,
      1000000000000000000ll};
  if (LIKELY(scale < INT64_SCALE_UPPER_BOUND)) return values[scale];
  return -1;  // Overflow
}

template <>
inline int128_t DecimalUtil::GetScaleMultiplier<int128_t>(int scale) {
  DCHECK_GE(scale, 0);
  static constexpr int128_t i10e18{1000000000000000000ll};
  static constexpr int128_t values[INT128_SCALE_UPPER_BOUND] = {
      static_cast<int128_t>(1ll),
      static_cast<int128_t>(10ll),
      static_cast<int128_t>(100ll),
      static_cast<int128_t>(1000ll),
      static_cast<int128_t>(10000ll),
      static_cast<int128_t>(100000ll),
      static_cast<int128_t>(1000000ll),
      static_cast<int128_t>(10000000ll),
      static_cast<int128_t>(100000000ll),
      static_cast<int128_t>(1000000000ll),
      static_cast<int128_t>(10000000000ll),
      static_cast<int128_t>(100000000000ll),
      static_cast<int128_t>(1000000000000ll),
      static_cast<int128_t>(10000000000000ll),
      static_cast<int128_t>(100000000000000ll),
      static_cast<int128_t>(1000000000000000ll),
      static_cast<int128_t>(10000000000000000ll),
      static_cast<int128_t>(100000000000000000ll),
      i10e18,
      i10e18 * 10ll,
      i10e18 * 100ll,
      i10e18 * 1000ll,
      i10e18 * 10000ll,
      i10e18 * 100000ll,
      i10e18 * 1000000ll,
      i10e18 * 10000000ll,
      i10e18 * 100000000ll,
      i10e18 * 1000000000ll,
      i10e18 * 10000000000ll,
      i10e18 * 100000000000ll,
      i10e18 * 1000000000000ll,
      i10e18 * 10000000000000ll,
      i10e18 * 100000000000000ll,
      i10e18 * 1000000000000000ll,
      i10e18 * 10000000000000000ll,
      i10e18 * 100000000000000000ll,
      i10e18 * i10e18,
      i10e18 * i10e18 * 10ll,
      i10e18 * i10e18 * 100ll};
  if (LIKELY(scale < INT128_SCALE_UPPER_BOUND)) return values[scale];
  return -1;  // Overflow
}

template <>
inline int256_t DecimalUtil::GetScaleMultiplier<int256_t>(int scale) {
  DCHECK_GE(scale, 0);
  static constexpr int256_t i10e18{1000000000000000000ll};
  static constexpr int256_t values[INT256_SCALE_UPPER_BOUND] = {
      static_cast<int256_t>(1ll),
      static_cast<int256_t>(10ll),
      static_cast<int256_t>(100ll),
      static_cast<int256_t>(1000ll),
      static_cast<int256_t>(10000ll),
      static_cast<int256_t>(100000ll),
      static_cast<int256_t>(1000000ll),
      static_cast<int256_t>(10000000ll),
      static_cast<int256_t>(100000000ll),
      static_cast<int256_t>(1000000000ll),
      static_cast<int256_t>(10000000000ll),
      static_cast<int256_t>(100000000000ll),
      static_cast<int256_t>(1000000000000ll),
      static_cast<int256_t>(10000000000000ll),
      static_cast<int256_t>(100000000000000ll),
      static_cast<int256_t>(1000000000000000ll),
      static_cast<int256_t>(10000000000000000ll),
      static_cast<int256_t>(100000000000000000ll),
      i10e18,
      i10e18 * 10ll,
      i10e18 * 100ll,
      i10e18 * 1000ll,
      i10e18 * 10000ll,
      i10e18 * 100000ll,
      i10e18 * 1000000ll,
      i10e18 * 10000000ll,
      i10e18 * 100000000ll,
      i10e18 * 1000000000ll,
      i10e18 * 10000000000ll,
      i10e18 * 100000000000ll,
      i10e18 * 1000000000000ll,
      i10e18 * 10000000000000ll,
      i10e18 * 100000000000000ll,
      i10e18 * 1000000000000000ll,
      i10e18 * 10000000000000000ll,
      i10e18 * 100000000000000000ll,
      i10e18 * i10e18,
      i10e18 * i10e18 * 10ll,
      i10e18 * i10e18 * 100ll,
      i10e18 * i10e18 * 1000ll,
      i10e18 * i10e18 * 10000ll,
      i10e18 * i10e18 * 100000ll,
      i10e18 * i10e18 * 1000000ll,
      i10e18 * i10e18 * 10000000ll,
      i10e18 * i10e18 * 100000000ll,
      i10e18 * i10e18 * 1000000000ll,
      i10e18 * i10e18 * 10000000000ll,
      i10e18 * i10e18 * 100000000000ll,
      i10e18 * i10e18 * 1000000000000ll,
      i10e18 * i10e18 * 10000000000000ll,
      i10e18 * i10e18 * 100000000000000ll,
      i10e18 * i10e18 * 1000000000000000ll,
      i10e18 * i10e18 * 10000000000000000ll,
      i10e18 * i10e18 * 100000000000000000ll,
      i10e18 * i10e18 * i10e18,
      i10e18 * i10e18 * i10e18 * 10ll,
      i10e18 * i10e18 * i10e18 * 100ll,
      i10e18 * i10e18 * i10e18 * 1000ll,
      i10e18 * i10e18 * i10e18 * 10000ll,
      i10e18 * i10e18 * i10e18 * 100000ll,
      i10e18 * i10e18 * i10e18 * 1000000ll,
      i10e18 * i10e18 * i10e18 * 10000000ll,
      i10e18 * i10e18 * i10e18 * 100000000ll,
      i10e18 * i10e18 * i10e18 * 1000000000ll,
      i10e18 * i10e18 * i10e18 * 10000000000ll,
      i10e18 * i10e18 * i10e18 * 100000000000ll,
      i10e18 * i10e18 * i10e18 * 1000000000000ll,
      i10e18 * i10e18 * i10e18 * 10000000000000ll,
      i10e18 * i10e18 * i10e18 * 100000000000000ll,
      i10e18 * i10e18 * i10e18 * 1000000000000000ll,
      i10e18 * i10e18 * i10e18 * 10000000000000000ll,
      i10e18 * i10e18 * i10e18 * 100000000000000000ll,
      i10e18 * i10e18 * i10e18 * i10e18,
      i10e18 * i10e18 * i10e18 * i10e18 * 10ll,
      i10e18 * i10e18 * i10e18 * i10e18 * 100ll,
      i10e18 * i10e18 * i10e18 * i10e18 * 1000ll,
      i10e18 * i10e18 * i10e18 * i10e18 * 10000ll};
  if (LIKELY(scale < INT256_SCALE_UPPER_BOUND)) return values[scale];
  return -1;  // Overflow
}
}
