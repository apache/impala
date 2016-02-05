// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_UTIL_DECIMAL_UTIL_H
#define IMPALA_UTIL_DECIMAL_UTIL_H

#include <ostream>
#include <string>
#include <boost/cstdint.hpp>

#include "runtime/types.h"
#include "runtime/multi-precision.h"
#include "util/bit-util.h"

namespace impala {

class DecimalUtil {
 public:
  /// Maximum absolute value of a valid Decimal4Value. This is 9 digits of 9's.
  static const int32_t MAX_UNSCALED_DECIMAL4;

  /// Maximum absolute value of a valid Decimal8Value. This is 18 digits of 9's.
  static const int64_t MAX_UNSCALED_DECIMAL8;

  /// Maximum absolute value a valid Decimal16Value. This is 38 digits of 9's.
  static int128_t MAX_UNSCALED_DECIMAL16;

  /// Initializes MAX_UNSCALED_DECIMAL16. Must be called once before using it.
  static void InitMaxUnscaledDecimal16();

  /// TODO: do we need to handle overflow here or at a higher abstraction.
  template<typename T>
  static T MultiplyByScale(const T& v, const ColumnType& t) {
    DCHECK(t.type == TYPE_DECIMAL);
    return MultiplyByScale(v, t.scale);
  }

  template<typename T>
  static T MultiplyByScale(const T& v, int scale) {
    return v * GetScaleMultiplier<T>(scale);
  }

  template<typename T>
  static T GetScaleMultiplier(int scale) {
    DCHECK_GE(scale, 0);
    T result = 1;
    for (int i = 0; i < scale; ++i) {
      result *= 10;
    }
    return result;
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

  template<typename T>
  static inline void DecodeFromFixedLenByteArray(
      const uint8_t* buffer, int fixed_len_size, T* v) {
    DCHECK_GT(fixed_len_size, 0);
    DCHECK_LE(fixed_len_size, sizeof(T));
    *v = 0;
    // We need to sign extend val. For example, if the original value was
    // -1, the original bytes were -1,-1,-1,-1. If we only wrote out 1 byte, after
    // the encode step above, val would contain (-1, 0, 0, 0). We need to sign
    // extend the remaining 3 bytes to get the original value.
    // We do this by filling in the most significant bytes and (arithmetic) bit
    // shifting down.
    int bytes_to_fill = sizeof(T) - fixed_len_size;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    BitUtil::ByteSwap(reinterpret_cast<int8_t*>(v) + bytes_to_fill, buffer, fixed_len_size);
#else
    memcpy(v, buffer, fixed_len_size);
#endif
    v->value() >>= (bytes_to_fill * 8);
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

  static inline int128_t GetScaleQuotient(int scale);
};

template <>
inline int32_t DecimalUtil::GetScaleMultiplier<int32_t>(int scale) {
  DCHECK_GE(scale, 0);
  static const int32_t values[] = {
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
  DCHECK_GE(sizeof(values) / sizeof(int32_t), ColumnType::MAX_DECIMAL4_PRECISION);
  if (LIKELY(scale < 10)) return values[scale];
  return -1;  // Overflow
}

template <>
inline int64_t DecimalUtil::GetScaleMultiplier<int64_t>(int scale) {
  DCHECK_GE(scale, 0);
  static const int64_t values[] = {
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
  DCHECK_GE(sizeof(values) / sizeof(int64_t), ColumnType::MAX_DECIMAL8_PRECISION);
  if (LIKELY(scale < 19)) return values[scale];
  return -1;  // Overflow
}

template <>
inline int128_t DecimalUtil::GetScaleMultiplier<int128_t>(int scale) {
  DCHECK_GE(scale, 0);
  static const int128_t values[] = {
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
      static_cast<int128_t>(1000000000000000000ll),
      static_cast<int128_t>(1000000000000000000ll) * 10ll,
      static_cast<int128_t>(1000000000000000000ll) * 100ll,
      static_cast<int128_t>(1000000000000000000ll) * 1000ll,
      static_cast<int128_t>(1000000000000000000ll) * 10000ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000ll,
      static_cast<int128_t>(1000000000000000000ll) * 1000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 10000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 1000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 10000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 1000000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 10000000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 1000000000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 10000000000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000000000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000000000000000ll * 10ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000000000000000ll * 100ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000000000000000ll * 1000ll};
  DCHECK_GE(sizeof(values) / sizeof(int128_t), ColumnType::MAX_PRECISION);
  if (LIKELY(scale < 39)) return values[scale];
  return -1;  // Overflow
}

inline int128_t DecimalUtil::GetScaleQuotient(int scale) {
  DCHECK_GT(scale, 0);
  DCHECK_LE(scale, ColumnType::MAX_PRECISION);
  static const int128_t values[] = {
      static_cast<int128_t>(0ll),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(1),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(2),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(3),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(4),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(5),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(6),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(7),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(8),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(9),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(10),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(11),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(12),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(13),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(14),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(15),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(16),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(17),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(18),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(19),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(20),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(21),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(22),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(23),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(24),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(25),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(26),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(27),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(28),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(29),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(30),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(31),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(32),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(33),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(34),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(35),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(36),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(37),
      MAX_UNSCALED_DECIMAL16 / GetScaleMultiplier<int128_t>(38)};
  DCHECK_GE(sizeof(values) / sizeof(int128_t), ColumnType::MAX_PRECISION);
  return values[scale];
}
}

#endif
