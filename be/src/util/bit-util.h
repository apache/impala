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


#ifndef IMPALA_BIT_UTIL_H
#define IMPALA_BIT_UTIL_H

#if defined(__APPLE__)
#include <machine/endian.h>
#else
#include <endian.h>
#endif

#include <climits>
#include <limits>
#include <typeinfo>
#include <type_traits>

#include "common/compiler-util.h"
#include "gutil/bits.h"
#include "runtime/multi-precision.h"
#include "util/cpu-info.h"
#include "util/sse-util.h"

namespace impala {

// Doubles the width of integer types (e.g. int32_t -> int64_t).
// Currently only works with a few signed types.
// Feel free to extend it to other types as well.
template <typename T>
struct DoubleWidth {};

template <>
struct DoubleWidth<int32_t> {
  using type = int64_t;
};

template <>
struct DoubleWidth<int64_t> {
  using type = int128_t;
};

template <>
struct DoubleWidth<int128_t> {
  using type = int256_t;
};

/// Utility class to do standard bit tricks
/// TODO: is this in boost or something else like that?
class BitUtil {
 public:

  /// Returns the width of the integer portion of the type, not counting the sign bit.
  /// Not safe for use with unknown or non-native types, so make it undefined
  template<typename T, typename CVR_REMOVED = typename std::decay<T>::type,
      typename std::enable_if<std::is_integral<CVR_REMOVED>{} ||
                              std::is_same<CVR_REMOVED, unsigned __int128>{} ||
                              std::is_same<CVR_REMOVED, __int128>{}, int>::type = 0>
  constexpr static inline int UnsignedWidth() {
    return std::is_integral<CVR_REMOVED>::value ?
        std::numeric_limits<CVR_REMOVED>::digits :
        std::is_same<CVR_REMOVED, unsigned __int128>::value ? 128 :
        std::is_same<CVR_REMOVED, __int128>::value ? 127 : -1;
  }

  /// Returns the max value that can be represented in T.
  template<typename T, typename CVR_REMOVED = typename std::decay<T>::type,
      typename std::enable_if<std::is_integral<CVR_REMOVED> {}||
                              std::is_same<CVR_REMOVED, __int128> {}, int>::type = 0>
  constexpr static inline CVR_REMOVED Max() {
    return std::is_integral<CVR_REMOVED>::value ?
        std::numeric_limits<CVR_REMOVED>::max() :
        std::is_same<CVR_REMOVED, __int128>::value ?
            static_cast<UnsignedType<CVR_REMOVED>>(-1) / 2 : -1;
  }

  /// Return an integer signifying the sign of the value, returning +1 for
  /// positive integers (and zero), -1 for negative integers.
  /// The extra shift is to silence GCC warnings about full width shift on
  /// unsigned types. It compiles out in optimized builds into the expected increment.
  template<typename T>
  constexpr static inline T Sign(T value) {
    return 1 | ((value >> (UnsignedWidth<T>() - 1)) >> 1);
  }

  /// Returns the ceil of value/divisor
  constexpr static inline int64_t Ceil(int64_t value, int64_t divisor) {
    return value / divisor + (value % divisor != 0);
  }

  /// Returns 'value' rounded up to the nearest multiple of 'factor'
  constexpr static inline int64_t RoundUp(int64_t value, int64_t factor) {
    return (value + (factor - 1)) / factor * factor;
  }

  /// Returns 'value' rounded down to the nearest multiple of 'factor'
  constexpr static inline int64_t RoundDown(int64_t value, int64_t factor) {
    return (value / factor) * factor;
  }

  /// Returns the smallest power of two that contains v. If v is a power of two, v is
  /// returned. Taken from
  /// http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
  static inline int64_t RoundUpToPowerOfTwo(int64_t v) {
    --v;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    ++v;
    return v;
  }

  /// Returns the largest power of two <= v.
  static inline int64_t RoundDownToPowerOfTwo(int64_t v) {
    int64_t v_rounded_up = RoundUpToPowerOfTwo(v);
    return v_rounded_up == v ? v : v_rounded_up / 2;
  }

  /// Returns 'value' rounded up to the nearest multiple of 'factor' when factor is
  /// a power of two
  static inline int64_t RoundUpToPowerOf2(int64_t value, int64_t factor) {
    DCHECK((factor > 0) && ((factor & (factor - 1)) == 0));
    return (value + (factor - 1)) & ~(factor - 1);
  }

  static inline int64_t RoundDownToPowerOf2(int64_t value, int64_t factor) {
    DCHECK((factor > 0) && ((factor & (factor - 1)) == 0));
    return value & ~(factor - 1);
  }

  constexpr static inline bool IsPowerOf2(int64_t value) {
    return (value & (value - 1)) == 0;
  }

  /// Specialized round up and down functions for frequently used factors,
  /// like 8 (bits->bytes), 32 (bits->i32), and 64 (bits->i64).
  /// Returns the rounded up number of bytes that fit the number of bits.
  constexpr static inline uint32_t RoundUpNumBytes(uint32_t bits) {
    return (bits + 7) >> 3;
  }

  /// Returns the rounded down number of bytes that fit the number of bits.
  constexpr static inline uint32_t RoundDownNumBytes(uint32_t bits) { return bits >> 3; }

  /// Returns the rounded up to 32 multiple. Used for conversions of bits to i32.
  constexpr static inline uint32_t RoundUpNumi32(uint32_t bits) {
    return (bits + 31) >> 5;
  }

  /// Returns the rounded up 32 multiple.
  constexpr static inline uint32_t RoundDownNumi32(uint32_t bits) { return bits >> 5; }

  /// Returns the rounded up to 64 multiple. Used for conversions of bits to i64.
  constexpr static inline uint32_t RoundUpNumi64(uint32_t bits) {
    return (bits + 63) >> 6;
  }

  /// Returns the rounded down to 64 multiple.
  constexpr static inline uint32_t RoundDownNumi64(uint32_t bits) { return bits >> 6; }

  /// Non hw accelerated pop count.
  /// TODO: we don't use this in any perf sensitive code paths currently.  There
  /// might be a much faster way to implement this.
  static inline int PopcountNoHw(uint64_t x) {
    int count = 0;
    for (; x != 0; ++count) x &= x-1;
    return count;
  }

  /// Returns the number of set bits in x
  static inline int Popcount(uint64_t x) {
    if (LIKELY(CpuInfo::IsSupported(CpuInfo::POPCNT))) {
      return POPCNT_popcnt_u64(x);
    } else {
      return PopcountNoHw(x);
    }
  }

  // Compute correct population count for various-width signed integers
  template<typename T>
  static inline int PopcountSigned(T v) {
    // Converting to same-width unsigned then extending preserves the bit pattern.
    return BitUtil::Popcount(static_cast<UnsignedType<T>>(v));
  }

  /// Returns the 'num_bits' least-significant bits of 'v'.
  /// Force inlining - GCC does not always inline this into hot loops.
  static ALWAYS_INLINE uint64_t TrailingBits(uint64_t v, int num_bits) {
    if (UNLIKELY(num_bits >= 64)) return v;
    return ((1UL << num_bits) - 1) & v;
  }

  /// Swaps the byte order (i.e. endianess)
  static inline int64_t ByteSwap(int64_t value) {
    return __builtin_bswap64(value);
  }
  static inline uint64_t ByteSwap(uint64_t value) {
    return __builtin_bswap64(value);
  }
  static inline int32_t ByteSwap(int32_t value) {
    return __builtin_bswap32(value);
  }
  static inline uint32_t ByteSwap(uint32_t value) {
    return __builtin_bswap32(value);
  }
  static inline int16_t ByteSwap(int16_t value) {
    return __builtin_bswap16(value);
  }
  static inline uint16_t ByteSwap(uint16_t value) {
    return __builtin_bswap16(value);
  }

  /// Write the swapped bytes into dest; source and dest must not overlap.
  /// This function is optimized for len <= 16. It reverts to a slow loop-based
  /// swap for len > 16.
  static void ByteSwap(void* dest, const void* source, int len);

/// Converts to big endian format (if not already in big endian) from the
/// machine's native endian format.
#if __BYTE_ORDER == __LITTLE_ENDIAN
  static inline int64_t ToBigEndian(int64_t value) { return ByteSwap(value); }
  static inline uint64_t ToBigEndian(uint64_t value) { return ByteSwap(value); }
  static inline int32_t ToBigEndian(int32_t value) { return ByteSwap(value); }
  static inline uint32_t ToBigEndian(uint32_t value) { return ByteSwap(value); }
  static inline int16_t ToBigEndian(int16_t value) { return ByteSwap(value); }
  static inline uint16_t ToBigEndian(uint16_t value) { return ByteSwap(value); }
#else
  static inline int64_t ToBigEndian(int64_t val) { return val; }
  static inline uint64_t ToBigEndian(uint64_t val) { return val; }
  static inline int32_t ToBigEndian(int32_t val) { return val; }
  static inline uint32_t ToBigEndian(uint32_t val) { return val; }
  static inline int16_t ToBigEndian(int16_t val) { return val; }
  static inline uint16_t ToBigEndian(uint16_t val) { return val; }
#endif

/// Converts from big endian format to the machine's native endian format.
#if __BYTE_ORDER == __LITTLE_ENDIAN
  static inline int64_t FromBigEndian(int64_t value) { return ByteSwap(value); }
  static inline uint64_t FromBigEndian(uint64_t value) { return ByteSwap(value); }
  static inline int32_t FromBigEndian(int32_t value) { return ByteSwap(value); }
  static inline uint32_t FromBigEndian(uint32_t value) { return ByteSwap(value); }
  static inline int16_t FromBigEndian(int16_t value) { return ByteSwap(value); }
  static inline uint16_t FromBigEndian(uint16_t value) { return ByteSwap(value); }
#else
  static inline int64_t FromBigEndian(int64_t val) { return val; }
  static inline uint64_t FromBigEndian(uint64_t val) { return val; }
  static inline int32_t FromBigEndian(int32_t val) { return val; }
  static inline uint32_t FromBigEndian(uint32_t val) { return val; }
  static inline int16_t FromBigEndian(int16_t val) { return val; }
  static inline uint16_t FromBigEndian(uint16_t val) { return val; }
#endif

  /// Returns true if 'value' is a non-negative 32-bit integer.
  constexpr static inline bool IsNonNegative32Bit(int64_t value) {
    return static_cast<uint64_t>(value) <= std::numeric_limits<int32_t>::max();
  }

  /// Logical right shift for signed integer types
  /// This is needed because the C >> operator does arithmetic right shift
  /// Negative shift amounts lead to undefined behavior
  template <typename T>
  constexpr static T ShiftRightLogical(T v, int shift) {
    // Conversion to unsigned ensures most significant bits always filled with 0's
    return static_cast<UnsignedType<T>>(v) >> shift;
  }

  /// Get an specific bit of a numeric type
  template<typename T>
  static inline int8_t GetBit(T v, int bitpos) {
    T masked = v & (static_cast<T>(0x1) << bitpos);
    return static_cast<int8_t>(ShiftRightLogical(masked, bitpos));
  }

  /// Set a specific bit to 1
  /// Behavior when bitpos is negative is undefined
  template <typename T>
  constexpr static T SetBit(T v, int bitpos) {
    return v | (static_cast<T>(0x1) << bitpos);
  }

  /// Set a specific bit to 0
  /// Behavior when bitpos is negative is undefined
  template <typename T>
  constexpr static T UnsetBit(T v, int bitpos) {
    return v & ~(static_cast<T>(0x1) << bitpos);
  }

  /// Wrappers around __builtin_ctz, which returns an undefined value when the argument is
  /// zero.
  static inline int CountTrailingZeros(
      unsigned int v, int otherwise = sizeof(unsigned int) * CHAR_BIT) {
    if (UNLIKELY(v == 0)) return otherwise;
    return __builtin_ctz(v);
  }

  static inline int CountTrailingZeros(
      unsigned long v, int otherwise = sizeof(unsigned long) * CHAR_BIT) {
    if (UNLIKELY(v == 0)) return otherwise;
    return __builtin_ctzl(v);
  }

  static inline int CountTrailingZeros(
      unsigned long long v, int otherwise = sizeof(unsigned long long) * CHAR_BIT) {
    if (UNLIKELY(v == 0)) return otherwise;
    return __builtin_ctzll(v);
  }

  template<typename T>
  static inline int CountLeadingZeros(T v) {
    DCHECK(v >= 0);
    if (sizeof(T) == 4) {
      uint32_t orig = static_cast<uint32_t>(v);
      return __builtin_clz(orig);
    } else if (sizeof(T) == 8) {
      uint64_t orig = static_cast<uint64_t>(v);
      return __builtin_clzll(orig);
    } else {
      DCHECK(sizeof(T) == 16);
      if (UNLIKELY(v == 0)) return 128;
      unsigned __int128 orig = static_cast<unsigned __int128>(v);
      unsigned __int128 shifted = orig >> 64;
      if (shifted != 0) {
        return __builtin_clzll(shifted);
      } else {
        return __builtin_clzll(orig) + 64;
      }
    }
  }

  // Wrap the gutil/ version for convenience.
  static inline int Log2Floor(uint32_t n) {
    return Bits::Log2Floor(n);
  }

  // Wrap the gutil/ version for convenience.
  static inline int Log2Floor64(uint64_t n) {
    return Bits::Log2Floor64(n);
  }

  // Wrap the gutil/ version for convenience.
  static inline int Log2FloorNonZero64(uint64_t n) {
    return Bits::Log2FloorNonZero64(n);
  }

  /// More efficient version of similar functions found in gutil/
  static inline int Log2Ceiling(uint32 n) {
    int floor = Log2Floor(n);
    // Check if zero or a power of two. This pattern is recognised by gcc and optimised
    // into branch-free code.
    if (0 == (n & (n - 1))) {
      return floor;
    } else {
      return floor + 1;
    }
  }

  static inline int Log2Ceiling64(uint64_t n) {
    int floor = Log2Floor64(n);
    // Check if zero or a power of two. This pattern is recognised by gcc and optimised
    // into branch-free code.
    if (0 == (n & (n - 1))) {
      return floor;
    } else {
      return floor + 1;
    }
  }

  static inline int Log2CeilingNonZero64(uint64_t n) {
    int floor = Log2FloorNonZero64(n);
    // Check if zero or a power of two. This pattern is recognised by gcc and optimised
    // into branch-free code.
    if (0 == (n & (n - 1))) {
      return floor;
    } else {
      return floor + 1;
    }
  }
};

template<>
inline int256_t BitUtil::Sign(int256_t value) {
  return value < 0 ? -1 : 1;
}

/// An encapsulation class of SIMD byteswap functions
class SimdByteSwap {
 public:
  /// Byteswap an array in the scalar way with some builtin optimization for arrays of
  /// length <= 16 bytes.
  ///   const void* src: the source address of the input array;
  ///   int len: the length of the input array;
  ///   void* dst: the destination address of the output array;
  static void ByteSwapScalar(const void* src, int len, void* dst);

  /// SIMD ByteSwap functions:
  /// ByteSwap128 is to byteswap an array of 16 bytes(128 bits) using SSSE3 intrinsics;
  /// ByteSwap256 is to byteswap an array of 32 bytes(256 bits) using AVX2 intrinsics;
  /// Function parameters have the same meaning as ByteSwapScalar.
  static void ByteSwap128(const uint8_t* src, uint8_t* dst);
  static void ByteSwap256(const uint8_t* src, uint8_t* dst);

  /// Template function ByteSwapSimd is the entry point function to byteswap an array
  /// using SIMD approach.
  /// Template parameter:
  ///   int TEMPLATE_DATA_WIDTH: only 16 or 32 are valid now;
  ///   16 means using ByteSwap128 as the internal SIMD implementation;
  ///   32 means using ByteSwap256 as the internal SIMD implementation;
  /// Function parameters have the same meaning as ByteSwapScalar.
  template <int TEMPLATE_DATA_WIDTH>
  static void ByteSwapSimd(const void* src, const int len, void* dst);
};
}

#endif
