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

#if defined(__APPLE__)
#include <machine/endian.h>
#else
#include <endian.h>
#endif

#include <algorithm>
#include <climits>
#include <cstdint>
#include <limits>
#include <vector>

#include "common/compiler-util.h"
#include "common/logging.h"
#include "gutil/bits.h"
#include "util/arithmetic-util.h"
#include "util/cpu-info.h"
#include "util/sse-util.h"

namespace impala {

/// Utility class to do standard bit tricks
/// TODO: is this in boost or something else like that?
class BitUtil {
 public:
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

  /// Returns whether the given byte is the start byte of a UTF-8 character.
  constexpr static inline bool IsUtf8StartByteRaw(uint8_t b) {
    // If the byte is not 10xxxxxx, it is a start byte.
    return (b & 0xC0) != 0x80;
  }

  /// Returns the byte length of a *legal* UTF-8 character (code point) given its first
  /// byte. The mapping table is as follows:
  ///   Char. number range  |        UTF-8 octet sequence
  ///      (hexadecimal)    |              (binary)
  ///   --------------------+---------------------------------------------
  ///   0000 0000-0000 007F | 0xxxxxxx
  ///   0000 0080-0000 07FF | 110xxxxx 10xxxxxx
  ///   0000 0800-0000 FFFF | 1110xxxx 10xxxxxx 10xxxxxx
  ///   0001 0000-0010 FFFF | 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
  /// See more details: https://www.rfc-editor.org/rfc/rfc3629.html#section-3
  constexpr static inline int8_t NumBytesInUtf8EncodingRaw(uint8_t first_byte) {
    // If the first byte is 0xxxxxxx, the UTF-8 character has 1 byte.
    if ((first_byte & 0x80) == 0x00) return 1;
    // If the first byte is 110xxxxx, the UTF-8 character has 2 bytes.
    if ((first_byte & 0xE0) == 0xC0) return 2;
    // If the first byte is 1110xxxx, the UTF-8 character has 3 bytes.
    if ((first_byte & 0xF0) == 0xE0) return 3;
    // If the first byte is 11110xxx, the UTF-8 character has 4 bytes.
    if ((first_byte & 0xF8) == 0xF0) return 4;
    // Otherwise, the byte is continuation (10xxxxxx) or invalid (11111xxx),
    // just returns 1 for safe.
    return 1;
  }

  template <class T, size_t... Is>
  constexpr static auto GenerateTable(T(*func)(uint8_t), std::index_sequence<Is...>) {
    return std::array<T, sizeof...(Is)>{{func(Is)...}};
  }

  /// Same as IsUtf8StartByteRaw() but returns the result using the byte value as an index
  /// from a precalculated table.
  static inline bool IsUtf8StartByte(uint8_t byte) {
    constexpr static auto is_utf8_start_byte_table =
        GenerateTable(IsUtf8StartByteRaw, std::make_index_sequence<256>{});
    return is_utf8_start_byte_table[byte];
  }

  /// Same as NumBytesInUtf8EncodingRaw() but returns the byte length of a UTF-8 character
  /// using the first byte value as an index from a precalculated table.
  static inline int NumBytesInUtf8Encoding(uint8_t first_byte) {
    constexpr static auto utf8_byte_length_table =
        GenerateTable(NumBytesInUtf8EncodingRaw, std::make_index_sequence<256>{});
    return utf8_byte_length_table[first_byte];
  }

  /// Non hw accelerated pop count.
  /// TODO: we don't use this in any perf sensitive code paths currently.  There
  /// might be a much faster way to implement this.
  static inline int PopcountNoHw(uint64_t x) {
    int count = 0;
    for (; x != 0; ++count) x &= x-1;
    return count;
  }

  /// Returns the number of set bits in x
#ifdef __aarch64__
  static inline int Popcount(uint64_t x) {
    return POPCNT_popcnt_u64(x);
  }
#else
  static inline int Popcount(uint64_t x) {
    if (LIKELY(CpuInfo::IsSupported(CpuInfo::POPCNT))) {
      return POPCNT_popcnt_u64(x);
    } else {
      return PopcountNoHw(x);
    }
  }
#endif

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
  static inline __int128_t ToBigEndian(__int128_t value) {
    __int128_t res = 0;
    ByteSwap(&res, &value, sizeof(__int128_t));
    return res;
  }
  static inline int64_t ToBigEndian(int64_t value) { return ByteSwap(value); }
  static inline uint64_t ToBigEndian(uint64_t value) { return ByteSwap(value); }
  static inline int32_t ToBigEndian(int32_t value) { return ByteSwap(value); }
  static inline uint32_t ToBigEndian(uint32_t value) { return ByteSwap(value); }
  static inline int16_t ToBigEndian(int16_t value) { return ByteSwap(value); }
  static inline uint16_t ToBigEndian(uint16_t value) { return ByteSwap(value); }
#else
  static inline __int128_t ToBigEndian(__int128_t val) { return val; }
  static inline int64_t ToBigEndian(int64_t val) { return val; }
  static inline uint64_t ToBigEndian(uint64_t val) { return val; }
  static inline int32_t ToBigEndian(int32_t val) { return val; }
  static inline uint32_t ToBigEndian(uint32_t val) { return val; }
  static inline int16_t ToBigEndian(int16_t val) { return val; }
  static inline uint16_t ToBigEndian(uint16_t val) { return val; }
#endif

/// Converts from big endian format to the machine's native endian format.
#if __BYTE_ORDER == __LITTLE_ENDIAN
  static inline __int128_t FromBigEndian(__int128_t value) {
    __int128_t res = 0;
    ByteSwap(&res, &value, sizeof(__int128_t));
    return res;
  }
  static inline int64_t FromBigEndian(int64_t value) { return ByteSwap(value); }
  static inline uint64_t FromBigEndian(uint64_t value) { return ByteSwap(value); }
  static inline int32_t FromBigEndian(int32_t value) { return ByteSwap(value); }
  static inline uint32_t FromBigEndian(uint32_t value) { return ByteSwap(value); }
  static inline int16_t FromBigEndian(int16_t value) { return ByteSwap(value); }
  static inline uint16_t FromBigEndian(uint16_t value) { return ByteSwap(value); }
#else
  static inline __int128_t FromBigEndian(__int128_t val) { return val; }
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
  static constexpr inline int CountLeadingZeros(T v) {
    static_assert(sizeof(T) == 4 || sizeof(T) == 8 || sizeof(T) == 16);
    DCHECK(v >= 0);

    // __builtin_clz() and __builtin_clzll() is undefined for 0.
    if (UNLIKELY(v == 0)) {
      return sizeof(T) * CHAR_BIT;
    }

    if (sizeof(T) == 4) {
      uint32_t orig = static_cast<uint32_t>(v);
      return __builtin_clz(orig);
    } else if (sizeof(T) == 8) {
      uint64_t orig = static_cast<uint64_t>(v);
      return __builtin_clzll(orig);
    } else {
      DCHECK(sizeof(T) == 16);
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

  /// The purpose of this function is to replicate Java's BigInteger.toByteArray()
  /// function. Receives an int input (it can be any size) and returns a buffer that
  /// represents the byte sequence representation of this number where the most
  /// significant byte is in the zeroth element.
  /// Note, the return value is stored in a string instead of in a vector of chars for
  /// potential optimisations with small strings.
  /// E.g. 520 = [2, 8]
  /// E.g. 12065530 = [0, -72, 26, -6]
  /// E.g. -129 = [-1, 127]
  template<typename T>
  static std::string IntToByteBuffer(T input) {
    std::string buffer;
    T value = input;
    for (int i = 0; i < sizeof(value); ++i) {
      // Applies a mask for a byte range on the input.
      char value_to_save = value & 0XFF;
      buffer.push_back(value_to_save);
      // Remove the just processed part from the input so that we can exit early if there
      // is nothing left to process.
      value >>= 8;
      if (value == 0 && value_to_save >= 0) break;
      if (value == -1 && value_to_save < 0) break;
    }
    std::reverse(buffer.begin(), buffer.end());
    return buffer;
  }
};

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
