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


#ifndef IMPALA_BIT_UTIL_H
#define IMPALA_BIT_UTIL_H

#if defined(__APPLE__)
#include <machine/endian.h>
#else
#include <endian.h>
#endif

#include <boost/type_traits/make_unsigned.hpp>
#include <limits>

#include "common/compiler-util.h"
#include "util/cpu-info.h"
#include "util/sse-util.h"

namespace impala {

using boost::make_unsigned;

/// Utility class to do standard bit tricks
/// TODO: is this in boost or something else like that?
class BitUtil {
 public:
  /// Returns the ceil of value/divisor
  static inline int64_t Ceil(int64_t value, int64_t divisor) {
    return value / divisor + (value % divisor != 0);
  }

  /// Returns 'value' rounded up to the nearest multiple of 'factor'
  static inline int64_t RoundUp(int64_t value, int64_t factor) {
    return (value + (factor - 1)) / factor * factor;
  }

  /// Returns 'value' rounded down to the nearest multiple of 'factor'
  static inline int64_t RoundDown(int64_t value, int64_t factor) {
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

  /// Returns 'value' rounded up to the nearest multiple of 'factor' when factor is
  /// a power of two
  static inline int RoundUpToPowerOf2(int value, int factor) {
    DCHECK((factor > 0) && ((factor & (factor - 1)) == 0));
    return (value + (factor - 1)) & ~(factor - 1);
  }

  static inline int RoundDownToPowerOf2(int value, int factor) {
    DCHECK((factor > 0) && ((factor & (factor - 1)) == 0));
    return value & ~(factor - 1);
  }

  /// Specialized round up and down functions for frequently used factors,
  /// like 8 (bits->bytes), 32 (bits->i32), and 64 (bits->i64).
  /// Returns the rounded up number of bytes that fit the number of bits.
  static inline uint32_t RoundUpNumBytes(uint32_t bits) {
    return (bits + 7) >> 3;
  }

  /// Returns the rounded down number of bytes that fit the number of bits.
  static inline uint32_t RoundDownNumBytes(uint32_t bits) {
    return bits >> 3;
  }

  /// Returns the rounded up to 32 multiple. Used for conversions of bits to i32.
  static inline uint32_t RoundUpNumi32(uint32_t bits) {
    return (bits + 31) >> 5;
  }

  /// Returns the rounded up 32 multiple.
  static inline uint32_t RoundDownNumi32(uint32_t bits) {
    return bits >> 5;
  }

  /// Returns the rounded up to 64 multiple. Used for conversions of bits to i64.
  static inline uint32_t RoundUpNumi64(uint32_t bits) {
    return (bits + 63) >> 6;
  }

  /// Returns the rounded down to 64 multiple.
  static inline uint32_t RoundDownNumi64(uint32_t bits) {
    return bits >> 6;
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
    return BitUtil::Popcount(static_cast<typename make_unsigned<T>::type>(v));
  }

  /// Returns the 'num_bits' least-significant bits of 'v'.
  static inline uint64_t TrailingBits(uint64_t v, int num_bits) {
    if (UNLIKELY(num_bits == 0)) return 0;
    if (UNLIKELY(num_bits >= 64)) return v;
    int n = 64 - num_bits;
    return (v << n) >> n;
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
  static inline int64_t  ToBigEndian(int64_t value)  { return ByteSwap(value); }
  static inline uint64_t ToBigEndian(uint64_t value) { return ByteSwap(value); }
  static inline int32_t  ToBigEndian(int32_t value)  { return ByteSwap(value); }
  static inline uint32_t ToBigEndian(uint32_t value) { return ByteSwap(value); }
  static inline int16_t  ToBigEndian(int16_t value)  { return ByteSwap(value); }
  static inline uint16_t ToBigEndian(uint16_t value) { return ByteSwap(value); }
#else
  static inline int64_t  ToBigEndian(int64_t val)  { return val; }
  static inline uint64_t ToBigEndian(uint64_t val) { return val; }
  static inline int32_t  ToBigEndian(int32_t val)  { return val; }
  static inline uint32_t ToBigEndian(uint32_t val) { return val; }
  static inline int16_t  ToBigEndian(int16_t val)  { return val; }
  static inline uint16_t ToBigEndian(uint16_t val) { return val; }
#endif

  /// Converts from big endian format to the machine's native endian format.
#if __BYTE_ORDER == __LITTLE_ENDIAN
  static inline int64_t  FromBigEndian(int64_t value)  { return ByteSwap(value); }
  static inline uint64_t FromBigEndian(uint64_t value) { return ByteSwap(value); }
  static inline int32_t  FromBigEndian(int32_t value)  { return ByteSwap(value); }
  static inline uint32_t FromBigEndian(uint32_t value) { return ByteSwap(value); }
  static inline int16_t  FromBigEndian(int16_t value)  { return ByteSwap(value); }
  static inline uint16_t FromBigEndian(uint16_t value) { return ByteSwap(value); }
#else
  static inline int64_t  FromBigEndian(int64_t val)  { return val; }
  static inline uint64_t FromBigEndian(uint64_t val) { return val; }
  static inline int32_t  FromBigEndian(int32_t val)  { return val; }
  static inline uint32_t FromBigEndian(uint32_t val) { return val; }
  static inline int16_t  FromBigEndian(int16_t val)  { return val; }
  static inline uint16_t FromBigEndian(uint16_t val) { return val; }
#endif

  /// Returns true if 'value' is a non-negative 32-bit integer.
  static inline bool IsNonNegative32Bit(int64_t value) {
    return static_cast<uint64_t>(value) <= std::numeric_limits<int32_t>::max();
  }

  /// Logical right shift for signed integer types
  /// This is needed because the C >> operator does arithmetic right shift
  /// Negative shift amounts lead to undefined behavior
  template<typename T>
  static T ShiftRightLogical(T v, int shift) {
    // Conversion to unsigned ensures most significant bits always filled with 0's
    return static_cast<typename make_unsigned<T>::type>(v) >> shift;
  }

  /// Get an specific bit of a numeric type
  template<typename T>
  static inline int8_t GetBit(T v, int bitpos) {
    T masked = v & (static_cast<T>(0x1) << bitpos);
    return static_cast<int8_t>(ShiftRightLogical(masked, bitpos));
  }

  /// Set a specific bit to 1
  /// Behavior when bitpos is negative is undefined
  template<typename T>
  static T SetBit(T v, int bitpos) {
    return v | (static_cast<T>(0x1) << bitpos);
  }

  /// Set a specific bit to 0
  /// Behavior when bitpos is negative is undefined
  template<typename T>
  static T UnsetBit(T v, int bitpos) {
    return v & ~(static_cast<T>(0x1) << bitpos);
  }
};

}

#endif
