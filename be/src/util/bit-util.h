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

#include <endian.h>

#include "common/compiler-util.h"
#include "util/cpu-info.h"

namespace impala {

// Utility class to do standard bit tricks
// TODO: is this in boost or something else like that?
class BitUtil {
 public:
  // Returns the ceil of value/divisor
  static inline int Ceil(int value, int divisor) {
    return value / divisor + (value % divisor != 0);
  }

  // Returns 'value' rounded up to the nearest multiple of 'factor'
  static inline int RoundUp(int value, int factor) {
    return (value + (factor - 1)) / factor * factor;
  }

  // Returns 'value' rounded down to the nearest multiple of 'factor'
  static inline int RoundDown(int value, int factor) {
    return (value / factor) * factor;
  }

  // Returns the smallest power of two that contains v. Taken from
  // http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
  static inline int64_t NextPowerOfTwo(int64_t v) {
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

  // Returns 'value' rounded up to the nearest multiple of 'factor' when factor is
  // a power of two
  static inline int RoundUpToPowerOf2(int value, int factor) {
    DCHECK((factor > 0) && ((factor & (factor - 1)) == 0));
    return (value + (factor - 1)) & ~(factor - 1);
  }

  static inline int RoundDownToPowerOf2(int value, int factor) {
    DCHECK((factor > 0) && ((factor & (factor - 1)) == 0));
    return value & ~(factor - 1);
  }

  // Specialized round up and down functions for frequently used factors,
  // like 8 (bits->bytes), 32 (bits->i32), and 64 (bits->i64).
  // Returns the rounded up number of bytes that fit the number of bits.
  static inline uint32_t RoundUpNumBytes(uint32_t bits) {
    return (bits + 7) >> 3;
  }

  // Returns the rounded down number of bytes that fit the number of bits.
  static inline uint32_t RoundDownNumBytes(uint32_t bits) {
    return bits >> 3;
  }

  // Returns the rounded up to 32 multiple. Used for conversions of bits to i32.
  static inline uint32_t RoundUpNumi32(uint32_t bits) {
    return (bits + 31) >> 5;
  }

  // Returns the rounded up 32 multiple.
  static inline uint32_t RoundDownNumi32(uint32_t bits) {
    return bits >> 5;
  }

  // Returns the rounded up to 64 multiple. Used for conversions of bits to i64.
  static inline uint32_t RoundUpNumi64(uint32_t bits) {
    return (bits + 63) >> 6;
  }

  // Returns the rounded down to 64 multiple.
  static inline uint32_t RoundDownNumi64(uint32_t bits) {
    return bits >> 6;
  }

  // Non hw accelerated pop count.
  // TODO: we don't use this in any perf sensitive code paths currently.  There
  // might be a much faster way to implement this.
  static inline int PopcountNoHw(uint64_t x) {
    int count = 0;
    for (; x != 0; ++count) x &= x-1;
    return count;
  }

  // Returns the number of set bits in x
  static inline int Popcount(uint64_t x) {
    if (LIKELY(CpuInfo::IsSupported(CpuInfo::POPCNT))) {
      return __builtin_popcountl(x);
    } else {
      return PopcountNoHw(x);
    }
  }

  // Returns the 'num_bits' least-significant bits of 'v'.
  static inline uint64_t TrailingBits(uint64_t v, int num_bits) {
    if (UNLIKELY(num_bits == 0)) return 0;
    if (UNLIKELY(num_bits >= 64)) return v;
    int n = 64 - num_bits;
    return (v << n) >> n;
  }

  // Returns ceil(log2(x)).
  // TODO: this could be faster if we use __builtin_clz.  Fix this if this ever shows up
  // in a hot path.
  static inline int Log2(uint64_t x) {
    DCHECK_GT(x, 0);
    if (x == 1) return 0;
    // Compute result = ceil(log2(x))
    //                = floor(log2(x - 1)) + 1, for x > 1
    // by finding the position of the most significant bit (1-indexed) of x - 1
    // (floor(log2(n)) = MSB(n) (0-indexed))
    --x;
    int result = 1;
    while (x >>= 1) ++result;
    return result;
  }

  // Swaps the byte order (i.e. endianess)
  static inline int64_t ByteSwap(int64_t value) {
    return __builtin_bswap64(value);
  }
  static inline uint64_t ByteSwap(uint64_t value) {
    return static_cast<uint64_t>(__builtin_bswap64(value));
  }
  static inline int32_t ByteSwap(int32_t value) {
    return __builtin_bswap32(value);
  }
  static inline uint32_t ByteSwap(uint32_t value) {
    return static_cast<uint32_t>(__builtin_bswap32(value));
  }
  static inline int16_t ByteSwap(int16_t value) {
    return (((value >> 8) & 0xff) | ((value & 0xff) << 8));
  }
  static inline uint16_t ByteSwap(uint16_t value) {
    return static_cast<uint16_t>(ByteSwap(static_cast<int16_t>(value)));
  }

  // Write the swapped bytes into dst. Src and st cannot overlap.
  static inline void ByteSwap(void* dst, const void* src, int len) {
    switch (len) {
      case 1:
        *reinterpret_cast<int8_t*>(dst) = *reinterpret_cast<const int8_t*>(src);
        return;
      case 2:
        *reinterpret_cast<int16_t*>(dst) =
            ByteSwap(*reinterpret_cast<const int16_t*>(src));
        return;
      case 4:
        *reinterpret_cast<int32_t*>(dst) =
            ByteSwap(*reinterpret_cast<const int32_t*>(src));
        return;
      case 8:
        *reinterpret_cast<int64_t*>(dst) =
            ByteSwap(*reinterpret_cast<const int64_t*>(src));
        return;
      default: break;
    }

    uint8_t* d = reinterpret_cast<uint8_t*>(dst);
    const uint8_t* s = reinterpret_cast<const uint8_t*>(src);
    for (int i = 0; i < len; ++i) {
      d[i] = s[len - i - 1];
    }
  }

  // Converts to big endian format (if not already in big endian) from the
  // machine's native endian format.
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

  // Converts from big endian format to the machine's native endian format.
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

};

}

#endif
