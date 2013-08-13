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

  // TODO: this could be faster if we use __builtin_clz
  // Fix this if this ever shows up in a hot path.
  static inline int Log2(uint64_t x) {
    DCHECK_GT(x, 0);
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

  // Write the swapped bytes into dst. len must be 1, 2, 4 or 8.
  static inline void ByteSwap(void* dst, void* src, int len) {
    switch (len) {
      case 1:
        *reinterpret_cast<int8_t*>(dst) = *reinterpret_cast<int8_t*>(src);
        break;
      case 2:
        *reinterpret_cast<int16_t*>(dst) = ByteSwap(*reinterpret_cast<int16_t*>(src));
        break;
      case 4:
        *reinterpret_cast<int32_t*>(dst) = ByteSwap(*reinterpret_cast<int32_t*>(src));
        break;
      case 8:
        *reinterpret_cast<int64_t*>(dst) = ByteSwap(*reinterpret_cast<int64_t*>(src));
        break;
      default: DCHECK(false);
    }
  }

#if __BYTE_ORDER == __LITTLE_ENDIAN
  // Converts to big endian format (if not already in big endian).
  static inline int64_t  BigEndian(int64_t value)  { return ByteSwap(value); }
  static inline uint64_t BigEndian(uint64_t value) { return ByteSwap(value); }
  static inline int32_t  BigEndian(int32_t value)  { return ByteSwap(value); }
  static inline uint32_t BigEndian(uint32_t value) { return ByteSwap(value); }
  static inline int16_t  BigEndian(int16_t value)  { return ByteSwap(value); }
  static inline uint16_t BigEndian(uint16_t value) { return ByteSwap(value); }
#else
  static inline int64_t  BigEndian(int64_t val)  { return val; }
  static inline uint64_t BigEndian(uint64_t val) { return val; }
  static inline int32_t  BigEndian(int32_t val)  { return val; }
  static inline uint32_t BigEndian(uint32_t val) { return val; }
  static inline int16_t  BigEndian(int16_t val)  { return val; }
  static inline uint16_t BigEndian(uint16_t val) { return val; }
#endif

};

}

#endif
