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
  // Maximum absolute value of int128_t that we use. This is 38 digits of 9's.
  static int128_t MAX_UNSCALED_DECIMAL;

  // Initializes MAX_UNSCALED_DECIMAL. Must be called once before using it.
  static void InitMaxUnscaledDecimal();

  // TODO: do we need to handle overflow here or at a higher abstraction.
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
    // TODO: this should be a lookup table.
    T result = 1;
    for (int i = 0; i < scale; ++i) {
      result *= 10;
    }
    return result;
  }

  // Write decimals as big endian (byte comparable) in fixed_len_size bytes.
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
};

}

#endif
