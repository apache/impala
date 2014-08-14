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

#ifndef IMPALA_UTIL_KEY_NORMALIZER_H_
#define IMPALA_UTIL_KEY_NORMALIZER_H_

#include "exprs/expr.h"

namespace impala {

// Provides support for normalizing Impala expr values into a memcmp-able,
// fixed-length format.
//
// To normalize a key, we first write a null byte (0 if nulls_first, 1 otw),
// followed by the normalized form of the key. We invert the bytes of the key (excluding
// the null byte) if the key should be sorted in descending order. Further, for any
// multi-byte data types, we ensure that the most significant byte is first by
// converting to big endian.
//
// In addition to inverting descending keys and converting to big endian, here is how
// we normalize specific types:
// Integers:
//     Invert the sign bit.
// Floats:
//     Write out the inverted sign bit, followed by the exponent, followed by
//     the fraction. If the float is negative, though, we need to invert both the exponent
//     and fraction (since smaller number means greater actual value when negative).
//     Conveniently, IEEE floating point numbers are already in the correct order.
// Timestamps:
//     32 bits for date: 23 bits for year, 4 bits for month, and 5 bits for day.
//     64 bits for time of day in nanoseconds.
//     All numbers assumed unsigned.
// Strings:
//     Write one character at a time with a null byte at the end (inverted if
//     sort descending). Unlike other data types, we may write partial strings.
//     NOTE: This assumes strings do not contain null characters.
// Booleans/Nulls:
//     Left as-is.
//
// Finally, we pad any remaining bytes of the key with zeroes.
class KeyNormalizer {
 public:
  // Initializes the normalizer with the key exprs and length alloted to each normalized
  // key.
  KeyNormalizer(const std::vector<ExprContext*>& key_exprs_ctxs, int key_len,
      const std::vector<bool>& is_asc, const std::vector<bool>& nulls_first)
      : key_expr_ctxs_(key_expr_ctxs), key_len_(key_len), is_asc_(is_asc),
        nulls_first_(nulls_first) {
  }

  // Normalizes all keys and writes the value into dst.
  // Returns true if we went over the max key size while writing the key.
  // If the return value is true, then key_idx_over_budget will be set to
  // the index of the key expr which went over.
  // TODO: Handle non-nullable columns
  bool NormalizeKey(TupleRow* tuple_row, uint8_t* dst, int* key_idx_over_budget = NULL);

 private:
  // Returns true if we went over the max key size while writing the null bit.
  static bool WriteNullBit(uint8_t null_bit, uint8_t* value, uint8_t* dst,
      int* bytes_left);

  // Stores the given value in the memory address given by dst, after
  // converting to big endian and inverting the value if the sort is descending.
  // Copy of 'value' intentional, we don't want to modify original.
  template <typename ValueType>
  static void StoreFinalValue(ValueType value, void* dst, bool is_asc);

  template <typename IntType>
  static void NormalizeInt(void* src, void* dst, bool is_asc);

  // ResultType should be an integer type of the same size as FloatType, used
  // to examine the bytes of the float.
  template <typename FloatType, typename ResultType>
  static void NormalizeFloat(void* src, void* dst, bool is_asc);

  static void NormalizeTimestamp(uint8_t* src, uint8_t* dst, bool is_asc);

  // Normalizes a sort key value and writes it to dst.
  // Updates bytes_left and returns true if we went over the max key size.
  static bool WriteNormalizedKey(const ColumnType& type, bool is_asc,
      uint8_t* value, uint8_t* dst, int* bytes_left);

  // Normalizes a column by writing a NULL byte and then the normalized value.
  // Updates bytes_left and returns true if we went over the max key size.
  static bool NormalizeKeyColumn(const ColumnType& type, uint8_t null_bit, bool is_asc,
      uint8_t* value, uint8_t* dst, int* bytes_left);

  std::vector<ExprContext*> key_expr_ctxs_;
  int key_len_;
  std::vector<bool> is_asc_;
  std::vector<bool> nulls_first_;
};

}

#endif
