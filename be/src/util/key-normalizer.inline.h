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

#ifndef IMPALA_UTIL_KEY_NORMALIZER_INLINE_H_
#define IMPALA_UTIL_KEY_NORMALIZER_INLINE_H_

#include "util/key-normalizer.h"

#include <boost/date_time/gregorian/gregorian_types.hpp>

#include "runtime/descriptors.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "util/bit-util.h"

namespace impala {

inline bool KeyNormalizer::WriteNullBit(uint8_t null_bit, uint8_t* value, uint8_t* dst,
    int* bytes_left) {
  // If there's not enough space for the null byte, return.
  if (*bytes_left < 1) return true;
  *dst = (value == NULL ? null_bit : !null_bit);
  --*bytes_left;
  return false;
}

template <typename ValueType>
inline void KeyNormalizer::StoreFinalValue(ValueType value, void* dst, bool is_asc) {
  if (sizeof(ValueType) > 1) value = BitUtil::ToBigEndian(value);
  if (!is_asc) value = ~value;
  memcpy(dst, &value, sizeof(ValueType));
}

template <typename IntType>
inline void KeyNormalizer::NormalizeInt(void* src, void* dst, bool is_asc) {
  const int num_bits = 8 * sizeof(IntType);
  IntType sign_bit = (1LL << (num_bits - 1));

  IntType value = *(reinterpret_cast<IntType*>(src));
  value = (sign_bit ^ value);
  StoreFinalValue<IntType>(value, dst, is_asc);
}

template <typename FloatType, typename ResultType>
inline void KeyNormalizer::NormalizeFloat(void* src, void* dst, bool is_asc) {
  DCHECK_EQ(sizeof(FloatType), sizeof(ResultType));

  const int num_bits = 8 * sizeof(FloatType);
  const ResultType sign_bit = (1LL << (num_bits - 1));

  ResultType value = *(reinterpret_cast<ResultType*>(src));
  if (value & sign_bit) {
    // If the sign is negative, we'll end up inverting the whole thing.
    value = ~value;
  } else {
    // Otherwise, just invert the sign bit.
    value = (sign_bit ^ value);
  }
  StoreFinalValue<ResultType>(value, dst, is_asc);
}

inline void KeyNormalizer::NormalizeTimestamp(uint8_t* src, uint8_t* dst, bool is_asc) {
  TimestampValue timestamp = *(reinterpret_cast<TimestampValue*>(src));

  // Need 5 bits for day and 4 bits for month. Rest given to year.
  boost::gregorian::date::ymd_type ymd = timestamp.date().year_month_day();
  uint32_t date = ymd.day | (ymd.month << 5) | (ymd.year << 9);
  StoreFinalValue<uint32_t>(date, dst, is_asc);

  // Write time of day in nanoseconds in the next slot.
  uint64_t time_ns = timestamp.time_of_day().total_nanoseconds();
  StoreFinalValue<uint64_t>(time_ns, dst + sizeof(date), is_asc);
}

inline bool KeyNormalizer::WriteNormalizedKey(const ColumnType& type, bool is_asc,
    uint8_t* value, uint8_t* dst, int* bytes_left) {
  // Expend bytes_left or fail if we don't have enough.
  // Variable-length data types (i.e., strings) account for themselves.
  int byte_size = type.GetByteSize();
  if (byte_size != 0) {
    if (*bytes_left >= byte_size) {
      *bytes_left -= byte_size;
    } else {
      return true;
    }
  }

  switch(type.type) {
    case TYPE_BIGINT:
      NormalizeInt<int64_t>(value, dst, is_asc);
      break;
    case TYPE_INT:
      NormalizeInt<int32_t>(value, dst, is_asc);
      break;
    case TYPE_SMALLINT:
      NormalizeInt<int16_t>(value, dst, is_asc);
      break;
    case TYPE_TINYINT:
      NormalizeInt<int8_t>(value, dst, is_asc);
      break;

    case TYPE_DOUBLE:
      NormalizeFloat<double, uint64_t>(value, dst, is_asc);
      break;
    case TYPE_FLOAT:
      NormalizeFloat<float, uint32_t>(value, dst, is_asc);
      break;

    case TYPE_TIMESTAMP:
      NormalizeTimestamp(value, dst, is_asc);
      break;

    case TYPE_STRING:
    case TYPE_VARCHAR: {
      StringValue* string_val = reinterpret_cast<StringValue*>(value);

      // Copy the string over, with an additional NULL at the end.
      int size = std::min(string_val->len, *bytes_left);
      for (int i = 0; i < size; ++i) {
        StoreFinalValue<uint8_t>(string_val->ptr[i], dst + i, is_asc);
      }
      *bytes_left -= size;

      if (*bytes_left == 0) return true;

      StoreFinalValue<uint8_t>(0, dst + size, is_asc);
      --*bytes_left;
      return false;
    }

    case TYPE_BOOLEAN:
      StoreFinalValue<uint8_t>(*reinterpret_cast<uint8_t*>(value), dst, is_asc);
      break;
    case TYPE_NULL:
      StoreFinalValue<uint8_t>(0, dst, is_asc);
      break;
    default:
      DCHECK(false) << "Value type not supported for normalization";
  }

  return false;
}

inline bool KeyNormalizer::NormalizeKeyColumn(const ColumnType& type, uint8_t null_bit,
    bool is_asc, uint8_t* value, uint8_t* dst, int* bytes_left) {
  bool went_over = WriteNullBit(null_bit, value, dst, bytes_left);
  if (went_over || value == NULL) return went_over;
  return WriteNormalizedKey(type, is_asc, value, dst + 1, bytes_left);
}

inline bool KeyNormalizer::NormalizeKey(TupleRow* row, uint8_t* dst,
    int* key_idx_over_budget) {
  int bytes_left = key_len_;
  for (int i = 0; i < key_expr_ctxs_.size(); ++i) {
    uint8_t* key = reinterpret_cast<uint8_t*>(key_expr_ctxs_[i]->GetValue(row));
    int offset = key_len_ - bytes_left;
    bool went_over = NormalizeKeyColumn(key_expr_ctxs_[i]->root()->type(),
        !nulls_first_[i], is_asc_[i], key, dst + offset, &bytes_left);
    if (went_over) {
      if (key_idx_over_budget != NULL) *key_idx_over_budget = i;
      return true;
    }
  }

  // Zero out any unused bytes of the sort key.
  int offset = key_len_ - bytes_left;
  bzero(dst + offset, bytes_left);

  return false;
}

}

#endif
