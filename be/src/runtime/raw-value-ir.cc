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

#include "runtime/raw-value.h"

#include <cmath>

#include "runtime/decimal-value.inline.h"
#include "runtime/raw-value.inline.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "util/hash-util.h"

using namespace impala;

int IR_ALWAYS_INLINE RawValue::Compare(
    const void* v1, const void* v2, const ColumnType& type) noexcept {
  const StringValue* string_value1;
  const StringValue* string_value2;
  const TimestampValue* ts_value1;
  const TimestampValue* ts_value2;
  const DateValue* date_value1;
  const DateValue* date_value2;
  float f1, f2;
  double d1, d2;
  int32_t i1, i2;
  int64_t b1, b2;
  switch (type.type) {
    case TYPE_NULL:
      return 0;
    case TYPE_BOOLEAN:
      return *reinterpret_cast<const bool*>(v1) - *reinterpret_cast<const bool*>(v2);
    case TYPE_TINYINT:
      return *reinterpret_cast<const int8_t*>(v1) - *reinterpret_cast<const int8_t*>(v2);
    case TYPE_SMALLINT:
      return *reinterpret_cast<const int16_t*>(v1) -
             *reinterpret_cast<const int16_t*>(v2);
    case TYPE_INT:
      i1 = *reinterpret_cast<const int32_t*>(v1);
      i2 = *reinterpret_cast<const int32_t*>(v2);
      return i1 > i2 ? 1 : (i1 < i2 ? -1 : 0);
    case TYPE_DATE:
      date_value1 = reinterpret_cast<const DateValue*>(v1);
      date_value2 = reinterpret_cast<const DateValue*>(v2);
      return *date_value1 > *date_value2 ? 1 : (*date_value1 < *date_value2 ? -1 : 0);
    case TYPE_BIGINT:
      b1 = *reinterpret_cast<const int64_t*>(v1);
      b2 = *reinterpret_cast<const int64_t*>(v2);
      return b1 > b2 ? 1 : (b1 < b2 ? -1 : 0);
    case TYPE_FLOAT:
      // TODO: can this be faster? (just returning the difference has underflow problems)
      f1 = *reinterpret_cast<const float*>(v1);
      f2 = *reinterpret_cast<const float*>(v2);
      if (UNLIKELY(std::isnan(f1) && std::isnan(f2))) return 0;
      if (UNLIKELY(std::isnan(f1))) return -1;
      if (UNLIKELY(std::isnan(f2))) return 1;
      return f1 > f2 ? 1 : (f1 < f2 ? -1 : 0);
    case TYPE_DOUBLE:
      // TODO: can this be faster?
      d1 = *reinterpret_cast<const double*>(v1);
      d2 = *reinterpret_cast<const double*>(v2);
      if (std::isnan(d1) && std::isnan(d2)) return 0;
      if (std::isnan(d1)) return -1;
      if (std::isnan(d2)) return 1;
      return d1 > d2 ? 1 : (d1 < d2 ? -1 : 0);
    case TYPE_STRING:
    case TYPE_VARCHAR:
      string_value1 = reinterpret_cast<const StringValue*>(v1);
      string_value2 = reinterpret_cast<const StringValue*>(v2);
      return string_value1->Compare(*string_value2);
    case TYPE_TIMESTAMP:
      ts_value1 = reinterpret_cast<const TimestampValue*>(v1);
      ts_value2 = reinterpret_cast<const TimestampValue*>(v2);
      return *ts_value1 > *ts_value2 ? 1 : (*ts_value1 < *ts_value2 ? -1 : 0);
    case TYPE_CHAR: {
      const char* v1ptr = reinterpret_cast<const char*>(v1);
      const char* v2ptr = reinterpret_cast<const char*>(v2);
      int64_t l1 = StringValue::UnpaddedCharLength(v1ptr, type.len);
      int64_t l2 = StringValue::UnpaddedCharLength(v2ptr, type.len);
      return StringCompare(v1ptr, l1, v2ptr, l2, std::min(l1, l2));
    }
    case TYPE_DECIMAL:
      switch (type.GetByteSize()) {
        case 4:
          return reinterpret_cast<const Decimal4Value*>(v1)->Compare(
                 *reinterpret_cast<const Decimal4Value*>(v2));
        case 8:
          return reinterpret_cast<const Decimal8Value*>(v1)->Compare(
                 *reinterpret_cast<const Decimal8Value*>(v2));
        case 16:
          return reinterpret_cast<const Decimal16Value*>(v1)->Compare(
                 *reinterpret_cast<const Decimal16Value*>(v2));
        default:
          DCHECK(false) << type;
          return 0;
      }
    default:
      DCHECK(false) << "invalid type: " << type.DebugString();
      return 0;
  };
}

uint32_t IR_ALWAYS_INLINE RawValue::GetHashValue(
    const void* v, const ColumnType& type, uint32_t seed) noexcept {
  // The choice of hash function needs to be consistent across all hosts of the cluster.

  // Use HashCombine with arbitrary constant to ensure we don't return seed.
  if (v == NULL) return HashUtil::HashCombine32(HASH_VAL_NULL, seed);

  switch (type.type) {
    case TYPE_CHAR:
    case TYPE_STRING:
    case TYPE_VARCHAR:
      return RawValue::GetHashValueNonNull<impala::StringValue>(
        reinterpret_cast<const StringValue*>(v), type, seed);
    case TYPE_BOOLEAN:
      return RawValue::GetHashValueNonNull<bool>(
        reinterpret_cast<const bool*>(v), type, seed);
    case TYPE_TINYINT:
      return RawValue::GetHashValueNonNull<int8_t>(
        reinterpret_cast<const int8_t*>(v), type, seed);
    case TYPE_SMALLINT:
      return RawValue::GetHashValueNonNull<int16_t>(
        reinterpret_cast<const int16_t*>(v), type, seed);
    case TYPE_INT:
      return RawValue::GetHashValueNonNull<int32_t>(
        reinterpret_cast<const int32_t*>(v), type, seed);
    case TYPE_DATE:
      return RawValue::GetHashValueNonNull<DateValue>(
        reinterpret_cast<const DateValue*>(v), type, seed);
    case TYPE_BIGINT:
      return RawValue::GetHashValueNonNull<int64_t>(
        reinterpret_cast<const int64_t*>(v), type, seed);
    case TYPE_FLOAT:
      return  RawValue::GetHashValueNonNull<float>(
        reinterpret_cast<const float*>(v), type, seed);
    case TYPE_DOUBLE:
      return RawValue::GetHashValueNonNull<double>(
        reinterpret_cast<const double*>(v), type, seed);
    case TYPE_TIMESTAMP:
      return  RawValue::GetHashValueNonNull<TimestampValue>(
        reinterpret_cast<const TimestampValue*>(v), type, seed);
    case TYPE_DECIMAL:
      switch(type.GetByteSize()) {
        case 4: return
          RawValue::GetHashValueNonNull<Decimal4Value>(
            reinterpret_cast<const impala::Decimal4Value*>(v), type, seed);
        case 8:
          return RawValue::GetHashValueNonNull<Decimal8Value>(
            reinterpret_cast<const Decimal8Value*>(v), type, seed);
        case 16:
          return RawValue::GetHashValueNonNull<Decimal16Value>(
            reinterpret_cast<const Decimal16Value*>(v), type, seed);
        DCHECK(false);
    }
    default:
      DCHECK(false);
      return 0;
  }
}

uint64_t IR_ALWAYS_INLINE RawValue::GetHashValueFastHash(const void* v,
    const ColumnType& type, uint64_t seed) {
  // Hash with an arbitrary constant to ensure we don't return seed.
  if (UNLIKELY(v == nullptr)) {
    return HashUtil::FastHash64(&HASH_VAL_NULL, sizeof(HASH_VAL_NULL), seed);
  }
  switch (type.type) {
    case TYPE_CHAR:
    case TYPE_STRING:
    case TYPE_VARCHAR:
      return RawValue::GetHashValueFastHashNonNull<impala::StringValue>(
          reinterpret_cast<const StringValue*>(v), type, seed);
    case TYPE_BOOLEAN:
      return RawValue::GetHashValueFastHashNonNull<bool>(
          reinterpret_cast<const bool*>(v), type, seed);
    case TYPE_TINYINT:
      return RawValue::GetHashValueFastHashNonNull<int8_t>(
          reinterpret_cast<const int8_t*>(v), type, seed);
    case TYPE_SMALLINT:
      return RawValue::GetHashValueFastHashNonNull<int16_t>(
          reinterpret_cast<const int16_t*>(v), type, seed);
    case TYPE_INT:
      return RawValue::GetHashValueFastHashNonNull<int32_t>(
          reinterpret_cast<const int32_t*>(v), type, seed);
    case TYPE_DATE:
      return RawValue::GetHashValueFastHashNonNull<DateValue>(
          reinterpret_cast<const DateValue*>(v), type, seed);
    case TYPE_BIGINT:
      return RawValue::GetHashValueFastHashNonNull<int64_t>(
          reinterpret_cast<const int64_t*>(v), type, seed);
    case TYPE_FLOAT:
      return RawValue::GetHashValueFastHashNonNull<float>(
          reinterpret_cast<const float*>(v), type, seed);
    case TYPE_DOUBLE:
      return RawValue::GetHashValueFastHashNonNull<double>(
          reinterpret_cast<const double*>(v), type, seed);
    case TYPE_TIMESTAMP:
      return RawValue::GetHashValueFastHashNonNull<TimestampValue>(
          reinterpret_cast<const TimestampValue*>(v), type, seed);
    case TYPE_DECIMAL:
      switch (type.GetByteSize()) {
        case 4:
          return RawValue::GetHashValueFastHashNonNull<Decimal4Value>(
              reinterpret_cast<const impala::Decimal4Value*>(v), type, seed);
        case 8:
          return RawValue::GetHashValueFastHashNonNull<Decimal8Value>(
              reinterpret_cast<const Decimal8Value*>(v), type, seed);
        case 16:
          return RawValue::GetHashValueFastHashNonNull<Decimal16Value>(
              reinterpret_cast<const Decimal16Value*>(v), type, seed);
        default:
          DCHECK(false);
          return 0;
      }
    default:
      DCHECK(false);
      return 0;
  }
}

uint32_t IR_ALWAYS_INLINE RawValue::GetHashValueFastHash32(
    const void* v, const ColumnType& type, uint32_t seed) noexcept {
  // the following trick converts the 64-bit hashcode to Fermat
  // residue, which shall retain information from both the higher
  // and lower parts of hashcode.
  uint64_t h = GetHashValueFastHash(v, type, seed);
  return h - (h >> 32);
}
