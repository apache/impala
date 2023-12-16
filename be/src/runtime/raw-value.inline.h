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


#ifndef IMPALA_RUNTIME_RAW_VALUE_INLINE_H
#define IMPALA_RUNTIME_RAW_VALUE_INLINE_H

#include "runtime/raw-value.h"

#include <cmath>

#include <boost/functional/hash.hpp>

#include "common/logging.h"
#include "runtime/date-value.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "util/hash-util.h"

namespace impala {

/// Arbitrary constants used to compute hash values for special cases. Constants were
/// obtained by taking lower bytes of generated UUID. NULL and empty strings should
/// hash to different values.
static const uint32_t HASH_VAL_NULL = 0x58081667;
static const uint32_t HASH_VAL_EMPTY = 0x7dca7eee;

inline bool RawValue::IsNaN(const void* val, const ColumnType& type) {
  switch(type.type) {
  case TYPE_FLOAT:
    return std::isnan(*reinterpret_cast<const float*>(val));
  case TYPE_DOUBLE:
    return std::isnan(*reinterpret_cast<const double*>(val));
  default:
    return false;
  }
}

inline bool RawValue::IsFloatingZero(const void* val, const ColumnType& type) {
  switch(type.type) {
  case TYPE_FLOAT:
    return *reinterpret_cast<const float*>(val) == CANONICAL_FLOAT_ZERO;
  case TYPE_DOUBLE:
    return *reinterpret_cast<const double*>(val) == CANONICAL_DOUBLE_ZERO;
  default:
    return false;
  }
}

inline const void* RawValue::CanonicalValue(const void* val, const ColumnType& type) {
  if (RawValue::IsNaN(val, type)) return RawValue::CanonicalNaNValue(type);
  if (RawValue::IsFloatingZero(val, type)) return RawValue::PositiveFloatingZero(type);
  return val;
}

inline const void* RawValue::CanonicalNaNValue(const ColumnType& type) {
  switch(type.type) {
  case TYPE_FLOAT:
    return &CANONICAL_FLOAT_NAN;
  case TYPE_DOUBLE:
    return &CANONICAL_DOUBLE_NAN;
  default:
    DCHECK(false);
    return nullptr;
  }
}

inline const void* RawValue::PositiveFloatingZero(const ColumnType& type) {
  switch(type.type) {
  case TYPE_FLOAT:
    return &CANONICAL_FLOAT_ZERO;
  case TYPE_DOUBLE:
    return &CANONICAL_DOUBLE_ZERO;
  default:
    DCHECK(false);
    return nullptr;
  }
}

inline bool RawValue::Eq(const void* v1, const void* v2, const ColumnType& type) {
  const StringValue* string_value1;
  const StringValue* string_value2;
  switch (type.type) {
    case TYPE_BOOLEAN:
      return *reinterpret_cast<const bool*>(v1)
          == *reinterpret_cast<const bool*>(v2);
    case TYPE_TINYINT:
      return *reinterpret_cast<const int8_t*>(v1)
          == *reinterpret_cast<const int8_t*>(v2);
    case TYPE_SMALLINT:
      return *reinterpret_cast<const int16_t*>(v1)
          == *reinterpret_cast<const int16_t*>(v2);
    case TYPE_INT:
      return *reinterpret_cast<const int32_t*>(v1)
          == *reinterpret_cast<const int32_t*>(v2);
    case TYPE_DATE:
      return *reinterpret_cast<const DateValue*>(v1)
          == *reinterpret_cast<const DateValue*>(v2);
    case TYPE_BIGINT:
      return *reinterpret_cast<const int64_t*>(v1)
          == *reinterpret_cast<const int64_t*>(v2);
    case TYPE_FLOAT:
      return *reinterpret_cast<const float*>(v1)
          == *reinterpret_cast<const float*>(v2);
    case TYPE_DOUBLE:
      return *reinterpret_cast<const double*>(v1)
          == *reinterpret_cast<const double*>(v2);
    case TYPE_STRING:
    case TYPE_VARCHAR:
      string_value1 = reinterpret_cast<const StringValue*>(v1);
      string_value2 = reinterpret_cast<const StringValue*>(v2);
      return string_value1->Eq(*string_value2);
    case TYPE_TIMESTAMP:
      return *reinterpret_cast<const TimestampValue*>(v1) ==
          *reinterpret_cast<const TimestampValue*>(v2);
    case TYPE_CHAR: {
      const char* v1ptr = reinterpret_cast<const char*>(v1);
      const char* v2ptr = reinterpret_cast<const char*>(v2);
      int64_t l1 = StringValue::UnpaddedCharLength(v1ptr, type.len);
      int64_t l2 = StringValue::UnpaddedCharLength(v2ptr, type.len);
      return StringCompare(v1ptr, l1, v2ptr, l2, std::min(l1, l2)) == 0;
    }
    case TYPE_DECIMAL:
      switch (type.GetByteSize()) {
        case 4:
          return reinterpret_cast<const Decimal4Value*>(v1)->value()
              == reinterpret_cast<const Decimal4Value*>(v2)->value();
        case 8:
          return reinterpret_cast<const Decimal8Value*>(v1)->value()
              == reinterpret_cast<const Decimal8Value*>(v2)->value();
        case 16:
          return reinterpret_cast<const Decimal16Value*>(v1)->value()
              == reinterpret_cast<const Decimal16Value*>(v2)->value();
        default:
          DCHECK(false) << "Unknown decimal byte size: " << type.GetByteSize();
          return 0;
      }
    default:
      DCHECK(false) << type;
      return 0;
  }
}

template<>
inline uint32_t RawValue::GetHashValueNonNull<bool>(const bool* v,
    const ColumnType& type, uint32_t seed) {
  DCHECK_EQ(type.type, TYPE_BOOLEAN);
  DCHECK(v != NULL);
  return HashUtil::HashCombine32(*reinterpret_cast<const bool*>(v), seed);
}

template<>
inline uint32_t RawValue::GetHashValueNonNull<int8_t>(const int8_t* v,
    const ColumnType& type, uint32_t seed) {
  DCHECK_EQ(type.type, TYPE_TINYINT);
  DCHECK(v != NULL);
  return HashUtil::MurmurHash2_64(v, 1, seed);
}

template<>
inline uint32_t RawValue::GetHashValueNonNull<int16_t>(const int16_t* v,
    const ColumnType& type, uint32_t seed) {
  DCHECK_EQ(type.type, TYPE_SMALLINT);
  DCHECK(v != NULL);
  return HashUtil::MurmurHash2_64(v, 2, seed);
}

template<>
inline uint32_t RawValue::GetHashValueNonNull<int32_t>(const int32_t* v,
    const ColumnType& type, uint32_t seed) {
  DCHECK_EQ(type.type, TYPE_INT);
  DCHECK(v != NULL);
  return HashUtil::MurmurHash2_64(v, 4, seed);
}

template<>
inline uint32_t RawValue::GetHashValueNonNull<int64_t>(const int64_t* v,
    const ColumnType& type, uint32_t seed) {
  DCHECK_EQ(type.type, TYPE_BIGINT);
  DCHECK(v != NULL);
  return HashUtil::MurmurHash2_64(v, 8, seed);
}

template<>
inline uint32_t RawValue::GetHashValueNonNull<float>(const float* v,
    const ColumnType& type, uint32_t seed) {
  DCHECK_EQ(type.type, TYPE_FLOAT);
  DCHECK(v != NULL);
  if (std::isnan(*v)) v = &RawValue::CANONICAL_FLOAT_NAN;
  return HashUtil::MurmurHash2_64(v, 4, seed);
}

template<>
inline uint32_t RawValue::GetHashValueNonNull<double>(const double* v,
    const ColumnType& type, uint32_t seed) {
  DCHECK_EQ(type.type, TYPE_DOUBLE);
  DCHECK(v != NULL);
  if (std::isnan(*v)) v = &RawValue::CANONICAL_DOUBLE_NAN;
  return HashUtil::MurmurHash2_64(v, 8, seed);
}

template<>
inline uint32_t RawValue::GetHashValueNonNull<impala::StringValue>(
    const impala::StringValue* v,const ColumnType& type, uint32_t seed) {
  DCHECK(v != NULL);
  if (type.type == TYPE_CHAR) {
    // This is a inlined CHAR(n) slot.
    // TODO: this is a bit wonky since it's not really a StringValue*. Handle CHAR(n)
    // in a separate function.
    return HashUtil::MurmurHash2_64(v, type.len, seed);
  } else {
    DCHECK(type.type == TYPE_STRING || type.type == TYPE_VARCHAR);
    StringValue::SimpleString s = v->ToSimpleString();
    if (s.len == 0) {
      return HashUtil::HashCombine32(HASH_VAL_EMPTY, seed);
    }
    return HashUtil::MurmurHash2_64(s.ptr, s.len, seed);
  }
}

template<>
inline uint32_t RawValue::GetHashValueNonNull<TimestampValue>(
    const TimestampValue* v, const ColumnType& type, uint32_t seed) {
  DCHECK_EQ(type.type, TYPE_TIMESTAMP);
  DCHECK(v != NULL);
  return HashUtil::MurmurHash2_64(v, 12, seed);
}

template<>
inline uint32_t RawValue::GetHashValueNonNull<DateValue>(const DateValue* v,
    const ColumnType& type, uint32_t seed) {
  DCHECK_EQ(type.type, TYPE_DATE);
  DCHECK(v != NULL);
  return HashUtil::MurmurHash2_64(v, 4, seed);
}

template<>
inline uint32_t RawValue::GetHashValueNonNull<Decimal4Value>(
    const Decimal4Value* v, const ColumnType& type, uint32_t seed) {
  DCHECK_EQ(type.type, TYPE_DECIMAL);
  DCHECK(v != NULL);
  return HashUtil::MurmurHash2_64(v, 4, seed);
}

template<>
inline uint32_t RawValue::GetHashValueNonNull<Decimal8Value>(
    const Decimal8Value* v, const ColumnType& type, uint32_t seed) {
  DCHECK_EQ(type.type, TYPE_DECIMAL);
  DCHECK(v != NULL);
  return HashUtil::MurmurHash2_64(v, 8, seed);
}

template<>
inline uint32_t RawValue::GetHashValueNonNull<Decimal16Value>(
    const Decimal16Value* v, const ColumnType& type, uint32_t seed) {
  DCHECK_EQ(type.type, TYPE_DECIMAL);
  DCHECK(v != NULL);
  return HashUtil::MurmurHash2_64(v, 16, seed);
}

template<typename T>
inline uint32_t RawValue::GetHashValue(const T* v, const ColumnType& type,
    uint32_t seed) noexcept {
  // Use HashCombine with arbitrary constant to ensure we don't return seed.
  if (UNLIKELY(v == NULL)) return HashUtil::HashCombine32(HASH_VAL_NULL, seed);
  return RawValue::GetHashValueNonNull<T>(v, type, seed);
}

template <>
inline uint64_t RawValue::GetHashValueFastHashNonNull<bool>(
    const bool* v, const ColumnType& type, uint64_t seed) {
  DCHECK_EQ(type.type, TYPE_BOOLEAN);
  DCHECK(v != NULL);
  return HashUtil::FastHash64(v, 1, seed);
}

template <>
inline uint64_t RawValue::GetHashValueFastHashNonNull<int8_t>(
    const int8_t* v, const ColumnType& type, uint64_t seed) {
  DCHECK_EQ(type.type, TYPE_TINYINT);
  DCHECK(v != NULL);
  return HashUtil::FastHash64(v, 1, seed);
}

template <>
inline uint64_t RawValue::GetHashValueFastHashNonNull<int16_t>(
    const int16_t* v, const ColumnType& type, uint64_t seed) {
  DCHECK_EQ(type.type, TYPE_SMALLINT);
  DCHECK(v != NULL);
  return HashUtil::FastHash64(v, 2, seed);
}

template <>
inline uint64_t RawValue::GetHashValueFastHashNonNull<int32_t>(
    const int32_t* v, const ColumnType& type, uint64_t seed) {
  DCHECK_EQ(type.type, TYPE_INT);
  DCHECK(v != NULL);
  return HashUtil::FastHash64(v, 4, seed);
}

template <>
inline uint64_t RawValue::GetHashValueFastHashNonNull<int64_t>(
    const int64_t* v, const ColumnType& type, uint64_t seed) {
  DCHECK_EQ(type.type, TYPE_BIGINT);
  DCHECK(v != NULL);
  return HashUtil::FastHash64(v, 8, seed);
}

template <>
inline uint64_t RawValue::GetHashValueFastHashNonNull<float>(
    const float* v, const ColumnType& type, uint64_t seed) {
  DCHECK_EQ(type.type, TYPE_FLOAT);
  DCHECK(v != NULL);
  if (std::isnan(*v)) v = &RawValue::CANONICAL_FLOAT_NAN;
  return HashUtil::FastHash64(v, 4, seed);
}

template <>
inline uint64_t RawValue::GetHashValueFastHashNonNull<double>(
    const double* v, const ColumnType& type, uint64_t seed) {
  DCHECK_EQ(type.type, TYPE_DOUBLE);
  DCHECK(v != NULL);
  if (std::isnan(*v)) v = &RawValue::CANONICAL_DOUBLE_NAN;
  return HashUtil::FastHash64(v, 8, seed);
}

template <>
inline uint64_t RawValue::GetHashValueFastHashNonNull<impala::StringValue>(
    const impala::StringValue* v, const ColumnType& type, uint64_t seed) {
  DCHECK(v != NULL);
  if (type.type == TYPE_CHAR) {
    // This is a inlined CHAR(n) slot.
    // TODO: this is a bit wonky since it's not really a StringValue*. Handle CHAR(n)
    // in a separate function.
    return HashUtil::FastHash64(v, type.len, seed);
  } else {
    DCHECK(type.type == TYPE_STRING || type.type == TYPE_VARCHAR);
    StringValue::SimpleString s = v->ToSimpleString();
    if (s.len == 0) {
      return HashUtil::FastHash64(&HASH_VAL_EMPTY, sizeof(HASH_VAL_EMPTY), seed);
    }
    return HashUtil::FastHash64(s.ptr, static_cast<size_t>(s.len), seed);
  }
}

template <>
inline uint64_t RawValue::GetHashValueFastHashNonNull<TimestampValue>(
    const TimestampValue* v, const ColumnType& type, uint64_t seed) {
  DCHECK_EQ(type.type, TYPE_TIMESTAMP);
  DCHECK(v != NULL);
  return HashUtil::FastHash64(v, 12, seed);
}

template <>
inline uint64_t RawValue::GetHashValueFastHashNonNull<DateValue>(
    const DateValue* v, const ColumnType& type, uint64_t seed) {
  DCHECK_EQ(type.type, TYPE_DATE);
  DCHECK(v != NULL);
  return HashUtil::FastHash64(v, 4, seed);
}

template <>
inline uint64_t RawValue::GetHashValueFastHashNonNull<Decimal4Value>(
    const Decimal4Value* v, const ColumnType& type, uint64_t seed) {
  DCHECK_EQ(type.type, TYPE_DECIMAL);
  DCHECK(v != NULL);
  return HashUtil::FastHash64(v, 4, seed);
}

template <>
inline uint64_t RawValue::GetHashValueFastHashNonNull<Decimal8Value>(
    const Decimal8Value* v, const ColumnType& type, uint64_t seed) {
  DCHECK_EQ(type.type, TYPE_DECIMAL);
  DCHECK(v != NULL);
  return HashUtil::FastHash64(v, 8, seed);
}

template <>
inline uint64_t RawValue::GetHashValueFastHashNonNull<Decimal16Value>(
    const Decimal16Value* v, const ColumnType& type, uint64_t seed) {
  DCHECK_EQ(type.type, TYPE_DECIMAL);
  DCHECK(v != NULL);
  return HashUtil::FastHash64(v, 16, seed);
}

template <typename T>
inline uint64_t RawValue::GetHashValueFastHash(
    const T* v, const ColumnType& type, uint64_t seed) {
  // Hash with an arbitrary constant to ensure we don't return seed.
  if (UNLIKELY(v == NULL)) {
    return HashUtil::FastHash64(&HASH_VAL_NULL, sizeof(HASH_VAL_NULL), seed);
  }
  return RawValue::GetHashValueFastHashNonNull<T>(v, type, seed);
}
}

#endif
