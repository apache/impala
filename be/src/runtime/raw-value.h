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


#ifndef IMPALA_RUNTIME_RAW_VALUE_H
#define IMPALA_RUNTIME_RAW_VALUE_H

#include <string>

#include <boost/functional/hash.hpp>
#include <math.h>

#include "common/logging.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "runtime/types.h"
#include "util/hash-util.h"

namespace impala {

class MemPool;
class SlotDescriptor;
class Tuple;

/// Useful utility functions for runtime values (which are passed around as void*).
class RawValue {
 public:
  /// Ascii output precision for double/float
  static const int ASCII_PRECISION;

  /// Convert 'value' into ascii and write to 'stream'. NULL turns into "NULL". 'scale'
  /// determines how many digits after the decimal are printed for floating point numbers,
  /// -1 indicates to use the stream's current formatting.
  /// TODO: for string types, we just print the result regardless of whether or not it
  /// ascii. This could be undesirable.
  static void PrintValue(const void* value, const ColumnType& type, int scale,
                         std::stringstream* stream);

  /// Write ascii value to string instead of stringstream.
  static void PrintValue(const void* value, const ColumnType& type, int scale,
                         std::string* str);

  /// Writes the byte representation of a value to a stringstream character-by-character
  static void PrintValueAsBytes(const void* value, const ColumnType& type,
                                std::stringstream* stream);

  /// Returns hash value for 'v' interpreted as 'type'.  The resulting hash value
  /// is combined with the seed value.
  static uint32_t GetHashValue(const void* v, const ColumnType& type, uint32_t seed = 0);

  /// Get a 32-bit hash value using the FNV hash function.
  /// Using different seeds with FNV results in different hash functions.
  /// GetHashValue() does not have this property and cannot be safely used as the first
  /// step in data repartitioning. However, GetHashValue() can be significantly faster.
  /// TODO: fix GetHashValue
  static uint32_t GetHashValueFnv(const void* v, const ColumnType& type, uint32_t seed);

  /// Compares both values.
  /// Return value is < 0  if v1 < v2, 0 if v1 == v2, > 0 if v1 > v2.
  static int Compare(const void* v1, const void* v2, const ColumnType& type);

  /// Writes the bytes of a given value into the slot of a tuple.
  /// For string values, the string data is copied into memory allocated from 'pool'
  /// only if pool is non-NULL.
  static void Write(const void* value, Tuple* tuple, const SlotDescriptor* slot_desc,
                    MemPool* pool);

  /// Writes 'src' into 'dst' for type.
  /// For string values, the string data is copied into 'pool' if pool is non-NULL.
  /// src must be non-NULL.
  static void Write(const void* src, void* dst, const ColumnType& type, MemPool* pool);

  /// Writes 'src' into 'dst' for type.
  /// String values are copied into *buffer and *buffer is updated by the length. *buf
  /// must be preallocated to be large enough.
  static void Write(const void* src, const ColumnType& type, void* dst, uint8_t** buf);

  /// Returns true if v1 == v2.
  /// This is more performant than Compare() == 0 for string equality, mostly because of
  /// the length comparison check.
  static bool Eq(const void* v1, const void* v2, const ColumnType& type);
};

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
      const char* v1ptr = StringValue::CharSlotToPtr(v1, type);
      const char* v2ptr = StringValue::CharSlotToPtr(v2, type);
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
          break;
      }
    default:
      DCHECK(false) << type;
      return 0;
  };
}

/// Arbitrary constants used to compute hash values for special cases. Constants were
/// obtained by taking lower bytes of generated UUID. NULL and empty strings should
/// hash to different values.
static const uint32_t HASH_VAL_NULL = 0x58081667;
static const uint32_t HASH_VAL_EMPTY = 0x7dca7eee;

inline uint32_t RawValue::GetHashValue(const void* v, const ColumnType& type,
    uint32_t seed) {
  // Use HashCombine with arbitrary constant to ensure we don't return seed.
  if (v == NULL) return HashUtil::HashCombine32(HASH_VAL_NULL, seed);

  switch (type.type) {
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      const StringValue* string_value = reinterpret_cast<const StringValue*>(v);
      if (string_value->len == 0) {
        return HashUtil::HashCombine32(HASH_VAL_EMPTY, seed);
      }
      return HashUtil::Hash(string_value->ptr, string_value->len, seed);
    }
    case TYPE_BOOLEAN:
      return HashUtil::HashCombine32(*reinterpret_cast<const bool*>(v), seed);
    case TYPE_TINYINT: return HashUtil::Hash(v, 1, seed);
    case TYPE_SMALLINT: return HashUtil::Hash(v, 2, seed);
    case TYPE_INT: return HashUtil::Hash(v, 4, seed);
    case TYPE_BIGINT: return HashUtil::Hash(v, 8, seed);
    case TYPE_FLOAT: return HashUtil::Hash(v, 4, seed);
    case TYPE_DOUBLE: return HashUtil::Hash(v, 8, seed);
    case TYPE_TIMESTAMP: return HashUtil::Hash(v, 12, seed);
    case TYPE_CHAR: return HashUtil::Hash(StringValue::CharSlotToPtr(v, type),
                                          type.len, seed);
    case TYPE_DECIMAL: return HashUtil::Hash(v, type.GetByteSize(), seed);
    default:
      DCHECK(false);
      return 0;
  }
}

inline uint32_t RawValue::GetHashValueFnv(const void* v, const ColumnType& type,
    uint32_t seed) {
  // Use HashCombine with arbitrary constant to ensure we don't return seed.
  if (v == NULL) return HashUtil::HashCombine32(HASH_VAL_NULL, seed);

  switch (type.type ) {
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      const StringValue* string_value = reinterpret_cast<const StringValue*>(v);
      if (string_value->len == 0) {
        return HashUtil::HashCombine32(HASH_VAL_EMPTY, seed);
      }
      return HashUtil::FnvHash64to32(string_value->ptr, string_value->len, seed);
    }
    case TYPE_BOOLEAN:
      return HashUtil::HashCombine32(*reinterpret_cast<const bool*>(v), seed);
    case TYPE_TINYINT: return HashUtil::FnvHash64to32(v, 1, seed);
    case TYPE_SMALLINT: return HashUtil::FnvHash64to32(v, 2, seed);
    case TYPE_INT: return HashUtil::FnvHash64to32(v, 4, seed);
    case TYPE_BIGINT: return HashUtil::FnvHash64to32(v, 8, seed);
    case TYPE_FLOAT: return HashUtil::FnvHash64to32(v, 4, seed);
    case TYPE_DOUBLE: return HashUtil::FnvHash64to32(v, 8, seed);
    case TYPE_TIMESTAMP: return HashUtil::FnvHash64to32(v, 12, seed);
    case TYPE_CHAR: return HashUtil::FnvHash64to32(StringValue::CharSlotToPtr(v, type),
                                                   type.len, seed);
    case TYPE_DECIMAL: return HashUtil::FnvHash64to32(v, type.GetByteSize(), seed);
    default:
      DCHECK(false);
      return 0;
  }
}

inline void RawValue::PrintValue(const void* value, const ColumnType& type, int scale,
    std::stringstream* stream) {
  if (value == NULL) {
    *stream << "NULL";
    return;
  }

  int old_precision = stream->precision();
  std::ios_base::fmtflags old_flags = stream->flags();
  if (scale > -1) {
    stream->precision(scale);
    // Setting 'fixed' causes precision to set the number of digits printed after the
    // decimal (by default it sets the maximum number of digits total).
    *stream << std::fixed;
  }

  const StringValue* string_val = NULL;
  switch (type.type) {
    case TYPE_BOOLEAN: {
      bool val = *reinterpret_cast<const bool*>(value);
      *stream << (val ? "true" : "false");
      return;
    }
    case TYPE_TINYINT:
      // Extra casting for chars since they should not be interpreted as ASCII.
      *stream << static_cast<int>(*reinterpret_cast<const int8_t*>(value));
      break;
    case TYPE_SMALLINT:
      *stream << *reinterpret_cast<const int16_t*>(value);
      break;
    case TYPE_INT:
      *stream << *reinterpret_cast<const int32_t*>(value);
      break;
    case TYPE_BIGINT:
      *stream << *reinterpret_cast<const int64_t*>(value);
      break;
    case TYPE_FLOAT:
      {
        float val = *reinterpret_cast<const float*>(value);
        if (LIKELY(std::isfinite(val))) {
          *stream << val;
        } else if (isinf(val)) {
          // 'Infinity' is Java's text representation of inf. By staying close to Java, we
          // allow Hive to read text tables containing non-finite values produced by
          // Impala. (The same logic applies to 'NaN', below).
          *stream << (val < 0 ? "-Infinity" : "Infinity");
        } else if (isnan(val)) {
          *stream << "NaN";
        }
      }
      break;
    case TYPE_DOUBLE:
      {
        double val = *reinterpret_cast<const double*>(value);
        if (LIKELY(std::isfinite(val))) {
          *stream << val;
        } else if (isinf(val)) {
          // See TYPE_FLOAT for rationale.
          *stream << (val < 0 ? "-Infinity" : "Infinity");
        } else if (isnan(val)) {
          *stream << "NaN";
        }
      }
      break;
    case TYPE_VARCHAR:
    case TYPE_STRING:
      string_val = reinterpret_cast<const StringValue*>(value);
      if (type.type == TYPE_VARCHAR) DCHECK(string_val->len <= type.len);
      stream->write(string_val->ptr, string_val->len);
      break;
    case TYPE_TIMESTAMP:
      *stream << *reinterpret_cast<const TimestampValue*>(value);
      break;
    case TYPE_CHAR:
      stream->write(StringValue::CharSlotToPtr(value, type), type.len);
      break;
    case TYPE_DECIMAL:
      switch (type.GetByteSize()) {
        case 4:
          *stream << reinterpret_cast<const Decimal4Value*>(value)->ToString(type);
          break;
        case 8:
          *stream << reinterpret_cast<const Decimal8Value*>(value)->ToString(type);
          break;
        case 16:
          *stream << reinterpret_cast<const Decimal16Value*>(value)->ToString(type);
          break;
        default:
          DCHECK(false) << type;
      }
      break;
    default:
      DCHECK(false);
  }
  stream->precision(old_precision);
  // Undo setting stream to fixed
  stream->flags(old_flags);
}

}

#endif
