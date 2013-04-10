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

#include "common/logging.h"
#include "runtime/primitive-type.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "util/hash-util.h"

namespace impala {

class MemPool;
class SlotDescriptor;
class Tuple;

// Useful utility functions for runtime values (which are passed around as void*).
class RawValue {
 public:
  // Ascii output precision for double/float
  static const int ASCII_PRECISION;

  // Convert value into ascii and write to 'stream'.
  // NULL turns into "NULL".
  static void PrintValue(const void* value, PrimitiveType type,
                         std::stringstream* stream);

  // Convert value into ascii and return via 'str'.
  // NULL turns into "NULL".
  static void PrintValue(const void* value, PrimitiveType type, std::string* str);

  // Writes the byte representation of a value to a stringstream character-by-character
  static void PrintValueAsBytes(const void* value, PrimitiveType type,
                                std::stringstream* stream);

  // Returns hash value for 'value' interpreted as 'type'.  The resulting hash value
  // is combined with the seed value.
  static uint32_t GetHashValue(const void* value, PrimitiveType type, uint32_t seed = 0);

  // Get the hash value using the fvn hash function.  Using different seeds with FVN
  // results in different hash functions.  GetHashValue() does not have this property
  // and cannot be safely used as the first step in data repartitioning.
  // However, GetHashValue() can be significantly faster.
  // TODO: fix GetHashValue
  static uint32_t GetHashValueFvn(const void* value, PrimitiveType type, uint32_t seed);

  // Compares both values.
  // Return value is < 0  if v1 < v2, 0 if v1 == v2, > 0 if v1 > v2.
  static int Compare(const void* v1, const void* v2, PrimitiveType type);

  // Writes the bytes of a given value into the slot of a tuple.
  // For string values, the string data is copied into memory allocated from 'pool'
  // only if pool is non-NULL.
  static void Write(const void* value, Tuple* tuple, const SlotDescriptor* slot_desc,
                    MemPool* pool);

  // Writes 'src' into 'dst' for type.  
  // For string values, the string data is copied into 'pool' if pool is non-NULL.
  // src must be non-NULL.
  static void Write(const void* src, void* dst, PrimitiveType type, MemPool* pool);

  // Returns true if v1 == v2.
  // This is more performant than Compare() == 0 for string equality, mostly because of
  // the length comparison check.
  static bool Eq(const void* v1, const void* v2, PrimitiveType type);
};

inline bool RawValue::Eq(const void* v1, const void* v2, PrimitiveType type) {
  const StringValue* string_value1;
  const StringValue* string_value2;
  switch (type) {
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
      string_value1 = reinterpret_cast<const StringValue*>(v1);
      string_value2 = reinterpret_cast<const StringValue*>(v2);
      return string_value1->Eq(*string_value2);
    case TYPE_TIMESTAMP:
      return *reinterpret_cast<const TimestampValue*>(v1) ==
          *reinterpret_cast<const TimestampValue*>(v2);
    default:
      DCHECK(false) << "invalid type: " << TypeToString(type);
      return 0;
  };
}

// Use boost::hash_combine for corner cases.  boost::hash_combine is reimplemented 
// here to use int32t's (instead of size_t)
// boost::hash_combine does:
//  seed ^= v + 0x9e3779b9 + (seed << 6) + (seed >> 2);
inline uint32_t RawValue::GetHashValue(const void* v, PrimitiveType type, uint32_t seed) {
  // Hash_combine with v = 0
  if (v == NULL) {
    uint32_t value = 0x9e3779b9;
    return seed ^ (value + (seed << 6) + (seed >> 2));
  }
  switch (type) {
    case TYPE_STRING: {
      const StringValue* string_value = reinterpret_cast<const StringValue*>(v);
      return HashUtil::Hash(string_value->ptr, string_value->len, seed);
    }
    case TYPE_BOOLEAN: {
      uint32_t value = *reinterpret_cast<const bool*>(v) + 0x9e3779b9;
      return seed ^ (value + (seed << 6) + (seed >> 2));
    }
    case TYPE_TINYINT:
      return HashUtil::Hash(v, 1, seed);
    case TYPE_SMALLINT:
      return HashUtil::Hash(v, 2, seed);
    case TYPE_INT:
      return HashUtil::Hash(v, 4, seed);
    case TYPE_BIGINT:
      return HashUtil::Hash(v, 8, seed);
    case TYPE_FLOAT:
      return HashUtil::Hash(v, 4, seed);
    case TYPE_DOUBLE:
      return HashUtil::Hash(v, 8, seed);
    case TYPE_TIMESTAMP:
      return HashUtil::Hash(v, 12, seed);
    default:
      DCHECK(false) << "invalid type: " << TypeToString(type);
      return 0;
  }
}

inline uint32_t RawValue::GetHashValueFvn(const void* v, PrimitiveType type, 
    uint32_t seed) {
  // Hash_combine with v = 0
  if (v == NULL) {
    uint32_t value = 0x9e3779b9;
    return seed ^ (value + (seed << 6) + (seed >> 2));
  }
  switch (type) {
    case TYPE_STRING: {
      const StringValue* string_value = reinterpret_cast<const StringValue*>(v);
      return HashUtil::FvnHash(string_value->ptr, string_value->len, seed);
    }
    case TYPE_BOOLEAN: {
      uint32_t value = *reinterpret_cast<const bool*>(v) + 0x9e3779b9;
      return seed ^ (value + (seed << 6) + (seed >> 2));
    }
    case TYPE_TINYINT:
      return HashUtil::FvnHash(v, 1, seed);
    case TYPE_SMALLINT:
      return HashUtil::FvnHash(v, 2, seed);
    case TYPE_INT:
      return HashUtil::FvnHash(v, 4, seed);
    case TYPE_BIGINT:
      return HashUtil::FvnHash(v, 8, seed);
    case TYPE_FLOAT:
      return HashUtil::FvnHash(v, 4, seed);
    case TYPE_DOUBLE:
      return HashUtil::FvnHash(v, 8, seed);
    case TYPE_TIMESTAMP:
      return HashUtil::FvnHash(v, 12, seed);
    default:
      DCHECK(false) << "invalid type: " << TypeToString(type);
      return 0;
  }
}

}

#endif
