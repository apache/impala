// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <string>

#include <glog/logging.h>
#include "runtime/primitive-type.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"

#ifndef IMPALA_RUNTIME_RAW_VALUE_H
#define IMPALA_RUNTIME_RAW_VALUE_H

namespace impala {

class MemPool;
class SlotDescriptor;
class Tuple;

// Useful utility functions for runtime values (which are passed around as void*).
class RawValue {
 public:
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

  // Returns hash value for 'value' interpreted as 'type'.
  static size_t GetHashValue(const void* value, PrimitiveType type);

  // Compares both values.
  // Return value is < 0  if v1 < v2, 0 if v1 == v2, > 0 if v1 > v2.
  static int Compare(const void* v1, const void* v2, PrimitiveType type);

  // Writes the bytes of a given value into the slot of a tuple.
  // For string values, the string data is copied into memory allocated from 'pool'
  // only if pool is non-NULL.
  static void Write(const void* value, Tuple* tuple, const SlotDescriptor* slot_desc,
                    MemPool* pool);

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
      return *reinterpret_cast<const bool*>(v1) == *reinterpret_cast<const bool*>(v2);
    case TYPE_TINYINT:
      return *reinterpret_cast<const int8_t*>(v1) == *reinterpret_cast<const int8_t*>(v2);
    case TYPE_SMALLINT:
      return *reinterpret_cast<const int16_t*>(v1) == *reinterpret_cast<const int16_t*>(v2);
    case TYPE_INT:
      return *reinterpret_cast<const int32_t*>(v1) == *reinterpret_cast<const int32_t*>(v2);
    case TYPE_BIGINT:
      return *reinterpret_cast<const int64_t*>(v1) == *reinterpret_cast<const int64_t*>(v2);
    case TYPE_FLOAT:
      return *reinterpret_cast<const float*>(v1) == *reinterpret_cast<const float*>(v2);
    case TYPE_DOUBLE:
      return *reinterpret_cast<const double*>(v1) == *reinterpret_cast<const double*>(v2);
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
}

#endif
