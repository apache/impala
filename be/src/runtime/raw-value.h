// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <string>

#include "runtime/primitive-type.h"

#ifndef IMPALA_RUNTIME_RAW_VALUE_H
#define IMPALA_RUNTIME_RAW_VALUE_H

namespace impala {

class MemPool;
class SlotDescriptor;
class Tuple;

// Useful utility functions for runtime values (which are passed around as void*).
class RawValue {
 public:
  // Convert value into ascii and return via 'str'.
  // NULL turns into "NULL".
  static void PrintValue(const void* value, PrimitiveType type, std::string* str);

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

};

}

#endif
