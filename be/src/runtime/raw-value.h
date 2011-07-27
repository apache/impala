// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <string>

#include "runtime/primitive-type.h"

#ifndef IMPALA_RUNTIME_RAW_VALUE_H
#define IMPALA_RUNTIME_RAW_VALUE_H

namespace impala {

// Useful utility functions for runtime values (which are passed around as void*).
class RawValue {
 public:
  // Convert value into ascii and return via 'str'.
  // NULL turns into "NULL".
  static void PrintValue(void* value, PrimitiveType type, std::string* str);
};

}

#endif
