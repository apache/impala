// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_STRING_VALUE_H
#define IMPALA_RUNTIME_STRING_VALUE_H

#include <cstring>
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"

namespace impala {

// The format of a string-typed slot.
struct StringValue {
  // TODO: change ptr to an offset relative to a contiguous memory block,
  // so that we can send row batches between nodes without having to swizzle
  // pointers
  void* ptr;
  int len;

  StringValue(void* ptr, int len): ptr(ptr), len(len) {}
  StringValue(): ptr(NULL), len(0) {}

  // Byte-by-byte comparison. Returns:
  // this < other: -1
  // this == other: 0
  // this > other: 1
  int Compare(const StringValue& other);

  // ==
  bool Eq(const StringValue& other) { return Compare(other) == 0; }
  // !=
  bool Ne(const StringValue& other) { return Compare(other) != 0; }
  // <=
  bool Le(const StringValue& other) { return Compare(other) <= 0; }
  // >=
  bool Ge(const StringValue& other) { return Compare(other) >= 0; }
  // <
  bool Lt(const StringValue& other) { return Compare(other) < 0; }
  // >
  bool Gt(const StringValue& other) { return Compare(other) > 0; }
};

}

#endif
