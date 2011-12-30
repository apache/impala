// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_STRING_VALUE_H
#define IMPALA_RUNTIME_STRING_VALUE_H

#include <string>

namespace impala {

// The format of a string-typed slot.
struct StringValue {
  // TODO: change ptr to an offset relative to a contiguous memory block,
  // so that we can send row batches between nodes without having to swizzle
  // pointers
  char* ptr;
  int len;

  StringValue(char* ptr, int len): ptr(ptr), len(len) {}
  StringValue(): ptr(NULL), len(0) {}

  // Byte-by-byte comparison. Returns:
  // this < other: -1
  // this == other: 0
  // this > other: 1
  int Compare(const StringValue& other) const;

  // ==
  bool Eq(const StringValue& other) const;
  // !=
  bool Ne(const StringValue& other) const { return Compare(other) != 0; }
  // <=
  bool Le(const StringValue& other) const { return Compare(other) <= 0; }
  // >=
  bool Ge(const StringValue& other) const { return Compare(other) >= 0; }
  // <
  bool Lt(const StringValue& other) const { return Compare(other) < 0; }
  // >
  bool Gt(const StringValue& other) const { return Compare(other) > 0; }

  std::string DebugString() const;
};

std::ostream& operator<<(std::ostream& os, const StringValue& string_value);

}

#endif
