// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_STRING_VALUE_H
#define IMPALA_RUNTIME_STRING_VALUE_H

#include <string>

namespace impala {

// The format of a string-typed slot.
// The returned StringValue of all functions that return StringValue
// shares its buffer the parent.
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
  bool operator==(const StringValue& other) const { return Eq(other); }
  // !=
  bool Ne(const StringValue& other) const { return !Eq(other); }
  // <=
  bool Le(const StringValue& other) const { return Compare(other) <= 0; }
  // >=
  bool Ge(const StringValue& other) const { return Compare(other) >= 0; }
  // <
  bool Lt(const StringValue& other) const { return Compare(other) < 0; }
  // >
  bool Gt(const StringValue& other) const { return Compare(other) > 0; }

  std::string DebugString() const;

  // Returns the substring starting at start_pos until the end of string.
  StringValue Substring(int start_pos) const;

  // Returns the substring starting at start_pos with given length.
  // If new_len < 0 then the substring from start_pos to end of string is returned.
  StringValue Substring(int start_pos, int new_len) const;

  // Trims leading and trailing spaces.
  StringValue Trim() const;

  // For C++/IR interop, we need to be able to look up types by name.
  static const char* LLVM_CLASS_NAME;
};

std::ostream& operator<<(std::ostream& os, const StringValue& string_value);

}

#endif
