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

  // Construct a StringValue from 's'.  's' must be valid for as long as
  // this object is valid.
  StringValue(const std::string& s) 
    : ptr(const_cast<char*>(s.c_str())), len(s.size()) {
  }

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
  // If new_len < 0 then the substring from start_pos to end of string is returned. If
  // new_len > len, len is extended to new_len.
  // TODO: len should never be extended. This is not a trivial fix because UrlParser
  // depends on the current behavior.
  StringValue Substring(int start_pos, int new_len) const;

  // Trims leading and trailing spaces.
  StringValue Trim() const;

  // For C++/IR interop, we need to be able to look up types by name.
  static const char* LLVM_CLASS_NAME;
};

std::ostream& operator<<(std::ostream& os, const StringValue& string_value);

}

#endif
