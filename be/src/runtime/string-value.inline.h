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


#ifndef IMPALA_RUNTIME_STRING_VALUE_INLINE_H
#define IMPALA_RUNTIME_STRING_VALUE_INLINE_H

#include "runtime/string-value.h"

#include <cstring>

namespace impala {

/// Compare two strings. Returns:
///   < 0 if s1 < s2
///   0 if s1 == s2
///   > 0 if s1 > s2
///
///   - s1/n1: ptr/len for the first string
///   - s2/n2: ptr/len for the second string
///   - len: min(n1, n2) - this can be more cheaply passed in by the caller
static inline int StringCompare(const char* s1, int n1, const char* s2, int n2, int len) {
  // memcmp has undefined behavior when called on nullptr for either pointer
  //
  // GCC gives a warning about overflowing the size argument of memcmp, because
  // it thinks 'len' can be negative. 'len' is never negative, so, this just uses
  // len <= 0 and returns 0 for that case to avoid the warning.
  const int result = (len <= 0) ? 0 : memcmp(s1, s2, len);
  if (result != 0) return result;
  return n1 - n2;
}

inline int StringValue::Compare(const StringValue& other) const {
  SimpleString this_s = ToSimpleString();
  SimpleString other_s = other.ToSimpleString();
  int l = std::min(this_s.len, other_s.len);
  if (l == 0) {
    if (this_s.len == other_s.len) {
      return 0;
    } else if (this_s.len == 0) {
      return -1;
    } else {
      DCHECK_EQ(other_s.len, 0);
      return 1;
    }
  }
  return StringCompare(this_s.ptr, this_s.len, other_s.ptr, other_s.len, l);
}

inline bool StringValue::Eq(const StringValue& other) const {
  SimpleString this_s = ToSimpleString();
  SimpleString other_s = other.ToSimpleString();
  if (this_s.len != other_s.len) return false;
  return StringCompare(this_s.ptr, this_s.len, other_s.ptr, other_s.len, this_s.len) == 0;
}

inline bool StringValue::operator==(const StringValue& other) const {
  return Eq(other);
}

inline bool StringValue::Ne(const StringValue& other) const {
  return !Eq(other);
}

inline bool StringValue::operator!=(const StringValue& other) const {
  return Ne(other);
}

inline bool StringValue::Le(const StringValue& other) const {
  return Compare(other) <= 0;
}

inline bool StringValue::operator<=(const StringValue& other) const {
  return Le(other);
}

inline bool StringValue::Ge(const StringValue& other) const {
  return Compare(other) >= 0;
}

inline bool StringValue::operator>=(const StringValue& other) const {
  return Ge(other);
}

inline bool StringValue::Lt(const StringValue& other) const {
  return Compare(other) < 0;
}

inline bool StringValue::operator<(const StringValue& other) const {
  return Lt(other);
}

inline bool StringValue::Gt(const StringValue& other) const {
  return Compare(other) > 0;
}

inline bool StringValue::operator>(const StringValue& other) const {
  return Gt(other);
}

inline StringValue StringValue::Substring(int start_pos) const {
  SimpleString this_s = ToSimpleString();
  return StringValue(this_s.ptr + start_pos, this_s.len - start_pos);
}

inline StringValue StringValue::Substring(int start_pos, int new_len) const {
  SimpleString this_s = ToSimpleString();
  return StringValue(this_s.ptr + start_pos,
      (new_len < 0) ? (this_s.len - start_pos) : new_len);
}

inline StringValue StringValue::Trim() const {
  SimpleString this_s = ToSimpleString();
  // Remove leading and trailing spaces.
  int32_t begin = 0;
  while (begin < this_s.len && this_s.ptr[begin] == ' ') {
    ++begin;
  }
  int32_t end = this_s.len - 1;
  while (end > begin && this_s.ptr[end] == ' ') {
    --end;
  }
  return StringValue(this_s.ptr + begin, end - begin + 1);
}

inline void StringValue::PadWithSpaces(char* cptr, int64_t cptr_len, int64_t num_chars) {
  DCHECK(cptr != NULL);
  DCHECK_GE(cptr_len, 1);
  DCHECK_GE(cptr_len, num_chars);
  memset(&cptr[num_chars], ' ', cptr_len - num_chars);
}

inline int64_t StringValue::UnpaddedCharLength(const char* cptr, int64_t len) {
  DCHECK(cptr != NULL);
  DCHECK_GE(len, 0);
  int64_t last = len - 1;
  while (last >= 0 && cptr[last] == ' ') --last;
  return last + 1;
}
}
#endif
