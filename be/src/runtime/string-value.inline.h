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


#ifndef IMPALA_RUNTIME_STRING_VALUE_INLINE_H
#define IMPALA_RUNTIME_STRING_VALUE_INLINE_H

#include "runtime/string-value.h"

#include <cstring>
#include "util/cpu-info.h"
#include "util/sse-util.h"

namespace impala {

/// Compare two strings using sse4.2 intrinsics if they are available. This code assumes
/// that the trivial cases are already handled (i.e. one string is empty).
/// Returns:
///   < 0 if s1 < s2
///   0 if s1 == s2
///   > 0 if s1 > s2
/// The SSE code path is just under 2x faster than the non-sse code path.
///   - s1/n1: ptr/len for the first string
///   - s2/n2: ptr/len for the second string
///   - len: min(n1, n2) - this can be more cheaply passed in by the caller
static inline int StringCompare(const char* s1, int n1, const char* s2, int n2, int len) {
  DCHECK_EQ(len, std::min(n1, n2));
  if (CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
    while (len >= SSEUtil::CHARS_PER_128_BIT_REGISTER) {
      __m128i xmm0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s1));
      __m128i xmm1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s2));
      int chars_match = SSE4_cmpestri(xmm0, SSEUtil::CHARS_PER_128_BIT_REGISTER,
          xmm1, SSEUtil::CHARS_PER_128_BIT_REGISTER, SSEUtil::STRCMP_MODE);
      if (chars_match != SSEUtil::CHARS_PER_128_BIT_REGISTER) {
        return s1[chars_match] - s2[chars_match];
      }
      len -= SSEUtil::CHARS_PER_128_BIT_REGISTER;
      s1 += SSEUtil::CHARS_PER_128_BIT_REGISTER;
      s2 += SSEUtil::CHARS_PER_128_BIT_REGISTER;
    }
  }
  // TODO: for some reason memcmp is way slower than strncmp (2.5x)  why?
  int result = strncmp(s1, s2, len);
  if (result != 0) return result;
  return n1 - n2;
}

inline int StringValue::Compare(const StringValue& other) const {
  int l = std::min(len, other.len);
  if (l == 0) {
    if (len == other.len) {
      return 0;
    } else if (len == 0) {
      return -1;
    } else {
      DCHECK_EQ(other.len, 0);
      return 1;
    }
  }
  return StringCompare(this->ptr, this->len, other.ptr, other.len, l);
}

inline bool StringValue::Eq(const StringValue& other) const {
  if (this->len != other.len) return false;
  return StringCompare(this->ptr, this->len, other.ptr, other.len, this->len) == 0;
}

inline StringValue StringValue::Substring(int start_pos) const {
  return StringValue(ptr + start_pos, len - start_pos);
}

inline StringValue StringValue::Substring(int start_pos, int new_len) const {
  return StringValue(ptr + start_pos, (new_len < 0) ? (len - start_pos) : new_len);
}

inline StringValue StringValue::Trim() const {
  // Remove leading and trailing spaces.
  int32_t begin = 0;
  while (begin < len && ptr[begin] == ' ') {
    ++begin;
  }
  int32_t end = len - 1;
  while (end > begin && ptr[end] == ' ') {
    --end;
  }
  return StringValue(ptr + begin, end - begin + 1);
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

inline char* StringValue::CharSlotToPtr(void* slot, const ColumnType& type) {
  DCHECK(type.type == TYPE_CHAR);
  if (slot == NULL) return NULL;
  if (type.IsVarLenStringType()) {
    StringValue* sv = reinterpret_cast<StringValue*>(slot);
    DCHECK_EQ(sv->len, type.len);
    return sv->ptr;
  }
  return reinterpret_cast<char*>(slot);
}

inline const char* StringValue::CharSlotToPtr(const void* slot, const ColumnType& type) {
  DCHECK(type.type == TYPE_CHAR);
  if (slot == NULL) return NULL;
  if (type.IsVarLenStringType()) {
    const StringValue* sv = reinterpret_cast<const StringValue*>(slot);
    DCHECK_EQ(sv->len, type.len);
    return sv->ptr;
  }
  return reinterpret_cast<const char*>(slot);
}

}
#endif
