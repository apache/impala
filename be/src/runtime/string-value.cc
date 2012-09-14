// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/string-value.h"
#include <cstring>
#include "util/cpu-info.h"
#include "util/sse-util.h"

using namespace std;

namespace impala {

const char* StringValue::LLVM_CLASS_NAME = "struct.impala::StringValue";

// Compare two strings using sse4.2 intrinsics if they are available. This code assumes
// that the trivial cases are already handled (i.e. one string is empty). 
// Returns:
//   < 0 if s1 < s2
//   0 if s1 == s2
//   > 0 if s1 > s2
// The SSE code path is just under 2x faster than the non-sse code path.
//   - s1/n1: ptr/len for the first string
//   - s2/n2: ptr/len for the second string
//   - len: min(n1, n2) - this can be more cheaply passed in by the caller
static inline int StringCompare(const char* s1, int n1, const char* s2, int n2, int len) {
  DCHECK_EQ(len, min(n1, n2));
  if (CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
    while (len >= SSEUtil::CHARS_PER_128_BIT_REGISTER) {
      __m128i xmm0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s1));
      __m128i xmm1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s2));
      int chars_match = _mm_cmpistri(xmm0, xmm1, SSEUtil::STRCMP_MODE);
      if (chars_match != SSEUtil::CHARS_PER_128_BIT_REGISTER) {
        return s1[chars_match] - s2[chars_match];
      }
      len -= SSEUtil::CHARS_PER_128_BIT_REGISTER;
      s1 += SSEUtil::CHARS_PER_128_BIT_REGISTER;
      s2 += SSEUtil::CHARS_PER_128_BIT_REGISTER;
    }
    if (len >= SSEUtil::CHARS_PER_64_BIT_REGISTER) {
      // Load 64 bits at a time, the upper 64 bits of the xmm register is set to 0
      __m128i xmm0 = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(s1));
      __m128i xmm1 = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(s2));
      // The upper bits always match (always 0), hence the comparison to 
      // CHAR_PER_128_REGISTER
      int chars_match = _mm_cmpistri(xmm0, xmm1, SSEUtil::STRCMP_MODE);
      if (chars_match != SSEUtil::CHARS_PER_128_BIT_REGISTER) {
        return s1[chars_match] - s2[chars_match];
      }
      len -= SSEUtil::CHARS_PER_64_BIT_REGISTER;
      s1 += SSEUtil::CHARS_PER_64_BIT_REGISTER;
      s2 += SSEUtil::CHARS_PER_64_BIT_REGISTER;
    } 
  }
  // TODO: for some reason memcmp is way slower than strncmp (2.5x)  why?
  int result = strncmp(s1, s2, len);
  if (result != 0) return result;
  return n1 - n2;
}

int StringValue::Compare(const StringValue& other) const {
  int l = min(len, other.len);
  if (l == 0) return 0;
  if (len == 0) return -1;
  if (other.len == 0) return 1;
  return StringCompare(this->ptr, this->len, other.ptr, other.len, l);
}

bool StringValue::Eq(const StringValue& other) const {
  if (this->len != other.len) return false;
  return StringCompare(this->ptr, this->len, other.ptr, other.len, this->len) == 0;
}

string StringValue::DebugString() const {
  return string(ptr, len);
}

ostream& operator<<(ostream& os, const StringValue& string_value) {
  return os << string_value.DebugString();
}

StringValue StringValue::Substring(int start_pos) const {
  return StringValue(ptr + start_pos, len - start_pos);
}

StringValue StringValue::Substring(int start_pos, int new_len) const {
  return StringValue(ptr + start_pos, (new_len < 0) ? (len - start_pos) : new_len);
}

StringValue StringValue::Trim() const {
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

}
