// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/string-value.h"
#include <cstring>
#include "util/cpu-info.h"
#include "util/sse-util.h"

using namespace std;

namespace impala {

int StringValue::Compare(const StringValue& other) const {
  if (len == 0 && other.len == 0) return 0;
  if (len == 0) return -1;
  if (other.len == 0) return 1;
  int result = memcmp(ptr, other.ptr, std::min(len, other.len));
  if (result == 0 && len != other.len) {
    return (len < other.len ? -1 : 1);
  } else {
    return result;
  }
}

// Compares two strings using sse4.2 intrinsics.  
// The function below will load data into the SSE registers if possible and compare the
// remainder using memcmp.
bool StringValue::Eq(const StringValue& other) const {
  if (this->len != other.len) return false;
  int len = this->len;
  const char* s1 = this->ptr;
  const char* s2 = other.ptr;
  if (CpuInfo::Instance()->IsSupported(CpuInfo::SSE4_2)) {
    while (len >= SSEUtil::CHARS_PER_128_BIT_REGISTER) {
      __m128i xmm0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s1));
      __m128i xmm1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s2));
      if (_mm_cmpistri(xmm0, xmm1, SSEUtil::STRCMP_MODE) != SSEUtil::CHARS_PER_128_BIT_REGISTER) {
        return false;
      }
      len -= SSEUtil::CHARS_PER_128_BIT_REGISTER;
      s1 += SSEUtil::CHARS_PER_128_BIT_REGISTER;
      s2 += SSEUtil::CHARS_PER_128_BIT_REGISTER;
    }
    if (len >= SSEUtil::CHARS_PER_64_BIT_REGISTER) {
      // Load 64 bits at a time, the upper 64 bits of the xmm register is set to 0
      __m128i xmm0 = _mm_loadl_epi64((__m128i*) s1);
      __m128i xmm1 = _mm_loadl_epi64((__m128i*) s2);
      // The upper bits always match (always 0), hence the comparision to CHAR_PER_128_REGISTER
      if (_mm_cmpistri(xmm0, xmm1, SSEUtil::STRCMP_MODE) != SSEUtil::CHARS_PER_128_BIT_REGISTER) {
        return false;
      }
      len -= SSEUtil::CHARS_PER_64_BIT_REGISTER;
      s1 += SSEUtil::CHARS_PER_64_BIT_REGISTER;
      s2 += SSEUtil::CHARS_PER_64_BIT_REGISTER;
    } 
  }
  return memcmp(s1, s2, len) == 0;
}

string StringValue::DebugString() const {
  return string(ptr, len);
}

ostream& operator<<(ostream& os, const StringValue& string_value) {
  return os << string_value.DebugString();
}

}
