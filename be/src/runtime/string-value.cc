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

#include "runtime/string-value.h"
#include <cstring>

#include "common/names.h"

namespace impala {

const char* StringValue::LLVM_CLASS_NAME = "class.impala::StringValue";

string StringValue::DebugString() const {
  return string(Ptr(), Len());
}

ostream& operator<<(ostream& os, const StringValue& string_value) {
  return os << string_value.DebugString();
}

uint64_t StringValue::ToUInt64() const {
  unsigned char bytes[8];
  *((uint64_t*)bytes) = 0;
  uint32_t len = Len();
  const char* ptr = Ptr();
  int chars_to_copy = (len < 8) ? len : 8;
  for (int i = 0; i < chars_to_copy; i++) {
    bytes[i] = static_cast<unsigned char>(ptr[i]);
  }
  return static_cast<uint64_t>(bytes[0]) << 56 | static_cast<uint64_t>(bytes[1]) << 48
      | static_cast<uint64_t>(bytes[2]) << 40 | static_cast<uint64_t>(bytes[3]) << 32
      | static_cast<uint64_t>(bytes[4]) << 24 | static_cast<uint64_t>(bytes[5]) << 16
      | static_cast<uint64_t>(bytes[6]) << 8 | static_cast<uint64_t>(bytes[7]);
}

string StringValue::LargestSmallerString() const {
  uint32_t len = Len();
  if (len == 0) return "";
  const char* ptr = Ptr();
  if (len == 1 && ptr[0] == 0x00) return "";

  int i = len - 1;
  while (i >= 0 && ptr[i] == 0x00) i--;
  if (UNLIKELY(i == -1)) {
    // All characters are 0x00. Return a string with len-1 0x00 chars.
    return string(len - 1, 0x00);
  }

  // i is pointing at a character != 0x00.
  if (i < len - 1) {
    // return 'this' without the last trailing 0x00
    return string(ptr, len - 1);
  }
  DCHECK_EQ(i, len - 1);

  // Copy characters of this in [0, i] to 'result' and perform a '-1' operation on the
  // ith char.
  string result;
  result.reserve(i + 1);
  // copy all i characters in [0, i-1] to 'result'
  result.append(ptr, i);
  // append a char which is ptr[i]-1
  result.append(1, (uint8_t)(ptr[i]) - 1);
  return result;
}

string StringValue::LeastLargerString() const {
  uint32_t len = Len();
  const char* ptr = Ptr();
  if (len == 0) return string("\00", 1);
  int i = len - 1;
  while (i >= 0 && ptr[i] == (int8_t)0xff) i--;
  if (UNLIKELY(i == -1)) {
    // All characters are 0xff.
    // Return a string with these many 0xff chars plus one 0x00 char
    string result;
    result.reserve(len + 1);
    result.append(len, 0xff);
    result.append(1, 0x00);
    return result;
  }
  // i is pointing at a character != 0xff.
  // Copy characters of this in [0, i] to 'result' and perform a '+1' operation on the
  // ith char.
  string result;
  result.reserve(i + 1);
  // copy all i characters in [0, i-1] to 'result'
  result.append(ptr, i);
  // append a char which is ptr[i]+1
  result.append(1, (uint8_t)(ptr[i]) + 1);
  return result;
}
}
