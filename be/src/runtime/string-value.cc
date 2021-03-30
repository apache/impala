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

const char* StringValue::LLVM_CLASS_NAME = "struct.impala::StringValue";

string StringValue::DebugString() const {
  return string(ptr, len);
}

ostream& operator<<(ostream& os, const StringValue& string_value) {
  return os << string_value.DebugString();
}

uint64_t StringValue::ToUInt64() const {
  unsigned char bytes[8];
  *((uint64_t*)bytes) = 0;
  int chars_to_copy = (len < 8) ? len : 8;
  for (int i = 0; i < chars_to_copy; i++) {
    bytes[i] = static_cast<unsigned char>(ptr[i]);
  }
  return static_cast<uint64_t>(bytes[0]) << 56 | static_cast<uint64_t>(bytes[1]) << 48
      | static_cast<uint64_t>(bytes[2]) << 40 | static_cast<uint64_t>(bytes[3]) << 32
      | static_cast<uint64_t>(bytes[4]) << 24 | static_cast<uint64_t>(bytes[5]) << 16
      | static_cast<uint64_t>(bytes[6]) << 8 | static_cast<uint64_t>(bytes[7]);
}
}
