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

#include "exec/read-write-util.h"

using namespace std;
using namespace impala;

// This function is not inlined because it can potentially cause LLVM to crash (see
// http://llvm.org/bugs/show_bug.cgi?id=19315), and inlining does not appear to have any
// performance impact.
int64_t ReadWriteUtil::ReadZLong(uint8_t** buf) {
  uint64_t zlong = 0;
  int shift = 0;
  bool more;
  do {
    DCHECK_LE(shift, 64);
    zlong |= static_cast<uint64_t>(**buf & 0x7f) << shift;
    shift += 7;
    more = (**buf & 0x80) != 0;
    ++(*buf);
  } while (more);
  return (zlong >> 1) ^ -(zlong & 1);
}

int ReadWriteUtil::PutZInt(int32_t integer, uint8_t* buf) {
  // Move the sign bit to the first bit.
  uint32_t uinteger = (integer << 1) ^ (integer >> 31);
  const int mask = 0x7f;
  const int cont = 0x80;
  buf[0] = uinteger & mask;
  int len = 1;
  while ((uinteger >>= 7) != 0) {
    // Set the continuation bit.
    buf[len - 1] |= cont;
    buf[len] = uinteger & mask;
    ++len;
  }

  return len;
}

int ReadWriteUtil::PutZLong(int64_t longint, uint8_t* buf) {
  // Move the sign bit to the first bit.
  uint64_t ulongint = (longint << 1) ^ (longint >> 63);
  const int mask = 0x7f;
  const int cont = 0x80;
  buf[0] = ulongint & mask;
  int len = 1;
  while ((ulongint >>= 7) != 0) {
    // Set the continuation bit.
    buf[len - 1] |= cont;
    buf[len] = ulongint & mask;
    ++len;
  }

  return len;
}

string ReadWriteUtil::HexDump(const uint8_t* buf, int64_t length) {
  stringstream ss;
  ss << std::hex;
  for (int i = 0; i < length; ++i) {
    ss << static_cast<int>(buf[i]) << " ";
  }
  return ss.str();
}

string ReadWriteUtil::HexDump(const char* buf, int64_t length) {
  return HexDump(reinterpret_cast<const uint8_t*>(buf), length);
}
