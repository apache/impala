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

#include "exec/read-write-util.h"

#include "common/names.h"

using namespace impala;

namespace {

// Returns MAX_ZLONG_LEN + 1 if the encoded int is more than MAX_ZLONG_LEN bytes long,
// otherwise returns the length of the encoded int. Reads MAX_ZLONG_LEN bytes in 'buf'.
int FindZIntegerLength(uint8_t* buf) {
  uint64_t x = *reinterpret_cast<uint64_t*>(buf);
  for (int i = 0; i < sizeof(x); ++i) {
    if ((x & (0x80LL << (i * 8))) == 0) return i + 1;
  }
  uint16_t y = *reinterpret_cast<uint16_t*>(buf + 8);
  if ((y & 0x80) == 0) return 9;
  if ((y & 0x8000) == 0) return 10;
  return 11;
}

// Slow path for ReadZInteger() that checks for out-of-bounds on every byte
template <int MAX_LEN, typename ZResultType>
ZResultType ReadZIntegerSlow(uint8_t** buf, uint8_t* buf_end) {
  uint64_t zlong = 0;
  int shift = 0;
  bool more = true;
  for (int i = 0; more && i < MAX_LEN; ++i) {
    if (UNLIKELY(*buf >= buf_end)) return ZResultType::error();
    DCHECK_LE(shift, 64);
    zlong |= static_cast<uint64_t>(**buf & 0x7f) << shift;
    shift += 7;
    more = (**buf & 0x80) != 0;
    ++(*buf);
  }
  // Invalid int that's longer than maximum
  if (UNLIKELY(more)) return ZResultType::error();
  return ZResultType((zlong >> 1) ^ -(zlong & 1));
}

}

// This function is not inlined because it can potentially cause LLVM to crash (see
// http://llvm.org/bugs/show_bug.cgi?id=19315), and inlining does not appear to have any
// performance impact.
template <int MAX_LEN, typename ZResultType>
ZResultType ReadWriteUtil::ReadZInteger(uint8_t** buf, uint8_t* buf_end) {
  DCHECK(MAX_LEN == MAX_ZINT_LEN || MAX_LEN == MAX_ZLONG_LEN);

  // Use MAX_ZLONG_LEN rather than MAX_LEN since FindZIntegerLength() always assumes at
  // least MAX_ZLONG_LEN bytes in buffer.
  if (UNLIKELY(buf_end - *buf < MAX_ZLONG_LEN)) {
    return ReadZIntegerSlow<MAX_LEN, ZResultType>(buf, buf_end);
  }
  // Once we get here, we don't need to worry about going off end of buffer.
  int num_bytes = FindZIntegerLength(*buf);
  if (UNLIKELY(num_bytes > MAX_LEN)) return ZResultType::error();

  uint64_t zlong = 0;
  int shift = 0;
  for (int i = 0; i < num_bytes; ++i) {
    zlong |= static_cast<uint64_t>(**buf & 0x7f) << shift;
    shift += 7;
    ++(*buf);
  }
  return ZResultType((zlong >> 1) ^ -(zlong & 1));
}

// Instantiate the template for long and int.
template ReadWriteUtil::ZLongResult
ReadWriteUtil::ReadZInteger<ReadWriteUtil::MAX_ZLONG_LEN, ReadWriteUtil::ZLongResult>(
    uint8_t** buf, uint8_t* buf_end);
template ReadWriteUtil::ZIntResult
ReadWriteUtil::ReadZInteger<ReadWriteUtil::MAX_ZINT_LEN, ReadWriteUtil::ZIntResult>(
    uint8_t** buf, uint8_t* buf_end);

int ReadWriteUtil::PutZInt(int32_t integer, uint8_t* buf) {
  // Move the sign bit to the first bit.
  uint32_t uinteger = (static_cast<uint32_t>(integer) << 1) ^ (integer >> 31);
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
  uint64_t ulongint = (static_cast<uint64_t>(longint) << 1) ^ (longint >> 63);
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
