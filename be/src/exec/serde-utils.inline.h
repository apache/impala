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


#ifndef IMPALA_EXEC_SERDE_UTILS_INLINE_H
#define IMPALA_EXEC_SERDE_UTILS_INLINE_H

#include "exec/serde-utils.h"

using namespace impala;

inline int16_t SerDeUtils::GetSmallInt(const uint8_t* buf) {
  return (buf[0] << 8) | buf[1];
}

inline int32_t SerDeUtils::GetInt(const uint8_t* buf) {
  return (buf[0] << 24) | (buf[1] << 16) | (buf[2] << 8) | buf[3];
}

inline int64_t SerDeUtils::GetLongInt(const uint8_t* buf) {
  return (static_cast<int64_t>(buf[0]) << 56) |
      (static_cast<int64_t>(buf[1]) << 48) |
      (static_cast<int64_t>(buf[2]) << 40) |
      (static_cast<int64_t>(buf[3]) << 32) |
      (buf[4] << 24) | (buf[5] << 16) | (buf[6] << 8) | buf[7];
}

inline void SerDeUtils::PutInt(uint8_t* buf, int32_t integer) {
  buf[0] = integer >> 24;
  buf[1] = integer >> 16;
  buf[2] = integer >> 8;
  buf[3] = integer;
}
  
inline int SerDeUtils::GetVInt(uint8_t* buf, int32_t* vint) {
  int64_t vlong = 0;
  int len = GetVLong(buf, &vlong);
  *vint = static_cast<int32_t>(vlong);
  return len;
}

inline int SerDeUtils::GetVLong(uint8_t* buf, int64_t* vlong) {
  return GetVLong(buf, 0, vlong);
}

inline int SerDeUtils::GetVLong(uint8_t* buf, int64_t offset, int64_t* vlong) {
  int8_t firstbyte = (int8_t) buf[0 + offset];

  int len = DecodeVIntSize(firstbyte);
  if (len > MAX_VINT_LEN) return -1;
  if (len == 1) {
    *vlong = static_cast<int64_t>(firstbyte);
    return len;
  }

  *vlong &= ~*vlong;

  for (int i = 1; i < len; i++) {
    *vlong = (*vlong << 8) | buf[i+offset];
  }

  if (IsNegativeVInt(firstbyte)) {
    *vlong = *vlong ^ ((int64_t) - 1);
  }

  return len;
}

inline bool SerDeUtils::IsNegativeVInt(int8_t byte) {
  return byte < -120 || (byte >= -112 && byte < 0);
}

inline int SerDeUtils::DecodeVIntSize(int8_t byte) {
  if (byte >= -112) {
    return 1;
  } else if (byte < -120) {
    return -119 - byte;
  }
  return -111 - byte;
}

#undef RETURN_IF_FALSE

#endif
