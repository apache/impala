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

#include "exec/scan-range-context.h"

using namespace impala;

// Macro to return false if condition is false. Only defined for this file.
#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return false

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
  
inline bool SerDeUtils::ReadBytes(ScanRangeContext* context, int length,
      uint8_t** buf, Status* status) {
  if (UNLIKELY(length < 0)) {
    *status = Status("Negative length");
    return false;
  }
  int bytes_read;
  bool dummy_eos;
  RETURN_IF_FALSE(context->GetBytes(buf, length, &bytes_read, &dummy_eos, status));
  if (UNLIKELY(length != bytes_read)) {
    *status = Status("incomplete read");
    return false;
  }
  return true;
}

inline bool SerDeUtils::SkipBytes(ScanRangeContext* context, int length, Status* status) {
  uint8_t* dummy_buf;
  int bytes_read;
  bool dummy_eos;
  RETURN_IF_FALSE(context->GetBytes(&dummy_buf, length, &bytes_read, &dummy_eos, status));
  if (UNLIKELY(length != bytes_read)) {
    *status = Status("incomplete read");
    return false;
  }
  return true;
}

inline bool SerDeUtils::SkipText(ScanRangeContext* context, Status* status) {
  uint8_t* dummy_buffer;
  int bytes_read;
  return SerDeUtils::ReadText(context, &dummy_buffer, &bytes_read, status);
}

inline bool SerDeUtils::ReadText(ScanRangeContext* context, 
    uint8_t** buf, int* len, Status* status) {
  RETURN_IF_FALSE(SerDeUtils::ReadVInt(context, len, status));
  RETURN_IF_FALSE(SerDeUtils::ReadBytes(context, *len, buf, status));
  return true;
}

inline bool SerDeUtils::ReadBoolean(ScanRangeContext* context, bool* b, Status* status) {
  uint8_t* val;
  RETURN_IF_FALSE(SerDeUtils::ReadBytes(context, 1, &val, status));
  *b = *val != 0;
  return true;
}

inline bool SerDeUtils::ReadInt(ScanRangeContext* context, int32_t* val, Status* status) {
  uint8_t* bytes;
  RETURN_IF_FALSE(SerDeUtils::ReadBytes(context, sizeof(int32_t), &bytes, status));
  *val = GetInt(bytes);
  return true;
}

inline bool SerDeUtils::ReadVInt(ScanRangeContext* context, int32_t* value, 
    Status* status) {
  int64_t vlong;
  RETURN_IF_FALSE(ReadVLong(context, &vlong, status));
  *value = static_cast<int32_t>(vlong);
  return true;
}

inline bool SerDeUtils::ReadVLong(ScanRangeContext* context, int64_t* value, 
    Status* status) {
  int8_t* firstbyte;
  uint8_t* bytes;

  RETURN_IF_FALSE(SerDeUtils::ReadBytes(context, 1, 
        reinterpret_cast<uint8_t**>(&firstbyte), status));

  int len = DecodeVIntSize(*firstbyte);
  if (len > MAX_VINT_LEN) {
    *status = Status("ReadVLong: size is too big");
    return false;
  }
  
  if (len == 1) {
    *value = static_cast<int64_t>(*firstbyte);
    return true;
  }
  --len;

  RETURN_IF_FALSE(SerDeUtils::ReadBytes(context, len, &bytes, status));

  *value = 0;

  for (int i = 0; i < len; i++) {
    *value = (*value << 8) | (bytes[i] & 0xFF);
  }

  if (IsNegativeVInt(*firstbyte)) {
    *value = *value ^ (static_cast<int64_t>(-1));
  }
  return true;
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
