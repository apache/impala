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


#ifndef IMPALA_EXEC_SCANNER_CONTEXT_INLINE_H
#define IMPALA_EXEC_SCANNER_CONTEXT_INLINE_H

#include "exec/scanner-context.h"
#include "exec/serde-utils.inline.h"

using namespace impala;

// Macro to return false if condition is false. Only defined for this file.
#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return false

inline bool ScannerContext::Stream::ReadBytes(int length, uint8_t** buf, Status* status) {
  if (UNLIKELY(length < 0)) {
    *status = Status("Negative length");
    return false;
  }
  int bytes_read;
  bool dummy_eos;
  RETURN_IF_FALSE(GetBytes(length, buf, &bytes_read, &dummy_eos, status));
  if (UNLIKELY(length != bytes_read)) {
    *status = Status("incomplete read");
    return false;
  }
  return true;
}

// TODO: consider implementing a Skip in the context/stream object that's more 
// efficient than GetBytes.
inline bool ScannerContext::Stream::SkipBytes(int length, Status* status) {
  uint8_t* dummy_buf;
  int bytes_read;
  bool dummy_eos;
  RETURN_IF_FALSE(GetBytes(length, &dummy_buf, &bytes_read, &dummy_eos, status));
  if (UNLIKELY(length != bytes_read)) {
    *status = Status("incomplete read");
    return false;
  }
  return true;
}

inline bool ScannerContext::Stream::SkipText(Status* status) {
  uint8_t* dummy_buffer;
  int bytes_read;
  return ReadText(&dummy_buffer, &bytes_read, status);
}

inline bool ScannerContext::Stream::ReadText(uint8_t** buf, int* len, Status* status) {
  RETURN_IF_FALSE(ReadVInt(len, status));
  RETURN_IF_FALSE(ReadBytes(*len, buf, status));
  return true;
}

inline bool ScannerContext::Stream::ReadBoolean(bool* b, Status* status) {
  uint8_t* val;
  RETURN_IF_FALSE(ReadBytes(1, &val, status));
  *b = (*val != 0);
  return true;
}

inline bool ScannerContext::Stream::ReadInt(int32_t* val, Status* status) {
  uint8_t* bytes;
  RETURN_IF_FALSE(ReadBytes(sizeof(int32_t), &bytes, status));
  *val = SerDeUtils::GetInt(bytes);
  return true;
}

inline bool ScannerContext::Stream::ReadVInt(int32_t* value, Status* status) {
  int64_t vlong;
  RETURN_IF_FALSE(ReadVLong(&vlong, status));
  *value = static_cast<int32_t>(vlong);
  return true;
}

inline bool ScannerContext::Stream::ReadVLong(int64_t* value, Status* status) {
  int8_t* firstbyte;
  uint8_t* bytes;

  RETURN_IF_FALSE(ReadBytes(1, reinterpret_cast<uint8_t**>(&firstbyte), status));

  int len = SerDeUtils::DecodeVIntSize(*firstbyte);
  if (len > SerDeUtils::MAX_VINT_LEN) {
    *status = Status("ReadVLong: size is too big");
    return false;
  }
  
  if (len == 1) {
    *value = static_cast<int64_t>(*firstbyte);
    return true;
  }
  --len;

  RETURN_IF_FALSE(ReadBytes(len, &bytes, status));

  *value = 0;

  for (int i = 0; i < len; i++) {
    *value = (*value << 8) | (bytes[i] & 0xFF);
  }

  if (SerDeUtils::IsNegativeVInt(*firstbyte)) {
    *value = *value ^ (static_cast<int64_t>(-1));
  }
  return true;
}

inline bool ScannerContext::Stream::ReadZLong(int64_t* value, Status* status) {
  uint64_t zlong = 0;
  int shift = 0;
  uint8_t* byte;
  do {
    DCHECK_LE(shift, 64);
    RETURN_IF_FALSE(ReadBytes(1, &byte, status));
    zlong |= static_cast<uint64_t>(*byte & 0x7f) << shift;
    shift += 7;
  } while (*byte & 0x80);
  *value = (zlong >> 1) ^ -(zlong & 1);
  return true;
}

#undef RETURN_IF_FALSE

#endif
