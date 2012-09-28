// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_SERDE_UTILS_INLINE_H
#define IMPALA_EXEC_SERDE_UTILS_INLINE_H

#include "exec/serde-utils.h"

#include "exec/scan-range-context.h"

using namespace impala;

inline int32_t SerDeUtils::GetInt(const uint8_t* buf) {
  return (buf[0] << 24) | (buf[1] << 16) | (buf[2] << 8) | buf[3];
}

inline void SerDeUtils::PutInt(uint8_t* buf, int32_t integer) {
  buf[0] = integer >> 24;
  buf[1] = integer >> 16;
  buf[2] = integer >> 8;
  buf[3] = integer;
}
  
inline Status SerDeUtils::ReadBytes(ScanRangeContext* context, int length,
      uint8_t** buf) {
  if (UNLIKELY(length < 0)) return Status("Negative length");

  int bytes_read;
  bool dummy_eos;
  RETURN_IF_ERROR(context->GetBytes(buf, length, &bytes_read, &dummy_eos));
  if (UNLIKELY(length != bytes_read)) return Status("incomplete read");
  return Status::OK;
}

inline Status SerDeUtils::SkipBytes(ScanRangeContext* context, int length) {
  uint8_t* dummy_buf;
  int bytes_read;
  bool dummy_eos;
  RETURN_IF_ERROR(context->GetBytes(&dummy_buf, length, &bytes_read, &dummy_eos));
  if (UNLIKELY(length != bytes_read)) return Status("incomplete read");
  return Status::OK;
}

inline Status SerDeUtils::SkipText(ScanRangeContext* context) {
  uint8_t* dummy_buffer;
  int bytes_read;
  return SerDeUtils::ReadText(context, &dummy_buffer, &bytes_read);
}

inline Status SerDeUtils::ReadText(ScanRangeContext* context, uint8_t** buf, int* len) {
  RETURN_IF_ERROR(SerDeUtils::ReadVInt(context, len));
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(context, *len, buf));
  return Status::OK;
}

inline Status SerDeUtils::ReadBoolean(ScanRangeContext* context, bool* b) {
  uint8_t* val;
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(context, 1, &val));
  *b = *val != 0;
  return Status::OK;
}

inline Status SerDeUtils::ReadInt(ScanRangeContext* context, int32_t* val) {
  uint8_t* bytes;
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(context, sizeof(int32_t), &bytes));
  *val = GetInt(bytes);
  return Status::OK;
}

inline Status SerDeUtils::ReadVInt(ScanRangeContext* context, int32_t* value) {
  int64_t vlong;
  RETURN_IF_ERROR(ReadVLong(context, &vlong));
  *value = static_cast<int32_t>(vlong);
  return Status::OK;
}

inline Status SerDeUtils::ReadVLong(ScanRangeContext* context, int64_t* value) {
  int8_t* firstbyte;
  uint8_t* bytes;

  RETURN_IF_ERROR(SerDeUtils::ReadBytes(context, 1, 
        reinterpret_cast<uint8_t**>(&firstbyte)));

  int len = DecodeVIntSize(*firstbyte);
  if (len > MAX_VINT_LEN) return Status("ReadVLong: size is too big");
  if (len == 1) {
    *value = static_cast<int64_t>(*firstbyte);
    return Status::OK;
  }
  --len;

  RETURN_IF_ERROR(SerDeUtils::ReadBytes(context, len, &bytes));

  *value = 0;

  for (int i = 0; i < len; i++) {
    *value = (*value << 8) | (bytes[i] & 0xFF);
  }

  if (IsNegativeVInt(*firstbyte)) {
    *value = *value ^ (static_cast<int64_t>(-1));
  }
  return Status::OK;
}

inline int SerDeUtils::GetVInt(uint8_t* buf, int32_t* vint) {
  int64_t vlong;
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

#endif
