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


#ifndef IMPALA_EXEC_SCANNER_CONTEXT_INLINE_H
#define IMPALA_EXEC_SCANNER_CONTEXT_INLINE_H

#include "exec/scanner-context.h"
#include "exec/read-write-util.h"
#include "runtime/string-buffer.h"

namespace impala {

/// Macro to return false if condition is false. Only defined for this file.
#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return false

/// Handle the fast common path where all the bytes are in the first buffer.  This
/// is the path used by sequence/rc/parquet file formats to read a very small number
/// (i.e. single int) of bytes.
inline bool ScannerContext::Stream::GetBytes(int64_t requested_len, uint8_t** buffer,
    int64_t* out_len, Status* status, bool peek) {
  if (UNLIKELY(requested_len < 0)) {
    *status = ReportInvalidRead(requested_len);
    return false;
  }
  if (UNLIKELY(requested_len == 0)) {
    *out_len = 0;
    return true;
  }
  if (LIKELY(requested_len <= *output_buffer_bytes_left_)) {
    *out_len = requested_len;
    *buffer = *output_buffer_pos_;
    if (LIKELY(!peek)) {
      total_bytes_returned_ += *out_len;
      *output_buffer_pos_ += *out_len;
      *output_buffer_bytes_left_ -= *out_len;
    }
    return true;
  }
  DCHECK_GT(requested_len, 0);
  *status = GetBytesInternal(requested_len, buffer, peek, out_len);
  return status->ok();
}

inline bool ScannerContext::Stream::ReadBytes(int64_t length, uint8_t** buf,
    Status* status, bool peek) {
  int64_t bytes_read;
  RETURN_IF_FALSE(GetBytes(length, buf, &bytes_read, status, peek));
  if (UNLIKELY(length != bytes_read)) {
    DCHECK_LT(bytes_read, length);
    *status = ReportIncompleteRead(length, bytes_read);
    return false;
  }
  return true;
}

/// TODO: consider implementing a Skip in the context/stream object that's more
/// efficient than GetBytes.
inline bool ScannerContext::Stream::SkipBytes(int64_t length, Status* status) {
  uint8_t* dummy_buf;
  int64_t bytes_read;
  RETURN_IF_FALSE(GetBytes(length, &dummy_buf, &bytes_read, status));
  if (UNLIKELY(length != bytes_read)) {
    DCHECK_LT(bytes_read, length);
    *status = ReportIncompleteRead(length, bytes_read);
    return false;
  }
  return true;
}

inline bool ScannerContext::Stream::SkipText(Status* status) {
  uint8_t* dummy_buffer;
  int64_t bytes_read;
  return ReadText(&dummy_buffer, &bytes_read, status);
}

inline bool ScannerContext::Stream::ReadText(uint8_t** buf, int64_t* len,
    Status* status) {
  RETURN_IF_FALSE(ReadVLong(len, status));
  RETURN_IF_FALSE(ReadBytes(*len, buf, status));
  return true;
}

inline bool ScannerContext::Stream::ReadBoolean(bool* b, Status* status) {
  uint8_t* val;
  RETURN_IF_FALSE(ReadBytes(1, &val, status));
  *b = (*val != 0);
  return true;
}

inline bool ScannerContext::Stream::ReadInt(int32_t* val, Status* status, bool peek) {
  uint8_t* bytes;
  RETURN_IF_FALSE(ReadBytes(sizeof(uint32_t), &bytes, status, peek));
  *val = ReadWriteUtil::GetInt<uint32_t>(bytes);
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

  int len = ReadWriteUtil::DecodeVIntSize(*firstbyte);
  if (len > ReadWriteUtil::MAX_VINT_LEN) {
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

  if (ReadWriteUtil::IsNegativeVInt(*firstbyte)) {
    *value = *value ^ (static_cast<int64_t>(-1));
  }
  return true;
}

inline bool ScannerContext::Stream::ReadZLong(int64_t* value, Status* status) {
  uint8_t* bytes;
  int64_t bytes_len;
  RETURN_IF_FALSE(
      GetBytes(ReadWriteUtil::MAX_ZLONG_LEN, &bytes, &bytes_len, status, true));

  uint8_t* new_bytes = bytes;
  ReadWriteUtil::ZLongResult r = ReadWriteUtil::ReadZLong(&new_bytes, bytes + bytes_len);
  if (UNLIKELY(!r.ok)) {
    *status = ReportInvalidInt();
    return false;
  }
  *value = r.val;
  int64_t bytes_read = new_bytes - bytes;
  RETURN_IF_FALSE(SkipBytes(bytes_read, status));
  return true;
}

#undef RETURN_IF_FALSE

} // namespace impala
#endif
