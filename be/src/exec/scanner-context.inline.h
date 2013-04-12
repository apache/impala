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
#include "exec/read-write-util.h"

using namespace impala;

// Macro to return false if condition is false. Only defined for this file.
#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return false

// Handle the fast common path where all the bytes are in the first buffer.  This
// is the path used by sequence/rc/parquet file formats to read a very small number
// (i.e. single int) of bytes.
inline bool ScannerContext::Stream::GetBytes(int requested_len, uint8_t** buffer,
    int* out_len, bool* eos, Status* status) {

  if (UNLIKELY(requested_len == 0)) {
    *status = GetBytesInternal(requested_len, buffer, false, out_len, eos);
    return status->ok();
  }

  // Note: the fast path does not grab any locks even though another thread might be 
  // updating current_buffer_bytes_left_, current_buffer_ and current_buffer_pos_.
  // See the implementation of AddBuffer() on why this is okay.
  if (LIKELY(requested_len < current_buffer_bytes_left_)) {
    *eos = false;
    // Memory barrier to guarantee current_buffer_pos_ is not read before the 
    // above if statement.
    __sync_synchronize();
    DCHECK(current_buffer_ != NULL);
    *buffer = current_buffer_pos_;
    *out_len = requested_len;
    current_buffer_bytes_left_ -= requested_len;
    current_buffer_pos_ += requested_len;
    total_bytes_returned_ += *out_len;
    if (UNLIKELY(current_buffer_bytes_left_ == 0)) {
      *eos = current_buffer_->eosr();
    }
    return true;
  }
  *status = GetBytesInternal(requested_len, buffer, false, out_len, eos);
  return status->ok();
}

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
  *val = ReadWriteUtil::GetInt(bytes);
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
