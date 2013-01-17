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


#ifndef IMPALA_UTIL_BIT_STREAM_UTILS_INLINE_H
#define IMPALA_UTIL_BIT_STREAM_UTILS_INLINE_H

#include "util/bit-stream-utils.h"

namespace impala {

inline bool BitWriter::PutBool(bool b) {
  if (UNLIKELY(byte_offset_ == num_bytes_)) return false;

  uint8_t value = 1 << bit_offset_;
  if (b) {
    buffer_[byte_offset_] |= value;
  } else {
    buffer_[byte_offset_] &= ~value;
  }
  if (++bit_offset_ == 8) {
    bit_offset_ = 0;
    ++byte_offset_;
  }

  return true;
}

inline uint8_t* BitWriter::GetNextBytePtr(int num_bytes) {
  if (UNLIKELY(bit_offset_ != 0)) {
    // Advance to next aligned byte
    ++byte_offset_;
    bit_offset_ = 0;
  } 
  if (byte_offset_ + num_bytes > num_bytes_) return NULL;
  uint8_t* ptr = buffer_ + byte_offset_;
  byte_offset_ += num_bytes;
  return ptr;
}

template<typename T>
inline bool BitWriter::PutAligned(T val) {
  uint8_t* byte_ptr = GetNextBytePtr(sizeof(T));
  if (UNLIKELY(byte_ptr == NULL)) return false;
  *reinterpret_cast<T*>(byte_ptr) = val;
  return true;
}

inline bool BitWriter::PutVlqInt(int32_t v) {
  bool result = true;
  while ((v & 0xFFFFFF80) != 0L) {
    result &= PutAligned<uint8_t>((v & 0x7F) | 0x80);
    v >>= 7;
  }
  result &= PutAligned<uint8_t>(v & 0x7F);
  return result;
}

inline bool BitReader::GetBool(bool* b) {
  if (UNLIKELY(byte_offset_ == num_bytes_)) return false;
  *b = (buffer_[byte_offset_] >> bit_offset_) & 1;
  if (++bit_offset_ == 8) {
    bit_offset_ = 0;
    ++byte_offset_;
  }
  return true;
}

template<typename T>
inline bool BitReader::GetAligned(T* v) {
  if (bit_offset_ != 0) {
    bit_offset_ = 0;
    ++byte_offset_;
  }
  if (UNLIKELY(byte_offset_ + sizeof(T) > num_bytes_)) return false;
  // TODO: this assumes unaligned words are supported and efficient.  
  // revisit for non-x86 architectures
  *v = *reinterpret_cast<T*>(buffer_ + byte_offset_);
  byte_offset_ += sizeof(T);
  return true;
}

inline bool BitReader::GetVlqInt(int32_t* v) {
  *v = 0;
  int shift = 0;
  int num_bytes = 0;
  uint8_t byte = 0;
  do {
    if (!GetAligned<uint8_t>(&byte)) return false;
    *v |= (byte & 0x7F) << shift;
    shift += 7;
    DCHECK_LE(++num_bytes, MAX_VLQ_BYTE_LEN);
  } while ((byte & 0x80) != 0);
  return true;
}

}

#endif
