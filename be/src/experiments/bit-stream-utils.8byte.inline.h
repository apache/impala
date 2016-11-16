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


#ifndef IMPALA_EXPERIMENTS_BIT_STREAM_UTILS_8BYTE_INLINE_H
#define IMPALA_EXPERIMENTS_BIT_STREAM_UTILS_8BYTE_INLINE_H

#include "experiments/bit-stream-utils.8byte.h"

namespace impala {

inline bool BitWriter_8byte::PutValue(uint64_t v, int num_bits) {
  DCHECK_LE(num_bits, 64);
  DCHECK(num_bits == 64 || v >> num_bits == 0)
      << "v = " << v << ", num_bits = " << num_bits;

  if (UNLIKELY(offset_ * 8 + bit_offset_ > max_bytes_ * 8 - num_bits)) return false;

  buffer_[offset_] |= v << bit_offset_;
  bit_offset_ += num_bits;
  if (bit_offset_ >= 64) {
    ++offset_;
    bit_offset_ -= 64;
    if (UNLIKELY(bit_offset_ > 0)) {
      // Write out bits of v that crossed into new byte offset
      // We cannot perform v >> num_bits (i.e. when bit_offset_ is 0) because v >> 64 != 0
      buffer_[offset_] = v >> (num_bits - bit_offset_);
    }
  }
  DCHECK_LT(bit_offset_, 64);
  return true;
}

inline uint8_t* BitWriter_8byte::GetNextBytePtr(int num_bytes) {
  if (UNLIKELY(bytes_written() + num_bytes > max_bytes_)) return NULL;

  // Advance to next aligned byte if necessary
  if (UNLIKELY(bit_offset_ > 56)) {
    ++offset_;
    bit_offset_ = 0;
  } else {
    bit_offset_ = BitUtil::RoundUpNumBytes(bit_offset_) * 8;
  }

  DCHECK_EQ(bit_offset_ % 8, 0);
  uint8_t* ptr = reinterpret_cast<uint8_t*>(buffer_ + offset_) + bit_offset_ / 8;
  bit_offset_ += num_bytes * 8;
  offset_ += bit_offset_ / 64;
  bit_offset_ %= 64;
  return ptr;
}

template<typename T>
inline bool BitWriter_8byte::PutAligned(T val, int num_bits) {
  // Align to byte boundary
  uint8_t* byte_ptr = GetNextBytePtr(0);
  bool result = PutValue(val, num_bits);
  if (!result) return false;
  // Pad to next byte boundary
  byte_ptr = GetNextBytePtr(0);
  DCHECK(byte_ptr != NULL);
  return true;
}

inline bool BitWriter_8byte::PutVlqInt(int32_t v) {
  bool result = true;
  while ((v & 0xFFFFFF80) != 0L) {
    result &= PutAligned<uint8_t>((v & 0x7F) | 0x80, 8);
    v >>= 7;
  }
  result &= PutAligned<uint8_t>(v & 0x7F, 8);
  return result;
}

template<typename T>
inline bool BitReader_8byte::GetValue(int num_bits, T* v) {
  int size = sizeof(T) * 8;
  DCHECK_LE(num_bits, size);
  if (UNLIKELY(offset_ * 8 + bit_offset_ > max_bytes_ * 8 - num_bits)) return false;

  *v = BitUtil::TrailingBits(buffer_[offset_], bit_offset_ + num_bits) >> bit_offset_;
  bit_offset_ += num_bits;
  if (bit_offset_ >= 64) {
    ++offset_;
    bit_offset_ -= 64;
    // Read bits of v that crossed into new byte offset
    *v |= BitUtil::TrailingBits(buffer_[offset_], bit_offset_)
        << (num_bits - bit_offset_);
  }
  DCHECK_LT(bit_offset_, 64);
  return true;
}

template<typename T>
inline bool BitReader_8byte::GetAligned(int num_bits, T* v) {
 Align();
  if (UNLIKELY(bytes_left() < BitUtil::Ceil(num_bits, 8))) return false;
  DCHECK_EQ(bit_offset_ % 8, 0);
  bool result = GetValue(num_bits, v);
  DCHECK(result);
  Align();
  return true;
}

inline bool BitReader_8byte::GetVlqInt(int32_t* v) {
  *v = 0;
  int shift = 0;
  int num_bytes = 0;
  uint8_t byte = 0;
  do {
    if (!GetAligned<uint8_t>(8, &byte)) return false;
    *v |= (byte & 0x7F) << shift;
    shift += 7;
    DCHECK_LE(++num_bytes, MAX_VLQ_BYTE_LEN);
  } while ((byte & 0x80) != 0);
  return true;
}

inline void BitReader_8byte::Align() {
  if (UNLIKELY(bit_offset_ > 56)) {
    ++offset_;
    bit_offset_ = 0;
    DCHECK_LE(offset_, max_bytes_);
  } else {
    bit_offset_ = BitUtil::RoundUpNumBytes(bit_offset_) * 8;
  }
}

}

#endif
