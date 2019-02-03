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

#ifndef IMPALA_UTIL_BIT_STREAM_UTILS_INLINE_H
#define IMPALA_UTIL_BIT_STREAM_UTILS_INLINE_H

#include "util/bit-stream-utils.h"

#include "common/compiler-util.h"
#include "util/bit-packing.h"

namespace impala {

inline bool BitWriter::PutValue(uint64_t v, int num_bits) {
  // TODO: revisit this limit if necessary (can be raised to 64 by fixing some edge cases)
  DCHECK_LE(num_bits, MAX_BITWIDTH);
  DCHECK_EQ(v >> num_bits, 0) << "v = " << v << ", num_bits = " << num_bits;

  if (UNLIKELY(byte_offset_ * 8 + bit_offset_ + num_bits > max_bytes_ * 8)) return false;

  buffered_values_ |= v << bit_offset_;
  bit_offset_ += num_bits;

  if (UNLIKELY(bit_offset_ >= 64)) {
    // Flush buffered_values_ and write out bits of v that did not fit
    memcpy(buffer_ + byte_offset_, &buffered_values_, 8);
    buffered_values_ = 0;
    byte_offset_ += 8;
    bit_offset_ -= 64;
    buffered_values_ = v >> (num_bits - bit_offset_);
  }
  DCHECK_LT(bit_offset_, 64);
  return true;
}

inline void BitWriter::Flush(bool align) {
  int num_bytes = BitUtil::Ceil(bit_offset_, 8);
  DCHECK_LE(byte_offset_ + num_bytes, max_bytes_);
  memcpy(buffer_ + byte_offset_, &buffered_values_, num_bytes);

  if (align) {
    buffered_values_ = 0;
    byte_offset_ += num_bytes;
    bit_offset_ = 0;
  }
}

inline uint8_t* BitWriter::GetNextBytePtr(int num_bytes) {
  Flush(/* align */ true);
  DCHECK_LE(byte_offset_, max_bytes_);
  if (byte_offset_ + num_bytes > max_bytes_) return NULL;
  uint8_t* ptr = buffer_ + byte_offset_;
  byte_offset_ += num_bytes;
  return ptr;
}

template<typename T>
inline bool BitWriter::PutAligned(T val, int num_bytes) {
  uint8_t* ptr = GetNextBytePtr(num_bytes);
  if (ptr == NULL) return false;
  memcpy(ptr, &val, num_bytes);
  return true;
}

inline bool BitWriter::PutUleb128Int(uint32_t v) {
  bool result = true;
  while ((v & 0xFFFFFF80) != 0L) {
    result &= PutAligned<uint8_t>((v & 0x7F) | 0x80, 1);
    v >>= 7;
  }
  result &= PutAligned<uint8_t>(v & 0x7F, 1);
  return result;
}

template<typename T>
inline int BatchedBitReader::UnpackBatch(int bit_width, int num_values, T* v) {
  DCHECK(buffer_pos_ != nullptr);
  DCHECK_GE(bit_width, 0);
  DCHECK_LE(bit_width, MAX_BITWIDTH);
  DCHECK_LE(bit_width, sizeof(T) * 8);
  DCHECK_GE(num_values, 0);

  int64_t num_read;
  std::tie(buffer_pos_, num_read) = BitPacking::UnpackValues(bit_width, buffer_pos_,
      bytes_left(), num_values, v);
  DCHECK_LE(buffer_pos_, buffer_end_);
  DCHECK_LE(num_read, num_values);
  return static_cast<int>(num_read);
}

inline bool BatchedBitReader::SkipBatch(int bit_width, int num_values_to_skip) {
  DCHECK(buffer_pos_ != nullptr);
  DCHECK_GT(bit_width, 0);
  DCHECK_LE(bit_width, MAX_BITWIDTH);
  DCHECK_GT(num_values_to_skip, 0);

  int skip_bytes = BitUtil::RoundUpNumBytes(bit_width * num_values_to_skip);
  if (skip_bytes > buffer_end_ - buffer_pos_) return false;
  buffer_pos_ += skip_bytes;
  return true;
}

template <typename T>
inline int BatchedBitReader::UnpackAndDecodeBatch(
    int bit_width, T* dict, int64_t dict_len, int num_values, T* v, int64_t stride) {
  DCHECK(buffer_pos_ != nullptr);
  DCHECK_GE(bit_width, 0);
  DCHECK_LE(bit_width, MAX_BITWIDTH);
  DCHECK_GE(num_values, 0);

  const uint8_t* new_buffer_pos;
  int64_t num_read;
  bool decode_error = false;
  std::tie(new_buffer_pos, num_read) = BitPacking::UnpackAndDecodeValues(bit_width,
      buffer_pos_, bytes_left(), dict, dict_len, num_values, v, stride, &decode_error);
  if (UNLIKELY(decode_error)) return -1;
  buffer_pos_ = new_buffer_pos;
  DCHECK_LE(buffer_pos_, buffer_end_);
  DCHECK_LE(num_read, num_values);
  return static_cast<int>(num_read);
}

template<typename T>
inline bool BatchedBitReader::GetBytes(int num_bytes, T* v) {
  DCHECK(buffer_pos_ != nullptr);
  DCHECK_GE(num_bytes, 0);
  DCHECK_LE(num_bytes, sizeof(T));
  if (UNLIKELY(buffer_pos_ + num_bytes > buffer_end_)) return false;
  *v = 0; // Ensure unset bytes are initialized to zero.
  memcpy(v, buffer_pos_, num_bytes);
  buffer_pos_ += num_bytes;
  return true;
}

inline bool BatchedBitReader::GetUleb128Int(uint32_t* v) {
  *v = 0;
  int shift = 0;
  uint8_t byte = 0;
  do {
    if (UNLIKELY(shift >= MAX_VLQ_BYTE_LEN * 7)) return false;
    if (!GetBytes(1, &byte)) return false;
    // The constant below must be explicitly unsigned to ensure that the result of the
    // bitwise-and is unsigned, so that the left shift is always defined behavior under
    // the C++ standard.
    *v |= (byte & 0x7Fu) << shift;
    shift += 7;
  } while ((byte & 0x80) != 0);
  return true;
}
}

#endif
