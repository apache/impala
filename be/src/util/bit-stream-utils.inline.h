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
#include "util/bit-packing.inline.h"

namespace impala {

inline bool BitWriter::PutValue(uint64_t v, int num_bits) {
  DCHECK_LE(num_bits, MAX_BITWIDTH);
  DCHECK(num_bits == MAX_BITWIDTH || v >> num_bits == 0)
      << "v = " << v << ", num_bits = " << num_bits;

  if (UNLIKELY(byte_offset_ * 8 + bit_offset_ + num_bits > max_bytes_ * 8)) return false;

  buffered_values_ |= v << bit_offset_;
  bit_offset_ += num_bits;

  if (UNLIKELY(bit_offset_ >= 64)) {
    // Flush buffered_values_ and write out bits of v that did not fit
    memcpy(buffer_ + byte_offset_, &buffered_values_, 8);
    byte_offset_ += 8;
    bit_offset_ -= 64;

    // Shifting with the same or greater amount than the number of bits in the number is
    // undefined behaviour.
    int shift = num_bits - bit_offset_;

    if (LIKELY(shift < 64)) {
      buffered_values_ = v >> shift;
    } else {
      buffered_values_ = 0;
    }
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

template<typename UINT_T>
inline bool BitWriter::PutUleb128(UINT_T v) {
  static_assert(std::is_integral<UINT_T>::value, "Integral type required.");
  static_assert(std::is_unsigned<UINT_T>::value, "Unsigned type required.");
  static_assert(!std::is_same<UINT_T, bool>::value, "Bools are not supported.");

  constexpr UINT_T mask_lowest_7_bits = std::numeric_limits<UINT_T>::max() ^ 0x7Fu;

  bool result = true;
  while ((v & mask_lowest_7_bits) != 0L) {
    result &= PutAligned<uint8_t>((v & 0x7Fu) | 0x80u, 1);
    v >>= 7;
  }
  result &= PutAligned<uint8_t>(v & 0x7Fu, 1);
  return result;
}

template<typename INT_T>
bool BitWriter::PutZigZagInteger(INT_T v) {
  static_assert(std::is_integral<INT_T>::value, "Integral type required.");
  static_assert(std::is_signed<INT_T>::value, "Signed type required.");

  using UINT_T = std::make_unsigned_t<INT_T>;
  constexpr int bit_length_minus_one = sizeof(INT_T) * 8 - 1;

  /// For negative values, << results in undefined behaviour (prior to C++20), so we treat
  /// `v` as unsigned.
  const UINT_T v_unsigned = v;

  /// For the right shift, we rely on implementation defined behaviour as we expect it to
  /// be an arithmetic right shift (bits equal to the MSB are shifted in).
  return PutUleb128((v_unsigned << 1) ^ (v >> bit_length_minus_one));
}

template<typename T>
constexpr int BitWriter::NumberOfSignificantBits(T value) {
  static_assert(std::is_integral_v<T>, "Integral type required.");
  static_assert(std::is_unsigned_v<T>, "Unsigned type required.");
  static_assert(!std::is_same_v<T, bool>, "Bools are not supported.");

  if (value == 0) return 0;

  return sizeof(T) * 8 - BitUtil::CountLeadingZeros(value);
}

template<typename T>
constexpr int BitWriter::VlqRequiredSize(T value) {
  static_assert(std::is_integral_v<T>, "Integral type required.");
  static_assert(std::is_unsigned_v<T>, "Unsigned type required.");
  static_assert(!std::is_same_v<T, bool>, "Bools are not supported.");

  /// 0 has zero significant bits but we cannot store it on zero bytes.
  if (value == 0) return 1;

  int significant_bits = NumberOfSignificantBits(value);

  return (significant_bits + 6) / 7;
}

template<typename T>
constexpr int BitWriter::ZigZagRequiredBytes(T value) {
  static_assert(std::is_integral_v<T>, "Integral type required.");
  static_assert(std::is_signed_v<T>, "Signed type required.");

  using T_UNSIGNED = std::make_unsigned_t<T>;
  constexpr int bit_number = sizeof(T) * 8;

  T_UNSIGNED value_unsigned = value;
  T_UNSIGNED value_zigzag = (value_unsigned << 1) ^ (value >> (bit_number - 1));
  return VlqRequiredSize(value_zigzag);
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

template <typename T, typename ParquetType>
inline int BatchedBitReader::UnpackAndDeltaDecodeBatch(
    int bit_width, ParquetType* base_value, ParquetType delta_offset, int num_values,
    T* v, int64_t stride) {
  DCHECK(buffer_pos_ != nullptr);
  DCHECK_GE(bit_width, 0);
  DCHECK_LE(bit_width, MAX_BITWIDTH);
  DCHECK_GE(num_values, 0);

  const uint8_t* new_buffer_pos;
  int64_t num_read;
  bool decode_error = false;
  std::tie(new_buffer_pos, num_read)
      = BitPacking::UnpackAndDeltaDecodeValues<T, ParquetType>(bit_width, buffer_pos_,
          bytes_left(), base_value, delta_offset, num_values, v, stride, &decode_error);
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

  // It is possible that we want to "read" when we're at the end of the buffer if for
  // example we're unpacking values of bitwidth 0, but 'num_bytes' must then be 0. In this
  // case 'buffer_pos_' == 'buffer_end_'.
  //
  // It is not completely clear whether a one-past-the-end pointer (like 'buffer_end_') is
  // valid when used as an argument to memcpy(), see
  // https://en.cppreference.com/w/cpp/string/byte/memcpy and
  // https://stackoverflow.com/questions/29844298/is-it-legal-to-call-memcpy-with-zero-length-on-a-pointer-just-past-the-end-of-an.
  // Placing the memcpy() call in a conditional block seems to work around the problem
  // described in IMPALA-12239. It is probably not the solution to the root cause,
  // however; see the Jira ticket for more details.
  if (LIKELY(buffer_pos_ != buffer_end_)) {
    memcpy(v, buffer_pos_, num_bytes);
    buffer_pos_ += num_bytes;
  } else {
    DCHECK_EQ(num_bytes, 0);
  }

  return true;
}

template<typename UINT_T>
inline bool BatchedBitReader::GetUleb128(UINT_T* v) {
  static_assert(std::is_integral<UINT_T>::value, "Integral type required.");
  static_assert(std::is_unsigned<UINT_T>::value, "Unsigned type required.");
  static_assert(!std::is_same<UINT_T, bool>::value, "Bools are not supported.");

  *v = 0;
  int shift = 0;
  uint8_t byte = 0;
  do {
    if (UNLIKELY(shift >= max_vlq_byte_len<UINT_T>() * 7)) return false;
    if (!GetBytes(1, &byte)) return false;

    /// We need to convert 'byte' to UINT_T so that the result of the bitwise and
    /// operation is at least as long an integer as '*v', otherwise the shift may be too
    /// big and lead to undefined behaviour.
    const UINT_T byte_as_UINT_T = byte;
    *v |= (byte_as_UINT_T & 0x7Fu) << shift;
    shift += 7;
  } while ((byte & 0x80u) != 0);
  return true;
}

template<typename INT_T>
bool BatchedBitReader::GetZigZagInteger(INT_T* v) {
  static_assert(std::is_integral<INT_T>::value, "Integral type required.");
  static_assert(std::is_signed<INT_T>::value, "Signed type required.");

  using UINT_T = std::make_unsigned_t<INT_T>;

  UINT_T v_unsigned;
  if (UNLIKELY(!GetUleb128<UINT_T>(&v_unsigned))) return false;

  /// Here we rely on implementation defined behaviour in converting UINT_T to INT_T.
  *v = (v_unsigned >> 1) ^ -(v_unsigned & 1u);

  return true;
}
}

#endif
