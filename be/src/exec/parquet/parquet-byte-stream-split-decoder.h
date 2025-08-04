/// Licensed to the Apache Software Foundation (ASF) under one
/// or more contributor license agreements.  See the NOTICE file
/// distributed with this work for additional information
/// regarding copyright ownership.  The ASF licenses this file
/// to you under the Apache License, Version 2.0 (the
/// "License"); you may not use this file except in compliance
/// with the License.  You may obtain a copy of the License at
///
///   http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing,
/// software distributed under the License is distributed on an
/// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
/// KIND, either express or implied.  See the License for the
/// specific language governing permissions and limitations
/// under the License.

#pragma once

#include "common/status.h"

namespace impala {

// This class can be used to decode byte stream split encoded values from a buffer.
// (https://github.com/apache/parquet-format/blob/master/Encodings.md#byte-stream-split-byte_stream_split--9)
// To decode values from a page:
// 1. pass the number of bytes a value takes up via the constructor or the template
//    parameter (`T_SIZE`). If `T_SIZE` is set to 0, the size of the type must be passed
//    as an argument to the constructor.
// 2. call `NewPage()` with a pointer to the start of the byte stream split encoded buffer
// 3. use the `NextValue()` or `NextValues()` functions to extract values.
//
// Passing the byte size via the constructor is only recommended if the byte size is
// not 4 or 8. Using the template parameter allows for better optimization.
template <size_t T_SIZE>
class ParquetByteStreamSplitDecoder {
 public:
  // This constructor should be used when the byte size comes from the template parameter
  // as a compile-time constant. This way it can be better optimized.
  template <size_t SIZE = T_SIZE, std::enable_if_t<SIZE != 0, bool> = true>
  ParquetByteStreamSplitDecoder() : size_in_bytes_(T_SIZE) {
    static_assert(SIZE == T_SIZE);
  }

  // This constructor should be used when the byte size does not come from the template
  // parameter. The byte size must be passed as an argument. This would usually be used
  // for fixed_len_byte_array types.
  template <size_t SIZE = T_SIZE, std::enable_if_t<SIZE == 0, bool> = true>
  ParquetByteStreamSplitDecoder(int byte_count) : size_in_bytes_(byte_count) {
    static_assert(SIZE == T_SIZE);
  }

  // Set a new byte stream split encoded page as the input.
  // The function sets the pointer and length of input data.
  // The buffer length must be non-negative and a multiple of `size_in_bytes_`.
  // The pointer must not be a nullpointer.
  // Returns an error, if either of these conditions are not met.
  void NewPage(const uint8_t* input_buffer, int input_buffer_len);

  // Returns the total number of values contained in this page.
  std::size_t GetTotalValueCount() const { return input_buffer_len_ / getByteSize(); }

  // Tries to decode a single value and write it to `*value`.
  // The type (T) must be the same size as `size_in_bytes_`.
  // Only valid to call when `NewPage()` has already been called.
  // Returns
  // * 1, if a value was successfully decoded,
  // * 0, if there were no values left to decode
  // * -1, if there was an error
  template <typename T>
  WARN_UNUSED_RESULT int NextValue(T* value) {
    DCHECK(sizeof(T) == getByteSize());
    return ParquetByteStreamSplitDecoder::NextValues(
        1, reinterpret_cast<uint8_t*>(value), getByteSize());
  }

  // Tries to decode `num_values` values and write them to the `*values` buffer with a
  // given stride.
  //
  // The `stride` is the distance (in bytes) between the first byte of each value written.
  // For example, if `size_in_bytes_ == 4` and `stride == 12`, then the buffer will have 4
  // bytes filled with the decoded value, then 8 bytes skipped (untouched),then
  // another 4 bytes filled, another 8 bytes skipped.... num_values times.
  //
  // The `stride` must not be less than the `size_in_bytes_`.
  // It is the caller's responsibility to make sure that the buffer is large enough to
  // hold the values (including the stride). The pointer needs to point to the first byte
  // that is to be written to.
  // `num_values` must be non-negative.
  // Only valid to call when `NewPage()` has already been called.
  //
  // If there are less values left to read than `num_values`, it will only read as many as
  // there are left.
  //
  // Returns
  // * -1, if there was an error,
  // * 0, if there were no values left to decode or
  // * the number of values successfully decoded.
  int NextValues(int num_values, uint8_t* values, std::size_t stride) WARN_UNUSED_RESULT;

  // Tries to skip num_values values. num_values must be non-negative.
  // Only valid to call when 'NewPage()' has already been called.
  //
  // Returns:
  // * -1, if there was an error,
  // * 0, if there were no values left or
  // * the number of values successfully skipped.
  int SkipValues(int num_values) WARN_UNUSED_RESULT;

 private:
  // number of bytes that the data type consists of
  const int size_in_bytes_;

  // points to the first byte of the input buffer
  const uint8_t* input_buffer_ = nullptr;

  // length of the buffer in bytes
  int input_buffer_len_ = 0;

  // index of next value to read
  int value_index_ = 0;

  ALWAYS_INLINE size_t getByteSize() const {
    return T_SIZE == 0 ? size_in_bytes_ : T_SIZE;
  }
};

} // namespace impala
