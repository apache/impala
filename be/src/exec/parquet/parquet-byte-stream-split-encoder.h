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

// This class can be used to encode values with byte stream split encoding
// (https://github.com/apache/parquet-format/blob/master/Encodings.md#byte-stream-split-byte_stream_split--9)
// and write them to a buffer. To encode values to a page:
// 1. pass the number of bytes a value takes up via the constructor or the template
//    parameter (`T_SIZE`). If `T_SIZE` is set to 0, the size of the type must be passed
//    as an argument to the constructor.
// 2. call `NewPage()` with a pointer to the first byte of an empty or prepopulated buffer
// 3. use the `Put()` or `PutBytes()` function to add a value to be encoded
// 4. call `FinalizePage()` with a pointer to the output buffer. Encoding happens upon
//    this call, and it also resets the encoder.
//
// Passing the byte size via the constructor is only recommended if the byte size is
// not 4 or 8. Using the template parameter allows for better optimization.
template <size_t T_SIZE>
class ParquetByteStreamSplitEncoder {
 public:
  // This constructor should be used when the byte size comes from the template parameter
  // as a compile-time constant. This way it can be better optimized.
  template <size_t SIZE = T_SIZE, std::enable_if_t<SIZE != 0, bool> = true>
  ParquetByteStreamSplitEncoder() : size_in_bytes_(T_SIZE) {
    static_assert(SIZE == T_SIZE);
  }

  // This constructor should be used when the byte size does not come from the template
  // parameter. The byte size must be passed as an argument. This would usually be used
  // for fixed_len_byte_array types.
  template <size_t SIZE = T_SIZE, std::enable_if_t<SIZE == 0, bool> = true>
  ParquetByteStreamSplitEncoder(int byte_count) : size_in_bytes_(byte_count) {
    static_assert(SIZE == T_SIZE);
  }

  // The function sets the pointer and the length of the input buffer.
  // `input_buffer` should point to the start of a buffer where the encoder can start
  // gathering values to be encoded. If `prepopulated` > 0, the first `prepopulated`
  // values in the buffer are treated as if they have already been added to the encoder.
  // The buffer length must be non-negative. The pointer must not be a nullpointer.
  void NewPage(uint8_t* input_buffer, int buffer_len, int prepopulated = 0);

  // Adds `value` to the list of values to be encoded.
  // The type (T) must be the same size as `size_in_bytes`.
  // Only valid to call when `NewPage()` has already been called.
  // Returns whether or not adding the value was successful.
  template <typename T>
  WARN_UNUSED_RESULT
  bool Put(T value) {
    DCHECK(sizeof(T) == getByteSize());
    DCHECK(input_buffer_ != nullptr); // NewPage() must be called first

    if (value_count_ * getByteSize() + sizeof(T) > input_buffer_len_) return false;

    memcpy(input_buffer_ + value_count_ * getByteSize(), &value, getByteSize());
    value_count_++;
    return true;
  }

  // Adds `value` to the list of values to be encoded.
  // It is the caller's responsibility to ensure that the value is the correct size.
  // Only valid to call when `NewPage()` has already been called.
  // Returns whether or not adding the value was successful.
  bool PutBytes(const uint8_t* value) WARN_UNUSED_RESULT;

  // Writes the encoded values to the `output_buffer`, and resets the encoder.
  // Returns the number of values encoded, or -1 if an error occured.
  // The `Put()` function can't be called until calling `NewPage()` again.
  int FinalizePage(uint8_t* output_buffer, int output_buffer_len) WARN_UNUSED_RESULT;

 private:
  // Number of bytes that T consists of.
  const int size_in_bytes_;

  // Points to the first byte of the input buffer.
  // This is where we store the values to be encoded.
  uint8_t* input_buffer_ = nullptr;

  // Length of the buffer in bytes.
  int input_buffer_len_ = 0;

  // The number of values to encode when `FinalizePage()` is called.
  // Note that this contains the number of values, not bytes.
  int value_count_ = 0;

  void Reset();

  // This is the helper function for FinalizePage() that does the actual encoding.
  static void Encode(const uint8_t* to_encode, uint8_t* encoded,
      int value_count, size_t byte_size);

  ALWAYS_INLINE size_t getByteSize() const {
    return T_SIZE == 0 ? size_in_bytes_ : T_SIZE;
  }
};

} // namespace impala
