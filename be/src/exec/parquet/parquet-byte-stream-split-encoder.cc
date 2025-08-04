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

#include "parquet-byte-stream-split-encoder.h"

#include <cstring>

namespace impala {

template <size_t T_SIZE>
void ParquetByteStreamSplitEncoder<T_SIZE>::NewPage(
    uint8_t* input_buffer, int buffer_len, int prepopulated) {
  DCHECK(input_buffer != nullptr);
  DCHECK_GE(buffer_len, 0);
  DCHECK_EQ(buffer_len % getByteSize(), 0);
  DCHECK_GE(prepopulated, 0);

  Reset();

  input_buffer_len_ = buffer_len;
  input_buffer_ = input_buffer;
  value_count_ = prepopulated;
}


template <size_t T_SIZE>
bool ParquetByteStreamSplitEncoder<T_SIZE>::PutBytes(const uint8_t* value) {
  DCHECK(input_buffer_ != nullptr); // NewPage() must be called first
  if ((value_count_ + 1) * getByteSize() > input_buffer_len_) return false;

  memcpy(input_buffer_ + value_count_ * getByteSize(), value, getByteSize());
  value_count_++;
  return true;
}

template <size_t T_SIZE>
int ParquetByteStreamSplitEncoder<T_SIZE>::FinalizePage(
    uint8_t* output_buffer, int output_buffer_len) {
  DCHECK(input_buffer_ != nullptr); // NewPage() must be called first
  DCHECK(output_buffer != nullptr);
  DCHECK_GE(output_buffer_len, input_buffer_len_);
  if (value_count_ == 0) return 0;

  Encode(input_buffer_, output_buffer, value_count_, getByteSize());

  int ret = value_count_;
  Reset();
  return ret;
}

template <size_t T_SIZE>
void ParquetByteStreamSplitEncoder<T_SIZE>::Reset() {
  input_buffer_ = nullptr;
  input_buffer_len_ = 0;
  value_count_ = 0;
}

template <size_t T_SIZE>
void ParquetByteStreamSplitEncoder<T_SIZE>::Encode(const uint8_t* to_encode,
    uint8_t* encoded, int value_count, size_t byte_size) {
  DCHECK(to_encode != nullptr);
  DCHECK(encoded != nullptr);

  for (int byte = 0; byte < byte_size; byte++) {
    for (int value = 0; value < value_count; value++) {
      encoded[byte * value_count + value] = to_encode[value * byte_size + byte];
    }
  }
}

template class ParquetByteStreamSplitEncoder<0>;
template class ParquetByteStreamSplitEncoder<4>;
template class ParquetByteStreamSplitEncoder<8>;

} // namespace impala
