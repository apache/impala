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

#include "parquet-byte-stream-split-decoder.h"

#include <cstring>

namespace impala {

template <size_t T_SIZE>
void ParquetByteStreamSplitDecoder<T_SIZE>::NewPage(
    const uint8_t* input_buffer, int input_buffer_len) {
  DCHECK_GE(input_buffer_len, 0);
  DCHECK(input_buffer_len % getByteSize() == 0);
  DCHECK(input_buffer != nullptr);

  input_buffer_ = input_buffer;
  input_buffer_len_ = input_buffer_len;
  value_index_ = 0;
}

template <size_t T_SIZE>
int ParquetByteStreamSplitDecoder<T_SIZE>::NextValues(
    int num_values, uint8_t* values, std::size_t stride) {
  DCHECK(input_buffer_ != nullptr);
  DCHECK_GE(num_values, 0);
  DCHECK_GE(stride, getByteSize());

  if (num_values == 0) return 0;
  if (input_buffer_len_ == 0) return 0;

  // If reading num_values values would overstep the total values stored,
  // then decrease its value to read until the last value possible.
  if (value_index_ + num_values > GetTotalValueCount()) {
    num_values = GetTotalValueCount() - value_index_;
  }

  for (int i = 0; i < num_values; i++) {
    for (int j = 0; j < getByteSize(); j++) {
      const uint8_t* ptr = input_buffer_ + value_index_ + j * GetTotalValueCount();
      values[i * stride + j] = *ptr;
    }
    value_index_++;
  }
  return num_values;
}

template <size_t T_SIZE>
int ParquetByteStreamSplitDecoder<T_SIZE>::SkipValues(int num_values) {
  DCHECK_GE(num_values, 0);
  DCHECK(input_buffer_ != nullptr);

  if (input_buffer_len_ == 0) return 0;

  if (value_index_ + num_values > GetTotalValueCount()) {
    num_values = GetTotalValueCount() - value_index_;
  }

  value_index_ += num_values;
  return num_values;
};

template class ParquetByteStreamSplitDecoder<0>;
template class ParquetByteStreamSplitDecoder<4>;
template class ParquetByteStreamSplitDecoder<8>;

} // namespace impala
