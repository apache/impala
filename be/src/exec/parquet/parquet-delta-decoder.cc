///this script to the Apache Software Foundation (ASF) under one
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

#include "exec/parquet/parquet-delta-decoder.h"

#include "util/bit-stream-utils.inline.h"

using strings::Substitute;

namespace impala {

template<typename INT_T>
Status ParquetDeltaDecoder<INT_T>::NewPage(const uint8_t* input_buffer,
    int input_buffer_len) {
  reader_.Reset(input_buffer, input_buffer_len);

  /// Read the header.
  if (!reader_.GetUleb128<std::size_t>(&block_size_in_values_)) {
    return Status("Error reading block size.");
  }

  if (!reader_.GetUleb128<std::size_t>(&miniblocks_in_block_)) {
    return Status("Error reading the number of miniblocks in a block.");
  }

  if (!reader_.GetUleb128<std::size_t>(&total_value_count_)) {
    return Status("Error reading the number of total values.");
  }

  if (!reader_.GetZigZagInteger<INT_T>(&last_read_value_)) {
    return Status("Error reading the first value.");
  }

  /// The number of values in a block must be a non-zero multiple of 128.
  if (UNLIKELY(block_size_in_values_ % 128 != 0 || block_size_in_values_ == 0)) {
    return Status(Substitute("The number of values in a block must be a non-zero "
        "multiple of 128. Current value: $0.", block_size_in_values_));
  }

  /// The number of miniblocks in a block must be a divisor of the number of
  /// values in a block.
  if (UNLIKELY(block_size_in_values_ % miniblocks_in_block_ != 0)) {
    return Status(Substitute("The number of miniblocks in a block must be a divisor of "
        "the number of values in a block. Number of miniblocks in a block: $0, number "
        "of values in a block: $1.", miniblocks_in_block_, block_size_in_values_));
  }

  miniblock_size_in_values_ = block_size_in_values_ / miniblocks_in_block_;

  /// The number of values in a miniblock must be a multiple of 32.
  if (UNLIKELY(miniblock_size_in_values_ % 32 != 0)) {
    return Status(Substitute("The number of values in a miniblock must be a multiple "
        "of 32. Current value: $0.", miniblock_size_in_values_));
  }

  bitwidths_.resize(miniblocks_in_block_);

  input_values_remaining_ = total_value_count_;
  num_buffered_values_ = 0;
  next_buffered_value_index_ = 0;

  /// If there are elements other than the first one, we read the first block header.
  if (LIKELY(input_values_remaining_ > 1)) {
    bool block_header_read = ReadBlockHeader();
    if (UNLIKELY(!block_header_read)) {
      return Status("First block header could not be read.");
    }
  }

  initialized_ = true;
  return Status::OK();
}

template<typename INT_T>
WARN_UNUSED_RESULT
int ParquetDeltaDecoder<INT_T>::NextValue(INT_T* value) {
  return NextValues(1, reinterpret_cast<uint8_t*>(value), sizeof(INT_T));
}

template<typename INT_T>
WARN_UNUSED_RESULT
int ParquetDeltaDecoder<INT_T>::NextValues(int num_values, uint8_t* values,
    std::size_t stride) {
  return GetOrSkipNextValues<false, INT_T>(num_values, values, stride);
}

template<typename INT_T>
template <typename OutType>
int ParquetDeltaDecoder<INT_T>::NextValuesConverted(int num_values, uint8_t* values,
    std::size_t stride) {
  static_assert(std::is_same_v<int8_t, OutType>
      || std::is_same_v<int16_t, OutType>
      || std::is_same_v<int32_t, OutType>
      || std::is_same_v<int64_t, OutType>
      || std::is_same_v<uint8_t, OutType>
      || std::is_same_v<uint16_t, OutType>
      || std::is_same_v<uint32_t, OutType>
      || std::is_same_v<uint64_t, OutType>);
  return GetOrSkipNextValues<false, OutType>(num_values, values, stride);
}

template<typename INT_T>
WARN_UNUSED_RESULT
int ParquetDeltaDecoder<INT_T>::SkipValues(int num_values) {
  return GetOrSkipNextValues<true, INT_T>(num_values, nullptr, sizeof(INT_T));
}

template<typename INT_T>
template<bool SKIP, typename OutType>
int ParquetDeltaDecoder<INT_T>::GetOrSkipNextValues(const int num_values,
    uint8_t* values, std::size_t stride) {
  static_assert(!SKIP || std::is_same_v<INT_T, OutType>,
      "In case of skipping values, the result type is not important.");

  DCHECK(initialized_);
  DCHECK_GE(stride, sizeof(OutType));
  DCHECK_GE(num_values, 0);

  int extracted_values = 0;

  /// Handle the very first value in the input which is in the header.
  if (UNLIKELY(input_values_remaining_ == total_value_count_)) {
    values = GetOrSkipFirstValue<SKIP, OutType>(values, stride);
    extracted_values++;
  }

  /// Get the values from the internal buffer.
  std::pair<int, uint8_t*> buffer_result = GetOrSkipBufferedValues<SKIP, OutType>(
      num_values - extracted_values, values, stride);
  extracted_values += buffer_result.first;
  values = buffer_result.second;

  /// At this point, all the values remaining are in the input buffer as we have consumed
  /// the internal buffer. We avoid extracting more values than available.
  const int yet_to_extract = std::min<std::size_t>(num_values - extracted_values,
      input_values_remaining_);
  if (UNLIKELY(yet_to_extract == 0)) return extracted_values;

  const int extracted_from_input = GetOrSkipFromInput<SKIP, OutType>(yet_to_extract,
      values, stride);
  if (UNLIKELY(extracted_from_input == -1)) return -1;

  extracted_values += extracted_from_input;
  return extracted_values;
}

template<typename INT_T>
bool ParquetDeltaDecoder<INT_T>::FillBuffer() {
  const int values = ReadAndDecodeBatchFromInput<false, INT_T>(buffer_.data(),
      sizeof(INT_T));
  DCHECK(initialized_);
  if (UNLIKELY(values <= 0)) return false;

  num_buffered_values_ = values;
  next_buffered_value_index_ = 0;

  return true;
}

template<typename INT_T>
template<bool SKIP, typename OutType>
uint8_t* ParquetDeltaDecoder<INT_T>::GetOrSkipFirstValue(uint8_t* values, int stride) {
  DCHECK(initialized_);
  DCHECK_GE(stride, sizeof(OutType));
  DCHECK(input_values_remaining_ == total_value_count_);

  if (!SKIP) {
    *reinterpret_cast<OutType*>(values) = last_read_value_;
    values += stride;
  }

  input_values_remaining_--;
  return values;
}

template<typename INT_T>
template<bool SKIP, typename OutType>
std::pair<int, uint8_t*> ParquetDeltaDecoder<INT_T>::GetOrSkipBufferedValues(
    int num_values, uint8_t* values, std::size_t stride) {
  DCHECK(initialized_);
  DCHECK_GE(stride, sizeof(OutType));
  DCHECK_GE(num_values, 0);

  const int remaining_buffered_values =
      num_buffered_values_ - next_buffered_value_index_;

  /// We avoid extracting more values than we have.
  const int num_to_extract = std::min(num_values, remaining_buffered_values);

  /// Consume the remaining values in the buffer.
  values = CopyOrSkipBufferedValues<SKIP, OutType>(num_to_extract, values, stride);
  return std::make_pair(num_to_extract, values);
}

template<typename INT_T>
template<bool SKIP, typename OutType>
int ParquetDeltaDecoder<INT_T>::GetOrSkipFromInput(int num_values, uint8_t* values,
    int stride) {
  DCHECK(initialized_);
  DCHECK_GE(stride, sizeof(OutType));
  DCHECK_GE(num_values, 0);
  DCHECK_LE(num_values, input_values_remaining_);

  /// We extract fully loaded buffers the number of times we can.
  const int full_buffers_to_extract = num_values / BATCH_SIZE;

  for (int i = 0; i < full_buffers_to_extract; i++) {
    const int num_values = ReadAndDecodeBatchFromInput<SKIP, OutType>(
        reinterpret_cast<OutType*>(values), stride);
    if (UNLIKELY(num_values != BATCH_SIZE)) return -1;

    values += num_values * stride;
  }

  int extracted_values = full_buffers_to_extract * BATCH_SIZE;

  /// Extract the remaining number of values.
  const int remainder = num_values - extracted_values;
  if (remainder == 0) return extracted_values;

  bool buffer_filled = FillBuffer();
  if (UNLIKELY(!buffer_filled)) return -1;

  CopyOrSkipBufferedValues<SKIP, OutType>(remainder, values, stride);
  extracted_values += remainder;

  return extracted_values;
}

template <typename INT_T>
template <bool SKIP, typename OutType>
int ParquetDeltaDecoder<INT_T>::ReadAndDecodeBatchFromInput(OutType* __restrict__ out,
    int stride) {
  DCHECK(initialized_);
  DCHECK_GE(stride, sizeof(OutType));
  DCHECK_GE(input_values_remaining_, 0);

  /// Check whether we need to initialise a new block or miniblock.
  /// Before the very first read, `NewPage` has already read the first block header.
  if (values_read_from_miniblock_ == miniblock_size_in_values_) {
    values_read_from_miniblock_ = 0;
    miniblocks_read_from_block_ += 1;

    if (miniblocks_read_from_block_ == miniblocks_in_block_) {
      bool block_header_read = ReadBlockHeader();
      if (UNLIKELY(!block_header_read)) return -1;
    }
  }

  const int bit_width = bitwidths_[miniblocks_read_from_block_];
  int values_read = -1;
  if (SKIP) {
    /// Even if we skip we need to decode the values because the they are needed to
    /// calculate the following values.
    /// TODO: We only need the sum of the deltas, not the intermediate values, so we could
    /// optimize this.
    values_read = reader_.UnpackAndDeltaDecodeBatch<INT_T, INT_T>(bit_width,
        &last_read_value_, min_delta_in_block_, BATCH_SIZE,
        buffer_.data(), sizeof(INT_T));
  } else {
    values_read = reader_.UnpackAndDeltaDecodeBatch<OutType, INT_T>( bit_width,
        &last_read_value_, min_delta_in_block_, BATCH_SIZE, out, stride);

  }

  /// Miniblocks always contain contain a multiple of BATCH_SIZE values or are
  /// padded.
  if (UNLIKELY(values_read != BATCH_SIZE)) return -1;

  values_read_from_miniblock_ += values_read;
  const int actual_values_read = std::min<std::size_t>(
      values_read, input_values_remaining_);
  input_values_remaining_ -= actual_values_read;

  return actual_values_read;
}

template<typename INT_T>
bool ParquetDeltaDecoder<INT_T>::ReadBlockHeader() {
  miniblocks_read_from_block_ = 0;
  values_read_from_miniblock_ = 0;

  bool min_delta_read = reader_.GetZigZagInteger<INT_T>(&min_delta_in_block_);
  if (UNLIKELY(!min_delta_read)) return false;

  for (std::size_t i = 0; i < miniblocks_in_block_; i++) {
    bool bitwidths_read = reader_.GetBytes(1, &bitwidths_[i]);

    if (UNLIKELY(!bitwidths_read)) return false;

    if (UNLIKELY(bitwidths_[i] > sizeof(INT_T) * 8)) {
      return false;
    }
  }

  return true;
}

template<typename INT_T>
template<bool SKIP, typename OutType>
uint8_t* ParquetDeltaDecoder<INT_T>::CopyOrSkipBufferedValues(int num_values,
    uint8_t* dest, int stride) {
  DCHECK_GE(num_values, 0);
  if (SKIP) {
    SkipBufferedValues(num_values);
    return nullptr;
  }

  return CopyBufferedValues<OutType>(num_values, dest, stride);
}

template<typename INT_T>
template <typename OutType>
uint8_t* ParquetDeltaDecoder<INT_T>::CopyBufferedValues(int num_values, uint8_t* dest,
    int stride) {
  DCHECK_GE(stride, sizeof(OutType));
  DCHECK(num_buffered_values_ - next_buffered_value_index_ >= num_values);

  for (int i = 0; i < num_values; i++) {
    *reinterpret_cast<OutType*>(dest) = buffer_[next_buffered_value_index_];

    next_buffered_value_index_++;
    dest += stride;
  }

  return dest;
}

template<typename INT_T>
void ParquetDeltaDecoder<INT_T>::SkipBufferedValues(int num_values) {
  DCHECK(num_buffered_values_ - next_buffered_value_index_ >= num_values);

  next_buffered_value_index_ += num_values;
}

template class ParquetDeltaDecoder<int32_t>;
template class ParquetDeltaDecoder<int64_t>;

template int ParquetDeltaDecoder<int32_t>::NextValuesConverted<int8_t>(int num_values,
    uint8_t* values, std::size_t stride);
template int ParquetDeltaDecoder<int32_t>::NextValuesConverted<int16_t>(int num_values,
    uint8_t* values, std::size_t stride);
template int ParquetDeltaDecoder<int32_t>::NextValuesConverted<int32_t>(int num_values,
    uint8_t* values, std::size_t stride);
template int ParquetDeltaDecoder<int32_t>::NextValuesConverted<int64_t>(int num_values,
    uint8_t* values, std::size_t stride);

template int ParquetDeltaDecoder<int64_t>::NextValuesConverted<int64_t>(int num_values,
    uint8_t* values, std::size_t stride);
} // namespace impala
