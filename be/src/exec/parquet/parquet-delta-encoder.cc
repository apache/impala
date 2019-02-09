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

#include "exec/parquet/parquet-delta-encoder.h"

#include <cmath>
#include <cstring>
#include <limits>

#include "util/arithmetic-util.h"
#include "util/bit-stream-utils.inline.h"
#include "util/bit-util.h"

namespace impala {

template<typename INT_T>
ParquetDeltaEncoder<INT_T>::ParquetDeltaEncoder()
  : block_size_in_values_(0),
    miniblocks_in_block_(0),
    miniblock_size_in_values_(0),
    max_page_value_count_(0),
    reserved_space_for_header_(0),
    total_value_count_(0),
    delta_buffer_{},
    min_delta_in_block_(std::numeric_limits<INT_T>::max()),
    output_buffer_(nullptr),
    output_buffer_len_(0)
{}

template<typename INT_T>
Status ParquetDeltaEncoder<INT_T>::Init(size_t block_size_in_values,
    size_t miniblocks_in_block, unsigned int max_page_value_count) {
  using strings::Substitute;

  if (IsInitialized()) {
    return Status("Already initialized.");
  }

  if (block_size_in_values == 0) {
    return Status("The number of values in a block must not be zero.");
  }

  if (block_size_in_values % 128 != 0) {
    return Status(Substitute("The number of values in a block must be a "
        "multiple of 128. $0 is not a multiple of 128.", block_size_in_values));
  }

  if (miniblocks_in_block == 0) {
    return Status("The number of miniblocks in a block must not be zero.");
  }

  if (block_size_in_values % miniblocks_in_block != 0) {
    return Status(Substitute("The number of miniblocks in a block must be a divisor of "
        "the number of values in a block. $0"" is not a divisor of $1.",
        miniblocks_in_block, block_size_in_values));
  }

  size_t miniblock_size_in_values = block_size_in_values / miniblocks_in_block;
  if (miniblock_size_in_values % 32 != 0) {
    return Status(Substitute("The number of values in a miniblock must be a multiple "
        "of 32. $0 is not a multiple of 32.", miniblock_size_in_values));
  }

  if (max_page_value_count == 0) {
    return Status("The number of values allowed in a page must be non-zero.");
  }

  block_size_in_values_ = block_size_in_values;
  miniblocks_in_block_ = miniblocks_in_block;
  miniblock_size_in_values_ = block_size_in_values / miniblocks_in_block;
  max_page_value_count_ = max_page_value_count;

  delta_buffer_.reserve(block_size_in_values_);
  return Status::OK();
}

template<typename INT_T>
void ParquetDeltaEncoder<INT_T>::NewPage(uint8_t* output_buffer, int output_buffer_len) {
  DCHECK(IsInitialized());
  DCHECK(output_buffer != nullptr);

  SetOutputBuffer(output_buffer, output_buffer_len);
  ResetBlock();

  /// Reset the counter. The other fields will automatically be updated when writing
  /// other pages.
  total_value_count_ = 0;
}

template<typename INT_T>
int ParquetDeltaEncoder<INT_T>::FinalizePage() {
  DCHECK(IsInitialized());

  // Write the last block.
  const bool flushing_succeeded = FlushBlock();
  DCHECK(flushing_succeeded);

  /// Move the written data if the space initially reserved for the header was too
  /// little or too large.
  const int actual_header_size = HeaderSize(first_value_, total_value_count_);

  uint8_t* const old_data_start_address = output_buffer_ + reserved_space_for_header_;
  const int data_len = output_buffer_pos_ - old_data_start_address;

  if (actual_header_size != reserved_space_for_header_) {
    uint8_t* const new_data_start_address = output_buffer_ + actual_header_size;

    /// After this, the value of output_buffer_pos_ and output_buffer_len_ may
    /// be invalid.
    std::memmove(new_data_start_address, old_data_start_address, data_len);
  }

  const bool header_written = WriteHeader();
  DCHECK(header_written);

  // Reset the output buffer and its length so that subsequent 'Put()' calls do not
  // succeed.
  SetOutputBuffer(nullptr, 0);

  /// Return the number of bytes written.
  return actual_header_size + data_len;
}

template<typename INT_T>
bool ParquetDeltaEncoder<INT_T>::Put(INT_T value) {
  DCHECK(IsInitialized());

  if (UNLIKELY(total_value_count_ == 0)) {
    first_value_ = value;
    previous_value_ = value;

    // The header size is not known in advance so we reserve the maximum amount of space
    // it may be. The data will be written after this. If the header turns out to be
    // smaller, the data will be moved in 'FinalizePage()' because padding between the
    // header and the data is not allowed.
    reserved_space_for_header_ = HeaderSize(first_value_, max_page_value_count_);
    if (reserved_space_for_header_ > output_buffer_len_) return false;
    AdvanceBufferPos(reserved_space_for_header_);

    ++total_value_count_;
    return true;
  }

  /// If this is the first value in a new block, we check if the block fits in the buffer
  /// in the worst case; if not, we reject the value and the caller has to call
  /// 'FinalizePage()', 'NewPage()' and put the value again.
  if (delta_buffer_.empty() && WorstCaseBlockSize() > output_buffer_len_) return false;

  /// We do not allow writing more values than the maximum.
  if (total_value_count_ >= max_page_value_count_) return false;

  INT_T delta = ArithmeticUtil::AsUnsigned<std::minus>(value, previous_value_);
  previous_value_ = value;

  delta_buffer_.push_back(delta);

  if (delta < min_delta_in_block_) {
    min_delta_in_block_ = delta;
  }

  if (delta_buffer_.size() == block_size_in_values_) {
    /// FlushBlock returns false if there is not enough space in the buffer. Flushing
    /// should never fail because we check whether the block fits inside the buffer in
    /// the worst case when processing the first element.
    bool success = FlushBlock();
    DCHECK(success);
  }

  ++total_value_count_;
  return true;
}

template <typename INT_T>
int ParquetDeltaEncoder<INT_T>::WorstCaseOutputSize(const int value_count) const {
  DCHECK(IsInitialized());

  if (value_count == 0) return 0;

  /// The most negative value will have maximal ZigZag bit size.
  const INT_T most_negative_first_value = std::numeric_limits<INT_T>::min();

  /// We pass 'max_page_value_count_' instead of value_count because the writer will
  /// reserve header space for that value.
  const int header_size = HeaderSize(most_negative_first_value, max_page_value_count_);

  /// The first value is in the header.
  const unsigned int values_in_blocks = value_count - 1;
  const unsigned int n_of_blocks = (values_in_blocks + block_size_in_values_ - 1)
    / block_size_in_values_;

  return header_size + n_of_blocks * WorstCaseBlockSize();
}

template<typename INT_T>
bool ParquetDeltaEncoder<INT_T>::IsInitialized() const {
  // If any of these values is 0 then all should be.
  if (block_size_in_values_ == 0
      || miniblocks_in_block_ == 0
      || miniblock_size_in_values_ == 0) {
    DCHECK(block_size_in_values_ == 0 &&
        block_size_in_values_ == miniblocks_in_block_ &&
        block_size_in_values_ == miniblock_size_in_values_);
  }
  return block_size_in_values_ != 0;
}

template<typename INT_T>
bool ParquetDeltaEncoder<INT_T>::WriteHeader() {
  DCHECK(IsInitialized());

  const int header_size = HeaderSize(first_value_, total_value_count_);

  /// Write the header.
  BitWriter writer(output_buffer_, header_size);

  bool success = true;
  success = success && writer.PutUleb128<UINT_T>(block_size_in_values_);
  success = success && writer.PutUleb128<UINT_T>(miniblocks_in_block_);
  success = success && writer.PutUleb128<UINT_T>(total_value_count_);
  success = success && writer.PutZigZagInteger<INT_T>(first_value_);

  /// Check whether we calculated the header size correctly.
  uint8_t* header_end_ptr = writer.GetNextBytePtr(0);
  DCHECK_EQ(header_end_ptr - output_buffer_, header_size);

  return success;
}

template<typename INT_T>
bool ParquetDeltaEncoder<INT_T>::FlushBlock() {
  DCHECK(IsInitialized());

  if (delta_buffer_.empty()) return true;

  /// Calculate the deltas relative to the min delta.
  for (std::size_t i = 0; i < delta_buffer_.size(); ++i) {
    delta_buffer_[i] = ArithmeticUtil::AsUnsigned<std::minus>(
        delta_buffer_[i], min_delta_in_block_);
  }

  if (!WriteMinDelta()) return false;

  const std::vector<uint8_t> miniblock_bitwidths = CalculateMiniblockBitwidths();

  if (!WriteMiniblockWidths(miniblock_bitwidths)) return false;
  if (!WriteMiniblocks(miniblock_bitwidths)) return false;

  ResetBlock();
  return true;
}

template<typename INT_T>
bool ParquetDeltaEncoder<INT_T>::WriteMinDelta() {
  DCHECK(IsInitialized());

  BitWriter writer(output_buffer_pos_, output_buffer_len_);
  bool success = writer.PutZigZagInteger<INT_T>(min_delta_in_block_);
  if (UNLIKELY(!success)) return false;

  /// Flushes the writer.
  uint8_t* next_free_byte = writer.GetNextBytePtr(0) ;
  int written_length = next_free_byte - output_buffer_pos_;

  AdvanceBufferPos(written_length);
  return true;
}

// Returns a vector with the bitwidths of the miniblocks in the current block. Does
// not include empty miniblocks.
template<typename INT_T>
std::vector<uint8_t> ParquetDeltaEncoder<INT_T>::CalculateMiniblockBitwidths() {
  DCHECK(IsInitialized());

  const int num_actual_miniblocks =
    (delta_buffer_.size() + miniblock_size_in_values_ - 1) / miniblock_size_in_values_;

  std::vector<uint8_t> result(num_actual_miniblocks);

  for (std::size_t miniblock_index = 0;
      miniblock_index < num_actual_miniblocks;
      ++miniblock_index) {
    const std::size_t miniblock_start_index =
        miniblock_index * miniblock_size_in_values_;
    const std::size_t miniblock_end_index = std::min(
        (miniblock_index + 1) * miniblock_size_in_values_, delta_buffer_.size());
    UINT_T mask = 0;

    for (std::size_t i = miniblock_start_index; i < miniblock_end_index; ++i) {
      mask |= delta_buffer_[i];
    }

    result[miniblock_index] = BitWriter::NumberOfSignificantBits(mask);
  }

  return result;
}

template<typename INT_T>
bool ParquetDeltaEncoder<INT_T>::WriteMiniblockWidths(
    const std::vector<uint8_t>& miniblock_bitwidths) {
    DCHECK(IsInitialized());
    DCHECK(miniblock_bitwidths.size() <= miniblocks_in_block_);

    if (UNLIKELY(miniblock_bitwidths.size() > output_buffer_len_)) return false;
    if (UNLIKELY(miniblock_bitwidths.empty())) return true;

    memcpy(output_buffer_pos_, miniblock_bitwidths.data(), miniblock_bitwidths.size());
    AdvanceBufferPos(miniblock_bitwidths.size());

    /// If the number of miniblocks in the last block is smaller than
    /// 'miniblocks_in_block_', we still have to write 'miniblocks_in_block_' bitwidths.
    if (UNLIKELY(miniblock_bitwidths.size() < miniblocks_in_block_)) {
      int diff = miniblocks_in_block_ - miniblock_bitwidths.size();
      std::memset(output_buffer_pos_, 0, diff);
      AdvanceBufferPos(diff);
    }

    return true;
  }

template<typename INT_T>
bool ParquetDeltaEncoder<INT_T>::WriteMiniblocks(
    const std::vector<uint8_t>& miniblock_bitwidths) {
  DCHECK(IsInitialized());

  const std::size_t num_miniblocks = miniblock_bitwidths.size();
  BitWriter writer(output_buffer_pos_, output_buffer_len_);
  std::size_t delta_buffer_index = 0;

  for (int miniblock_index = 0; miniblock_index < num_miniblocks; ++miniblock_index) {
    const int bitwidth = miniblock_bitwidths[miniblock_index];

    for (int i = 0; i < miniblock_size_in_values_; ++i) {
      /// We convert the value to unsigned as BitWriter expects an uint64_t. Relying on
      /// the automatic conversion leads to an incorrect result in case of 32 bit integers
      /// as they are sign-extended during the conversion. If there is no value left, we
      /// pad with zeroes.
      const UINT_T value_to_write = delta_buffer_index < delta_buffer_.size() ?
        delta_buffer_[delta_buffer_index] : 0;

      /// BitWriter checks whether there is enough space left in the buffer and returns
      /// false if not.
      bool success = writer.PutValue(value_to_write, bitwidth);
      if (UNLIKELY(!success)) return false;
      ++delta_buffer_index;
    }

    /// Get the next byte position after this miniblock. This flushes the writer. For the
    /// last miniblock in the last block, the number of values written may not be byte
    /// aligned but BitWriter pads the byte with zeroes.
    const uint8_t* next_free_byte = writer.GetNextBytePtr(0);
    const int written_bytes = next_free_byte ?
      next_free_byte - output_buffer_pos_ : output_buffer_len_;
    AdvanceBufferPos(written_bytes);
  }

  return true;
}

template<typename INT_T>
void ParquetDeltaEncoder<INT_T>::ResetBlock() {
  DCHECK(IsInitialized());

  min_delta_in_block_ = std::numeric_limits<INT_T>::max();
  delta_buffer_.clear();
}

template<typename INT_T>
void ParquetDeltaEncoder<INT_T>::SetOutputBuffer(uint8_t* output_buffer,
    int output_buffer_len) {
  DCHECK(IsInitialized());
  DCHECK(output_buffer != nullptr || output_buffer_len == 0);
  output_buffer_ = output_buffer;
  output_buffer_pos_ = output_buffer_;
  output_buffer_len_ = output_buffer_len;
}

template<typename INT_T>
int ParquetDeltaEncoder<INT_T>::HeaderSize(INT_T first_value,
    unsigned int total_value_count) const {
  DCHECK(IsInitialized());

  const int block_size_len = BitWriter::VlqRequiredSize(block_size_in_values_);
  const int miniblock_count_len = BitWriter::VlqRequiredSize(miniblocks_in_block_);
  const int total_value_count_len = BitWriter::VlqRequiredSize(total_value_count);
  const int first_value_len = BitWriter::ZigZagRequiredBytes(first_value);

  return block_size_len + miniblock_count_len + total_value_count_len + first_value_len;
}

template<typename INT_T>
int ParquetDeltaEncoder<INT_T>::WorstCaseBlockSize() const {
  DCHECK(IsInitialized());

  const int min_delta_length = MAX_ZIGZAG_BYTE_LEN;
  const int miniblock_widths_length = miniblocks_in_block_;

  /// In the worst case, each delta takes the same amount of space as the original value.
  const int miniblock_size = miniblock_size_in_values_ * sizeof(INT_T);

  return min_delta_length + miniblock_widths_length
    + miniblocks_in_block_ * miniblock_size;
}

template<typename INT_T>
void ParquetDeltaEncoder<INT_T>::AdvanceBufferPos(int bytes) {
  DCHECK(IsInitialized());
  DCHECK(bytes <= output_buffer_len_);
  output_buffer_len_ -= bytes;
  output_buffer_pos_ += bytes;
}

template class ParquetDeltaEncoder<int32_t>;
template class ParquetDeltaEncoder<int64_t>;

} // namespace impala
