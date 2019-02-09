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

#include <algorithm>
#include <limits>
#include <random>
#include <vector>

#include "gtest/gtest.h"

#include "exec/parquet/parquet-delta-decoder.h"
#include "exec/parquet/parquet-delta-encoder.h"
#include "exec/parquet/parquet-delta-coder-test-data.h"

#include "testutil/gtest-util.h"
#include "testutil/rand-util.h"

#include <iostream>
#include <fstream>

namespace impala {

template <typename INT_T>
class DeltaCoderTest {
 public:
  static std::vector<uint8_t> encode_all(const std::vector<INT_T>& plain,
      const std::size_t block_size,
      const std::size_t miniblock_size) {
    const std::size_t miniblocks_in_block = block_size / miniblock_size;
    DCHECK(block_size % miniblock_size == 0);

    ParquetDeltaEncoder<INT_T> encoder;
    Status init_success = encoder.Init(block_size, miniblocks_in_block);
    EXPECT_OK(init_success);

    std::vector<uint8_t> buffer(encoder.WorstCaseOutputSize(plain.size()), 0);
    encoder.NewPage(buffer.data(), buffer.size());

    for (INT_T value : plain) {
      EXPECT_TRUE(encoder.Put(value));
    }

    int written_bytes = encoder.FinalizePage();
    EXPECT_GT(written_bytes, 0);

    buffer.resize(written_bytes);

    return buffer;
  }

  static std::vector<uint8_t> decode_all(const std::vector<uint8_t> encoded, int stride) {
    DCHECK_GE(stride, sizeof(INT_T));
    ParquetDeltaDecoder<INT_T> decoder;
    Status page_init_success = decoder.NewPage(encoded.data(), encoded.size());
    EXPECT_OK(page_init_success);

    const std::size_t total_value_count = decoder.GetTotalValueCount();
    std::vector<uint8_t> result(total_value_count * stride, 0);

    int decoded_values = decoder.NextValues(total_value_count, result.data(), stride);
    EXPECT_EQ(total_value_count, decoded_values);

    return result;
  }

  template <typename OutType>
  static std::vector<uint8_t> decode_and_convert_all(const std::vector<uint8_t> encoded,
      int stride) {
    DCHECK_GE(stride, sizeof(OutType));
    ParquetDeltaDecoder<INT_T> decoder;
    Status page_init_success = decoder.NewPage(encoded.data(), encoded.size());
    EXPECT_OK(page_init_success);

    const std::size_t total_value_count = decoder.GetTotalValueCount();
    std::vector<uint8_t> result(total_value_count * stride, 0);

    int decoded_values = decoder.template NextValuesConverted<OutType>(total_value_count,
        result.data(), stride);
    EXPECT_EQ(total_value_count, decoded_values);

    return result;
  }

  static void expect_encoded_data(const std::vector<INT_T>& data,
      const std::vector<uint8_t>& expected) {
    const std::vector<uint8_t> encoded = encode_all(data, 128, 32);

    EXPECT_EQ(expected, encoded);
  }

  static void expect_decoded_data(const std::vector<INT_T>& plain,
      const std::vector<uint8_t>& encoded) {
    constexpr int stride = sizeof(INT_T);
    const std::vector<uint8_t> decoded_bytes = decode_all(encoded, stride);
    std::vector<INT_T> decoded(decoded_bytes.size() / sizeof(INT_T));
    memcpy(decoded.data(), decoded_bytes.data(), decoded_bytes.size());
    EXPECT_EQ(plain, decoded);
  }

  template <typename OutType>
  static void expect_equal_with_stride(const std::vector<INT_T>& plain,
      const vector<uint8_t>& decoded, int stride) {
    ASSERT_EQ(plain.size(), decoded.size() / stride);
    ASSERT_EQ(0, decoded.size() % stride);

    for (std::size_t i = 0; i < plain.size(); i++) {
      const uint8_t* decoded_ptr = &decoded[i * stride];
      const OutType decoded_value = *reinterpret_cast<const OutType*>(decoded_ptr);

      const OutType plain_converted = static_cast<OutType>(plain[i]);
      EXPECT_EQ(plain_converted, decoded_value) << "Index: " << i << ".";
    }
  }

  template <typename OutType>
  static void expect_decoded_data_converted(const std::vector<INT_T>& plain,
      const std::vector<uint8_t>& encoded, int stride) {
    DCHECK_GE(stride, sizeof(OutType));
    const std::vector<uint8_t> decoded_bytes = decode_and_convert_all<OutType>(encoded,
        stride);

    expect_equal_with_stride<OutType>(plain, decoded_bytes, stride);
  }

  static void expect_decoded_data_one_by_one(const std::vector<INT_T>& plain,
      const std::vector<uint8_t>& encoded) {
    ParquetDeltaDecoder<INT_T> decoder;
    Status page_init_success = decoder.NewPage(encoded.data(), encoded.size());
    EXPECT_OK(page_init_success);

    const std::size_t total_value_count = decoder.GetTotalValueCount();
    EXPECT_EQ(plain.size(), total_value_count);

    std::vector<uint8_t> decoded_bytes(total_value_count * sizeof(INT_T), 0);

    for (std::size_t i = 0; i < total_value_count; i++) {
      const std::size_t byte_index = i * sizeof(INT_T);
      INT_T* const int_ptr = reinterpret_cast<INT_T*>(&decoded_bytes[byte_index]);
      const int decoded_values = decoder.NextValue(int_ptr);
      EXPECT_EQ(1, decoded_values);
    }

    std::vector<INT_T> decoded(decoded_bytes.size() / sizeof(INT_T));
    memcpy(decoded.data(), decoded_bytes.data(), decoded_bytes.size());
    EXPECT_EQ(plain, decoded);
  }

};

template <typename INT_T>
struct block_aligned_for_type;

template <>
struct block_aligned_for_type<int32_t> {
  constexpr static const std::vector<int32_t>& plain = block_aligned_plain;
  constexpr static const std::vector<uint8_t>& encoded = block_aligned_encoded;
};

template <>
struct block_aligned_for_type<int64_t> {
  constexpr static const std::vector<int64_t>& plain = block_aligned_64_bits_plain;
  constexpr static const std::vector<uint8_t>& encoded = block_aligned_64_bits_encoded;
};

std::vector<int64_t> convert_vec_to_int64_t(const vector<int32_t>& vec) {
  std::vector<int64_t> res(vec.size());

  for (std::size_t i = 0; i < vec.size(); i++) {
    res[i] = vec[i];
  }

  return res;
}

/// Encoder tests.
TEST(ParquetDeltaEncoder, Initialize) {
  using INT_T = int64_t;
  ParquetDeltaEncoder<INT_T> encoder;

  Status init_success;

  // Block size not divisible by 128.
  init_success = encoder.Init(64, 2);
  EXPECT_FALSE(init_success.ok());

  // Block size of 0.
  init_success = encoder.Init(0, 2);
  EXPECT_FALSE(init_success.ok());

  // Miniblock number not a divisor of block size.
  init_success = encoder.Init(128, 3);
  EXPECT_FALSE(init_success.ok());

  // Zero miniblocks in block.
  init_success = encoder.Init(128, 0);
  EXPECT_FALSE(init_success.ok());

  // Miniblock size not divisible by 32.
  init_success = encoder.Init(128, 8);
  EXPECT_FALSE(init_success.ok());

  // Zero values allowed in page.
  init_success = encoder.Init(256, 8, 0);
  EXPECT_FALSE(init_success.ok());

  // All correct.
  init_success = encoder.Init(256, 8);
  EXPECT_TRUE(init_success.ok());

  // Re-initialisation is not allowed.
  init_success = encoder.Init(256, 8);
  EXPECT_FALSE(init_success.ok());
}

TEST(ParquetDeltaEncoder, HeaderOnly32) {
  DeltaCoderTest<int32_t>::expect_encoded_data(header_only_data, header_only_expected);
}

TEST(ParquetDeltaEncoder, HeaderOnly64) {
  DeltaCoderTest<int64_t>::expect_encoded_data(convert_vec_to_int64_t(header_only_data),
      header_only_expected);
}

TEST(ParquetDeltaEncoder, ShortBlock32) {
  DeltaCoderTest<int32_t>::expect_encoded_data(short_block_plain, short_block_encoded);
}

TEST(ParquetDeltaEncoder, BlockAligned32) {
  DeltaCoderTest<int32_t>::expect_encoded_data(block_aligned_plain,
      block_aligned_encoded);
}

TEST(ParquetDeltaEncoder, BlockAligned64) {
  DeltaCoderTest<int64_t>::expect_encoded_data(block_aligned_64_bits_plain,
      block_aligned_64_bits_encoded);
}

TEST(ParquetDeltaEncoder, BlockNotFullyWritten32) {
  DeltaCoderTest<int32_t>::expect_encoded_data(block_not_fully_written_plain,
      block_not_fully_written_encoded);
}

TEST(ParquetDeltaEncoder, MiniblockNotFullyWritten32) {
  DeltaCoderTest<int32_t>::expect_encoded_data(miniblock_not_fully_written_plain,
      miniblock_not_fully_written_encoded);
}

TEST(ParquetDeltaEncoder, NegativeDeltas32) {
  DeltaCoderTest<int32_t>::expect_encoded_data(negative_deltas_plain,
      negative_deltas_encoded);
}

TEST(ParquetDeltaEncoder, DeltasAreTheSame32) {
  DeltaCoderTest<int32_t>::expect_encoded_data(deltas_are_the_same_plain,
      deltas_are_the_same_encoded);
}

TEST(ParquetDeltaEncoder, ValuesAreTheSame32) {
  DeltaCoderTest<int32_t>::expect_encoded_data(values_are_the_same_plain,
      values_are_the_same_encoded);
}

TEST(ParquetDeltaEncoder, DeltaIsZeroForEachBlock32) {
  DeltaCoderTest<int32_t>::expect_encoded_data(delta_is_zero_for_each_block_plain,
                      delta_is_zero_for_each_block_encoded);
}

TEST(ParquetDeltaEncoder, DataIsNotAlignedWithBlock32) {
  DeltaCoderTest<int32_t>::expect_encoded_data(data_is_not_aligned_with_block_plain,
                      data_is_not_aligned_with_block_encoded);
}

TEST(ParquetDeltaEncoder, MaxMinValue32) {
  DeltaCoderTest<int32_t>::expect_encoded_data(max_min_value_plain,
      max_min_value_encoded);
}

template <class INT_T>
void PutAfterFinalizePageFails() {
  constexpr int block_size = 128;
  constexpr int miniblocks_in_block = 4;

  ParquetDeltaEncoder<INT_T> encoder;
  Status init_success = encoder.Init(block_size, miniblocks_in_block);
  ASSERT_OK(init_success);

  std::vector<INT_T> first_page = {1, 2};

  // We should have room for at least a full block after we insert values and
  // finalise the page.
  const int values_reservation = block_size + first_page.size();
  std::vector<uint8_t> buffer(encoder.WorstCaseOutputSize(values_reservation), 0);
  encoder.NewPage(buffer.data(), buffer.size());

  for (INT_T value : first_page) {
    EXPECT_TRUE(encoder.Put(value));
  }

  // Finalize page.
  int written_bytes = encoder.FinalizePage();
  EXPECT_GT(written_bytes, 0);

  // Try to insert a new value without starting a new page.
  constexpr INT_T second_page_value = 1;
  EXPECT_FALSE(encoder.Put(second_page_value));

  // Check that only the legally inserted elements are present.
  DeltaCoderTest<INT_T>::expect_decoded_data(first_page, buffer);

  // Start a new page (we can overwrite the previous one now). After this the values
  // should be inserted successfully.
  encoder.NewPage(buffer.data(), buffer.size());
  EXPECT_TRUE(encoder.Put(second_page_value));

  // Finalize the second page.
  written_bytes = encoder.FinalizePage();
  EXPECT_GT(written_bytes, 0);

  DeltaCoderTest<INT_T>::expect_decoded_data({second_page_value}, buffer);
}

TEST(ParquetDeltaEncoder, PutAfterFinalizePageFails32) {
  PutAfterFinalizePageFails<int32_t>();
}

TEST(ParquetDeltaEncoder, PutAfterFinalizePageFails64) {
  PutAfterFinalizePageFails<int64_t>();
}

template <class INT_T>
void PutFailureAtBufferEndLeavesCleanState() {
  constexpr int block_size = 128;
  constexpr int miniblocks_in_block = 4;

  ParquetDeltaEncoder<INT_T> encoder;
  Status init_success = encoder.Init(block_size, miniblocks_in_block);
  ASSERT_OK(init_success);

  const int buffer_len_for_block = encoder.WorstCaseOutputSize(block_size);
  const int real_buffer_len = encoder.WorstCaseOutputSize(block_size + 10);

  std::vector<INT_T> first_block;
  // The first value is in the header, so we need one more to fill the block.
  for (int i = 0; i < block_size + 1; i++) {
    // We should have large deltas so no space remains in the block.
    INT_T value = i % 2 == 0 ?
      std::numeric_limits<INT_T>::max() : std::numeric_limits<INT_T>::min();
    first_block.push_back(value);
  }

  std::vector<uint8_t> buffer(real_buffer_len);
  encoder.NewPage(buffer.data(), buffer_len_for_block);

  for (INT_T value : first_block) {
    EXPECT_TRUE(encoder.Put(value));
  }

  // There is no space for more elements.
  EXPECT_FALSE(encoder.Put(10));

  // Finalize page.
  int written_bytes = encoder.FinalizePage();
  EXPECT_GT(written_bytes, 0);

  // Check that only the legally inserted elements are present.
  DeltaCoderTest<INT_T>::expect_decoded_data(first_block, buffer);
}

TEST(ParquetDeltaEncoder, PutFailureAtBufferEndLeavesCleanState32) {
  PutFailureAtBufferEndLeavesCleanState<int32_t>();
}

TEST(ParquetDeltaEncoder, PutFailureAtBufferEndLeavesCleanState64) {
  PutFailureAtBufferEndLeavesCleanState<int64_t>();
}

template <class INT_T>
void PutFailureMaxNumValuesLeavesCleanState() {
  constexpr int block_size = 128;
  constexpr int miniblocks_in_block = 4;
  constexpr unsigned int max_page_value_count =
      ParquetDeltaEncoder<INT_T>::DEFAULT_MAX_TOTAL_VALUE_COUNT;

  ParquetDeltaEncoder<INT_T> encoder;
  Status init_success = encoder.Init(block_size, miniblocks_in_block,
      max_page_value_count);
  ASSERT_OK(init_success);

  const int buffer_len = encoder.WorstCaseOutputSize(max_page_value_count);
  std::vector<uint8_t> buffer(buffer_len);

  std::vector<INT_T> first_page(max_page_value_count, 0);

  encoder.NewPage(buffer.data(), buffer.size());

  for (INT_T value : first_page) {
    EXPECT_TRUE(encoder.Put(value));
  }

  // There would be space for more elements because all values are the same, which takes
  // up much less space than the worst case for which we have reservation; but the maximal
  // number of values for a page has been reached.
  EXPECT_FALSE(encoder.Put(0));

  // Finalize page.
  int written_bytes = encoder.FinalizePage();
  EXPECT_GT(written_bytes, 0);

  // Check that only the legally inserted elements are present.
  DeltaCoderTest<INT_T>::expect_decoded_data(first_page, buffer);
}

TEST(ParquetDeltaEncoder, PutFailureMaxNumValuesLeavesCleanState32) {
  PutFailureMaxNumValuesLeavesCleanState<int32_t>();
}

TEST(ParquetDeltaEncoder, PutFailureMaxNumValuesLeavesCleanState64) {
  PutFailureMaxNumValuesLeavesCleanState<int64_t>();
}

template <class INT_T>
void StartNewPageWithoutFinalizeDiscardsValues() {
  constexpr int block_size = 128;
  constexpr int miniblocks_in_block = 4;

  ParquetDeltaEncoder<INT_T> encoder;
  Status init_success = encoder.Init(block_size, miniblocks_in_block);
  ASSERT_OK(init_success);

  constexpr int num_values = 5;
  const int buffer_len = encoder.WorstCaseOutputSize(num_values);
  std::vector<uint8_t> buffer(buffer_len);

  const std::vector<INT_T> unwritten_page(num_values, 0);
  encoder.NewPage(buffer.data(), buffer.size());
  for (INT_T value : unwritten_page) {
    EXPECT_TRUE(encoder.Put(value));
  }

  // We start a new page without finalising the previous one.
  const std::vector<INT_T> written_page(num_values, 5);
  encoder.NewPage(buffer.data(), buffer.size());
  for (INT_T value : written_page) {
    EXPECT_TRUE(encoder.Put(value));
  }

  // Finalize page.
  int written_bytes = encoder.FinalizePage();
  EXPECT_GT(written_bytes, 0);

  // Check that only the elements from the second page elements are present.
  DeltaCoderTest<INT_T>::expect_decoded_data(written_page, buffer);
}

TEST(ParquetDeltaEncoder, StartNewPageWithoutFinalizeDiscardsValues32) {
  StartNewPageWithoutFinalizeDiscardsValues<int32_t>();
}

TEST(ParquetDeltaEncoder, StartNewPageWithoutFinalizeDiscardsValues64) {
  StartNewPageWithoutFinalizeDiscardsValues<int64_t>();
}

/// Decoder tests.
TEST(ParquetDeltaDecoder, HeaderOnly32) {
  DeltaCoderTest<int32_t>::expect_decoded_data(header_only_data,
      header_only_expected);
}

TEST(ParquetDeltaDecoder, HeaderOnly64) {
  DeltaCoderTest<int64_t>::expect_decoded_data(convert_vec_to_int64_t(header_only_data),
      header_only_expected);
}

TEST(ParquetDeltaDecoder, ShortBlock32) {
  DeltaCoderTest<int32_t>::expect_decoded_data(short_block_plain,
      short_block_encoded);
}

TEST(ParquetDeltaDecoder, BlockAligned32) {
  DeltaCoderTest<int32_t>::expect_decoded_data(block_aligned_plain,
      block_aligned_encoded);
}

TEST(ParquetDeltaDecoder, BlockNotFullyWritten32) {
  DeltaCoderTest<int32_t>::expect_decoded_data(block_not_fully_written_plain,
      block_not_fully_written_encoded);
}

TEST(ParquetDeltaDecoder, MiniblockNotFullyWritten32) {
  DeltaCoderTest<int32_t>::expect_decoded_data(miniblock_not_fully_written_plain,
      miniblock_not_fully_written_encoded);
}

TEST(ParquetDeltaDecoder, NegativeDeltas32) {
  DeltaCoderTest<int32_t>::expect_decoded_data(negative_deltas_plain,
      negative_deltas_encoded);
}

TEST(ParquetDeltaDecoder, DeltasAreTheSame32) {
  DeltaCoderTest<int32_t>::expect_decoded_data(deltas_are_the_same_plain,
      deltas_are_the_same_encoded);
}

TEST(ParquetDeltaDecoder, ValuesAreTheSame32) {
  DeltaCoderTest<int32_t>::expect_decoded_data(values_are_the_same_plain,
      values_are_the_same_encoded);
}

TEST(ParquetDeltaDecoder, DeltaIsZeroForEachBlock32) {
  DeltaCoderTest<int32_t>::expect_decoded_data(delta_is_zero_for_each_block_plain,
      delta_is_zero_for_each_block_encoded);
}

TEST(ParquetDeltaDecoder, DataIsNotAlignedWithBlock32) {
  DeltaCoderTest<int32_t>::expect_decoded_data(data_is_not_aligned_with_block_plain,
      data_is_not_aligned_with_block_encoded);
}

TEST(ParquetDeltaDecoder, MaxMinValue32) {
  DeltaCoderTest<int32_t>::expect_decoded_data(max_min_value_plain,
      max_min_value_encoded);
}

TEST(ParquetDeltaDecoder, BlockAligned64) {
  DeltaCoderTest<int64_t>::expect_decoded_data(block_aligned_64_bits_plain,
      block_aligned_64_bits_encoded);
}

TEST(ParquetDeltaDecoder, OneByOne32) {
  DeltaCoderTest<int32_t>::expect_decoded_data_one_by_one(block_aligned_plain,
      block_aligned_encoded);
}

TEST(ParquetDeltaDecoder, OneByOne64) {
  DeltaCoderTest<int64_t>::expect_decoded_data_one_by_one(block_aligned_64_bits_plain,
      block_aligned_64_bits_encoded);
}

void ConversionAndStride() {
  // The INT64 Parquet type is only used with the BIGINT Impala type, so no conversion is
  // possible.
  using INT_T = int32_t;

  const std::vector<INT_T>& plain = block_aligned_for_type<INT_T>::plain;
  const std::vector<uint8_t>& encoded = block_aligned_for_type<INT_T>::encoded;

  constexpr int extra_stride = 3;

  DeltaCoderTest<INT_T>::template expect_decoded_data_converted<int8_t>(plain, encoded,
      sizeof(int8_t));
  DeltaCoderTest<INT_T>::template expect_decoded_data_converted<int8_t>(plain, encoded,
      sizeof(int8_t) + extra_stride);

  DeltaCoderTest<INT_T>::template expect_decoded_data_converted<int16_t>(plain, encoded,
      sizeof(int16_t));
  DeltaCoderTest<INT_T>::template expect_decoded_data_converted<int16_t>(plain, encoded,
      sizeof(int16_t) + extra_stride);

  DeltaCoderTest<INT_T>::template expect_decoded_data_converted<int64_t>(plain, encoded,
      sizeof(int64_t));
  DeltaCoderTest<INT_T>::template expect_decoded_data_converted<int64_t>(plain, encoded,
      sizeof(int64_t) + extra_stride);
}

TEST(ParquetDeltaDecoder, ConversionAndStride) {
  ConversionAndStride();
}

/// Wrong block and miniblock sizes and bitwidths.
template <typename INT_T>
void ExpectPageInitFails(const std::vector<uint8_t>& encoded) {
  ParquetDeltaDecoder<INT_T> reader;
  Status page_init_success = reader.NewPage(encoded.data(), encoded.size());
  EXPECT_FALSE(page_init_success.ok());
}

template <typename INT_T>
void BlockSizeNotMultipleOf128() {
  std::vector<uint8_t> encoded = {0x8c, 0x01 /* 140 */, 0x04, 0x01, 0x04};
  ExpectPageInitFails<INT_T>(encoded);
}

TEST(ParquetDeltaDecoder, BlockSizeNotMultipleOf128_32) {
  BlockSizeNotMultipleOf128<int32_t>();
}

TEST(ParquetDeltaDecoder, BlockSizeNotMultipleOf128_64) {
  BlockSizeNotMultipleOf128<int64_t>();
}

template <typename INT_T>
void MiniblockSizeNotMultipleOf32() {
  std::vector<uint8_t> encoded = {0x80, 0x01, 0x08 /* miniblock size is 16 */,
                                  0x01, 0x04};
  ExpectPageInitFails<INT_T>(encoded);
}

TEST(ParquetDeltaDecoder, MiniblockSizeNotMultipleOf32_32) {
  MiniblockSizeNotMultipleOf32<int32_t>();
}

TEST(ParquetDeltaDecoder, MiniblockSizeNotMultipleOf32_64) {
  MiniblockSizeNotMultipleOf32<int64_t>();
}

template <typename INT_T>
void BlockSizeZero() {
  std::vector<uint8_t> encoded = {0x00, 0x08, 0x01, 0x04};
  ExpectPageInitFails<INT_T>(encoded);
}

TEST(ParquetDeltaDecoder, BlockSizeZero32) {
  BlockSizeZero<int32_t>();
}

TEST(ParquetDeltaDecoder, BlockSizeZero64) {
  BlockSizeZero<int64_t>();
}

template <typename INT_T>
void MiniblockBiggerThanBlock() {
  std::vector<uint8_t> encoded = {0x80, 0x01, 0x80, 0x02, 0x01, 0x04};
  ExpectPageInitFails<INT_T>(encoded);
}

TEST(ParquetDeltaDecoder, MiniblockBiggerThanBlock32) {
  MiniblockBiggerThanBlock<int32_t>();
}

TEST(ParquetDeltaDecoder, MiniblockBiggerThanBlock64) {
  MiniblockBiggerThanBlock<int64_t>();
}

template <typename INT_T>
void BitwidthTooBig() {
  std::vector<uint8_t> encoded = {0x80, 0x01, 0x04, 0x02, 0x04, /// Header.
    0x00, /// Min delta in block.
    0x41, 0x10, 0x10, 0x10, // Bitwidths --- 0x41 == 65 is too big.

    // Enough bytes for the values so that the buffer is not too short and the expected
    // error comes from the wrong bitwidth.
    0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33,
    0x33, 0x33, 0x33, 0x33, 0x33};

  ParquetDeltaDecoder<INT_T> reader;

  // The exact location where the reading fails is an implementation detail, but it should
  // happen at the latest when trying to read the first value with the wrong bitwidth,
  // which is the second value in total (the first value is in the header).

  Status page_init_success = reader.NewPage(encoded.data(), encoded.size());
  if (!page_init_success.ok()) return;

  INT_T value;

  // The first value is in the header.
  bool first_value_success = reader.NextValue(&value);
  if (!first_value_success) return;

  // This is the value that has reported bitwidth 65.
  bool second_value_success = reader.NextValue(&value);

  EXPECT_FALSE(second_value_success);
}

TEST(ParquetDeltaDecoder, BitwidthTooBig32) {
  BitwidthTooBig<int32_t>();
}

TEST(ParquetDeltaDecoder, BitwidthTooBig64) {
  BitwidthTooBig<int64_t>();
}

// Strides and batch sizes and skip.
template <typename INT_T>
void DifferentStrides() {
  const vector<INT_T>& plain = block_aligned_for_type<INT_T>::plain;

  const std::vector<uint8_t> encoded =
      DeltaCoderTest<INT_T>::encode_all(plain, 128, 32);

  for (int i = sizeof(INT_T) + 1; i < 10; i++) {
    const std::vector<uint8_t> decoded = DeltaCoderTest<INT_T>::decode_all(encoded, i);
    DeltaCoderTest<INT_T>::template expect_equal_with_stride<INT_T>(plain, decoded, i);
  }
}

TEST(ParquetDeltaDecoder, DifferentStrides32) {
  DifferentStrides<int32_t>();
}

TEST(ParquetDeltaDecoder, DifferentStrides64) {
  DifferentStrides<int64_t>();
}

template <typename INT_T>
void DifferentBatchSizes() {
  const std::vector<INT_T>& plain = block_aligned_for_type<INT_T>::plain;
  const std::vector<uint8_t> encoded =
      DeltaCoderTest<INT_T>::encode_all(plain, 128, 32);

  const std::vector<int> batch_sizes = {10, 14, 50, 25};

  ParquetDeltaDecoder<INT_T> reader;
  Status page_init_success = reader.NewPage(encoded.data(), encoded.size());
  EXPECT_OK(page_init_success);

  const std::size_t total_value_count = reader.GetTotalValueCount();
  std::size_t read_values = 0;

  std::vector<INT_T> decoded(total_value_count);
  std::size_t batch_index = 0;
  while (read_values < total_value_count) {
    int batch_size = batch_sizes[batch_index];
    int values_read_now = reader.NextValues(batch_size,
        (uint8_t*) &decoded[read_values], sizeof(INT_T));
    ASSERT_GT(values_read_now, 0);
    ASSERT_LE(values_read_now, batch_size);

    read_values += values_read_now;
    batch_index = (batch_index + 1) % batch_sizes.size();

    EXPECT_TRUE(values_read_now == batch_size || read_values == total_value_count);
  }

  EXPECT_EQ(plain, decoded);
}

TEST(ParquetDeltaDecoder, DifferentBatchSizes32) {
  DifferentBatchSizes<int32_t>();
}

TEST(ParquetDeltaDecoder, DifferentBatchSizes64) {
  DifferentBatchSizes<int64_t>();
}

/// We alternate skipping and writing the number of values that is the next element in the
/// skip pattern. The remaining values in the input are written.
template <typename INT_T>
std::vector<INT_T> EraseSkipPattern(std::vector<INT_T> plain,
    const std::vector<std::size_t>& skip_pattern) {
  DCHECK_LE(std::accumulate(skip_pattern.begin(), skip_pattern.end(), 0), plain.size())
      << "Input vector not long enough for pattern.";
  std::size_t write_count = 0;
  for (std::size_t i = 0; i < skip_pattern.size(); i++) {
    if (i % 2 == 0) {
      /// Skipping.
      const std::size_t skip = skip_pattern[i];
      plain.erase(plain.begin() + write_count, plain.begin() + write_count + skip);
    } else {
      write_count += skip_pattern[i];
    }
  }

  return plain;
}

template <typename INT_T>
void Skip() {
  std::vector<std::vector<std::size_t>> skip_patterns = {
    {12, 100, 50},
    {50, 54, 125, 14, 282}
  };

  for (int miniblock_size : {32, 64, 96}) {
    constexpr int MINIBLOCKS_IN_BLOCK = 4;
    const int block_size = miniblock_size * MINIBLOCKS_IN_BLOCK;

    std::vector<INT_T> plain = block_aligned_for_type<INT_T>::plain;
    const std::vector<uint8_t> encoded = DeltaCoderTest<INT_T>::encode_all(plain,
        block_size, miniblock_size);

    const int stride = sizeof(INT_T);

    for (const std::vector<std::size_t>& skip_pattern : skip_patterns) {
      ParquetDeltaDecoder<INT_T> decoder;
      Status page_init_success = decoder.NewPage(encoded.data(), encoded.size());
      EXPECT_OK(page_init_success);

      const std::vector<INT_T> expected = EraseSkipPattern(plain, skip_pattern);

      std::vector<INT_T> output(expected.size());
      uint8_t* const output_data_ptr = reinterpret_cast<uint8_t* const>(output.data());

      std::size_t write_count = 0;
      for (std::size_t i = 0; i < skip_pattern.size(); i++) {
        if (i % 2 == 0) {
          /// Skipping.
          const std::size_t to_skip = skip_pattern[i];
          const int skipped = decoder.SkipValues(to_skip);
          EXPECT_EQ(to_skip, skipped);
        } else {
          const std::size_t to_write = skip_pattern[i];
          const int written = decoder.NextValues(to_write,
              output_data_ptr + write_count * stride, stride);
          EXPECT_EQ(to_write, written);

          write_count += to_write;
        }
      }

      const std::size_t values_left = expected.size() - write_count;
      const int written = decoder.NextValues(values_left,
          output_data_ptr + write_count * stride, stride);
      EXPECT_EQ(values_left, written);

      EXPECT_EQ(expected.size(), write_count + written);
      EXPECT_EQ(expected, output);
    }
  }
}

TEST(ParquetDeltaDecoder, Skip32) {
  Skip<int32_t>();
}

TEST(ParquetDeltaDecoder, Skip64) {
  Skip<int64_t>();
}

/// Round-trip tests.
template <typename INT_T>
void expect_code_and_decode(const std::vector<INT_T>& data,
    int block_size, int miniblock_size) {
  const std::vector<uint8_t> encoded = DeltaCoderTest<INT_T>::encode_all(data,
      block_size, miniblock_size);
  const std::vector<uint8_t> decoded_bytes = DeltaCoderTest<INT_T>::decode_all(encoded,
      sizeof(INT_T));
  std::vector<INT_T> decoded(decoded_bytes.size() / sizeof(INT_T));
  memcpy(decoded.data(), decoded_bytes.data(), decoded_bytes.size());

  EXPECT_EQ(data.size(), decoded.size());
  EXPECT_EQ(data, decoded) << "Block size: " << block_size << ". Miniblock size: "
      << miniblock_size << ".";
}

template <typename INT_T>
void DifferentBlockAndMiniblockSizes() {
  for (std::size_t i = 1; i < 10; i++) {
    const std::size_t block_size = i * 128;
    for (std::size_t j = 1; j * 32 <= block_size; j++) {
      const std::size_t miniblock_size = j * 32;
      if (block_size % miniblock_size == 0) {
        const vector<INT_T>& plain = block_aligned_for_type<INT_T>::plain;
        expect_code_and_decode<INT_T>(plain, block_size, miniblock_size);
      }
    }
  }
}

TEST(ParquetDeltaCoder, DifferentBlockAndMiniblockSizes32) {
  DifferentBlockAndMiniblockSizes<int32_t>();
}

TEST(ParquetDeltaCoder, DifferentBlockAndMiniblockSizes64) {
  DifferentBlockAndMiniblockSizes<int64_t>();
}

int ChooseRandomMiniblockSize(std::mt19937& gen, int block_size) {
  /// Choose the valid miniblock sizes.
  std::vector<int> valid_sizes;

  for (int v = 32; v <= block_size; v += 32) {
    if (block_size % v == 0) valid_sizes.push_back(v);
  }

  /// Inclusive range.
  std::uniform_int_distribution<std::size_t> dist(0, valid_sizes.size() - 1);

  const std::size_t index = dist(gen);
  return valid_sizes[index];
}

/// Returns a uniform distribution within a range of logarithmically scaled width.
template <typename INT_T>
std::uniform_int_distribution<INT_T> GetValueDistribution(std::mt19937& gen) {
  std::uniform_int_distribution<int> dist(0, sizeof(INT_T) - 3);
  int scale = dist(gen);
  INT_T lower = 1u << scale;
  INT_T upper = 1u << (scale + 1);

  std::bernoulli_distribution b(0.5);
  const bool change_sign = b(gen);

  if (change_sign) {
    int32_t tmp = upper;
    upper = -lower;
    lower = -tmp;
  }

  return std::uniform_int_distribution<INT_T>(lower, upper);
}

template <typename INT_T>
void random_test(std::mt19937& gen, double p_change_range) {
  std::uniform_int_distribution<std::size_t> block_size_dist(1, 4);
  int block_size = 128 * block_size_dist(gen);
  int miniblock_size = ChooseRandomMiniblockSize(gen, block_size);

  std::uniform_int_distribution<std::size_t> num_values_dist(100, 1000);
  const std::size_t num_values = num_values_dist(gen);

  std::bernoulli_distribution change_range_dist(p_change_range);

  std::uniform_int_distribution<INT_T> values_dist = GetValueDistribution<INT_T>(gen);

  std::vector<INT_T> values(num_values);
  for (std::size_t i = 0; i < num_values; i++) {
    values[i] = values_dist(gen);

    if (change_range_dist(gen)) {
      values_dist = GetValueDistribution<INT_T>(gen);
    }
  }

  const std::vector<uint8_t> encoded = DeltaCoderTest<INT_T>::encode_all(values,
      block_size, miniblock_size);
  const std::vector<uint8_t> decoded_bytes = DeltaCoderTest<INT_T>::decode_all(encoded,
      sizeof(INT_T));
  std::vector<INT_T> decoded(decoded_bytes.size() / sizeof(INT_T));
  memcpy(decoded.data(), decoded_bytes.data(), decoded_bytes.size());

  EXPECT_EQ(values, decoded);
}

template <typename INT_T>
void RandomTests() {
  std::mt19937 gen;
  RandTestUtil::SeedRng("PARQUET_DELTA_CODER_TEST_RANDOM_SEED", &gen);

  for (int i = 0; i < 20; i++) {
    random_test<INT_T>(gen, 0.05);
  }
}

TEST(ParquetDeltaCoder, RandomTests32) {
  RandomTests<int32_t>();
}

TEST(ParquetDeltaCoder, RandomTests64) {
  RandomTests<int64_t>();
}

}
