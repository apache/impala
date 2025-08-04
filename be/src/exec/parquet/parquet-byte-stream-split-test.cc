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

#include <vector>

#include "gtest/gtest.h"
#include "parquet-byte-stream-split-coder-test-data.h"
#include "parquet-byte-stream-split-decoder.h"
#include "parquet-byte-stream-split-encoder.h"

namespace impala {

namespace {
void checkAllBytes(uint8_t* buffer, int buffer_len, uint8_t check_against) {
  for (int i = 0; i < buffer_len; i++) {
    EXPECT_EQ(check_against, *(buffer + i));
  }
}

template <size_t T_SIZE>
ParquetByteStreamSplitDecoder<T_SIZE> createDecoder(int byte_size) {
  if constexpr (T_SIZE != 0) {
    EXPECT_EQ(byte_size, T_SIZE);
    return ParquetByteStreamSplitDecoder<T_SIZE>();
  } else {
    return ParquetByteStreamSplitDecoder<0>(byte_size);
  }
}

template <size_t T_SIZE>
ParquetByteStreamSplitEncoder<T_SIZE> createEncoder(int byte_size) {
  if constexpr (T_SIZE != 0) {
    EXPECT_EQ(byte_size, T_SIZE);
    return ParquetByteStreamSplitEncoder<T_SIZE>();
  } else {
    return ParquetByteStreamSplitEncoder<0>(byte_size);
  }
}

} // anonymous namespace

// ---------------------------------- DECODER TESTS ----------------------------------- //

class ParquetByteStreamSplitDecoderTest : public testing::Test {
 protected:
  ParquetByteStreamSplitDecoderTest() {}

  template <typename T>
  static void initialization_test(const std::vector<uint8_t>& input) {
    constexpr size_t byte_size = sizeof(T);
    ParquetByteStreamSplitDecoder<byte_size> decoder;
    const int max_value_count = input.size() / byte_size;

    T single_output = 0;
    std::vector<uint8_t> vec_output(max_value_count * byte_size);

    // As the decoder hasn't been initialized yet, there should be 0 values.
    EXPECT_EQ(0, decoder.GetTotalValueCount());

    // As the decoder hasn't been correctly initialized yet, there should be 0 values.
    EXPECT_EQ(0, decoder.GetTotalValueCount());

    // We should be able to initialize even with 0 values.
    decoder.NewPage(input.data(), 0);

    // But shouldn't be able to read from it.
    EXPECT_EQ(0, decoder.NextValue(&single_output));
    EXPECT_EQ(0, decoder.NextValues(1, vec_output.data(), byte_size));

    // And it should still have 0 values.
    EXPECT_EQ(0, decoder.GetTotalValueCount());

    // This is a correct call.
    // It also tests that after reassigning the decoder, it's updated correctly.
    decoder.NewPage(input.data(), byte_size * max_value_count);

    // The decoder has finally been correctly initialized.
    EXPECT_EQ(max_value_count, decoder.GetTotalValueCount());
  }

  template <typename T>
  static void decode_singles(
      const std::vector<uint8_t>& input, const std::vector<uint8_t>& expected) {
    constexpr size_t byte_size = sizeof(T);
    ParquetByteStreamSplitDecoder<byte_size> decoder;
    decoder.NewPage(input.data(), input.size());
    const int max_value_count = decoder.GetTotalValueCount();

    T single_output = 0;
    std::vector<T> expected_numerals(expected.size() / byte_size);
    memcpy(expected_numerals.data(), expected.data(), expected.size());

    for (int i = 0; i < max_value_count; i++) {
      EXPECT_EQ(1, decoder.NextValue(&single_output));
      EXPECT_EQ(expected_numerals[i], single_output);
    }

    // trying to overread
    EXPECT_EQ(0, decoder.NextValue(&single_output));
  }

  template <size_t BYTE_SIZE>
  static void decode_batch(const std::vector<uint8_t>& input,
      const std::vector<uint8_t>& expected, const size_t runtime_byte_size) {
    // The tests require at least 3 values in the input.
    // This is to make sure that the overread tests are valid.
    ASSERT_GE(input.size(), 3);

    ParquetByteStreamSplitDecoder<BYTE_SIZE> decoder =
        createDecoder<BYTE_SIZE>(runtime_byte_size);
    decoder.NewPage(input.data(), input.size());
    const int max_value_count = decoder.GetTotalValueCount();

    std::vector<uint8_t> output(0, 0);
    vector<uint8_t> expected_subset;

    EXPECT_EQ(0, decoder.NextValues(0, output.data(), runtime_byte_size));

    // Read one value, then the remaining.
    output.assign(max_value_count * runtime_byte_size, 0);
    expected_subset.assign(expected.begin(), expected.begin() + runtime_byte_size);
    expected_subset.resize(expected.size(), 0);
    EXPECT_EQ(1, decoder.NextValues(1, output.data(), runtime_byte_size));
    EXPECT_EQ(expected_subset, output);

    output.assign(max_value_count * runtime_byte_size, 0);
    expected_subset.assign(expected.begin() + 1 * runtime_byte_size, expected.end());
    expected_subset.resize(expected.size(), 0);
    EXPECT_EQ(max_value_count - 1, // because we read 1 value already
        decoder.NextValues(max_value_count - 1, output.data(), runtime_byte_size));
    EXPECT_EQ(expected_subset, output);

    // The page is depleted, we need to reset it.
    decoder.NewPage(input.data(), input.size());

    // Read two values, then overread the remaining.
    output.assign(max_value_count * runtime_byte_size, 0);
    expected_subset.assign(expected.begin(), expected.begin() + 2 * runtime_byte_size);
    expected_subset.resize(expected.size(), 0);
    EXPECT_EQ(2, decoder.NextValues(2, output.data(), runtime_byte_size));
    EXPECT_EQ(expected_subset, output);

    output.assign(max_value_count * runtime_byte_size, 0);
    expected_subset.assign(expected.begin() + 2 * runtime_byte_size, expected.end());
    expected_subset.resize(expected.size(), 0);
    EXPECT_EQ(max_value_count - 2, // because we read 2 values already
        decoder.NextValues(max_value_count, output.data(), runtime_byte_size));
    EXPECT_EQ(expected_subset, output);

    // The page is depleted, we need to reset it.
    decoder.NewPage(input.data(), input.size());

    // Getting all values in page in one go.
    output.assign(max_value_count * runtime_byte_size, 0);
    EXPECT_EQ(max_value_count,
        decoder.NextValues(max_value_count, output.data(), runtime_byte_size));
    EXPECT_EQ(expected, output);

    // Decoder page is depleted, try to overread.
    output.assign(max_value_count * runtime_byte_size, 0);
    EXPECT_EQ(0, decoder.NextValues(max_value_count, output.data(), runtime_byte_size));

    // The page is depleted, we need to reset it.
    decoder.NewPage(input.data(), input.size());

    // Try to overread the entire page.
    output.assign(max_value_count * runtime_byte_size, 0);
    EXPECT_EQ(max_value_count,
        decoder.NextValues(max_value_count + 1, output.data(), runtime_byte_size));
    EXPECT_EQ(expected, output);
  }

  template <typename T>
  static void decode_combined(
      const std::vector<uint8_t>& input, const std::vector<uint8_t>& expected) {
    // Decode the first value with `NextValue()` then the rest with `NextValues()`

    constexpr size_t byte_size = sizeof(T);
    ParquetByteStreamSplitDecoder<byte_size> decoder;
    decoder.NewPage(input.data(), input.size());
    const int max_value_count = decoder.GetTotalValueCount();

    std::vector<uint8_t> output(max_value_count * byte_size, 0);

    // Read the first value into output.
    EXPECT_EQ(1, decoder.NextValue(reinterpret_cast<T*>(output.data())));
    EXPECT_EQ(expected[0], output[0]);

    // Read the rest of the values into output.
    EXPECT_EQ(max_value_count - 1,
        decoder.NextValues(max_value_count - 1, output.data() + byte_size, byte_size));
    EXPECT_EQ(expected, output);

    // Decode all values but the last with `NextValues()` then the last with `NextValue().

    // The page is depleted, we need to reset it.
    decoder.NewPage(input.data(), input.size());

    // Read all values but the last into output.
    EXPECT_EQ(max_value_count - 1,
        decoder.NextValues(max_value_count - 1, output.data(), byte_size));
    EXPECT_EQ(1,
        decoder.NextValue(
            reinterpret_cast<T*>(output.data() + (max_value_count - 1) * byte_size)));
    EXPECT_EQ(expected, output);
  }

  template <typename T>
  static void decode_with_stride(const std::vector<uint8_t>& input,
      const std::vector<uint8_t>& expected, const size_t stride) {
    constexpr size_t byte_size = sizeof(T);
    ParquetByteStreamSplitDecoder<byte_size> decoder;
    decoder.NewPage(input.data(), input.size());
    const int max_value_count = decoder.GetTotalValueCount();

    std::vector<uint8_t> output(max_value_count * stride, 0);

    // Read the all values into output.
    EXPECT_EQ(
        max_value_count, decoder.NextValues(max_value_count, output.data(), stride));

    // Check that each value read is correct
    for (int i = 0; i < max_value_count * byte_size; i++) {
      EXPECT_EQ(expected[i], output[i / byte_size * stride + i % byte_size]);
    }

    // Check that the "skipped" bytes are unchanged
    for (int i = byte_size; i < max_value_count; i += stride) {
      checkAllBytes(&output[i], stride - byte_size, 0);
    }
  }

  template <typename T>
  static void skip(
      const std::vector<uint8_t>& input, const std::vector<uint8_t>& expected) {
    constexpr size_t byte_size = sizeof(T);
    ParquetByteStreamSplitDecoder<byte_size> decoder;
    decoder.NewPage(input.data(), input.size());
    const int max_value_count = decoder.GetTotalValueCount();

    std::vector<uint8_t> output(max_value_count * byte_size, 0);
    std::vector<uint8_t> expected_subset;

    EXPECT_EQ(2, decoder.SkipValues(2));

    output.assign(max_value_count * byte_size, 0);
    expected_subset.assign(expected.begin() + 2 * byte_size, expected.end());
    expected_subset.resize(expected.size(), 0);
    EXPECT_EQ(max_value_count - 2, // because we skipped 2 values already
        decoder.NextValues(max_value_count - 2, output.data(), byte_size));
    EXPECT_EQ(expected_subset, output);

    // Reset page, then try to skip more values than there are
    decoder.NewPage(input.data(), input.size());
    EXPECT_EQ(
        expected.size() / byte_size, decoder.SkipValues(expected.size() / byte_size + 1));
  }
};

// -------------------- Basic Functionality Tests --------------------- //

TEST_F(ParquetByteStreamSplitDecoderTest, FloatBaseFuncTest) {
  initialization_test<float>(float_encoded_200v);
};

TEST_F(ParquetByteStreamSplitDecoderTest, Int32BaseFuncTest) {
  initialization_test<int32_t>(int_encoded_200v);
};

TEST_F(ParquetByteStreamSplitDecoderTest, DoubleBaseFuncTest) {
  initialization_test<double>(double_encoded_200v);
};

TEST_F(ParquetByteStreamSplitDecoderTest, Int64BaseFuncTest) {
  initialization_test<int64_t>(long_encoded_200v);
};

// -------------------- Basic Single Values Tests --------------------- //

TEST_F(ParquetByteStreamSplitDecoderTest, FloatDecodeSinglesTest) {
  decode_singles<float>(float_encoded_200v, float_decoded_200v);
}

TEST_F(ParquetByteStreamSplitDecoderTest, Int32DecodeSinglesTest) {
  decode_singles<int32_t>(int_encoded_200v, int_decoded_200v);
}

TEST_F(ParquetByteStreamSplitDecoderTest, DoubleDecodeSinglesTest) {
  decode_singles<double>(double_encoded_200v, double_decoded_200v);
}

TEST_F(ParquetByteStreamSplitDecoderTest, Int64DecodeSinglesTest) {
  decode_singles<int64_t>(long_encoded_200v, long_decoded_200v);
}

// --------------------- Decoding In Batch Tests ---------------------- //

TEST_F(ParquetByteStreamSplitDecoderTest, FloatDecodeBatchTest) {
  decode_batch<sizeof(float)>(float_encoded_200v, float_decoded_200v, sizeof(float));
  decode_batch<0>(float_encoded_200v, float_decoded_200v, sizeof(float));
}

TEST_F(ParquetByteStreamSplitDecoderTest, Int32DecodeBatchTest) {
  decode_batch<sizeof(int32_t)>(int_encoded_200v, int_decoded_200v, sizeof(int32_t));
  decode_batch<0>(int_encoded_200v, int_decoded_200v, sizeof(int32_t));
}

TEST_F(ParquetByteStreamSplitDecoderTest, DoubleDecodeBatchTest) {
  decode_batch<sizeof(double)>(double_encoded_200v, double_decoded_200v, sizeof(double));
  decode_batch<0>(double_encoded_200v, double_decoded_200v, sizeof(double));
}

TEST_F(ParquetByteStreamSplitDecoderTest, Int64DecodeBatchTest) {
  decode_batch<sizeof(int64_t)>(long_encoded_200v, long_decoded_200v, sizeof(int64_t));
  decode_batch<0>(long_encoded_200v, long_decoded_200v, sizeof(int64_t));
}

TEST_F(ParquetByteStreamSplitDecoderTest, VaryingSizeDecodeBatchTest) {
  decode_batch<0>(fix3b_encoded_200v, fix3b_decoded_200v, 3);
  decode_batch<0>(fix5b_encoded_200v, fix5b_decoded_200v, 5);
  decode_batch<0>(fix7b_encoded_200v, fix7b_decoded_200v, 7);
  decode_batch<0>(fix11b_encoded_200v, fix11b_decoded_200v, 11);
}

// --------------------- Decoding Combined Tests ---------------------- //

TEST_F(ParquetByteStreamSplitDecoderTest, FloatDecodeCombinedTest) {
  decode_combined<float>(float_encoded_200v, float_decoded_200v);
}

TEST_F(ParquetByteStreamSplitDecoderTest, Int32DecodeCombinedTest) {
  decode_combined<int32_t>(int_encoded_200v, int_decoded_200v);
}

TEST_F(ParquetByteStreamSplitDecoderTest, DoubleDecodeCombinedTest) {
  decode_combined<double>(double_encoded_200v, double_decoded_200v);
}

TEST_F(ParquetByteStreamSplitDecoderTest, Int64DecodeCombinedTest) {
  decode_combined<int64_t>(long_encoded_200v, long_decoded_200v);
}

// -------------------- Decoding With Stride Tests -------------------- //

TEST_F(ParquetByteStreamSplitDecoderTest, FloatDecodeStrideTest) {
  decode_with_stride<float>(
      float_encoded_200v, float_decoded_200v, 2 * sizeof(float) + 2);
}

TEST_F(ParquetByteStreamSplitDecoderTest, Int32DecodeStrideTest) {
  decode_with_stride<int32_t>(
      int_encoded_200v, int_decoded_200v, 2 * sizeof(int32_t) + 2);
}

TEST_F(ParquetByteStreamSplitDecoderTest, DoubleDecodeStrideTest) {
  decode_with_stride<double>(
      double_encoded_200v, double_decoded_200v, 2 * sizeof(double) + 2);
}

TEST_F(ParquetByteStreamSplitDecoderTest, Int64DecodeStrideTest) {
  decode_with_stride<int64_t>(
      long_encoded_200v, long_decoded_200v, 2 * sizeof(int64_t) + 2);
}

//--------------------Skip Tests--------------------//

TEST_F(ParquetByteStreamSplitDecoderTest, FloatSkipTest) {
  skip<float>(float_encoded_200v, float_decoded_200v);
}

TEST_F(ParquetByteStreamSplitDecoderTest, Int32SkipTest) {
  skip<int32_t>(int_encoded_200v, int_decoded_200v);
}

TEST_F(ParquetByteStreamSplitDecoderTest, DoubleSkipTest) {
  skip<double>(double_encoded_200v, double_decoded_200v);
}

TEST_F(ParquetByteStreamSplitDecoderTest, Int64SkipTest) {
  skip<int64_t>(long_encoded_200v, long_decoded_200v);
}

// ---------------------------------- ENCODER TESTS ----------------------------------- //

class ParquetByteStreamSplitEncoderTest : public testing::Test {
 protected:
  ParquetByteStreamSplitEncoderTest() {}

  template <typename T>
  static void initialization_test(const std::vector<T>& values) {
    constexpr size_t byte_size = sizeof(T);
    ParquetByteStreamSplitEncoder<byte_size> encoder;
    const int max_value_count = values.size();
    std::vector<uint8_t> input(byte_size * max_value_count);
    std::vector<uint8_t> output(byte_size * max_value_count);

    // should encode 0 values if the buffer size is 0
    encoder.NewPage(input.data(), 0);
    EXPECT_EQ(0, encoder.FinalizePage(output.data(), 0));

    // should encode 0 values if there was none put in
    encoder.NewPage(input.data(), input.size());
    EXPECT_EQ(0, encoder.FinalizePage(output.data(), output.size()));

    // should encode as many values as successfully put in
    encoder.NewPage(input.data(), input.size());
    EXPECT_TRUE(encoder.Put(values[0]));
    EXPECT_TRUE(encoder.Put(values[0]));
    EXPECT_TRUE(encoder.Put(values[0]));
    EXPECT_EQ(3, encoder.FinalizePage(output.data(), output.size()));

    // everything should be reset, so this should be fine
    encoder.NewPage(input.data(), input.size());
    EXPECT_TRUE(encoder.Put(values[0]));
    EXPECT_EQ(1, encoder.FinalizePage(output.data(), output.size()));
  }

  template <typename T>
  static void put_and_finalize(
      const std::vector<T>& values, const std::vector<uint8_t>& expected) {
    constexpr size_t byte_size = sizeof(T);
    ParquetByteStreamSplitEncoder<byte_size> encoder;
    std::vector<uint8_t> input(byte_size * values.size());
    std::vector<uint8_t> output(byte_size * values.size());

    // put just one value, finalize and check
    encoder.NewPage(input.data(), input.size());
    EXPECT_TRUE(encoder.Put(values[0]));
    EXPECT_EQ(1, encoder.FinalizePage(output.data(), output.size()));
    // since we're only decoding one value, the output should be the same as the input
    EXPECT_EQ(0, memcmp(values.data(), output.data(), sizeof(T)));

    // put 2 values (more than 1, but not filling up the buffer), finalize and check
    encoder.NewPage(input.data(), input.size());
    if (values.size() >= 2) {
      for (int i = 0; i < 2; i++) {
        EXPECT_TRUE(encoder.Put(values[i]));
      }

      // this is needed, because the expected vector has the values scattered
      // here we gather the bytes of the first 2 values
      std::vector<uint8_t> expected_subset;
      for (int i = 0; i < sizeof(T); i++) {
        expected_subset.push_back(expected[i * values.size()]);
        expected_subset.push_back(expected[i * values.size() + 1]);
      }

      EXPECT_EQ(2, encoder.FinalizePage(output.data(), output.size()));
      EXPECT_EQ(0, memcmp(expected_subset.data(), output.data(), sizeof(T) * 2));
    }

    // put all, finalize and check
    encoder.NewPage(input.data(), input.size());
    for (int i = 0; i < values.size(); i++) {
      EXPECT_TRUE(encoder.Put(values[i]));
    }

    // try to put more than there is space for
    EXPECT_FALSE(encoder.Put(values[0]));
    EXPECT_EQ(values.size(), encoder.FinalizePage(output.data(), output.size()));

    EXPECT_EQ(expected, output);
  }

  template <typename T>
  static void prepopulated_init_test(
      const std::vector<T>& values, const std::vector<uint8_t>& expected) {
    constexpr size_t byte_size = sizeof(T);
    ParquetByteStreamSplitEncoder<byte_size> encoder;
    std::vector<uint8_t> input(byte_size * values.size());
    std::vector<uint8_t> output(byte_size * values.size());

    memcpy(input.data(), values.data(), byte_size * values.size());

    encoder.NewPage(input.data(), input.size(), values.size());
    EXPECT_EQ(values.size(), encoder.FinalizePage(output.data(), output.size()));
    EXPECT_EQ(0, memcmp(expected.data(), output.data(), byte_size * values.size()));
  }
};

// -------------------- Basic Functionality Tests --------------------- //

TEST_F(ParquetByteStreamSplitEncoderTest, FloatBasicFuncTest) {
  initialization_test(float_values_200v);
}

TEST_F(ParquetByteStreamSplitEncoderTest, Int32BasicFuncTest) {
  initialization_test(int_values_200v);
}

TEST_F(ParquetByteStreamSplitEncoderTest, DoubleBasicFuncTest) {
  initialization_test(double_values_200v);
}

TEST_F(ParquetByteStreamSplitEncoderTest, Int64BasicFuncTest) {
  initialization_test(long_values_200v);
}

// ---------------------- Put and Finalize Tests ---------------------- //

TEST_F(ParquetByteStreamSplitEncoderTest, FloatPutFinalizeTest) {
  put_and_finalize(float_values_200v, float_encoded_200v);
}

TEST_F(ParquetByteStreamSplitEncoderTest, Int32PutFinalizeTest) {
  put_and_finalize(int_values_200v, int_encoded_200v);
}

TEST_F(ParquetByteStreamSplitEncoderTest, DoublePutFinalizeTest) {
  put_and_finalize(double_values_200v, double_encoded_200v);
}

TEST_F(ParquetByteStreamSplitEncoderTest, Int64PutFinalizeTest) {
  put_and_finalize(long_values_200v, long_encoded_200v);
}

// ------------------------ Prepopulated Tests ------------------------ //

TEST_F(ParquetByteStreamSplitEncoderTest, FloatPrepopulatedTest) {
  prepopulated_init_test(float_values_200v, float_encoded_200v);
}

TEST_F(ParquetByteStreamSplitEncoderTest, Int32PrepopulatedTest) {
  prepopulated_init_test(int_values_200v, int_encoded_200v);
}

TEST_F(ParquetByteStreamSplitEncoderTest, DoublePrepopulatedTest) {
  prepopulated_init_test(double_values_200v, double_encoded_200v);
}

TEST_F(ParquetByteStreamSplitEncoderTest, Int64PrepopulatedTest) {
  prepopulated_init_test(long_values_200v, long_encoded_200v);
}

// --------------------------- TWO-DIRECTIONAL CODER TESTS ---------------------------- //

class ParquetByteStreamSplitCoderTest : public testing::Test {
 protected:
  ParquetByteStreamSplitCoderTest() {}

  template <typename T>
  static void encode_then_decode(const std::vector<T>& input, const int size) {
    constexpr size_t byte_size = sizeof(T);

    ParquetByteStreamSplitEncoder<byte_size> encoder;
    std::vector<uint8_t> encoder_buff(byte_size * size);
    std::vector<uint8_t> encoded(byte_size * size);
    encoder.NewPage(encoder_buff.data(), encoder_buff.size());
    for (int i = 0; i < size; i++) {
      EXPECT_TRUE(encoder.Put(input[i]));
    }

    EXPECT_EQ(size, encoder.FinalizePage(encoded.data(), encoded.size()));

    ParquetByteStreamSplitDecoder<byte_size> decoder;
    std::vector<T> result(size);
    decoder.NewPage(encoded.data(), encoded.size());
    for (int i = 0; i < size; i++) {
      EXPECT_EQ(1, decoder.NextValue(&result[i]));
    }

    EXPECT_EQ(0, memcmp(input.data(), result.data(), size * byte_size));
  }

  template <int BYTE_SIZE>
  static void encode_then_decode_in_batch(const std::vector<uint8_t>& input,
      const int test_size, const size_t runtime_byte_size) {
    ParquetByteStreamSplitEncoder<BYTE_SIZE> encoder =
        createEncoder<BYTE_SIZE>(runtime_byte_size);
    std::vector<uint8_t> temp(runtime_byte_size * test_size);
    std::vector<uint8_t> encoded(runtime_byte_size * test_size);
    encoder.NewPage(temp.data(), temp.size());
    for (int i = 0; i < test_size; i++) {
      EXPECT_TRUE(encoder.PutBytes(&input[i * runtime_byte_size]));
    }
    EXPECT_EQ(test_size, encoder.FinalizePage(encoded.data(), encoded.size()));

    ParquetByteStreamSplitDecoder<BYTE_SIZE> decoder =
        createDecoder<BYTE_SIZE>(runtime_byte_size);
    std::vector<uint8_t> result(runtime_byte_size * test_size);
    decoder.NewPage(encoded.data(), encoded.size());
    EXPECT_EQ(test_size, decoder.NextValues(test_size, result.data(), runtime_byte_size));

    EXPECT_EQ(0, memcmp(input.data(), result.data(), test_size * runtime_byte_size));
  }

  template <int BYTE_SIZE>
  static void encode_then_decode_with_stride(const std::vector<uint8_t>& input,
      const int stride, const int test_size, const size_t runtime_byte_size) {
    ParquetByteStreamSplitEncoder<BYTE_SIZE> encoder =
        createEncoder<BYTE_SIZE>(runtime_byte_size);
    std::vector<uint8_t> temp(runtime_byte_size * test_size);
    std::vector<uint8_t> encoded(runtime_byte_size * test_size);
    encoder.NewPage(temp.data(), temp.size());
    for (int i = 0; i < test_size; i++) {
      EXPECT_TRUE(encoder.PutBytes(&input[i * runtime_byte_size]));
    }
    EXPECT_EQ(test_size, encoder.FinalizePage(encoded.data(), encoded.size()));

    ParquetByteStreamSplitDecoder<BYTE_SIZE> decoder =
        createDecoder<BYTE_SIZE>(runtime_byte_size);
    std::vector<uint8_t> result(test_size * stride);
    decoder.NewPage(encoded.data(), encoded.size());
    EXPECT_EQ(test_size, decoder.NextValues(test_size, result.data(), stride));

    for (int i = 0; i < test_size; i++) {
      EXPECT_EQ(0,
          memcmp(&input[i * runtime_byte_size], &result[i * stride], runtime_byte_size));
      checkAllBytes(
          &result[i * stride + runtime_byte_size], stride - runtime_byte_size, 0);
    }
  }

  template <typename T>
  static void decode_then_encode(const std::vector<uint8_t>& input, const int size) {
    constexpr size_t byte_size = sizeof(T);
    ParquetByteStreamSplitDecoder<byte_size> decoder;
    decoder.NewPage(input.data(), size * byte_size);

    std::vector<T> decoded(size);

    for (int i = 0; i < size; i++) {
      EXPECT_EQ(1, decoder.NextValue(&decoded[i]));
    }

    ParquetByteStreamSplitEncoder<byte_size> encoder;
    std::vector<uint8_t> temp(byte_size * size);
    std::vector<uint8_t> result(byte_size * size);
    encoder.NewPage(temp.data(), temp.size());
    for (int i = 0; i < size; i++) {
      EXPECT_TRUE(encoder.Put(decoded[i]));
    }

    EXPECT_EQ(size, encoder.FinalizePage(result.data(), result.size()));
    EXPECT_EQ(0, memcmp(input.data(), result.data(), size * byte_size));
  }

  template <int BYTE_SIZE>
  static void decode_in_batch_then_encode(const std::vector<uint8_t>& input,
      const int test_size, const size_t runtime_byte_size) {
    ParquetByteStreamSplitDecoder<BYTE_SIZE> decoder =
        createDecoder<BYTE_SIZE>(runtime_byte_size);

    int num_input_values = input.size() / runtime_byte_size;
    decoder.NewPage(input.data(), num_input_values * runtime_byte_size);
    std::vector<uint8_t> decoded(test_size * runtime_byte_size);

    EXPECT_EQ(
        test_size, decoder.NextValues(test_size, decoded.data(), runtime_byte_size));

    ParquetByteStreamSplitEncoder<BYTE_SIZE> encoder =
        createEncoder<BYTE_SIZE>(runtime_byte_size);
    std::vector<uint8_t> temp(runtime_byte_size * test_size);
    std::vector<uint8_t> result(runtime_byte_size * test_size);
    encoder.NewPage(temp.data(), temp.size());
    for (int i = 0; i < test_size; i++) {
      EXPECT_TRUE(encoder.PutBytes(&decoded[i * runtime_byte_size]));
    }

    EXPECT_EQ(test_size, encoder.FinalizePage(result.data(), result.size()));

    // The `input` has all values encoded, while `result` has only `test_size`
    // values. This causes `input` to have more values than `result`, so we need to
    // compare them in chunks of `test_size`. For example if input had 10 vales, and
    // `test_size` was 5: input  = a1 b1 c1 d1 e1 f1 g1 h1 i1 j1 a2 b2 c2 d2 e2 f2 g2 h2
    // i2 j2 ... result = a1 b1 c1 d1 e1 a2 b2 c2 d2 e2 a3 b3 c3 d3 e3 a4 b4 c4 d4 e4 ...
    for (int i = 0; i < runtime_byte_size; i++) {
      EXPECT_EQ(
          0, memcmp(&input[i * num_input_values], &result[i * test_size], test_size));
    }
  }

  template <int BYTE_SIZE>
  static void decode_with_stride_then_encode(const std::vector<uint8_t>& input,
      const int stride, const int test_size, const size_t runtime_byte_size) {
    ParquetByteStreamSplitDecoder<BYTE_SIZE> decoder =
        createDecoder<BYTE_SIZE>(runtime_byte_size);

    int num_input_values = input.size() / runtime_byte_size;
    decoder.NewPage (input.data(), num_input_values * runtime_byte_size);
    std::vector<uint8_t> decoded(test_size * stride);

    EXPECT_EQ(test_size, decoder.NextValues(test_size, decoded.data(), stride));

    ParquetByteStreamSplitEncoder<BYTE_SIZE> encoder =
        createEncoder<BYTE_SIZE>(runtime_byte_size);
    std::vector<uint8_t> temp(runtime_byte_size * test_size);
    std::vector<uint8_t> result(runtime_byte_size * test_size);
    encoder.NewPage(temp.data(), temp.size());
    for (int i = 0; i < test_size; i++) {
      EXPECT_TRUE(encoder.PutBytes(&decoded[i * stride]));
    }

    EXPECT_EQ(test_size, encoder.FinalizePage(result.data(), result.size()));
    // The `input` has all values encoded, while `result` has only `test_size`
    // values. This causes `input` to have more values than `result`, so we need to
    // compare them in chunks of `test_size`. For example if `input` had 10 vales, and
    // `test_size` was 5:
    //  input = a1 b1 c1 d1 e1 f1 g1 h1 i1 j1|a2 b2 c2 d2 e2 f2 g2 h2 i2 j2 ...
    // result = a1 b1 c1 d1 e1|a2 b2 c2 d2 e2|a3 b3 c3 d3 e3|a4 b4 c4 d4 e4 ...
    for (int i = 0; i < runtime_byte_size; i++) {
      EXPECT_EQ(
          0, memcmp(&input[i * num_input_values], &result[i * test_size], test_size));
    }
  }
};

//--------------------Encode -> Decode Tests--------------------//

TEST_F(ParquetByteStreamSplitCoderTest, FloatEncodeDecodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    encode_then_decode(float_values_200v, test_size);
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    encode_then_decode(float_values_200v, test_size);
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, Int32EncodeDecodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    encode_then_decode(int_values_200v, test_size);
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    encode_then_decode(int_values_200v, test_size);
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, DoubleEncodeDecodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    encode_then_decode(double_values_200v, test_size);
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    encode_then_decode(double_values_200v, test_size);
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, Int64EncodeDecodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    encode_then_decode(long_values_200v, test_size);
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    encode_then_decode(long_values_200v, test_size);
  }
}

//--------------------Encode -> Decode In Batch Tests--------------------//

TEST_F(ParquetByteStreamSplitCoderTest, FloatEncodeDecodeBatchTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    encode_then_decode_in_batch<sizeof(float)>(
        float_decoded_200v, test_size, sizeof(float));
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    encode_then_decode_in_batch<sizeof(float)>(
        float_decoded_200v, test_size, sizeof(float));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, Int32EncodeDecodeBatchTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    encode_then_decode_in_batch<sizeof(int32_t)>(
        int_decoded_200v, test_size, sizeof(int32_t));
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    encode_then_decode_in_batch<sizeof(int32_t)>(
        int_decoded_200v, test_size, sizeof(int32_t));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, DoubleEncodeDecodeBatchTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    encode_then_decode_in_batch<sizeof(double)>(
        double_decoded_200v, test_size, sizeof(double));
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    encode_then_decode_in_batch<sizeof(double)>(
        double_decoded_200v, test_size, sizeof(double));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, Int64EncodeDecodeBatchTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    encode_then_decode_in_batch<sizeof(int64_t)>(
        long_decoded_200v, test_size, sizeof(int64_t));
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    encode_then_decode_in_batch<sizeof(int64_t)>(
        long_decoded_200v, test_size, sizeof(int64_t));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, VaryingSizeEncodeDecodeBatchTest) {
  for (int b_size = 1; b_size <= 10; b_size++) {
    vector<uint8_t> float_decoded = float_decoded_200v;
    vector<uint8_t> int_decoded = int_decoded_200v;
    vector<uint8_t> double_decoded = double_decoded_200v;
    vector<uint8_t> long_decoded = long_decoded_200v;

    // With `vec.size() / b_size`, we get the maximum number of values that can be read
    // from the encoded vector with the given byte size.
    int value_count4b = float_decoded_200v.size() / b_size;
    int value_count8b = double_decoded_200v.size() / b_size;

    float_decoded.resize(value_count4b * b_size);
    int_decoded.resize(value_count4b * b_size);
    double_decoded.resize(value_count8b * b_size);
    long_decoded.resize(value_count8b * b_size);

    for (int test_size = 10; test_size <= 20; test_size++) {
      encode_then_decode_in_batch<0>(float_decoded, test_size, b_size);
      encode_then_decode_in_batch<0>(int_decoded, test_size, b_size);
      encode_then_decode_in_batch<0>(double_decoded, test_size, b_size);
      encode_then_decode_in_batch<0>(long_decoded, test_size, b_size);
    }
    for (int test_size = value_count4b - 10; test_size <= value_count4b; test_size++) {
      encode_then_decode_in_batch<0>(float_decoded, test_size, b_size);
      encode_then_decode_in_batch<0>(int_decoded, test_size, b_size);
    }
    for (int test_size = value_count8b - 10; test_size <= value_count8b; test_size++) {
      encode_then_decode_in_batch<0>(double_decoded, test_size, b_size);
      encode_then_decode_in_batch<0>(long_decoded, test_size, b_size);
    }
  }
}

//--------------------Encode -> Decode With Stride Tests--------------------//

TEST_F(ParquetByteStreamSplitCoderTest, FloatEncodeDecodeStrideTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    encode_then_decode_with_stride<sizeof(float)>(
        float_decoded_200v, sizeof(float) + 1, test_size, sizeof(float));
    encode_then_decode_with_stride<sizeof(float)>(
        float_decoded_200v, sizeof(float) + 2, test_size, sizeof(float));
    encode_then_decode_with_stride<sizeof(float)>(
        float_decoded_200v, sizeof(float) + 3, test_size, sizeof(float));
    encode_then_decode_with_stride<sizeof(float)>(
        float_decoded_200v, sizeof(float) * 3, test_size, sizeof(float));
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    encode_then_decode_with_stride<sizeof(float)>(
        float_decoded_200v, sizeof(float) + 1, test_size, sizeof(float));
    encode_then_decode_with_stride<sizeof(float)>(
        float_decoded_200v, sizeof(float) + 2, test_size, sizeof(float));
    encode_then_decode_with_stride<sizeof(float)>(
        float_decoded_200v, sizeof(float) + 3, test_size, sizeof(float));
    encode_then_decode_with_stride<sizeof(float)>(
        float_decoded_200v, sizeof(float) * 3, test_size, sizeof(float));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, Int32EncodeDecodeStrideTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    encode_then_decode_with_stride<sizeof(int32_t)>(
        int_decoded_200v, sizeof(int32_t) + 1, test_size, sizeof(int32_t));
    encode_then_decode_with_stride<sizeof(int32_t)>(
        int_decoded_200v, sizeof(int32_t) + 2, test_size, sizeof(int32_t));
    encode_then_decode_with_stride<sizeof(int32_t)>(
        int_decoded_200v, sizeof(int32_t) + 3, test_size, sizeof(int32_t));
    encode_then_decode_with_stride<sizeof(int32_t)>(
        int_decoded_200v, sizeof(int32_t) * 3, test_size, sizeof(int32_t));
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    encode_then_decode_with_stride<sizeof(int32_t)>(
        int_decoded_200v, sizeof(int32_t) + 1, test_size, sizeof(int32_t));
    encode_then_decode_with_stride<sizeof(int32_t)>(
        int_decoded_200v, sizeof(int32_t) + 2, test_size, sizeof(int32_t));
    encode_then_decode_with_stride<sizeof(int32_t)>(
        int_decoded_200v, sizeof(int32_t) + 3, test_size, sizeof(int32_t));
    encode_then_decode_with_stride<sizeof(int32_t)>(
        int_decoded_200v, sizeof(int32_t) * 3, test_size, sizeof(int32_t));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, DoubleEncodeDecodeStrideTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    encode_then_decode_with_stride<sizeof(double)>(
        double_decoded_200v, sizeof(double) + 1, test_size, sizeof(double));
    encode_then_decode_with_stride<sizeof(double)>(
        double_decoded_200v, sizeof(double) + 2, test_size, sizeof(double));
    encode_then_decode_with_stride<sizeof(double)>(
        double_decoded_200v, sizeof(double) + 3, test_size, sizeof(double));
    encode_then_decode_with_stride<sizeof(double)>(
        double_decoded_200v, sizeof(double) * 3, test_size, sizeof(double));
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    encode_then_decode_with_stride<sizeof(double)>(
        double_decoded_200v, sizeof(double) + 1, test_size, sizeof(double));
    encode_then_decode_with_stride<sizeof(double)>(
        double_decoded_200v, sizeof(double) + 2, test_size, sizeof(double));
    encode_then_decode_with_stride<sizeof(double)>(
        double_decoded_200v, sizeof(double) + 3, test_size, sizeof(double));
    encode_then_decode_with_stride<sizeof(double)>(
        double_decoded_200v, sizeof(double) * 3, test_size, sizeof(double));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, Int64EncodeDecodeStrideTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    encode_then_decode_with_stride<sizeof(int64)>(
        long_decoded_200v, sizeof(int64_t) + 1, test_size, sizeof(int64_t));
    encode_then_decode_with_stride<sizeof(int64)>(
        long_decoded_200v, sizeof(int64_t) + 2, test_size, sizeof(int64_t));
    encode_then_decode_with_stride<sizeof(int64)>(
        long_decoded_200v, sizeof(int64_t) + 3, test_size, sizeof(int64_t));
    encode_then_decode_with_stride<sizeof(int64)>(
        long_decoded_200v, sizeof(int64_t) * 3, test_size, sizeof(int64_t));
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    encode_then_decode_with_stride<sizeof(int64)>(
        long_decoded_200v, sizeof(int64_t) + 1, test_size, sizeof(int64_t));
    encode_then_decode_with_stride<sizeof(int64)>(
        long_decoded_200v, sizeof(int64_t) + 2, test_size, sizeof(int64_t));
    encode_then_decode_with_stride<sizeof(int64)>(
        long_decoded_200v, sizeof(int64_t) + 3, test_size, sizeof(int64_t));
    encode_then_decode_with_stride<sizeof(int64)>(
        long_decoded_200v, sizeof(int64_t) * 3, test_size, sizeof(int64_t));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, VaryingSizeEncodeDecodeStrideTest) {
  for (int b_size = 1; b_size <= 10; b_size++) {
    vector<uint8_t> float_decoded = float_decoded_200v;
    vector<uint8_t> int_decoded = int_decoded_200v;
    vector<uint8_t> double_decoded = double_decoded_200v;
    vector<uint8_t> long_decoded = long_decoded_200v;

    // With `count / b_size`, we get the maximum number of values that can be read from
    // the encoded vector with the given byte size.
    int value_count4b = float_decoded_200v.size() / b_size;
    int value_count8b = double_decoded_200v.size() / b_size;

    float_decoded.resize(value_count4b * b_size);
    int_decoded.resize(value_count4b * b_size);
    double_decoded.resize(value_count8b * b_size);
    long_decoded.resize(value_count8b * b_size);

    for (int test_size = 10; test_size <= 20; test_size++) {
      encode_then_decode_with_stride<0>(float_decoded, b_size + 1, test_size, b_size);
      encode_then_decode_with_stride<0>(int_decoded, b_size + 2, test_size, b_size);
      encode_then_decode_with_stride<0>(double_decoded, b_size + 3, test_size, b_size);
      encode_then_decode_with_stride<0>(long_decoded, b_size * 3, test_size, b_size);
    }
    for (int test_size = value_count4b - 10; test_size <= value_count4b; test_size++) {
      encode_then_decode_with_stride<0>(float_decoded, b_size + 1, test_size, b_size);
      encode_then_decode_with_stride<0>(int_decoded, b_size + 2, test_size, b_size);
    }
    for (int test_size = value_count8b - 10; test_size <= value_count8b; test_size++) {
      encode_then_decode_with_stride<0>(double_decoded, b_size + 3, test_size, b_size);
      encode_then_decode_with_stride<0>(long_decoded, b_size * 3, test_size, b_size);
    }
  }
}

//--------------------Decode -> Encode Tests--------------------//

TEST_F(ParquetByteStreamSplitCoderTest, FloatDecodeEncodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    decode_then_encode<float>(float_encoded_200v, test_size);
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    decode_then_encode<float>(float_encoded_200v, test_size);
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, Int32DecodeEncodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    decode_then_encode<int32_t>(int_encoded_200v, test_size);
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    decode_then_encode<int32_t>(int_encoded_200v, test_size);
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, DoubleDecodeEncodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    decode_then_encode<double>(double_encoded_200v, test_size);
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    decode_then_encode<double>(double_encoded_200v, test_size);
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, Int64DecodeEncodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    decode_then_encode<int64_t>(long_encoded_200v, test_size);
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    decode_then_encode<int64_t>(long_encoded_200v, test_size);
  }
}

//--------------------Decode In Batch -> Encode Tests--------------------//

TEST_F(ParquetByteStreamSplitCoderTest, FloatDecodeBatchEncodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    decode_in_batch_then_encode<sizeof(float)>(
        float_encoded_200v, test_size, sizeof(float));
  }
  for (int test_size = 10; test_size <= 20; test_size++) {
    decode_in_batch_then_encode<sizeof(float)>(
        float_encoded_200v, test_size, sizeof(float));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, Int32DecodeBatchEncodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    decode_in_batch_then_encode<sizeof(int32_t)>(
        int_encoded_200v, test_size, sizeof(int32_t));
  }
  for (int test_size = 10; test_size <= 20; test_size++) {
    decode_in_batch_then_encode<sizeof(int32_t)>(
        int_encoded_200v, test_size, sizeof(int32_t));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, DoubleDecodeBatchEncodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    decode_in_batch_then_encode<sizeof(double)>(
        double_encoded_200v, test_size, sizeof(double));
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    decode_in_batch_then_encode<sizeof(double)>(
        double_encoded_200v, test_size, sizeof(double));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, Int64DecodeBatchEncodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    decode_in_batch_then_encode<sizeof(int64_t)>(
        long_encoded_200v, test_size, sizeof(int64_t));
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    decode_in_batch_then_encode<sizeof(int64_t)>(
        long_encoded_200v, test_size, sizeof(int64_t));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, VaryingSizeDecodeBatchEncodeTest) {
  for (int b_size = 1; b_size <= 10; b_size++) {
    vector<uint8_t> float_encoded = float_encoded_200v;
    vector<uint8_t> int_encoded = int_encoded_200v;
    vector<uint8_t> double_encoded = double_encoded_200v;
    vector<uint8_t> long_encoded = long_encoded_200v;

    // With `count / b_size`, we get the maximum number of values that can be read from
    // the encoded vector with the given byte size.
    int value_count4b = float_encoded_200v.size() / b_size;
    int value_count8b = float_encoded_200v.size() / b_size;

    float_encoded.resize(value_count4b * b_size);
    int_encoded.resize(value_count4b * b_size);
    double_encoded.resize(value_count8b * b_size);
    long_encoded.resize(value_count8b * b_size);

    for (int test_size = 10; test_size <= 20; test_size++) {
      decode_in_batch_then_encode<0>(float_encoded_200v, test_size, b_size);
      decode_in_batch_then_encode<0>(int_encoded_200v, test_size, b_size);
      decode_in_batch_then_encode<0>(double_encoded_200v, test_size, b_size);
      decode_in_batch_then_encode<0>(long_encoded_200v, test_size, b_size);
    }
    for (int test_size = value_count4b - 10; test_size <= value_count4b; test_size++) {
      decode_in_batch_then_encode<0>(float_encoded_200v, test_size, b_size);
      decode_in_batch_then_encode<0>(int_encoded_200v, test_size, b_size);
    }
    for (int test_size = value_count8b - 10; test_size <= value_count8b; test_size++) {
      decode_in_batch_then_encode<0>(double_encoded_200v, test_size, b_size);
      decode_in_batch_then_encode<0>(long_encoded_200v, test_size, b_size);
    }
  }
}

//--------------------Decode With Stride -> Encode Tests--------------------//

TEST_F(ParquetByteStreamSplitCoderTest, FloatDecodeStrideEncodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    decode_with_stride_then_encode<sizeof(float)>(
        float_encoded_200v, 2 * sizeof(float) + 2, test_size, sizeof(float));
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    decode_with_stride_then_encode<sizeof(float)>(
        float_encoded_200v, 2 * sizeof(float) + 2, test_size, sizeof(float));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, Int32DecodeStrideEncodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    decode_with_stride_then_encode<sizeof(int32_t)>(
        int_encoded_200v, 2 * sizeof(int32_t) + 2, test_size, sizeof(int32_t));
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    decode_with_stride_then_encode<sizeof(int32_t)>(
        int_encoded_200v, 2 * sizeof(int32_t) + 2, test_size, sizeof(int32_t));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, DoubleDecodeStrideEncodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    decode_with_stride_then_encode<sizeof(double)>(
        double_encoded_200v, 2 * sizeof(double) + 2, test_size, sizeof(double));
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    decode_with_stride_then_encode<sizeof(double)>(
        double_encoded_200v, 2 * sizeof(double) + 2, test_size, sizeof(double));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, Int64DecodeStrideEncodeTest) {
  for (int test_size = 10; test_size <= 20; test_size++) {
    decode_with_stride_then_encode<sizeof(int64_t)>(
        double_encoded_200v, 2 * sizeof(int64_t) + 2, test_size, sizeof(int64_t));
  }
  for (int test_size = 190; test_size <= 200; test_size++) {
    decode_with_stride_then_encode<sizeof(int64_t)>(
        double_encoded_200v, 2 * sizeof(int64_t) + 2, test_size, sizeof(int64_t));
  }
}

TEST_F(ParquetByteStreamSplitCoderTest, VaryingSizeDecodeStrideEncodeTest) {
  for (int b_size = 1; b_size <= 10; b_size++) {
    vector<uint8_t> float_encoded = float_encoded_200v;
    vector<uint8_t> int_encoded = int_encoded_200v;
    vector<uint8_t> double_encoded = double_encoded_200v;
    vector<uint8_t> long_encoded = long_encoded_200v;

    // With `count / b_size`, we get the maximum number of values that can be read from
    // the encoded vector with the given byte size.
    int value_count4b = float_encoded_200v.size() / b_size;
    int value_count8b = double_encoded_200v.size() / b_size;

    float_encoded.resize(value_count4b * b_size);
    int_encoded.resize(value_count4b * b_size);
    double_encoded.resize(value_count8b * b_size);
    long_encoded.resize(value_count8b * b_size);

    for (int test_size = 10; test_size <= 20; test_size++) {
      decode_with_stride_then_encode<0>(float_encoded, b_size + 1, test_size, b_size);
      decode_with_stride_then_encode<0>(int_encoded, b_size + 2, test_size, b_size);
      decode_with_stride_then_encode<0>(double_encoded, b_size + 3, test_size, b_size);
      decode_with_stride_then_encode<0>(long_encoded, b_size * 3, test_size, b_size);
    }
    for (int test_size = value_count4b - 10; test_size <= value_count4b; test_size++) {
      decode_with_stride_then_encode<0>(float_encoded, b_size + 1, test_size, b_size);
      decode_with_stride_then_encode<0>(int_encoded, b_size + 2, test_size, b_size);
    }
    for (int test_size = value_count8b - 10; test_size <= value_count8b; test_size++) {
      decode_with_stride_then_encode<0>(double_encoded, b_size + 3, test_size, b_size);
      decode_with_stride_then_encode<0>(long_encoded, b_size * 3, test_size, b_size);
    }
  }
}

} // namespace impala