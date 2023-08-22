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

#include <utility>

#include "runtime/mem-tracker.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "testutil/gtest-util.h"
#include "testutil/rand-util.h"
#include "util/dict-encoding.h"
#include "util/encoding-test-util.h"

#include "common/names.h"

namespace impala {

// Helper function to validate that 'decoder' decodes the expected values.
// If 'skip_at' and 'skip_count' aren't zero, then the value skipping logic
// is also exercised.
template<typename InternalType>
void ValidateValues(DictDecoder<InternalType>& decoder,
const vector<InternalType>& values, int skip_at, int skip_count, bool skip_success) {
  for (int i = 0; i < skip_at; ++i) {
    InternalType j;
    ASSERT_TRUE(decoder.GetNextValue(&j));
    EXPECT_EQ(values[i], j) << i;
  }
  EXPECT_EQ(decoder.SkipValues(skip_count), skip_success);
  for (int i = skip_at + skip_count; i < values.size(); ++i) {
    InternalType j;
    ASSERT_TRUE(decoder.GetNextValue(&j));
    EXPECT_EQ(values[i], j) << i;
  }
}

template<typename InternalType, parquet::Type::type PARQUET_TYPE>
void ValidateDict(const vector<InternalType>& values,
    const vector<InternalType>& dict_values, int fixed_buffer_byte_size,
    int skip_at = 0, int skip_count = 0, bool skip_success = true) {
  set<InternalType> values_set(values.begin(), values.end());

  int bytes_alloc = 0;
  MemTracker track_encoder;
  MemTracker tracker;
  MemPool pool(&tracker);
  DictEncoder<InternalType> encoder(&pool, fixed_buffer_byte_size, &track_encoder);
  encoder.UsedbyTest();
  for (const InternalType& i: values) encoder.Put(i);
  bytes_alloc = encoder.DictByteSize();
  EXPECT_EQ(track_encoder.consumption(), bytes_alloc);
  EXPECT_EQ(encoder.num_entries(), values_set.size());

  uint8_t dict_buffer[encoder.dict_encoded_size()];
  encoder.WriteDict(dict_buffer);

  int data_buffer_len = encoder.EstimatedDataEncodedSize() * 2;
  uint8_t data_buffer[data_buffer_len];
  int data_len = encoder.WriteData(data_buffer, data_buffer_len);
  EXPECT_GT(data_len, 0);
  encoder.ClearIndices();

  MemTracker decode_tracker;
  DictDecoder<InternalType> decoder(&decode_tracker);
  ASSERT_TRUE(decoder.template Reset<PARQUET_TYPE>(dict_buffer,
      encoder.dict_encoded_size(),fixed_buffer_byte_size));
  bytes_alloc = decoder.DictByteSize();
  EXPECT_EQ(decode_tracker.consumption(), bytes_alloc);

  // Test direct access to the dictionary via indexes
  for (int i = 0; i < dict_values.size(); ++i) {
    InternalType expected_value = dict_values[i];
    InternalType out_value;

    decoder.GetValue(i, &out_value);
    EXPECT_EQ(expected_value, out_value);
  }
  // Test access to dictionary via internal stream
  ASSERT_OK(decoder.SetData(data_buffer, data_len));
  ValidateValues(decoder, values, skip_at, skip_count, skip_success);
  pool.FreeAll();
}

TEST(DictTest, TestStrings) {
  StringValue sv1("hello world");
  StringValue sv2("foo");
  StringValue sv3("bar");
  StringValue sv4("baz");

  vector<StringValue> dict_values;
  dict_values.push_back(sv1);
  dict_values.push_back(sv2);
  dict_values.push_back(sv3);
  dict_values.push_back(sv4);

  vector<StringValue> values;
  values.push_back(sv1);
  values.push_back(sv1);
  values.push_back(sv1);
  values.push_back(sv2);
  values.push_back(sv1);
  values.push_back(sv2);
  values.push_back(sv2);
  values.push_back(sv3);
  values.push_back(sv3);
  values.push_back(sv3);
  values.push_back(sv4);

  ValidateDict<StringValue, parquet::Type::BYTE_ARRAY>(values, dict_values, -1);
}

TEST(DictTest, TestTimestamps) {
  TimestampValue tv1 = TimestampValue::ParseSimpleDateFormat("2011-01-01 09:01:01");
  TimestampValue tv2 = TimestampValue::ParseSimpleDateFormat("2012-01-01 09:01:01");
  TimestampValue tv3 = TimestampValue::ParseSimpleDateFormat("2011-01-01 09:01:02");

  vector<TimestampValue> dict_values;
  dict_values.push_back(tv1);
  dict_values.push_back(tv2);
  dict_values.push_back(tv3);

  vector<TimestampValue> values;
  values.push_back(tv1);
  values.push_back(tv2);
  values.push_back(tv3);
  values.push_back(tv1);
  values.push_back(tv1);
  values.push_back(tv1);

  ValidateDict<TimestampValue, parquet::Type::INT96>(values, dict_values,
      ParquetPlainEncoder::EncodedByteSize(ColumnType(TYPE_TIMESTAMP)));
}

template<typename InternalType>
void IncrementValue(InternalType* t) { ++(*t); }

template <class T> void IncrementValue(DecimalValue<T>* t) {
  const T value = t->value();
  t->set_value(value + 1);
}

template<typename InternalType, parquet::Type::type PARQUET_TYPE>
void TestNumbers(int max_value, int repeat, int value_byte_size) {
  vector<InternalType> values;
  vector<InternalType> dict_values;
  for (InternalType val = 0; val < max_value; IncrementValue(&val)) {
    for (int i = 0; i < repeat; ++i) {
      values.push_back(val);
    }
    dict_values.push_back(val);
  }

  ValidateDict<InternalType, PARQUET_TYPE>(values, dict_values, value_byte_size);
}

template<typename InternalType, parquet::Type::type PARQUET_TYPE>
void TestNumbers(int value_byte_size) {
  TestNumbers<InternalType, PARQUET_TYPE>(100, 1, value_byte_size);
  TestNumbers<InternalType, PARQUET_TYPE>(1, 100, value_byte_size);
  TestNumbers<InternalType, PARQUET_TYPE>(1, 1, value_byte_size);
  TestNumbers<InternalType, PARQUET_TYPE>(1, 2, value_byte_size);
}

TEST(DictTest, TestNumbers) {
  TestNumbers<int8_t, parquet::Type::INT32>(
      ParquetPlainEncoder::EncodedByteSize(ColumnType(TYPE_TINYINT)));
  TestNumbers<int16_t, parquet::Type::INT32>(
      ParquetPlainEncoder::EncodedByteSize(ColumnType(TYPE_SMALLINT)));
  TestNumbers<int32_t, parquet::Type::INT32>(
      ParquetPlainEncoder::EncodedByteSize(ColumnType(TYPE_INT)));
  TestNumbers<int64_t, parquet::Type::INT64>(
      ParquetPlainEncoder::EncodedByteSize(ColumnType(TYPE_BIGINT)));
  TestNumbers<float, parquet::Type::FLOAT>(
      ParquetPlainEncoder::EncodedByteSize(ColumnType(TYPE_FLOAT)));
  TestNumbers<double, parquet::Type::DOUBLE>(
      ParquetPlainEncoder::EncodedByteSize(ColumnType(TYPE_DOUBLE)));

  for (int i = 1; i <= 16; ++i) {
    if (i <= 4) TestNumbers<Decimal4Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(i);
    if (i <= 8) TestNumbers<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(i);
    TestNumbers<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(i);
  }
}

TEST(DictTest, TestInvalidStrings) {
  uint8_t buffer[sizeof(int32_t) + 10];
  int32_t len = -10;
  memcpy(buffer, &len, sizeof(int32_t));

  // Test a dictionary with a string encoded with negative length. Initializing
  // the decoder should fail.
  MemTracker tracker;
  DictDecoder<StringValue> decoder(&tracker);
  ASSERT_FALSE(decoder.template Reset<parquet::Type::BYTE_ARRAY>(buffer, sizeof(buffer),
      0));
}

TEST(DictTest, TestStringBufferOverrun) {
  // Test string length past end of buffer.
  uint8_t buffer[sizeof(int32_t) + 10];
  int32_t len = 100;
  memcpy(buffer, &len, sizeof(int32_t));

  // Initializing the dictionary should fail, since the string would reference
  // invalid memory.
  MemTracker tracker;
  DictDecoder<StringValue> decoder(&tracker);
  ASSERT_FALSE(decoder.template Reset<parquet::Type::BYTE_ARRAY>(buffer, sizeof(buffer),
      0));
}

// Make sure that SetData() resets the dictionary decoder, including the embedded RLE
// decoder to a clean state, even if the input is not fully consumed. The RLE decoder
// has various state that needs to be reset.
TEST(DictTest, SetDataAfterPartialRead) {
  int bytes_alloc = 0;
  MemTracker tracker;
  MemTracker track_encoder;
  MemTracker track_decoder;
  MemPool pool(&tracker);
  DictEncoder<int> encoder(&pool, sizeof(int), &track_encoder);
  encoder.UsedbyTest();

  // Literal run followed by a repeated run.
  vector<int> values{1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9};
  for (int val: values) encoder.Put(val);

  vector<uint8_t> dict_buffer(encoder.dict_encoded_size());
  encoder.WriteDict(dict_buffer.data());
  vector<uint8_t> data_buffer(encoder.EstimatedDataEncodedSize() * 2);
  int data_len = encoder.WriteData(data_buffer.data(), data_buffer.size());
  bytes_alloc = encoder.DictByteSize();
  EXPECT_EQ(track_encoder.consumption(), bytes_alloc);
  ASSERT_GT(data_len, 0);
  encoder.ClearIndices();

  DictDecoder<int> decoder(&track_decoder);
  ASSERT_TRUE(decoder.template Reset<parquet::Type::INT32>(
      dict_buffer.data(), dict_buffer.size(), sizeof(int)));

  // Test decoding some of the values, then resetting. If the decoder incorrectly
  // caches some values, this could produce incorrect results.
  for (int num_to_decode = 0; num_to_decode < values.size(); ++num_to_decode) {
    ASSERT_OK(decoder.SetData(data_buffer.data(), data_buffer.size()));
    for (int i = 0; i < num_to_decode; ++i) {
      int val;
      ASSERT_TRUE(decoder.GetNextValue(&val));
      EXPECT_EQ(values[i], val) << num_to_decode << " " << i;
    }
  }
  bytes_alloc = decoder.DictByteSize();
  EXPECT_EQ(track_decoder.consumption(), bytes_alloc);
}

// Make sure the decoder detects if the bit width is too high.
TEST(DictTest, SetDataInvalidBitwidthFails) {
  // The maximum bit width that bit packing can handle is higher but DictDecoder uses 32
  // bit integers to store the indices.
  const int high_bit_width = 33;
  uint8_t buffer[5] = {}; // Initialise the buffer.

  MemTracker tracker;
  DictDecoder<int> decoder(&tracker);

  // Accept valid bit widths.
  for (int i = 0; i < high_bit_width; i++) {
    buffer[0] = i;
    Status status = decoder.SetData(buffer, 5);
    EXPECT_OK(status);
  }

  // Reject too high bit widths.
  for (int i = high_bit_width; i < BatchedBitReader::MAX_BITWIDTH; i++) {
    buffer[0] = i;
    Status status = decoder.SetData(buffer, 5);
    EXPECT_FALSE(status.ok());
  }
}

// Test handling of decode errors from out-of-range values.
TEST(DictTest, DecodeErrors) {
  int bytes_alloc = 0;
  MemTracker tracker;
  MemTracker track_encoder;
  MemTracker track_decoder;
  MemPool pool(&tracker);
  DictEncoder<int> small_dict_encoder(&pool, sizeof(int), &track_encoder);
  small_dict_encoder.UsedbyTest();

  // Generate a dictionary with 9 values (requires 4 bits to encode).
  vector<int> small_dict_values{1, 2, 3, 4, 5, 6, 7, 8, 9};
  for (int val: small_dict_values) small_dict_encoder.Put(val);

  vector<uint8_t> small_dict_buffer(small_dict_encoder.dict_encoded_size());
  small_dict_encoder.WriteDict(small_dict_buffer.data());
  bytes_alloc = small_dict_encoder.DictByteSize();
  EXPECT_EQ(track_encoder.consumption(), bytes_alloc);
  small_dict_encoder.ClearIndices();

  DictDecoder<int> small_dict_decoder(&track_decoder);
  ASSERT_TRUE(small_dict_decoder.template Reset<parquet::Type::INT32>(
        small_dict_buffer.data(), small_dict_buffer.size(), sizeof(int)));

  // Generate dictionary-encoded data with between 9 and 15 distinct values to test that
  // error is detected when the decoder reads a 4-bit value that is out of range.
  using TestCase = pair<string, vector<int>>;
  vector<TestCase> test_cases{
    {"Out-of-range value in a repeated run",
        {10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10}},
    {"Out-of-range literal run in the last < 32 element batch",
      {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
    {"Out-of-range literal run in the middle of a 32 element batch",
      {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
       11, 12, 13, 14, 15, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
       1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}}};
  for (TestCase& test_case: test_cases) {
    // Encode the values. This will produce a dictionary with more distinct values than
    // the small dictionary that we'll use to decode it.
    DictEncoder<int> large_dict_encoder(&pool, sizeof(int), &track_encoder);
    large_dict_encoder.UsedbyTest();
    // Initialize the dictionary with the values already in the small dictionary.
    for (int val : small_dict_values) large_dict_encoder.Put(val);
    large_dict_encoder.Close();

    for (int val: test_case.second) large_dict_encoder.Put(val);

    vector<uint8_t> data_buffer(large_dict_encoder.EstimatedDataEncodedSize() * 2);
    int data_len = large_dict_encoder.WriteData(data_buffer.data(), data_buffer.size());
    ASSERT_GT(data_len, 0);
    bytes_alloc = large_dict_encoder.DictByteSize();
    EXPECT_EQ(track_encoder.consumption(), bytes_alloc);
    large_dict_encoder.Close();

    ASSERT_OK(small_dict_decoder.SetData(data_buffer.data(), data_buffer.size()));
    bool failed = false;
    for (int i = 0; i < test_case.second.size(); ++i) {
      int val;
      failed = !small_dict_decoder.GetNextValue(&val);
      if (failed) break;
    }
    bytes_alloc = small_dict_decoder.DictByteSize();
    EXPECT_EQ(track_decoder.consumption(), bytes_alloc);
    EXPECT_TRUE(failed) << "Should have detected out-of-range dict-encoded value in test "
        << test_case.first;
  }
}

TEST(DictTest, TestGetNextValuesAndSkippingFuzzy) {
  const int values_size = 8192;
  const int rounds = 100;
  MemTracker tracker;
  MemTracker track_encoder;
  MemTracker track_decoder;
  MemPool pool(&tracker);
  DictEncoder<int> large_dict_encoder(&pool, sizeof(int), &track_encoder);

  std::default_random_engine random_eng;
  RandTestUtil::SeedRng("DICT_TEST_SEED", &random_eng);

  // Generates random number between 'bottom' and 'top' (inclusive intervals).
  auto GetRandom = [&random_eng](int bottom, int top) {
    std::uniform_int_distribution<int> uni_dist(bottom, top);
    return uni_dist(random_eng);
  };

  vector<int> values = MakeRandomSequence(random_eng, values_size, 200, 10);
  for (int val : values) {
    large_dict_encoder.Put(val);
  }

  vector<uint8_t> data_buffer(large_dict_encoder.EstimatedDataEncodedSize() * 2);
  int data_len = large_dict_encoder.WriteData(data_buffer.data(), data_buffer.size());
  ASSERT_GT(data_len, 0);

  vector<uint8_t> dict_buffer(large_dict_encoder.dict_encoded_size());
  large_dict_encoder.WriteDict(dict_buffer.data());
  large_dict_encoder.Close();

  vector<int32_t> decoded_values(values.size());
  DictDecoder<int> decoder(&track_decoder);
  ASSERT_TRUE(decoder.template Reset<parquet::Type::INT32>(
      dict_buffer.data(), dict_buffer.size(), sizeof(int)));

  for (int round = 0; round < rounds; ++round) {
    ASSERT_OK(decoder.SetData(data_buffer.data(), data_buffer.size()));
    int i = 0;
    while (i < values.size()) {
      int length = GetRandom(1, 200);
      if (i + length > values.size()) length = values.size() - i;
      int skip_or_get = GetRandom(0, 1);
      if (skip_or_get == 0) {
        // skip values
        ASSERT_TRUE(decoder.SkipValues(length));
      } else {
        // decode values
        ASSERT_TRUE(decoder.GetNextValues(&decoded_values[i],
                sizeof(int32_t), length));
        for (int j = 0; j < length; ++j) {
          EXPECT_EQ(values[i+j], decoded_values[i+j]);
        }
      }
      i += length;
    }
  }
}

TEST(DictTest, TestSkippingValues) {
  auto ValidateSkipping = [](const vector<int32_t>& values,
      const vector<int32_t>& dict_values, int skip_at, int skip_count,
      bool skip_success = true) {
    const int value_byte_size = ParquetPlainEncoder::EncodedByteSize(
        ColumnType(TYPE_INT));
    ValidateDict<int32_t, parquet::Type::INT32>(values, dict_values, value_byte_size,
        skip_at, skip_count, skip_success);
  };

  vector<int32_t> literal_values;
  for (int i = 0; i < 200; ++i) literal_values.push_back(i);
  ValidateSkipping(literal_values, literal_values, 0, 4);
  ValidateSkipping(literal_values, literal_values, 0, 130);
  ValidateSkipping(literal_values, literal_values, 2, 4);
  ValidateSkipping(literal_values, literal_values, 4, 48);
  ValidateSkipping(literal_values, literal_values, 7, 130);
  // Skipping too many values should fail
  ValidateSkipping(literal_values, literal_values, 4, 300, false);

  vector<int32_t> repeated_values(200, 1000);
  ValidateSkipping(repeated_values, {1000}, 0, 4);
  ValidateSkipping(repeated_values, {1000}, 0, 49);
  ValidateSkipping(repeated_values, {1000}, 0, 145);
  ValidateSkipping(repeated_values, {1000}, 3, 4);
  ValidateSkipping(repeated_values, {1000}, 4, 49);
  ValidateSkipping(repeated_values, {1000}, 4, 150);
  // Skipping too many values should fail
  ValidateSkipping(repeated_values, {1000}, 4, 300, false);

  auto Concat = [](const vector<int32_t>& a, const vector<int32_t>& b) {
    vector<int32_t> ab(a);
    ab.insert(ab.end(), b.begin(), b.end());
    return ab;
  };
  vector<int32_t> literal_then_repeated = Concat(literal_values, repeated_values);
  vector<int32_t> literal_then_repeated_dict = Concat(literal_values, {1000});
  ValidateSkipping(literal_then_repeated, literal_then_repeated_dict, 0, 4);
  ValidateSkipping(literal_then_repeated, literal_then_repeated_dict, 0, 87);
  ValidateSkipping(literal_then_repeated, literal_then_repeated_dict, 0, 200);
  ValidateSkipping(literal_then_repeated, literal_then_repeated_dict, 0, 222);
  ValidateSkipping(literal_then_repeated, literal_then_repeated_dict, 4, 19);
  ValidateSkipping(literal_then_repeated, literal_then_repeated_dict, 4, 200);
  ValidateSkipping(literal_then_repeated, literal_then_repeated_dict, 200, 47);
  ValidateSkipping(literal_then_repeated, literal_then_repeated_dict, 234, 166);

  vector<int32_t> repeated_then_literal = Concat(repeated_values, literal_values);
  vector<int32_t> repeated_then_literal_dict = Concat({1000}, literal_values);
  ValidateSkipping(repeated_then_literal, repeated_then_literal_dict, 0, 4);
  ValidateSkipping(repeated_then_literal, repeated_then_literal_dict, 0, 89);
  ValidateSkipping(repeated_then_literal, repeated_then_literal_dict, 0, 200);
  ValidateSkipping(repeated_then_literal, repeated_then_literal_dict, 0, 232);
  ValidateSkipping(repeated_then_literal, repeated_then_literal_dict, 4, 8);
  ValidateSkipping(repeated_then_literal, repeated_then_literal_dict, 4, 88);
  ValidateSkipping(repeated_then_literal, repeated_then_literal_dict, 4, 288);
  ValidateSkipping(repeated_then_literal, repeated_then_literal_dict, 230, 11);
  ValidateSkipping(repeated_then_literal, repeated_then_literal_dict, 230, 79);
  ValidateSkipping(repeated_then_literal, repeated_then_literal_dict, 230, 170);
}

}

