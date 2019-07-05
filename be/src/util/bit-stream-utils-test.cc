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

#include "testutil/gtest-util.h"
#include "util/bit-packing.inline.h"
#include "util/bit-stream-utils.inline.h"

#include "common/names.h"

namespace impala {

const int MAX_WIDTH = BatchedBitReader::MAX_BITWIDTH;

TEST(BitArray, TestBool) {
  const int len = 8;
  uint8_t buffer[len];

  BitWriter writer(buffer, len);

  // Write alternating 0's and 1's
  for (int i = 0; i < 8; ++i) {
    bool result = writer.PutValue(static_cast<uint64_t>(i % 2), 1);
    EXPECT_TRUE(result);
  }
  writer.Flush();
  EXPECT_EQ((int)buffer[0], BOOST_BINARY(1 0 1 0 1 0 1 0));

  // Write 00110011
  for (int i = 0; i < 8; ++i) {
    bool result;
    switch (i) {
      case 0:
      case 1:
      case 4:
      case 5:
        result = writer.PutValue(0, 1);
        break;
      default:
        result = writer.PutValue(1, 1);
        break;
    }
    EXPECT_TRUE(result);
  }
  writer.Flush();

  // Validate the exact bit value
  EXPECT_EQ((int)buffer[0], BOOST_BINARY(1 0 1 0 1 0 1 0));
  EXPECT_EQ((int)buffer[1], BOOST_BINARY(1 1 0 0 1 1 0 0));

  // Use the reader and validate
  BatchedBitReader reader(buffer, len);

  // Ensure it returns the same results after Reset().
  for (int trial = 0; trial < 2; ++trial) {
    bool batch_vals[16];
    EXPECT_EQ(16, reader.UnpackBatch(1, 16, batch_vals));
    for (int i = 0; i < 8; ++i)  EXPECT_EQ(batch_vals[i], i % 2);

    for (int i = 0; i < 8; ++i) {
      switch (i) {
        case 0:
        case 1:
        case 4:
        case 5:
          EXPECT_EQ(batch_vals[8 + i], false);
          break;
        default:
          EXPECT_EQ(batch_vals[8 + i], true);
          break;
      }
    }
    reader.Reset(buffer, len);
  }
}

// Tests SkipBatch() by comparing the results of a reader with skipping
// against a reader that doesn't skip.
template <typename T>
void TestSkipping(const uint8_t* buffer, const int len, const int bit_width,
    const int skip_at, const int skip_count) {
  constexpr int MAX_LEN = 512;
  DCHECK_LE(len, MAX_LEN);
  DCHECK_EQ((skip_at * bit_width) % 8, 0);
  DCHECK_EQ((skip_count * bit_width) % 8, 0);
  int value_count = len * 8 / bit_width;

  T all_vals[MAX_LEN];
  BatchedBitReader expected_reader(buffer, len);
  expected_reader.UnpackBatch(bit_width, value_count, all_vals);

  BatchedBitReader skipping_reader(buffer, len);
  T vals[MAX_LEN];
  skipping_reader.UnpackBatch(bit_width, skip_at, vals);
  skipping_reader.SkipBatch(bit_width, skip_count);
  skipping_reader.UnpackBatch(bit_width, value_count - skip_count, vals + skip_at);

  for (int i = 0; i < skip_at; ++i) {
    EXPECT_EQ(all_vals[i], vals[i]);
  }
  for (int i = skip_at + skip_count; i < len; ++i) {
    EXPECT_EQ(all_vals[i], vals[i - skip_count]);
  }
}

TEST(BitArray, TestBoolSkip) {
  const int len = 4;
  uint8_t buffer[len];

  BitWriter writer(buffer, len);
  // Write 00000000 11111111 11111111 00000000
  for (int i = 0; i < 8; ++i) ASSERT_TRUE(writer.PutValue(0, 1));
  for (int i = 0; i < 16; ++i) ASSERT_TRUE(writer.PutValue(1, 1));
  for (int i = 0; i < 8; ++i) ASSERT_TRUE(writer.PutValue(0, 1));
  writer.Flush();

  TestSkipping<bool>(buffer, len, 1, 0, 8);
  TestSkipping<bool>(buffer, len, 1, 0, 16);
  TestSkipping<bool>(buffer, len, 1, 8, 8);
  TestSkipping<bool>(buffer, len, 1, 8, 16);
  TestSkipping<bool>(buffer, len, 1, 16, 8);
  TestSkipping<bool>(buffer, len, 1, 16, 16);
}

TEST(BitArray, TestIntSkip) {
  constexpr int len = 512;
  const int bit_width = 6;
  uint8_t buffer[len];

  BitWriter writer(buffer, len);
  for (int i = 0; i < (1 << bit_width); ++i) {
    ASSERT_TRUE(writer.PutValue(i, bit_width));
  }
  int bytes_written = writer.bytes_written();
  TestSkipping<int>(buffer, bytes_written, bit_width, 0, 4);
  TestSkipping<int>(buffer, bytes_written, bit_width, 4, 4);
  TestSkipping<int>(buffer, bytes_written, bit_width, 4, 8);
  TestSkipping<int>(buffer, bytes_written, bit_width, 8, 56);
}

// Writes 'num_vals' values with width 'bit_width' and reads them back.
void TestBitArrayValues(int bit_width, int num_vals) {
  const int len = BitUtil::Ceil(bit_width * num_vals, 8);
  const int64_t mod = bit_width == 64 ? 1 : 1LL << bit_width;

  uint8_t buffer[(len > 0) ? len : 1];
  BitWriter writer(buffer, len);
  for (int i = 0; i < num_vals; ++i) {
    bool result = writer.PutValue(i % mod, bit_width);
    EXPECT_TRUE(result);
  }
  writer.Flush();
  EXPECT_EQ(writer.bytes_written(), len);

  BatchedBitReader reader(buffer, len);
  BatchedBitReader reader2(reader); // Test copy constructor.
  // Ensure it returns the same results after Reset().
  for (int trial = 0; trial < 2; ++trial) {
    // Unpack all values at once with one batched reader and in small batches with the
    // other batched reader.
    vector<int64_t> batch_vals(num_vals);
    const int BATCH_SIZE = 32;
    vector<int64_t> batch_vals2(BATCH_SIZE);
    EXPECT_EQ(num_vals,
        reader.UnpackBatch(bit_width, num_vals, batch_vals.data()));
    for (int i = 0; i < num_vals; ++i) {
      if (i % BATCH_SIZE == 0) {
        int num_to_unpack = min(BATCH_SIZE, num_vals - i);
        EXPECT_EQ(num_to_unpack,
           reader2.UnpackBatch(bit_width, num_to_unpack, batch_vals2.data()));
      }
      EXPECT_EQ(i % mod, batch_vals[i]);
      EXPECT_EQ(i % mod, batch_vals2[i % BATCH_SIZE]);
    }
    EXPECT_EQ(reader.bytes_left(), 0);
    EXPECT_EQ(reader2.bytes_left(), 0);
    reader.Reset(buffer, len);
    reader2.Reset(buffer, len);
  }
}

TEST(BitArray, TestValues) {
  for (int width = 0; width <= MAX_WIDTH; ++width) {
    TestBitArrayValues(width, 1);
    TestBitArrayValues(width, 2);
    // Don't write too many values
    TestBitArrayValues(width, (width < 12) ? (1 << width) : 4096);
    TestBitArrayValues(width, 1024);
  }
}

}

