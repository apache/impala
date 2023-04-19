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

#include <boost/utility.hpp>

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
    // We use uint8_t instead of bool because unpacking is only supported for unsigned
    // integers.
    uint8_t batch_vals[16];
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
  bool skipped = skipping_reader.SkipBatch(bit_width, skip_count);
  ASSERT_TRUE(skipped);
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

  // We use uint8_t instead of bool because unpacking is only supported for unsigned
  // integers.
  TestSkipping<uint8_t>(buffer, len, 1, 0, 8);
  TestSkipping<uint8_t>(buffer, len, 1, 0, 16);
  TestSkipping<uint8_t>(buffer, len, 1, 8, 8);
  TestSkipping<uint8_t>(buffer, len, 1, 8, 16);
  TestSkipping<uint8_t>(buffer, len, 1, 16, 8);
  TestSkipping<uint8_t>(buffer, len, 1, 16, 16);
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
  TestSkipping<uint32_t>(buffer, bytes_written, bit_width, 0, 4);
  TestSkipping<uint32_t>(buffer, bytes_written, bit_width, 4, 4);
  TestSkipping<uint32_t>(buffer, bytes_written, bit_width, 4, 8);
  TestSkipping<uint32_t>(buffer, bytes_written, bit_width, 8, 56);
}

// Writes 'num_vals' values with width 'bit_width' starting from 'start' and increasing
// and reads them back.
void TestBitArrayValues(int bit_width, uint64_t start, uint64_t num_vals) {
  const int len = BitUtil::Ceil(bit_width * num_vals, 8);
  const uint64_t mask = bit_width == 64 ? ~0UL : (1UL << bit_width) - 1;

  uint8_t buffer[(len > 0) ? len : 1];
  BitWriter writer(buffer, len);
  for (uint64_t i = 0; i < num_vals; ++i) {
    bool result = writer.PutValue((start + i) & mask, bit_width);
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
    vector<uint64_t> batch_vals(num_vals);
    const uint64_t BATCH_SIZE = 32;
    vector<uint64_t> batch_vals2(BATCH_SIZE);
    EXPECT_EQ(num_vals,
        reader.UnpackBatch(bit_width, num_vals, batch_vals.data()));
    for (uint64_t i = 0; i < num_vals; ++i) {
      if (i % BATCH_SIZE == 0) {
        int num_to_unpack = min(BATCH_SIZE, num_vals - i);
        EXPECT_EQ(num_to_unpack,
           reader2.UnpackBatch(bit_width, num_to_unpack, batch_vals2.data()));
      }
      EXPECT_EQ((start + i) & mask, batch_vals[i]);
      EXPECT_EQ((start + i) & mask, batch_vals2[i % BATCH_SIZE]);
    }

    EXPECT_EQ(reader.bytes_left(), 0);
    EXPECT_EQ(reader2.bytes_left(), 0);
    reader.Reset(buffer, len);
    reader2.Reset(buffer, len);
  }
}

TEST(BitArray, TestValues) {
  for (int width = 0; width <= MAX_WIDTH; ++width) {
    TestBitArrayValues(width, 0, 1);
    TestBitArrayValues(width, 0, 2);
    // Don't write too many values
    TestBitArrayValues(width, 0, (width < 12) ? (1 << width) : 4096);
    TestBitArrayValues(width, 0, 1024);

    // Also test values that need bitwidth > 32.
    TestBitArrayValues(width, 1099511627775LL, 1024);
  }
}

template<typename UINT_T>
void TestUleb128Encode(const UINT_T value, std::vector<uint8_t> expected_bytes) {
  const int len = BatchedBitReader::max_vlq_byte_len<UINT_T>();

  std::vector<uint8_t> buffer(len, 0);
  BitWriter writer(buffer.data(), len);

  const bool success = writer.PutUleb128(value);
  EXPECT_TRUE(success);

  if (expected_bytes.size() < len) {
    // Allow the user to input fewer bytes than the maximum.
    expected_bytes.resize(len, 0);
  }

  EXPECT_EQ(expected_bytes, buffer);
}

TEST(VLQInt, TestPutUleb128) {
  TestUleb128Encode<uint32_t>(5, {0x05});
  TestUleb128Encode<uint32_t>(2018, {0xe2, 0x0f});
  TestUleb128Encode<uint32_t>(25248, {0xa0, 0xc5, 0x01});
  TestUleb128Encode<uint32_t>(55442211, {0xa3, 0xf6, 0xb7, 0x1a});
  TestUleb128Encode<uint32_t>(4294967295, {0xff, 0xff, 0xff, 0xff, 0x0f});

  TestUleb128Encode<uint64_t>(5, {0x05});
  TestUleb128Encode<uint64_t>(55442211, {0xa3, 0xf6, 0xb7, 0x1a});
  TestUleb128Encode<uint64_t>(1649267441664, {0x80, 0x80, 0x80, 0x80, 0x80, 0x30});
  TestUleb128Encode<uint64_t>(60736917191292289,
                             {0x81, 0xeb, 0xc1, 0xaf, 0xf8, 0xfc, 0xf1, 0x6b});
  TestUleb128Encode<uint64_t>(18446744073709551615U,
                             {0xff, 0xff, 0xff, 0xff, 0xff,
                              0xff, 0xff, 0xff, 0xff, 0x01});
}

template<typename UINT_T>
void TestUleb128Decode(const UINT_T expected_value, const std::vector<uint8_t>& bytes) {
  BatchedBitReader reader(bytes.data(), bytes.size());
  UINT_T value;
  const bool success = reader.GetUleb128<UINT_T>(&value);
  EXPECT_TRUE(success);
  EXPECT_EQ(expected_value, value);
}

TEST(VLQInt, TestGetUleb128) {
  TestUleb128Decode<uint32_t>(5, {0x05});
  TestUleb128Decode<uint32_t>(2018, {0xe2, 0x0f});
  TestUleb128Decode<uint32_t>(25248, {0xa0, 0xc5, 0x01});
  TestUleb128Decode<uint32_t>(55442211, {0xa3, 0xf6, 0xb7, 0x1a});
  TestUleb128Decode<uint32_t>(4294967295, {0xff, 0xff, 0xff, 0xff, 0x0f});

  TestUleb128Decode<uint64_t>(5, {0x05});
  TestUleb128Decode<uint64_t>(55442211, {0xa3, 0xf6, 0xb7, 0x1a});
  TestUleb128Decode<uint64_t>(1649267441664, {0x80, 0x80, 0x80, 0x80, 0x80, 0x30});
  TestUleb128Decode<uint64_t>(60736917191292289,
                             {0x81, 0xeb, 0xc1, 0xaf, 0xf8, 0xfc, 0xf1, 0x6b});
  TestUleb128Decode<uint64_t>(18446744073709551615U,
                             {0xff, 0xff, 0xff, 0xff, 0xff,
                              0xff, 0xff, 0xff, 0xff, 0x01});
}

template<typename INT_T>
void TestZigZagEncode(const INT_T value, std::vector<uint8_t> expected_bytes) {
  const int len = BatchedBitReader::max_vlq_byte_len<INT_T>();

  std::vector<uint8_t> buffer(len, 0);
  BitWriter writer(buffer.data(), len);

  const bool success = writer.PutZigZagInteger(value);
  EXPECT_TRUE(success);

  if (expected_bytes.size() < len) {
    // Allow the user to input fewer bytes than the maximum.
    expected_bytes.resize(len, 0);
  }

  EXPECT_EQ(expected_bytes, buffer);
}

TEST(VLQInt, TestPutZigZagInteger) {
  TestZigZagEncode<int32_t>(-3, {0x05});
  TestZigZagEncode<int32_t>(-2485, {0xe9, 0x26});
  TestZigZagEncode<int32_t>(5648448, {0x80, 0xc1, 0xb1, 0x5});

  TestZigZagEncode<int64_t>(1629267541664, {0xc0, 0xfa, 0xcd, 0xfe, 0xea, 0x5e});
  TestZigZagEncode<int64_t>(-9223372036854775808U, // Most negative int64_t.
                           {0xff, 0xff, 0xff, 0xff, 0xff,
                            0xff, 0xff, 0xff, 0xff, 0x1});
}

template<typename INT_T>
void TestZigZagDecode(const INT_T expected_value, std::vector<uint8_t> bytes) {
  BatchedBitReader reader(bytes.data(), bytes.size());
  INT_T value;
  const bool success = reader.GetZigZagInteger<INT_T>(&value);
  EXPECT_TRUE(success);
  EXPECT_EQ(expected_value, value);
}

TEST(VLQInt, TestGetZigZagInteger) {
  TestZigZagDecode<int32_t>(-3, {0x05});
  TestZigZagDecode<int32_t>(-2485, {0xe9, 0x26});
  TestZigZagDecode<int32_t>(5648448, {0x80, 0xc1, 0xb1, 0x5});

  TestZigZagDecode<int64_t>(1629267541664, {0xc0, 0xfa, 0xcd, 0xfe, 0xea, 0x5e});
  TestZigZagDecode<int64_t>(-9223372036854775808U, // Most negative int64_t.
                           {0xff, 0xff, 0xff, 0xff, 0xff,
                            0xff, 0xff, 0xff, 0xff, 0x1});
}

TEST(VLQInt, TestMaxVlqByteLen) {
  EXPECT_EQ(2, BatchedBitReader::max_vlq_byte_len<uint8_t>());
  EXPECT_EQ(3, BatchedBitReader::max_vlq_byte_len<uint16_t>());
  EXPECT_EQ(5, BatchedBitReader::max_vlq_byte_len<uint32_t>());
  EXPECT_EQ(10, BatchedBitReader::max_vlq_byte_len<uint64_t>());
}

}
