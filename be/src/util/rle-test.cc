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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include <boost/utility.hpp>
#include <math.h>

#include "testutil/gtest-util.h"
#include "util/rle-encoding.h"
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
    bool result = writer.PutValue(i % 2, 1);
    EXPECT_TRUE(result);
  }
  writer.Flush();
  EXPECT_EQ((int)buffer[0], BOOST_BINARY(1 0 1 0 1 0 1 0));

  // Write 00110011
  for (int i = 0; i < 8; ++i) {
    bool result = false;
    switch (i) {
      case 0:
      case 1:
      case 4:
      case 5:
        result = writer.PutValue(false, 1);
        break;
      default:
        result = writer.PutValue(true, 1);
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

// Writes 'num_vals' values with width 'bit_width' and reads them back.
void TestBitArrayValues(int bit_width, int num_vals) {
  const int len = BitUtil::Ceil(bit_width * num_vals, 8);
  const uint64_t mod = bit_width == 64 ? 1 : 1LL << bit_width;

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

/// Get many values from a batch RLE decoder.
template <typename T>
static bool GetRleValues(RleBatchDecoder<T>* decoder, int num_vals, T* vals) {
  int decoded = 0;
  // Decode repeated and literal runs until we've filled the output.
  while (decoded < num_vals) {
    if (decoder->NextNumRepeats() > 0) {
      EXPECT_EQ(0, decoder->NextNumLiterals());
      int num_repeats_to_output =
          min<int>(decoder->NextNumRepeats(), num_vals - decoded);
      T repeated_val = decoder->GetRepeatedValue(num_repeats_to_output);
      for (int i = 0; i < num_repeats_to_output; ++i) {
        *vals = repeated_val;
        ++vals;
      }
      decoded += num_repeats_to_output;
      continue;
    }
    int num_literals_to_output =
          min<int>(decoder->NextNumLiterals(), num_vals - decoded);
    if (num_literals_to_output == 0) return false;
    if (!decoder->GetLiteralValues(num_literals_to_output, vals)) return false;
    decoded += num_literals_to_output;
    vals += num_literals_to_output;
  }
  return true;
}

// Validates encoding of values by encoding and decoding them.  If
// expected_encoding != NULL, also validates that the encoded buffer is
// exactly 'expected_encoding'.
// if expected_len is not -1, it will validate the encoded size is correct.
void ValidateRle(const vector<int>& values, int bit_width,
                 uint8_t* expected_encoding, int expected_len) {
  const int len = 64 * 1024;
  uint8_t buffer[len];
  EXPECT_LE(expected_len, len);

  RleEncoder encoder(buffer, len, bit_width);
  for (int i = 0; i < values.size(); ++i) {
    bool result = encoder.Put(values[i]);
    EXPECT_TRUE(result);
  }
  int encoded_len = encoder.Flush();

  if (expected_len != -1) {
    EXPECT_EQ(encoded_len, expected_len);
  }
  if (expected_encoding != NULL) {
    EXPECT_TRUE(memcmp(buffer, expected_encoding, expected_len) == 0);
  }

  // Verify read
  RleBatchDecoder<uint64_t> decoder(buffer, len, bit_width);
  RleBatchDecoder<uint64_t> decoder2(buffer, len, bit_width);
  // Ensure it returns the same results after Reset().
  for (int trial = 0; trial < 2; ++trial) {
    for (int i = 0; i < values.size(); ++i) {
      uint64_t val;
      EXPECT_TRUE(decoder.GetSingleValue(&val));
      EXPECT_EQ(values[i], val) << i;
    }
    // Unpack everything at once from the second batch decoder.
    vector<uint64_t> decoded_values(values.size());
    EXPECT_TRUE(GetRleValues(&decoder2, values.size(), decoded_values.data()));
    for (int i = 0; i < values.size(); ++i) {
      EXPECT_EQ(values[i], decoded_values[i]) << i;
    }
    decoder.Reset(buffer, len, bit_width);
    decoder2.Reset(buffer, len, bit_width);
  }
}

/// Basic test case for literal unpacking - two literals in a run.
TEST(Rle, TwoLiteralRun) {
  vector<int> values{1, 0};
  ValidateRle(values, 1, nullptr, -1);
  for (int width = 1; width <= MAX_WIDTH; ++width) {
    ValidateRle(values, width, nullptr, -1);
  }
}

TEST(Rle, SpecificSequences) {
  const int len = 1024;
  uint8_t expected_buffer[len];
  vector<int> values;

  // Test 50 0' followed by 50 1's
  values.resize(100);
  for (int i = 0; i < 50; ++i) {
    values[i] = 0;
  }
  for (int i = 50; i < 100; ++i) {
    values[i] = 1;
  }

  // expected_buffer valid for bit width <= 1 byte
  expected_buffer[0] = (50 << 1);
  expected_buffer[1] = 0;
  expected_buffer[2] = (50 << 1);
  expected_buffer[3] = 1;
  for (int width = 1; width <= 8; ++width) {
    ValidateRle(values, width, expected_buffer, 4);
  }

  for (int width = 9; width <= MAX_WIDTH; ++width) {
    ValidateRle(values, width, NULL, 2 * (1 + BitUtil::Ceil(width, 8)));
  }

  // Test 100 0's and 1's alternating
  for (int i = 0; i < 100; ++i) {
    values[i] = i % 2;
  }
  int num_groups = BitUtil::Ceil(100, 8);
  expected_buffer[0] = (num_groups << 1) | 1;
  for (int i = 1; i <= 100/8; ++i) {
    expected_buffer[i] = BOOST_BINARY(1 0 1 0 1 0 1 0);
  }
  // Values for the last 4 0 and 1's. The upper 4 bits should be padded to 0.
  expected_buffer[100/8 + 1] = BOOST_BINARY(0 0 0 0 1 0 1 0);

  // num_groups and expected_buffer only valid for bit width = 1
  ValidateRle(values, 1, expected_buffer, 1 + num_groups);
  for (int width = 2; width <= MAX_WIDTH; ++width) {
    int num_values = BitUtil::Ceil(100, 8) * 8;
    ValidateRle(values, width, NULL, 1 + BitUtil::Ceil(width * num_values, 8));
  }
}

// ValidateRle on 'num_vals' values with width 'bit_width'. If 'value' != -1, that value
// is used, otherwise alternating values are used.
void TestRleValues(int bit_width, int num_vals, int value = -1) {
  const uint64_t mod = (bit_width == 64) ? 1 : 1LL << bit_width;
  vector<int> values;
  for (int v = 0; v < num_vals; ++v) {
    values.push_back((value != -1) ? value : (v % mod));
  }
  ValidateRle(values, bit_width, NULL, -1);
}

TEST(Rle, TestValues) {
  for (int width = 1; width <= MAX_WIDTH; ++width) {
    TestRleValues(width, 1);
    TestRleValues(width, 1024);
    TestRleValues(width, 1024, 0);
    TestRleValues(width, 1024, 1);
  }
}

TEST(Rle, BitWidthZeroRepeated) {
  uint8_t buffer[1];
  const int num_values = 15;
  buffer[0] = num_values << 1; // repeated indicator byte
  RleBatchDecoder<uint8_t> decoder(buffer, sizeof(buffer), 0);
  // Ensure it returns the same results after Reset().
  for (int trial = 0; trial < 2; ++trial) {
    uint8_t val;
    for (int i = 0; i < num_values; ++i) {
      EXPECT_TRUE(decoder.GetSingleValue(&val));
      EXPECT_EQ(val, 0);
    }
    EXPECT_FALSE(decoder.GetSingleValue(&val));

    // Test decoding all values in a batch.
    decoder.Reset(buffer, sizeof(buffer), 0);
    uint8_t decoded_values[num_values];
    EXPECT_TRUE(GetRleValues(&decoder, num_values, decoded_values));
    for (int i = 0; i < num_values; i++) EXPECT_EQ(0, decoded_values[i]) << i;
    EXPECT_FALSE(decoder.GetSingleValue(&val));
    decoder.Reset(buffer, sizeof(buffer), 0);
  }
}

TEST(Rle, BitWidthZeroLiteral) {
  uint8_t buffer[1];
  const int num_groups = 4;
  buffer[0] = num_groups << 1 | 1; // literal indicator byte
  RleBatchDecoder<uint8_t> decoder(buffer, sizeof(buffer), 0);
  // Ensure it returns the same results after Reset().
  for (int trial = 0; trial < 2; ++trial) {
    const int num_values = num_groups * 8;
    uint8_t val;
    for (int i = 0; i < num_values; ++i) {
      EXPECT_TRUE(decoder.GetSingleValue(&val));
      EXPECT_EQ(val, 0); // can only encode 0s with bit width 0
    }

    // Test decoding the whole batch at once.
    decoder.Reset(buffer, sizeof(buffer), 0);
    uint8_t decoded_values[num_values];
    EXPECT_TRUE(GetRleValues(&decoder, num_values, decoded_values));
    for (int i = 0; i < num_values; ++i) EXPECT_EQ(0, decoded_values[i]);

    EXPECT_FALSE(GetRleValues(&decoder, 1, decoded_values));
    decoder.Reset(buffer, sizeof(buffer), 0);
  }
}

// Test that writes out a repeated group and then a literal
// group but flush before finishing.
TEST(BitRle, Flush) {
  vector<int> values;
  for (int i = 0; i < 16; ++i) values.push_back(1);
  values.push_back(0);
  ValidateRle(values, 1, NULL, -1);
  values.push_back(1);
  ValidateRle(values, 1, NULL, -1);
  values.push_back(1);
  ValidateRle(values, 1, NULL, -1);
  values.push_back(1);
  ValidateRle(values, 1, NULL, -1);
}

// Test some random sequences.
TEST(BitRle, Random) {
  int iters = 0;
  while (iters < 1000) {
    srand(iters++);
    if (iters % 10000 == 0) LOG(ERROR) << "Seed: " << iters;
    vector<int> values;
    bool parity = 0;
    for (int i = 0; i < 1000; ++i) {
      int group_size = rand() % 20 + 1;
      if (group_size > 16) {
        group_size = 1;
      }
      for (int i = 0; i < group_size; ++i) {
        values.push_back(parity);
      }
      parity = !parity;
    }
    ValidateRle(values, (iters % MAX_WIDTH) + 1, NULL, -1);
  }
}

// Test a sequence of 1 0's, 2 1's, 3 0's. etc
// e.g. 011000111100000
TEST(BitRle, RepeatedPattern) {
  vector<int> values;
  const int min_run = 1;
  const int max_run = 32;

  for (int i = min_run; i <= max_run; ++i) {
    int v = i % 2;
    for (int j = 0; j < i; ++j) {
      values.push_back(v);
    }
  }

  // And go back down again
  for (int i = max_run; i >= min_run; --i) {
    int v = i % 2;
    for (int j = 0; j < i; ++j) {
      values.push_back(v);
    }
  }

  ValidateRle(values, 1, NULL, -1);
}

TEST(BitRle, Overflow) {
  for (int bit_width = 1; bit_width < 32; bit_width += 3) {
    const int len = RleEncoder::MinBufferSize(bit_width);
    uint8_t buffer[len];
    int num_added = 0;
    bool parity = true;

    RleEncoder encoder(buffer, len, bit_width);
    // Insert alternating true/false until there is no space left
    while (true) {
      bool result = encoder.Put(parity);
      parity = !parity;
      if (!result) break;
      ++num_added;
    }

    int bytes_written = encoder.Flush();
    EXPECT_LE(bytes_written, len);
    EXPECT_GT(num_added, 0);

    RleBatchDecoder<uint32_t> decoder(buffer, bytes_written, bit_width);
    // Ensure it returns the same results after Reset().
    for (int trial = 0; trial < 2; ++trial) {
      parity = true;
      uint32_t v;
      for (int i = 0; i < num_added; ++i) {
        EXPECT_TRUE(decoder.GetSingleValue(&v));
        EXPECT_EQ(v, parity);
        parity = !parity;
      }
      // Make sure we get false when reading past end a couple times.
      EXPECT_FALSE(decoder.GetSingleValue(&v));
      EXPECT_FALSE(decoder.GetSingleValue(&v));

      decoder.Reset(buffer, bytes_written, bit_width);
      uint32_t decoded_values[num_added];
      EXPECT_TRUE(GetRleValues(&decoder, num_added, decoded_values));
      for (int i = 0; i < num_added; ++i) EXPECT_EQ(i % 2 == 0, decoded_values[i]) << i;

      decoder.Reset(buffer, bytes_written, bit_width);
    }
  }
}

// Tests handling of a specific data corruption scenario where
// the literal or repeat count is decoded as 0 (which is invalid).
TEST(Rle, ZeroLiteralOrRepeatCount) {
  const int len = 1024;
  uint8_t buffer[len];
  RleBatchDecoder<uint64_t> decoder(buffer, len, 0);
  // Test the RLE repeated values path.
  memset(buffer, 0, len);
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(0, decoder.NextNumLiterals());
    EXPECT_EQ(0, decoder.NextNumRepeats());
  }

  // Test the RLE literal values path
  memset(buffer, 1, len);
  decoder.Reset(buffer, len, 0);
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(0, decoder.NextNumLiterals());
    EXPECT_EQ(0, decoder.NextNumRepeats());
  }
}

}

IMPALA_TEST_MAIN();
