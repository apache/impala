// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include <boost/utility.hpp>
#include <gtest/gtest.h>
#include <math.h>

#include "util/rle-encoding.h"
#include "util/bit-stream-utils.h"

using namespace std;

namespace impala {

TEST(BitArray, TestBool) {
  const int len = 8;
  uint8_t buffer[len];

  BitWriter writer(buffer, len);

  // Write alternating 0's and 1's
  for (int i = 0; i < 8; ++i) {
    bool result = writer.PutBool(i % 2);
    EXPECT_TRUE(result);
  }
  EXPECT_EQ((int)buffer[0], BOOST_BINARY(1 0 1 0 1 0 1 0));

  // Write 00110011
  for (int i = 0; i < 8; ++i) {
    bool result = false;
    switch (i) {
      case 0:
      case 1:
      case 4:
      case 5:
        result = writer.PutBool(false);
        break;
      default:
        result = writer.PutBool(true);
        break;
    }
    EXPECT_TRUE(result);
  }
  
  // Validate the exact bit value
  EXPECT_EQ((int)buffer[0], BOOST_BINARY(1 0 1 0 1 0 1 0));
  EXPECT_EQ((int)buffer[1], BOOST_BINARY(1 1 0 0 1 1 0 0));

  // Use the reader and validate
  BitReader reader(buffer, len);
  for (int i = 0; i < 8; ++i) {
    bool val;
    bool result = reader.GetBool(&val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, i % 2);
  }
  
  for (int i = 0; i < 8; ++i) {
    bool val;
    bool result = reader.GetBool(&val);
    EXPECT_TRUE(result);
    switch (i) {
      case 0:
      case 1:
      case 4:
      case 5:
        EXPECT_EQ(val, false);
        break;
      default:
        EXPECT_EQ(val, true);
        break;
    }
  }
}

#if 0
Re-enable these tests when we have support for multiple bit, bit packed
encoding implemented.
// Tests writing all bytes
TEST(BitArray, TestByte) {
  const int len = 256;
  uint8_t buffer[len];
  BitWriter writer(buffer, len);
  for (int i = 0; i < len; ++i) {
    bool result = writer.Put<uint8_t>(i);
    EXPECT_TRUE(result);
    EXPECT_EQ(buffer[i], i);
  }

  BitReader reader(buffer, len);
  for (int i = 0; i < 8; ++i) {
    uint8_t val;
    bool result = reader.Get<uint8_t>(&val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, i);
  }
}

// Test some mixed values
TEST(BitArray, TestMixed) {
  const int len = 1024;
  uint8_t buffer[len];
  bool parity = true;

  BitWriter writer(buffer, len);
  for (int i = 0; i < len; ++i) {
    bool result;
    if (i % 2 == 0) {
      result = writer.Put<bool>(parity);
      parity = !parity;
    } else {
      result = writer.Put<uint8_t>(i);
    }
    EXPECT_TRUE(result);
  }

  parity = true;
  BitReader reader(buffer, len);
  for (int i = 0; i < len; ++i) {
    bool result;
    if (i % 2 == 0) {
      bool val;
      result = reader.Get<bool>(&val);
      EXPECT_EQ(val, parity);
      parity = !parity;
    } else {
      uint8_t val;
      result = reader.Get<uint8_t>(&val);
      EXPECT_EQ(val, static_cast<uint8_t>(i));
    }
    EXPECT_TRUE(result);
  }
}
#endif

// Validates encoding of values by encoding and decoding them.  If 
// expected_encoding != NULL, also validates that the encoded buffer is 
// exactly 'expected_encoding'.
// if expected_len is not -1, it will validate the encoded size is correct.
void ValidateRle(const vector<int>& values, 
    uint8_t* expected_encoding, int expected_len) {
  const int len = 64 * 1024;
  uint8_t buffer[len];
  EXPECT_LE(expected_len, len);

  RleEncoder encoder(buffer, len);
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
  RleDecoder decoder(buffer, encoded_len);
  for (int i = 0; i < values.size(); ++i) {
    uint8_t val;
    bool result = decoder.Get(&val);
    EXPECT_TRUE(result);
    EXPECT_EQ(values[i], val);
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
  expected_buffer[0] = (50 << 1);
  expected_buffer[1] = 0;
  expected_buffer[2] = (50 << 1);
  expected_buffer[3] = 1;
  ValidateRle(values, expected_buffer, 4);

  // Test 100 0's and 1's alternating
  for (int i = 0; i < 100; ++i) {
    values[i] = i % 2;
  }
  int num_groups = BitUtil::Ceil(100, 8);
  expected_buffer[0] = (num_groups << 1) | 1;
  for (int i = 0; i < 100/8; ++i) {
    expected_buffer[i + 1] = BOOST_BINARY(1 0 1 0 1 0 1 0);
  }
  // Values for the last 4 0 and 1's
  expected_buffer[1 + 100/8] = BOOST_BINARY(0 0 0 0 1 0 1 0);
  ValidateRle(values, expected_buffer, 1 + num_groups);
}

// Tests alternating true/false values.
TEST(Rle, AlternateTest) { 
  const int len = 2048;
  vector<int> values;
  for (int i = 0; i < len; ++i) {
    values.push_back(i % 2);
  }
  ValidateRle(values, NULL, -1);
}

// Tests all true/false values
TEST(BitRle, AllSame) {
  const int len = 1024;
  vector<int> values;

  for (int v = 0; v < 2; ++v) {
    values.clear();
    for (int i = 0; i < len; ++i) {
      values.push_back(v);
    }

    ValidateRle(values, NULL, 3);
  }
}

// Test that writes out a repeated group and then a literal
// group but flush before finishing.
TEST(BitRle, Flush) {
  vector<int> values;
  for (int i = 0; i < 16; ++i) values.push_back(1);
  values.push_back(0);
  ValidateRle(values, NULL, -1);
  values.push_back(1);
  ValidateRle(values, NULL, -1);
  values.push_back(1);
  ValidateRle(values, NULL, -1);
  values.push_back(1);
  ValidateRle(values, NULL, -1);
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
    ValidateRle(values, NULL, -1);
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

  ValidateRle(values, NULL, -1);
}

TEST(BitRle, Overflow) {
  return;
  const int len = 16;
  uint8_t buffer[len];
  int num_added = 0;
  bool parity = true;

  RleEncoder encoder(buffer, len);
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

  RleDecoder decoder(buffer, bytes_written);
  parity = true;
  for (int i = 0; i < num_added; ++i) {
    uint8_t v;
    bool result = decoder.Get(&v);
    EXPECT_TRUE(result);
    EXPECT_EQ(v, parity);
    parity = !parity;
  }
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

