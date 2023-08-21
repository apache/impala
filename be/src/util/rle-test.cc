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
#include <random>

#include "testutil/gtest-util.h"
#include "testutil/rand-util.h"
#include "util/bit-packing.inline.h"
#include "util/bit-stream-utils.h"
#include "util/encoding-test-util.h"
#include "util/rle-encoding.h"

#include "common/names.h"

namespace impala {

const int MAX_WIDTH = BatchedBitReader::MAX_BITWIDTH;

class RleTest : public ::testing::Test {
 protected:

  /// Get many values from a batch RLE decoder using its low level functions.
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

  /// Get many values from a batch RLE decoder using its low level functions.
  template <typename T>
  static bool GetRleValuesSkip(RleBatchDecoder<T>* decoder, int num_vals, T* vals,
      int skip_at, int skip_count) {
    if (!GetRleValues(decoder, skip_at, vals)) return false;
    if (decoder->SkipValues(skip_count) != skip_count) return false;
    int consumed = skip_at + skip_count;
    if (!GetRleValues(decoder, num_vals - consumed, vals + skip_at)) return false;
    return true;
  }

  /// Get many values from a batch RLE decoder using its GetValues() function.
  template <typename T>
  static bool GetRleValuesBatched(RleBatchDecoder<T>* decoder, int num_vals, T* vals) {
    return num_vals == decoder->GetValues(num_vals, vals);
  }

  /// Get many values from a batch RLE decoder using its GetValues() function.
  template <typename T>
  static bool GetRleValuesBatchedSkip(RleBatchDecoder<T>* decoder, int num_vals, T* vals,
      int skip_at, int skip_count) {
    int cnt = 0;
    if (skip_at > 0) cnt += decoder->GetValues(skip_at, vals);
    if (decoder->SkipValues(skip_count) != skip_count) return false;
    cnt += skip_count;
    if (num_vals - cnt > 0) cnt += decoder->GetValues(num_vals - cnt, vals + skip_at);
    return cnt == num_vals;
  }

  // Validates encoding of values by encoding and decoding them.
  // If expected_encoding != NULL, validates that the encoded buffer is
  // exactly 'expected_encoding'.
  // if expected_len is not -1, validates that is is the same as the encoded size (in
  // bytes).
  int ValidateRle(const vector<int>& values, int bit_width, uint8_t* expected_encoding,
      int expected_len) {
    stringstream ss;
    ss << "bit_width=" << bit_width;
    const string& description = ss.str();
    const int len = 64 * 1024;
    uint8_t buffer[len];
    EXPECT_LE(expected_len, len);

    RleEncoder encoder(buffer, len, bit_width);

    int encoded_len = 0;
    for (int clear_count = 0; clear_count < 2; clear_count++) {
      if (clear_count >= 1) {
        // Check that we can reuse the encoder after calling Clear().
        encoder.Clear();
      }
      for (int i = 0; i < values.size(); ++i) {
        bool result = encoder.Put(values[i]);
        EXPECT_TRUE(result);
      }
      encoded_len = encoder.Flush();

      if (expected_len != -1) {
        EXPECT_EQ(encoded_len, expected_len);
      }
      if (expected_encoding != NULL) {
        EXPECT_TRUE(memcmp(buffer, expected_encoding, expected_len) == 0);
      }

      // Verify read.
      RleBatchDecoder<uint64_t> per_value_decoder(buffer, len, bit_width);
      RleBatchDecoder<uint64_t> per_run_decoder(buffer, len, bit_width);
      RleBatchDecoder<uint64_t> batch_decoder(buffer, len, bit_width);
      // Ensure it returns the same results after Reset().
      for (int trial = 0; trial < 2; ++trial) {
        for (int i = 0; i < values.size(); ++i) {
          uint64_t val;
          EXPECT_TRUE(per_value_decoder.GetSingleValue(&val)) << description;
          EXPECT_EQ(values[i], val) << description << " i=" << i << " trial=" << trial;
        }
        // Unpack everything at once from the other decoders.
        vector<uint64_t> decoded_values1(values.size());
        vector<uint64_t> decoded_values2(values.size());
        EXPECT_TRUE(GetRleValues(
            &per_run_decoder, decoded_values1.size(), decoded_values1.data()));
        EXPECT_TRUE(GetRleValuesBatched(
            &batch_decoder, decoded_values2.size(), decoded_values2.data()));
        for (int i = 0; i < values.size(); ++i) {
          EXPECT_EQ(values[i], decoded_values1[i]) << description << " i=" << i;
          EXPECT_EQ(values[i], decoded_values2[i]) << description << " i=" << i;
        }
        per_value_decoder.Reset(buffer, len, bit_width);
        per_run_decoder.Reset(buffer, len, bit_width);
        batch_decoder.Reset(buffer, len, bit_width);
      }
    }
    return encoded_len;
  }

  int ValidateRleSkip(
      const vector<int>& values, int bit_width, int skip_at, int skip_count) {
    stringstream ss;
    ss << "bit_width=" << bit_width
       << " skip_at=" << skip_at
       << " skip_count=" << skip_count
       << " values.size()=" << values.size();
    const string& description = ss.str();
    const int len = 64 * 1024;
    uint8_t buffer[len];

    RleEncoder encoder(buffer, len, bit_width);
    int encoded_len = 0;

    for (int i = 0; i < values.size(); ++i) {
      bool result = encoder.Put(values[i]);
      EXPECT_TRUE(result);
    }
    encoded_len = encoder.Flush();

    vector<int> expected_values(values.begin(), values.begin() + skip_at);
    for (int i = skip_at + skip_count; i < values.size(); ++i) {
      expected_values.push_back(values[i]);
    }

    // Verify read.
    RleBatchDecoder<uint64_t> per_value_decoder(buffer, len, bit_width);
    RleBatchDecoder<uint64_t> per_run_decoder(buffer, len, bit_width);
    RleBatchDecoder<uint64_t> batch_decoder(buffer, len, bit_width);
    // Ensure it returns the same results after Reset().
    for (int trial = 0; trial < 2; ++trial) {
      for (int i = 0; i < skip_at; ++i) {
        uint64_t val;
        EXPECT_TRUE(per_value_decoder.GetSingleValue(&val)) << description;
        EXPECT_EQ(expected_values[i], val) << description << " i=" << i << " trial="
            << trial;
      }
      per_value_decoder.SkipValues(skip_count);
      for (int i = skip_at; i < expected_values.size(); ++i) {
              uint64_t val;
              EXPECT_TRUE(per_value_decoder.GetSingleValue(&val)) << description;
              EXPECT_EQ(expected_values[i], val) << description << " i=" << i << " trial="
                  << trial;
      }
      // Unpack everything at once from the other decoders.
      vector<uint64_t> decoded_values1(expected_values.size());
      vector<uint64_t> decoded_values2(expected_values.size());
      EXPECT_TRUE(
          GetRleValuesSkip(&per_run_decoder, values.size(),
              decoded_values1.data(), skip_at, skip_count)) << description;
      EXPECT_TRUE(
          GetRleValuesBatchedSkip(&batch_decoder, values.size(),
              decoded_values2.data(), skip_at, skip_count)) << description;
      for (int i = 0; i < expected_values.size(); ++i) {
        EXPECT_EQ(expected_values[i], decoded_values1[i]) << description << " i=" << i;
        EXPECT_EQ(expected_values[i], decoded_values2[i]) << description << " i=" << i;
      }
      per_value_decoder.Reset(buffer, len, bit_width);
      per_run_decoder.Reset(buffer, len, bit_width);
      batch_decoder.Reset(buffer, len, bit_width);
    }
    return encoded_len;
  }

  // ValidateRle on 'num_vals' values with width 'bit_width'. If 'value' != -1, that value
  // is used, otherwise alternating values are used.
  void TestRleValues(int bit_width, int num_vals, int value = -1) {
    const int64_t mod = (bit_width == 64) ? 1 : 1LL << bit_width;
    vector<int> values;
    values.reserve(num_vals);
    for (int v = 0; v < num_vals; ++v) {
      values.push_back((value != -1) ? value : (v % mod));
    }
    ValidateRle(values, bit_width, NULL, -1);
  }

  /// Returns the total number of bytes written when encoding the boolean values passed.
  int RleBooleanLength(const vector<int>& values) {
    return ValidateRle(values, 1, nullptr, -1);
  }

  /// Make a sequence of values.
  /// intitial_literal_length: the length of an initial literal sequence.
  /// repeated_length: the length of a repeated sequence.
  /// trailing_literal_length: the length of a closing literal sequence.
  /// bit_width: the bit length of the values being used.
  vector<int>& MakeSequenceBitWidth(vector<int>& values, int intitial_literal_length,
      int repeated_length, int trailing_literal_length, int bit_width) {
    const int64_t mod = 1LL << bit_width;
    values.clear();
    for (int i = 0; i < intitial_literal_length; ++i) {
      values.push_back(i % mod);
    }
    for (int i = 0; i < repeated_length; ++i) {
      values.push_back(1);
    }
    for (int i = 0; i < trailing_literal_length; ++i) {
      values.push_back(i % mod);
    }
    return values;
  }

  /// Same as above with bit width being 1.
  vector<int>& MakeSequence(vector<int>& values, int intitial_literal_length,
      int repeated_length, int trailing_literal_length) {
    return MakeSequenceBitWidth(values, intitial_literal_length, repeated_length,
        trailing_literal_length, 1);
  }
};

/// Basic test case for literal unpacking - two literals in a run.
TEST_F(RleTest, TwoLiteralRun) {
  vector<int> values{1, 0};
  ValidateRle(values, 1, nullptr, -1);
  for (int width = 1; width <= MAX_WIDTH; ++width) {
    ValidateRle(values, width, nullptr, -1);
    }
}

TEST_F(RleTest, ValueSkipping) {
  vector<int> seq;
    for (int bit_width : {1, 3, 7, 8, 20, 32}) {
      MakeSequenceBitWidth(seq, 100, 100, 100, bit_width);
      ValidateRleSkip(seq, bit_width, 0, 7);
      ValidateRleSkip(seq, bit_width, 0, 64);
      ValidateRleSkip(seq, bit_width, 0, 75);
      ValidateRleSkip(seq, bit_width, 0, 100);
      ValidateRleSkip(seq, bit_width, 0, 105);
      ValidateRleSkip(seq, bit_width, 0, 155);
      ValidateRleSkip(seq, bit_width, 0, 200);
      ValidateRleSkip(seq, bit_width, 0, 213);
      ValidateRleSkip(seq, bit_width, 0, 267);
      ValidateRleSkip(seq, bit_width, 0, 300);
      ValidateRleSkip(seq, bit_width, 7, 7);
      ValidateRleSkip(seq, bit_width, 35, 64);
      ValidateRleSkip(seq, bit_width, 55, 75);
      ValidateRleSkip(seq, bit_width, 99, 100);
      ValidateRleSkip(seq, bit_width, 100, 11);
      ValidateRleSkip(seq, bit_width, 101, 55);
      ValidateRleSkip(seq, bit_width, 102, 155);
      ValidateRleSkip(seq, bit_width, 104, 17);
      ValidateRleSkip(seq, bit_width, 122, 178);
      ValidateRleSkip(seq, bit_width, 200, 3);
      ValidateRleSkip(seq, bit_width, 200, 65);
      ValidateRleSkip(seq, bit_width, 203, 17);
      ValidateRleSkip(seq, bit_width, 215, 70);
      ValidateRleSkip(seq, bit_width, 217, 83);
  }
}

// Tests value-skipping on randomly generated input and
// random skipping positions and counts.
TEST_F(RleTest, ValueSkippingFuzzy) {
  const int bitwidth_iteration = 10;
  const int probe_iteration = 100;
  const int total_sequence_length = 2048;

  std::default_random_engine random_eng;
  RandTestUtil::SeedRng("RLE_TEST_SEED", &random_eng);

  // Generates random number between 'bottom' and 'top' (inclusive intervals).
  auto GetRandom = [&random_eng](int bottom, int top) {
    std::uniform_int_distribution<int> uni_dist(bottom, top);
    return uni_dist(random_eng);
  };

    for (int i = 0; i < bitwidth_iteration; ++i) {
      int bit_width = GetRandom(1, 32);
      int max_run_length = GetRandom(5, 200);
      vector<int> seq = MakeRandomSequence(random_eng, total_sequence_length,
          max_run_length, bit_width);
      for (int j = 0; j < probe_iteration; ++j) {
        int skip_at = GetRandom(0, seq.size() - 1);
        int skip_count = GetRandom(1, seq.size() - skip_at);
        ValidateRleSkip(seq, bit_width, skip_at, skip_count);
      }
  }
}

TEST_F(RleTest, SpecificSequences) {
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
  expected_buffer[0] = static_cast<uint8_t>((num_groups << 1) | 1);
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

TEST_F(RleTest, TestValues) {
  for (int width = 1; width <= MAX_WIDTH; ++width) {
    TestRleValues(width, 1);
    TestRleValues(width, 1024);
    TestRleValues(width, 1024, 0);
    TestRleValues(width, 1024, 1);
  }
}

TEST_F(RleTest, BitWidthZeroRepeated) {
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

TEST_F(RleTest, BitWidthZeroLiteral) {
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
TEST_F(RleTest, Flush) {
    vector<int> values;
    values.reserve(18);
    for (int i = 0; i < 16; ++i) values.push_back(1);
    values.push_back(0);
    ValidateRle(values, 1, NULL, -1);

    values.push_back(1);
    ValidateRle(values, 1, NULL, -1);
}

// Test some random sequences.
TEST_F(RleTest, Random) {
  int iters = 0;
  while (iters < 1000) {
    srand(static_cast<unsigned int>(iters++));
    if (iters % 10000 == 0) LOG(ERROR) << "Seed: " << iters;
    vector<int> values;
    bool parity = 0;
    for (int i = 0; i < 1000; ++i) {
      int group_size = rand() % 20 + 1;
      if (group_size > 16) {
        group_size = 1;
      }
      for (int j = 0; j < group_size; ++j) {
        values.push_back(parity);
      }
      parity = !parity;
    }
    ValidateRle(values, (iters % MAX_WIDTH) + 1, NULL, -1);
  }
}

// Test a sequence of 1 0's, 2 1's, 3 0's. etc
// e.g. 011000111100000
TEST_F(RleTest, RepeatedPattern) {
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

TEST_F(RleTest, Overflow) {
    for (int bit_width = 1; bit_width < 32; bit_width += 1) {
      for (int pad_buffer = 0; pad_buffer < 64; pad_buffer++) {
        const int len = RleEncoder::MinBufferSize(bit_width) + pad_buffer;
        uint8_t buffer[len];
        int num_added = 0;
        bool parity = true;

        RleEncoder encoder(buffer, len, bit_width);
        // Insert alternating true/false until there is no space left.
        while (true) {
          bool result = encoder.Put(static_cast<uint64_t>(parity));
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
          for (int i = 0; i < num_added; ++i)
            EXPECT_EQ(i % 2 == 0, decoded_values[i]) << i;

          decoder.Reset(buffer, bytes_written, bit_width);
        }
    }
  }
}

/// Construct data sequences in the vector values for bit_widths of 1 and 2, such that
/// encoding using runs results in the encoding occupying more space than if the sequences
/// had been encoded using literal values.
void AddPathologicalValues(vector<int>& values, int bit_width) {
  EXPECT_LE(bit_width, 2);
  // Using the notation of 'RXX' for a repeated run of length XX (so R16 is a
  // run of length 16), and 'LYY' for a literal run of length YY.
  // For bit_width=1 the sequence (L8 R16 L8 R16 L8) is encoded as following
  // for different values of min_run_length.
  //                                  L8  R16 L8  R16 L8
  // min_run_length 8  (old default)  2   2   2   2   2
  // min_run_length 16 (new default)  2   2   2   2   2
  // min_run_length 24                2   2   1   2   1 (one long literal run)
  //
  // So it would have been better not to use rle for this sequence.
  // Note that min_run_length=24 is not *always* better.
  for (int i = 0; i < 8; i++) {
    // A literal sequence.
    values.push_back(i % 2);
  }
  // For bit_width = 2, a run of 8 values is needed.
  // For bit_width = 1, a run of 16 values is needed.
  int length_run = 16 / bit_width;
  // For small amounts of data, the value returned by MaxBufferSize is dominated by
  // MinBufferSize. Add enough data that this is not true.
  for (int runs = 0; runs < 200; runs++) {
    for (int i = 0; i < length_run; i++) {
      // A sequence that can be encoded as a run.
      values.push_back(1);
    }
    for (int i = 0; i < 8; i++) {
      // A literal sequence.
      values.push_back(i % 2);
    }
  }
}

// Tests handling of a specific data corruption scenario where
// the literal or repeat count is decoded as 0 (which is invalid).
TEST_F(RleTest, ZeroLiteralOrRepeatCount) {
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

// Regression test for handing of repeat counts >= 2^31: IMPALA-6946.
TEST_F(RleTest, RepeatCountOverflow) {
  const int BUFFER_LEN = 1024;
  uint8_t buffer[BUFFER_LEN];

  for (bool literal_run : {true, false}) {
    memset(buffer, 0, BUFFER_LEN);
    LOG(INFO) << "Testing negative " << (literal_run ? "literal" : "repeated");
    BitWriter writer(buffer, BUFFER_LEN);
    // Literal runs have lowest bit 1. Repeated runs have lowest bit 0. All other bits
    // are 1.
    const uint32_t REPEATED_RUN_HEADER = 0xfffffffe;
    const uint32_t LITERAL_RUN_HEADER = 0xffffffff;
    bool success = writer.PutUleb128<uint32_t>(literal_run ?
        LITERAL_RUN_HEADER : REPEATED_RUN_HEADER);
    ASSERT_TRUE(success);
    writer.Flush();

    RleBatchDecoder<uint64_t> decoder(buffer, BUFFER_LEN, 1);
    // Repeated run length fits in an int32_t.
    if (literal_run) {
      EXPECT_EQ(0, decoder.NextNumRepeats()) << "Not a repeated run";
      // Literal run length would overflow int32_t - should gracefully fail decoding.
      EXPECT_EQ(0, decoder.NextNumLiterals());
    } else {
      EXPECT_EQ(0x7fffffff, decoder.NextNumRepeats());
      EXPECT_EQ(0, decoder.NextNumLiterals()) << "Not a literal run";
    }

    // IMPALA-6946: reading back run lengths that don't fit in int32_t hit various
    // DCHECKs.
    uint64_t val;
    if (literal_run) {
      EXPECT_EQ(0, decoder.GetValues(1, &val)) << "Decoding failed above.";
    } else {
      EXPECT_EQ(1, decoder.GetValues(1, &val));
      EXPECT_EQ(0, val) << "Buffer was initialized with all zeroes";
    }
  }
}

/// Test that encoded lengths are as expected as min_repeated_run_length varies.
TEST_F(RleTest, MeasureOutputLengths) {
  vector<int> values;
  // With min_repeated_run_length = 8, a sequence of 8 is inefficient.
  EXPECT_EQ(12, RleBooleanLength(MakeSequence(values, 32, 8, 32)));
  EXPECT_EQ(12, RleBooleanLength(MakeSequence(values, 32, 16, 32)));
  EXPECT_EQ(12, RleBooleanLength(MakeSequence(values, 32, 24, 32)));
  EXPECT_EQ(12, RleBooleanLength(MakeSequence(values, 32, 32, 32)));
}
}

