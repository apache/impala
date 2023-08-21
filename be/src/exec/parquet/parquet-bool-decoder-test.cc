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

#include "exec/parquet/parquet-bool-decoder.h"
#include "testutil/gtest-util.h"

#include <vector>

#include "common/names.h"

namespace impala {

void EncodeData(const vector<bool>& data, parquet::Encoding::type encoding,
    uint8_t* buffer, int buffer_len) {
  if (encoding == parquet::Encoding::PLAIN) {
    BitWriter writer(buffer, buffer_len);
    for (int b : data) {
      ASSERT_TRUE(writer.PutValue(b, 1));
    }
    writer.Flush();
  } else {
    DCHECK(encoding == parquet::Encoding::RLE);
    // We need to pass 'buffer + 4' because the ParquetBoolDecoder ignores the first 4
    // bytes (in Parquet RLE, the first 4 bytes are used to encode the data size).
    RleEncoder encoder(buffer + 4, buffer_len - 4, 1);
    for (int b : data) {
      ASSERT_TRUE(encoder.Put(b));
    }
    encoder.Flush();
  }
}

void TestSkipping(parquet::Encoding::type encoding, uint8_t* encoded_data,
    int encoded_data_len, const vector<bool>& expected_data, int skip_at,
    int skip_count) {
  using namespace parquet;
  ParquetBoolDecoder decoder;
  decoder.SetData(encoding, encoded_data, encoded_data_len);

  auto Decode = [encoding, &decoder]() {
    bool b;
    if (encoding == Encoding::PLAIN) {
      EXPECT_TRUE(decoder.DecodeValue<Encoding::PLAIN>(&b));
    } else {
      EXPECT_TRUE(decoder.DecodeValue<Encoding::RLE>(&b));
    }
    return b;
  };

  for (int i = 0; i < skip_at && i < expected_data.size(); ++i) {
    EXPECT_EQ(Decode(), expected_data[i]) << i;
  }
  decoder.SkipValues(skip_count);
  for (int i = skip_at + skip_count; i < expected_data.size(); ++i) {
    EXPECT_EQ(Decode(), expected_data[i]) << i;
  }
}

TEST(ParquetBoolDecoder, TestDecodeAndSkipping) {
  vector<bool> expected_data;
  // Write 100 falses, 100 trues, 100 alternating falses and trues, 100 falses
  expected_data.reserve(400);
  for (int i = 0; i < 100; ++i) expected_data.push_back(false);
  for (int i = 0; i < 100; ++i) expected_data.push_back(true);
  for (int i = 0; i < 100; ++i) expected_data.push_back(i % 2);
  for (int i = 0; i < 100; ++i) expected_data.push_back(false);

  for (auto encoding : {parquet::Encoding::PLAIN, parquet::Encoding::RLE}) {
    constexpr int buffer_len = 128;
    uint8_t buffer[buffer_len];
    EncodeData(expected_data, encoding, buffer, buffer_len);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 0, 8);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 0, 79);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 0, 160);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 0, 260);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 0, 370);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 27, 13);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 50, 112);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 50, 183);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 50, 270);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 50, 350);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 123, 8);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 125, 100);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 225, 17);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 225, 70);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 235, 160);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 337, 17);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 337, 60);
    TestSkipping(encoding, buffer, buffer_len, expected_data, 337, 63);
  }
}

}

