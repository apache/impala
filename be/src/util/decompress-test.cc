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
#include <gtest/gtest.h>
#include "util/decompress.h"
#include "util/compress.h"
#include "gen-cpp/Descriptors_types.h"

using namespace std;
using namespace boost;

namespace impala {

// Fixture for testing class Decompressor
class DecompressorTest : public ::testing::Test{
 protected:
  DecompressorTest() {
    uint8_t* ip = input_;
    for (int i = 0; i < 1024; i++) {
      for (uint8_t ch = 'a'; ch <= 'z'; ++ch) {
        *ip++ = ch;
      }
      for (uint8_t ch = 'Z'; ch >= 'A'; --ch) {
        *ip++ = ch;
      }
    }
  }

  void RunTest(THdfsCompression::type format) {
    scoped_ptr<Codec> compressor;
    scoped_ptr<Codec> decompressor;
    MemPool* mem_pool = new MemPool;

    EXPECT_TRUE(
        Codec::CreateCompressor(NULL, mem_pool, true, format, &compressor).ok());
    EXPECT_TRUE(
        Codec::CreateDecompressor(NULL, mem_pool, true, format, &decompressor).ok());

    uint8_t* compressed;
    int compressed_length = 0;
    EXPECT_TRUE(compressor->ProcessBlock(sizeof(input_),
          input_, &compressed_length, &compressed).ok());
    uint8_t* output;
    int out_len = 0;
    EXPECT_TRUE(
        decompressor->ProcessBlock(compressed_length,
            compressed, &out_len, &output).ok());

    EXPECT_TRUE(memcmp(&input_, output, sizeof(input_)) == 0);

    // Try again specifying the output buffer and length.
    out_len = sizeof (input_);
    output = mem_pool->Allocate(out_len);
    EXPECT_TRUE(decompressor->ProcessBlock(compressed_length,
          compressed, &out_len, &output).ok());

    EXPECT_TRUE(memcmp(&input_, output, sizeof(input_)) == 0);
  }

  uint8_t input_[2 * 26 * 1024];
};

TEST_F(DecompressorTest, Default) {
  RunTest(THdfsCompression::DEFAULT);
}

TEST_F(DecompressorTest, Gzip) {
  RunTest(THdfsCompression::GZIP);
}

TEST_F(DecompressorTest, Deflate) {
  RunTest(THdfsCompression::DEFLATE);
}

TEST_F(DecompressorTest, Bzip) {
  RunTest(THdfsCompression::BZIP2);
}

TEST_F(DecompressorTest, Snappy) {
  RunTest(THdfsCompression::SNAPPY);
}

TEST_F(DecompressorTest, SnappyBlocked) {
  RunTest(THdfsCompression::SNAPPY_BLOCKED);
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

