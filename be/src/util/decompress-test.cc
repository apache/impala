// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <gtest/gtest.h>
#include "util/decompress.h"
#include "util/compress.h"

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

  void RunTest(const char* codec) {
    scoped_ptr<Codec> compressor;
    scoped_ptr<Codec> decompressor;
    MemPool* mem_pool = new MemPool;

    EXPECT_TRUE(
        Codec::CreateCompressor(NULL, mem_pool, true, codec, &compressor).ok());
    EXPECT_TRUE(Codec::CreateDecompressor(NULL,
        mem_pool, true, codec, &decompressor).ok());

    uint8_t* compressed;
    int compressed_length = 0;
    EXPECT_TRUE(compressor->ProcessBlock(sizeof (input_),
          input_, &compressed_length, &compressed).ok());
    uint8_t* output;
    int out_len = 0;
    EXPECT_TRUE(
        decompressor->ProcessBlock(compressed_length,
            compressed, &out_len, &output).ok());

    EXPECT_TRUE(memcmp(&input_, output, sizeof (input_)) == 0);

    // Try again specifying the output buffer and length.
    out_len = sizeof (input_);
    output = mem_pool->Allocate(out_len);
    EXPECT_TRUE(decompressor->ProcessBlock(compressed_length,
          compressed, &out_len, &output).ok());

    EXPECT_TRUE(memcmp(&input_, output, sizeof (input_)) == 0);
  }

  uint8_t input_[2 * 26 * 1024];
};

TEST_F(DecompressorTest, Default) {
  RunTest(Codec::DEFAULT_COMPRESSION);
}

TEST_F(DecompressorTest, Gzip) {
  RunTest(Codec::GZIP_COMPRESSION);
}

TEST_F(DecompressorTest, Bzip) {
  RunTest(Codec::BZIP2_COMPRESSION);
}

TEST_F(DecompressorTest, Snappy) {
  RunTest(Codec::SNAPPY_COMPRESSION);
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

