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

  void RunTest(const char* codec_str) {
    scoped_ptr<Compressor> compressor;
    scoped_ptr<Decompressor> decompressor;
    MemPool* mem_pool = new MemPool;
    vector<char> codec(strlen(codec_str));
    memcpy(&codec[0], codec_str, strlen(codec_str));

    EXPECT_TRUE(
        Compressor::CreateCompressor(NULL, mem_pool, true, codec, &compressor).ok());
    EXPECT_TRUE(Decompressor::CreateDecompressor(NULL,
        mem_pool, true, codec, &decompressor).ok());

    uint8_t* compressed;
    int compressed_length = 0;
    EXPECT_TRUE(compressor->ProcessBlock(sizeof (input_),
          input_, &compressed_length, &compressed).ok());
    uint8_t* output;
    EXPECT_TRUE(
        decompressor->ProcessBlock(compressed_length, compressed, 0, &output).ok());

    EXPECT_TRUE(memcmp(&input_, output, sizeof (input_)) == 0);

    // Try again specifying the output buffer and length.
    output = mem_pool->Allocate(sizeof (input_));
    EXPECT_TRUE(decompressor->ProcessBlock(compressed_length,
          compressed, sizeof (input_), &output).ok());

    EXPECT_TRUE(memcmp(&input_, output, sizeof (input_)) == 0);
  }

  uint8_t input_[2 * 26 * 1024];
};

TEST_F(DecompressorTest, Default) {
  RunTest(Decompressor::Decompressor::DEFAULT_COMPRESSION);
}

TEST_F(DecompressorTest, Gzip) {
  RunTest(Decompressor::Decompressor::GZIP_COMPRESSION);
}

TEST_F(DecompressorTest, Bzip) {
  RunTest(Decompressor::Decompressor::BZIP2_COMPRESSION);
}

TEST_F(DecompressorTest, Snappy) {
  RunTest(Decompressor::Decompressor::SNAPPY_COMPRESSION);
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

