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
#include "runtime/mem-tracker.h"
#include "util/decompress.h"
#include "util/compress.h"
#include "gen-cpp/Descriptors_types.h"

using namespace std;
using namespace boost;

namespace impala {

// Fixture for testing class Decompressor
class DecompressorTest : public ::testing::Test {
 protected:
  DecompressorTest() : mem_pool_(&mem_tracker_) {
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

  ~DecompressorTest() {
    mem_pool_.FreeAll();
  }

  void RunTest(THdfsCompression::type format) {
    scoped_ptr<Codec> compressor;
    scoped_ptr<Codec> decompressor;

    EXPECT_TRUE(
        Codec::CreateCompressor(&mem_pool_, true, format, &compressor).ok());
    EXPECT_TRUE(
        Codec::CreateDecompressor(&mem_pool_, true, format, &decompressor).ok());

    // LZ4 is not implemented to work without an allocated output
    if(format == THdfsCompression::LZ4) {
      CompressAndDecompressNoOutputAllocated(compressor.get(), decompressor.get(),
          sizeof(input_), input_);
      CompressAndDecompressNoOutputAllocated(compressor.get(), decompressor.get(),
          0, NULL);
    } else {
      CompressAndDecompress(compressor.get(), decompressor.get(), sizeof(input_),
          input_);
      if (format != THdfsCompression::BZIP2) {
        CompressAndDecompress(compressor.get(), decompressor.get(), 0, NULL);
      } else {
        // bzip does not allow NULL input
        CompressAndDecompress(compressor.get(), decompressor.get(), 0, input_);
      }
    }

    compressor->Close();
    decompressor->Close();
  }

  void CompressAndDecompress(Codec* compressor, Codec* decompressor,
      int input_len, uint8_t* input) {
    // Non-preallocated output buffers
    uint8_t* compressed;
    int compressed_length;
    EXPECT_TRUE(compressor->ProcessBlock(false, input_len,
        input, &compressed_length, &compressed).ok());
    uint8_t* output;
    int output_len;
    EXPECT_TRUE(
        decompressor->ProcessBlock(false, compressed_length,
        compressed, &output_len, &output).ok());

    EXPECT_EQ(output_len, input_len);
    EXPECT_EQ(memcmp(input, output, input_len), 0);

    // Preallocated output buffers
    int max_compressed_length = compressor->MaxOutputLen(input_len, input);

    // Don't redo compression if compressor doesn't support MaxOutputLen()
    if (max_compressed_length != -1) {
      EXPECT_GE(max_compressed_length, 0);
      uint8_t* compressed = mem_pool_.Allocate(max_compressed_length);
      compressed_length = max_compressed_length;
      EXPECT_TRUE(compressor->ProcessBlock(true, input_len,
          input, &compressed_length, &compressed).ok());
    }

    output_len = decompressor->MaxOutputLen(compressed_length, compressed);
    if (output_len == -1) output_len = input_len;
    output = mem_pool_.Allocate(output_len);

    EXPECT_TRUE(decompressor->ProcessBlock(true, compressed_length,
        compressed, &output_len, &output).ok());

    EXPECT_EQ(output_len, input_len);
    EXPECT_EQ(memcmp(input, output, input_len), 0);
  }

 // Only tests compressors and decompressors with allocated output.
  void CompressAndDecompressNoOutputAllocated(Codec* compressor,
      Codec* decompressor, int input_len, uint8_t* input) {
    // Preallocated output buffers for compressor
    int max_compressed_length = compressor->MaxOutputLen(input_len, input);
    uint8_t* compressed = mem_pool_.Allocate(max_compressed_length);
    int compressed_length = max_compressed_length;

    EXPECT_TRUE(compressor->ProcessBlock(true, input_len,
        input, &compressed_length, &compressed).ok());

    int output_len = decompressor->MaxOutputLen(compressed_length, compressed);
    if (output_len == -1) output_len = input_len;
    uint8_t* output = mem_pool_.Allocate(output_len);

    EXPECT_TRUE(decompressor->ProcessBlock(true, compressed_length,
        compressed, &output_len, &output).ok());

    EXPECT_EQ(output_len, input_len);
    EXPECT_EQ(memcmp(input, output, input_len), 0);
  }

  uint8_t input_[2 * 26 * 1024];
  MemTracker mem_tracker_;
  MemPool mem_pool_;
};

TEST_F(DecompressorTest, Default) {
  RunTest(THdfsCompression::DEFAULT);
}

TEST_F(DecompressorTest, Snappy) {
  RunTest(THdfsCompression::SNAPPY);
}

TEST_F(DecompressorTest, LZ4) {
  RunTest(THdfsCompression::LZ4);
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

TEST_F(DecompressorTest, SnappyBlocked) {
  RunTest(THdfsCompression::SNAPPY_BLOCKED);
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
