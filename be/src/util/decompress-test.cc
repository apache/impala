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

#include "testutil/gtest-util.h"
#include "runtime/mem-tracker.h"
#include "runtime/mem-pool.h"
#include "util/decompress.h"
#include "util/compress.h"
#include "gen-cpp/Descriptors_types.h"

#include "common/names.h"

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

    // The input for the streaming tests is a larger buffer which contains input_
    // at the beginning and end and is null otherwise.
    memset(&input_streaming_, 0, sizeof(input_streaming_));
    memcpy(&input_streaming_, &input_, sizeof(input_));
    memcpy(&input_streaming_[sizeof(input_streaming_) - sizeof(input_)],
        &input_, sizeof(input_));
  }

  ~DecompressorTest() {
    mem_pool_.FreeAll();
  }

  void RunTest(THdfsCompression::type format) {
    scoped_ptr<Codec> compressor;
    scoped_ptr<Codec> decompressor;

    EXPECT_OK(Codec::CreateCompressor(&mem_pool_, true, format, &compressor));
    EXPECT_OK(Codec::CreateDecompressor(&mem_pool_, true, format, &decompressor));

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

  void RunTestStreaming(THdfsCompression::type format) {
    scoped_ptr<Codec> compressor;
    scoped_ptr<Codec> decompressor;
    EXPECT_OK(Codec::CreateCompressor(&mem_pool_, true, format, &compressor));
    EXPECT_OK(Codec::CreateDecompressor(&mem_pool_, true, format, &decompressor));

    CompressAndStreamingDecompress(compressor.get(), decompressor.get(),
        sizeof(input_streaming_), input_streaming_);
    CompressAndStreamingDecompress(compressor.get(), decompressor.get(),
        0, NULL);
    CompressAndStreamingDecompress(compressor.get(), decompressor.get(),
        0, input_);

    compressor->Close();
    decompressor->Close();
  }

  void CompressAndDecompress(Codec* compressor, Codec* decompressor,
      int64_t input_len, uint8_t* input) {
    // Non-preallocated output buffers
    uint8_t* compressed;
    int64_t compressed_length;
    EXPECT_OK(compressor->ProcessBlock(false, input_len,
        input, &compressed_length, &compressed));
    uint8_t* output;
    int64_t output_len;
    EXPECT_OK(decompressor->ProcessBlock(false, compressed_length,
        compressed, &output_len, &output));

    EXPECT_EQ(output_len, input_len);
    EXPECT_EQ(memcmp(input, output, input_len), 0);

    // Preallocated output buffers
    int64_t max_compressed_length = compressor->MaxOutputLen(input_len, input);

    // Don't redo compression if compressor doesn't support MaxOutputLen()
    if (max_compressed_length != -1) {
      EXPECT_GE(max_compressed_length, 0);
      uint8_t* compressed = mem_pool_.Allocate(max_compressed_length);
      compressed_length = max_compressed_length;


      EXPECT_OK(compressor->ProcessBlock(true, input_len, input, &compressed_length,
                                           &compressed));
    }

    output_len = decompressor->MaxOutputLen(compressed_length, compressed);
    if (output_len == -1) output_len = input_len;
    output = mem_pool_.Allocate(output_len);

    EXPECT_OK(decompressor->ProcessBlock(true, compressed_length, compressed,
                                           &output_len, &output));

    EXPECT_EQ(output_len, input_len);
    EXPECT_EQ(memcmp(input, output, input_len), 0);
  }

  void Compress(Codec* compressor, int64_t input_len, uint8_t* input,
      int64_t* output_len, uint8_t** output, bool output_preallocated) {
    if (input == NULL && compressor->file_extension() == "bz2") {
      // bzip does not allow NULL input
      *output = NULL;
      *output_len = 0;
      return;
    }
    EXPECT_OK(compressor->ProcessBlock(output_preallocated, input_len,
        input, output_len, output));
  }

  void StreamingDecompress(Codec* decompressor, int64_t input_len, uint8_t* input,
      int64_t uncompressed_len, uint8_t* uncompressed_input) {
    // Should take multiple calls to ProcessBlockStreaming() to decompress the buffer.
    int64_t total_output_produced = 0;
    int64_t compressed_bytes_remaining = input_len;
    uint8_t* compressed_input = input;
    do {
      EXPECT_LE(total_output_produced, uncompressed_len);
      uint8_t* output = NULL;
      int64_t output_len = 0;
      int64_t compressed_bytes_read = 0;
      bool stream_end = false;
      EXPECT_OK(decompressor->ProcessBlockStreaming(compressed_bytes_remaining,
          compressed_input, &compressed_bytes_read, &output_len, &output, &stream_end));
      EXPECT_EQ(memcmp(uncompressed_input + total_output_produced, output, output_len), 0);
      total_output_produced += output_len;
      compressed_input = compressed_input + compressed_bytes_read;
      compressed_bytes_remaining -= compressed_bytes_read;
    } while (compressed_bytes_remaining > 0);
    EXPECT_EQ(0, compressed_bytes_remaining);
    EXPECT_EQ(total_output_produced, uncompressed_len);
  }

  void CompressAndStreamingDecompress(Codec* compressor, Codec* decompressor,
      int64_t input_len, uint8_t* input) {
    uint8_t* compressed = NULL;
    int64_t compressed_length = 0;
    Compress(compressor, input_len, input, &compressed_length, &compressed, false);
    StreamingDecompress(decompressor, compressed_length, compressed, input_len, input);
  }

  // Only tests compressors and decompressors with allocated output.
  void CompressAndDecompressNoOutputAllocated(Codec* compressor,
      Codec* decompressor, int64_t input_len, uint8_t* input) {
    // Preallocated output buffers for compressor
    int64_t max_compressed_length = compressor->MaxOutputLen(input_len, input);
    uint8_t* compressed = mem_pool_.Allocate(max_compressed_length);
    int64_t compressed_length = max_compressed_length;

    EXPECT_OK(compressor->ProcessBlock(true, input_len, input, &compressed_length,
        &compressed));

    int64_t output_len = decompressor->MaxOutputLen(compressed_length, compressed);
    if (output_len == -1) output_len = input_len;
    uint8_t* output = mem_pool_.Allocate(output_len);

    EXPECT_OK(decompressor->ProcessBlock(true, compressed_length, compressed,
        &output_len, &output));

    EXPECT_EQ(output_len, input_len);
    EXPECT_EQ(memcmp(input, output, input_len), 0);
  }


  uint8_t input_[2 * 26 * 1024];

  // Buffer for testing ProcessBlockStreaming() which allocates 8mb output buffers. This
  // is 4x + 1 the size of the output buffers to ensure that the decompressed output
  // requires several calls and doesn't need to be nicely aligned (the last call gets a
  // small amount of data).
  uint8_t input_streaming_[32 * 1024 * 1024 + 1];

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
  RunTestStreaming(THdfsCompression::GZIP);
}

TEST_F(DecompressorTest, Deflate) {
  RunTest(THdfsCompression::DEFLATE);
  RunTestStreaming(THdfsCompression::GZIP);
}

TEST_F(DecompressorTest, Bzip) {
  RunTest(THdfsCompression::BZIP2);
  RunTestStreaming(THdfsCompression::BZIP2);
}

TEST_F(DecompressorTest, SnappyBlocked) {
  RunTest(THdfsCompression::SNAPPY_BLOCKED);
}

TEST_F(DecompressorTest, Impala1506) {
  // Regression test for IMPALA-1506
  MemTracker trax;
  MemPool pool(&trax);
  scoped_ptr<Codec> compressor;
  Codec::CreateCompressor(&pool, true, impala::THdfsCompression::GZIP, &compressor);

  int64_t input_len = 3;
  const uint8_t input[3] = {1, 2, 3};
  int64_t output_len = -1;
  uint8_t* output = NULL;

  // call twice because the compressor will reallocate the first time
  EXPECT_OK(compressor->ProcessBlock(false, input_len, input, &output_len, &output));
  EXPECT_GE(output_len, 0);
  output_len = -1;
  EXPECT_OK(compressor->ProcessBlock(false, input_len, input, &output_len, &output));
  EXPECT_GE(output_len, 0);

  pool.FreeAll();
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
