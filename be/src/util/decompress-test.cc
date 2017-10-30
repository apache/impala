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

#include "gen-cpp/Descriptors_types.h"

#include "runtime/mem-tracker.h"
#include "runtime/mem-pool.h"
#include "testutil/gtest-util.h"
#include "util/decompress.h"
#include "util/compress.h"

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
      CompressAndDecompress(compressor.get(), decompressor.get(), sizeof(input_), input_);
      // Test with odd-length input (to test the calculation of block-sizes in
      // SnappyBlockCompressor)
      CompressAndDecompress(compressor.get(), decompressor.get(), sizeof(input_) - 1,
          input_);
      // Test with input length of 1024 (to test SnappyBlockCompressor with a single
      // block)
      CompressAndDecompress(compressor.get(), decompressor.get(), 1024, input_);
      // Test with empty input
      if (format != THdfsCompression::BZIP2) {
        CompressAndDecompress(compressor.get(), decompressor.get(), 0, NULL);
      } else {
        // bzip does not allow NULL input
        CompressAndDecompress(compressor.get(), decompressor.get(), 0, input_);
      }
    }
    DecompressOverUnderSizedOutputBuffer(compressor.get(), decompressor.get(),
        sizeof(input_), input_);
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

  // Test the behavior when the decompressor is given too little / too much space.
  // Verify that the decompressor returns an error when the space is not enough, gives
  // the correct output size when the space is enough, and does not write beyond the
  // output size it claims.
  void DecompressOverUnderSizedOutputBuffer(Codec* compressor, Codec* decompressor,
      int64_t input_len, uint8_t* input) {
    uint8_t* compressed;
    int64_t compressed_length;
    bool compress_preallocated = false;
    int64_t max_compressed_length = compressor->MaxOutputLen(input_len, input);

    if (max_compressed_length > 0) {
      compressed = mem_pool_.Allocate(max_compressed_length);
      compressed_length = max_compressed_length;
      compress_preallocated = true;
    }
    EXPECT_OK(compressor->ProcessBlock(compress_preallocated, input_len,
        input, &compressed_length, &compressed));
    int64_t output_len = decompressor->MaxOutputLen(compressed_length, compressed);
    if (output_len == -1) output_len = input_len;
    uint8_t* output = mem_pool_.Allocate(output_len);

    // Check that the decompressor respects the output_len by passing in an
    // output len that is 4 bytes too small and verifying that those 4 bytes
    // are not touched. The decompressor should return a non-ok status, as it
    // does not have space to decompress the full output.
    output_len = output_len - 4;
    u_int32_t *canary = (u_int32_t *) &output[output_len];
    *canary = 0x66aa77bb;
    Status status = decompressor->ProcessBlock(true, compressed_length, compressed,
        &output_len, &output);
    EXPECT_EQ(*canary, 0x66aa77bb);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(output_len, 0);

    // Check that the output length is the same as input when the decompressor is provided
    // with abundant space.
    output_len = input_len * 2;
    output = mem_pool_.Allocate(output_len);
    EXPECT_TRUE(decompressor->ProcessBlock(true, compressed_length, compressed,
        &output_len, &output).ok());
    EXPECT_EQ(output_len, input_len);
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

  Status StreamingDecompress(Codec* decompressor, int64_t input_len, uint8_t* input,
      int64_t uncompressed_len, uint8_t* uncompressed_input, bool expected_stream_end,
      int64_t* bytes_decompressed = NULL) {
    // Should take multiple calls to ProcessBlockStreaming() to decompress the buffer.
    int64_t decompressed_len = 0;
    int64_t compressed_bytes_remaining = input_len;
    uint8_t* compressed_input = input;
    bool stream_end;
    do {
      uint8_t* output = NULL;
      int64_t output_len = 0;
      int64_t compressed_bytes_read = 0;
      RETURN_IF_ERROR(decompressor->ProcessBlockStreaming(compressed_bytes_remaining,
          compressed_input, &compressed_bytes_read, &output_len, &output, &stream_end));
      EXPECT_EQ(memcmp(uncompressed_input + decompressed_len, output, output_len), 0);
      decompressed_len += output_len;
      EXPECT_LE(decompressed_len, uncompressed_len);
      compressed_input = compressed_input + compressed_bytes_read;
      compressed_bytes_remaining -= compressed_bytes_read;
    } while (compressed_bytes_remaining > 0);

    EXPECT_EQ(0, compressed_bytes_remaining);
    EXPECT_EQ(stream_end, expected_stream_end);
    if (stream_end) EXPECT_EQ(decompressed_len, uncompressed_len);
    if (bytes_decompressed != NULL) *bytes_decompressed = decompressed_len;

    return Status::OK();
  }

  void CompressAndStreamingDecompress(Codec* compressor, Codec* decompressor,
      int64_t input_len, uint8_t* input) {
    uint8_t* compressed = NULL;
    int64_t compressed_length = 0;
    Compress(compressor, input_len, input, &compressed_length, &compressed, false);
    // If compressed_len is 0, there is nothing to decompress so should not expect
    // "stream_end == true" either.
    // Note the gzip compressor will generate some compressed data even if input == NULL
    // or input_len == 0.
    EXPECT_OK(StreamingDecompress(decompressor, compressed_length, compressed, input_len,
        input, compressed_length > 0));
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

  void RunTestMultiStreamDecompressing(THdfsCompression::type format) {
    uint8_t* compressed = NULL;
    uint8_t* uncompressed = NULL;
    int64_t uncompressed_len = 0;
    int64_t compressed_len = 0;

    // Generate multistream test data
    GenerateMultiStreamData(format, &uncompressed_len, &uncompressed,
        &compressed_len, &compressed);

    scoped_ptr<Codec> decompressor;
    EXPECT_OK(Codec::CreateDecompressor(&mem_pool_, true, format, &decompressor));

    // Test case 1. normal streams.
    EXPECT_OK(StreamingDecompress(decompressor.get(), compressed_len, compressed,
        uncompressed_len, uncompressed, true));

    // Test case 2. multistream that is truncated. We should get stream_end == false
    // but with no error.
    int truncated = rand() % 512 + 1;
    int64_t bytes_decompressed = 0;
    ASSERT_LE(truncated, compressed_len);
    EXPECT_OK(StreamingDecompress(decompressor.get(), compressed_len - truncated,
        compressed, uncompressed_len, uncompressed, false, &bytes_decompressed));
    // Decompress the remaining.
    EXPECT_OK(StreamingDecompress(decompressor.get(), truncated,
        compressed + (compressed_len - truncated), uncompressed_len - bytes_decompressed,
        uncompressed + bytes_decompressed, true));

    // Test case 3. multistream with junk data at the end.
    EXPECT_ERROR(StreamingDecompress(decompressor.get(), COMPRESSED_BUFFER_SIZE,
        compressed, uncompressed_len, uncompressed, false),
        TErrorCode::COMPRESSED_FILE_BLOCK_CORRUPTED);
    decompressor->Close();
  }

  // Try to simulate pbzip2 behavior. pbzip2 splits large input into smaller chunks
  // and compresses them separately, then concatenate the compressed streams together.
  // We generate ~16MB compressed data to make sure it's bigger than the decompressor's
  // output buffer size(STREAM_OUT_BUF_SIZE). With the generated raw input data, we
  // expect ~2:1 compression ratio so we need 4xSTREAM_OUT_BUF_SIZE input data in total.
  void GenerateMultiStreamData(THdfsCompression::type format, int64_t* uncompressed_len,
      uint8_t** uncompressed_data, int64_t* compressed_len, uint8_t** compressed_data) {
    uint8_t raw_input[RAW_INPUT_SIZE + 1];
    for (int i = 0; i < RAW_INPUT_SIZE; ++i) {
      raw_input[i] = 'a' + rand() % 26;
    }
    raw_input[RAW_INPUT_SIZE] = 0;

    // Repeatedly pick random-size input data(~1MB), compress it, then concatenate
    // those small compressed streams into one big buffer. Also save random input
    // into a single buffer to verify decompressor output.
    *compressed_data = mem_pool_.Allocate(COMPRESSED_BUFFER_SIZE);
    *uncompressed_data = mem_pool_.Allocate(UNCOMPRESSED_BUFFER_SIZE);
    *uncompressed_len = 0;
    *compressed_len = 0;

    scoped_ptr<Codec> compressor;
    EXPECT_OK(Codec::CreateCompressor(&mem_pool_, true, format, &compressor));

    // Make sure we don't completely fill the buffer, leave at least RAW_INPUT_SIZE
    // bytes free in compressed buffer for junk data testing (Test case 3).
    while (*compressed_len < (COMPRESSED_BUFFER_SIZE - RAW_INPUT_SIZE)
        && *uncompressed_len < (UNCOMPRESSED_BUFFER_SIZE - RAW_INPUT_SIZE)) {
      int len = RAW_INPUT_SIZE - (rand() % 1024);
      uint8_t* compressed_stream = NULL;
      int64_t compressed_length = 0;
      EXPECT_OK(compressor->ProcessBlock(false, len, raw_input, &compressed_length,
          &compressed_stream));
      memcpy(*compressed_data + *compressed_len, compressed_stream, compressed_length);
      memcpy(*uncompressed_data + *uncompressed_len, raw_input, len);
      *uncompressed_len += len;
      *compressed_len += compressed_length;
    }
    compressor->Close();
  }

  // Buffer to hold generated random data. Size doesn't matter, use 1MB for easy
  // calculation.
  static const int RAW_INPUT_SIZE = 1024 * 1024;
  // Need 2x STREAM_OUT_BUF_SIZE compressed data to make sure it's bigger than the
  // decompressor's output buffer size.
  static const int COMPRESSED_BUFFER_SIZE = 2 * Codec::STREAM_OUT_BUF_SIZE;
  // With the generated raw input data, we expect ~2:1 compression ratio so we need
  // 4x COMPRESSED_BUFFER_SIZE input data in total.
  static const int UNCOMPRESSED_BUFFER_SIZE = 2 * COMPRESSED_BUFFER_SIZE;
  // Buffer to hold generated random data that contains repeated letter [a..z] and [A..Z]
  // for compressor/decompressor testing.
  uint8_t input_[2 * 26 * 1024];

  // Buffer for testing ProcessBlockStreaming() which allocates STREAM_OUT_BUF_SIZE output
  // buffer. This is 4x the size of the output buffers to ensure that the decompressed output
  // requires several calls and doesn't need to be nicely aligned (the last call gets a
  // small amount of data).
  uint8_t input_streaming_[UNCOMPRESSED_BUFFER_SIZE];

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
  RunTestMultiStreamDecompressing(THdfsCompression::GZIP);
}

TEST_F(DecompressorTest, Deflate) {
  RunTest(THdfsCompression::DEFLATE);
  RunTestStreaming(THdfsCompression::DEFLATE);
  RunTestMultiStreamDecompressing(THdfsCompression::DEFLATE);
}

TEST_F(DecompressorTest, Bzip) {
  RunTest(THdfsCompression::BZIP2);
  RunTestStreaming(THdfsCompression::BZIP2);
  RunTestMultiStreamDecompressing(THdfsCompression::BZIP2);
}

TEST_F(DecompressorTest, SnappyBlocked) {
  RunTest(THdfsCompression::SNAPPY_BLOCKED);
}

TEST_F(DecompressorTest, Impala1506) {
  // Regression test for IMPALA-1506
  MemTracker trax;
  MemPool pool(&trax);
  scoped_ptr<Codec> compressor;
  EXPECT_OK(
      Codec::CreateCompressor(&pool, true, impala::THdfsCompression::GZIP, &compressor));

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

TEST_F(DecompressorTest, Impala5250) {
  // Regression test for IMPALA-5250. It tests that SnappyDecompressor handles an input
  // buffer with a zero byte correctly. It should set the output_length to 0.
  MemTracker trax;
  MemPool pool(&trax);
  scoped_ptr<Codec> decompressor;
  EXPECT_OK(Codec::CreateDecompressor(&pool, true, impala::THdfsCompression::SNAPPY,
      &decompressor));
  uint8_t buf[1]{0};
  uint8_t out_buf[1];
  int64_t output_length = 1;
  uint8_t* output = out_buf;
  EXPECT_OK(decompressor->ProcessBlock(true, 1, buf, &output_length, &output));
  EXPECT_EQ(output_length, 0);
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, false, impala::TestInfo::BE_TEST);
  int rand_seed = time(NULL);
  LOG(INFO) << "rand_seed: " << rand_seed;
  srand(rand_seed);
  return RUN_ALL_TESTS();
}
