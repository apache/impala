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

#pragma once

#include <cstdint>
#include <string>

// We need zlib.h here to declare stream_ below.
#include <bzlib.h>
#include <zlib.h>
#include <zstd.h>

#include "common/status.h"
#include "util/codec.h"

namespace impala {

class MemPool;

class GzipDecompressor : public Codec {
 public:
  GzipDecompressor(
      MemPool* mem_pool = nullptr, bool reuse_buffer = false, bool is_deflate = false);
  virtual ~GzipDecompressor();

  virtual Status Init() override WARN_UNUSED_RESULT;

  virtual int64_t MaxOutputLen(
      int64_t input_len, const uint8_t* input = nullptr) override;

  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length,
      uint8_t** output) override WARN_UNUSED_RESULT;

  virtual Status ProcessBlockStreaming(int64_t input_length, const uint8_t* input,
      int64_t* input_bytes_read, int64_t* output_length, uint8_t** output,
      bool* stream_end) override WARN_UNUSED_RESULT;

  virtual std::string file_extension() const override { return "gz"; }

 private:
  std::string DebugStreamState() const;

  /// If set assume deflate format, otherwise zlib or gzip
  bool is_deflate_;

  z_stream stream_;

  /// These are magic numbers from zlib.h.  Not clear why they are not defined there.
  const static int WINDOW_BITS = 15;    // Maximum window size
  const static int DETECT_CODEC = 32;   // Determine if this is libz or gzip from header.
};

class BzipDecompressor : public Codec {
 public:
  BzipDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~BzipDecompressor();

  virtual Status Init() override WARN_UNUSED_RESULT;
  virtual int64_t MaxOutputLen(
      int64_t input_len, const uint8_t* input = nullptr) override;
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length,
      uint8_t** output) override WARN_UNUSED_RESULT;
  virtual Status ProcessBlockStreaming(int64_t input_length, const uint8_t* input,
      int64_t* input_bytes_read, int64_t* output_length, uint8_t** output,
      bool* stream_end) override WARN_UNUSED_RESULT;
  virtual std::string file_extension() const override { return "bz2"; }

 private:
  std::string DebugStreamState() const;

  /// Used for streaming decompression.
  bz_stream stream_;
};

class SnappyDecompressor : public Codec {
 public:
  /// Snappy-compressed data block includes trailing 4-byte checksum. Decompressor
  /// doesn't expect this.
  static const uint TRAILING_CHECKSUM_LEN = 4;

  SnappyDecompressor(MemPool* mem_pool = nullptr, bool reuse_buffer = false);
  virtual ~SnappyDecompressor() { }

  virtual int64_t MaxOutputLen(
      int64_t input_len, const uint8_t* input = nullptr) override;
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length,
      uint8_t** output) override WARN_UNUSED_RESULT;
  virtual std::string file_extension() const override { return "snappy"; }
};

/// Lz4 is a compression codec with similar compression ratios as snappy but much faster
/// decompression. This decompressor is not able to decompress unless the output buffer
/// is allocated and will cause an error if asked to do so.
class Lz4Decompressor : public Codec {
 public:
  virtual ~Lz4Decompressor() { }
  Lz4Decompressor(MemPool* mem_pool = nullptr, bool reuse_buffer = false);

  virtual int64_t MaxOutputLen(
      int64_t input_len, const uint8_t* input = nullptr) override;
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length,
      uint8_t** output) override WARN_UNUSED_RESULT;
  virtual std::string file_extension() const override { return "lz4"; }
};

class SnappyBlockDecompressor : public Codec {
 public:
  SnappyBlockDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~SnappyBlockDecompressor() { }

  virtual int64_t MaxOutputLen(
      int64_t input_len, const uint8_t* input = nullptr) override;
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length,
      uint8_t** output) override WARN_UNUSED_RESULT;
  virtual std::string file_extension() const override { return "snappy"; }
};

/// Zstandard is a real-time compression algorithm, providing high compression ratios.
/// It offers a very wide range of compression/speed trade-off. This decompressor
/// supports both block and streaming, while block decompress requires output buffer be
/// pre-allocated.
class ZstandardDecompressor : public Codec {
 public:
  ZstandardDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~ZstandardDecompressor();

  virtual int64_t MaxOutputLen(
      int64_t input_len, const uint8_t* input = nullptr) override;
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length,
      uint8_t** output) override WARN_UNUSED_RESULT;
  /// File extension to use for this compression codec.
  /// Except parquet which uses ".parq" as the file extension.
  virtual std::string file_extension() const override { return "zst"; }
  virtual Status ProcessBlockStreaming(int64_t input_length, const uint8_t* input,
      int64_t* input_bytes_read, int64_t* output_length, uint8_t** output,
      bool* stream_end) override WARN_UNUSED_RESULT;

 private:
  /// Allocate one context per thread, and re-use for many time decompression.
  ZSTD_DCtx* stream_ = NULL;
};

/// Hadoop's block compression scheme on top of LZ4.
class Lz4BlockDecompressor : public Codec {
 public:
  virtual ~Lz4BlockDecompressor() { }
  Lz4BlockDecompressor(MemPool* mem_pool = nullptr, bool reuse_buffer = false);

  virtual int64_t MaxOutputLen(
      int64_t input_len, const uint8_t* input = nullptr) override;
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length,
      uint8_t** output) override WARN_UNUSED_RESULT;
  virtual std::string file_extension() const override { return "lz4"; }
};
}
