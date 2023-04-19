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

/// We need zlib.h here to declare stream_ below.
#include <zlib.h>
#include <zstd.h>

#include "common/status.h"
#include "util/codec.h"

namespace impala {

class MemPool;

/// Different compression classes.  The classes all expose the same API and
/// abstracts the underlying calls to the compression libraries.
/// TODO: reconsider the abstracted API

class GzipCompressor : public Codec {
 public:
  /// Compression formats supported by the zlib library
  enum Format {
    ZLIB,
    DEFLATE,
    GZIP,
  };

  GzipCompressor(Format format, MemPool* mem_pool = nullptr, bool reuse_buffer = false);
  virtual ~GzipCompressor();

  virtual Status Init() override WARN_UNUSED_RESULT;
  virtual int64_t MaxOutputLen(
      int64_t input_len, const uint8_t* input = nullptr) override;
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length,
      uint8_t** output) override WARN_UNUSED_RESULT;

  virtual std::string file_extension() const override { return "gz"; }

 private:
  Format format_;

  /// Structure used to communicate with the library.
  z_stream stream_;

  /// These are magic numbers from zlib.h.  Not clear why they are not defined there.
  const static int WINDOW_BITS = 15;    // Maximum window size
  const static int GZIP_CODEC = 16;     // Output Gzip.

  /// Compresses 'input' into 'output'.  Output must be preallocated and
  /// at least big enough.
  /// *output_length should be called with the length of the output buffer and on return
  /// is the length of the output.
  Status Compress(int64_t input_length, const uint8_t* input, int64_t* output_length,
      uint8_t* output) WARN_UNUSED_RESULT;
};

class BzipCompressor : public Codec {
 public:
  BzipCompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~BzipCompressor() { }

  virtual int64_t MaxOutputLen(
      int64_t input_len, const uint8_t* input = nullptr) override;
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length,
      uint8_t** output) override WARN_UNUSED_RESULT;
  virtual std::string file_extension() const override { return "bz2"; }
};

class SnappyBlockCompressor : public Codec {
 public:
  SnappyBlockCompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~SnappyBlockCompressor() { }

  virtual int64_t MaxOutputLen(
      int64_t input_len, const uint8_t* input = nullptr) override;
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length,
      uint8_t** output) override WARN_UNUSED_RESULT;
  virtual std::string file_extension() const override { return "snappy"; }
};

class SnappyCompressor : public Codec {
 public:
  SnappyCompressor(MemPool* mem_pool = nullptr, bool reuse_buffer = false);
  virtual ~SnappyCompressor() { }

  virtual int64_t MaxOutputLen(
      int64_t input_len, const uint8_t* input = nullptr) override;
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length,
      uint8_t** output) override WARN_UNUSED_RESULT;
  virtual std::string file_extension() const override { return "snappy"; }

  /// Computes the crc checksum that snappy expects when used in a framing format.
  /// This checksum needs to come after the compressed data.
  /// http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt
  static uint32_t ComputeChecksum(int64_t input_len, const uint8_t* input);
};

/// Lz4 is a compression codec with similar compression ratios as snappy but much faster
/// decompression. This compressor is not able to compress unless the output buffer is
/// allocated and will cause an error if asked to do so.
class Lz4Compressor : public Codec {
 public:
  Lz4Compressor(MemPool* mem_pool = nullptr, bool reuse_buffer = false);
  virtual ~Lz4Compressor() { }

  virtual int64_t MaxOutputLen(
      int64_t input_len, const uint8_t* input = nullptr) override;
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length,
      uint8_t** output) override WARN_UNUSED_RESULT;
  virtual std::string file_extension() const override { return "lz4"; }
};

/// ZStandard compression codec.
class ZstandardCompressor : public Codec {
 public:
  ZstandardCompressor(MemPool* mem_pool = nullptr, bool reuse_buffer = false,
      int clevel = ZSTD_CLEVEL_DEFAULT);
  virtual ~ZstandardCompressor();

  virtual int64_t MaxOutputLen(
      int64_t input_len, const uint8_t* input = nullptr) override;
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length,
      uint8_t** output) override WARN_UNUSED_RESULT;
  virtual std::string file_extension() const override { return "zst"; }

 private:
  int clevel_;
  ZSTD_CCtx* stream_ = nullptr;
};

/// Hadoop's block compression scheme on top of LZ4.
class Lz4BlockCompressor : public Codec {
 public:
  Lz4BlockCompressor(MemPool* mem_pool = nullptr, bool reuse_buffer = false);
  virtual ~Lz4BlockCompressor() { }

  virtual int64_t MaxOutputLen(
      int64_t input_len, const uint8_t* input = nullptr) override;
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length,
      uint8_t** output) override WARN_UNUSED_RESULT;
  virtual std::string file_extension() const override { return "lz4"; }
};
}
