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

#include "util/codec.h"

#include <ostream>
#include <utility>

#include <boost/algorithm/string.hpp>
#include <zstd.h>

#include "common/compiler-util.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem-pool.h"
#include "util/bit-util.h"
#include "util/compress.h"
#include "util/decompress.h"

#include "gen-cpp/CatalogObjects_constants.h"

#include "common/names.h"

using namespace impala;
using namespace strings;

const char* const Codec::DEFAULT_COMPRESSION =
    "org.apache.hadoop.io.compress.DefaultCodec";
// An alias for DefaultCodec
const char* const Codec::DEFLATE_COMPRESSION =
    "org.apache.hadoop.io.compress.DeflateCodec";
const char* const Codec::GZIP_COMPRESSION = "org.apache.hadoop.io.compress.GzipCodec";
const char* const Codec::BZIP2_COMPRESSION = "org.apache.hadoop.io.compress.BZip2Codec";
const char* const Codec::SNAPPY_COMPRESSION = "org.apache.hadoop.io.compress.SnappyCodec";
const char* const Codec::LZ4_COMPRESSION = "org.apache.hadoop.io.compress.Lz4Codec";
const char* const Codec::ZSTD_COMPRESSION =
    "org.apache.hadoop.io.compress.ZStandardCodec";
const char* const Codec::UNKNOWN_CODEC_ERROR =
    "This compression codec is currently unsupported: ";
const char* const NO_LZO_MSG = "LZO codecs may not be created via the Codec interface. "
    "Instead LZO is decoded by an optional text scanner plugin.";

const Codec::CodecMap Codec::CODEC_MAP = {{"", THdfsCompression::NONE},
    {DEFAULT_COMPRESSION, THdfsCompression::DEFAULT},
    {DEFLATE_COMPRESSION, THdfsCompression::DEFAULT},
    {GZIP_COMPRESSION, THdfsCompression::GZIP},
    {BZIP2_COMPRESSION, THdfsCompression::BZIP2},
    {SNAPPY_COMPRESSION, THdfsCompression::SNAPPY_BLOCKED},
    {LZ4_COMPRESSION, THdfsCompression::LZ4_BLOCKED},
    {ZSTD_COMPRESSION, THdfsCompression::ZSTD}};

string Codec::GetCodecName(THdfsCompression::type type) {
  return boost::algorithm::to_lower_copy(
      string(_THdfsCompression_VALUES_TO_NAMES.find(type)->second));
}

Status Codec::GetHadoopCodecClassName(THdfsCompression::type type, string* out_name) {
  for (const CodecMap::value_type& codec: CODEC_MAP) {
    if (codec.second == type) {
      out_name->assign(codec.first);
      return Status::OK();
    }
  }
  return Status(Substitute("Unsupported codec for given file type: $0",
      _THdfsCompression_VALUES_TO_NAMES.find(type)->second));
}

Codec::~Codec() {}

Status Codec::CreateCompressor(MemPool* mem_pool, bool reuse, const string& codec,
    scoped_ptr<Codec>* compressor) {
  CodecMap::const_iterator type = CODEC_MAP.find(codec);
  if (type == CODEC_MAP.end()) {
    return Status(Substitute("$0$1", UNKNOWN_CODEC_ERROR, codec));
  }

  CodecInfo codec_info(
      type->second, (type->second == THdfsCompression::ZSTD) ? ZSTD_CLEVEL_DEFAULT : 0);
  RETURN_IF_ERROR(CreateCompressor(mem_pool, reuse, codec_info, compressor));
  return Status::OK();
}

Status Codec::CreateCompressor(MemPool* mem_pool, bool reuse, const CodecInfo& codec_info,
    scoped_ptr<Codec>* compressor) {
  THdfsCompression::type format = codec_info.format_;
  switch (format) {
    case THdfsCompression::NONE:
      compressor->reset(nullptr);
      return Status::OK();
    case THdfsCompression::GZIP:
      compressor->reset(new GzipCompressor(GzipCompressor::GZIP, mem_pool, reuse));
      break;
    case THdfsCompression::DEFAULT:
      compressor->reset(new GzipCompressor(GzipCompressor::ZLIB, mem_pool, reuse));
      break;
    case THdfsCompression::DEFLATE:
      compressor->reset(new GzipCompressor(GzipCompressor::DEFLATE, mem_pool, reuse));
      break;
    case THdfsCompression::BZIP2:
      compressor->reset(new BzipCompressor(mem_pool, reuse));
      break;
    case THdfsCompression::SNAPPY_BLOCKED:
      compressor->reset(new SnappyBlockCompressor(mem_pool, reuse));
      break;
    case THdfsCompression::SNAPPY:
      compressor->reset(new SnappyCompressor(mem_pool, reuse));
      break;
    case THdfsCompression::LZ4:
      compressor->reset(new Lz4Compressor(mem_pool, reuse));
      break;
    case THdfsCompression::ZSTD:
      compressor->reset(new ZstandardCompressor(mem_pool, reuse,
          codec_info.compression_level_));
      break;
    case THdfsCompression::LZ4_BLOCKED:
      compressor->reset(new Lz4BlockCompressor(mem_pool, reuse));
      break;
    default: {
      if (format == THdfsCompression::LZO) return Status(NO_LZO_MSG);
      return Status(Substitute("Unsupported codec: $0", format));
    }
  }

  return (*compressor)->Init();
}

Status Codec::CreateDecompressor(MemPool* mem_pool, bool reuse, const string& codec,
    scoped_ptr<Codec>* decompressor) {
  CodecMap::const_iterator type = CODEC_MAP.find(codec);
  if (type == CODEC_MAP.end()) {
    return Status(Substitute("$0$1", UNKNOWN_CODEC_ERROR, codec));
  }

  RETURN_IF_ERROR(
      CreateDecompressor(mem_pool, reuse, type->second, decompressor));
  return Status::OK();
}

Status Codec::CreateDecompressor(MemPool* mem_pool, bool reuse,
    THdfsCompression::type format, scoped_ptr<Codec>* decompressor) {
  switch (format) {
    case THdfsCompression::NONE:
      decompressor->reset(nullptr);
      return Status::OK();
    case THdfsCompression::DEFAULT:
    case THdfsCompression::GZIP:
      decompressor->reset(new GzipDecompressor(mem_pool, reuse, false));
      break;
    case THdfsCompression::DEFLATE:
      decompressor->reset(new GzipDecompressor(mem_pool, reuse, true));
      break;
    case THdfsCompression::BZIP2:
      decompressor->reset(new BzipDecompressor(mem_pool, reuse));
      break;
    case THdfsCompression::SNAPPY_BLOCKED:
      decompressor->reset(new SnappyBlockDecompressor(mem_pool, reuse));
      break;
    case THdfsCompression::SNAPPY:
      decompressor->reset(new SnappyDecompressor(mem_pool, reuse));
      break;
    case THdfsCompression::LZ4:
      decompressor->reset(new Lz4Decompressor(mem_pool, reuse));
      break;
    case THdfsCompression::ZSTD:
      decompressor->reset(new ZstandardDecompressor(mem_pool, reuse));
      break;
    case THdfsCompression::LZ4_BLOCKED:
      decompressor->reset(new Lz4BlockDecompressor(mem_pool, reuse));
      break;
    default: {
      if (format == THdfsCompression::LZO) return Status(NO_LZO_MSG);
      return Status(Substitute("Unsupported codec: $0", format));
    }
  }

  return (*decompressor)->Init();
}

Codec::Codec(MemPool* mem_pool, bool reuse_buffer, bool supports_streaming)
  : memory_pool_(mem_pool),
    reuse_buffer_(reuse_buffer),
    supports_streaming_(supports_streaming) {
  if (memory_pool_ != nullptr) {
    temp_memory_pool_.reset(new MemPool(memory_pool_->mem_tracker()));
  }
}

void Codec::Close() {
  if (temp_memory_pool_.get() != nullptr) {
    DCHECK(memory_pool_ != nullptr);
    memory_pool_->AcquireData(temp_memory_pool_.get(), false);
  }
}

Status Codec::ProcessBlock32(bool output_preallocated, int input_length,
    const uint8_t* input, int* output_length, uint8_t** output) {
  int64_t input_len64 = input_length;
  int64_t output_len64 = *output_length;
  RETURN_IF_ERROR(
      ProcessBlock(output_preallocated, input_len64, input, &output_len64, output));
  // Buffer size should be between [0, (2^31 - 1)] bytes.
  if (UNLIKELY(!BitUtil::IsNonNegative32Bit(output_len64))) {
    return Status(Substitute("Arithmetic overflow in codec function. Output length is $0",
        output_len64));;
  }
  *output_length = static_cast<int>(output_len64);
  return Status::OK();
}
