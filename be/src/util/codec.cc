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

#include "util/codec.h"
#include <boost/assign/list_of.hpp>

#include "util/compress.h"
#include "util/decompress.h"

#include "gen-cpp/Descriptors_types.h"
#include "gen-cpp/Descriptors_constants.h"

using namespace std;
using namespace boost;
using namespace boost::assign;
using namespace impala;

const char* const Codec::DEFAULT_COMPRESSION =
     "org.apache.hadoop.io.compress.DefaultCodec";

const char* const Codec::GZIP_COMPRESSION =
     "org.apache.hadoop.io.compress.GzipCodec";

const char* const Codec::BZIP2_COMPRESSION =
     "org.apache.hadoop.io.compress.BZip2Codec";

const char* const Codec::SNAPPY_COMPRESSION =
     "org.apache.hadoop.io.compress.SnappyCodec";

const Codec::CodecMap Codec::CODEC_MAP = map_list_of
  ("", THdfsCompression::NONE)
  (Codec::DEFAULT_COMPRESSION, THdfsCompression::DEFAULT)
  (Codec::GZIP_COMPRESSION, THdfsCompression::GZIP)
  (Codec::BZIP2_COMPRESSION, THdfsCompression::BZIP2)
  (Codec::SNAPPY_COMPRESSION, THdfsCompression::SNAPPY_BLOCKED);

string Codec::GetCodecName(THdfsCompression::type type) {
  map<const string, THdfsCompression::type>::const_iterator im;
  for (im = g_Descriptors_constants.COMPRESSION_MAP.begin();
       im != g_Descriptors_constants.COMPRESSION_MAP.end(); ++im) {
    if (im->second == type) return im->first;
  }
  DCHECK(im != g_Descriptors_constants.COMPRESSION_MAP.end());
  return "INVALID";
}

Status Codec::CreateCompressor(RuntimeState* runtime_state, MemPool* mem_pool,
                                     bool reuse, const string& codec,
                                     scoped_ptr<Codec>* compressor) {
  map<const string, const THdfsCompression::type>::const_iterator
      type = CODEC_MAP.find(codec);

  if (type == CODEC_MAP.end()) {
    stringstream ss;
    ss << "Unknown Codec: " << codec;
    return Status(ss.str());
  }
  Codec* comp;
  RETURN_IF_ERROR(
      CreateCompressor(runtime_state, mem_pool, reuse, type->second, &comp));
  compressor->reset(comp);
  return Status::OK;
}

Status Codec::CreateCompressor(RuntimeState* runtime_state, MemPool* mem_pool,
                                     bool reuse, THdfsCompression::type format,
                                     Codec** compressor) {
  switch (format) {
    case THdfsCompression::NONE:
      *compressor = NULL;
      return Status::OK;
    case THdfsCompression::GZIP:
      *compressor = new GzipCompressor(mem_pool, reuse, true);
      break;
    case THdfsCompression::DEFAULT:
      *compressor = new GzipCompressor(mem_pool, reuse, false);
      break;
    case THdfsCompression::BZIP2:
      *compressor = new BzipCompressor(mem_pool, reuse);
      break;
    case THdfsCompression::SNAPPY_BLOCKED:
      *compressor = new SnappyBlockCompressor(mem_pool, reuse);
      break;
    case THdfsCompression::SNAPPY:
      *compressor = new SnappyCompressor(mem_pool, reuse);
      break;
  }

  return (*compressor)->Init();
}

Status Codec::CreateDecompressor(RuntimeState* runtime_state, MemPool* mem_pool,
                                       bool reuse, const string& codec,
                                       scoped_ptr<Codec>* decompressor) {
  map<const string, const THdfsCompression::type>::const_iterator
      type = CODEC_MAP.find(codec);

  if (type == CODEC_MAP.end()) {
    stringstream ss;
    ss << "Unknown Codec: " << codec;
    return Status(ss.str());
  }
  Codec* decom;
  RETURN_IF_ERROR(
      CreateDecompressor(runtime_state, mem_pool, reuse, type->second, &decom));
  decompressor->reset(decom);
  return Status::OK;
}

Status Codec::CreateDecompressor(RuntimeState* runtime_state, MemPool* mem_pool,
                                       bool reuse, THdfsCompression::type format,
                                       Codec** decompressor) {
  switch (format) {
    case THdfsCompression::NONE:
      *decompressor = NULL;
      return Status::OK;
    case THdfsCompression::DEFAULT:
    case THdfsCompression::GZIP:
      *decompressor = new GzipDecompressor(mem_pool, reuse);
      break;
    case THdfsCompression::BZIP2:
      *decompressor = new BzipDecompressor(mem_pool, reuse);
      break;
    case THdfsCompression::SNAPPY_BLOCKED:
      *decompressor = new SnappyBlockDecompressor(mem_pool, reuse);
      break;
    case THdfsCompression::SNAPPY:
      *decompressor = new SnappyDecompressor(mem_pool, reuse);
      break;
  }

  return (*decompressor)->Init();
}

Codec::Codec(MemPool* mem_pool, bool reuse_buffer)
  : memory_pool_(mem_pool),
    reuse_buffer_(reuse_buffer),
    out_buffer_(NULL),
    buffer_length_(0) {
}

