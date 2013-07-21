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

#include "util/compress.h"
#include "exec/read-write-util.h"
#include "runtime/runtime-state.h"

// Codec libraries
#include <zlib.h>
#include <bzlib.h>
#include <snappy.h>

using namespace std;
using namespace boost;
using namespace impala;

GzipCompressor::GzipCompressor(Format format, MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer),
    format_(format) {
  bzero(&stream_, sizeof(stream_));
}

GzipCompressor::~GzipCompressor() {
  (void)deflateEnd(&stream_);
}

Status GzipCompressor::Init() {
  int ret;
  // Initialize to run specified format
  int window_bits = WINDOW_BITS;
  if (format_ == DEFLATE) {
    window_bits = -window_bits;
  } else if (format_ == GZIP) {
    window_bits += GZIP_CODEC;
  }
  if ((ret = deflateInit2(&stream_, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                          window_bits, 9, Z_DEFAULT_STRATEGY )) != Z_OK) {
    return Status("zlib deflateInit failed: " +  string(stream_.msg));
  }

  return Status::OK;
}

int GzipCompressor::MaxOutputLen(int input_len, const uint8_t* input) {
#if !defined ZLIB_VERNUM || ZLIB_VERNUM <= 0x1230
  if (UNLIKELY(input_len == 0 && format_ == GZIP)) {
    // zlib version 1.2.3 has a bug in deflateBound() that causes it to return too low a
    // bound for this case. Hardcode the value returned in zlib version 1.2.3.1+.
    return 23;
  }
#endif
  return deflateBound(&stream_, input_len);
}

Status GzipCompressor::Compress(int input_length, uint8_t* input,
    int* output_length, uint8_t* output) {
  DCHECK_GE(*output_length, MaxOutputLen(input_length));
  stream_.next_in = reinterpret_cast<Bytef*>(input);
  stream_.avail_in = input_length;
  stream_.next_out = reinterpret_cast<Bytef*>(output);
  stream_.avail_out = *output_length;

  int ret = 0;
  if ((ret = deflate(&stream_, Z_FINISH)) != Z_STREAM_END) {
    stringstream ss;
    ss << "zlib deflate failed: " << stream_.msg;
    return Status(ss.str());
  }

  *output_length = *output_length - stream_.avail_out;

  if (deflateReset(&stream_) != Z_OK) {
    return Status("zlib deflateReset failed: " + string(stream_.msg));
  }
  return Status::OK;
}

Status GzipCompressor::ProcessBlock(bool output_preallocated,
                                    int input_length, uint8_t* input,
                                    int* output_length, uint8_t** output) {
  int max_compressed_len = MaxOutputLen(input_length);
  if (!output_preallocated) {
    if (!reuse_buffer_ || buffer_length_ < max_compressed_len || out_buffer_ == NULL) {
      DCHECK(memory_pool_ != NULL) << "Can't allocate without passing in a mem pool";
      buffer_length_ = max_compressed_len;
      out_buffer_ = memory_pool_->Allocate(buffer_length_);
      *output_length = buffer_length_;
    }
    *output = out_buffer_;
  } else if (*output_length < max_compressed_len) {
    return Status("GzipCompressor::ProcessBlock: output length too small");
  }

  RETURN_IF_ERROR(Compress(input_length, input, output_length, *output));
  return Status::OK;
}

BzipCompressor::BzipCompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer) {
}

int BzipCompressor::MaxOutputLen(int input_len, const uint8_t* input) {
  // TODO: is it possible to get a bound with bzip.
  return -1;
}

Status BzipCompressor::ProcessBlock(bool output_preallocated,
                                    int input_length, uint8_t* input,
                                    int *output_length, uint8_t** output) {
  // The bz2 library does not allow input to be NULL, even when input_length is 0. This
  // should be OK because we do not write any file formats that support bzip compression.
  DCHECK(input != NULL);

  // If length is set then the output has been allocated.
  if (output_preallocated) {
    buffer_length_ = *output_length;
    out_buffer_ = *output;
  } else if (!reuse_buffer_ || out_buffer_ == NULL) {
    // guess that we will need no more the input length.
    buffer_length_ = input_length;
    out_buffer_ = temp_memory_pool_->Allocate(buffer_length_);
  }

  unsigned int outlen;
  int ret = BZ_OUTBUFF_FULL;
  while (ret == BZ_OUTBUFF_FULL) {
    if (out_buffer_ == NULL) {
      DCHECK(!output_preallocated);
      temp_memory_pool_->Clear();
      buffer_length_ = buffer_length_ * 2;
      out_buffer_ = temp_memory_pool_->Allocate(buffer_length_);
    }
    outlen = static_cast<unsigned int>(buffer_length_);
    if ((ret = BZ2_bzBuffToBuffCompress(reinterpret_cast<char*>(out_buffer_), &outlen,
        reinterpret_cast<char*>(input),
        static_cast<unsigned int>(input_length), 5, 2, 0)) == BZ_OUTBUFF_FULL) {
      if (output_preallocated) {
        return Status("Too small buffer passed to BzipCompressor");
      }
      out_buffer_ = NULL;
    }
  }
  if (ret !=  BZ_OK) {
    stringstream ss;
    ss << "bzlib BZ2_bzBuffToBuffCompressor failed: " << ret;
    return Status(ss.str());

  }

  *output = out_buffer_;
  *output_length = outlen;
  memory_pool_->AcquireData(temp_memory_pool_.get(), false);
  return Status::OK;
}

// Currently this is only use for testing of the decompressor.
SnappyBlockCompressor::SnappyBlockCompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer) {
}

int SnappyBlockCompressor::MaxOutputLen(int input_len, const uint8_t* input) {
  // TODO: is it possible to get a bound on this?
  return -1;
}

Status SnappyBlockCompressor::ProcessBlock(bool output_preallocated,
                                           int input_length, uint8_t* input,
                                           int *output_length, uint8_t** output) {
  // Hadoop uses a block compression scheme on top of snappy.  First there is
  // an integer which is the size of the decompressed data followed by a
  // sequence of compressed blocks each preceded with an integer size.
  // For testing purposes we are going to generate two blocks.
  int block_size = input_length / 2;
  size_t length = snappy::MaxCompressedLength(block_size) * 2;
  length += 3 * sizeof (int32_t);
  DCHECK(!output_preallocated || length <= *output_length);

  if (output_preallocated) {
    buffer_length_ = *output_length;
    out_buffer_ = *output;
  } else if (!reuse_buffer_ || out_buffer_ == NULL || buffer_length_ < length) {
    buffer_length_ = length;
    out_buffer_ = memory_pool_->Allocate(buffer_length_);
  }

  uint8_t* outp = out_buffer_;
  uint8_t* sizep;
  ReadWriteUtil::PutInt(outp, static_cast<uint32_t>(input_length));
  outp += sizeof (int32_t);
  while (input_length > 0) {
    // TODO: should this be a while or a do-while loop? Check what Hadoop does.
    // Point at the spot to store the compressed size.
    sizep = outp;
    outp += sizeof (int32_t);
    size_t size;
    snappy::RawCompress(reinterpret_cast<const char*>(input),
        static_cast<size_t>(block_size), reinterpret_cast<char*>(outp), &size);

    ReadWriteUtil::PutInt(sizep, static_cast<uint32_t>(size));
    input += block_size;
    input_length -= block_size;
    outp += size;
  }

  *output = out_buffer_;
  *output_length = outp - out_buffer_;
  return Status::OK;
}

SnappyCompressor::SnappyCompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer) {
}

int SnappyCompressor::MaxOutputLen(int input_len, const uint8_t* input) {
  return snappy::MaxCompressedLength(input_len);
}

Status SnappyCompressor::ProcessBlock(bool output_preallocated,
                                      int input_length, uint8_t* input,
                                      int* output_length, uint8_t** output) {
  int max_compressed_len = MaxOutputLen(input_length);
  if (output_preallocated && *output_length < max_compressed_len) {
    return Status("SnappyCompressor::ProcessBlock: output length too small");
  }

  if (!output_preallocated) {
      if ((!reuse_buffer_ || buffer_length_ < max_compressed_len)) {
        DCHECK(memory_pool_ != NULL) << "Can't allocate without passing in a mem pool";
        buffer_length_ = max_compressed_len;
        out_buffer_ = memory_pool_->Allocate(buffer_length_);
      }
    *output = out_buffer_;
  }

  size_t out_len;
  snappy::RawCompress(reinterpret_cast<const char*>(input),
      static_cast<size_t>(input_length),
      reinterpret_cast<char*>(*output), &out_len);
  *output_length = out_len;
  return Status::OK;
}
