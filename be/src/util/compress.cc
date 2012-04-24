// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "util/decompress.h"
#include "util/compress.h"
#include "exec/serde-utils.h"
#include "runtime/runtime-state.h"

// Compression libraries
#include <zlib.h>
#include <bzlib.h>
#include <snappy.h>

using namespace std;
using namespace boost;
using namespace impala;

Status Compressor::CreateCompressor(RuntimeState* runtime_state, MemPool* mem_pool,
                                    bool reuse, const vector<char>& codec,
                                    scoped_ptr<Compressor>* compressor) {
  bool is_gzip = false;
  if (strncmp(&codec[0], Decompressor::DEFAULT_COMPRESSION, codec.size()) == 0) {
    compressor->reset(new GzipCompressor(mem_pool, reuse));
  } else if (strncmp(&codec[0], Decompressor::GZIP_COMPRESSION, codec.size()) == 0) {
    compressor->reset(new GzipCompressor(mem_pool, reuse));
    is_gzip = true;
  } else if (strncmp(&codec[0], Decompressor::BZIP2_COMPRESSION, codec.size()) == 0) {
    compressor->reset(new BzipCompressor(mem_pool, reuse));
  } else if (strncmp(&codec[0], Decompressor::SNAPPY_COMPRESSION, codec.size()) == 0) {
    compressor->reset(new SnappyCompressor(mem_pool, reuse));
  } else {
    if (runtime_state != NULL && runtime_state->LogHasSpace()) {
      runtime_state->error_stream() << "Unknown Codec: " 
         << string(&codec[0], codec.size());
    }
    return Status("Unknown Codec");
  }

  RETURN_IF_ERROR(compressor->get()->Init(is_gzip));
  return Status::OK;
}

Compressor::Compressor(MemPool* mem_pool, bool reuse_buffer) 
  : memory_pool_(mem_pool),
    reuse_buffer_(reuse_buffer),
    out_buffer_(NULL),
    buffer_length_(0) {
}

GzipCompressor::GzipCompressor(MemPool* mem_pool, bool reuse_buffer)
  : Compressor(mem_pool, reuse_buffer) {
  bzero(&stream_, sizeof(stream_));
}

GzipCompressor::~GzipCompressor() {
  (void)deflateEnd(&stream_);
}

Status GzipCompressor::Init(bool is_gzip) {
  int ret;
  // Initialize to run either zlib or gzib deflate. 
  if ((ret = deflateInit2(&stream_, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                          WINDOW_BITS + (is_gzip ? GZIP_CODEC : 0),
                          9, Z_DEFAULT_STRATEGY )) != Z_OK) {
    return Status("zlib deflateInit failed: " +  string(stream_.msg));
  }

  return Status::OK;
}

Status GzipCompressor::ProcessBlock(int input_length, uint8_t* input,
                                    int* output_length, uint8_t** output) {
  // If length is set then the output has been allocated.
  if (*output_length != 0) {
    buffer_length_ = *output_length;
    out_buffer_ = *output;
  } else if (!reuse_buffer_ || out_buffer_ == NULL) {
    buffer_length_ = deflateBound(&stream_, input_length);
    out_buffer_ = memory_pool_->Allocate(buffer_length_);
  }

  stream_.next_in = reinterpret_cast<Bytef*>(input);
  stream_.avail_in = input_length;
  stream_.next_out = reinterpret_cast<Bytef*>(out_buffer_);
  stream_.avail_out = buffer_length_;

  int ret = 0;
  if ((ret = deflate(&stream_, Z_FINISH)) != Z_STREAM_END) {
    return Status("zlib deflate failed: " + string(stream_.msg));
  }
  if (deflateReset(&stream_) != Z_OK) {
    return Status("zlib deflateReset failed: " + string(stream_.msg));
  }

  *output = out_buffer_;
  *output_length = buffer_length_ - stream_.avail_out;
  return Status::OK;
}

BzipCompressor::BzipCompressor(MemPool* mem_pool, bool reuse_buffer)
  : Compressor(mem_pool, reuse_buffer) {
}

Status BzipCompressor::ProcessBlock(int input_length, uint8_t* input,
                                    int *output_length, uint8_t** output) {
  // If length is set then the output has been allocated.
  if (*output_length != 0) {
    buffer_length_ = *output_length;
    out_buffer_ = *output;
  } else if (!reuse_buffer_ || out_buffer_ == NULL) {
    if (temp_memory_pool_.get() == NULL) {
      temp_memory_pool_.reset(new MemPool);
    }
    // guess that we will need no more the input length.
    buffer_length_ = input_length;
    out_buffer_ = temp_memory_pool_->Allocate(buffer_length_);
  }

  unsigned int outlen;
  int ret = BZ_OUTBUFF_FULL;
  while (ret == BZ_OUTBUFF_FULL) {
    if (out_buffer_ == NULL) {
      DCHECK_EQ(*output_length, 0);
      temp_memory_pool_->Clear();
      buffer_length_ = buffer_length_ * 2;
      out_buffer_ = temp_memory_pool_->Allocate(buffer_length_);
    }
    outlen = static_cast<unsigned int>(buffer_length_);
    if ((ret = BZ2_bzBuffToBuffCompress(reinterpret_cast<char*>(out_buffer_), &outlen,
        reinterpret_cast<char*>(input),
        static_cast<unsigned int>(input_length), 5, 2, 0)) == BZ_OUTBUFF_FULL) {
      // If the output_length was passed we must have enough room.
      DCHECK_EQ(*output_length, 0);
      if (*output_length != 0) {
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

SnappyCompressor::SnappyCompressor(MemPool* mem_pool, bool reuse_buffer)
  : Compressor(mem_pool, reuse_buffer) {
}

Status SnappyCompressor::ProcessBlock(int input_length, uint8_t* input,
                                      int *output_length, uint8_t** output) {

  // Hadoop uses a block compression scheme on top of snappy.  First there is
  // an integer which is the size of the decompressed data followed by a
  // sequence of compressed blocks each preceeded with an integer size.
  // For testing purposes we are going to generate two blocks.
  int block_size = input_length / 2;
  size_t length = snappy::MaxCompressedLength(block_size) * 2;
  length += 3 * sizeof (int32_t);
  DCHECK(*output_length == 0 || length == *output_length);

  // If length is non-zero then the output has been allocated.
  if (*output_length != 0) {
    buffer_length_ = *output_length;
    out_buffer_ = *output;
  } else if (!reuse_buffer_ || out_buffer_ == NULL || buffer_length_ < length) {
    buffer_length_ = length;
    out_buffer_ = memory_pool_->Allocate(buffer_length_);
  }

  uint8_t* outp = out_buffer_;
  uint8_t* sizep;
  SerDeUtils::PutInt(outp, input_length);
  outp += sizeof (int32_t);
  do {
    // Point at the spot to store the compressed size.
    sizep = outp;
    outp += sizeof (int32_t);
    size_t size;
    snappy::RawCompress(reinterpret_cast<const char*>(input),
        static_cast<size_t>(block_size), reinterpret_cast<char*>(outp), &size);

    SerDeUtils::PutInt(sizep, size);
    input += block_size;
    input_length -= block_size;
    outp += size;
  } while (input_length > 0);

  *output = out_buffer_;
  *output_length = outp - out_buffer_;
  return Status::OK;
}
