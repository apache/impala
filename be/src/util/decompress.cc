// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <boost/assign/list_of.hpp>
#include "util/decompress.h"
#include "exec/serde-utils.h"
#include "runtime/runtime-state.h"
#include "gen-cpp/Descriptors_types.h"

// Codec libraries
#include <zlib.h>
#include <bzlib.h>
#include <snappy.h>

using namespace std;
using namespace boost;
using namespace impala;

GzipDecompressor::GzipDecompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer) {
  bzero(&stream_, sizeof(stream_));
}

GzipDecompressor::~GzipDecompressor() {
  (void)inflateEnd(&stream_);
}

Status GzipDecompressor::Init() {
  int ret;
  // Initialize to run either zlib or gzib inflate.
  if ((ret = inflateInit2(&stream_, WINDOW_BITS | DETECT_CODEC)) != Z_OK) {
    return Status("zlib inflateInit failed: " +  string(stream_.msg));
  }

  return Status::OK;
}

Status GzipDecompressor::ProcessBlock(int input_length, uint8_t* input,
                                      int* output_length, uint8_t** output) {
  bool use_temp = false;
  // If length is set then the output has been allocated.
  if (*output_length != 0) {
    buffer_length_ = *output_length;
    out_buffer_ = *output;
  } else if (!reuse_buffer_ || out_buffer_ == NULL) {
    // guess that we will need 2x the input length.
    buffer_length_ = input_length * 2;
    out_buffer_ = temp_memory_pool_.Allocate(buffer_length_);
    use_temp = true;
  }

  int ret = 0;
  while (ret != Z_STREAM_END) {
    stream_.next_in = reinterpret_cast<Bytef*>(input);
    stream_.avail_in = input_length;
    stream_.next_out = reinterpret_cast<Bytef*>(out_buffer_);
    stream_.avail_out = buffer_length_;

    if ((ret = inflate(&stream_, 1)) != Z_STREAM_END) {
      if (ret == Z_OK) {
        // Not enough output space.
        DCHECK_EQ(*output_length, 0);
        if (*output_length != 0) {
          return Status("Too small a buffer passed to GzipDecompressor");
        }
        temp_memory_pool_.Clear();
        buffer_length_ *= 2;
        out_buffer_ = temp_memory_pool_.Allocate(buffer_length_);
        if (inflateReset(&stream_) != Z_OK) {
          return Status("zlib inflateEnd failed: " + string(stream_.msg));
        }
        continue;
      }
      return Status("zlib inflate failed: " + string(stream_.msg));
    }
  }
  if (inflateReset(&stream_) != Z_OK) {
    return Status("zlib inflateEnd failed: " + string(stream_.msg));
  }

  *output = out_buffer_;
  if (*output_length == 0) *output_length = stream_.avail_out;
  if (use_temp) memory_pool_->AcquireData(&temp_memory_pool_, reuse_buffer_);
  return Status::OK;
}

BzipDecompressor::BzipDecompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer) {
}

Status BzipDecompressor::ProcessBlock(int input_length, uint8_t* input,
                                      int* output_length, uint8_t** output) {
  bool use_temp = false;
  // If length is set then the output has been allocated.
  if (*output_length != 0) {
    buffer_length_ = *output_length;
    out_buffer_ = *output;
  } else if (!reuse_buffer_ || out_buffer_ == NULL) {
    // guess that we will need 2x the input length.
    buffer_length_ = input_length * 2;
    out_buffer_ = temp_memory_pool_.Allocate(buffer_length_);
    use_temp = true;
  }

  int ret = BZ_OUTBUFF_FULL;
  unsigned int outlen;
  while (ret == BZ_OUTBUFF_FULL) {
    if (out_buffer_ == NULL) {
      DCHECK_EQ(*output_length, 0);
      temp_memory_pool_.Clear();
      buffer_length_ = buffer_length_ * 2;
      out_buffer_ = temp_memory_pool_.Allocate(buffer_length_);
    }
    outlen = static_cast<unsigned int>(buffer_length_);
    if ((ret = BZ2_bzBuffToBuffDecompress(reinterpret_cast<char*>(out_buffer_), &outlen,
        reinterpret_cast<char*>(input),
        static_cast<unsigned int>(input_length), 0, 0)) == BZ_OUTBUFF_FULL) {
      // If the output_length was passed we must have enough room.
      DCHECK_EQ(*output_length, 0);
      if (*output_length != 0) {
        return Status("Too small a buffer passed to BzipDecompressor");
      }
      out_buffer_ = NULL;
    }
  }
  if (ret !=  BZ_OK) {
    stringstream ss;
    ss << "bzlib BZ2_bzBuffToBuffDecompressor failed: " << ret;
    return Status(ss.str());

  }

  *output = out_buffer_;
  if (*output_length == 0) *output_length = outlen;
  if (use_temp) memory_pool_->AcquireData(&temp_memory_pool_, reuse_buffer_);
  return Status::OK;
}

SnappyDecompressor::SnappyDecompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer) {
}

Status SnappyDecompressor::ProcessBlock(int input_length, uint8_t* input,
                                        int* output_length, uint8_t** output) {
  // If length is set then the output has been allocated.
  size_t uncompressed_length;
  if (*output_length != 0) {
    buffer_length_ = *output_length;
    out_buffer_ = *output;
  } else {
    // Snappy saves the uncompressed length so we never have to retry.
    if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(input),
        input_length, &uncompressed_length)) {
      return Status("Snappy: GetUncompressedLength failed");
    }
    if (!reuse_buffer_ || out_buffer_ == NULL || buffer_length_ < uncompressed_length) {
      buffer_length_ = uncompressed_length;
      out_buffer_ = memory_pool_->Allocate(buffer_length_);
    }
  }

  if (!snappy::RawUncompress(reinterpret_cast<const char*>(input),
      static_cast<size_t>(input_length), reinterpret_cast<char*>(out_buffer_))) {
    return Status("Snappy: RawUncompress failed");
  }
  if (*output_length == 0) *output_length = uncompressed_length;
  return Status::OK;
}

SnappyBlockDecompressor::SnappyBlockDecompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer) {
}

Status SnappyBlockDecompressor::ProcessBlock(int input_length, uint8_t* input,
                                        int* output_length, uint8_t** output) {
  // Hadoop uses a block compression scheme on top of snappy.  First there is
  // an integer which is the size of the decompressed data followed by a
  // sequence of compressed blocks each preceded with an integer size.
  int32_t length = SerDeUtils::GetInt(input);
  DCHECK(*output_length == 0 || length == *output_length);

  // If length is non-zero then the output has been allocated.
  if (*output_length != 0) {
    buffer_length_ = *output_length;
    out_buffer_ = *output;
  } else if (!reuse_buffer_ || out_buffer_ == NULL || buffer_length_ < length) {
    buffer_length_ = length;
    out_buffer_ = memory_pool_->Allocate(buffer_length_);
  }

  input += sizeof(length);
  input_length -= sizeof(length);

  uint8_t* outp = out_buffer_;
  do {
    // Read the length of the next block.
    length = SerDeUtils::GetInt(input);

    if (length == 0) break;

    input += sizeof(length);
    input_length -= sizeof(length);

    // Read how big the output will be.
    size_t uncompressed_length;
    if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(input),
        input_length, &uncompressed_length)) {
      return Status("Snappy: GetUncompressedLength failed");
    }

    DCHECK_GT(uncompressed_length, 0);
    if (!snappy::RawUncompress(reinterpret_cast<const char*>(input),
        static_cast<size_t>(length), reinterpret_cast<char*>(outp))) {
      return Status("Snappy: RawUncompress failed");
    }
    input += length;
    input_length -= length;
    outp += uncompressed_length;
  } while (input_length > 0);

  *output = out_buffer_;
  if (*output_length == 0) *output_length = outp - out_buffer_;
  return Status::OK;
}
