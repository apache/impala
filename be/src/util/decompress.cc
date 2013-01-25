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

#include <boost/assign/list_of.hpp>
#include "util/decompress.h"
#include "exec/serde-utils.inline.h"
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
    if (buffer_length_ > MAX_BLOCK_SIZE) {
      return Status("Decompressor: block size is too big");
    }
    out_buffer_ = temp_memory_pool_.Allocate(buffer_length_);
    use_temp = true;
  }

  int ret = 0;
  while (ret != Z_STREAM_END) {
    stream_.next_in = reinterpret_cast<Bytef*>(input);
    stream_.avail_in = input_length;
    stream_.next_out = reinterpret_cast<Bytef*>(out_buffer_);
    stream_.avail_out = buffer_length_;

    ret = inflate(&stream_, 1);

    if (ret != Z_STREAM_END) {
      if (ret == Z_OK) {
        // Not enough output space.
        DCHECK_EQ(*output_length, 0);
        if (*output_length != 0) {
          return Status("Too small a buffer passed to GzipDecompressor");
        }
        temp_memory_pool_.Clear();
        buffer_length_ *= 2;
        if (buffer_length_ > MAX_BLOCK_SIZE) {
          return Status("Decompressor: block size is too big");
        }
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
  // stream_.avail_out is the number of bytes *left* in the out buffer, but
  // we're interested in the number of bytes used.
  if (*output_length == 0) *output_length = buffer_length_ - stream_.avail_out;
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
    if (buffer_length_ > MAX_BLOCK_SIZE) {
      return Status("Decompressor: block size is too big");
    }
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
      if (buffer_length_ > MAX_BLOCK_SIZE) {
        return Status("Decompressor: block size is too big");
      }
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
      if (buffer_length_ > MAX_BLOCK_SIZE) {
        return Status("Decompressor: block size is too big");
      }
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

// Hadoop uses a block compression scheme on top of snappy.  As per the hadoop docs
// the input is split into blocks.  Each block "contains the uncompressed length for
// the block followed by one of more length-prefixed blocks of compressed data."
// This is essentially blocks of blocks.  
// The outer block consists of:
//   - 4 byte little endian uncompressed_size
//   < inner blocks >
//   ... repeated until input_len is consumed ..
// The inner blocks have:
//   - 4-byte little endian compressed_size
//   < snappy compressed block >
//   - 4-byte little endian compressed_size
//   < snappy compressed block >
//   ... repeated until uncompressed_size from outer block is consumed ...

// Utility function to decompress snappy block compressed data.  If size_only is true,
// this function does not decompress but only computes the output size and writes 
// the result to *output_len.  
// If size_only is false, output must be preallocated to output_len and this needs to 
// be exactly big enough to hold the decompressed output.
// size_only is a O(1) operations (just reads a single varint for each snappy block).
static Status SnappyBlockDecompress(int input_len, uint8_t* input, bool size_only,
    int* output_len, char* output) {
  
  int uncompressed_total_len = 0;
  while (input_len > 0) {
    size_t uncompressed_block_len = SerDeUtils::GetInt(input);
    input += sizeof(int32_t);
    input_len -= sizeof(int32_t);

    if (uncompressed_block_len > Codec::MAX_BLOCK_SIZE || uncompressed_block_len == 0) {
      if (uncompressed_total_len == 0) {
        return Status("Decompressor: block size is too big.  Data is likely corrupt.");
      }
      break;
    }

    if (!size_only) {
      int remaining_output_size = *output_len - uncompressed_total_len;
      DCHECK_GE(remaining_output_size, uncompressed_block_len);
    }

    while (uncompressed_block_len > 0) {
      // Read the length of the next snappy compressed block.
      size_t compressed_len = SerDeUtils::GetInt(input);
      input += sizeof(int32_t);
      input_len -= sizeof(int32_t);

      if (compressed_len == 0 || compressed_len > input_len) {
        if (uncompressed_total_len == 0) {
          return Status(
              "Decompressor: invalid compressed length.  Data is likely corrupt.");
        }
        input_len =0;
        break;
      }
    
      // Read how big the output will be.
      size_t uncompressed_len;
      if (!snappy::GetUncompressedLength(reinterpret_cast<char*>(input), 
            input_len, &uncompressed_len)) {
        if (uncompressed_total_len == 0) {
          return Status("Snappy: GetUncompressedLength failed");
        }
        input_len =0;
        break;
      }
      DCHECK_GT(uncompressed_len, 0);
    
      if (!size_only) {
        // Decompress this snappy block
        if (!snappy::RawUncompress(reinterpret_cast<char*>(input), 
              compressed_len, output)) {
          return Status("Snappy: RawUncompress failed");
        }
        output += uncompressed_len;
      }
    
      input += compressed_len;
      input_len -= compressed_len;
      uncompressed_block_len -= uncompressed_len;
      uncompressed_total_len += uncompressed_len;
    }
  }

  if (size_only) {
    *output_len = uncompressed_total_len;
  } else if (*output_len != uncompressed_total_len) {
    return Status("Snappy: Decompressed size is not correct.");
  }
  return Status::OK;
}

Status SnappyBlockDecompressor::ProcessBlock(int input_len, uint8_t* input,
    int* output_len, uint8_t** output) {
  if (*output_len == 0) {
    // If we don't know the size beforehand, compute it.
    RETURN_IF_ERROR(SnappyBlockDecompress(input_len, input, true, output_len, NULL));
    DCHECK_NE(*output_len, 0);
    
    if (!reuse_buffer_ || out_buffer_ == NULL || buffer_length_ < *output_len) {
      // Need to allocate a new buffer
      buffer_length_ = *output_len;
      out_buffer_ = memory_pool_->Allocate(buffer_length_);
    } 
    *output = out_buffer_;
  }
  DCHECK(*output != NULL);
  
  if (*output_len > MAX_BLOCK_SIZE) {
    // TODO: is this check really robust?
    return Status("Decompressor: block size is too big.  Data is likely corrupt.");
  }
    
  char* out_ptr = reinterpret_cast<char*>(*output);
  RETURN_IF_ERROR(SnappyBlockDecompress(input_len, input, false, output_len, out_ptr));
  return Status::OK;
}
