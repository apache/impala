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
#include "exec/read-write-util.h"
#include "runtime/runtime-state.h"
#include "common/logging.h"
#include "gen-cpp/Descriptors_types.h"

// Codec libraries
#include <zlib.h>
#include <bzlib.h>
#include <snappy.h>
#include <lz4.h>

using namespace std;
using namespace boost;
using namespace impala;

// Output buffer size for streaming gzip
const int64_t STREAM_GZIP_OUT_BUF_SIZE = 16 * 1024 * 1024;

GzipDecompressor::GzipDecompressor(MemPool* mem_pool, bool reuse_buffer, bool is_deflate)
  : Codec(mem_pool, reuse_buffer),
    is_deflate_(is_deflate) {
  bzero(&stream_, sizeof(stream_));
}

GzipDecompressor::~GzipDecompressor() {
  (void)inflateEnd(&stream_);
}

Status GzipDecompressor::Init() {
  int ret;
  // Initialize to run either deflate or zlib/gzip format
  int window_bits = is_deflate_ ? -WINDOW_BITS : WINDOW_BITS | DETECT_CODEC;
  if ((ret = inflateInit2(&stream_, window_bits)) != Z_OK) {
    return Status("zlib inflateInit failed: " +  string(stream_.msg));
  }

  return Status::OK;
}

int64_t GzipDecompressor::MaxOutputLen(int64_t input_len, const uint8_t* input) {
  return -1;
}

string GzipDecompressor::DebugStreamState() const {
  stringstream ss;
  ss << "next_in=" << (void*)stream_.next_in;
  ss << " avail_in=" << stream_.avail_in;
  ss << " total_in=" << stream_.total_in;
  ss << " next_out=" << (void*)stream_.next_out;
  ss << " avail_out=" << stream_.avail_out;
  ss << " total_out=" << stream_.total_out;
  return ss.str();
}

Status GzipDecompressor::ProcessBlockStreaming(int64_t input_length, const uint8_t* input,
    int64_t* input_bytes_read, int64_t* output_length, uint8_t** output, bool* eos) {
  if (!reuse_buffer_ || out_buffer_ == NULL) {
    buffer_length_ = STREAM_GZIP_OUT_BUF_SIZE;
    out_buffer_ = memory_pool_->Allocate(buffer_length_);
  }
  *output = out_buffer_;
  *output_length = buffer_length_;
  *input_bytes_read = 0;
  *eos = false;

  stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
  stream_.avail_in = input_length;
  stream_.next_out = reinterpret_cast<Bytef*>(*output);
  stream_.avail_out = *output_length;
  VLOG_ROW << "ProcessBlockStreaming() stream: " << DebugStreamState();

  int ret = inflate(&stream_, Z_SYNC_FLUSH);
  if (ret != Z_OK && ret != Z_STREAM_END && ret != Z_BUF_ERROR) {
    stringstream ss;
    ss << "GzipDecompressor failed, ret=" << ret;
    if (stream_.msg != NULL) ss << " msg=" << stream_.msg;
    return Status(ss.str());
  }

  // stream_.avail_out is the number of bytes *left* in the out buffer, but
  // we're interested in the number of bytes used.
  *output_length = *output_length - stream_.avail_out;
  *input_bytes_read = input_length - stream_.avail_in;
  VLOG_ROW << "inflate() ret=" << ret << " consumed=" << *input_bytes_read
           << " produced=" << *output_length << " stream: " << DebugStreamState();

  if (ret == Z_BUF_ERROR) {
    // Z_BUF_ERROR is returned if no progress was made. This should be very unlikely.
    // The caller should check for this case (where 0 bytes were consumed, 0 bytes
    // produced) and try again with more input.
    DCHECK_EQ(0, *output_length);
    DCHECK_EQ(0, *input_bytes_read);
  } else if (ret == Z_STREAM_END) {
    *eos = true;
    if (inflateReset(&stream_) != Z_OK) {
      return Status("zlib inflateReset failed: " + string(stream_.msg));
    }
  }
  return Status::OK;
}

Status GzipDecompressor::ProcessBlock(bool output_preallocated, int64_t input_length,
    const uint8_t* input, int64_t* output_length, uint8_t** output) {
  if (output_preallocated && *output_length == 0) {
    // The zlib library does not allow *output to be NULL, even when output_length is 0
    // (inflate() will return Z_STREAM_ERROR). We don't consider this an error, so bail
    // early if no output is expected. Note that we don't signal an error if the input
    // actually contains compressed data.
    return Status::OK;
  }

  bool use_temp = false;
  if (!output_preallocated) {
    if (!reuse_buffer_ || out_buffer_ == NULL) {
      // guess that we will need 2x the input length.
      buffer_length_ = input_length * 2;
      if (buffer_length_ > MAX_BLOCK_SIZE) {
        return Status("Decompressor: block size is too big");
      }
      out_buffer_ = temp_memory_pool_->Allocate(buffer_length_);
    }
    use_temp = true;
    *output = out_buffer_;
    *output_length = buffer_length_;
  }

  // Reset the stream for this block
  if (inflateReset(&stream_) != Z_OK) {
    return Status("zlib inflateReset failed: " + string(stream_.msg));
  }

  int ret = 0;
  // gzip can run in streaming mode or non-streaming mode.  We only
  // support the non-streaming use case where we present it the entire
  // compressed input and a buffer big enough to contain the entire
  // compressed output.  In the case where we don't know the output,
  // we just make a bigger buffer and try the non-streaming mode
  // from the beginning again.
  // TODO: support streaming, especially for compressed text.
  while (ret != Z_STREAM_END) {
    stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
    stream_.avail_in = input_length;
    stream_.next_out = reinterpret_cast<Bytef*>(*output);
    stream_.avail_out = *output_length;

    if (use_temp) {
      // We don't know the output size, so this might fail.
      ret = inflate(&stream_, Z_PARTIAL_FLUSH);
    } else {
      // We know the output size.  In this case, we can use Z_FINISH
      // which is more efficient.
      ret = inflate(&stream_, Z_FINISH);
    }
    if (ret == Z_STREAM_END || ret != Z_OK) break;

    // Not enough output space.
    if (!use_temp) {
      stringstream ss;
      ss << "Too small a buffer passed to GzipDecompressor. InputLength="
        << input_length << " OutputLength=" << *output_length;
      return Status(ss.str());
    }

    // User didn't supply the buffer, double the buffer and try again.
    temp_memory_pool_->Clear();
    buffer_length_ *= 2;
    if (buffer_length_ > MAX_BLOCK_SIZE) {
      stringstream ss;
      ss << "GzipDecompressor: block size is too big: " << buffer_length_;
      return Status(ss.str());
    }

    out_buffer_ = temp_memory_pool_->Allocate(buffer_length_);
    *output = out_buffer_;
    *output_length = buffer_length_;
    ret = inflateReset(&stream_);
  }

  if (ret != Z_STREAM_END) {
    stringstream ss;
    ss << "GzipDecompressor failed: ";
    if (stream_.msg != NULL) ss << stream_.msg;
    return Status(ss.str());
  }

  // stream_.avail_out is the number of bytes *left* in the out buffer, but
  // we're interested in the number of bytes used.
  *output_length = *output_length - stream_.avail_out;
  if (use_temp) memory_pool_->AcquireData(temp_memory_pool_.get(), reuse_buffer_);
  return Status::OK;
}

BzipDecompressor::BzipDecompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer) {
}

int64_t BzipDecompressor::MaxOutputLen(int64_t input_len, const uint8_t* input) {
  return -1;
}

Status BzipDecompressor::ProcessBlock(bool output_preallocated, int64_t input_length,
    const uint8_t* input, int64_t* output_length, uint8_t** output) {
  if (output_preallocated && *output_length == 0) {
    // Same problem as zlib library, see comment in GzipDecompressor::ProcessBlock()
    return Status::OK;
  }

  bool use_temp = false;
  if (output_preallocated) {
    buffer_length_ = *output_length;
    out_buffer_ = *output;
  } else if (!reuse_buffer_ || out_buffer_ == NULL) {
    // guess that we will need 2x the input length.
    buffer_length_ = input_length * 2;
    if (buffer_length_ > MAX_BLOCK_SIZE) {
      return Status("Decompressor: block size is too big");
    }
    out_buffer_ = temp_memory_pool_->Allocate(buffer_length_);
    use_temp = true;
  }

  int ret = BZ_OUTBUFF_FULL;
  unsigned int outlen;
  while (ret == BZ_OUTBUFF_FULL) {
    if (out_buffer_ == NULL) {
      DCHECK(!output_preallocated);
      temp_memory_pool_->Clear();
      buffer_length_ = buffer_length_ * 2;
      if (buffer_length_ > MAX_BLOCK_SIZE) {
        return Status("Decompressor: block size is too big");
      }
      out_buffer_ = temp_memory_pool_->Allocate(buffer_length_);
    }
    outlen = static_cast<unsigned int>(buffer_length_);
    if ((ret = BZ2_bzBuffToBuffDecompress(reinterpret_cast<char*>(out_buffer_), &outlen,
        const_cast<char*>(reinterpret_cast<const char*>(input)),
        static_cast<unsigned int>(input_length), 0, 0)) == BZ_OUTBUFF_FULL) {
      if (output_preallocated) {
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
  *output_length = outlen;
  if (use_temp) memory_pool_->AcquireData(temp_memory_pool_.get(), reuse_buffer_);
  return Status::OK;
}


SnappyBlockDecompressor::SnappyBlockDecompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer) {
}

int64_t SnappyBlockDecompressor::MaxOutputLen(int64_t input_len, const uint8_t* input) {
  return -1;
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
static Status SnappyBlockDecompress(int64_t input_len, const uint8_t* input,
    bool size_only, int64_t* output_len, char* output) {
  int64_t uncompressed_total_len = 0;
  while (input_len > 0) {
    uint32_t uncompressed_block_len = ReadWriteUtil::GetInt<uint32_t>(input);
    input += sizeof(uint32_t);
    input_len -= sizeof(uint32_t);

    if (uncompressed_block_len > Codec::MAX_BLOCK_SIZE) {
      if (uncompressed_total_len == 0) {
        // TODO: is this check really robust?
        stringstream ss;
        ss << "Decompressor: block size is too big.  Data is likely corrupt. "
           << "Size: " << uncompressed_block_len;
        return Status(ss.str());
      }
      break;
    }

    if (!size_only) {
      int64_t remaining_output_size = *output_len - uncompressed_total_len;
      DCHECK_GE(remaining_output_size, uncompressed_block_len);
    }

    while (uncompressed_block_len > 0) {
      // Read the length of the next snappy compressed block.
      size_t compressed_len = ReadWriteUtil::GetInt<uint32_t>(input);
      input += sizeof(uint32_t);
      input_len -= sizeof(uint32_t);

      if (compressed_len == 0 || compressed_len > input_len) {
        if (uncompressed_total_len == 0) {
          return Status(
              "Decompressor: invalid compressed length.  Data is likely corrupt.");
        }
        input_len = 0;
        break;
      }

      // Read how big the output will be.
      size_t uncompressed_len;
      if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(input),
            input_len, &uncompressed_len)) {
        if (uncompressed_total_len == 0) {
          return Status("Snappy: GetUncompressedLength failed");
        }
        input_len = 0;
        break;
      }
      DCHECK_GT(uncompressed_len, 0);

      if (!size_only) {
        // Decompress this snappy block
        if (!snappy::RawUncompress(reinterpret_cast<const char*>(input),
              compressed_len, output)) {
          return Status("SnappyBlock: RawUncompress failed");
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

Status SnappyBlockDecompressor::ProcessBlock(bool output_preallocated, int64_t input_len,
    const uint8_t* input, int64_t* output_len, uint8_t** output) {
  if (!output_preallocated) {
    // If we don't know the size beforehand, compute it.
    RETURN_IF_ERROR(SnappyBlockDecompress(input_len, input, true, output_len, NULL));

    if (!reuse_buffer_ || out_buffer_ == NULL || buffer_length_ < *output_len) {
      // Need to allocate a new buffer
      buffer_length_ = *output_len;
      out_buffer_ = memory_pool_->Allocate(buffer_length_);
    }
    *output = out_buffer_;
  }

  if (*output_len > MAX_BLOCK_SIZE) {
    // TODO: is this check really robust?
    stringstream ss;
    ss << "Decompressor: block size is too big.  Data is likely corrupt. "
       << "Size: " << *output_len;
    return Status(ss.str());
  }

  char* out_ptr = reinterpret_cast<char*>(*output);
  RETURN_IF_ERROR(SnappyBlockDecompress(input_len, input, false, output_len, out_ptr));
  return Status::OK;
}

SnappyDecompressor::SnappyDecompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer) {
}

int64_t SnappyDecompressor::MaxOutputLen(int64_t input_len, const uint8_t* input) {
  DCHECK(input != NULL);
  size_t result;
  if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(input),
           input_len, &result)) {
    return -1;
  }
  return result;
}

Status SnappyDecompressor::ProcessBlock(bool output_preallocated, int64_t input_length,
    const uint8_t* input, int64_t* output_length, uint8_t** output) {
  if (!output_preallocated) {
    int64_t uncompressed_length = MaxOutputLen(input_length, input);
    if (uncompressed_length < 0) return Status("Snappy: GetUncompressedLength failed");
    if (!reuse_buffer_ || out_buffer_ == NULL || buffer_length_ < uncompressed_length) {
      buffer_length_ = uncompressed_length;
      if (buffer_length_ > MAX_BLOCK_SIZE) {
        return Status("Decompressor: block size is too big");
      }
      out_buffer_ = memory_pool_->Allocate(buffer_length_);
    }
    *output = out_buffer_;
    *output_length = uncompressed_length;
  }

  if (!snappy::RawUncompress(reinterpret_cast<const char*>(input),
           static_cast<size_t>(input_length), reinterpret_cast<char*>(*output))) {
    return Status("Snappy: RawUncompress failed");
  }

  return Status::OK;
}

Lz4Decompressor::Lz4Decompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer) {
}

int64_t Lz4Decompressor::MaxOutputLen(int64_t input_len, const uint8_t* input) {
  DCHECK(input != NULL) << "Passed null input to Lz4 Decompressor";
  return -1;
}

Status Lz4Decompressor::ProcessBlock(bool output_preallocated, int64_t input_length,
    const uint8_t* input, int64_t* output_length, uint8_t** output) {
  DCHECK(output_preallocated) << "Lz4 Codec implementation must have allocated output";
  // LZ4_uncompress will cause a segmentation fault if passed a NULL output.
  if(*output_length == 0) return Status::OK;
  if (LZ4_uncompress(reinterpret_cast<const char*>(input),
          reinterpret_cast<char*>(*output), *output_length) != input_length) {
    return Status("Lz4: uncompress failed");
  }

  return Status::OK;
}
