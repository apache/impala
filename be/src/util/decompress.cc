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

#include "util/decompress.h"

#include <strings.h>

#include <sstream>

// Codec libraries
#include <zlib.h>
#include <bzlib.h>
#include <lz4.h>
#include <snappy.h>
#include <zstd.h>
#include <zstd_errors.h>

#include "common/logging.h"
#include "exec/read-write-util.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"

#include "common/compiler-util.h"
#include "gen-cpp/ErrorCodes_types.h"

#include "common/names.h"

using namespace impala;
using namespace strings;

const string DECOMPRESSOR_MEM_LIMIT_EXCEEDED =
    "$0Decompressor failed to allocate $1 bytes.";

GzipDecompressor::GzipDecompressor(MemPool* mem_pool, bool reuse_buffer, bool is_deflate)
  : Codec(mem_pool, reuse_buffer, true),
    is_deflate_(is_deflate) {
  bzero(&stream_, sizeof(stream_));
}

GzipDecompressor::~GzipDecompressor() {
  (void)inflateEnd(&stream_);
}

Status GzipDecompressor::Init() {
  // Initialize to run either deflate or zlib/gzip format
  int window_bits = is_deflate_ ? -WINDOW_BITS : WINDOW_BITS | DETECT_CODEC;
  int ret = inflateInit2(&stream_, window_bits);
  if (ret != Z_OK) {
    return Status(TErrorCode::COMPRESSED_FILE_DECOMPRESSOR_ERROR, "Gzip",
        "inflateInit2()", ret);
  }

  return Status::OK();
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
    int64_t* input_bytes_read, int64_t* output_length, uint8_t** output,
    bool* stream_end) {
  if (!reuse_buffer_ || out_buffer_ == nullptr) {
    buffer_length_ = STREAM_OUT_BUF_SIZE;
    out_buffer_ = memory_pool_->TryAllocate(buffer_length_);
    if (UNLIKELY(out_buffer_ == nullptr)) {
      string details = Substitute(DECOMPRESSOR_MEM_LIMIT_EXCEEDED, "Gzip",
          buffer_length_);
      return memory_pool_->mem_tracker()->MemLimitExceeded(
          nullptr, details, buffer_length_);
    }
  }
  *output = out_buffer_;

  stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
  stream_.avail_in = input_length;
  stream_.next_out = reinterpret_cast<Bytef*>(*output);
  stream_.avail_out = buffer_length_;

  *stream_end = false;
  *input_bytes_read = 0;
  *output_length = 0;
  while (stream_.avail_out > 0 && stream_.avail_in > 0) {
    *stream_end = false;
    // inflate() performs one or both of the following actions:
    //   Decompress more input starting at next_in and update next_in and avail_in
    //       accordingly.
    //   Provide more output starting at next_out and update next_out and avail_out
    //       accordingly.
    // inflate() returns Z_OK if some progress has been made (more input processed
    // or more output produced)
    int ret = inflate(&stream_, Z_SYNC_FLUSH);
    *input_bytes_read = input_length - stream_.avail_in;
    *output_length = buffer_length_ - stream_.avail_out;
    VLOG_ROW << "inflate() ret=" << ret << " consumed=" << *input_bytes_read
             << " produced=" << *output_length << " stream: " << DebugStreamState();

    if (ret == Z_DATA_ERROR) {
      return Status(TErrorCode::COMPRESSED_FILE_BLOCK_CORRUPTED, "Gzip");
    } else if (ret == Z_BUF_ERROR) {
      // Z_BUF_ERROR indicates that inflate() could not consume more input or
      // produce more output. inflate() can be called again with more output space
      // or more available input.
      VLOG_ROW << "inflate() ret=" << ret << ", cannot make progress, need more input";
      return Status::OK();
    } else if (ret == Z_STREAM_END) {
      *stream_end = true;
      ret = inflateReset(&stream_);
      if (ret != Z_OK) {
        return Status(TErrorCode::COMPRESSED_FILE_DECOMPRESSOR_ERROR, "Gzip",
            "inflateReset()", ret);
      }
    } else if (ret != Z_OK) {
      return Status(TErrorCode::COMPRESSED_FILE_DECOMPRESSOR_ERROR, "Gzip",
          "inflate()", ret);
    }
    DCHECK_EQ(ret, Z_OK);
  }

  return Status::OK();
}

Status GzipDecompressor::ProcessBlock(bool output_preallocated, int64_t input_length,
    const uint8_t* input, int64_t* output_length, uint8_t** output) {
  int64_t output_length_local = *output_length;
  *output_length = 0;
  if (UNLIKELY(output_preallocated && output_length_local == 0)) {
    // The zlib library does not allow *output to be nullptr, even when output_length is 0
    // (inflate() will return Z_STREAM_ERROR). We don't consider this an error, so bail
    // early if no output is expected. Note that we don't signal an error if the input
    // actually contains compressed data.
    return Status::OK();
  }

  bool use_temp = false;
  if (!output_preallocated) {
    if (!reuse_buffer_ || out_buffer_ == nullptr) {
      // guess that we will need 2x the input length.
      buffer_length_ = input_length * 2;
      out_buffer_ = temp_memory_pool_->TryAllocate(buffer_length_);
      if (UNLIKELY(out_buffer_ == nullptr)) {
        string details = Substitute(DECOMPRESSOR_MEM_LIMIT_EXCEEDED, "Gzip",
            buffer_length_);
        return temp_memory_pool_->mem_tracker()->MemLimitExceeded(
            nullptr, details, buffer_length_);
      }
    }
    use_temp = true;
    *output = out_buffer_;
    output_length_local = buffer_length_;
  }

  // Reset the stream for this block
  int ret = inflateReset(&stream_);
  if (ret != Z_OK) {
    return Status(TErrorCode::COMPRESSED_FILE_DECOMPRESSOR_ERROR, "Gzip",
        "inflateReset()", ret);
  }

  // We only support the non-streaming use case where we present it the entire
  // compressed input and a buffer big enough to contain the entire decompressed
  // output.  In the case where we don't know the output, we just make a bigger
  // buffer and try the non-streaming mode from the beginning again.
  // TODO: IMPALA-3073 Verify if compressed block could be multistream. If yes, we need
  // to support it and shouldn't stop decompressing while ret == Z_STREAM_END.
  while (ret != Z_STREAM_END) {
    stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
    stream_.avail_in = input_length;
    stream_.next_out = reinterpret_cast<Bytef*>(*output);
    stream_.avail_out = output_length_local;

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
        << input_length << " OutputLength=" << output_length_local;
      return Status(ss.str());
    }

    // User didn't supply the buffer, double the buffer and try again.
    temp_memory_pool_->Clear();
    buffer_length_ *= 2;
    out_buffer_ = temp_memory_pool_->TryAllocate(buffer_length_);
    if (UNLIKELY(out_buffer_ == nullptr)) {
      string details = Substitute(DECOMPRESSOR_MEM_LIMIT_EXCEEDED, "Gzip",
          buffer_length_);
      return temp_memory_pool_->mem_tracker()->MemLimitExceeded(
          nullptr, details, buffer_length_);
    }
    *output = out_buffer_;
    output_length_local = buffer_length_;
    ret = inflateReset(&stream_);
  }

  if (ret == Z_DATA_ERROR) {
    return Status(TErrorCode::COMPRESSED_FILE_BLOCK_CORRUPTED, "Gzip");
  } else if (ret != Z_STREAM_END) {
    stringstream ss;
    ss << "GzipDecompressor failed: ";
    if (stream_.msg != nullptr) ss << stream_.msg;
    return Status(ss.str());
  }

  // stream_.avail_out is the number of bytes *left* in the out buffer, but
  // we're interested in the number of bytes used.
  *output_length = output_length_local - stream_.avail_out;
  if (use_temp) memory_pool_->AcquireData(temp_memory_pool_.get(), reuse_buffer_);
  return Status::OK();
}

BzipDecompressor::BzipDecompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer, true) {
  bzero(&stream_, sizeof(stream_));
}

BzipDecompressor::~BzipDecompressor() {
  BZ2_bzDecompressEnd(&stream_);
}

Status BzipDecompressor::Init() {
  int ret = BZ2_bzDecompressInit(&stream_, 0, 0);
  if (ret != BZ_OK) {
    return Status(TErrorCode::COMPRESSED_FILE_DECOMPRESSOR_ERROR, "Bzip2",
        "BZ2_bzDecompressInit()", ret);
  }
  return Status::OK();
}

int64_t BzipDecompressor::MaxOutputLen(int64_t input_len, const uint8_t* input) {
  return -1;
}

Status BzipDecompressor::ProcessBlock(bool output_preallocated, int64_t input_length,
    const uint8_t* input, int64_t* output_length, uint8_t** output) {
  int64_t output_length_local = *output_length;
  *output_length = 0;
  if (UNLIKELY(output_preallocated && output_length_local == 0)) {
    // Same problem as zlib library, see comment in GzipDecompressor::ProcessBlock().
    return Status::OK();
  }

  bool use_temp = false;
  if (output_preallocated) {
    buffer_length_ = output_length_local;
    out_buffer_ = *output;
  } else if (!reuse_buffer_ || out_buffer_ == nullptr) {
    // guess that we will need 2x the input length.
    buffer_length_ = input_length * 2;
    out_buffer_ = temp_memory_pool_->TryAllocate(buffer_length_);
    if (UNLIKELY(out_buffer_ == nullptr)) {
      string details = Substitute(DECOMPRESSOR_MEM_LIMIT_EXCEEDED, "Bzip",
          buffer_length_);
      return temp_memory_pool_->mem_tracker()->MemLimitExceeded(
          nullptr, details, buffer_length_);
    }
    use_temp = true;
  }

  int ret = BZ_OUTBUFF_FULL;
  unsigned int outlen = static_cast<unsigned int>(buffer_length_);
  // TODO: IMPALA-3073 Verify if compressed block could be multistream. If yes, we need
  // to support it and shouldn't stop decompressing while ret == BZ_STREAM_END.
  while (ret == BZ_OUTBUFF_FULL) {
    if (out_buffer_ == nullptr) {
      DCHECK(!output_preallocated);
      temp_memory_pool_->Clear();
      buffer_length_ = buffer_length_ * 2;
      out_buffer_ = temp_memory_pool_->TryAllocate(buffer_length_);
      if (UNLIKELY(out_buffer_ == nullptr)) {
        string details = Substitute(DECOMPRESSOR_MEM_LIMIT_EXCEEDED, "Bzip",
            buffer_length_);
        return temp_memory_pool_->mem_tracker()->MemLimitExceeded(
            nullptr, details, buffer_length_);
      }
    }
    outlen = static_cast<unsigned int>(buffer_length_);
    if ((ret = BZ2_bzBuffToBuffDecompress(reinterpret_cast<char*>(out_buffer_), &outlen,
        const_cast<char*>(reinterpret_cast<const char*>(input)),
        static_cast<unsigned int>(input_length), 0, 0)) == BZ_OUTBUFF_FULL) {
      if (output_preallocated) {
        return Status("Too small a buffer passed to BzipDecompressor");
      }
      out_buffer_ = nullptr;
    }
  }

  if (ret == BZ_DATA_ERROR) {
    return Status(TErrorCode::COMPRESSED_FILE_BLOCK_CORRUPTED, "Bzip2");
  } else if (ret !=  BZ_OK) {
    return Status(TErrorCode::COMPRESSED_FILE_DECOMPRESSOR_ERROR, "Bzip2",
        "BZ2_bzBuffToBuffDecompressor()", ret);
  }

  *output = out_buffer_;
  *output_length = outlen;
  if (use_temp) memory_pool_->AcquireData(temp_memory_pool_.get(), reuse_buffer_);
  return Status::OK();
}

string BzipDecompressor::DebugStreamState() const {
  stringstream ss;
  ss << "Stream: " << &stream_;
  ss << " next_in=" << stream_.next_in;
  ss << " avail_in=" << stream_.avail_in;
  ss << " next_out=" << stream_.next_out;
  ss << " avail_out=" << stream_.avail_out;
  return ss.str();
}

// Decompress bzip2 data as a stream so we don't need to read the whole file
// to decompress at once. Possible formats:
// 1. Single stream file,
//    ProcessBlockStreaming() will be called until the end of the file is reached.
// 2. Multiple streams concatenated into a single file.
//    ProcessBlockStreaming() should be called multiple times until the end
//    of the file is reached. Each stream could be pretty small (<= 900k).
//    We try to consume as many streams as possible in one call to avoid
//    re-reading input data and allocating output buffer for each stream.
//
// Return if the output buffer is full or reach end of file or encounter an
// error.
Status BzipDecompressor::ProcessBlockStreaming(int64_t input_length, const uint8_t* input,
    int64_t* input_bytes_read, int64_t* output_length, uint8_t** output,
    bool* stream_end) {
  if (!reuse_buffer_ || out_buffer_ == nullptr) {
    buffer_length_ = STREAM_OUT_BUF_SIZE;
    out_buffer_ = memory_pool_->TryAllocate(buffer_length_);
    if (UNLIKELY(out_buffer_ == nullptr)) {
      string details = Substitute(DECOMPRESSOR_MEM_LIMIT_EXCEEDED, "Bzip",
          buffer_length_);
      return memory_pool_->mem_tracker()->MemLimitExceeded(
          nullptr, details, buffer_length_);
    }
  }
  *output = out_buffer_;

  stream_.next_in = const_cast<char*>(reinterpret_cast<const char*>(input));
  stream_.avail_in = input_length;
  stream_.next_out = reinterpret_cast<char*>(*output);
  stream_.avail_out = buffer_length_;

  *stream_end = false;
  *input_bytes_read = 0;
  *output_length = 0;
  while (stream_.avail_out > 0 && stream_.avail_in > 0) {
    *stream_end = false;
    int ret = BZ2_bzDecompress(&stream_);

    *output_length = buffer_length_ - stream_.avail_out;
    *input_bytes_read = input_length - stream_.avail_in;

    if (ret == BZ_DATA_ERROR || ret == BZ_DATA_ERROR_MAGIC) {
      return Status(TErrorCode::COMPRESSED_FILE_BLOCK_CORRUPTED, "Bzip2");
    } else if (ret == BZ_STREAM_END) {
      *stream_end = true;
      ret = BZ2_bzDecompressEnd(&stream_);
      if (ret != BZ_OK) {
        return Status(TErrorCode::COMPRESSED_FILE_DECOMPRESSOR_ERROR, "Bzip2",
            "BZ2_bzDecompressEnd()", ret);
      }
      ret = BZ2_bzDecompressInit(&stream_, 0, 0);
      if (ret != BZ_OK) {
        return Status(TErrorCode::COMPRESSED_FILE_DECOMPRESSOR_ERROR, "Bzip2",
            "BZ2_bzDecompressInit()", ret);
      }
    } else if (ret != BZ_OK) {
      return Status(TErrorCode::COMPRESSED_FILE_DECOMPRESSOR_ERROR, "Bzip2",
          "BZ2_bzDecompress()", ret);
    }
    DCHECK_EQ(ret, BZ_OK);
  }

  return Status::OK();
}

SnappyBlockDecompressor::SnappyBlockDecompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer) {
}

int64_t SnappyBlockDecompressor::MaxOutputLen(int64_t input_len, const uint8_t* input) {
  return -1;
}

// Hadoop uses a block compression scheme on top of snappy.  As per the hadoop docs
// (BlockCompressorStream.java and BlockDecompressorStream.java) the input is split
// into blocks.  Each block "contains the uncompressed length for the block followed
// by one of more length-prefixed blocks of compressed data."
// This is essentially blocks of blocks.
// The outer block consists of:
//   - 4 byte big endian uncompressed_size
//   < inner blocks >
//   ... repeated until input_len is consumed ..
// The inner blocks have:
//   - 4-byte big endian compressed_size
//   < snappy compressed block >
//   - 4-byte big endian compressed_size
//   < snappy compressed block >
//   ... repeated until uncompressed_size from outer block is consumed ...

// Utility function to decompress snappy block compressed data.  If size_only is true,
// this function does not decompress but only computes the output size and writes
// the result to *output_len.
// If size_only is false, output buffer size must be at least *output_len. *output_len is
// updated with the actual output size if the decompression succeeds, and is set to 0
// otherwise.
// size_only is an O(1) operation (just reads a single varint for each snappy block).
static Status SnappyBlockDecompress(int64_t input_len, const uint8_t* input,
    bool size_only, int64_t* output_len, char* output) {
  int64_t buffer_size = *output_len;
  *output_len = 0;
  int64_t uncompressed_total_len = 0;
  while (input_len > 0) {
    uint32_t uncompressed_block_len = ReadWriteUtil::GetInt<uint32_t>(input);
    input += sizeof(uint32_t);
    input_len -= sizeof(uint32_t);

    if (!size_only) {
      int64_t remaining_output_size = buffer_size - uncompressed_total_len;
      if (remaining_output_size < uncompressed_block_len) {
        return Status(TErrorCode::SNAPPY_DECOMPRESS_DECOMPRESS_SIZE_INCORRECT);
      }
    }

    while (uncompressed_block_len > 0) {
      // Check that input length should not be negative.
      if (input_len < 0) {
        stringstream ss;
        ss << " Corruption snappy decomp input_len " << input_len;
        return Status(ss.str());
      }
      // Read the length of the next snappy compressed block.
      size_t compressed_len = ReadWriteUtil::GetInt<uint32_t>(input);
      input += sizeof(uint32_t);
      input_len -= sizeof(uint32_t);

      if (compressed_len == 0 || compressed_len > input_len) {
        return Status(TErrorCode::SNAPPY_DECOMPRESS_INVALID_COMPRESSED_LENGTH);
      }

      // Read how big the output will be.
      size_t uncompressed_len;
      if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(input),
              compressed_len, &uncompressed_len)) {
        return Status(TErrorCode::SNAPPY_DECOMPRESS_UNCOMPRESSED_LENGTH_FAILED);
      }
      // Check that uncompressed length should be greater than 0.
      if (uncompressed_len <= 0) {
        return Status(TErrorCode::SNAPPY_DECOMPRESS_UNCOMPRESSED_LENGTH_FAILED);
      }
      DCHECK_GT(uncompressed_len, 0);

      if (!size_only) {
        // Check output bounds
        int64_t remaining_output_size = buffer_size - uncompressed_total_len;
        if (remaining_output_size < uncompressed_len) {
          return Status(TErrorCode::SNAPPY_DECOMPRESS_DECOMPRESS_SIZE_INCORRECT);
        }
        // Decompress this snappy block
        if (!snappy::RawUncompress(reinterpret_cast<const char*>(input),
                compressed_len, output)) {
          return Status(TErrorCode::SNAPPY_DECOMPRESS_RAW_UNCOMPRESS_FAILED);
        }
        output += uncompressed_len;
      }

      input += compressed_len;
      input_len -= compressed_len;
      uncompressed_block_len -= uncompressed_len;
      uncompressed_total_len += uncompressed_len;
    }
  }
  *output_len = uncompressed_total_len;
  return Status::OK();
}

Status SnappyBlockDecompressor::ProcessBlock(bool output_preallocated, int64_t input_len,
    const uint8_t* input, int64_t* output_len, uint8_t** output) {
  int64_t output_length_local = *output_len;
  *output_len = 0;
  if (!output_preallocated) {
    // If we don't know the size beforehand, compute it.
    RETURN_IF_ERROR(SnappyBlockDecompress(input_len, input, true, &output_length_local,
        nullptr));
    if (!reuse_buffer_ || out_buffer_ == nullptr || buffer_length_ < output_length_local) {
      // Need to allocate a new buffer
      buffer_length_ = output_length_local;
      out_buffer_ = memory_pool_->TryAllocate(buffer_length_);
      if (UNLIKELY(out_buffer_ == nullptr)) {
        string details = Substitute(DECOMPRESSOR_MEM_LIMIT_EXCEEDED, "SnappyBlock",
            buffer_length_);
        return memory_pool_->mem_tracker()->MemLimitExceeded(
            nullptr, details, buffer_length_);
      }
    }
    *output = out_buffer_;
  }

  char* out_ptr = reinterpret_cast<char*>(*output);
  RETURN_IF_ERROR(SnappyBlockDecompress(input_len, input, false, &output_length_local,
      out_ptr));
  *output_len = output_length_local;
  return Status::OK();
}

SnappyDecompressor::SnappyDecompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer) {
}

int64_t SnappyDecompressor::MaxOutputLen(int64_t input_len, const uint8_t* input) {
  if (input_len <= 0) return -1;
  DCHECK(input != nullptr);
  size_t result;
  if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(input),
          input_len, &result)) {
    return -1;
  }
  return result;
}

Status SnappyDecompressor::ProcessBlock(bool output_preallocated, int64_t input_length,
    const uint8_t* input, int64_t* output_length, uint8_t** output) {
  int64_t output_length_local = *output_length;
  *output_length = 0;
  int64_t uncompressed_length = MaxOutputLen(input_length, input);
  if (uncompressed_length < 0) {
    return Status(TErrorCode::SNAPPY_DECOMPRESS_UNCOMPRESSED_LENGTH_FAILED);
  }

  if (!output_preallocated) {
    if (!reuse_buffer_ || out_buffer_ == nullptr
        || buffer_length_ < uncompressed_length) {
      buffer_length_ = uncompressed_length;
      out_buffer_ = memory_pool_->TryAllocate(buffer_length_);
      if (UNLIKELY(out_buffer_ == nullptr)) {
        string details = Substitute(DECOMPRESSOR_MEM_LIMIT_EXCEEDED, "Snappy",
            buffer_length_);
        return memory_pool_->mem_tracker()->MemLimitExceeded(
            nullptr, details, buffer_length_);
      }
    }
    *output = out_buffer_;
  } else {
    // If the preallocated buffer is too small (e.g. if the file metadata is corrupt),
    // bail out early. Otherwise, this could result in a buffer overrun.
    if (uncompressed_length > output_length_local) {
      return Status(TErrorCode::SNAPPY_DECOMPRESS_DECOMPRESS_SIZE_INCORRECT);
    }
  }
  if (!snappy::RawUncompress(reinterpret_cast<const char*>(input),
          static_cast<size_t>(input_length), reinterpret_cast<char*>(*output))) {
    return Status("Snappy: RawUncompress failed");
  }
  *output_length = uncompressed_length;
  return Status::OK();
}

Lz4Decompressor::Lz4Decompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer) {
}

int64_t Lz4Decompressor::MaxOutputLen(int64_t input_len, const uint8_t* input) {
  DCHECK(input != nullptr) << "Passed null input to Lz4 Decompressor";
  return -1;
}

Status Lz4Decompressor::ProcessBlock(bool output_preallocated, int64_t input_length,
    const uint8_t* input, int64_t* output_length, uint8_t** output) {
  DCHECK(output_preallocated) << "Lz4 Codec implementation must have allocated output";
  if(*output_length == 0) return Status::OK();
  int ret = LZ4_decompress_safe(reinterpret_cast<const char*>(input),
      reinterpret_cast<char*>(*output), input_length, *output_length);
  if (ret < 0) {
    *output_length = 0;
    return Status("Lz4: uncompress failed");
  }
  *output_length = ret;
  return Status::OK();
}

ZstandardDecompressor::ZstandardDecompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer, true) {}

ZstandardDecompressor::~ZstandardDecompressor() {
  if (stream_ != NULL) {
    static_cast<void>(ZSTD_freeDCtx(stream_));
  }
}

int64_t ZstandardDecompressor::MaxOutputLen(int64_t input_len, const uint8_t* input) {
  return -1;
}

Status ZstandardDecompressor::ProcessBlock(bool output_preallocated, int64_t input_length,
    const uint8_t* input, int64_t* output_length, uint8_t** output) {
  DCHECK(output_preallocated) << "Output was not allocated for Zstd Codec";
  if (*output_length == 0) return Status::OK();
  if (stream_ == NULL) {
    stream_ = ZSTD_createDCtx();
    if (stream_ == NULL) {
      return Status(TErrorCode::COMPRESSED_FILE_DECOMPRESSOR_ERROR, "Zstd",
          "ZSTD_createDCtx()", 0);
    }
  }
  size_t ret = ZSTD_decompressDCtx(stream_, *output, *output_length, input, input_length);
  if (ZSTD_isError(ret)) {
    *output_length = 0;
    return Status(TErrorCode::ZSTD_ERROR, "ZSTD_decompress",
        ZSTD_getErrorString(ZSTD_getErrorCode(ret)));
  }
  *output_length = ret;
  return Status::OK();
}

Status ZstandardDecompressor::ProcessBlockStreaming(int64_t input_length,
    const uint8_t* input, int64_t* input_bytes_read, int64_t* output_length,
    uint8_t** output, bool* stream_end) {
  if (!reuse_buffer_ || out_buffer_ == nullptr) {
    buffer_length_ = STREAM_OUT_BUF_SIZE;
    out_buffer_ = memory_pool_->TryAllocate(buffer_length_);
    if (UNLIKELY(out_buffer_ == nullptr)) {
      string details =
          Substitute(DECOMPRESSOR_MEM_LIMIT_EXCEEDED, "Zstd", buffer_length_);
      return memory_pool_->mem_tracker()->MemLimitExceeded(
          nullptr, details, buffer_length_);
    }
  }
  if (stream_ == NULL) {
    stream_ = ZSTD_createDCtx();
    if (stream_ == NULL) {
      return Status(TErrorCode::COMPRESSED_FILE_DECOMPRESSOR_ERROR, "Zstd",
          "ZSTD_createDCtx()", 0);
    }
  }
  *input_bytes_read = 0;
  *output = out_buffer_;
  *output_length = 0;

  *stream_end = false;

  ZSTD_inBuffer input_buffer{input, static_cast<size_t>(input_length), 0};
  ZSTD_outBuffer output_buffer{*output, static_cast<size_t>(buffer_length_), 0};
  while (input_buffer.pos < input_buffer.size && output_buffer.pos < output_buffer.size) {
    size_t ret = ZSTD_decompressStream(stream_, &output_buffer, &input_buffer);
    if (ZSTD_isError(ret)) {
      *output_length = 0;
      return Status(TErrorCode::ZSTD_ERROR, "ZSTD_decompressStream",
          ZSTD_getErrorString(ZSTD_getErrorCode(ret)));
    }
    if (input_buffer.pos == input_buffer.size) *stream_end = true;
  }
  *input_bytes_read = input_buffer.pos;
  *output_length = output_buffer.pos;
  return Status::OK();
}

Lz4BlockDecompressor::Lz4BlockDecompressor(MemPool* mem_pool, bool reuse_buffer)
  : Codec(mem_pool, reuse_buffer) {
}

int64_t Lz4BlockDecompressor::MaxOutputLen(int64_t input_len, const uint8_t* input) {
  DCHECK(input != nullptr) << "Passed null input to Lz4 Decompressor";
  return -1;
}

// Decompresses a block compressed using Hadoop's lz4 block compression scheme. The
// compressed block layout is similar to Hadoop's snappy block compression scheme, with
// the only difference being the compression codec used. For more details please refer
// to the comment section for the SnappyBlockDecompress above.
Status Lz4BlockDecompressor::ProcessBlock(bool output_preallocated, int64_t input_len,
    const uint8_t* input, int64_t* output_len, uint8_t** output) {
  DCHECK(output_preallocated) << "Lz4 Codec implementation must have allocated output";
  if(*output_len == 0) return Status::OK();
  uint8_t* out_ptr = *output;
  int64_t uncompressed_total_len = 0;
  const int64_t buffer_size = *output_len;
  *output_len = 0;

  while (input_len > 0) {
    uint32_t uncompressed_block_len = ReadWriteUtil::GetInt<uint32_t>(input);
    input += sizeof(uint32_t);
    input_len -= sizeof(uint32_t);
    int64_t remaining_output_size = buffer_size - uncompressed_total_len;
    if (remaining_output_size < uncompressed_block_len) {
      return Status(TErrorCode::LZ4_BLOCK_DECOMPRESS_DECOMPRESS_SIZE_INCORRECT);
    }

    while (uncompressed_block_len > 0) {
      // Check that input length should not be negative.
      if (input_len < 0) {
        return Status(TErrorCode::LZ4_BLOCK_DECOMPRESS_INVALID_INPUT_LENGTH);
      }
      // Read the length of the next lz4 compressed block.
      size_t compressed_len = ReadWriteUtil::GetInt<uint32_t>(input);
      input += sizeof(uint32_t);
      input_len -= sizeof(uint32_t);

      if (compressed_len == 0 || compressed_len > input_len) {
        return Status(TErrorCode::LZ4_BLOCK_DECOMPRESS_INVALID_COMPRESSED_LENGTH);
      }

      // Decompress this block.
      int64_t remaining_output_size = buffer_size - uncompressed_total_len;
      int uncompressed_len = LZ4_decompress_safe(reinterpret_cast<const char*>(input),
          reinterpret_cast<char*>(out_ptr), compressed_len, remaining_output_size);
      if (uncompressed_len < 0) {
        return Status(TErrorCode::LZ4_DECOMPRESS_SAFE_FAILED);
      }

      out_ptr += uncompressed_len;
      input += compressed_len;
      input_len -= compressed_len;
      uncompressed_block_len -= uncompressed_len;
      uncompressed_total_len += uncompressed_len;
    }
  }
  *output_len = uncompressed_total_len;
  return Status::OK();
}
