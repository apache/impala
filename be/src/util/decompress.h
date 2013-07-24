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


#ifndef IMPALA_UTIL_DECOMPRESS_H
#define IMPALA_UTIL_DECOMPRESS_H

// We need zlib.h here to declare stream_ below.
#include <zlib.h>

#include "util/codec.h"
#include "exec/hdfs-scanner.h"
#include "runtime/mem-pool.h"

namespace impala {

class GzipDecompressor : public Codec {
 public:
  GzipDecompressor(
      MemPool* mem_pool = NULL, bool reuse_buffer = false, bool is_deflate = false);
  virtual ~GzipDecompressor();

  virtual int MaxOutputLen(int input_len, const uint8_t* input = NULL);
  virtual Status ProcessBlock(bool output_preallocated,
                              int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Initialize the decompressor.
  virtual Status Init();

 private:
  // If set assume deflate format, otherwise zlib or gzip
  bool is_deflate_;

  z_stream stream_;

  // These are magic numbers from zlib.h.  Not clear why they are not defined there.
  const static int WINDOW_BITS = 15;    // Maximum window size
  const static int DETECT_CODEC = 32;   // Determine if this is libz or gzip from header.
};

class BzipDecompressor : public Codec {
 public:
  BzipDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~BzipDecompressor() { }

  virtual int MaxOutputLen(int input_len, const uint8_t* input = NULL);
  virtual Status ProcessBlock(bool output_preallocated,
                              int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);
 protected:
  // Bzip does not need initialization
  virtual Status Init() { return Status::OK; }
};

class SnappyDecompressor : public Codec {
 public:
  SnappyDecompressor(MemPool* mem_pool = NULL, bool reuse_buffer = false);
  virtual ~SnappyDecompressor() { }

  virtual int MaxOutputLen(int input_len, const uint8_t* input = NULL);
  virtual Status ProcessBlock(bool output_preallocated,
                              int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Snappy does not need initialization
  virtual Status Init() { return Status::OK; }

};

class SnappyBlockDecompressor : public Codec {
 public:
  SnappyBlockDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~SnappyBlockDecompressor() { }

  virtual int MaxOutputLen(int input_len, const uint8_t* input = NULL);
  virtual Status ProcessBlock(bool output_preallocated,
                              int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Snappy does not need initialization
  virtual Status Init() { return Status::OK; }
};

}
#endif
