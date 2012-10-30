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


#ifndef IMPALA_UTIL_COMPRESS_H
#define IMPALA_UTIL_COMPRESS_H

// We need zlib.h here to declare stream_ below.
#include <zlib.h>

#include "util/codec.h"
#include "exec/hdfs-scanner.h"
#include "runtime/mem-pool.h"

namespace impala {

// Create a compressor object.

class GzipCompressor : public Codec {
 public:
  // If gzip is set then we create gzip otherwise lzip.
  GzipCompressor(MemPool* mem_pool, bool reuse_buffer, bool is_gzip);
  virtual ~GzipCompressor();

  //Process a block of data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Initialize the compressor.
  virtual Status Init();

 private:
  // If set we use the gzip algorithm otherwise the lzip.
  bool is_gzip_;

  // Structure used to communicate with the library.
  z_stream stream_;

  // These are magic numbers from zlib.h.  Not clear why they are not defined there.
  const static int WINDOW_BITS = 15;    // Maximum window size
  const static int GZIP_CODEC = 16;     // Output Gzip.
  
};

class BzipCompressor : public Codec {
 public:
  BzipCompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~BzipCompressor() { }

  //Process a block of data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);
  // Initialize the compressor.
  virtual Status Init() { return Status::OK; }

};

class SnappyBlockCompressor : public Codec {
 public:
  SnappyBlockCompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~SnappyBlockCompressor() { }

  //Process a block of data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Snappy does not need initialization
  virtual Status Init() { return Status::OK; }

};

class SnappyCompressor : public Codec {
 public:
  SnappyCompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~SnappyCompressor() { }

  //Process a block of data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Snappy does not need initialization
  virtual Status Init() { return Status::OK; }

};

}
#endif
