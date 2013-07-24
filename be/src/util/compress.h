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

// Different compression classes.  The classes all expose the same API and
// abstracts the underlying calls to the compression libraries.
// TODO: reconsider the abstracted API

class GzipCompressor : public Codec {
 public:
  // Compression formats supported by the zlib library
  enum Format {
    ZLIB,
    DEFLATE,
    GZIP,
  };

  // If gzip is set then we create gzip otherwise lzip.
  GzipCompressor(Format format, MemPool* mem_pool = NULL, bool reuse_buffer = false);

  virtual ~GzipCompressor();

  // Returns an upper bound on the max compressed length.
  virtual int MaxOutputLen(int input_len, const uint8_t* input = NULL);

  // Process a block of data.
  virtual Status ProcessBlock(bool output_preallocated,
                              int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Initialize the compressor.
  virtual Status Init();

 private:
  Format format_;

  // Structure used to communicate with the library.
  z_stream stream_;

  // These are magic numbers from zlib.h.  Not clear why they are not defined there.
  const static int WINDOW_BITS = 15;    // Maximum window size
  const static int GZIP_CODEC = 16;     // Output Gzip.
  
  // Compresses 'input' into 'output'.  Output must be preallocated and
  // at least big enough.
  // *output_length should be called with the length of the output buffer and on return
  // is the length of the output.
  Status Compress(int input_length, uint8_t* input, 
      int* output_length, uint8_t* output);
};

class BzipCompressor : public Codec {
 public:
  BzipCompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~BzipCompressor() { }

  // Returns an upper bound on the max compressed length.
  virtual int MaxOutputLen(int input_len, const uint8_t* input = NULL);

  // Process a block of data.
  virtual Status ProcessBlock(bool output_preallocated,
                              int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);
  // Initialize the compressor.
  virtual Status Init() { return Status::OK; }
};

class SnappyBlockCompressor : public Codec {
 public:
  SnappyBlockCompressor(MemPool* mem_pool, bool reuse_buffer);
  
  virtual ~SnappyBlockCompressor() { }

  // Returns an upper bound on the max compressed length.
  virtual int MaxOutputLen(int input_len, const uint8_t* input = NULL);

  // Process a block of data.
  virtual Status ProcessBlock(bool output_preallocated,
                              int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Snappy does not need initialization
  virtual Status Init() { return Status::OK; }
};

class SnappyCompressor : public Codec {
 public:
  SnappyCompressor(MemPool* mem_pool = NULL, bool reuse_buffer = false);
  virtual ~SnappyCompressor() { }

  // Returns an upper bound on the max compressed length.
  virtual int MaxOutputLen(int input_len, const uint8_t* input = NULL);

  // Process a block of data.
  virtual Status ProcessBlock(bool output_preallocated,
                              int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);
  
 protected:
  // Snappy does not need initialization
  virtual Status Init() { return Status::OK; }
};

}
#endif
