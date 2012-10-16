// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

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
  GzipDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~GzipDecompressor();

  //Process a block of data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Initialize the decompressor.
  virtual Status Init();

 private:
  z_stream stream_;

  // These are magic numbers from zlib.h.  Not clear why they are not defined there.
  const static int WINDOW_BITS = 15;    // Maximum window size
  const static int DETECT_CODEC = 32;   // Determine if this is libz or gzip from header.
};

class BzipDecompressor : public Codec {
 public:
  BzipDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~BzipDecompressor() { }

  //Process a block of data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);
 protected:
  // Bzip does not need initialization
  virtual Status Init() { return Status::OK;  }

};

class SnappyDecompressor : public Codec {
 public:
  SnappyDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~SnappyDecompressor() { }

  //Process a block of data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Snappy does not need initialization
  virtual Status Init() { return Status::OK; }

};
class SnappyBlockDecompressor : public Codec {
 public:
  SnappyBlockDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~SnappyBlockDecompressor() { }

  //Process a block of data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Snappy does not need initialization
  virtual Status Init() { return Status::OK; }

};

}
#endif
