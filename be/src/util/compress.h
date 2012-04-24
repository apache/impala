// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_COMPRESS_H
#define IMPALA_EXEC_COMPRESS_H

// We need zlib.h here to declare stream_ below.
#include <zlib.h>

#include "exec/hdfs-scanner.h"
#include "runtime/mem-pool.h"

namespace impala {

// Create a compressor object.
// This class is only used for testing the decomprssor class.
// This is the base class for specific compression algorithms.
class Compressor {
 public:
  virtual ~Compressor() {}

  // Process a block of data.  The compressor will allocate the output buffer
  // if output_length is passed as 0. If it is non-zero the length must be the
  // correct size to hold the compressed output.
  // Inputs:
  //   input_length: length of the data to compress
  //   input: data to compress
  // Input/Output:
  //   output_length: Pointer to length of the output, if known, 0 otherwise.
  //                  Set to actual length on return.
  // Output:
  //   output: Pointer to compressed data
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output)  = 0;

  // Create the compressor.
  // Input: 
  //  runtime_state: the current runtime state.
  //  mem_pool: the memory pool used to store the compressed data.
  //  reuse: if true the allocated buffer can be reused.
  //  codec: the string representing the codec of the current file.
  // Output:
  //  compressor: pointer to the compressor class to use.
  static Status CreateCompressor(RuntimeState* runtime_state, MemPool* mem_pool,
                                   bool reuse, const std::vector<char>& codec,
                                   boost::scoped_ptr<Compressor>* compressor);

 protected:
  // Create a compressor
  // Inputs:
  //   mem_pool: memory pool to allocate the output buffer, this implies that the
  //             caller is responsible for the memory allocated by the compressor.
  //   reuse_buffer: if false always allocate a new buffer rather than reuse.
  Compressor(MemPool* mem_pool, bool reuse_buffer);

  // Initialize the compressor.
  // is_gzip: true if the output should be in gzip rather than lzip.
  virtual Status Init(bool is_gzip) = 0;

  // Pool to allocate the buffer to hold compressed data.
  MemPool* memory_pool_;

  // Temporary memory pool: in case we get the output size too small we can
  // use this to free unused buffers.
  boost::scoped_ptr<MemPool> temp_memory_pool_;

  // Can we reuse the output buffer or do we need to allocate on each call?
  bool reuse_buffer_;

  // Buffer to hold compressed data.
  // Either passed from the caller or allocated from memory_pool_.
  uint8_t* out_buffer_;

  // Length of the output buffer.
  int buffer_length_;
};

class GzipCompressor : public Compressor {
 public:
  GzipCompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~GzipCompressor();

  //Process a block of data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Initialize the compressor.
  virtual Status Init(bool is_gzip);

 private:
  z_stream stream_;

  // These are magic numbers from zlib.h.  Not clear why they are not defined there.
  const static int WINDOW_BITS = 15;    // Maximum window size
  const static int GZIP_CODEC = 16;     // Output Gzip.
  
};

class BzipCompressor : public Compressor {
 public:
  BzipCompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~BzipCompressor() { }

  //Process a block fo data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);
  // Initialize the compressor.
  virtual Status Init(bool is_gzip) { return Status::OK; }

};

class SnappyCompressor : public Compressor {
 public:
  SnappyCompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~SnappyCompressor() { }

  //Process a block fo data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Snappy does not need initializaton
  virtual Status Init(bool is_gzip) { return Status::OK; }

};

}
#endif
