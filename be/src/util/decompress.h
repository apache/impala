// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_DECOMPRESS_H
#define IMPALA_EXEC_DECOMPRESS_H

// We need zlib.h here to declare stream_ below.
#include <zlib.h>

#include "exec/hdfs-scanner.h"
#include "runtime/mem-pool.h"

namespace impala {

// Create a decompressor object.  This is the base class for specific decompression
// algorithms.
class Decompressor {
 public:
  //These are the codec strings recognized by CreateDecompressor.
  static const char* const DEFAULT_COMPRESSION;
  static const char* const GZIP_COMPRESSION;
  static const char* const BZIP2_COMPRESSION;
  static const char* const SNAPPY_COMPRESSION;

  virtual ~Decompressor() {}

  // Process a block of data.  The decompressor will allocate the output buffer
  // if output_length is passed as 0. If it is non-zero the length must be the
  // correct size to hold the decompressed output.
  // Inputs:
  //   input_length: length of the data to decompress
  //   input: data to decompress
  //   output_length: Length of the output, if known, 0 otherwise.
  // Output:
  //   output: Pointer to decompressed data
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int output_length, uint8_t** output)  = 0;

  // Create the decompressor.
  // Input: 
  //  runtime_state: the current runtime state.
  //  mem_pool: the memory pool used to store the decompressed data.
  //  reuse: if true the allocated buffer can be reused.
  //  codec: the string representing the codec of the current file.
  // Output:
  //  decompressor: pointer to the decompressor class to use.
  static Status CreateDecompressor(RuntimeState* runtime_state, MemPool* mem_pool,
                                   bool reuse, const std::vector<char>& codec,
                                   boost::scoped_ptr<Decompressor>* decompressor);

 protected:
  // Create a decompressor
  // Inputs:
  //   mem_pool: memory pool to allocate the output buffer, this implies that the
  //             caller is responsible for the memory allocated by the decompressor.
  //   reuse_buffer: if false always allocate a new buffer rather than reuse.
  Decompressor(MemPool* mem_pool, bool reuse_buffer);

  // Initialize the decompressor.
  virtual Status Init() = 0;

  // Pool to allocate the buffer to hold decompressed data.
  MemPool* memory_pool_;

  // Temporary memory pool: in case we get the output size too small we can
  // use this to free unused buffers.
  boost::scoped_ptr<MemPool> temp_memory_pool_;

  // Can we reuse the output buffer or do we need to allocate on each call?
  bool reuse_buffer_;

  // Buffer to hold decompressed data.
  // Either passed from the caller or allocated from memory_pool_.
  uint8_t* out_buffer_;

  // Length of the output buffer.
  int buffer_length_;
};

class GzipDecompressor : public Decompressor {
 public:
  GzipDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~GzipDecompressor();

  //Process a block of data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int output_length, uint8_t** output);

 protected:
  // Initialize the decompressor.
  virtual Status Init();

 private:
  z_stream stream_;

  // These are magic numbers from zlib.h.  Not clear why they are not defined there.
  const static int WINDOW_BITS = 15;    // Maximum window size
  const static int DETECT_CODEC = 32;   // Determine if this is libz or gzip from header.
  
};

class BzipDecompressor : public Decompressor {
 public:
  BzipDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~BzipDecompressor() { }

  //Process a block fo data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int output_length, uint8_t** output);
 protected:
  // Bzip does not need initialization
  virtual Status Init() { return Status::OK;  }

};

class SnappyDecompressor : public Decompressor {
 public:
  SnappyDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~SnappyDecompressor() { }

  //Process a block fo data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int output_length, uint8_t** output);

 protected:
  // Snappy does not need initialization
  virtual Status Init() { return Status::OK; }

};

}
#endif
