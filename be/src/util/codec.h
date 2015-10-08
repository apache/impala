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


#ifndef IMPALA_UTIL_CODEC_H
#define IMPALA_UTIL_CODEC_H

#include "common/status.h"
#include "runtime/mem-pool.h"
#include "util/runtime-profile.h"

#include <boost/scoped_ptr.hpp>
#include "gen-cpp/Descriptors_types.h"

namespace impala {

class MemPool;
class RuntimeState;

/// Create a compression object.  This is the base class for all compression algorithms. A
/// compression algorithm is either a compressor or a decompressor.  To add a new
/// algorithm, generally, both a compressor and a decompressor will be added.  Each of
/// these objects inherits from this class. The objects are instantiated in the Create
/// static methods defined here.  The type of compression is defined in the Thrift
/// interface THdfsCompression.
/// TODO: make this pure virtual (no members) so that external codecs (e.g. Lzo)
/// can implement this without binary dependency issues.
/// TODO: this interface is clunky. There should be one class that implements both the
/// compress and decompress APIs so remove duplication.
class Codec {
 public:
  /// These are the codec string representations used in Hadoop.
  static const char* const DEFAULT_COMPRESSION;
  static const char* const GZIP_COMPRESSION;
  static const char* const BZIP2_COMPRESSION;
  static const char* const SNAPPY_COMPRESSION;
  static const char* const UNKNOWN_CODEC_ERROR;

  /// Map from codec string to compression format
  typedef std::map<const std::string, const THdfsCompression::type> CodecMap;
  static const CodecMap CODEC_MAP;

  /// Create a decompressor.
  /// Input:
  ///  mem_pool: the memory pool used to store the decompressed data.
  ///  reuse: if true the allocated buffer can be reused.
  ///  format: the type of decompressor to create.
  /// Output:
  ///  decompressor: scoped pointer to the decompressor class to use.
  /// If mem_pool is NULL, then the resulting codec will never allocate memory and
  /// the caller must be responsible for it.
  static Status CreateDecompressor(MemPool* mem_pool, bool reuse,
    THdfsCompression::type format, boost::scoped_ptr<Codec>* decompressor);

  /// Alternate factory method: takes a codec string and populates a scoped pointer.
  static Status CreateDecompressor(MemPool* mem_pool, bool reuse,
      const std::string& codec, boost::scoped_ptr<Codec>* decompressor);

  /// Create a compressor.
  /// Input:
  ///  mem_pool: the memory pool used to store the compressed data.
  ///  reuse: if true the allocated buffer can be reused.
  ///  format: The type of compressor to create.
  /// Output:
  ///  compressor: scoped pointer to the compressor class to use.
  static Status CreateCompressor(MemPool* mem_pool, bool reuse,
      THdfsCompression::type format, boost::scoped_ptr<Codec>* compressor);

  /// Alternate factory method: takes a codec string and populates a scoped pointer.
  static Status CreateCompressor(MemPool* mem_pool, bool reuse,
      const std::string& codec, boost::scoped_ptr<Codec>* compressor);

  /// Return the name of a compression algorithm.
  static std::string GetCodecName(THdfsCompression::type);
  /// Returns the java class name for the given compression type
  static Status GetHadoopCodecClassName(THdfsCompression::type, std::string* out_name);

  virtual ~Codec() {}

  /// Process a block of data, either compressing or decompressing it.
  //
  /// If output_preallocated is true, *output_length must be the length of *output and data
  /// will be written directly to *output (*output must be big enough to contain the
  /// transformed output). If output_preallocated is false, *output will be allocated from
  /// the codec's mempool. In this case, a mempool must have been passed into the c'tor.
  //
  /// In either case, *output_length will be set to the actual length of the transformed
  /// output.
  //
  /// Inputs:
  ///   input_length: length of the data to process
  ///   input: data to process
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length, uint8_t** output) = 0;

  /// Wrapper to the actual ProcessBlock() function. This wrapper uses lengths as ints and
  /// not int64_ts. We need to keep this interface because the Parquet thrift uses ints.
  /// See IMPALA-1116.
  Status ProcessBlock32(bool output_preallocated, int input_length, const uint8_t* input,
      int* output_length, uint8_t** output);

  /// Process data like ProcessBlock(), but can consume partial input and may only produce
  /// partial output. *input_bytes_read returns the number of bytes of input that have
  /// been consumed. Even if all input has been consumed, the caller must continue calling
  /// to fetch output until *output_length==0.
  ///
  /// On the same codec object, we should call either ProcessBlock() or
  /// ProcessBlockStreaming() but not both. Use ProcessBlockStreaming() when decompressing
  /// file. Use ProcessBlock() when decompressing blocks.
  ///
  /// Inputs:
  ///   input_length: length, in bytes of the data to decompress
  ///   input: data to decompress
  ///
  /// Outputs:
  ///   input_bytes_read: number of bytes of 'input' that were decompressed
  ///   output_length: length of decompresed data
  ///   output: decompressed data
  ///   stream_end: end of output buffer corresponds to the end of a compressed stream.
  virtual Status ProcessBlockStreaming(int64_t input_length, const uint8_t* input,
      int64_t* input_bytes_read, int64_t* output_length, uint8_t** output, bool* stream_end) {
    return Status("Not implemented.");
  }

  /// Returns the maximum result length from applying the codec to input.
  /// Note this is not the exact result length, simply a bound to allow preallocating
  /// a buffer.
  /// This must be an O(1) operation (i.e. cannot read all of input).  Codecs that
  /// don't support this should return -1.
  virtual int64_t MaxOutputLen(int64_t input_len, const uint8_t* input = NULL) = 0;

  /// Must be called on codec before destructor for final cleanup.
  virtual void Close();

  /// File extension to use for this compression codec.
  virtual std::string file_extension() const = 0;

  bool reuse_output_buffer() const { return reuse_buffer_; }

  bool supports_streaming() const { return supports_streaming_; }

  /// Largest block we will compress/decompress: 2GB.
  /// We are dealing with compressed blocks that are never this big but we want to guard
  /// against a corrupt file that has the block length as some large number.
  static const int MAX_BLOCK_SIZE = (2L * 1024 * 1024 * 1024) - 1;

 protected:
  /// Create a compression operator
  /// Inputs:
  ///   mem_pool: memory pool to allocate the output buffer. If mem_pool is NULL then the
  ///             caller must always preallocate *output in ProcessBlock().
  ///   reuse_buffer: if false always allocate a new buffer rather than reuse.
  Codec(MemPool* mem_pool, bool reuse_buffer, bool supports_streaming = false);

  /// Initialize the codec. This should only be called once.
  virtual Status Init() = 0;

  /// Pool to allocate the buffer to hold transformed data.
  MemPool* memory_pool_;

  /// Temporary memory pool: in case we get the output size too small we can use this to
  /// free unused buffers.
  boost::scoped_ptr<MemPool> temp_memory_pool_;

  /// Can we reuse the output buffer or do we need to allocate on each call?
  bool reuse_buffer_;

  /// Buffer to hold transformed data.
  /// Either passed from the caller or allocated from memory_pool_.
  uint8_t* out_buffer_;

  /// Length of the output buffer.
  int64_t buffer_length_;

  /// Can decompressor support streaming mode.
  /// This is set to true for codecs that implement ProcessBlockStreaming().
  bool supports_streaming_;
};

}
#endif
