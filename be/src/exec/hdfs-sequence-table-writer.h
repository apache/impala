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

#ifndef IMPALA_EXEC_HDFS_SEQUENCE_WRITER_H
#define IMPALA_EXEC_HDFS_SEQUENCE_WRITER_H

#include <hdfs.h>
#include <sstream>

#include "runtime/descriptors.h"
#include "exec/hdfs-table-sink.h"
#include "exec/hdfs-table-writer.h"
#include "util/codec.h"
#include "write-stream.h"

namespace impala {

class Expr;
class TupleDescriptor;
class TupleRow;
class RuntimeState;
struct StringValue;
struct OutputPartition;

/// Sequence files are flat files consisting of binary key/value pairs. Essentially there
/// are 3 different formats for sequence files depending on the 'compression_codec' and
/// 'seq_compression_mode' query options:
/// - Uncompressed sequence file format
/// - Record-compressed sequence file format
/// - Block-compressed sequence file format
/// All of them share a common header described below.
///
/// Sequence File Header
/// --------------------
/// - version - 3 bytes of magic header SEQ, followed by 1 byte of actual version number
///   (e.g. SEQ4 or SEQ6)
/// - keyClassName - key class
/// - valueClassName - value class
/// - compression - A boolean which specifies if compression is turned on for keys/values
///   in this file.
/// - blockCompression - A boolean which specifies if block-compression is turned on for
///   keys/values in this file.
/// - compression codec - compression codec class which is used for compression of keys
///   and/or values (if compression is enabled).
/// - metadata - SequenceFile.Metadata for this file.
/// - sync - A 16 byte sync marker to denote end of the header.
///
/// Uncompressed Sequence File Format
/// ---------------------------------
/// - Header
/// - Record
///   - Record length
///   - Key length
///   - Key
///   - Value
/// - "\xFF\xFF\xFF\xFF" followed by a sync-marker every few 100 bytes or so.
///
/// Record-Compressed Sequence File Format
/// --------------------------------------
/// - Header
/// - Record
///   - Record length
///   - Key length
///   - Key
///   - Compressed Value
/// - "\xFF\xFF\xFF\xFF" followed by a sync-marker every few 100 bytes or so.
///
/// Block-Compressed Sequence File Format
/// -------------------------------------
/// - Header
/// - Record Block
///   - Uncompressed number of records in the block
///   - Compressed key-lengths block-size
///   - Compressed key-lengths block
///   - Compressed keys block-size
///   - Compressed keys block
///   - Compressed value-lengths block-size
///   - Compressed value-lengths block
///   - Compressed values block-size
///   - Compressed values block
/// - "\xFF\xFF\xFF\xFF" followed by a sync-marker every block.
/// The compressed blocks of key lengths and value lengths consist of the actual lengths
/// of individual keys/values encoded in zero-compressed integer format.

/// Consumes rows and outputs the rows into a sequence file in HDFS
/// Output is buffered to fill sequence file blocks.
class HdfsSequenceTableWriter : public HdfsTableWriter {
 public:
  HdfsSequenceTableWriter(HdfsTableSink* parent, RuntimeState* state,
      OutputPartition* output, const HdfsPartitionDescriptor* partition,
      const HdfsTableDescriptor* table_desc);

  ~HdfsSequenceTableWriter() { }

  virtual Status Init();
  virtual Status Finalize() { return Flush(); }
  virtual Status InitNewFile() { return WriteFileHeader(); }
  virtual void Close();
  virtual uint64_t default_block_size() const { return 0; }
  virtual std::string file_extension() const { return "seq"; }

  /// Outputs the given rows into an HDFS sequence file. The rows are buffered
  /// to fill a sequence file block.
  virtual Status AppendRows(
      RowBatch* rows, const std::vector<int32_t>& row_group_indices, bool* new_file);

 private:
  /// processes a single row, delegates to Compress or NoCompress ConsumeRow().
  inline Status ConsumeRow(TupleRow* row);

  /// writes the SEQ file header to HDFS
  Status WriteFileHeader();

  /// writes the contents of out_value_lengths_block_ and out_ as a single
  /// block-compressed record.
  Status WriteCompressedBlock();

  /// writes the tuple row to the given buffer; separates fields by field_delim_,
  /// escapes string.
  inline void EncodeRow(TupleRow* row, WriteStream* buf);

  /// writes the str_val to the buffer, escaping special characters
  inline void WriteEscapedString(const StringValue* str_val, WriteStream* buf);

  /// flushes the output -- clearing out_ and writing to HDFS
  /// if compress_flag_, will write contents of out_ as a single compressed block
  Status Flush();

  /// desired size of each block (bytes); actual block size will vary +/- the
  /// size of a row; this is before compression is applied.
  uint64_t approx_block_size_;

  /// buffer which holds accumulated output
  WriteStream out_;

  /// buffer which holds accumulated value-lengths output (used with block-compressed
  /// sequence files)
  WriteStream out_value_lengths_block_;

  /// Temporary Buffer for a single row
  WriteStream row_buf_;

  /// memory pool used by codec to allocate output buffer
  boost::scoped_ptr<MemPool> mem_pool_;

  /// true if compression is enabled
  bool compress_flag_;

  /// number of rows consumed since last flush
  uint64_t unflushed_rows_;

  /// name of codec, only set if compress_flag_
  std::string codec_name_;
  /// the codec for compressing, only set if compress_flag_
  boost::scoped_ptr<Codec> compressor_;

  /// true if compression is applied on each record individually
  bool record_compression_;

  /// Character delimiting fields
  char field_delim_;

  /// Escape character for text encoding
  char escape_char_;

  /// 16 byte sync marker (a uuid)
  std::string sync_marker_;
  /// A -1 infront of the sync marker, used in decompressed formats
  std::string neg1_sync_marker_;

  /// Name of java class to use when reading the keys
  static const char* KEY_CLASS_NAME;
  /// Name of java class to use when reading the values
  static const char* VALUE_CLASS_NAME;
  /// Magic characters used to identify the file type
  static const uint8_t SEQ6_CODE[4];
};

} // namespace impala
#endif
