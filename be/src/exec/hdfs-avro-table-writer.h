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

#ifndef IMPALA_EXEC_HDFS_AVRO_WRITER_H
#define IMPALA_EXEC_HDFS_AVRO_WRITER_H

#include <hdfs.h>
#include <sstream>
#include <string>

#include "exec/hdfs-table-writer.h"
#include "util/codec.h"
#include "exec/write-stream.h"

namespace impala {

class Expr;
class TupleDescriptor;
class TupleRow;
class RuntimeState;
class HdfsTableSink;
struct StringValue;
struct OutputPartition;

/// Consumes rows and outputs the rows into an Avro file in HDFS
/// Each Avro file contains a block of records (rows). The file metadata specifies the
/// schema of the records in addition to the name of the codec, if any, used to compress
/// blocks. The structure is:
///   [ Metadata ]
///   [ Sync Marker ]
///   [ Data Block ]
///     ...
///   [ Data Block ]
//
/// Each Data Block consists of:
///   [ Number of Rows in Block ]
///   [ Size of serialized objects, after compression ]
///   [ Serialized objects, compressed ]
///   [ Sync Marker ]
//
/// If compression is used, each block is compressed individually. The block size defaults
/// to about 64KB before compression.
/// This writer implements the Avro 1.7.7 spec:
/// http://avro.apache.org/docs/1.7.7/spec.html
class HdfsAvroTableWriter : public HdfsTableWriter {
 public:
  HdfsAvroTableWriter(HdfsTableSink* parent,
                      RuntimeState* state, OutputPartition* output,
                      const HdfsPartitionDescriptor* partition,
                      const HdfsTableDescriptor* table_desc,
                      const std::vector<ExprContext*>& output_exprs);

  virtual ~HdfsAvroTableWriter() { }

  virtual Status Init();
  virtual Status Finalize() { return Flush(); }
  virtual Status InitNewFile() { return WriteFileHeader(); }
  virtual void Close();
  virtual uint64_t default_block_size() const { return 0; }
  virtual std::string file_extension() const { return "avro"; }

  /// Outputs the given rows into an HDFS sequence file. The rows are buffered
  /// to fill a sequence file block.
  virtual Status AppendRows(
      RowBatch* rows, const std::vector<int32_t>& row_group_indices, bool* new_file);

 private:
  /// Processes a single row, appending to out_
  void ConsumeRow(TupleRow* row);

  /// Adds an encoded field to out_
  inline void AppendField(const ColumnType& type, const void* value);

  /// Writes the Avro file header to HDFS
  Status WriteFileHeader();

  /// Writes the contents of out_ to HDFS as a single Avro file block.
  /// Returns an error if write to HDFS fails.
  Status Flush();

  /// Buffer which holds accumulated output
  WriteStream out_;

  /// Memory pool used by codec to allocate output buffer.
  /// Owned by this class. Initialized using parent's memtracker.
  boost::scoped_ptr<MemPool> mem_pool_;

  /// Number of rows consumed since last flush
  uint64_t unflushed_rows_;

  /// Name of codec, only set if codec_type_ != NONE
  std::string codec_name_;

  /// Type of the codec, will be NONE if no compression is used
  THdfsCompression::type codec_type_;

  /// The codec for compressing, only set if codec_type_ != NONE
  boost::scoped_ptr<Codec> compressor_;

  /// 16 byte sync marker (a uuid)
  std::string sync_marker_;
};

} // namespace impala
#endif
