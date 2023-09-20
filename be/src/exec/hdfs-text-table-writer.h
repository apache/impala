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


#ifndef IMPALA_EXEC_HDFS_TEXT_TABLE_WRITER_H
#define IMPALA_EXEC_HDFS_TEXT_TABLE_WRITER_H

#include <hdfs.h>
#include <sstream>
#include <boost/scoped_ptr.hpp>

#include "runtime/descriptors.h"
#include "exec/hdfs-table-sink.h"
#include "exec/hdfs-table-writer.h"

namespace impala {

class Codec;
class Expr;
class MemPool;
struct OutputPartition;
class RuntimeState;
class StringValue;
class TupleDescriptor;
class TupleRow;

/// The writer consumes all rows passed to it and writes the evaluated output_exprs_
/// as delimited text into Hdfs files.
class HdfsTextTableWriter : public HdfsTableWriter {
 public:
  HdfsTextTableWriter(TableSinkBase* parent,
      RuntimeState* state, OutputPartition* output,
      const HdfsPartitionDescriptor* partition,
      const HdfsTableDescriptor* table_desc);

  ~HdfsTextTableWriter() { }

  virtual Status Init();
  virtual Status Finalize();
  virtual Status InitNewFile();
  virtual void Close();
  virtual uint64_t default_block_size() const;
  virtual std::string file_extension() const;

  /// Appends delimited string representation of the rows in the batch to output partition.
  /// The resulting output is buffered until HDFS_FLUSH_WRITE_SIZE before being written
  /// to HDFS.
  Status AppendRows(RowBatch* current_row, const std::vector<int32_t>& row_group_indices,
      bool* new_file);

 private:
  /// Escapes occurrences of field_delim_ and escape_char_ with escape_char_ and
  /// writes the escaped result into rowbatch_stringstream_. Neither Hive nor Impala
  /// support escaping tuple_delim_.
  inline void PrintEscaped(const StringValue* str_val);

  /// Writes the buffered data in rowbatch_stringstream_ to HDFS, applying
  /// compression if necessary.
  Status Flush();

  /// Character delimiting tuples.
  char tuple_delim_;

  /// Character delimiting fields (to become slots).
  char field_delim_;

  /// Escape character.
  char escape_char_;

  /// Size in rowbatch_stringstream_ before we call flush.
  int64_t flush_size_;

  /// Stringstream to buffer output.  The stream is cleared between HDFS
  /// Write calls to allow for the internal buffers to be reused.
  std::stringstream rowbatch_stringstream_;
};

}
#endif
