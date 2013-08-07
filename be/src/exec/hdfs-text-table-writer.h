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


#ifndef IMPALA_EXEC_HDFS_TEXT_TABLE_WRITER_H
#define IMPALA_EXEC_HDFS_TEXT_TABLE_WRITER_H

#include <hdfs.h>

#include <sstream>

#include "runtime/descriptors.h"
#include "exec/hdfs-table-sink.h"
#include "exec/hdfs-table-writer.h"

namespace impala {

class Expr;
class TupleDescriptor;
class TupleRow;
class RuntimeState;
class StringValue;
struct OutputPartition;

// The writer consumes all rows passed to it and writes the evaluated output_exprs_
// as delimited text into Hdfs files.
class HdfsTextTableWriter : public HdfsTableWriter {
 public:
  HdfsTextTableWriter(HdfsTableSink* parent,
                      RuntimeState* state, OutputPartition* output,
                      const HdfsPartitionDescriptor* partition,
                      const HdfsTableDescriptor* table_desc,
                      const std::vector<Expr*>& output_exprs);

  ~HdfsTextTableWriter() { }

  // There is nothing to do for text.
  virtual Status Init() { return Status::OK; }
  virtual Status Finalize() { return Status::OK; }
  virtual Status InitNewFile() { return Status::OK; }
  virtual uint64_t default_block_size() { return 0; }

  // Appends delimited string representation of the rows in the batch to output partition.
  Status AppendRowBatch(RowBatch* current_row,
                        const std::vector<int32_t>& row_group_indices, bool* new_file);

 private:
  // Escapes occurrences of field_delim_ and escape_char_ with escape_char_ and
  // writes the escaped result into rowbatch_stringstream_. Neither Hive nor Impala
  // support escaping tuple_delim_.
  inline void PrintEscaped(const StringValue* str_val);

  // Character delimiting tuples.
  char tuple_delim_;

  // Character delimiting fields (to become slots).
  char field_delim_;

  // Escape character.
  char escape_char_;
  
  // Stringstream to buffer output.  The stream is cleared between HDFS
  // Write calls to allow for the internal buffers to be reused.
  std::stringstream rowbatch_stringstream_;
};

}
#endif
