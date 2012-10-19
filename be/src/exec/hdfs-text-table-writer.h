// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HDFS_TEXT_TABLE_WRITER_H
#define IMPALA_EXEC_HDFS_TEXT_TABLE_WRITER_H

#include <hdfs.h>

#include "runtime/descriptors.h"
#include "exec/hdfs-table-sink.h"
#include "exec/hdfs-table-writer.h"

namespace impala {

class Expr;
class TupleDescriptor;
class TupleRow;
class RuntimeState;
class OutputPartition;

// The writer consumes all rows passed to it and writes the evaluated output_exprs_
// as delimited text into Hdfs files.
class HdfsTextTableWriter : public HdfsTableWriter {
 public:
  HdfsTextTableWriter(RuntimeState* state, OutputPartition* output,
                      const HdfsPartitionDescriptor* partition,
                      const HdfsTableDescriptor* table_desc,
                      const std::vector<Expr*>& output_exprs);

  ~HdfsTextTableWriter() { }

  // There is nothing to do for text.
  virtual Status Init() { return Status::OK; }
  virtual Status Finalize() { return Status::OK; }

  // Appends delimited string representation of the rows in the batch to output partition.
  Status AppendRowBatch(RowBatch* current_row,
                        const std::vector<int32_t>& row_group_indices, bool* new_file);

 private:
  // Character delimiting tuples.
  char tuple_delim_;

  // Character delimiting fields (to become slots).
  char field_delim_;

  // Escape character. TODO: Escape output.
  char escape_char_;
};

}
#endif
