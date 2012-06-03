// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HDFS_TABLE_WRITER_H
#define IMPALA_EXEC_HDFS_TABLE_WRITER_H

#include <hdfs.h>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "runtime/descriptors.h"
#include "exec/hdfs-table-sink.h"
#include "util/hdfs-util.h"

namespace impala {

// Pure virtual class for writing to hdfs table partition files.
// Subclasses implement the code needed to write to a specific file type.
// A subclass needs to implement functions to format and add rows to the file
// and to do whatever processing is needed prior to closing the file.
class HdfsTableWriter {
 public:
  // The implementation of a writer may reference the parameters to the constructor
  // during the lifetime of the object.
  // output -- Information on the output partition file.
  // partition -- the descriptor for the partition being written
  // table_desc -- the descriptor for the table being written.
  // output_exprs -- expressions which generate the output values.
  HdfsTableWriter(OutputPartition* output,
                  const HdfsPartitionDescriptor* partition,
                  const HdfsTableDescriptor* table_desc,
                  const std::vector<Expr*>& output_exprs);

  virtual ~HdfsTableWriter() { }

  // Appends the current row to the partition.
  virtual Status AppendRow(TupleRow* current_row) = 0;

  // Finalize this partition. The writer needs to finish processing
  // all data have written out after the return from this call.
  virtual Status Finalize(OutputPartition* output) = 0;

 protected:
  // Write to the current hdfs file.
  Status Write(const char* data, int32_t len) {
    return Write(reinterpret_cast<const uint8_t*>(data), len);
  }
  Status Write(const uint8_t* data, int32_t len);

  // Structure describing partition written to by this writer.
  OutputPartition* output_;

  // Table descriptor of table to be written.
  const HdfsTableDescriptor* table_desc_;

  // Expressions that materialize output values.
  std::vector<Expr*> output_exprs_;
};
}
#endif
