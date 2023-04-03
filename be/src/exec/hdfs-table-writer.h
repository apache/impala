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


#ifndef IMPALA_EXEC_HDFS_TABLE_WRITER_H
#define IMPALA_EXEC_HDFS_TABLE_WRITER_H

#include <vector>
#include "common/hdfs.h"

#include "common/status.h"
#include "gen-cpp/control_service.pb.h"

namespace impala {

class HdfsPartitionDescriptor;
class HdfsTableDescriptor;
class TableSinkBase;
struct OutputPartition;
class RowBatch;
class RuntimeState;
class ScalarExprEvaluator;

/// Per column statistics to be written to Iceberg manifest files (for each data file).
/// min_binary and max_binary members contain Single-Value serialized lower and upper
/// column stats
/// (https://iceberg.apache.org/spec/#appendix-d-single-value-serialization).
struct IcebergColumnStats {
  bool has_min_max_values;
  std::string min_binary;
  std::string max_binary;
  int64_t value_count;
  int64_t null_count;
  int64_t column_size;
};

typedef std::unordered_map<int, IcebergColumnStats> IcebergFileStats;

/// Pure virtual class for writing to hdfs table partition files.
/// Subclasses implement the code needed to write to a specific file type.
/// A subclass needs to implement functions to format and add rows to the file
/// and to do whatever processing is needed prior to closing the file.
class HdfsTableWriter {
 public:
  /// The implementation of a writer may reference the parameters to the constructor
  /// during the lifetime of the object.
  /// output_partition -- Information on the output partition file.
  /// partition -- the descriptor for the partition being written
  /// table_desc -- the descriptor for the table being written.
  HdfsTableWriter(TableSinkBase* parent,
                  RuntimeState* state, OutputPartition* output_partition,
                  const HdfsPartitionDescriptor* partition_desc,
                  const HdfsTableDescriptor* table_desc);

  virtual ~HdfsTableWriter() { }

  /// The sequence of calls to this object are:
  /// 1. Init()
  /// 2. InitNewFile()
  /// 3. AppendRows() - called repeatedly
  /// 4. Finalize()
  /// For files formats that are splittable (and therefore can be written to an
  /// arbitrarily large file), 1-4 is called once.
  /// For files formats that are not splittable (i.e. columnar formats, compressed
  /// text), 1) is called once and 2-4) is called repeatedly for each file.

  /// Do initialization of writer.
  virtual Status Init() WARN_UNUSED_RESULT = 0;

  /// Called when a new file is started.
  virtual Status InitNewFile() WARN_UNUSED_RESULT = 0;

  /// Appends rows of 'batch' to the partition that are selected via 'row_group_indices',
  /// and if the latter is empty, appends every row.
  /// If the current file is full, the writer stops appending and returns with
  /// *new_file == true. A new file will be opened and the same row batch will be passed
  /// again. The writer must track how much of the batch it had already processed asking
  /// for a new file. Otherwise the writer will return with *newfile == false.
  virtual Status AppendRows(RowBatch* batch,
      const std::vector<int32_t>& row_group_indices,
      bool* new_file) WARN_UNUSED_RESULT = 0;

  /// Finalize this partition. The writer needs to finish processing
  /// all data have written out after the return from this call.
  /// This is called once for each call to InitNewFile()
  virtual Status Finalize() WARN_UNUSED_RESULT = 0;

  /// Called once when this writer should cleanup any resources.
  virtual void Close() = 0;

  /// Returns the stats for this writer.
  const DmlStatsPB& stats() const { return stats_; }

  /// Returns the stats for the latest iceberg file written by this writer.
  const IcebergFileStats& iceberg_file_stats() const { return iceberg_file_stats_; }

  /// Default block size to use for this file format.  If the file format doesn't
  /// care, it should return 0 and the hdfs config default will be used.
  virtual uint64_t default_block_size() const = 0;

  /// Returns the file extension for this writer.
  virtual std::string file_extension() const = 0;

 protected:
  /// Size to buffer output before calling Write() (which calls hdfsWrite), in bytes
  /// to minimize the overhead of Write()
  static const int HDFS_FLUSH_WRITE_SIZE = 50 * 1024;

  /// Write to the current hdfs file.
  Status Write(const char* data, int32_t len) {
    return Write(reinterpret_cast<const uint8_t*>(data), len);
  }
  Status Write(const uint8_t* data, int32_t len);

  template<typename T>
  Status Write(T v) {
    return Write(reinterpret_cast<uint8_t*>(&v), sizeof(T));
  }

  /// Parent table sink object
  TableSinkBase* parent_;

  /// Runtime state.
  RuntimeState* state_;

  /// Structure describing partition written to by this writer.
  /// NOTE: OutputPartition is usually accessed with a unique_ptr. It is safe to use
  /// a raw pointer here, because the OutputPartition maintains a scoped_ptr to
  /// the HdfsTableWriter objects. This will never outlive the OutputPartition.
  OutputPartition* output_;

  /// Table descriptor of table to be written.
  const HdfsTableDescriptor* table_desc_;

  /// Reference to the evaluators of expressions which generate the output value.
  /// The evaluators are owned by sink which owns this table writer.
  const std::vector<ScalarExprEvaluator*>& output_expr_evals_;

  /// Subclass should populate any file format specific stats.
  DmlStatsPB stats_;

  /// Contains the per-column stats for the latest file written by this writer.
  /// Used with iceberg only.
  IcebergFileStats iceberg_file_stats_;
};
}
#endif
