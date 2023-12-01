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

#pragma once

#include "exec/iceberg-delete-sink-base.h"
#include "exec/output-partition.h"
#include "exec/table-sink-base.h"

#include <unordered_map>

namespace impala {

class Expr;
class IcebergDeleteSinkConfig;
class TupleDescriptor;
class TupleRow;
class MemTracker;

class IcebergDeleteSink : public IcebergDeleteSinkBase {
 public:
  IcebergDeleteSink(TDataSinkId sink_id, const IcebergDeleteSinkConfig& sink_config,
    RuntimeState* state);

  /// Prepares output_exprs and partition_key_exprs, and connects to HDFS.
  Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) override;

  /// Opens output_exprs and partition_key_exprs.
  Status Open(RuntimeState* state) override;

  /// Append all rows in batch to position delete files. It is assumed that
  /// that rows are ordered by partitions, filepaths, and positions.
  Status Send(RuntimeState* state, RowBatch* batch) override;

  /// Finalize any open files.
  /// TODO: IMPALA-2988: Move calls to functions that can fail in Close() to FlushFinal()
  Status FlushFinal(RuntimeState* state) override;

  /// Closes writers, output_exprs and partition_key_exprs and releases resources.
  void Close(RuntimeState* state) override;

  std::string DebugString() const override;

 private:
  /// Verifies that the row batch does not contain duplicated rows. This can only happen
  /// in the context of UPDATE FROM statements when we are updating a table based on
  /// another table, e.g.:
  /// UPDATE t SET t.x = s.x FROM ice_t t, source_tbl s where t.id = s.id;
  /// Now, if 'source_tbl' has duplicate rows then the JOIN operator would produce
  /// multiple matches for the same row, and we would insert them to the table.
  /// Therefore, we should always raise an error if we find duplicated rows (i.e rows
  /// having the same filepath + position), because that would corrupt the table data
  /// and the delete files as well.
  /// For a case where deduplication is not possible at the sink level, see the comment
  /// in IcebergUpdateImpl.buildAndValidateSelectExprs() in the Frontend Java code.
  Status VerifyRowsNotDuplicated(RowBatch* batch);

  /// Maps all rows in 'batch' to partitions and appends them to their temporary Hdfs
  /// files. The input must be ordered by the partition key expressions.
  Status WriteClusteredRowBatch(RuntimeState* state, RowBatch* batch) WARN_UNUSED_RESULT;

  /// Sets and initializes the 'current_partition_' based on key. For unpartitioned tables
  /// it is only invoked once to initialize the only output partition.
  /// For partitioned tables the rows are clustered based on partition data, i.e. when the
  /// key changes we initialize a new output partition.
  Status SetCurrentPartition(RuntimeState* state, const TupleRow* row,
      const std::string& key) WARN_UNUSED_RESULT;

  /// The sink writes partitions one-by-one.
  PartitionPair current_partition_;

  /// Variables necessary for validating that row batches don't contain duplicates.
  std::string prev_file_path_;
  int64_t prev_position_ = -1;
};

}


