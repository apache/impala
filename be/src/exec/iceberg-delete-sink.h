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

#include "exec/output-partition.h"
#include "exec/table-sink-base.h"

#include <unordered_map>

namespace impala {

class Expr;
class TupleDescriptor;
class TupleRow;
class RuntimeState;
class MemTracker;

class IcebergDeleteSinkConfig : public TableSinkBaseConfig {
 public:
  DataSink* CreateSink(RuntimeState* state) const override;

  ~IcebergDeleteSinkConfig() override {}

 protected:
  Status Init(const TDataSink& tsink, const RowDescriptor* input_row_desc,
      FragmentState* state) override;
};

class IcebergDeleteSink : public TableSinkBase {
 public:
  IcebergDeleteSink(TDataSinkId sink_id, const IcebergDeleteSinkConfig& sink_config,
    const TIcebergDeleteSink& hdfs_sink, RuntimeState* state);

  /// Prepares output_exprs and partition_key_exprs, and connects to HDFS.
  Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) override;

  /// Opens output_exprs and partition_key_exprs, prepares the single output partition for
  /// static inserts, and populates partition_descriptor_map_.
  Status Open(RuntimeState* state) override;

  /// Append all rows in batch to the temporary Hdfs files corresponding to partitions.
  Status Send(RuntimeState* state, RowBatch* batch) override;

  /// Finalize any open files.
  /// TODO: IMPALA-2988: Move calls to functions that can fail in Close() to FlushFinal()
  Status FlushFinal(RuntimeState* state) override;

  /// Closes writers, output_exprs and partition_key_exprs and releases resources.
  /// The temporary files will be moved to their final destination by the Coordinator.
  void Close(RuntimeState* state) override;

  TSortingOrder::type sorting_order() const override { return TSortingOrder::LEXICAL; }

  std::string DebugString() const override;

 private:
  /// Fills output_partition's partition_name, raw_partition_names and
  /// external_partition_name based on the row's columns. In case of partitioned
  /// tables 'row' must contain the Iceberg virtual columns PARTITION__SPEC__ID and
  /// ICEBERG__PARTITION__SERIALIZED. Every information needed for 'output_partition' can
  /// be retrieved from these fields and from the 'table_desc_'.
  Status ConstructPartitionInfo(
      const TupleRow* row,
      OutputPartition* output_partition) override;

  /// Returns the human-readable representation of a partition transform value. It is used
  /// to create the file paths. IcebergUtil.partitionDataFromDataFile() also expects
  /// partition values in this representation.
  std::string HumanReadablePartitionValue(
      TIcebergPartitionTransformType::type transform_type, const std::string& value,
      Status* transform_result);

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
};

}


