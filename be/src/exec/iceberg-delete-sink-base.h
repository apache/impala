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

#include "exec/table-sink-base.h"

namespace impala {

class IcebergDeleteSinkConfig;
class MemTracker;
class RowBatch;
class RuntimeState;
class TupleRow;

class IcebergDeleteSinkBase : public TableSinkBase {
 public:
  IcebergDeleteSinkBase(TDataSinkId sink_id, const IcebergDeleteSinkConfig& sink_config,
      const std::string& name, RuntimeState* state);

  /// Prepares output_exprs and partition_key_exprs, and connects to HDFS.
  Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) override;

  /// Opens output_exprs and partition_key_exprs.
  Status Open(RuntimeState* state) override;

  TSortingOrder::type sorting_order() const override { return TSortingOrder::LEXICAL; }

 protected:
  /// Fills output_partition's partition_name, raw_partition_names and
  /// external_partition_name based on the row's columns. In case of partitioned
  /// tables 'row' must contain the Iceberg virtual columns PARTITION__SPEC__ID and
  /// ICEBERG__PARTITION__SERIALIZED. Every information needed for 'output_partition' can
  /// be retrieved from these fields and from the 'table_desc_'.
  Status ConstructPartitionInfo(const TupleRow* row, OutputPartition* output_partition);
  Status ConstructPartitionInfo(int32_t spec_id, const std::string& partitions,
      OutputPartition* output_partition);

  /// Returns the human-readable representation of a partition transform value. It is used
  /// to create the file paths. IcebergUtil.partitionDataFromDataFile() also expects
  /// partition values in this representation.
  /// E.g. if 'part_field' has transform MONTH and 'value' is "7" this function returns
  /// "1970-08". If 'part_field' has transform IDENTITY but the column is DATE we also
  /// need to transform the partition value to a human-readable format.
  /// Parse errors are set in 'transform_result'. If it is not OK, the return value
  /// of this function does not contain any meaningful value.
  std::string HumanReadablePartitionValue(
      const TIcebergPartitionField& part_field, const std::string& value,
      Status* transform_result);
};

}