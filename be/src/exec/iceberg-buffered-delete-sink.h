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
#include "runtime/string-value.h"

#include <unordered_map>

namespace impala {

class MemPool;

/// IcebergBufferedDeleteSink buffers the Iceberg position delete records in its
/// internal data structures. In FlushFinal() it sorts the buffered records (per
/// partition) and writes them out to position delete files (writing one partition
/// at a time). So the main difference between IcebergBufferedDeleteSink and
/// IcebergDeleteSink is that IcebergBufferedDeleteSink doesn't assume a sorted
/// input of delete records.
class IcebergBufferedDeleteSink : public IcebergDeleteSinkBase {
 public:
  IcebergBufferedDeleteSink(TDataSinkId sink_id,
      const IcebergDeleteSinkConfig& sink_config,
      RuntimeState* state);

  /// Prepares output_exprs and partition_key_exprs, and connects to HDFS.
  Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) override;

  /// Opens output_exprs and partition_key_exprs.
  Status Open(RuntimeState* state) override;

  /// Buffers incoming row batches in 'current_partition_'.
  Status Send(RuntimeState* state, RowBatch* batch) override;

  /// Writes the buffered records to position delete files in the correct order.
  Status FlushFinal(RuntimeState* state) override;

  /// Releases buffers and merges the member 'dml_exec_state_' to
  /// 'state->dml_exec_state()'.
  void Close(RuntimeState* state) override;

  std::string DebugString() const override;

 private:
  /// Contains partition spec id and the encoded partition values.
  typedef std::pair<int32_t, std::string> PartitionInfo;
  /// Contains the filepaths and the corresponding file positions of deleted records.
  /// It's necessary to use a std::map so we can get back the file paths in order.
  typedef std::map<StringValue, std::vector<int64_t>> FilePositions;

  /// Nested iterator class to conveniently iterate over a FilePositions object.
  class FilePositionsIterator;

  /// Retreives partition information from 'row'.
  PartitionInfo GetPartitionInfo(TupleRow* row);

  /// Retreives delete record data from 'row'.
  std::pair<impala_udf::StringVal, int64_t> GetDeleteRecord(TupleRow* row);

  /// Iterates over 'batch' and stores delete records in 'partitions_to_file_positions_'.
  Status BufferDeleteRecords(RowBatch* batch);

  /// Sorts the buffered records.
  void SortBufferedRecords();

  /// Logs the buffered records at VLOG_ROW level.
  void VLogBufferedRecords();

  /// Verifies that there are no duplicates in the buffered delete records. It assumes
  /// that SortBufferedRecords() has been called already.
  Status VerifyBufferedRecords();

  /// Writes all buffered delete records to position delete files.
  Status FlushBufferedRecords(RuntimeState* state);

  /// Registers the referenced data files in dml_exec_state_
  void RegisterDataFilesInDmlExecState();

  /// Initializes an empty output batch.
  Status InitializeOutputRowBatch(RowBatch* batch);

  /// Uses 'iterator' to write delete records from 'partitions_to_file_positions_'
  /// to 'batch'.
  Status GetNextRowBatch(RowBatch* batch, FilePositionsIterator* iterator);

  /// Writes a single delete record <filepah, offset> to 'row'.
  void WriteRow(StringValue filepath, int64_t offset, TupleRow* row);

  /// Tries to allocate a buffer with size 'buffer_size'. Returns error when cannot
  /// serve the request.
  Status TryAllocateUnalignedBuffer(int buffer_size, uint8_t** buffer);

  /// Sets the 'current_partition_' based on 'spec_id' and 'partitions'.
  Status SetCurrentPartition(RuntimeState* state, int32_t spec_id,
      const std::string& partitions);

  /// Timer for 'SortBufferedRecords()'.
  RuntimeProfile::Counter* position_sort_timer_;

  /// Timer for 'FlushBufferedRecords()'.
  RuntimeProfile::Counter* flush_timer_;

  /// Buffer pool to serve allocation requests.
  std::unique_ptr<MemPool> buffered_delete_pool_;

  /// The sink writes partitions one-by-one.
  std::unique_ptr<OutputPartition> current_partition_;

  /// We collect the delete records from the input row batches in this member.
  std::unordered_map<PartitionInfo, FilePositions> partitions_to_file_positions_;

  /// This sink has its own DmlExecState object because in the context of UPADTEs we
  /// cannot modify the same DmlExecState object simultaneously (from the INSERT and
  /// DELETE sinks). It is merged into state->dml_exec_state() in Close().
  DmlExecState dml_exec_state_;
};

}
