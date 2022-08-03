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

#ifndef IMPALA_EXEC_HBASE_TABLE_SINK_H
#define IMPALA_EXEC_HBASE_TABLE_SINK_H

#include <vector>

#include "common/status.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/descriptors.h"
#include "exec/data-sink.h"
#include "exec/hbase/hbase-table-writer.h"
#include "gen-cpp/Data_types.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

class HBaseTableSinkConfig : public DataSinkConfig {
 public:
  DataSink* CreateSink(RuntimeState* state) const override;

  ~HBaseTableSinkConfig() override {}
};

/// Class to take row batches and send them to the HBaseTableWriter to
/// eventually be written into an HBase table.
class HBaseTableSink : public DataSink {
 public:
  HBaseTableSink(
      TDataSinkId sink_id, const DataSinkConfig& sink_config, RuntimeState* state);
  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker);
  virtual Status Send(RuntimeState* state, RowBatch* batch);
  virtual Status FlushFinal(RuntimeState* state);
  virtual void Close(RuntimeState* state);

 private:
  /// Used to get the HBaseTableDescriptor from the RuntimeState
  TableId table_id_;

  /// The description of the table.  Used for table name and column mapping.
  HBaseTableDescriptor* table_desc_;

  /// The object that this sink uses to write to hbase.
  /// hbase_table_writer is owned by this sink and should be closed
  /// when this is Close'd.
  boost::scoped_ptr<HBaseTableWriter> hbase_table_writer_;
};

}  // namespace impala

#endif  // IMPALA_EXEC_HBASE_TABLE_SINK_H
