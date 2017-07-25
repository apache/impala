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

#include "exec/hbase-table-sink.h"

#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "gen-cpp/ImpalaInternalService_constants.h"
#include "runtime/mem-tracker.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

const static string& ROOT_PARTITION_KEY =
    g_ImpalaInternalService_constants.ROOT_PARTITION_KEY;

HBaseTableSink::HBaseTableSink(const RowDescriptor* row_desc, const TDataSink& tsink)
  : DataSink(row_desc),
    table_id_(tsink.table_sink.target_table_id),
    table_desc_(NULL),
    hbase_table_writer_(NULL) {
  DCHECK(tsink.__isset.table_sink);
}

Status HBaseTableSink::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  SCOPED_TIMER(profile()->total_time_counter());

  // Get the hbase table descriptor. The table name will be used.
  table_desc_ = static_cast<HBaseTableDescriptor*>(
      state->desc_tbl().GetTableDescriptor(table_id_));
  // Now that expressions are ready to materialize tuples, create the writer.
  hbase_table_writer_.reset(
      new HBaseTableWriter(table_desc_, output_expr_evals_, profile()));

  // Try and init the table writer. This can create connections to HBase and
  // to zookeeper.
  RETURN_IF_ERROR(hbase_table_writer_->Init(state));

  // Add a 'root partition' status in which to collect insert statistics
  TInsertPartitionStatus root_status;
  root_status.__set_num_modified_rows(0L);
  root_status.__set_stats(TInsertStats());
  root_status.__set_id(-1L);
  state->per_partition_status()->insert(make_pair(ROOT_PARTITION_KEY, root_status));

  return Status::OK();
}

Status HBaseTableSink::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  expr_results_pool_->Clear();
  RETURN_IF_ERROR(state->CheckQueryState());
  // Since everything is set up just forward everything to the writer.
  RETURN_IF_ERROR(hbase_table_writer_->AppendRows(batch));
  (*state->per_partition_status())[ROOT_PARTITION_KEY].num_modified_rows +=
      batch->num_rows();
  return Status::OK();
}

Status HBaseTableSink::FlushFinal(RuntimeState* state) {
  // No buffered state to flush.
  return Status::OK();
}

void HBaseTableSink::Close(RuntimeState* state) {
  if (closed_) return;
  SCOPED_TIMER(profile()->total_time_counter());

  if (hbase_table_writer_.get() != NULL) {
    hbase_table_writer_->Close(state);
    hbase_table_writer_.reset(NULL);
  }
  DataSink::Close(state);
  closed_ = true;
}

}  // namespace impala
