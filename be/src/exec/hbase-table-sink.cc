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

#include "exec/hbase-table-sink.h"

#include <vector>

#include "util/logging.h"
#include "exprs/expr.h"

using namespace std;

namespace impala {

HBaseTableSink::HBaseTableSink(const RowDescriptor& row_desc,
                               const vector<TExpr>& select_list_texprs,
                               const TDataSink& tsink)
    : table_id_(tsink.table_sink.target_table_id),
      table_desc_(NULL),
      hbase_table_writer_(NULL),
      row_desc_(row_desc),
      select_list_texprs_(select_list_texprs) {
}

Status HBaseTableSink::PrepareExprs(RuntimeState* state) {
  // From the thrift expressions create the real exprs.
  RETURN_IF_ERROR(Expr::CreateExprTrees(state->obj_pool(), select_list_texprs_,
      &output_exprs_));
  // Prepare the exprs to run.
  RETURN_IF_ERROR(Expr::Prepare(output_exprs_, state, row_desc_));
  return Status::OK;
}

Status HBaseTableSink::Init(RuntimeState* state) {
  runtime_profile_ = state->obj_pool()->Add(
      new RuntimeProfile(state->obj_pool(), "HbaseTableSink"));
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  // Get the hbase table descriptor.  The table name will be used.
  table_desc_ = static_cast<HBaseTableDescriptor*>(
      state->desc_tbl().GetTableDescriptor(table_id_));
  // Prepare the expressions.
  RETURN_IF_ERROR(PrepareExprs(state));
  // Now that expressions are ready to materialize tuples, create the writer.
  hbase_table_writer_.reset(new HBaseTableWriter(table_desc_, output_exprs_));

  // Try and init the table writer.  This can create connections to HBase and
  // to zookeeper.
  RETURN_IF_ERROR(hbase_table_writer_->Init(state));

  (*state->num_appended_rows())[""] = 0L;

  return Status::OK;
}

Status HBaseTableSink::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  // Since everything is set up just forward everything to the writer.
  RETURN_IF_ERROR(hbase_table_writer_->AppendRowBatch(batch));
  (*state->num_appended_rows())[""] += batch->num_rows();
  return Status::OK;
}

Status HBaseTableSink::Close(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  if (hbase_table_writer_.get() != NULL) {
    RETURN_IF_ERROR(hbase_table_writer_->Close(state));
    hbase_table_writer_.reset(NULL);
  }

  // Return OK even if the hbase_table_writer_ was null.
  // Assume that there's nothing to close if it's null.
  return Status::OK;
}

}  // namespace impala
