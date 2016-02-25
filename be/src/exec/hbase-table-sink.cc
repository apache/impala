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

#include "common/logging.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "gen-cpp/ImpalaInternalService_constants.h"

#include "common/names.h"

namespace impala {

const static string& ROOT_PARTITION_KEY =
    g_ImpalaInternalService_constants.ROOT_PARTITION_KEY;

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
                                        &output_expr_ctxs_));
  // Prepare the exprs to run.
  RETURN_IF_ERROR(
      Expr::Prepare(output_expr_ctxs_, state, row_desc_, expr_mem_tracker_.get()));
  return Status::OK();
}

Status HBaseTableSink::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(DataSink::Prepare(state));
  runtime_profile_ = state->obj_pool()->Add(
      new RuntimeProfile(state->obj_pool(), "HbaseTableSink"));
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  // Get the hbase table descriptor.  The table name will be used.
  table_desc_ = static_cast<HBaseTableDescriptor*>(
      state->desc_tbl().GetTableDescriptor(table_id_));
  // Prepare the expressions.
  RETURN_IF_ERROR(PrepareExprs(state));
  // Now that expressions are ready to materialize tuples, create the writer.
  hbase_table_writer_.reset(
      new HBaseTableWriter(table_desc_, output_expr_ctxs_, runtime_profile_));

  // Try and init the table writer.  This can create connections to HBase and
  // to zookeeper.
  RETURN_IF_ERROR(hbase_table_writer_->Init(state));

  // Add a 'root partition' status in which to collect insert statistics
  TInsertPartitionStatus root_status;
  root_status.__set_num_appended_rows(0L);
  root_status.__set_stats(TInsertStats());
  root_status.__set_id(-1L);
  state->per_partition_status()->insert(make_pair(ROOT_PARTITION_KEY, root_status));

  return Status::OK();
}

Status HBaseTableSink::Open(RuntimeState* state) {
  return Expr::Open(output_expr_ctxs_, state);
}

Status HBaseTableSink::Send(RuntimeState* state, RowBatch* batch, bool eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ExprContext::FreeLocalAllocations(output_expr_ctxs_);
  RETURN_IF_ERROR(state->CheckQueryState());
  // Since everything is set up just forward everything to the writer.
  RETURN_IF_ERROR(hbase_table_writer_->AppendRowBatch(batch));
  (*state->per_partition_status())[ROOT_PARTITION_KEY].num_appended_rows +=
      batch->num_rows();
  return Status::OK();
}

void HBaseTableSink::Close(RuntimeState* state) {
  if (closed_) return;
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  if (hbase_table_writer_.get() != NULL) {
    hbase_table_writer_->Close(state);
    hbase_table_writer_.reset(NULL);
  }
  Expr::Close(output_expr_ctxs_, state);
  DataSink::Close(state);
  closed_ = true;
}

}  // namespace impala
