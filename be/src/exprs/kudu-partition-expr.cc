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

#include "exprs/kudu-partition-expr.h"

#include <kudu/common/partial_row.h>

#include "exec/kudu-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/types.h"

#include "common/names.h"

namespace impala {

/// Per-thread context for KuduPartitionExpr.
struct KuduPartitionExprCtx {
  /// Used to call into Kudu to determine partitions.
  unique_ptr<kudu::client::KuduPartitioner> partitioner;

  /// Stores the col values for each row that is partitioned.
  unique_ptr<kudu::KuduPartialRow> row;
};

KuduPartitionExpr::KuduPartitionExpr(const TExprNode& node)
  : ScalarExpr(node), tkudu_partition_expr_(node.kudu_partition_expr) {}

Status KuduPartitionExpr::Init(
    const RowDescriptor& row_desc, bool is_entry_point, FragmentState* state) {
  RETURN_IF_ERROR(ScalarExpr::Init(row_desc, is_entry_point, state));
  DCHECK_EQ(tkudu_partition_expr_.referenced_columns.size(), children_.size());

  // Create the KuduPartitioner we'll use to get the partition index for each row.
  TableDescriptor* table_desc =
      state->desc_tbl().GetTableDescriptor(tkudu_partition_expr_.target_table_id);
  DCHECK(table_desc != nullptr);
  DCHECK(dynamic_cast<KuduTableDescriptor*>(table_desc))
      << "Target table for KuduPartitioner must be a Kudu table.";
  table_desc_ = static_cast<KuduTableDescriptor*>(table_desc);
  kudu::client::KuduClient* client;
  RETURN_IF_ERROR(ExecEnv::GetInstance()->GetKuduClient(
      table_desc_->kudu_master_addresses(), &client));
  KUDU_RETURN_IF_ERROR(client->OpenTable(table_desc_->table_name(), &table_),
      "Failed to open Kudu table.");
  return Status::OK();
}

Status KuduPartitionExpr::OpenEvaluator(FunctionContext::FunctionStateScope scope,
    RuntimeState* state, ScalarExprEvaluator* eval) const {
  RETURN_IF_ERROR(ScalarExpr::OpenEvaluator(scope, state, eval));

  DCHECK_GE(fn_ctx_idx_, 0);
  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
  kudu::client::KuduPartitionerBuilder b(table_);
  kudu::client::KuduPartitioner* partitioner;
  KUDU_RETURN_IF_ERROR(b.Build(&partitioner), "Failed to build Kudu partitioner.");

  // Ownership of the KuduPartitioner and PartialRow transfer to the function state.
  // They are destroyed in CloseEvaluator().
  fn_ctx->SetFunctionState(FunctionContext::THREAD_LOCAL,
      new KuduPartitionExprCtx{unique_ptr<kudu::client::KuduPartitioner>(partitioner),
          unique_ptr<kudu::KuduPartialRow>(table_->schema().NewRow())});
  return Status::OK();
}

void KuduPartitionExpr::CloseEvaluator(FunctionContext::FunctionStateScope scope,
    RuntimeState* state, ScalarExprEvaluator* eval) const {
  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
  KuduPartitionExprCtx* ctx = reinterpret_cast<KuduPartitionExprCtx*>(
      fn_ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  // The destructor of KuduPartitionExprCtx handles all cleanup. Note that 'ctx' may be
  // NULL, in which case this is a no-op.
  delete ctx;
}

IntVal KuduPartitionExpr::GetIntValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
  KuduPartitionExprCtx* ctx = reinterpret_cast<KuduPartitionExprCtx*>(
      fn_ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  DCHECK(ctx != nullptr);
  kudu::KuduPartialRow* kudu_row = ctx->row.get();
  for (int i = 0; i < children_.size(); ++i) {
    void* val = eval->GetValue(*GetChild(i), row);
    if (val == NULL) {
      // We don't currently support nullable partition columns, but pass it along and let
      // the KuduTableSink generate the error message.
      return IntVal(-1);
    }
    int col = tkudu_partition_expr_.referenced_columns[i];
    const ColumnDescriptor& col_desc = table_desc_->col_descs()[col];
    const ColumnType& type = col_desc.type();
    DCHECK_EQ(GetChild(i)->type().type, type.type);
    Status s = WriteKuduValue(col, type, val, false, kudu_row);
    // This can only fail if we set a col to an incorect type, which would be a bug in
    // planning, so we can DCHECK.
    DCHECK(s.ok()) << "WriteKuduValue failed for col = " << col_desc.name()
                   << " and type = " << col_desc.type() << ": " << s.GetDetail();
  }

  int32_t kudu_partition = -1;
  kudu::Status s = ctx->partitioner->PartitionRow(*kudu_row, &kudu_partition);
  // This can only fail if we fail to supply some of the partition cols, which would be a
  // bug in planning, so we can DCHECK.
  DCHECK(s.ok()) << "KuduPartitioner::PartitionRow failed on row = '"
                 << kudu_row->ToString() << "': " << s.ToString();
  return IntVal(kudu_partition);
}

Status KuduPartitionExpr::GetCodegendComputeFnImpl(
    LlvmCodeGen* codegen, llvm::Function** fn) {
  return GetCodegendComputeFnWrapper(codegen, fn);
}

} // namespace impala
