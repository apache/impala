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

#include "exprs/scalar-expr-evaluator.h"

namespace impala {

void KuduPartitionExpr::SetKuduPartialRowAndPartitioner(
    ScalarExprEvaluator* eval, int fn_ctx_idx,
    kudu::KuduPartialRow** row, kudu::client::KuduPartitioner** partitioner) {
  DCHECK(row != nullptr);
  DCHECK(partitioner != nullptr);

  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx);
  KuduPartitionExprCtx* ctx = reinterpret_cast<KuduPartitionExprCtx*>(
      fn_ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  DCHECK(ctx != nullptr);

  kudu::KuduPartialRow* kudu_row = ctx->row.get();
  DCHECK(kudu_row != nullptr);

  kudu::client::KuduPartitioner* kudu_partitioner = ctx->partitioner.get();
  DCHECK(kudu_partitioner != nullptr);

  *row = kudu_row;
  *partitioner = kudu_partitioner;
}

IntVal GetKuduPartitionRow(kudu::client::KuduPartitioner* partitioner,
    kudu::KuduPartialRow* row){
  int32_t partition_id = -1;
  kudu::Status s = partitioner->PartitionRow(*row, &partition_id);
  // This can only fail if we fail to supply some of the partition cols, which would be a
  // bug in planning, so we can DCHECK.
  DCHECK(s.ok()) << "KuduPartitioner::PartitionRow failed on row = '"
      << row->ToString() << "': " << s.ToString();
  return IntVal(partition_id);
}

}
