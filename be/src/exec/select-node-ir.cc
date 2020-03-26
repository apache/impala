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

#include "exec/select-node.h"

#include "exec/exec-node.inline.h"
#include "runtime/tuple-row.h"

using namespace impala;

void SelectNode::CopyRows(RowBatch* output_batch) {
  ScalarExprEvaluator* const* conjunct_evals = conjunct_evals_.data();
  int num_conjuncts = conjuncts_.size();
  DCHECK_EQ(num_conjuncts, conjunct_evals_.size());

  FOREACH_ROW(child_row_batch_.get(), child_row_idx_, batch_iter) {
    // Add a new row to output_batch
    int dst_row_idx = output_batch->AddRow();
    TupleRow* dst_row = output_batch->GetRow(dst_row_idx);
    TupleRow* src_row = batch_iter.Get();
    ++child_row_idx_;
    if (EvalConjuncts(conjunct_evals, num_conjuncts, src_row)) {
      output_batch->CopyRow(src_row, dst_row);
      output_batch->CommitLastRow();
      IncrementNumRowsReturned(1);
      if (ReachedLimit() || output_batch->AtCapacity()) return;
    }
  }
}
