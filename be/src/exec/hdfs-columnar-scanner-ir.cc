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

#include "exec/exec-node.inline.h"
#include "exec/hdfs-columnar-scanner.h"
#include "runtime/row-batch.h"
#include "exec/scratch-tuple-batch.h"

namespace impala {

int HdfsColumnarScanner::ProcessScratchBatch(RowBatch* dst_batch) {
  DCHECK(scratch_batch_ != nullptr);
  ScalarExprEvaluator* const* conjunct_evals = conjunct_evals_->data();
  const int num_conjuncts = conjunct_evals_->size();

  // Start/end/current iterators over the output rows.
  Tuple** output_row_start =
      reinterpret_cast<Tuple**>(dst_batch->GetRow(dst_batch->num_rows()));
  Tuple** output_row_end =
      output_row_start + (dst_batch->capacity() - dst_batch->num_rows());
  Tuple** output_row = output_row_start;

  // Start/end/current iterators over the scratch tuples.
  uint8_t* scratch_tuple_start = scratch_batch_->CurrTuple();
  uint8_t* scratch_tuple_end = scratch_batch_->TupleEnd();
  uint8_t* scratch_tuple = scratch_tuple_start;
  const int tuple_size = scratch_batch_->tuple_byte_size;

  // Loop until the scratch batch is exhausted or the output batch is full.
  // Do not use batch_->AtCapacity() in this loop because it is not necessary
  // to perform the memory capacity check.
  bool* is_selected = scratch_batch_->selected_rows.get() + scratch_batch_->tuple_idx;
  while (scratch_tuple != scratch_tuple_end) {
    *output_row = reinterpret_cast<Tuple*>(scratch_tuple);
    scratch_tuple += tuple_size;
    // Evaluate runtime filters and conjuncts. Short-circuit the evaluation if
    // the filters/conjuncts are empty to avoid function calls.
    if (!EvalRuntimeFilters(reinterpret_cast<TupleRow*>(output_row))) {
      *is_selected++ = false;
      continue;
    }
    if (!ExecNode::EvalConjuncts(conjunct_evals, num_conjuncts,
        reinterpret_cast<TupleRow*>(output_row))) {
      *is_selected++ = false;
      continue;
    }
    // Row survived runtime filters and conjuncts.
    *is_selected++ = true;
    ++output_row;
    if (output_row == output_row_end) break;
  }
  scratch_batch_->tuple_idx += (scratch_tuple - scratch_tuple_start) / tuple_size;
  return output_row - output_row_start;
}

}
