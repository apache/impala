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

#include "exec/cte-consumer-node.h"
#include "runtime/tuple-row.h"

using namespace impala;

void IR_ALWAYS_INLINE CTEConsumerNode::MaterializeExprs(
    const vector<ScalarExprEvaluator*>& evals, TupleRow* row, uint8_t* tuple_buf,
    RowBatch* dst_batch) {
  DCHECK(!dst_batch->AtCapacity());
  Tuple* dst_tuple = reinterpret_cast<Tuple*>(tuple_buf);
  TupleRow* dst_row = dst_batch->GetRow(dst_batch->AddRow());
  dst_tuple->MaterializeExprs<false, false>(row, *tuple_desc_, evals,
      dst_batch->tuple_data_pool());
  dst_row->SetTuple(0, dst_tuple);
  dst_batch->CommitLastRow();
}

void CTEConsumerNode::MaterializeBatch(
    RowBatch* input_batch, RowBatch* dst_batch, uint8_t** tuple_buf) {
  // Take all references to member variables out of the loop to reduce the number of
  // loads and stores.
  int tuple_byte_size = tuple_desc_->byte_size();
  uint8_t* cur_tuple = *tuple_buf;

  int num_rows_to_process = std::min(input_batch->num_rows(),
      dst_batch->capacity() - dst_batch->num_rows());
  FOREACH_ROW_LIMIT(input_batch, 0, num_rows_to_process, batch_iter) {
    MaterializeExprs(input_expr_evals_, batch_iter.Get(), cur_tuple, dst_batch);
    cur_tuple += tuple_byte_size;
  }

  *tuple_buf = cur_tuple;
}
