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

#include "exec/topn-node.h"

using namespace impala;

void TopNNode::InsertBatch(RowBatch* batch) {
  // TODO: after inlining the comparator calls with codegen - IMPALA-4065 - we could
  // probably squeeze more performance out of this loop by ensure that as many loads
  // are hoisted out of the loop as possible (either via code changes or __restrict__)
  // annotations.
  FOREACH_ROW(batch, 0, iter) {
    bool replaced_existing_row = heap_->InsertTupleRow(this, iter.Get());
    if (replaced_existing_row) ++rows_to_reclaim_;
  }
}

bool TopNNode::Heap::InsertTupleRow(TopNNode* node, TupleRow* input_row) {
  const TupleDescriptor& tuple_desc = *node->output_tuple_desc_;
  bool replaced_existing_row = false;
  Tuple* insert_tuple = nullptr;
  if (priority_queue_.size() < heap_capacity()) {
    // Add all tuples until we hit capacity.
    insert_tuple = reinterpret_cast<Tuple*>(
        node->tuple_pool_->Allocate(node->tuple_byte_size()));
    insert_tuple->MaterializeExprs<false, false>(input_row, tuple_desc,
        node->output_tuple_expr_evals_, node->tuple_pool_.get());
  } else {
    // We're at capacity - compare to the first row in the priority queue to see if
    // we need to insert this row into the queue.
    DCHECK(!priority_queue_.empty());
    Tuple* top_tuple = priority_queue_.front();
    node->tmp_tuple_->MaterializeExprs<false, true>(input_row, tuple_desc,
        node->output_tuple_expr_evals_, nullptr);
    if (node->tuple_row_less_than_->Less(node->tmp_tuple_, top_tuple)) {
      // Pop off the old head, and replace with the new tuple. Deep copy into 'top_tuple'
      // to reuse the fixed-length memory of 'top_tuple'.
      node->tmp_tuple_->DeepCopy(top_tuple, tuple_desc, node->tuple_pool_.get());
      insert_tuple = top_tuple;
      PopHeap(&priority_queue_,
          ComparatorWrapper<TupleRowComparator>(*node->tuple_row_less_than_));
      replaced_existing_row = true;
    }
  }
  if (insert_tuple != nullptr) {
    PushHeap(&priority_queue_,
        ComparatorWrapper<TupleRowComparator>(*node->tuple_row_less_than_),
        insert_tuple);
  }
  return replaced_existing_row;
}
