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

#include "exec/singular-row-src-node.h"
#include "exec/subplan-node.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"

namespace impala {

SingularRowSrcNode::SingularRowSrcNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs) : ExecNode(pool, tnode, descs) {
}

Status SingularRowSrcNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  // We do not time this function, check for cancellation, or perform the usual per-batch
  // query maintenance because those would dominate the execution cost of this node.
  DCHECK(containing_subplan_ != NULL) << "set_containing_subplan() must be called";

  // Only produces a single row per GetNext() call.
  *eos = true;
  int row_idx = row_batch->AddRow();
  TupleRow* row = row_batch->GetRow(row_idx);
  row_batch->CopyRow(containing_subplan_->current_row(), row);
  row_batch->CommitLastRow();
  return Status::OK();
}

}
