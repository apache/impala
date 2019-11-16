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

#include "exec/empty-set-node.h"

#include "exec/exec-node-util.h"
#include "runtime/runtime-state.h"

#include "common/names.h"

namespace impala {

Status EmptySetPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new EmptySetNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

EmptySetNode::EmptySetNode(
    ObjectPool* pool, const EmptySetPlanNode& pnode, const DescriptorTbl& descs)
  : ExecNode(pool, pnode, descs) {}

Status EmptySetNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  return ExecNode::Open(state);
}

Status EmptySetNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  ScopedGetNextEventAdder ea(this, eos);
  *eos = true;
  return Status::OK();
}
} // namespace impala
