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

#include "exec/sequence-node.h"

#include <boost/range/adaptor/reversed.hpp>

#include "exec/exec-node-util.h"
#include "runtime/exec-env.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"
#include "util/runtime-profile.h"

#include "common/names.h"

namespace impala {

Status SequencePlanNode::CreateExecNode(
    RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new SequenceNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

SequenceNode::SequenceNode(
    ObjectPool* pool, const SequencePlanNode& pnode, const DescriptorTbl& descs)
    : ExecNode(pool, pnode, descs) {
}

SequenceNode::~SequenceNode() = default;

Status SequenceNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  return Status::OK();
}

Status SequenceNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile()->total_time_counter());
  ScopedOpenEventAdder ea(this);

  RETURN_IF_ERROR(ExecNode::Open(state));
  // Open all terminal children before the first (passthrough) child.
  for (ExecNode* child : boost::adaptors::reverse(children_)) {
    RETURN_IF_ERROR(child->Open(state));
  }

  return Status::OK();
}

Status SequenceNode::GetNext(
    RuntimeState* state, RowBatch* output_row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile()->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  // Save the number of rows in case GetNext() is called with a non-empty batch,
  // which can happen in a subplan.
  int num_rows_before = output_row_batch->num_rows();

  // Return rows from the passthrough child.
  RETURN_IF_ERROR(children_.front()->GetNext(state, output_row_batch, eos));

  // Note: SequenceNode does not alter its child's output (or the equivalent
  // output from the cache), so it does not enforce its own limit on the output.
  // Any limit should be enforced elsewhere, and this code omits the logic
  // to enforce a limit.
  int num_rows_added = output_row_batch->num_rows() - num_rows_before;
  DCHECK_GE(num_rows_added, 0);
  IncrementNumRowsReturned(num_rows_added);
  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

Status SequenceNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  // Reset() is not supported.
  const char* msg = "Internal error: sequence nodes should not appear in subplans.";
  DCHECK(false) << msg;
  return Status(msg);
}

void SequenceNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  // Close children in opposite order from how we opened them.
  for (ExecNode* child : children_) {
    child->Close(state);
  }
  ExecNode::Close(state);
}

void SequenceNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "SequenceNode(";
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

}
