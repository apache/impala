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

#include <gflags/gflags.h>

#include "exec/tuple-cache-node.h"
#include "exec/exec-node-util.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"

// Global feature flag for tuple caching. If false, enable_tuple_cache cannot be true
// and the coordinator cannot produce plans with TupleCacheNodes.
DEFINE_bool(allow_tuple_caching, false, "If false, tuple caching cannot be used.");

namespace impala {

Status TupleCachePlanNode::CreateExecNode(
    RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new TupleCacheNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

TupleCacheNode::TupleCacheNode(
    ObjectPool* pool, const TupleCachePlanNode& pnode, const DescriptorTbl& descs)
    : ExecNode(pool, pnode, descs)
    , subtree_hash_(pnode.tnode_->tuple_cache_node.subtree_hash) {
}

Status TupleCacheNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));

  // The frontend cannot create a TupleCacheNode if enable_tuple_cache=false
  // Fail the query if we see this.
  if (!state->query_options().enable_tuple_cache) {
    return Status("Invalid tuple caching configuration: enable_tuple_cache=false");
  }

  RETURN_IF_ERROR(child(0)->Open(state));

  return Status::OK();
}

Status TupleCacheNode::GetNext(
    RuntimeState* state, RowBatch* output_row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  // Save the number of rows in case GetNext() is called with a non-empty batch,
  // which can happen in a subplan.
  int num_rows_before = output_row_batch->num_rows();

  RETURN_IF_ERROR(child(0)->GetNext(state, output_row_batch, eos));

  // Note: TupleCacheNode does not alter its child's output (or the equivalent
  // output from the cache), so it does not enforce its own limit on the output.
  // Any limit should be enforced elsewhere, and this code omits the logic
  // to enforce a limit.
  int num_rows_added = output_row_batch->num_rows() - num_rows_before;
  DCHECK_GE(num_rows_added, 0);
  IncrementNumRowsReturned(num_rows_added);
  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

void TupleCacheNode::DebugString(int indentation_level, std::stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "TupleCacheNode(" << subtree_hash_;
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

}
