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

#include "exec/cte-producer-node.h"
#include "exec/exec-node-util.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/local-exchanger.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"
#include "util/runtime-profile.h"
#include "util/scope-exit-trigger.h"

#include "common/names.h"

namespace impala {

Status CTEProducerPlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  DCHECK(tnode.__isset.cte_producer);
  // Register the exchanger here while plan setup is single-threaded.
  for (const TPlanFragmentInstanceCtx* instance_ctx : state->instance_ctxs()) {
    DCHECK_GT(instance_ctx->num_cte_consumers, 0);
    unique_ptr<LocalExchanger> exchanger(
        new LocalExchanger(instance_ctx->num_cte_consumers));
    string exchange_instance = GetCTEName(instance_ctx->per_fragment_instance_idx);
    VLOG_QUERY << "Registering CTE exchange " << exchange_instance << " with "
              << instance_ctx->num_cte_consumers << " consumers.";
    state->query_state()->RegisterExchanger(exchange_instance, exchanger.get());
    exchangers_.emplace(instance_ctx->per_fragment_instance_idx, std::move(exchanger));
  }
  needs_batch_deep_copy_ = WillNeedDeepCopy(state->query_options());
  return Status::OK();
}

Status CTEProducerPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new CTEProducerNode(pool, *this, state));
  return Status::OK();
}

CTEProducerNode::CTEProducerNode(
    ObjectPool* pool, const CTEProducerPlanNode& pnode, RuntimeState* state)
  : ExecNode(pool, pnode, state->desc_tbl()),
    needs_batch_deep_copy_(pnode.needs_batch_deep_copy_) {
  int32_t idx = state->instance_ctx().per_fragment_instance_idx;
  exchanger_ = pnode.exchangers_.at(idx).get();
  name_ = pnode.GetCTEName(idx);
}

Status CTEProducerNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  RETURN_IF_ERROR(exchanger_->Init(runtime_profile(), mem_tracker(), name_));
  return Status::OK();
}

Status CTEProducerNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile()->total_time_counter());
  auto close_on_exit = MakeScopeExitTrigger([&]() {
    VLOG_QUERY << "Closing producer of CTE exchange " << name_;
    exchanger_->CloseProducer();
  });
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_ERROR(child(0)->Open(state));
  // Claim reservation after the child has been opened to reduce the peak reservation
  // requirement.
  if (!buffer_pool_client()->is_registered()) {
    RETURN_IF_ERROR(ClaimBufferReservation(state));
  }

  DCHECK(row_desc()->Equals(*child(0)->row_desc()));
  bool eos = false;
  do {
    RETURN_IF_CANCELLED(state);
    unique_ptr<RowBatch> child_batch(new RowBatch(
        child(0)->row_desc(), state->batch_size(), mem_tracker()));
    RETURN_IF_ERROR(children_[0]->GetNext(state, child_batch.get(), &eos));
    VLOG_PROGRESS << "Adding " << child_batch->num_rows()
                  << " rows to CTE exchange " << name_ << " in " << label();
    // Add all row batches, even if empty, to avoid freeing the tuple data pool.
    int num_rows = child_batch->num_rows();
    if (needs_batch_deep_copy_ && num_rows > 0) {
      unique_ptr<RowBatch> copy(
          new RowBatch(child(0)->row_desc(), num_rows, mem_tracker()));
      child_batch->DeepCopyTo(copy.get());
      RETURN_IF_ERROR(exchanger_->Push(std::move(copy)));
    } else {
      DCHECK(!child_batch->needs_deep_copy() || num_rows == 0);
      RETURN_IF_ERROR(exchanger_->Push(std::move(child_batch)));
    }
    IncrementNumRowsReturned(num_rows);
  } while (!eos);
  return Status::OK();
}

Status CTEProducerNode::GetNext(
    RuntimeState* state, RowBatch* output_row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  while (!exchanger_->ReadFinished(/*timeout_ms=*/ 10)) {
    // Wait for exchanger to finish, handling cancellation and query maintenance.
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
  }
  *eos = true;
  return Status::OK();
}

Status CTEProducerNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  // Reset() is not supported.
  const char* msg = "Internal error: CTE producer nodes should not appear in subplans.";
  DCHECK(false) << msg;
  return Status(msg);
}

void CTEProducerNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  VLOG_QUERY << "Releasing CTE exchange " << name_;
  exchanger_->Release();
  ExecNode::Close(state);
}

void CTEProducerNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ') << "CTEProducerNode(" << name_;
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

}
