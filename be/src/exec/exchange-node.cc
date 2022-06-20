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

#include "exec/exchange-node.h"

#include <boost/scoped_ptr.hpp>

#include "exec/exec-node-util.h"
#include "exprs/scalar-expr.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/krpc-data-stream-mgr.h"
#include "runtime/krpc-data-stream-recvr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"
#include "util/time.h"
#include "util/tuple-row-compare.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

DECLARE_int32(stress_datastream_recvr_delay_ms);

using namespace impala;

DEFINE_int64(exchg_node_buffer_size_bytes, 1024 * 1024 * 10,
    "(Advanced) Maximum size of per-query receive-side buffer");

Status ExchangePlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  bool is_merging = tnode_->exchange_node.__isset.sort_info;
  if (!is_merging) return Status::OK();

  const TSortInfo& sort_info = tnode.exchange_node.sort_info;
  RETURN_IF_ERROR(ScalarExpr::Create(
      sort_info.ordering_exprs, *row_descriptor_, state, &ordering_exprs_));
  row_comparator_config_ =
      state->obj_pool()->Add(new TupleRowComparatorConfig(sort_info, ordering_exprs_));
  state->CheckAndAddCodegenDisabledMessage(codegen_status_msgs_);
  return Status::OK();
}

void ExchangePlanNode::Close() {
  ScalarExpr::Close(ordering_exprs_);
  PlanNode::Close();
}

Status ExchangePlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new ExchangeNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

ExchangeNode::ExchangeNode(
    ObjectPool* pool, const ExchangePlanNode& pnode, const DescriptorTbl& descs)
  : ExecNode(pool, pnode, descs),
    num_senders_(0),
    stream_recvr_(),
    input_row_desc_(descs, pnode.tnode_->exchange_node.input_row_tuples,
        vector<bool>(pnode.tnode_->nullable_tuples.begin(),
                        pnode.tnode_->nullable_tuples.begin()
                            + pnode.tnode_->exchange_node.input_row_tuples.size())),
    next_row_idx_(0),
    is_merging_(pnode.tnode_->exchange_node.__isset.sort_info),
    offset_(pnode.tnode_->exchange_node.__isset.offset ?
            pnode.tnode_->exchange_node.offset :
            0),
    num_rows_skipped_(0) {
  DCHECK_GE(offset_, 0);
  DCHECK(is_merging_ || (offset_ == 0));
  if (!is_merging_) return;
  less_than_.reset(new TupleRowLexicalComparator(*pnode.row_comparator_config_));
}

Status ExchangeNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  convert_row_batch_timer_ = ADD_TIMER(runtime_profile(), "ConvertRowBatchTime");

#ifndef NDEBUG
  if (FLAGS_stress_datastream_recvr_delay_ms > 0) {
    SleepForMs(FLAGS_stress_datastream_recvr_delay_ms);
  }
#endif

  RETURN_IF_ERROR(ExecEnv::GetInstance()->buffer_pool()->RegisterClient(
      Substitute("Exchg Recvr (id=$0)", id_), nullptr,
      ExecEnv::GetInstance()->buffer_reservation(), mem_tracker(),
      numeric_limits<int64_t>::max(), runtime_profile(), &recvr_buffer_pool_client_,
      MemLimit::HARD));

  // TODO: figure out appropriate buffer size
  DCHECK_GT(num_senders_, 0);
  stream_recvr_ = ExecEnv::GetInstance()->stream_mgr()->CreateRecvr(
      &input_row_desc_, *state, state->fragment_instance_id(), id_, num_senders_,
      FLAGS_exchg_node_buffer_size_bytes, is_merging_, runtime_profile(), mem_tracker(),
      &recvr_buffer_pool_client_);

  return Status::OK();
}

void ExchangePlanNode::Codegen(FragmentState* state) {
  DCHECK(state->ShouldCodegen());
  PlanNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;

  if (row_comparator_config_ != nullptr) {
    Status codegen_status;
    llvm::Function* compare_fn = nullptr;
    codegen_status = row_comparator_config_->Codegen(state, &compare_fn);
    if (codegen_status.ok()) {
      codegen_status =
          SortedRunMerger::Codegen(state, compare_fn, &codegend_heapify_helper_fn_);
    }
    AddCodegenStatus(codegen_status);
  }
}

Status ExchangeNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_CANCELLED(state);
  if (is_merging_) {
    const ExchangePlanNode& pnode = static_cast<const ExchangePlanNode&>(plan_node_);
    // CreateMerger() will populate its merging heap with batches from the stream_recvr_,
    // so it is not necessary to call FillInputRowBatch().
    RETURN_IF_ERROR(
        less_than_->Open(pool_, state, expr_perm_pool(), expr_results_pool()));
    RETURN_IF_ERROR(stream_recvr_->CreateMerger(*less_than_.get(),
        pnode.codegend_heapify_helper_fn_));
  } else {
    RETURN_IF_ERROR(FillInputRowBatch(state));
  }
  return Status::OK();
}

Status ExchangeNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  DCHECK(false) << "NYI";
  return Status("NYI");
}

void ExchangeNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (less_than_.get() != nullptr) less_than_->Close(state);
  if (stream_recvr_ != nullptr) stream_recvr_->Close();
  ExecEnv::GetInstance()->buffer_pool()->DeregisterClient(&recvr_buffer_pool_client_);
  ExecNode::Close(state);
}

Status ExchangeNode::FillInputRowBatch(RuntimeState* state) {
  DCHECK(!is_merging_);
  Status ret_status;
  {
    SCOPED_TIMER(state->total_network_receive_timer());
    ret_status = stream_recvr_->GetBatch(&input_batch_);
  }
  VLOG_FILE << "exch: has batch=" << (input_batch_ == NULL ? "false" : "true")
            << " #rows=" << (input_batch_ != NULL ? input_batch_->num_rows() : 0)
            << " is_cancelled=" << (ret_status.IsCancelled() ? "true" : "false")
            << " instance_id=" << PrintId(state->fragment_instance_id());
  return ret_status;
}

void ExchangeNode::ReleaseRecvrResources(RowBatch* output_batch) {
  stream_recvr_->TransferAllResources(output_batch);
  stream_recvr_->CancelStream();
}

Status ExchangeNode::GetNext(RuntimeState* state, RowBatch* output_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  if (ReachedLimit()) {
    ReleaseRecvrResources(output_batch);
    *eos = true;
    return Status::OK();
  } else {
    *eos = false;
  }

  if (is_merging_) return GetNextMerging(state, output_batch, eos);

  while (true) {
    {
      SCOPED_TIMER(convert_row_batch_timer_);
      RETURN_IF_CANCELLED(state);
      RETURN_IF_ERROR(QueryMaintenance(state));
      // copy rows until we hit the limit/capacity or until we exhaust input_batch_
      while (!ReachedLimit() && !output_batch->AtCapacity()
          && input_batch_ != NULL && next_row_idx_ < input_batch_->num_rows()) {
        TupleRow* src = input_batch_->GetRow(next_row_idx_);
        ++next_row_idx_;
        int j = output_batch->AddRow();
        TupleRow* dest = output_batch->GetRow(j);
        // if the input row is shorter than the output row, make sure not to leave
        // uninitialized Tuple* around
        output_batch->ClearRow(dest);
        // this works as expected if rows from input_batch form a prefix of
        // rows in output_batch
        input_batch_->CopyRow(src, dest);
        output_batch->CommitLastRow();
        IncrementNumRowsReturned(1);
      }
      COUNTER_SET(rows_returned_counter_, rows_returned());

      if (ReachedLimit()) {
        ReleaseRecvrResources(output_batch);
        *eos = true;
        return Status::OK();
      }
      if (output_batch->AtCapacity()) return Status::OK();
    }

    // we need more rows
    stream_recvr_->TransferAllResources(output_batch);
    RETURN_IF_ERROR(FillInputRowBatch(state));
    *eos = (input_batch_ == nullptr);
    // No need to call CancelStream() on the receiver here as all incoming row batches
    // have been consumed so we should have replied to all senders already.
    if (*eos) return Status::OK();
    next_row_idx_ = 0;
    DCHECK(input_batch_->row_desc()->LayoutIsPrefixOf(*output_batch->row_desc()));
  }
}

Status ExchangeNode::GetNextMerging(RuntimeState* state, RowBatch* output_batch,
    bool* eos) {
  DCHECK_EQ(output_batch->num_rows(), 0);
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  // Clear any expr result allocations made by the merger.
  expr_results_pool_->Clear();
  RETURN_IF_ERROR(stream_recvr_->GetNext(output_batch, eos));

  while (num_rows_skipped_ < offset_) {
    num_rows_skipped_ += output_batch->num_rows();
    // Throw away rows in the output batch until the offset is skipped.
    int64_t rows_to_keep = num_rows_skipped_ - offset_;
    if (rows_to_keep > 0) {
      output_batch->CopyRows(0, output_batch->num_rows() - rows_to_keep, rows_to_keep);
      output_batch->set_num_rows(rows_to_keep);
    } else {
      output_batch->set_num_rows(0);
    }
    if (rows_to_keep > 0 || *eos || output_batch->AtCapacity()) break;
    RETURN_IF_ERROR(stream_recvr_->GetNext(output_batch, eos));
  }

  CheckLimitAndTruncateRowBatchIfNeeded(output_batch, eos);

  // On eos, transfer all remaining resources from the input batches maintained
  // by the merger to the output batch. Also cancel the underlying receiver so
  // the senders' fragments can exit early.
  if (*eos) ReleaseRecvrResources(output_batch);

  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

void ExchangeNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "ExchangeNode(#senders=" << num_senders_;
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
