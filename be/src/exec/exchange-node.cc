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

#include "exprs/scalar-expr.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/data-stream-recvr.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/exec-env.h"
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

ExchangeNode::ExchangeNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    num_senders_(0),
    stream_recvr_(),
    input_row_desc_(descs, tnode.exchange_node.input_row_tuples,
        vector<bool>(
          tnode.nullable_tuples.begin(),
          tnode.nullable_tuples.begin() + tnode.exchange_node.input_row_tuples.size())),
    next_row_idx_(0),
    is_merging_(tnode.exchange_node.__isset.sort_info),
    offset_(tnode.exchange_node.__isset.offset ? tnode.exchange_node.offset : 0),
    num_rows_skipped_(0) {
  DCHECK_GE(offset_, 0);
  DCHECK(is_merging_ || (offset_ == 0));
}

Status ExchangeNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Init(tnode, state));
  if (!is_merging_) return Status::OK();

  RETURN_IF_ERROR(ScalarExpr::Create(tnode.exchange_node.sort_info.ordering_exprs,
      row_descriptor_, state, &ordering_exprs_));
  is_asc_order_ = tnode.exchange_node.sort_info.is_asc_order;
  nulls_first_ = tnode.exchange_node.sort_info.nulls_first;
  return Status::OK();
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
      numeric_limits<int64_t>::max(), runtime_profile(), &recvr_buffer_pool_client_));

  // TODO: figure out appropriate buffer size
  DCHECK_GT(num_senders_, 0);
  stream_recvr_ = ExecEnv::GetInstance()->stream_mgr()->CreateRecvr(
      &input_row_desc_, state->fragment_instance_id(), id_, num_senders_,
      FLAGS_exchg_node_buffer_size_bytes, is_merging_, runtime_profile(), mem_tracker(),
      &recvr_buffer_pool_client_);

  if (is_merging_) {
    less_than_.reset(
        new TupleRowComparator(ordering_exprs_, is_asc_order_, nulls_first_));
    AddCodegenDisabledMessage(state);
  }
  return Status::OK();
}

void ExchangeNode::Codegen(RuntimeState* state) {
  DCHECK(state->ShouldCodegen());
  ExecNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;

  if (is_merging_) {
    Status codegen_status = less_than_->Codegen(state);
    runtime_profile()->AddCodegenMsg(codegen_status.ok(), codegen_status);
  }
}

Status ExchangeNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_CANCELLED(state);
  if (is_merging_) {
    // CreateMerger() will populate its merging heap with batches from the stream_recvr_,
    // so it is not necessary to call FillInputRowBatch().
    RETURN_IF_ERROR(
        less_than_->Open(pool_, state, expr_perm_pool(), expr_results_pool()));
    RETURN_IF_ERROR(stream_recvr_->CreateMerger(*less_than_.get()));
  } else {
    RETURN_IF_ERROR(FillInputRowBatch(state));
  }
  return Status::OK();
}

Status ExchangeNode::Reset(RuntimeState* state) {
  DCHECK(false) << "NYI";
  return Status("NYI");
}

void ExchangeNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (less_than_.get() != nullptr) less_than_->Close(state);
  if (stream_recvr_ != nullptr) stream_recvr_->Close();
  ExecEnv::GetInstance()->buffer_pool()->DeregisterClient(&recvr_buffer_pool_client_);
  ScalarExpr::Close(ordering_exprs_);
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
            << " instance_id=" << state->fragment_instance_id();
  return ret_status;
}

Status ExchangeNode::GetNext(RuntimeState* state, RowBatch* output_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  if (ReachedLimit()) {
    stream_recvr_->TransferAllResources(output_batch);
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
          && input_batch_ != NULL && next_row_idx_ < input_batch_->capacity()) {
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
        ++num_rows_returned_;
      }
      COUNTER_SET(rows_returned_counter_, num_rows_returned_);

      if (ReachedLimit()) {
        stream_recvr_->TransferAllResources(output_batch);
        *eos = true;
        return Status::OK();
      }
      if (output_batch->AtCapacity()) return Status::OK();
    }

    // we need more rows
    stream_recvr_->TransferAllResources(output_batch);
    RETURN_IF_ERROR(FillInputRowBatch(state));
    *eos = (input_batch_ == NULL);
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
    int rows_to_keep = num_rows_skipped_ - offset_;
    if (rows_to_keep > 0) {
      output_batch->CopyRows(0, output_batch->num_rows() - rows_to_keep, rows_to_keep);
      output_batch->set_num_rows(rows_to_keep);
    } else {
      output_batch->set_num_rows(0);
    }
    if (rows_to_keep > 0 || *eos || output_batch->AtCapacity()) break;
    RETURN_IF_ERROR(stream_recvr_->GetNext(output_batch, eos));
  }

  num_rows_returned_ += output_batch->num_rows();
  if (ReachedLimit()) {
    output_batch->set_num_rows(output_batch->num_rows() - (num_rows_returned_ - limit_));
    num_rows_returned_ = limit_;
    *eos = true;
  }

  // On eos, transfer all remaining resources from the input batches maintained
  // by the merger to the output batch.
  if (*eos) stream_recvr_->TransferAllResources(output_batch);

  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK();
}

void ExchangeNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "ExchangeNode(#senders=" << num_senders_;
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
