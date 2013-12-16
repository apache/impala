// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/exchange-node.h"

#include <boost/scoped_ptr.hpp>

#include "runtime/data-stream-mgr.h"
#include "runtime/data-stream-recvr.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace impala;
using namespace std;
using namespace boost;

DEFINE_int32(exchg_node_buffer_size_bytes, 1024 * 1024 * 10,
             "(Advanced) Maximum size of per-query receive-side buffer");

ExchangeNode::ExchangeNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    num_senders_(0),
    stream_recvr_(NULL),
    input_row_desc_(descs, tnode.exchange_node.input_row_tuples,
        vector<bool>(
          tnode.nullable_tuples.begin(),
          tnode.nullable_tuples.begin() + tnode.exchange_node.input_row_tuples.size())),
    next_row_idx_(0) {
}

Status ExchangeNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  convert_row_batch_timer_ = ADD_TIMER(runtime_profile(), "ConvertRowBatchTime");
  // TODO: figure out appropriate buffer size
  DCHECK_GT(num_senders_, 0);
  stream_recvr_ = state->CreateRecvr(input_row_desc_, id_, num_senders_,
      FLAGS_exchg_node_buffer_size_bytes, runtime_profile());
  return Status::OK;
}

Status ExchangeNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::OPEN, state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  return Status::OK;
}

void ExchangeNode::Close(RuntimeState* state) {
  input_batch_.reset();
  if (stream_recvr_ != NULL) stream_recvr_->Close();
  ExecNode::Close(state);
}

Status ExchangeNode::GetNext(RuntimeState* state, RowBatch* output_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(state->CheckQueryState());
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  if (ReachedLimit()) {
    *eos = true;
    return Status::OK;
  }

  while (true) {
    {
      SCOPED_TIMER(convert_row_batch_timer_);
      // copy rows until we hit the limit/capacity or until we exhaust input_batch_
      while (!ReachedLimit() && !output_batch->IsFull()
          && input_batch_.get() != NULL && next_row_idx_ < input_batch_->capacity()) {
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
        *eos = true;
        return Status::OK;
      }
      if (output_batch->IsFull()) return Status::OK;
    }

    // we need more rows
    if (input_batch_.get() != NULL) input_batch_->TransferResourceOwnership(output_batch);
    bool is_cancelled;
    {
      SCOPED_TIMER(state->total_network_wait_timer());
      input_batch_.reset(stream_recvr_->GetBatch(&is_cancelled));
    }
    VLOG_FILE << "exch: has batch=" << (input_batch_.get() == NULL ? "false" : "true")
              << " #rows=" << (input_batch_.get() != NULL ? input_batch_->num_rows() : 0)
              << " is_cancelled=" << (is_cancelled ? "true" : "false")
              << " instance_id=" << state->fragment_instance_id();
    if (is_cancelled) return Status(TStatusCode::CANCELLED);
    *eos = (input_batch_.get() == NULL);
    if (*eos) return Status::OK;
    next_row_idx_ = 0;
    DCHECK(input_batch_->row_desc().IsPrefixOf(output_batch->row_desc()));
  }
}

void ExchangeNode::DebugString(int indentation_level, std::stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "ExchangeNode(#senders=" << num_senders_;
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
