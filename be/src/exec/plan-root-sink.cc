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

#include "exec/plan-root-sink.h"

#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "service/query-result-set.h"
#include "util/pretty-printer.h"

#include <memory>
#include <boost/thread/mutex.hpp>

using namespace std;
using boost::unique_lock;
using boost::mutex;

namespace impala {

PlanRootSink::PlanRootSink(
    TDataSinkId sink_id, const RowDescriptor* row_desc, RuntimeState* state)
  : DataSink(sink_id, row_desc, "PLAN_ROOT_SINK", state),
    num_rows_produced_limit_(state->query_options().num_rows_produced_limit) {}

namespace {

/// Validates that all collection-typed slots in the given batch are set to NULL.
/// See SubplanNode for details on when collection-typed slots are set to NULL.
/// TODO: This validation will become obsolete when we can return collection values.
/// We will then need a different mechanism to assert the correct behavior of the
/// SubplanNode with respect to setting collection-slots to NULL.
void ValidateCollectionSlots(const RowDescriptor& row_desc, RowBatch* batch) {
#ifndef NDEBUG
  if (!row_desc.HasVarlenSlots()) return;
  for (int i = 0; i < batch->num_rows(); ++i) {
    TupleRow* row = batch->GetRow(i);
    for (int j = 0; j < row_desc.tuple_descriptors().size(); ++j) {
      const TupleDescriptor* tuple_desc = row_desc.tuple_descriptors()[j];
      if (tuple_desc->collection_slots().empty()) continue;
      for (int k = 0; k < tuple_desc->collection_slots().size(); ++k) {
        const SlotDescriptor* slot_desc = tuple_desc->collection_slots()[k];
        int tuple_idx = row_desc.GetTupleIdx(slot_desc->parent()->id());
        const Tuple* tuple = row->GetTuple(tuple_idx);
        if (tuple == NULL) continue;
        DCHECK(tuple->IsNull(slot_desc->null_indicator_offset()));
      }
    }
  }
#endif
}
} // namespace

Status PlanRootSink::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  ValidateCollectionSlots(*row_desc_, batch);
  int current_batch_row = 0;

  // Check to ensure that the number of rows produced by query execution does not exceed
  // rows_returned_limit_. Since the PlanRootSink has a single producer, the
  // num_rows_returned_ value can be verified without acquiring the lock_.
  num_rows_produced_ += batch->num_rows();
  if (num_rows_produced_limit_ > 0 && num_rows_produced_ > num_rows_produced_limit_) {
    Status err = Status::Expected(TErrorCode::ROWS_PRODUCED_LIMIT_EXCEEDED,
        PrintId(state->query_id()),
        PrettyPrinter::Print(num_rows_produced_limit_, TUnit::NONE));
    VLOG_QUERY << err.msg().msg();
    return err;
  }

  // Don't enter the loop if batch->num_rows() == 0; no point triggering the consumer with
  // 0 rows to return. Be wary of ever returning 0-row batches to the client; some poorly
  // written clients may not cope correctly with them. See IMPALA-4335.
  while (current_batch_row < batch->num_rows()) {
    unique_lock<mutex> l(lock_);
    // Wait until the consumer gives us a result set to fill in, or the fragment
    // instance has been cancelled.
    while (results_ == nullptr && !state->is_cancelled()) {
      SCOPED_TIMER(profile_->inactive_timer());
      sender_cv_.Wait(l);
    }
    RETURN_IF_CANCELLED(state);

    // Otherwise the consumer is ready. Fill out the rows.
    DCHECK(results_ != nullptr);
    int num_to_fetch = batch->num_rows() - current_batch_row;
    if (num_rows_requested_ > 0) num_to_fetch = min(num_to_fetch, num_rows_requested_);
    RETURN_IF_ERROR(
        results_->AddRows(output_expr_evals_, batch, current_batch_row, num_to_fetch));
    current_batch_row += num_to_fetch;
    // Prevent expr result allocations from accumulating.
    expr_results_pool_->Clear();
    // Signal the consumer.
    results_ = nullptr;
    consumer_cv_.NotifyAll();
  }
  return Status::OK();
}

Status PlanRootSink::FlushFinal(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  unique_lock<mutex> l(lock_);
  sender_state_ = SenderState::EOS;
  consumer_cv_.NotifyAll();
  return Status::OK();
}

void PlanRootSink::Close(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  unique_lock<mutex> l(lock_);
  // FlushFinal() won't have been called when the fragment instance encounters an error
  // before sending all rows.
  if (sender_state_ == SenderState::ROWS_PENDING) {
    sender_state_ = SenderState::CLOSED_NOT_EOS;
  }
  consumer_cv_.NotifyAll();
  DataSink::Close(state);
}

void PlanRootSink::Cancel(RuntimeState* state) {
  DCHECK(state->is_cancelled());
  sender_cv_.NotifyAll();
  consumer_cv_.NotifyAll();
}

Status PlanRootSink::GetNext(
    RuntimeState* state, QueryResultSet* results, int num_results, bool* eos) {
  unique_lock<mutex> l(lock_);

  results_ = results;
  num_rows_requested_ = num_results;
  sender_cv_.NotifyAll();

  // Wait while the sender is still producing rows and hasn't filled in the current
  // result set.
  while (sender_state_ == SenderState::ROWS_PENDING && results_ != nullptr &&
      !state->is_cancelled()) {
    consumer_cv_.Wait(l);
  }

  *eos = sender_state_ == SenderState::EOS;
  return state->GetQueryStatus();
}
}
