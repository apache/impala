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

#include "exec/buffered-plan-root-sink.h"
#include "runtime/deque-row-batch-queue.h"
#include "service/query-result-set.h"

#include "common/names.h"

namespace impala {

// The maximum number of row batches to queue before calls to Send() start to block.
// After this many row batches have been added, Send() will block until GetNext() reads
// RowBatches from the queue.
const uint32_t MAX_QUEUED_ROW_BATCHES = 10;

BufferedPlanRootSink::BufferedPlanRootSink(
    TDataSinkId sink_id, const RowDescriptor* row_desc, RuntimeState* state)
  : PlanRootSink(sink_id, row_desc, state),
    batch_queue_(new DequeRowBatchQueue(MAX_QUEUED_ROW_BATCHES)) {}

Status BufferedPlanRootSink::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  // If the batch is empty, we have nothing to do so just return Status::OK().
  if (batch->num_rows() == 0) return Status::OK();

  // Close should only be called by the producer thread, no RowBatches should be sent
  // after the sink is closed.
  DCHECK(!closed_);
  DCHECK(batch_queue_->IsOpen());
  PlanRootSink::ValidateCollectionSlots(*row_desc_, batch);
  RETURN_IF_ERROR(PlanRootSink::UpdateAndCheckRowsProducedLimit(state, batch));

  // Make a copy of the given RowBatch and place it on the queue.
  unique_ptr<RowBatch> output_batch =
      make_unique<RowBatch>(batch->row_desc(), batch->capacity(), mem_tracker());
  batch->DeepCopyTo(output_batch.get());

  {
    // Add the copied batch to the RowBatch queue and wake up the consumer thread if it is
    // waiting for rows to process.
    unique_lock<mutex> l(lock_);

    // If the queue is full, wait for the producer thread to read batches from it.
    while (!state->is_cancelled() && batch_queue_->IsFull()) {
      batch_queue_has_capacity_.Wait(l);
    }
    RETURN_IF_CANCELLED(state);

    // Add the batch to the queue and then notify the consumer that rows are available.
    if (!batch_queue_->AddBatch(move(output_batch))) {
      // Adding a batch should always be successful because the queue should always be
      // open when Send is called, and the call to batch_queue_has_capacity_.Wait(l)
      // ensures space is available.
      DCHECK(false) << "DequeueRowBatchQueue::AddBatch should never return false";
    }
  }
  // Release the lock before calling notify so the consumer thread can immediately acquire
  // the lock.
  rows_available_.NotifyOne();
  return Status::OK();
}

Status BufferedPlanRootSink::FlushFinal(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  DCHECK(!closed_);
  unique_lock<mutex> l(lock_);
  sender_state_ = SenderState::EOS;
  // If no batches are ever added, wake up the consumer thread so it can check the
  // SenderState and return appropriately.
  rows_available_.NotifyAll();
  // Wait until the consumer has read all rows from the batch_queue_.
  consumer_eos_.Wait(l);
  RETURN_IF_CANCELLED(state);
  return Status::OK();
}

void BufferedPlanRootSink::Close(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  unique_lock<mutex> l(lock_);
  // FlushFinal() won't have been called when the fragment instance encounters an error
  // before sending all rows.
  if (sender_state_ == SenderState::ROWS_PENDING) {
    sender_state_ = SenderState::CLOSED_NOT_EOS;
  }
  batch_queue_->Close();
  // While it should be safe to call NotifyOne() here, prefer to use NotifyAll() to
  // ensure that all sleeping threads are awoken. The call to NotifyAll() is not on the
  // fast path so any overhead from calling it should be negligible.
  rows_available_.NotifyAll();
  DataSink::Close(state);
}

void BufferedPlanRootSink::Cancel(RuntimeState* state) {
  DCHECK(state->is_cancelled());
  // Wake up all sleeping threads so they can check the cancellation state.
  // While it should be safe to call NotifyOne() here, prefer to use NotifyAll() to
  // ensure that all sleeping threads are awoken. The calls to NotifyAll() are not on the
  // fast path so any overhead from calling it should be negligible.
  rows_available_.NotifyAll();
  consumer_eos_.NotifyAll();
  batch_queue_has_capacity_.NotifyAll();
}

Status BufferedPlanRootSink::GetNext(
    RuntimeState* state, QueryResultSet* results, int num_results, bool* eos) {
  {
    unique_lock<mutex> l(lock_);
    while (batch_queue_->IsEmpty() && sender_state_ == SenderState::ROWS_PENDING
        && !state->is_cancelled()) {
      rows_available_.Wait(l);
    }

    // If the query was cancelled while the sink was waiting for rows to become available,
    // or if the query was cancelled before the current call to GetNext, set eos and then
    // return. The queue could be empty if the sink was closed while waiting for rows to
    // become available, or if the sink was closed before the current call to GetNext.
    if (!state->is_cancelled() && !batch_queue_->IsEmpty()) {
      unique_ptr<RowBatch> batch = batch_queue_->GetBatch();
      // TODO for now, if num_results < batch->num_rows(), we terminate returning results
      // early until we can properly handle fetch requests where
      // num_results < batch->num_rows().
      if (num_results > 0 && num_results < batch->num_rows()) {
        *eos = true;
        batch_queue_has_capacity_.NotifyOne();
        consumer_eos_.NotifyOne();
        return Status::Expected(TErrorCode::NOT_IMPLEMENTED_ERROR,
            "BufferedPlanRootSink does not support setting num_results < BATCH_SIZE");
      }
      RETURN_IF_ERROR(
          results->AddRows(output_expr_evals_, batch.get(), 0, batch->num_rows()));
      batch->Reset();
    }
    *eos = batch_queue_->IsEmpty() && sender_state_ == SenderState::EOS;
    if (*eos) consumer_eos_.NotifyOne();
  }
  // Release the lock before calling notify so the consumer thread can immediately
  // acquire the lock. It is safe to call notify batch_queue_has_capacity_ regardless of
  // whether a RowBatch is read. Either (1) a RowBatch is read and the queue is no longer
  // full, so notify the consumer thread or (2) a Rowbatch was not read, which means
  // either FlushFinal was called or the query was cancelled. If FlushFinal was called
  // then the consumer thread has completed. If the query is cancelled, then we wake up
  // the consumer thread so it can check the cancellation status and return. Releasing
  // the lock is safe because the consumer always loops until the queue is actually has
  // space.
  batch_queue_has_capacity_.NotifyOne();
  return state->GetQueryStatus();
}
}
