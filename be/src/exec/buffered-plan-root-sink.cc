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
#include "service/query-result-set.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

const int BufferedPlanRootSink::MAX_FETCH_SIZE;

/// If the fetch size is <= 0, the default number of RowBatches to return in one call to
/// 'GetNext'.
const int FETCH_NUM_BATCHES = 10;

BufferedPlanRootSink::BufferedPlanRootSink(TDataSinkId sink_id,
    const DataSinkConfig& sink_config, RuntimeState* state,
    const TDebugOptions& debug_options)
  : PlanRootSink(sink_id, sink_config, state),
    resource_profile_(sink_config.tsink_->plan_root_sink.resource_profile),
    debug_options_(debug_options) {}

Status BufferedPlanRootSink::Prepare(
    RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(PlanRootSink::Prepare(state, parent_mem_tracker));
  row_batches_send_wait_timer_ = ADD_TIMER(profile(), "RowBatchSendWaitTime");
  row_batches_get_wait_timer_ = ADD_TIMER(profile(), "RowBatchGetWaitTime");
  return Status::OK();
}

Status BufferedPlanRootSink::Open(RuntimeState* state) {
  // Debug action before Open is called.
  RETURN_IF_ERROR(DebugAction(state->query_options(), "BPRS_BEFORE_OPEN"));
  RETURN_IF_ERROR(DataSink::Open(state));
  current_batch_ = make_unique<RowBatch>(row_desc_, state->batch_size(), mem_tracker());
  int64_t max_spilled_mem = state->query_options().max_spilled_result_spooling_mem;
  // max_spilled_result_spooling_mem = 0 means unbounded.
  if (max_spilled_mem <= 0) max_spilled_mem = INT64_MAX;
  batch_queue_.reset(new SpillableRowBatchQueue(name_, max_spilled_mem, state,
      mem_tracker(), profile(), row_desc_, resource_profile_, debug_options_));
  RETURN_IF_ERROR(batch_queue_->Open());
  return Status::OK();
}

Status BufferedPlanRootSink::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  // If the batch is empty, we have nothing to do so just return Status::OK().
  if (batch->num_rows() == 0) return Status::OK();

  // Close should only be called by the producer thread, no RowBatches should be sent
  // after the sink is closed.
  DCHECK(!closed_);
  DCHECK(batch_queue_->IsOpen());
  RETURN_IF_ERROR(PlanRootSink::UpdateAndCheckRowsProducedLimit(state, batch));

  {
    // Add the copied batch to the RowBatch queue and wake up the consumer thread if it is
    // waiting for rows to process.
    unique_lock<mutex> l(lock_);

    // If the queue is full, wait for the producer thread to read batches from it.
    while (!state->is_cancelled() && batch_queue_->IsFull()) {
      SCOPED_TIMER(profile()->inactive_timer());
      SCOPED_TIMER(row_batches_send_wait_timer_);
      // Set this to true means the batch queue is full.
      discard_result(all_results_spooled_.Set(true));
      batch_queue_has_capacity_.Wait(l);
    }
    RETURN_IF_CANCELLED(state);

    // Debug action before AddBatch is called.
    RETURN_IF_ERROR(DebugAction(state->query_options(), "BPRS_BEFORE_ADD_BATCH"));

    // Add the batch to the queue and then notify the consumer that rows are available.
    RETURN_IF_ERROR(batch_queue_->AddBatch(batch));
    rows_sent_counter_->Add(batch->num_rows());
  }
  // Release the lock before calling notify so the consumer thread can immediately acquire
  // the lock.
  rows_available_.NotifyOne();
  return Status::OK();
}

Status BufferedPlanRootSink::FlushFinal(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  // Debug action before FlushFinal is called.
  RETURN_IF_ERROR(DebugAction(state->query_options(), "BPRS_BEFORE_FLUSH_FINAL"));
  DCHECK(!closed_);
  unique_lock<mutex> l(lock_);
  sender_state_ = SenderState::EOS;
  discard_result(all_results_spooled_.Set(false));
  // If no batches are ever added, wake up the consumer thread so it can check the
  // SenderState and return appropriately.
  rows_available_.NotifyAll();
  // Wait until the consumer has read all rows from the batch_queue_ or this has
  // been cancelled.
  while (!IsCancelledOrClosed(state) && !IsQueueEmpty(state)) {
    SCOPED_TIMER(profile()->inactive_timer());
    consumer_eos_.Wait(l);
  }
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
  discard_result(all_results_spooled_.Set(false));
  if (current_batch_row_ != 0) {
    current_batch_->Reset();
  }
  current_batch_.reset();
  if (batch_queue_ != nullptr) batch_queue_->Close();
  // While it should be safe to call NotifyOne() here, prefer to use NotifyAll() to
  // ensure that all sleeping threads are awoken. The call to NotifyAll() is not on the
  // fast path so any overhead from calling it should be negligible.
  rows_available_.NotifyAll();
  DataSink::Close(state);
}

void BufferedPlanRootSink::Cancel(RuntimeState* state) {
  DCHECK(state->is_cancelled());
  // Get the lock_ to synchronize with FlushFinal(). Either FlushFinal() will be waiting
  // on the consumer_eos_ condition variable and get signalled below, or it will see
  // that is_cancelled() is true after it gets the lock. Drop the the lock before
  // signalling the CV so that a blocked thread can immediately acquire the mutex when
  // it wakes up.
  {
    unique_lock<mutex> l(lock_);
  }
  // Wake up all sleeping threads so they can check the cancellation state.
  // While it should be safe to call NotifyOne() here, prefer to use NotifyAll() to
  // ensure that all sleeping threads are awoken. The calls to NotifyAll() are not on the
  // fast path so any overhead from calling it should be negligible.
  rows_available_.NotifyAll();
  consumer_eos_.NotifyAll();
  batch_queue_has_capacity_.NotifyAll();
  discard_result(all_results_spooled_.Set(false));
}

Status BufferedPlanRootSink::GetNext(RuntimeState* state, QueryResultSet* results,
    int num_results, bool* eos, int64_t timeout_us) {
  {
    // Used to track how long the consumer waits for RowBatches to be produced and
    // materialized.
    DCHECK_GE(timeout_us, 0);
    MonotonicStopWatch wait_timeout_timer;
    wait_timeout_timer.Start();

    unique_lock<mutex> l(lock_);
    *eos = false;

    // Cap the maximum number of results fetched by this call to GetNext so that the
    // resulting QueryResultSet does not consume excessive amounts of memory.
    num_results = min(num_results, MAX_FETCH_SIZE);

    // Track the number of rows read from the queue and the number of rows to read.
    int num_rows_read = 0;
    // If 'num_results' <= 0 then by default fetch FETCH_NUM_BATCHES batches.
    const int num_rows_to_read =
        num_results <= 0 ? FETCH_NUM_BATCHES * state->batch_size() : num_results;

    // True if the consumer timed out waiting for the producer to send rows or if the
    // consumer timed out while materializing rows, false otherwise.
    bool timed_out = false;

    // Read from the queue until the query is cancelled or the sink is closed, eos is
    // hit, all requested rows have been read, or the timeout has been hit.
    while (!IsCancelledOrClosed(state) && !*eos && num_rows_read < num_rows_to_read
        && !timed_out) {
      // Wait for the queue to have rows in it.
      while (!IsCancelledOrClosed(state) && IsQueueEmpty(state)
          && sender_state_ == SenderState::ROWS_PENDING && !timed_out) {
        if (timeout_us == 0) {
          rows_available_.Wait(l);
        } else {
          // Wait fetch_rows_timeout_us_ - row_batches_get_wait_timer_ microseconds for
          // rows to become available before returning to the client. Subtracting
          // wait_timeout_timer ensures the client only ever waits up to
          // fetch_rows_timeout_us_ microseconds before returning.
          int64_t wait_duration_us = max(static_cast<int64_t>(1),
              timeout_us - static_cast<int64_t>(round(
                               wait_timeout_timer.ElapsedTime() / NANOS_PER_MICRO)));
          SCOPED_TIMER(row_batches_get_wait_timer_);
          timed_out = !rows_available_.WaitFor(l, wait_duration_us);
        }
      }

      // If the query was cancelled while the sink was waiting for rows to become
      // available, or if the query was cancelled before the current call to GetNext, set
      // eos and then return. The queue could be empty if the sink was closed while
      // waiting for rows to become available, or if the sink was closed before the
      // current call to GetNext.
      if (!IsCancelledOrClosed(state) && !IsQueueEmpty(state)) {
        // If current_batch_ is empty, then read directly from the queue.
        if (current_batch_row_ == 0) {
          // Debug action before GetBatch is called.
          RETURN_IF_ERROR(DebugAction(state->query_options(), "BPRS_BEFORE_GET_BATCH"));
          RETURN_IF_ERROR(batch_queue_->GetBatch(current_batch_.get()));

          // After reading a RowBatch from the queue, it now has additional capacity,
          // notify the producer so it can add more RowBatches. Even though the lock is
          // still held when batch_queue_has_capacity_ is notified, the lock may be
          // released if the current thread waits on rows_available_.
          batch_queue_has_capacity_.NotifyOne();
        }

        // Set the number of rows to be fetched from 'current_batch_'. Either read all
        // remaining rows in the batch, or read up to the 'num_rows_to_read' limit.
        int num_rows_to_fetch = min(current_batch_->num_rows() - current_batch_row_,
            num_rows_to_read - num_rows_read);
        DCHECK_GE(num_rows_to_fetch, 0);

        {
          SCOPED_TIMER(create_result_set_timer_);
          // Read rows from 'current_batch_' and add them to 'results'.
          RETURN_IF_ERROR(results->AddRows(output_expr_evals_, current_batch_.get(),
              current_batch_row_, num_rows_to_fetch));
        }
        num_rows_read += num_rows_to_fetch;
        current_batch_row_ += num_rows_to_fetch;

        // If all rows have been read from 'current_batch_' then reset the batch and its
        // index.
        DCHECK_LE(current_batch_row_, current_batch_->num_rows());
        if (current_batch_row_ == current_batch_->num_rows()) {
          current_batch_row_ = 0;
          current_batch_->Reset();
        }

        // Prevent expr result allocations from accumulating.
        expr_results_pool_->Clear();
      }
      timed_out =
          timed_out || wait_timeout_timer.ElapsedTime() / NANOS_PER_MICRO >= timeout_us;
      // If we have read all rows, then break out of the while loop.
      *eos = IsGetNextEos(state);
    }
    // If the query was cancelled while reading rows, update eos and return.
    *eos = IsGetNextEos(state);
    if (*eos) consumer_eos_.NotifyOne();
  }
  return state->GetQueryStatus();
}
} // namespace impala
