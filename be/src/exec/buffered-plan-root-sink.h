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

#pragma once

#include "exec/plan-root-sink.h"
#include "runtime/spillable-row-batch-queue.h"
#include "runtime/query-state.h"
#include "util/condition-variable.h"

namespace impala {

class DequeRowBatchQueue;

/// PlanRootSink that buffers RowBatches from the 'sender' (fragment) thread. RowBatches
/// are buffered in memory until the queue is full (the definition of 'full' depends on
/// the queue being used, in the current implementation the SpillableRowBatchQueue is
/// 'full' when the amount of spilled data exceeds the configured limit). Any subsequent
/// calls to Send will block until the 'consumer' (coordinator) thread has read enough
/// RowBatches to free up sufficient space in the queue. The blocking behavior follows
/// the same semantics as BlockingPlanRootSink.
///
/// FlushFinal() blocks until the consumer has read all RowBatches from the queue or
/// until the sink is either closed or cancelled. This ensures that the coordinator
/// fragment stays alive until the client fetches all results, but allows all other
/// fragments to complete and release their resources.
///
/// The sink assumes a non-thread safe RowBatchQueue is injected and uses a single lock to
/// synchronize access to the queue.
class BufferedPlanRootSink : public PlanRootSink {
 public:
  BufferedPlanRootSink(TDataSinkId sink_id, const DataSinkConfig& sink_config,
      RuntimeState* state, const TDebugOptions& debug_options);

  /// Initializes the row_batches_get_wait_timer_ and row_batches_send_wait_timer_
  /// counters.
  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) override;

  /// Creates and opens the SpillableRowBatchQueue, returns an error Status if the queue
  /// could not be opened. Failure to open the queue could occur if the initial
  /// reservation for the BufferedTupleStream could not be acquired.
  virtual Status Open(RuntimeState* state) override;

  /// Creates a copy of the given RowBatch and adds it to the queue. The copy is
  /// necessary as the ownership of 'batch' remains with the sender.
  virtual Status Send(RuntimeState* state, RowBatch* batch) override;

  /// Notifies the consumer of producer eos and blocks until the consumer has read all
  /// batches from the queue, or until the sink is either closed or cancelled.
  virtual Status FlushFinal(RuntimeState* state) override;

  /// Releases resources and unblocks the consumer thread.
  virtual void Close(RuntimeState* state) override;

  /// Blocks until rows are available for consumption. GetNext() always returns 'num_rows'
  /// rows unless (1) there are not enough rows left in the result set to return
  /// 'num_rows' rows, or (2) the value of 'num_rows' exceeds MAX_FETCH_SIZE.
  virtual Status GetNext(RuntimeState* state, QueryResultSet* result_set, int num_rows,
      bool* eos, int64_t timeout_us) override;

  /// Notifies both consumer and producer threads so they can check the cancellation
  /// status.
  virtual void Cancel(RuntimeState* state) override;

  /// Blocks until all results are spooled or we fail to do this due to batch_queue_ is
  /// full, cancellation or any errors. Returns if we fail to do this due to batch_queue_
  /// is full.
  bool WaitForAllResultsSpooled() {
    return all_results_spooled_.Get();
  }

 private:
  /// The maximum number of rows that can be fetched at a time. Set to 100x the
  /// DEFAULT_BATCH_SIZE. Limiting the fetch size is necessary so that the resulting
  /// QueryResultSet does not take up too much memory. Memory used by a QueryResultSet
  /// is not tracked or reserved, so creating QueryResultSets that are too big can throw
  /// off admission control.
  static const int MAX_FETCH_SIZE = QueryState::DEFAULT_BATCH_SIZE * 100;

  /// Protects the RowBatchQueue and all ConditionVariables.
  std::mutex lock_;

  /// Waited on by the consumer inside GetNext() until rows are available for consumption.
  /// Signaled when the producer adds a RowBatch to the queue. Also signaled by
  /// FlushFinal(), Close() and Cancel() to unblock the sender.
  ConditionVariable rows_available_;

  /// Waited on by the producer inside FlushFinal() until the consumer has hit eos.
  /// Signaled when the consumer reads all RowBatches from the queue. Also signaled in
  /// Cancel() to unblock the producer.
  ConditionVariable consumer_eos_;

  /// Waited on by the producer inside Send() if the RowBatchQueue is full. Signaled
  /// when the consumer reads a batch from the RowBatchQueue. Also signaled in Cancel()
  /// to unblock the producer.
  ConditionVariable batch_queue_has_capacity_;

  /// A SpillableRowBatchQueue that buffers RowBatches from the sender for consumption by
  /// the consumer. The queue is not thread safe and access is protected by 'lock_'.
  std::unique_ptr<SpillableRowBatchQueue> batch_queue_;

  /// The TBackendResourceProfile created by the fe/ for the PlanRootSink. Passed to the
  /// SpillableRowBatchQueue to impose the necessary memory limits.
  const TBackendResourceProfile& resource_profile_;

  /// Required by the SpillableRowBatchQueue's ReservationManager when claiming the
  /// initial reservation.
  const TDebugOptions& debug_options_;

  /// Measures the amount of time spent by Impala waiting for the result spooling queue
  /// to have more space. The queue may become full if Impala has produced enough rows to
  /// fill up the queue, and the client hasn't not consumed any rows, or is consuming
  /// rows in at a slower rate than Impala is producing them. Specifically, this counter
  /// measures the amount of time spent waiting on 'batch_queue_has_capacity_' in the
  /// 'Send'  method.
  RuntimeProfile::Counter* row_batches_send_wait_timer_ = nullptr;

  /// Measures the amount of time spend by the client waiting for the result spooling
  /// queue to have rows. The queue may be empty if the query has not produced any rows
  /// yet or if the client is consuming rows at a faster rate than Impala is producing
  /// them. Specifically, this counter measures the amount of time spent waiting on
  /// 'rows_available_' in the 'GetNext' method.
  RuntimeProfile::Counter* row_batches_get_wait_timer_ = nullptr;

  /// The RowBatch currently being read by 'GetNext'. Necessary for calls to 'GetNext'
  /// that only read part of a RowBatch from the queue. If nullptr, 'GetNext' will read
  /// the next RowBatch from the queue. The pointer is reset whenever 'GetNext' has
  /// finished reading all rows from the batch.
  std::unique_ptr<RowBatch> current_batch_;

  /// The index of the next row to be read from 'current_batch_' in the next call to
  /// 'GetNext'. If 'current_batch_' is nullptr, the value of 'current_batch_row_' is 0.
  int current_batch_row_ = 0;

  /// Set when all results are spooled or we fail to do this due to batch_queue_ full,
  /// cancellation or any errors. Set by either the fragment instance execution thread or
  /// the cancellation thread. The boolean result is just used to decide whether to log
  /// a warning. The result is true when batch_queue_ is full so we can't spool all
  /// results. Coordinator will log a warning on this if it's waiting on this promise.
  Promise<bool, PromiseMode::MULTIPLE_PRODUCER> all_results_spooled_;

  /// Returns true if the 'queue' (not the 'batch_queue_') is empty. 'queue' refers to
  /// the logical queue of RowBatches and thus includes any RowBatch that
  /// 'current_batch_' points to. Must be called while holding 'lock_'. Cannot be called
  /// once the sink has been closed.
  bool IsQueueEmpty(RuntimeState* state) const {
    DCHECK(!closed_);
    return batch_queue_->IsEmpty() && current_batch_row_ == 0;
  }

  /// Sets the value of eos inside GetNext. eos is set to true if the queue is closed or
  /// empty and the producer has set sender_state_ to EOS.
  bool IsGetNextEos(RuntimeState* state) const {
    return (IsCancelledOrClosed(state) || IsQueueEmpty(state))
        && sender_state_ == SenderState::EOS;
  }

  /// Returns true if the query has been cancelled or if the PlanRootSink has been
  /// closed, returns false otherwise. Cancellation can occur asynchronously, so this
  /// may become true at any point.
  bool IsCancelledOrClosed(RuntimeState* state) const {
    return state->is_cancelled() || closed_;
  }
};
}
