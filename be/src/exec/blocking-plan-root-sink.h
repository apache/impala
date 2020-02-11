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
#include "util/condition-variable.h"

namespace impala {

class TupleRow;
class RowBatch;
class QueryResultSet;
class ScalarExprEvaluator;

/// PlanRootSink that handoffs a single RowBatch from the 'sender' (fragment) thread to
/// the 'consumer' (coordinator) thread at a time. Calls to Send will block until the
/// sent RowBatch is consumed by the coordinator thread (e.g. until a enough calls to
/// GetNext read all the data from the sent RowBatch).
///
/// Since calls to Send block until the client thread starts fetching results, this class
/// implements a back-pressure mechanism on the entire ExecNode tree. Rows are only
/// materialized from the tree at the same rate that clients read results.
///
/// The consumer calls GetNext() with a QueryResultSet and a requested fetch
/// size. GetNext() shares these fields with Send(), and then signals Send() to begin
/// populating the result set. GetNext() returns when a) the sender has sent all of its
/// rows b) the requested fetch size has been satisfied or c) the sender fragment
/// instance was cancelled.
///
/// The sender uses Send() to fill in as many rows as are requested from the current
/// batch. When the batch is exhausted - which may take several calls to GetNext() -
/// Send() returns so that the fragment instance can produce another row batch.
class BlockingPlanRootSink : public PlanRootSink {
 public:
  BlockingPlanRootSink(
    TDataSinkId sink_id, const DataSinkConfig& sink_config, RuntimeState* state);

  /// TODO: Currently, this does nothing, it just calls DataSink::Prepare. However, adding
  /// it is necessary because BufferedPlanRootSink needs to use PlanRootSink::Prepare.
  /// Once IMPALA-8825 (add counters to track how long the producer and consumer threads
  /// block, and the rate at which rows are read / sent) is done, this should do the work
  /// to initialize the necessary counters.
  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) override;

  /// Blocks until the consumer has consumed 'batch' by calling GetNext().
  virtual Status Send(RuntimeState* state, RowBatch* batch) override;

  /// Notifies consumer thread of producer eos.
  virtual Status FlushFinal(RuntimeState* state) override;

  /// Release resources and unblocks consumer.
  virtual void Close(RuntimeState* state) override;

  /// Only a single RowBatch is passed from the producer at a time, so QueryResultSet will
  /// only be filled up to 'min(num_rows, batch->num_rows())'.
  virtual Status GetNext(RuntimeState* state, QueryResultSet* result_set, int num_rows,
      bool* eos, int64_t timeout_us) override;

  /// Notifies both consumer and producer threads so they can check the cancellation
  /// status.
  virtual void Cancel(RuntimeState* state) override;

 private:
  /// Protects all members, including the condition variables.
  std::mutex lock_;

  /// Waited on by the sender only. Signalled when the consumer has written results_ and
  /// num_rows_requested_, and so the sender may begin satisfying that request for rows
  /// from its current batch. Also signalled when Cancel() is called, to unblock the
  /// sender.
  ConditionVariable sender_cv_;

  /// Waited on by the consumer only. Signalled when the sender has finished serving a
  /// request for rows. Also signalled by FlushFinal(), Close() and Cancel() to unblock
  /// the consumer.
  ConditionVariable consumer_cv_;

  /// The current result set passed to GetNext(), to fill in Send(). Not owned by this
  /// sink. Reset to nullptr after Send() completes the request to signal to the consumer
  /// that it can return.
  QueryResultSet* results_ = nullptr;

  /// Set by GetNext() to indicate to Send() how many rows it should write to results_.
  int num_rows_requested_ = 0;
};
}
