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

#ifndef IMPALA_EXEC_PLAN_ROOT_SINK_H
#define IMPALA_EXEC_PLAN_ROOT_SINK_H

#include "exec/data-sink.h"

namespace impala {

class TupleRow;
class RowBatch;
class QueryResultSet;
class ScalarExprEvaluator;

class PlanRootSinkConfig : public DataSinkConfig {
 public:
  DataSink* CreateSink(RuntimeState* state) const override;

  ~PlanRootSinkConfig() override {}
};

/// Sink which manages the handoff between a 'sender' (a fragment instance) that produces
/// batches by calling Send(), and a 'consumer' (e.g. the coordinator) which consumes rows
/// formed by computing a set of output expressions over the input batches, by calling
/// GetNext(). Send() and GetNext() are called concurrently.
///
/// The sender uses Send() to fill in as many rows as are requested from the current
/// batch. When the batch is exhausted - which may take several calls to GetNext() -
/// Send() returns so that the fragment instance can produce another row batch.
///
/// The consumer uses GetNext() to fetch a specified number of rows into a given
/// QueryResultSet. Calls to GetNext() block until rows are available from the sender
/// thread.
///
/// FlushFinal() should be called by the sender to signal it has finished calling
/// Send() for all rows. Close() should be called by the sender to release resources.
///
/// When the fragment instance is cancelled, Cancel() is called to unblock both the
/// sender and consumer. Cancel() may be called concurrently with Send(), GetNext() and
/// Close().
///
/// The sink is thread safe up to a single sender and single consumer.
///
/// Lifetime: The sink is owned by the QueryState and has the same lifetime as
/// QueryState. The QueryState references from the fragment instance and the Coordinator
/// ensures that this outlives any calls to Send() and GetNext(), respectively.
class PlanRootSink : public DataSink {
 public:
  PlanRootSink(
    TDataSinkId sink_id, const DataSinkConfig& sink_config, RuntimeState* state);
  virtual ~PlanRootSink();

  /// Called before Send(), Open(), or Close(). Performs any additional setup necessary,
  /// such as initializing runtime counters.
  virtual Status Prepare(
      RuntimeState* state, MemTracker* parent_mem_tracker) override = 0;

  /// Sends a new batch. Ownership of 'batch' remains with the sender.
  virtual Status Send(RuntimeState* state, RowBatch* batch) override = 0;

  /// Indicates eos to the producer. When this method is called, all rows have
  /// successfully been sent by the producer.
  virtual Status FlushFinal(RuntimeState* state) override = 0;

  /// To be called by sender only. Release resources and shutdowns the sink.
  virtual void Close(RuntimeState* state) override = 0;

  /// To be called by the consumer only. 'result_set' with up to 'num_rows' rows
  /// produced by the fragment instance that calls Send(). *eos is set to 'true' when
  /// there are no more rows to consume. If Cancel() or Close() are called concurrently,
  /// GetNext() will return and may not populate 'result_set'. All subsequent calls
  /// after Cancel() or Close() set eos and then return the current query status.
  /// 'timeout' is the amount of time (in microseonds) this method should wait for enough
  /// rows to become available before returning (e.g. how long the consumer thread waits
  /// for the producer thread to produce RowBatches). If the timeout is hit, GetNext() can
  /// return before adding 'num_rows' rows to 'result_set'. It is possible 0 rows are
  /// added to the 'result_set' if the producer thread does not produce rows within the
  /// timeout. A timeout of 0 causes this method to wait indefinitely.
  virtual Status GetNext(RuntimeState* state, QueryResultSet* result_set, int num_rows,
      bool* eos, int64_t timeout_us) = 0;

  /// Notifies both the consumer and sender that the query has been cancelled so they can
  /// check the cancellation flag in the RuntimeState. The cancellation flag should be set
  /// prior to calling this. Called by a separate cancellation thread.
  virtual void Cancel(RuntimeState* state) = 0;

 protected:
  /// Check to ensure that the number of rows produced by query execution does not exceed
  /// the NUM_ROWS_PRODUCED_LIMIT query option. Returns an error Status if the given
  /// batch causes the limit to be exceeded. Updates the value of num_rows_produced_.
  Status UpdateAndCheckRowsProducedLimit(RuntimeState* state, RowBatch* batch);

  /// State of the sender:
  /// - ROWS_PENDING: the sender is still producing rows; the only non-terminal state
  /// - EOS: the sender has passed all rows to Send()
  /// - CLOSED_NOT_EOS: the sender (i.e. sink) was closed before all rows were passed to
  ///   Send()
  enum class SenderState { ROWS_PENDING, EOS, CLOSED_NOT_EOS };
  SenderState sender_state_ = SenderState::ROWS_PENDING;

  /// The number of rows sent to this PlanRootSink via Send(). Initialized in
  /// Prepare().
  RuntimeProfile::Counter* rows_sent_counter_ = nullptr;

  /// The rate at which rows are sent to this PlanRootSink via Send(). Initialized in
  /// Prepare().
  RuntimeProfile::Counter* rows_sent_rate_ = nullptr;

  /// Measures the amount of time spend converting results to the client protocol
  RuntimeProfile::Counter* create_result_set_timer_ = nullptr;

 private:
  /// Limit on the number of rows produced by this query, initialized by the constructor.
  const int64_t num_rows_produced_limit_;

  /// Updated by CheckRowsProducedLimit() to indicate the total number of rows produced
  /// by query execution.
  int64_t num_rows_produced_ = 0;
};
}

#endif
