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

#include <boost/thread/condition_variable.hpp>

namespace impala {

class TupleRow;
class RowBatch;
class QueryResultSet;
class ExprContext;

/// Sink which manages the handoff between a 'sender' (a fragment instance) that produces
/// batches by calling Send(), and a 'consumer' (e.g. the coordinator) which consumes rows
/// formed by computing a set of output expressions over the input batches, by calling
/// GetNext(). Send() and GetNext() are called concurrently.
///
/// The consumer calls GetNext() with a QueryResultSet and a requested fetch
/// size. GetNext() shares these fields with Send(), and then signals Send() to begin
/// populating the result set. GetNext() returns when either the sender has sent all of
/// its rows, or the requested fetch size has been satisfied.
///
/// Send() fills in as many rows as are requested from the current batch. When the batch
/// is exhausted - which may take several calls to GetNext() - control is returned to the
/// sender to produce another row batch.
///
/// Consumers must call CloseConsumer() when finished to allow the fragment to shut
/// down. Senders must call Close() to signal to the consumer that no more batches will be
/// produced.
///
/// The sink is thread safe up to a single producer and single consumer.
///
/// TODO: The consumer drives the sender in lock-step with GetNext() calls, forcing a
/// context-switch on every invocation. Measure the impact of this, and consider moving to
/// a fully asynchronous implementation with a queue to manage buffering between sender
/// and consumer. See IMPALA-4268.
class PlanRootSink : public DataSink {
 public:
  PlanRootSink(const RowDescriptor& row_desc, const std::vector<TExpr>& output_exprs,
      const TDataSink& thrift_sink);

  virtual std::string GetName() { return NAME; }

  virtual Status Prepare(RuntimeState* state, MemTracker* tracker);

  virtual Status Open(RuntimeState* state);

  /// Sends a new batch. Ownership of 'batch' remains with the sender. Blocks until the
  /// consumer has consumed 'batch' by calling GetNext().
  virtual Status Send(RuntimeState* state, RowBatch* batch);

  /// Sets eos and notifies consumer.
  virtual Status FlushFinal(RuntimeState* state);

  /// To be called by sender only. Signals to the consumer that no more batches will be
  /// produced, then blocks until the consumer calls CloseConsumer().
  virtual void Close(RuntimeState* state);

  /// Populates 'result_set' with up to 'num_rows' rows produced by the fragment instance
  /// that calls Send(). *eos is set to 'true' when there are no more rows to consume.
  Status GetNext(
      RuntimeState* state, QueryResultSet* result_set, int num_rows, bool* eos);

  /// Signals to the producer that the sink will no longer be used. It's an error to call
  /// GetNext() after this returns. May be called more than once; only the first call has
  /// any effect.
  void CloseConsumer();

  static const std::string NAME;

 private:
  /// Protects all members, including the condition variables.
  boost::mutex lock_;

  /// Waited on by the sender only. Signalled when the consumer has written results_ and
  /// num_rows_requested_, and so the sender may begin satisfying that request for rows
  /// from its current batch. Also signalled when CloseConsumer() is called, to unblock
  /// the sender.
  boost::condition_variable sender_cv_;

  /// Waited on by the consumer only. Signalled when the sender has finished serving a
  /// request for rows. Also signalled by Close() and FlushFinal() to signal to the
  /// consumer that no more rows are coming.
  boost::condition_variable consumer_cv_;

  /// Signals to producer that the consumer is done, and the sink may be torn down.
  bool consumer_done_ = false;

  /// Signals to consumer that the sender is done, and that there are no more row batches
  /// to consume.
  bool sender_done_ = false;

  /// The current result set passed to GetNext(), to fill in Send(). Not owned by this
  /// sink. Reset to nullptr after Send() completes the request to signal to the consumer
  /// that it can return.
  QueryResultSet* results_ = nullptr;

  /// Set by GetNext() to indicate to Send() how many rows it should write to results_.
  int num_rows_requested_ = 0;

  /// Set to true in Send() and FlushFinal() when the Sink() has finished producing rows.
  bool eos_ = false;

  /// Output expressions to map plan row batches onto result set rows.
  std::vector<TExpr> thrift_output_exprs_;
  std::vector<ExprContext*> output_expr_ctxs_;

  /// Writes a single row into 'result' and 'scales' by evaluating output_expr_ctxs_ over
  /// 'row'.
  void GetRowValue(TupleRow* row, std::vector<void*>* result, std::vector<int>* scales);
};
}

#endif
