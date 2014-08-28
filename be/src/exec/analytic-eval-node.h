// Copyright 2014 Cloudera Inc.
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

#ifndef IMPALA_EXEC_ANALYTIC_EVAL_NODE_H
#define IMPALA_EXEC_ANALYTIC_EVAL_NODE_H

#include "exec/exec-node.h"
#include "exec/sort-exec-exprs.h"
#include "runtime/buffered-block-mgr.h"
#include "runtime/buffered-tuple-stream.h"
#include "util/tuple-row-compare.h"

namespace impala {

class AggFnEvaluator;

// Evaluates analytic functions with a single pass over input rows. It is assumed
// that the input has already been sorted on all of the partition exprs and then the
// order by exprs. If there is no order by clause or partition clause, the input is
// unsorted. Uses a BufferedTupleStream to buffer input rows which are returned in a
// streaming fashion as entire row batches of output are ready to be returned, though in
// some cases the entire input must actually be consumed to produce any output rows.
//
// The output row is composed of the tuples from the child node followed by a single
// result tuple that holds the values of the evaluated analytic functions (one slot per
// analytic function).
//
// When enough input rows have been consumed to produce the results of all analytic
// functions for one or more rows (e.g. because the order by values are different for a
// RANGE window), the results of all the analytic functions for those rows are produced
// in an output result tuple by calling Finalize() on the evaluators and storing the
// tuple in result_tuples_. Input row batches are fetched from the BufferedTupleStream,
// copied into output row batches, and the associated result tuple is set in each
// corresponding row. Result tuples may apply to many rows (e.g. an arbitrary number or
// an entire partition) so result_tuples_ stores a pair of the stream index (the last
// row in the stream it applies to) and the tuple.
//
// Input rows are consumed in a streaming fashion until enough input has been consumed
// in order to produce enough output rows. In some cases, this may mean that only a
// single input batch is needed to produce the results for an output batch, e.g.
// "SELECT RANK OVER (ORDER BY unique_col) ... ", but in other cases, an arbitrary
// number of rows may need to be buffered before result rows can be produced, e.g. if
// multiple rows have the same values for the order by exprs. The number of buffered
// rows may be an entire partition or even the entire input. Therefore, the output
// rows are buffered and may spill to disk via the BufferedTupleStream.
//
// TODO: Support non-default windows (UNBOUNDED PRECEDING to CURRENT ROW)
class AnalyticEvalNode : public ExecNode {
 public:
  AnalyticEvalNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode);
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual void Close(RuntimeState* state);

 protected:
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  // The scope over which analytic functions are evaluated. Functions are either
  // evaluated over a window (specified by a TAnalyticWindow) or an entire partition.
  // This is used to avoid more complex logic where we often branch based on these
  // cases, e.g. whether or not there is a window (i.e. no window = PARTITION) is stored
  // separately from the window type (assuming there is a window).
  enum AnalyticFnScope {
    // Analytic functions are evaluated over an entire partition (or the entire data set
    // if no partition clause was specified).
    PARTITION,

    // Functions are evaluated over windows specified with range boundaries.
    RANGE,

    // Functions are evaluated over windows specified with rows boundaries.
    ROWS
  };

  // Returns the AnalyticFnScope from the TAnalyticNode. Used to set the const fn_scope_
  // in the initializer list.
  static AnalyticFnScope GetAnalyticFnScope(const TAnalyticNode& node);

  // Evaluates analytic functions over the input batch. Each input row is passed to the
  // evaluators and added to input_stream_ where they are stored until a tuple
  // containing the results of the analytic functions for that row is ready to be
  // returned. When enough rows have been processed so that results can be produced for
  // one or more rows, a tuple containing those results are stored in result_tuples_.
  // That tuple gets set in the associated output row(s) later in GetNextOutputBatch().
  Status ProcessInputBatch(RuntimeState* state, RowBatch* row_batch);

  // Returns a batch of output rows from input_stream_ with the analytic function
  // results (from result_tuples_) set as the last tuple.
  Status GetNextOutputBatch(RowBatch* row_batch, bool* eos);

  // Creates a new output tuple (described by output_tuple_desc_). If current_tuple_ is
  // not NULL (only happens in Open() to initialize), current_tuple_ is copied into the
  // new tuple.
  Tuple* CreateOutputTuple();

  // Determines if there is a window ending at the previous row, and if so, finalizes an
  // output tuple with the analytic fn results. next_partition indicates if the current
  // row is the start of a new partition. stream_idx is the index of the current input
  // row from input_stream_.
  void TryFinalizePrevRow(bool next_partition, int64_t stream_idx, TupleRow* row);

  // Determines if there is a window ending at the current row, and if so, finalizes an
  // output tuple with the analytic fn results. stream_idx is the index of the current
  // input row from input_stream_.
  void TryFinalizeCurrentRow(int64_t stream_idx, TupleRow* row);

  // Initializes state at the start of a new partition. stream_idx is the index of the
  // current input row from input_stream_.
  void InitPartition(int64_t stream_idx);

  // Copies current_tuple_ to a new output tuple and finalizes over the evaluators_. The
  // output tuple is stored in result_tuples_ with the index into input_stream_ specified
  // by stream_idx.
  void FinalizeOutputTuple(int64_t stream_idx);

  // Gets the window start and end index offsets for ROWS windows.
  int64_t rows_start_idx() const;
  int64_t rows_end_idx() const;

  // Debug string about the rows that have been evaluated and are ready to be returned.
  std::string DebugEvaluatedRowsString() const;

  // Debug string containing the window definition.
  std::string DebugWindowString() const;

  // Tuple descriptor for storing intermediate values of analytic fn evaluation.
  const TupleDescriptor* intermediate_tuple_desc_;

  // Tuple descriptor for storing results of analytic fn evaluation.
  const TupleDescriptor* output_tuple_desc_;

  // The scope over which analytic functions are evaluated.
  // TODO: fn_scope_ and window_ are candidates to be removed during codegen
  const AnalyticFnScope fn_scope_;

  // Window over which the analytic functions are evaluated. Only used if fn_scope_
  // is ROWS or RANGE.
  const TAnalyticWindow window_;

  // Exprs on which the analytic function input is partitioned. Used to identify
  // partition boundaries using partition_comparator_. Empty if no partition-by clause
  // is specified.
  // TODO: Remove and use TAnalyticNode.partition_by_lt
  SortExecExprs partition_exprs_;
  boost::scoped_ptr<TupleRowComparator> partition_comparator_;

  // Exprs specified by an order-by clause for RANGE windows. Used to evaluate RANGE
  // window boundaries using ordering_comparator_. Empty if no order-by clause is
  // specified or for windows specifying ROWS.
  // TODO: Remove and use TAnalyticNode.order_by_lt
  SortExecExprs ordering_exprs_;
  boost::scoped_ptr<TupleRowComparator> ordering_comparator_;

  // Analytic function evaluators.
  std::vector<AggFnEvaluator*> evaluators_;

  // FunctionContext for each analytic function. String data returned by the analytic
  // functions is allocated via these contexts.
  std::vector<impala_udf::FunctionContext*> fn_ctxs_;

  // Queue of tuples which are ready to be set in output rows, with the index into
  // the input_stream_ stream of the last TupleRow that gets the Tuple. Pairs are
  // pushed onto the queue in ProcessInputBatch() and dequeued in order in
  // GetNextOutputBatch(). The size of result_tuples_ is limited by 2 times the
  // row batch size because we only process input batches if there are not enough
  // result tuples to produce a single batch of output rows. In the worst case there
  // may be a single result tuple per output row and result_tuples_.size() may be one
  // less than the row batch size, in which case we will process another input row batch
  // (inserting one result tuple per input row) before returning a row batch.
  std::list<std::pair<int64_t, Tuple*> > result_tuples_;

  // The output tuple described by output_tuple_desc_ storing intermediate state for
  // the evaluators_. When enough input rows have been consumed to produce the analytic
  // function results, a copy is created. The copy gets finalized over the evaluators
  // to set the results and then added to result_tuples_.
  Tuple* current_tuple_;

  // Pool used to allocate output tuples.
  boost::scoped_ptr<MemPool> output_tuple_pool_;

  // Number of tuples currently owned by output_tuple_pool_. Resources are transfered
  // to the output row batches when the number of tuples reaches the row batch size.
  int num_owned_output_tuples_;

  // Index of the row in input_stream_ at which the current partition started.
  int64_t current_partition_stream_idx_;

  // Previous input row used to compare partition boundaries and to determine when the
  // order-by expressions change.
  // TODO: Maintain the previous two row batches rather than deep copying into mem_pool_
  // (which we do because the last row in a batch references memory belonging to the
  // previous batch).
  TupleRow* prev_input_row_;

  BufferedBlockMgr::Client* client_;

  // Buffers input rows added in ProcessInputBatch() until enough rows are able to
  // be returned by GetNextOutputBatch(), in which case row batches are returned from
  // the front of the stream and the underlying buffered blocks are deleted once read.
  // The number of rows that must be buffered may vary from an entire partition (e.g.
  // no order by clause) to a single row (e.g. ROWS windows). When the amount of
  // buffered data exceeds the available memory in the underlying BufferedBlockMgr,
  // input_stream_ is unpinned (i.e., possibly spilled to disk if necessary).
  // TODO: Consider re-pinning unpinned streams when possible.
  boost::scoped_ptr<BufferedTupleStream> input_stream_;

  // RowBatch used to read rows from input_stream_.
  boost::scoped_ptr<RowBatch> input_batch_;

  // Pool used for allocations that live until Close().
  boost::scoped_ptr<MemPool> mem_pool_;

  // Current index in input_batch_.
  int input_row_idx_;

  // True when there are no more input rows to consume from our child.
  bool input_eos_;

  // Time spent processing the child rows.
  RuntimeProfile::Counter* evaluation_timer_;
};

}

#endif
