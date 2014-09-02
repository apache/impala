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

// Evaluates analytic functions with a single pass over sorted input rows. Uses a
// BufferedTupleStream to buffer output rows which are returned in a streaming fashion
// as entire row batches of output are ready to be returned, though in some cases the
// entire input must actually be consumed to produce any output rows. It is assumed
// that the input has already been sorted on all of the partition keys and then the
// order by exprs.
//
// The row format is composed of the tuples from the child node followed by a single
// result tuple containing the slots that are the results of the analytic functions
// evaluated by this node.
//
// When enough rows have been consumed to produce the analytic function result for one
// or more rows (e.g. because the order by values are different or a different
// partition), the results of the analytic functions for those rows are produced in an
// output result tuple by calling Finalize() on the evaluators and storing the tuple in
// result_tuples_.  Row batches are fetched from the BufferedTupleStream and the
// associated result tuple is set in each row. Result tuples may apply to many rows
// (e.g. an arbitrary number or an entire partition) so result_tuples_ stores a pair of
// the stream index (the last row in the stream it applies to) and the tuple.
//
// Input rows are consumed in a streaming fashion until enough input has been consumed
// in order to produce enough output rows. In some cases, this may mean that only a
// single input batch is needed to produce the results for an output batch, e.g.
// "SELECT RANK OVER (ORDER BY unique_col) ... ", but in other cases, an arbitrary
// number of rows may need to be buffered before result rows can be produced, e.g. if
// multiple rows have the save values for the order by exprs. The number of buffered
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

  // Copies current_tuple_ to a new output tuple and finalizes over the evaluators_. The
  // output tuple is stored in result_tuples_ with the current input_stream_ index.
  // If reinitialize_current_tuple is true, also resets current_tuple_ and initializes
  // over the evaluators_.
  void FinalizeOutputTuple(bool reinitialize_current_tuple);

  // Debug string about the rows that have been evaluated and are ready to be returned.
  std::string DebugEvaluatedRowsString() const;

  // Tuple descriptor storing results of analytic fn evaluation.
  const TupleDescriptor* output_tuple_desc_;

  // Exprs on which the analytic function input is partitioned. Used to identify
  // partition boundaries using partition_comparator_. Empty if no partition-by clause
  // is specified.
  SortExecExprs partition_exprs_;
  boost::scoped_ptr<TupleRowComparator> partition_comparator_;

  // Exprs specified by an order-by clause for RANGE windows. Used to evaluate RANGE
  // window boundaries using ordering_comparator_. Empty if no order-by clause is
  // specified or for windows specifying ROWS.
  SortExecExprs ordering_exprs_;
  boost::scoped_ptr<TupleRowComparator> ordering_comparator_;

  // Window over which the analytic functions are evaluated. Only used if there
  // are ordering exprs.
  TAnalyticWindow window_;

  // Analytic function evaluators.
  std::vector<AggFnEvaluator*> evaluators_;

  // FunctionContext for each analytic function. String data returned by the analytic
  // functions is allocated via these contexts.
  std::vector<impala_udf::FunctionContext*> fn_ctxs_;

  // Queue of tuples which are ready to be set in output rows, with the index into
  // the input_stream_ stream of the last TupleRow that gets the Tuple. Pairs are
  // pushed onto the queue in ProcessInputBatch() and dequeued in order in
  // GetNextOutputBatch().
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
  // The number of rows that must be buffered may vary from the entire partitions (e.g.
  // no order by clause) to a single row (e.g. ROWS windows). When the amount of
  // buffered data exceeds the available memory in the underlying BufferedBlockMgr,
  // input_stream_ is unpinned (i.e. spilled to disk).
  // TODO: Consider re-pinning unpinned streams when possible.
  boost::scoped_ptr<BufferedTupleStream> input_stream_;

  // RowBatch used to read rows from input_stream_.
  boost::scoped_ptr<RowBatch> input_batch_;

  // Pool used for allocations that live until Close().
  boost::scoped_ptr<MemPool> mem_pool_;

  // Current index in input_batch_.
  int input_row_idx_;

  // True when there are no more input rows to consume.
  bool input_eos_;

  // Time spent processing the child rows.
  RuntimeProfile::Counter* evaluation_timer_;
};

}

#endif
