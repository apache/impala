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

#ifndef IMPALA_EXEC_ANALYTIC_EVAL_NODE_H
#define IMPALA_EXEC_ANALYTIC_EVAL_NODE_H

#include <deque>
#include <memory>

#include "exec/exec-node.h"
#include "runtime/buffered-tuple-stream.h"
#include "runtime/tuple.h"

namespace impala {

class AggFn;
class AggFnEvaluator;
class ScalarExpr;
class ScalarExprEvaluator;

class AnalyticEvalPlanNode : public PlanNode {
 public:
  virtual Status Init(const TPlanNode& tnode, FragmentState* state) override;
  virtual void Close() override;
  virtual Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;

  ~AnalyticEvalPlanNode(){}

  /// Analytic functions which live in the runtime-state's objpool.
  std::vector<AggFn*> analytic_fns_;

  /// Indicates if each evaluator is the lead() fn. Used by ResetLeadFnSlots() to
  /// determine which slots need to be reset.
  std::vector<bool> is_lead_fn_;

  /// A predicate that checks if child tuple '<' buffered tuple for partitioning exprs.
  ScalarExpr* partition_by_eq_expr_ = nullptr;

  /// A predicate that checks if child tuple '<' buffered tuple for order by exprs.
  ScalarExpr* order_by_eq_expr_ = nullptr;
};

/// Evaluates analytic functions with a single pass over input rows. It is assumed
/// that the input has already been sorted on all of the partition exprs and then the
/// order by exprs. If there is no order by clause or partition clause, the input is
/// unsorted. Uses a BufferedTupleStream to buffer input rows which are returned in a
/// streaming fashion as entire row batches of output are ready to be returned, though in
/// some cases the entire input must actually be consumed to produce any output rows.
///
/// The output row is composed of the tuples from the child node followed by a single
/// result tuple that holds the values of the evaluated analytic functions (one slot per
/// analytic function).
///
/// When enough input rows have been consumed to produce the results of all analytic
/// functions for one or more rows (e.g. because the order by values are different for a
/// RANGE window), the results of all the analytic functions for those rows are produced
/// in a result tuple by calling GetValue()/Finalize() on the evaluators and storing the
/// tuple in result_tuples_. Input row batches are fetched from the BufferedTupleStream,
/// copied into output row batches, and the associated result tuple is set in each
/// corresponding row. Result tuples may apply to many rows (e.g. an arbitrary number or
/// an entire partition) so result_tuples_ stores a pair of the stream index (the last
/// row in the stream it applies to) and the tuple.
///
/// Input rows are consumed in a streaming fashion until enough input has been consumed
/// in order to produce enough output rows. In some cases, this may mean that only a
/// single input batch is needed to produce the results for an output batch, e.g.
/// "SELECT RANK OVER (ORDER BY unique_col) ... ", but in other cases, an arbitrary
/// number of rows may need to be buffered before result rows can be produced, e.g. if
/// multiple rows have the same values for the order by exprs. The number of buffered
/// rows may be an entire partition or even the entire input. Therefore, the output
/// rows are buffered and may spill to disk via the BufferedTupleStream.

class AnalyticEvalNode : public ExecNode {
 public:
  AnalyticEvalNode(ObjectPool* pool, const AnalyticEvalPlanNode& pnode,
      const TAnalyticNode& analytic_node, const DescriptorTbl& descs);
  virtual ~AnalyticEvalNode();

  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch);
  virtual void Close(RuntimeState* state);

 protected:
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  /// The scope over which analytic functions are evaluated. Functions are either
  /// evaluated over a window (specified by a TAnalyticWindow) or an entire partition.
  /// This is used to avoid more complex logic where we often branch based on these
  /// cases, e.g. whether or not there is a window (i.e. no window = PARTITION) is stored
  /// separately from the window type (assuming there is a window).
  enum AnalyticFnScope {
    /// Analytic functions are evaluated over an entire partition (or the entire data set
    /// if no partition clause was specified). Every row within a partition is added to
    /// curr_tuple_ and buffered in the input_stream_. Once all rows in a partition have
    /// been consumed, a single result tuple is added to result_tuples_ for all rows in
    /// that partition.
    PARTITION,

    /// Functions are evaluated over windows specified with range boundaries. Currently
    /// only supports the 'default window', i.e. UNBOUNDED PRECEDING to CURRENT ROW. In
    /// this case, when the values of the order by expressions change between rows a
    /// result tuple is added to result_tuples_ for the previous rows with the same values
    /// for the order by expressions. This happens in TryAddResultTupleForPrevRow()
    /// because we determine if the order by expression values changed between the
    /// previous and current row.
    RANGE,

    /// Functions are evaluated over windows specified with rows boundaries. A result
    /// tuple is added for every input row (except for some cases where the window extends
    /// before or after the partition). When the end boundary is offset from the current
    /// row, input rows are consumed and result tuples are produced for the associated
    /// preceding or following row. When the start boundary is offset from the current
    /// row, the first tuple (i.e. the input to the analytic functions) from the input
    /// rows are buffered in window_tuples_ because they must later be removed from the
    /// window (by calling AggFnEvaluator::Remove() with the expired tuple to remove it
    /// from the current row). When either the start or end boundaries are offset from the
    /// current row, there is special casing around partition boundaries.
    ROWS
  };

  /// Evaluates analytic functions over curr_child_batch_. Each input row is passed
  /// to the evaluators and added to input_stream_ where they are stored until a tuple
  /// containing the results of the analytic functions for that row is ready to be
  /// returned. When enough rows have been processed so that results can be produced for
  /// one or more rows, a tuple containing those results are stored in result_tuples_.
  /// That tuple gets set in the associated output row(s) later in GetNextOutputBatch().
  Status ProcessChildBatch(RuntimeState* state);

  /// Processes child batches (calling ProcessChildBatch()) until enough output rows
  /// are ready to return an output batch.
  Status ProcessChildBatches(RuntimeState* state);

  /// Returns a batch of output rows from input_stream_ with the analytic function
  /// results (from result_tuples_) set as the last tuple.
  Status GetNextOutputBatch(RuntimeState* state, RowBatch* row_batch, bool* eos);

  /// Adds the row to the evaluators and the tuple stream.
  Status AddRow(int64_t stream_idx, TupleRow* row);

  /// Determines if there is a window ending at the previous row by evaluating
  /// 'child_tuple_cmp_row', and if so, calls AddResultTuple() with the index
  /// of the previous row in 'input_stream_'. 'next_partition' indicates if
  /// the current row is the start of a new partition. 'stream_idx' is the
  /// index of the current input row from 'input_stream_'.  Returns an error
  /// when memory limit is exceeded.
  Status TryAddResultTupleForPrevRow(
      const TupleRow* child_tuple_cmp_row, bool next_partition, int64_t stream_idx);

  /// Determines if there is a window ending at the current row, and if so, calls
  /// AddResultTuple() with the index of the current row in 'input_stream_'.
  /// 'stream_idx' is the index of the current input row from 'input_stream_'.
  /// Returns an error when memory limit is exceeded.
  Status TryAddResultTupleForCurrRow(int64_t stream_idx);

  /// Adds additional result tuples at the end of a partition, e.g. if the end bound is
  /// FOLLOWING. partition_idx is the index into input_stream_ of the new partition,
  /// 'prev_partition_idx' is the index of the previous partition.
  /// Returns an error when memory limit is exceeded.
  Status TryAddRemainingResults(int64_t partition_idx, int64_t prev_partition_idx);

  /// Removes rows from curr_tuple_ (by calling AggFnEvaluator::Remove()) that are no
  /// longer in the window (i.e. they are before the window start boundary). stream_idx
  /// is the index of the row in input_stream_ that is currently being processed in
  /// ProcessChildBatch().
  void TryRemoveRowsBeforeWindow(int64_t stream_idx);

  /// Initializes state at the start of a new partition. stream_idx is the index of the
  /// current input row from input_stream_.
  Status InitNextPartition(RuntimeState* state, int64_t stream_idx);

  /// Produces a result tuple with analytic function results by calling GetValue() or
  /// Finalize() for 'curr_tuple_' on the 'evaluators'. The result tuple is stored in
  /// 'result_tuples_' with the index into 'input_stream_' specified by 'stream_idx'.
  /// Returns an error when memory limit is exceeded.
  Status AddResultTuple(int64_t stream_idx);

  /// Gets the number of rows that are ready to be returned by subsequent calls to
  /// GetNextOutputBatch().
  int64_t NumOutputRowsReady() const;

  /// Resets the slots in current_tuple_ that store the intermedatiate results for lead().
  /// This is necessary to produce the default value (set by Init()).
  void ResetLeadFnSlots();

  /// Evaluates the predicate pred_eval over child_tuple_cmp_row, which is
  /// a TupleRow* containing the previous row and the current row.
  bool PrevRowCompare(
      ScalarExprEvaluator* pred_eval, const TupleRow* child_tuple_cmp_row);

  /// Return true if there are partition or order by expression evaluators. These
  /// evaluators are created in Prepare() if PARTITION BY or ORDER BY clauses exist
  /// for the analytics.
  bool has_partition_or_order_by_expr_eval() const {
    return partition_by_eq_expr_eval_ != nullptr || order_by_eq_expr_eval_ != nullptr;
  }

  /// Debug string containing current state. If 'detailed', per-row state is included.
  std::string DebugStateString(bool detailed) const;

  std::string DebugEvaluatedRowsString() const;

  /// Debug string containing the window definition.
  std::string DebugWindowString() const;

  /// The RuntimeState for the fragment instance containing this AnalyticEvalNode. Set
  /// in Init().
  RuntimeState* state_;

  /// Window over which the analytic functions are evaluated. Only used if fn_scope_
  /// is ROWS or RANGE.
  /// TODO: fn_scope_ and window_ are candidates to be removed during codegen
  const TAnalyticWindow window_;

  /// Tuple descriptor for storing intermediate values of analytic fn evaluation.
  const TupleDescriptor* intermediate_tuple_desc_ = nullptr;

  /// Tuple descriptor for storing results of analytic fn evaluation.
  const TupleDescriptor* result_tuple_desc_ = nullptr;

  /// Tuple descriptor of the buffered tuple (identical to the input child tuple, which is
  /// assumed to come from a single SortNode). NULL if both partition_exprs and
  /// order_by_exprs are empty.
  TupleDescriptor* buffered_tuple_desc_ = nullptr;

  /// A predicate that checks if child tuple '<' buffered tuple for partitioning exprs
  /// and its evaluator.
  ScalarExpr* partition_by_eq_expr_ = nullptr;
  ScalarExprEvaluator* partition_by_eq_expr_eval_ = nullptr;

  /// A predicate that checks if child tuple '<' buffered tuple for order by exprs and
  /// its evaluator.
  ScalarExpr* order_by_eq_expr_ = nullptr;
  ScalarExprEvaluator* order_by_eq_expr_eval_ = nullptr;

  /// The scope over which analytic functions are evaluated.
  /// TODO: Consider adding additional state to capture whether different kinds of window
  /// bounds need to be maintained, e.g. (fn_scope_ == ROWS && window_.__isset.end_bound).
  AnalyticFnScope fn_scope_;

  /// Offset from the current row for ROWS windows with start or end bounds specified
  /// with offsets. Is positive if the offset is FOLLOWING, negative if PRECEDING, and 0
  /// if type is CURRENT ROW or UNBOUNDED PRECEDING/FOLLOWING.
  int64_t rows_start_offset_ = 0;
  int64_t rows_end_offset_ = 0;

  /// Analytic functions and their evaluators. 'analytic_fns_' live in the query-state's
  /// objpool while the evaluators live in the exec node's objpool.
  const std::vector<AggFn*>& analytic_fns_;
  std::vector<AggFnEvaluator*> analytic_fn_evals_;

  /// Indicates if each evaluator is the lead() fn. Used by ResetLeadFnSlots() to
  /// determine which slots need to be reset.
  std::vector<bool> is_lead_fn_;

  /// If true, evaluating FIRST_VALUE requires special null handling when initializing new
  /// partitions determined by the offset. Set in Open() by inspecting the agg fns.
  bool has_first_val_null_offset_ = false;
  long first_val_null_offset_ = 0;

  /// Pools used to allocate result tuples (added to result_tuples_ and later returned)
  /// and window tuples (added to window_tuples_ to buffer the current window). Resources
  /// are transferred from curr_tuple_pool_ to prev_tuple_pool_ once it is at least
  /// MAX_TUPLE_POOL_SIZE bytes. Resources from prev_tuple_pool_ are transferred to an
  /// output row batch when all result tuples it contains have been returned and all
  /// window tuples it contains are no longer needed, or upon eos.
  boost::scoped_ptr<MemPool> curr_tuple_pool_;
  boost::scoped_ptr<MemPool> prev_tuple_pool_;

  /// A tuple described by result_tuple_desc_ used when calling Finalize() on the
  /// analytic_fn_evals_ to release resources between partitions; the value is never used.
  /// Owned by expr_perm_pool_ and allocated in Prepare()
  /// TODO: Remove when agg fns implement a separate Close() method to release resources.
  Tuple* dummy_result_tuple_ = nullptr;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Queue of tuples which are ready to be set in output rows, with the index into
  /// the input_stream_ stream of the last TupleRow that gets the Tuple, i.e. this is a
  /// sparse structure. For example, if result_tuples_ contains tuples with indexes x1 and
  /// x2 where x1 < x2, output rows with indexes in [0, x1] get the first result tuple and
  /// output rows with indexes in (x1, x2] get the second result tuple. Pairs are pushed
  /// onto the queue in ProcessChildBatch() and dequeued in order in GetNextOutputBatch().
  /// The size of result_tuples_ is limited by 2 times the row batch size because we only
  /// process input batches if there are not enough result tuples to produce a single
  /// batch of output rows. In the worst case there may be a single result tuple per
  /// output row and result_tuples_.size() may be one less than the row batch size, in
  /// which case we will process another input row batch (inserting one result tuple per
  /// input row) before returning a row batch.
  std::deque<std::pair<int64_t, Tuple*>> result_tuples_;

  /// Index in input_stream_ of the most recently added result tuple.
  int64_t last_result_idx_ = -1;

  /// Child tuples that are currently within the window and the index into input_stream_
  /// of the row they're associated with. Only used when window start bound is PRECEDING
  /// or FOLLOWING. Tuples in this list are deep copied and owned by
  /// curr_window_tuple_pool_.
  /// TODO: Remove and use BufferedTupleStream (needs support for multiple readers).
  std::deque<std::pair<int64_t, Tuple*>> window_tuples_;

  /// The index of the last row from input_stream_ associated with output row containing
  /// resources in prev_tuple_pool_. -1 when the pool is empty. Resources from
  /// prev_tuple_pool_ can only be transferred to an output batch once all rows containing
  /// these tuples have been returned.
  int64_t prev_pool_last_result_idx_ = -1;

  /// The index of the last row from input_stream_ associated with window tuples
  /// containing resources in prev_tuple_pool_. -1 when the pool is empty. Resources from
  /// prev_tuple_pool_ can only be transferred to an output batch once all rows containing
  /// these tuples are no longer needed (removed from the window_tuples_).
  int64_t prev_pool_last_window_idx_ = -1;

  /// The tuple described by intermediate_tuple_desc_ storing intermediate state for the
  /// analytic_eval_fns_. When enough input rows have been consumed to produce the
  /// analytic function results, a result tuple (described by result_tuple_desc_) is
  /// created and the agg fn results are written to that tuple by calling Finalize()/
  /// GetValue() on the evaluators with curr_tuple_ as the source tuple. Owned by
  /// expr_perm_pool_, allocated in Prepare() and initialized in Open().
  Tuple* curr_tuple_ = nullptr;

  /// True if AggFnEvaluator::Init() was called on 'curr_tuple_', which means that
  /// AggFnEvaluator::Finalize() needs to be called on it.
  bool curr_tuple_init_ = false;

  /// Index of the row in input_stream_ at which the current partition started.
  int64_t curr_partition_idx_ = -1;

  /// Previous input tuple used to compare partition boundaries and to determine when the
  /// order-by expressions change. We only need to store the first tuple of the row
  /// because all the partitioning and ordering columns are in the first tuple. Initially
  /// this points to the first tuple of the last row processed from 'curr_child_batch_',
  /// but it is later deep copied into 'prev_input_tuple_pool_' before 'curr_child_batch_'
  /// is reset.
  Tuple* prev_input_tuple_ = nullptr;
  std::unique_ptr<MemPool> prev_input_tuple_pool_;

  /// Current input row batch from the child. Allocated once and reused.
  std::unique_ptr<RowBatch> curr_child_batch_;

  /// Buffers input rows added in ProcessChildBatch() until enough rows are able to
  /// be returned by GetNextOutputBatch(), in which case row batches are returned from
  /// the front of the stream and the underlying buffers are deleted once read.
  /// The number of rows that must be buffered may vary from an entire partition (e.g.
  /// no order by clause) to a single row (e.g. ROWS windows). If the amount of buffered
  /// data in 'input_stream_' exceeds the ExecNode's buffer reservation and the stream
  /// cannot increase the reservation, then 'input_stream_' is unpinned (i.e., spilled to
  /// disk). The input stream owns tuple data backing rows returned in GetNext(). The
  /// buffers with tuple data are attached to an output row batch on eos or
  /// ReachedLimit().
  /// TODO: Consider re-pinning unpinned streams when possible.
  boost::scoped_ptr<BufferedTupleStream> input_stream_;

  /// True when there are no more input rows to consume from our child.
  bool input_eos_ = false;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// Time spent processing the child rows.
  RuntimeProfile::Counter* evaluation_timer_ = nullptr;
};

}

#endif
