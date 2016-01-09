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

#include "exec/analytic-eval-node.h"

#include "exprs/agg-fn-evaluator.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "udf/udf-internal.h"

#include "common/names.h"

static const int MAX_TUPLE_POOL_SIZE = 8 * 1024 * 1024; // 8MB

namespace impala {

AnalyticEvalNode::AnalyticEvalNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    window_(tnode.analytic_node.window),
    intermediate_tuple_desc_(
        descs.GetTupleDescriptor(tnode.analytic_node.intermediate_tuple_id)),
    result_tuple_desc_(
        descs.GetTupleDescriptor(tnode.analytic_node.output_tuple_id)),
    buffered_tuple_desc_(NULL),
    partition_by_eq_expr_ctx_(NULL),
    order_by_eq_expr_ctx_(NULL),
    rows_start_offset_(0),
    rows_end_offset_(0),
    has_first_val_null_offset_(false),
    first_val_null_offset_(0),
    client_(NULL),
    child_tuple_cmp_row_(NULL),
    last_result_idx_(-1),
    prev_pool_last_result_idx_(-1),
    prev_pool_last_window_idx_(-1),
    curr_tuple_(NULL),
    dummy_result_tuple_(NULL),
    curr_partition_idx_(-1),
    prev_input_row_(NULL),
    input_stream_(NULL),
    input_eos_(false),
    evaluation_timer_(NULL) {
  if (tnode.analytic_node.__isset.buffered_tuple_id) {
    buffered_tuple_desc_ = descs.GetTupleDescriptor(
        tnode.analytic_node.buffered_tuple_id);
  }
  if (!tnode.analytic_node.__isset.window) {
    fn_scope_ = AnalyticEvalNode::PARTITION;
  } else if (tnode.analytic_node.window.type == TAnalyticWindowType::RANGE) {
    fn_scope_ = AnalyticEvalNode::RANGE;
    DCHECK(!window_.__isset.window_start)
      << "RANGE windows must have UNBOUNDED PRECEDING";
    DCHECK(!window_.__isset.window_end ||
        window_.window_end.type == TAnalyticWindowBoundaryType::CURRENT_ROW)
      << "RANGE window end bound must be CURRENT ROW or UNBOUNDED FOLLOWING";
  } else {
    DCHECK_EQ(tnode.analytic_node.window.type, TAnalyticWindowType::ROWS);
    fn_scope_ = AnalyticEvalNode::ROWS;
    if (window_.__isset.window_start) {
      TAnalyticWindowBoundary b = window_.window_start;
      if (b.__isset.rows_offset_value) {
        rows_start_offset_ = b.rows_offset_value;
        if (b.type == TAnalyticWindowBoundaryType::PRECEDING) rows_start_offset_ *= -1;
      } else {
        DCHECK_EQ(b.type, TAnalyticWindowBoundaryType::CURRENT_ROW);
        rows_start_offset_ = 0;
      }
    }
    if (window_.__isset.window_end) {
      TAnalyticWindowBoundary b = window_.window_end;
      if (b.__isset.rows_offset_value) {
        rows_end_offset_ = b.rows_offset_value;
        if (b.type == TAnalyticWindowBoundaryType::PRECEDING) rows_end_offset_ *= -1;
      } else {
        DCHECK_EQ(b.type, TAnalyticWindowBoundaryType::CURRENT_ROW);
        rows_end_offset_ = 0;
      }
    }
  }
  VLOG_FILE << id() << " Window=" << DebugWindowString();
}

AnalyticEvalNode::~AnalyticEvalNode() {
  // Check that we didn't leak any memory.
  DCHECK(input_stream_ == NULL);
}

Status AnalyticEvalNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(ExecNode::Init(tnode));
  DCHECK_EQ(conjunct_ctxs_.size(), 0);
  const TAnalyticNode& analytic_node = tnode.analytic_node;
  bool has_lead_fn = false;
  for (int i = 0; i < analytic_node.analytic_functions.size(); ++i) {
    AggFnEvaluator* evaluator;
    RETURN_IF_ERROR(AggFnEvaluator::Create(
          pool_, analytic_node.analytic_functions[i], true, &evaluator));
    evaluators_.push_back(evaluator);
    const TFunction& fn = analytic_node.analytic_functions[i].nodes[0].fn;
    is_lead_fn_.push_back("lead" == fn.name.function_name);
    has_lead_fn = has_lead_fn || is_lead_fn_.back();
  }
  DCHECK(!has_lead_fn || !window_.__isset.window_start);
  DCHECK(fn_scope_ != PARTITION || analytic_node.order_by_exprs.empty());
  DCHECK(window_.__isset.window_end || !window_.__isset.window_start)
      << "UNBOUNDED FOLLOWING is only supported with UNBOUNDED PRECEDING.";
  if (analytic_node.__isset.partition_by_eq) {
    DCHECK(analytic_node.__isset.buffered_tuple_id);
    RETURN_IF_ERROR(Expr::CreateExprTree(pool_, analytic_node.partition_by_eq,
          &partition_by_eq_expr_ctx_));
  }
  if (analytic_node.__isset.order_by_eq) {
    DCHECK(analytic_node.__isset.buffered_tuple_id);
    RETURN_IF_ERROR(Expr::CreateExprTree(pool_, analytic_node.order_by_eq,
          &order_by_eq_expr_ctx_));
  }
  return Status::OK();
}

Status AnalyticEvalNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  DCHECK(child(0)->row_desc().IsPrefixOf(row_desc()));
  curr_tuple_pool_.reset(new MemPool(mem_tracker()));
  prev_tuple_pool_.reset(new MemPool(mem_tracker()));
  mem_pool_.reset(new MemPool(mem_tracker()));
  fn_pool_.reset(new MemPool(mem_tracker()));
  evaluation_timer_ = ADD_TIMER(runtime_profile(), "EvaluationTime");

  DCHECK_EQ(result_tuple_desc_->slots().size(), evaluators_.size());
  for (int i = 0; i < evaluators_.size(); ++i) {
    impala_udf::FunctionContext* ctx;
    RETURN_IF_ERROR(evaluators_[i]->Prepare(state, child(0)->row_desc(),
        intermediate_tuple_desc_->slots()[i], result_tuple_desc_->slots()[i],
        fn_pool_.get(), &ctx));
    fn_ctxs_.push_back(ctx);
    state->obj_pool()->Add(ctx);
  }

  if (partition_by_eq_expr_ctx_ != NULL || order_by_eq_expr_ctx_ != NULL) {
    DCHECK(buffered_tuple_desc_ != NULL);
    vector<TTupleId> tuple_ids;
    tuple_ids.push_back(child(0)->row_desc().tuple_descriptors()[0]->id());
    tuple_ids.push_back(buffered_tuple_desc_->id());
    RowDescriptor cmp_row_desc(state->desc_tbl(), tuple_ids, vector<bool>(2, false));
    if (partition_by_eq_expr_ctx_ != NULL) {
      RETURN_IF_ERROR(
          partition_by_eq_expr_ctx_->Prepare(state, cmp_row_desc, expr_mem_tracker()));
      AddExprCtxToFree(partition_by_eq_expr_ctx_);
    }
    if (order_by_eq_expr_ctx_ != NULL) {
      RETURN_IF_ERROR(
          order_by_eq_expr_ctx_->Prepare(state, cmp_row_desc, expr_mem_tracker()));
      AddExprCtxToFree(order_by_eq_expr_ctx_);
    }
  }

  RETURN_IF_ERROR(state->block_mgr()->RegisterClient(2, false, mem_tracker(), state,
      &client_));
  return Status::OK();
}

Status AnalyticEvalNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  RETURN_IF_ERROR(child(0)->Open(state));
  DCHECK(client_ != NULL);
  DCHECK(input_stream_ == NULL);
  input_stream_ = new BufferedTupleStream(state, child(0)->row_desc(),
      state->block_mgr(), client_, false /* use_initial_small_buffers */,
      true /* read_write */);
  RETURN_IF_ERROR(input_stream_->Init(id(), runtime_profile(), true));
  RETURN_IF_ERROR(input_stream_->PrepareForRead(true));

  DCHECK_EQ(evaluators_.size(), fn_ctxs_.size());
  for (int i = 0; i < evaluators_.size(); ++i) {
    RETURN_IF_ERROR(evaluators_[i]->Open(state, fn_ctxs_[i]));
    DCHECK(!evaluators_[i]->is_merge());

    if (!has_first_val_null_offset_ &&
        "first_value_rewrite" == evaluators_[i]->fn_name() &&
        fn_ctxs_[i]->GetNumArgs() == 2) {
      DCHECK(!has_first_val_null_offset_);
      first_val_null_offset_ =
        reinterpret_cast<BigIntVal*>(fn_ctxs_[i]->GetConstantArg(1))->val;
      VLOG_FILE << id() << " FIRST_VAL rewrite null offset: " << first_val_null_offset_;
      has_first_val_null_offset_ = true;
    }
  }

  if (partition_by_eq_expr_ctx_ != NULL) {
    RETURN_IF_ERROR(partition_by_eq_expr_ctx_->Open(state));
  }
  if (order_by_eq_expr_ctx_ != NULL) {
    RETURN_IF_ERROR(order_by_eq_expr_ctx_->Open(state));
  }

  if (buffered_tuple_desc_ != NULL) {
    // The backing mem_pool_ is freed in Reset(), so we need to allocate
    // a new row every time we Open().
    child_tuple_cmp_row_ = reinterpret_cast<TupleRow*>(
        mem_pool_->Allocate(sizeof(Tuple*) * 2));
  }

  // An intermediate tuple is only allocated once and is reused.
  curr_tuple_ = Tuple::Create(intermediate_tuple_desc_->byte_size(), mem_pool_.get());
  AggFnEvaluator::Init(evaluators_, fn_ctxs_, curr_tuple_);
  // Allocate dummy_result_tuple_ even if AggFnEvaluator::Init() may have failed
  // as it is needed in Close().
  dummy_result_tuple_ = Tuple::Create(result_tuple_desc_->byte_size(), mem_pool_.get());
  // Check for failures during AggFnEvaluator::Init().
  RETURN_IF_ERROR(state->GetQueryStatus());

  // Initialize state for the first partition.
  RETURN_IF_ERROR(InitNextPartition(state, 0));
  prev_child_batch_.reset(new RowBatch(child(0)->row_desc(), state->batch_size(),
      mem_tracker()));
  curr_child_batch_.reset(new RowBatch(child(0)->row_desc(), state->batch_size(),
      mem_tracker()));
  return Status::OK();
}

string DebugWindowBoundString(const TAnalyticWindowBoundary& b) {
  if (b.type == TAnalyticWindowBoundaryType::CURRENT_ROW) {
    return "CURRENT_ROW";
  }
  stringstream ss;
  if (b.__isset.rows_offset_value) {
    ss << b.rows_offset_value;
  } else {
    // TODO: Return debug string when range offsets are supported
    DCHECK(false) << "Range offsets not yet implemented";
  }
  if (b.type == TAnalyticWindowBoundaryType::PRECEDING) {
    ss << " PRECEDING";
  } else {
    DCHECK_EQ(b.type, TAnalyticWindowBoundaryType::FOLLOWING);
    ss << " FOLLOWING";
  }
  return ss.str();
}

string AnalyticEvalNode::DebugWindowString() const {
  stringstream ss;
  if (fn_scope_ == PARTITION) {
    ss << "NO WINDOW";
    return ss.str();
  }
  ss << "{type=";
  if (fn_scope_ == RANGE) {
    ss << "RANGE";
  } else {
    ss << "ROWS";
  }
  ss << ", start=";
  if (window_.__isset.window_start) {
    ss << DebugWindowBoundString(window_.window_start);
  } else {
    ss << "UNBOUNDED_PRECEDING";
  }

  ss << ", end=";
  if (window_.__isset.window_end) {
    ss << DebugWindowBoundString(window_.window_end) << "}";
  } else {
    ss << "UNBOUNDED_FOLLOWING";
  }
  return ss.str();
}

string AnalyticEvalNode::DebugStateString(bool detailed = false) const {
  stringstream ss;
  ss << "num_returned=" << input_stream_->rows_returned()
     << " num_rows=" << input_stream_->num_rows()
     << " curr_partition_idx_=" << curr_partition_idx_
     << " last_result_idx=" << last_result_idx_;
  if (detailed) {
    ss << " result_tuples idx: [";
    for (list<pair<int64_t, Tuple*> >::const_iterator it = result_tuples_.begin();
        it != result_tuples_.end(); ++it) {
      ss << it->first;
      if (*it != result_tuples_.back()) ss << ", ";
    }
    ss << "]";
    if (fn_scope_ == ROWS && window_.__isset.window_start) {
      ss << " window_tuples idx: [";
      for (list<pair<int64_t, Tuple*> >::const_iterator it = window_tuples_.begin();
          it != window_tuples_.end(); ++it) {
        ss << it->first;
        if (*it != window_tuples_.back()) ss << ", ";
      }
      ss << "]";
    }
  } else {
    if (fn_scope_ == ROWS && window_.__isset.window_start) {
      if (window_tuples_.empty()) {
        ss << " window_tuples empty";
      } else {
        ss << " window_tuples idx range: (" << window_tuples_.front().first << ","
          << window_tuples_.back().first << ")";
      }
    }
    if (result_tuples_.empty()) {
      ss << " result_tuples empty";
    } else {
      ss << " result_tuples idx range: (" << result_tuples_.front().first << ","
        << result_tuples_.back().first << ")";
    }
  }
  return ss.str();
}

inline Status AnalyticEvalNode::AddRow(int64_t stream_idx, TupleRow* row) {
  if (fn_scope_ != ROWS || !window_.__isset.window_start ||
      stream_idx - rows_start_offset_ >= curr_partition_idx_) {
    VLOG_ROW << id() << " Update idx=" << stream_idx;
    AggFnEvaluator::Add(evaluators_, fn_ctxs_, row, curr_tuple_);
    if (window_.__isset.window_start) {
      VLOG_ROW << id() << " Adding tuple to window at idx=" << stream_idx;
      Tuple* tuple = row->GetTuple(0)->DeepCopy(
          *child(0)->row_desc().tuple_descriptors()[0],
          curr_tuple_pool_.get());
      window_tuples_.push_back(pair<int64_t, Tuple*>(stream_idx, tuple));
    }
  }

  Status status = Status::OK();
  // Buffer the entire input row to be returned later with the analytic eval results.
  if (UNLIKELY(!input_stream_->AddRow(row, &status))) {
    // AddRow returns false if an error occurs (available via status()) or there is
    // not enough memory (status() is OK). If there isn't enough memory, we unpin
    // the stream and continue writing/reading in unpinned mode.
    // TODO: Consider re-pinning later if the output stream is fully consumed.
    RETURN_IF_ERROR(status);
    RETURN_IF_ERROR(input_stream_->UnpinStream());
    VLOG_FILE << id() << " Unpin input stream while adding row idx=" << stream_idx;
    if (!input_stream_->AddRow(row, &status)) {
      // Rows should be added in unpinned mode unless an error occurs.
      RETURN_IF_ERROR(status);
      DCHECK(false);
    }
  }
  DCHECK(status.ok());
  return status;
}

void AnalyticEvalNode::AddResultTuple(int64_t stream_idx) {
  VLOG_ROW << id() << " AddResultTuple idx=" << stream_idx;
  DCHECK(curr_tuple_ != NULL);
  Tuple* result_tuple = Tuple::Create(result_tuple_desc_->byte_size(),
      curr_tuple_pool_.get());

  AggFnEvaluator::GetValue(evaluators_, fn_ctxs_, curr_tuple_, result_tuple);
  DCHECK_GT(stream_idx, last_result_idx_);
  result_tuples_.push_back(pair<int64_t, Tuple*>(stream_idx, result_tuple));
  last_result_idx_ = stream_idx;
  VLOG_ROW << id() << " Added result tuple, final state: " << DebugStateString(true);
}

inline void AnalyticEvalNode::TryAddResultTupleForPrevRow(bool next_partition,
    int64_t stream_idx, TupleRow* row) {
  // The analytic fns are finalized after the previous row if we found a new partition
  // or the window is a RANGE and the order by exprs changed. For ROWS windows we do not
  // need to compare the current row to the previous row.
  VLOG_ROW << id() << " TryAddResultTupleForPrevRow partition=" << next_partition
           << " idx=" << stream_idx;
  if (fn_scope_ == ROWS) return;
  if (next_partition || (fn_scope_ == RANGE && window_.__isset.window_end &&
      !PrevRowCompare(order_by_eq_expr_ctx_))) {
    AddResultTuple(stream_idx - 1);
  }
}

inline void AnalyticEvalNode::TryAddResultTupleForCurrRow(int64_t stream_idx,
    TupleRow* row) {
  VLOG_ROW << id() << " TryAddResultTupleForCurrRow idx=" << stream_idx;
  // We only add results at this point for ROWS windows (unless unbounded following)
  if (fn_scope_ != ROWS || !window_.__isset.window_end) return;

  // Nothing to add if the end offset is before the start of the partition.
  if (stream_idx - rows_end_offset_ < curr_partition_idx_) return;
  AddResultTuple(stream_idx - rows_end_offset_);
}

inline void AnalyticEvalNode::TryRemoveRowsBeforeWindow(int64_t stream_idx) {
  if (fn_scope_ != ROWS || !window_.__isset.window_start) return;
  // The start of the window may have been before the current partition, in which case
  // there is no tuple to remove in window_tuples_. Check the index of the row at which
  // tuples from window_tuples_ should begin to be removed.
  int64_t remove_idx = stream_idx - rows_end_offset_ +
      min<int64_t>(rows_start_offset_, 0) - 1;
  if (remove_idx < curr_partition_idx_) return;
  VLOG_ROW << id() << " Remove idx=" << remove_idx << " stream_idx=" << stream_idx;
  DCHECK(!window_tuples_.empty()) << DebugStateString(true);
  DCHECK_EQ(remove_idx + max<int64_t>(rows_start_offset_, 0),
      window_tuples_.front().first) << DebugStateString(true);
  TupleRow* remove_row = reinterpret_cast<TupleRow*>(&window_tuples_.front().second);
  AggFnEvaluator::Remove(evaluators_, fn_ctxs_, remove_row, curr_tuple_);
  window_tuples_.pop_front();
}

inline void AnalyticEvalNode::TryAddRemainingResults(int64_t partition_idx,
    int64_t prev_partition_idx) {
  DCHECK_LT(prev_partition_idx, partition_idx);
  // For PARTITION, RANGE, or ROWS with UNBOUNDED PRECEDING: add a result tuple for the
  // remaining rows in the partition that do not have an associated result tuple yet.
  if (fn_scope_ != ROWS || !window_.__isset.window_end) {
    if (last_result_idx_ < partition_idx - 1) AddResultTuple(partition_idx - 1);
    return;
  }

  // lead() is re-written to a ROWS window with an end bound FOLLOWING. Any remaining
  // results need the default value (set by Init()). If this is the case, the start bound
  // is UNBOUNDED PRECEDING (DCHECK in Init()).
  for (int i = 0; i < evaluators_.size(); ++i) {
    if (is_lead_fn_[i]) evaluators_[i]->Init(fn_ctxs_[i], curr_tuple_);
  }

  // If the start bound is not UNBOUNDED PRECEDING and there are still rows in the
  // partition for which we need to produce result tuples, we need to continue removing
  // input tuples at the start of the window from each row that we're adding results for.
  VLOG_ROW << id() << " TryAddRemainingResults prev_partition_idx=" << prev_partition_idx
           << " " << DebugStateString(true);
  for (int64_t next_result_idx = last_result_idx_ + 1; next_result_idx < partition_idx;
      ++next_result_idx) {
    if (window_tuples_.empty()) break;
    if (next_result_idx + rows_start_offset_ > window_tuples_.front().first) {
      DCHECK_EQ(next_result_idx + rows_start_offset_ - 1, window_tuples_.front().first);
      // For every tuple that is removed from the window: Remove() from the evaluators
      // and add the result tuple at the next index.
      VLOG_ROW << id() << " Remove window_row_idx=" << window_tuples_.front().first
               << " for result row at idx=" << next_result_idx;
      TupleRow* remove_row = reinterpret_cast<TupleRow*>(&window_tuples_.front().second);
      AggFnEvaluator::Remove(evaluators_, fn_ctxs_, remove_row, curr_tuple_);
      window_tuples_.pop_front();
    }
    AddResultTuple(last_result_idx_ + 1);
  }

  // If there are still rows between the row with the last result (AddResultTuple() may
  // have updated last_result_idx_) and the partition boundary, add the current results
  // for the remaining rows with the same result tuple (curr_tuple_ is not modified).
  if (last_result_idx_ < partition_idx - 1) AddResultTuple(partition_idx - 1);
}

inline Status AnalyticEvalNode::InitNextPartition(RuntimeState* state,
    int64_t stream_idx) {
  VLOG_FILE << id() << " InitNextPartition idx=" << stream_idx;
  DCHECK_LT(curr_partition_idx_, stream_idx);
  int64_t prev_partition_stream_idx = curr_partition_idx_;
  curr_partition_idx_ = stream_idx;

  // If the window has an end bound preceding the current row, we will have output tuples
  // for rows beyond the previous partition, so they should be removed.  Because
  // result_tuples_ is a sparse structure, the last result tuple of the previous
  // partition may have been added to result_tuples_ with a stream index equal to or
  // beyond curr_partition_idx_. So the last entry in result_tuples_ with a stream index
  // >= curr_partition_idx_ is the last result tuple of the previous partition.  Adding
  // the last result tuple to result_tuples_ with a stream index curr_partition_idx_ - 1
  // ensures that all rows in the previous partition have corresponding analytic results.
  Tuple* prev_partition_last_result_tuple = NULL;
  while (!result_tuples_.empty() && result_tuples_.back().first >= curr_partition_idx_) {
    DCHECK(fn_scope_ == ROWS && window_.__isset.window_end &&
        window_.window_end.type == TAnalyticWindowBoundaryType::PRECEDING);
    VLOG_ROW << id() << " Removing result past partition idx: "
             << result_tuples_.back().first;
    prev_partition_last_result_tuple = result_tuples_.back().second;
    result_tuples_.pop_back();
  }
  if (prev_partition_last_result_tuple != NULL) {
    if (result_tuples_.empty() ||
        result_tuples_.back().first < curr_partition_idx_ - 1) {
      // prev_partition_last_result_tuple was the last result tuple in the partition, add
      // it back with the index of the last row in the partition so that all output rows
      // in this partition get the correct value.
      result_tuples_.push_back(pair<int64_t, Tuple*>(curr_partition_idx_ - 1,
          prev_partition_last_result_tuple));
    }
    DCHECK(!result_tuples_.empty());
    last_result_idx_ = result_tuples_.back().first;
    VLOG_ROW << id() << " After removing results past partition: "
             << DebugStateString(true);
    DCHECK_EQ(last_result_idx_, curr_partition_idx_ - 1);
    DCHECK_LE(input_stream_->rows_returned(), last_result_idx_);
  }
  DCHECK(result_tuples_.empty() || (last_result_idx_ == result_tuples_.back().first));

  if (fn_scope_ == ROWS && stream_idx > 0 && (!window_.__isset.window_end ||
        window_.window_end.type == TAnalyticWindowBoundaryType::FOLLOWING)) {
    TryAddRemainingResults(stream_idx, prev_partition_stream_idx);
  }
  window_tuples_.clear();

  VLOG_ROW << id() << " Reset curr_tuple";
  // Call finalize to release resources; result is not needed but the dst tuple must be
  // a tuple described by result_tuple_desc_.
  AggFnEvaluator::Finalize(evaluators_, fn_ctxs_, curr_tuple_, dummy_result_tuple_);
  // Re-initialize curr_tuple_.
  curr_tuple_->Init(intermediate_tuple_desc_->byte_size());
  AggFnEvaluator::Init(evaluators_, fn_ctxs_, curr_tuple_);
  // Check for errors in AggFnEvaluator::Init().
  RETURN_IF_ERROR(state->GetQueryStatus());

  // Add a result tuple containing values set by Init() (e.g. NULL for sum(), 0 for
  // count()) for output rows that have no input rows in the window. We need to add this
  // result tuple before any input rows are consumed and the evaluators are updated.
  if (fn_scope_ == ROWS && window_.__isset.window_end &&
      window_.window_end.type == TAnalyticWindowBoundaryType::PRECEDING) {
    if (has_first_val_null_offset_) {
      // Special handling for FIRST_VALUE which has the window rewritten in the FE
      // in order to evaluate the fn efficiently with a trivial agg fn implementation.
      // This occurs when the original analytic window has a start bound X PRECEDING. In
      // that case, the window is rewritten to have an end bound X PRECEDING which would
      // normally mean we add the newly Init()'d result tuple X rows down (so that those
      // first rows have the initial value because they have no rows in their windows).
      // However, the original query did not actually have X PRECEDING so we need to do
      // one of the following:
      // 1) Do not insert the initial result tuple with at all, indicated by
      //    first_val_null_offset_ == -1. This happens when the original end bound was
      //    actually CURRENT ROW or Y FOLLOWING.
      // 2) Insert the initial result tuple at first_val_null_offset_. This happens when
      //    the end bound was actually Y PRECEDING.
      if (first_val_null_offset_ != -1) {
        AddResultTuple(curr_partition_idx_ + first_val_null_offset_ - 1);
      }
    } else {
      AddResultTuple(curr_partition_idx_ - rows_end_offset_ - 1);
    }
  }
  return Status::OK();
}

inline bool AnalyticEvalNode::PrevRowCompare(ExprContext* pred_ctx) {
  DCHECK(pred_ctx != NULL);
  BooleanVal result = pred_ctx->GetBooleanVal(child_tuple_cmp_row_);
  DCHECK(!result.is_null);
  return result.val;
}

Status AnalyticEvalNode::ProcessChildBatches(RuntimeState* state) {
  // Consume child batches until eos or there are enough rows to return more than an
  // output batch. Ensuring there is at least one more row left after returning results
  // allows us to simplify the logic dealing with last_result_idx_ and result_tuples_.
  while (!input_eos_ && NumOutputRowsReady() < state->batch_size() + 1) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(child(0)->GetNext(state, curr_child_batch_.get(), &input_eos_));
    RETURN_IF_ERROR(QueryMaintenance(state));
    RETURN_IF_ERROR(ProcessChildBatch(state));
    // TODO: DCHECK that the size of result_tuples_ is bounded. It shouldn't be larger
    // than 2x the batch size unless the end bound has an offset preceding, in which
    // case it may be slightly larger (proportional to the offset but still bounded).
    prev_child_batch_->Reset();
    prev_child_batch_.swap(curr_child_batch_);
  }
  if (input_eos_) {
    curr_child_batch_.reset();
    prev_child_batch_.reset();
  }
  return Status::OK();
}

Status AnalyticEvalNode::ProcessChildBatch(RuntimeState* state) {
  // TODO: DCHECK input is sorted (even just first row vs prev_input_row_)
  VLOG_FILE << id() << " ProcessChildBatch: " << DebugStateString()
            << " input batch size:" << curr_child_batch_->num_rows()
            << " tuple pool size:" << curr_tuple_pool_->total_allocated_bytes();
  SCOPED_TIMER(evaluation_timer_);

  // BufferedTupleStream::num_rows() returns the total number of rows that have been
  // inserted into the stream (it does not decrease when we read rows), so the index of
  // the next input row that will be inserted will be the current size of the stream.
  int64_t stream_idx = input_stream_->num_rows();

  // The very first row in the stream is handled specially because there is no previous
  // row to compare and we cannot rely on PrevRowCompare() returning true even for the
  // same row pointers if there are NaN values.
  int batch_idx = 0;
  if (UNLIKELY(stream_idx == 0 && curr_child_batch_->num_rows() > 0)) {
    TupleRow* row = curr_child_batch_->GetRow(0);
    RETURN_IF_ERROR(AddRow(0, row));
    TryAddResultTupleForCurrRow(0, row);
    prev_input_row_ = row;
    ++batch_idx;
    ++stream_idx;
  }

  for (; batch_idx < curr_child_batch_->num_rows(); ++batch_idx, ++stream_idx) {
    TupleRow* row = curr_child_batch_->GetRow(batch_idx);
    if (partition_by_eq_expr_ctx_ != NULL || order_by_eq_expr_ctx_ != NULL) {
      // Only set the tuples in child_tuple_cmp_row_ if there are partition exprs or
      // order by exprs that require comparing the current and previous rows. If there
      // aren't partition or order by exprs (i.e. empty OVER() clause), there was no sort
      // and there could be nullable tuples (whereas the sort node does not produce
      // them), see IMPALA-1562.
      child_tuple_cmp_row_->SetTuple(0, prev_input_row_->GetTuple(0));
      child_tuple_cmp_row_->SetTuple(1, row->GetTuple(0));
    }
    TryRemoveRowsBeforeWindow(stream_idx);

    // Every row is compared against the previous row to determine if (a) the row
    // starts a new partition or (b) the row does not share the same values for the
    // ordering exprs. When either of these occurs, the evaluators_ are finalized and
    // the result tuple is added to result_tuples_ so that it may be added to output
    // rows in GetNextOutputBatch(). When a new partition is found (a), a new, empty
    // result tuple is created and initialized over the evaluators_. If the row has
    // different values for the ordering exprs (b), then a new tuple is created but
    // copied from curr_tuple_ because the original is used for one or more previous
    // row(s) but the incremental state still applies to the current row.
    bool next_partition = false;
    if (partition_by_eq_expr_ctx_ != NULL) {
      // partition_by_eq_expr_ctx_ checks equality over the predicate exprs
      next_partition = !PrevRowCompare(partition_by_eq_expr_ctx_);
    }
    TryAddResultTupleForPrevRow(next_partition, stream_idx, row);
    if (next_partition) RETURN_IF_ERROR(InitNextPartition(state, stream_idx));

    // The evaluators_ are updated with the current row.
    RETURN_IF_ERROR(AddRow(stream_idx, row));

    TryAddResultTupleForCurrRow(stream_idx, row);
    prev_input_row_ = row;
  }

  if (UNLIKELY(input_eos_ && stream_idx > curr_partition_idx_)) {
    // We need to add the results for the last row(s).
    TryAddRemainingResults(stream_idx, curr_partition_idx_);
  }

  // Transfer resources to prev_tuple_pool_ when enough resources have accumulated
  // and the prev_tuple_pool_ has already been transfered to an output batch.
  if (curr_tuple_pool_->total_allocated_bytes() > MAX_TUPLE_POOL_SIZE &&
      (prev_pool_last_result_idx_ == -1 || prev_pool_last_window_idx_ == -1)) {
    prev_tuple_pool_->AcquireData(curr_tuple_pool_.get(), false);
    prev_pool_last_result_idx_ = last_result_idx_;
    if (window_tuples_.size() > 0) {
      prev_pool_last_window_idx_ = window_tuples_.back().first;
    } else {
      prev_pool_last_window_idx_ = -1;
    }
    VLOG_FILE << id() << " Transfer resources from curr to prev pool at idx: "
              << stream_idx << ", stores tuples with last result idx: "
              << prev_pool_last_result_idx_ << " last window idx: "
              << prev_pool_last_window_idx_;
  }
  return Status::OK();
}

Status AnalyticEvalNode::GetNextOutputBatch(RuntimeState* state, RowBatch* output_batch,
    bool* eos) {
  SCOPED_TIMER(evaluation_timer_);
  VLOG_FILE << id() << " GetNextOutputBatch: " << DebugStateString()
            << " tuple pool size:" << curr_tuple_pool_->total_allocated_bytes();
  if (input_stream_->rows_returned() == input_stream_->num_rows()) {
    *eos = true;
    return Status::OK();
  }

  const int num_child_tuples = child(0)->row_desc().tuple_descriptors().size();
  RowBatch input_batch(child(0)->row_desc(), output_batch->capacity(), mem_tracker());
  int64_t stream_idx = input_stream_->rows_returned();
  RETURN_IF_ERROR(input_stream_->GetNext(&input_batch, eos));
  for (int i = 0; i < input_batch.num_rows(); ++i) {
    if (ReachedLimit()) break;
    DCHECK(!output_batch->AtCapacity());
    DCHECK(!result_tuples_.empty());
    VLOG_ROW << id() << " Output row idx=" << stream_idx << " " << DebugStateString(true);

    // CopyRow works as expected: input_batch tuples form a prefix of output_batch
    // tuples.
    TupleRow* dest = output_batch->GetRow(output_batch->AddRow());
    input_batch.CopyRow(input_batch.GetRow(i), dest);
    dest->SetTuple(num_child_tuples, result_tuples_.front().second);
    output_batch->CommitLastRow();
    ++num_rows_returned_;

    // Remove the head of result_tuples_ if all rows using that evaluated tuple
    // have been returned.
    DCHECK_LE(stream_idx, result_tuples_.front().first);
    if (stream_idx >= result_tuples_.front().first) result_tuples_.pop_front();
    ++stream_idx;
  }
  input_batch.TransferResourceOwnership(output_batch);
  if (ReachedLimit()) *eos = true;
  return Status::OK();
}

inline int64_t AnalyticEvalNode::NumOutputRowsReady() const {
  if (result_tuples_.empty()) return 0;
  int64_t rows_to_return = last_result_idx_ - input_stream_->rows_returned();
  if (last_result_idx_ > input_stream_->num_rows()) {
    // This happens when we were able to add a result tuple before consuming child rows,
    // e.g. initializing a new partition with an end bound that is X preceding. The first
    // X rows get the default value and we add that tuple to result_tuples_ before
    // consuming child rows. It's possible the result is negative, and that's fine
    // because this result is only used to determine if the number of rows to return
    // is at least as big as the batch size.
    rows_to_return -= last_result_idx_ - input_stream_->num_rows();
  } else {
    DCHECK_GE(rows_to_return, 0);
  }
  return rows_to_return;
}

Status AnalyticEvalNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  VLOG_FILE << id() << " GetNext: " << DebugStateString();
  DCHECK(input_stream_ != NULL); // input_stream_ is NULL if we already hit eos.

  if (ReachedLimit()) {
    // TODO: This transfer is simple and correct, but not necessarily efficient. We
    // should optimize the use/transfer of memory to better amortize allocations
    // over multiple Reset()/Open()/GetNext()* cycles.
    row_batch->tuple_data_pool()->AcquireData(prev_tuple_pool_.get(), false);
    row_batch->tuple_data_pool()->AcquireData(curr_tuple_pool_.get(), false);
    DCHECK(input_stream_ != NULL);
    row_batch->AddTupleStream(input_stream_);
    input_stream_ = NULL;
    *eos = true;
    return Status::OK();
  } else {
    *eos = false;
  }

  RETURN_IF_ERROR(ProcessChildBatches(state));

  bool output_eos = false;
  RETURN_IF_ERROR(GetNextOutputBatch(state, row_batch, &output_eos));
  if (curr_child_batch_.get() == NULL && output_eos) {
    // Transfer the ownership of all row-backing resources on eos for simplicity.
    // TODO: This transfer is simple and correct, but not necessarily efficient. We
    // should optimize the use/transfer of memory to better amortize allocations
    // over multiple Reset()/Open()/GetNext()* cycles.
    row_batch->tuple_data_pool()->AcquireData(prev_tuple_pool_.get(), false);
    row_batch->tuple_data_pool()->AcquireData(curr_tuple_pool_.get(), false);
    row_batch->AddTupleStream(input_stream_);
    input_stream_ = NULL;
    *eos = true;
  }

  // Transfer resources to the output row batch if enough have accumulated and they're
  // no longer needed by output rows to be returned later.
  if (input_stream_ != NULL && prev_pool_last_result_idx_ != -1 &&
      prev_pool_last_result_idx_ < input_stream_->rows_returned() &&
      prev_pool_last_window_idx_ < window_tuples_.front().first) {
    VLOG_FILE << id() << " Transfer prev pool to output batch, "
              << " pool size: " << prev_tuple_pool_->total_allocated_bytes()
              << " last result idx: " << prev_pool_last_result_idx_
              << " last window idx: " << prev_pool_last_window_idx_;
    row_batch->tuple_data_pool()->AcquireData(prev_tuple_pool_.get(), !*eos);
    prev_pool_last_result_idx_ = -1;
    prev_pool_last_window_idx_ = -1;
  }

  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK();
}

Status AnalyticEvalNode::Reset(RuntimeState* state) {
  result_tuples_.clear();
  window_tuples_.clear();
  last_result_idx_ = -1;
  curr_partition_idx_ = -1;
  prev_pool_last_result_idx_ = -1;
  prev_pool_last_window_idx_ = -1;
  input_eos_ = false;
  // TODO: The Reset() contract allows calling Reset() even if eos has not been reached,
  // but the analytic eval node currently does not support that. In practice, we only
  // call Reset() after eos.
  DCHECK_EQ(curr_tuple_pool_->total_allocated_bytes(), 0);
  DCHECK_EQ(prev_tuple_pool_->total_allocated_bytes(), 0);
  // Call Finalize() to clear evaluator allocations, but do not Close() them,
  // so we can keep evaluating them.
  if (curr_tuple_ != NULL) {
    for (int i = 0; i < evaluators_.size(); ++i) {
      evaluators_[i]->Finalize(fn_ctxs_[i], curr_tuple_, dummy_result_tuple_);
    }
  }
  mem_pool_->Clear();
  // The following members will be re-created in Open().
  DCHECK(input_stream_ == NULL); // input_stream_ should have been attached to last batch.
  curr_tuple_ = NULL;
  child_tuple_cmp_row_ = NULL;
  dummy_result_tuple_ = NULL;
  prev_input_row_ = NULL;
  prev_child_batch_.reset();
  curr_child_batch_.reset();
  return ExecNode::Reset(state);
}

void AnalyticEvalNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (client_ != NULL) state->block_mgr()->ClearReservations(client_);
  if (input_stream_ != NULL) {
    // We may need to clean up input_stream_ if an error occurred at some point.
    input_stream_->Close();
    delete input_stream_;
    input_stream_ = NULL;
  }

  // Close all evaluators and fn ctxs. If an error occurred in Init or Prepare there may
  // be fewer ctxs than evaluators. We also need to Finalize if curr_tuple_ was created
  // in Open.
  DCHECK_LE(fn_ctxs_.size(), evaluators_.size());
  DCHECK(curr_tuple_ == NULL || fn_ctxs_.size() == evaluators_.size());
  for (int i = 0; i < evaluators_.size(); ++i) {
    // Need to make sure finalize is called in case there is any state to clean up.
    if (curr_tuple_ != NULL) {
      evaluators_[i]->Finalize(fn_ctxs_[i], curr_tuple_, dummy_result_tuple_);
    }
    evaluators_[i]->Close(state);
  }
  for (int i = 0; i < fn_ctxs_.size(); ++i) fn_ctxs_[i]->impl()->Close();

  if (partition_by_eq_expr_ctx_ != NULL) partition_by_eq_expr_ctx_->Close(state);
  if (order_by_eq_expr_ctx_ != NULL) order_by_eq_expr_ctx_->Close(state);
  if (prev_child_batch_.get() != NULL) prev_child_batch_.reset();
  if (curr_child_batch_.get() != NULL) curr_child_batch_.reset();
  if (curr_tuple_pool_.get() != NULL) curr_tuple_pool_->FreeAll();
  if (prev_tuple_pool_.get() != NULL) prev_tuple_pool_->FreeAll();
  if (mem_pool_.get() != NULL) mem_pool_->FreeAll();
  if (fn_pool_.get() != NULL) fn_pool_->FreeAll();
  ExecNode::Close(state);
}

void AnalyticEvalNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "AnalyticEvalNode("
       << " window=" << DebugWindowString();
  if (partition_by_eq_expr_ctx_ != NULL) {
    *out << " partition_exprs=" << partition_by_eq_expr_ctx_->root()->DebugString();
  }
  if (order_by_eq_expr_ctx_ != NULL) {
    *out << " order_by_exprs=" << order_by_eq_expr_ctx_->root()->DebugString();
  }
  *out << AggFnEvaluator::DebugString(evaluators_);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

Status AnalyticEvalNode::QueryMaintenance(RuntimeState* state) {
  for (int i = 0; i < evaluators_.size(); ++i) {
    ExprContext::FreeLocalAllocations(evaluators_[i]->input_expr_ctxs());
  }
  return ExecNode::QueryMaintenance(state);
}

}
