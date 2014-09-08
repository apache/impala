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

using namespace std;

// Maximum number of result tuples allocated from curr_result_tuple_pool_ before
// they are transferred to the output batch.
static const int MAX_NUM_OWNED_RESULT_TUPLES = 1024;

namespace impala {

AnalyticEvalNode::AnalyticEvalNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    intermediate_tuple_desc_(
        descs.GetTupleDescriptor(tnode.analytic_node.intermediate_tuple_id)),
    result_tuple_desc_(
        descs.GetTupleDescriptor(tnode.analytic_node.output_tuple_id)),
    fn_scope_(GetAnalyticFnScope(tnode.analytic_node)),
    window_(tnode.analytic_node.window),
    curr_tuple_(NULL),
    dummy_result_tuple_(NULL),
    num_tuples_in_curr_result_pool_(0),
    prev_result_tuple_pool_stream_idx_(-1),
    curr_partition_stream_idx_(0),
    prev_input_row_(NULL),
    input_stream_batch_idx_(0),
    input_stream_idx_(0),
    input_eos_(false),
    evaluation_timer_(NULL) {
}

AnalyticEvalNode::AnalyticFnScope AnalyticEvalNode::GetAnalyticFnScope(
    const TAnalyticNode& node) {
  if (!node.__isset.window) return AnalyticEvalNode::PARTITION;
  if (node.window.type == TAnalyticWindowType::RANGE) return AnalyticEvalNode::RANGE;
  DCHECK_EQ(node.window.type, TAnalyticWindowType::ROWS);
  return AnalyticEvalNode::ROWS;
}

Status AnalyticEvalNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(ExecNode::Init(tnode));
  const TAnalyticNode& analytic_node = tnode.analytic_node;
  for (int i = 0; i < analytic_node.analytic_functions.size(); ++i) {
    AggFnEvaluator* evaluator;
    RETURN_IF_ERROR(AggFnEvaluator::Create(
          pool_, analytic_node.analytic_functions[i], true, &evaluator));
    evaluators_.push_back(evaluator);
  }
  DCHECK(fn_scope_ != PARTITION || analytic_node.order_by_exprs.empty());
  DCHECK(fn_scope_ == PARTITION || window_.__isset.window_end)
      << "UNBOUNDED FOLLOWING is not supported.";
  RETURN_IF_ERROR(partition_exprs_.Init(analytic_node.partition_exprs, NULL, pool_));
  RETURN_IF_ERROR(ordering_exprs_.Init(analytic_node.order_by_exprs, NULL, pool_));
  return Status::OK;
}

Status AnalyticEvalNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  DCHECK(child(0)->row_desc().IsPrefixOf(row_desc()));
  curr_result_tuple_pool_.reset(new MemPool(mem_tracker()));
  prev_result_tuple_pool_.reset(new MemPool(mem_tracker()));
  mem_pool_.reset(new MemPool(mem_tracker()));
  evaluation_timer_ = ADD_TIMER(runtime_profile(), "EvaluationTime");
  input_stream_batch_.reset(new RowBatch(child(0)->row_desc(), state->batch_size(),
      mem_tracker()));

  fn_ctxs_.resize(evaluators_.size());
  DCHECK_EQ(result_tuple_desc_->slots().size(), evaluators_.size());
  for (int i = 0; i < evaluators_.size(); ++i) {
    RETURN_IF_ERROR(evaluators_[i]->Prepare(state, child(0)->row_desc(),
        intermediate_tuple_desc_->slots()[i], result_tuple_desc_->slots()[i],
        mem_pool_.get(), &fn_ctxs_[i]));
    state->obj_pool()->Add(fn_ctxs_[i]);
  }
  RETURN_IF_ERROR(partition_exprs_.Prepare(state, child(0)->row_desc(), row_descriptor_));
  RETURN_IF_ERROR(ordering_exprs_.Prepare(state, child(0)->row_desc(), row_descriptor_));
  return Status::OK;
}

Status AnalyticEvalNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(state->CheckQueryState());
  RETURN_IF_ERROR(child(0)->Open(state));
  RETURN_IF_ERROR(state->block_mgr()->RegisterClient(2, mem_tracker(), state, &client_));
  input_stream_.reset(new BufferedTupleStream(state, child(0)->row_desc(),
      state->block_mgr(), client_, true /* delete_on_read */, true /* read_write */));
  RETURN_IF_ERROR(input_stream_->Init());

  DCHECK_EQ(evaluators_.size(), fn_ctxs_.size());
  for (int i = 0; i < evaluators_.size(); ++i) {
    RETURN_IF_ERROR(evaluators_[i]->Open(state, fn_ctxs_[i]));
  }

  RETURN_IF_ERROR(partition_exprs_.Open(state));
  partition_comparator_.reset(new TupleRowComparator(
      partition_exprs_.lhs_ordering_expr_ctxs(),
      partition_exprs_.rhs_ordering_expr_ctxs(), false, false));
  RETURN_IF_ERROR(ordering_exprs_.Open(state));
  ordering_comparator_.reset(new TupleRowComparator(
      ordering_exprs_.lhs_ordering_expr_ctxs(),
      ordering_exprs_.rhs_ordering_expr_ctxs(), false, false));

  // An intermediate tuple is only allocated once and is reused.
  curr_tuple_ = Tuple::Create(intermediate_tuple_desc_->byte_size(), mem_pool_.get());
  AggFnEvaluator::Init(evaluators_, fn_ctxs_, curr_tuple_);
  dummy_result_tuple_ = Tuple::Create(result_tuple_desc_->byte_size(), mem_pool_.get());

  // Initialize state for the first partition.
  InitPartition(0);

  // Fetch the first input batch so that some prev_input_row_ can be set here to avoid
  // special casing in GetNext().
  prev_child_batch_.reset(new RowBatch(child(0)->row_desc(), state->batch_size(),
      mem_tracker()));
  curr_child_batch_.reset(new RowBatch(child(0)->row_desc(), state->batch_size(),
      mem_tracker()));
  while (!input_eos_ && prev_input_row_ == NULL) {
    RETURN_IF_ERROR(child(0)->GetNext(state, curr_child_batch_.get(), &input_eos_));
    if (curr_child_batch_->num_rows() > 0) {
      prev_input_row_ = curr_child_batch_->GetRow(0);
    } else {
      // Empty batch, still need to reset.
      curr_child_batch_->Reset();
    }
  }
  if (prev_input_row_ == NULL) {
    DCHECK(input_eos_);
    // Delete curr_child_batch_ to indicate there is no batch to process in GetNext()
    curr_child_batch_.reset();
  }
  return Status::OK;
}

string DebugWindowBoundString(const TAnalyticWindowBoundary& b) {
  if (b.type == TAnalyticWindowBoundaryType::CURRENT_ROW) {
    return "CURRENT_ROW";
  }
  stringstream ss;
  if (b.__isset.rows_offset_idx) {
    ss << b.rows_offset_idx;
  } else {
    DCHECK(b.__isset.range_offset_expr);
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
  DCHECK(window_.__isset.window_end) << "UNBOUNDED FOLLOWING is not implemented";
  ss << ", end=" << DebugWindowBoundString(window_.window_end) << "}";
  return ss.str();
}

string AnalyticEvalNode::DebugEvaluatedRowsString() const {
  stringstream ss;
  ss << "stream idx=" << input_stream_idx_ << " num_returned="
     << input_stream_->rows_returned() << " num_rows="
     << input_stream_->num_rows() << " evaluated_tuple_idx=[";
  for (list<pair<int64_t, Tuple*> >::const_iterator it = result_tuples_.begin();
       it != result_tuples_.end(); ++it) {
    ss << it->first;
    if (*it != result_tuples_.back()) ss << ", ";
  }
  ss << "]";
  return ss.str();
}

void AnalyticEvalNode::AddResultTuple(int64_t stream_idx) {
  VLOG_ROW << "AddResultTuple idx=" << stream_idx;
  DCHECK(curr_tuple_ != NULL);
  Tuple* result_tuple = Tuple::Create(result_tuple_desc_->byte_size(),
      curr_result_tuple_pool_.get());
  ++num_tuples_in_curr_result_pool_;

  for (int i = 0; i < evaluators_.size(); ++i) {
    // TODO: Assumes we can call GetValue() or Finalize() repeatedly; need to register
    // agg fns as being compatible for analytic fn evaluation.
    if (evaluators_[i]->SupportsGetValue()) {
      evaluators_[i]->GetValue(fn_ctxs_[i], curr_tuple_, result_tuple);
    } else {
      evaluators_[i]->Finalize(fn_ctxs_[i], curr_tuple_, result_tuple);
    }
  }

  DCHECK(result_tuples_.empty() || stream_idx > result_tuples_.back().first)
      << "stream_idx=" << stream_idx << " state:" << DebugEvaluatedRowsString();
  result_tuples_.push_back(
      pair<int64_t, Tuple*>(stream_idx, result_tuple));
  VLOG_ROW << "Added result tuple, final state: " << DebugEvaluatedRowsString();
}

inline void AnalyticEvalNode::TryAddResultTupleForPrevRow(bool next_partition,
    int64_t stream_idx, TupleRow* row) {
  // The analytic fns are finalized after the previous row if we found a new partition
  // or the window is a RANGE and the order by exprs changed. For ROWS windows we do not
  // need to compare the current row to the previous row.
  VLOG_ROW << "TryAddResultTupleForPrevRow partition=" << next_partition
           << " idx=" << stream_idx;
  if (fn_scope_ == ROWS) return;
  if (next_partition || (fn_scope_ == RANGE &&
      0 != ordering_comparator_->Compare(prev_input_row_, row))) {
    AddResultTuple(stream_idx - 1);
  }
}

inline void AnalyticEvalNode::TryAddResultTupleForCurrRow(int64_t stream_idx,
    TupleRow* row) {
  VLOG_ROW << "TryAddResultTupleForCurrRow idx=" << stream_idx;
  // Analytic functions are only finalized for the current row for ROWS windows.
  if (fn_scope_ != ROWS) return;
  DCHECK(window_.__isset.window_end);
  if (stream_idx - rows_end_idx() < curr_partition_stream_idx_) return;
  AddResultTuple(stream_idx - rows_end_idx());
}

inline void AnalyticEvalNode::InitPartition(int64_t stream_idx) {
  VLOG_ROW << "InitPartition idx=" << stream_idx;
  curr_partition_stream_idx_ = stream_idx;

  // If the window has an end bound preceding the current row, we will have output
  // tuples for rows beyond the partition so they should be removed.
  while (!result_tuples_.empty() &&
      result_tuples_.back().first >= curr_partition_stream_idx_) {
    DCHECK(window_.window_end.type == TAnalyticWindowBoundaryType::PRECEDING);
    VLOG_ROW << "Removing result past partition, idx=" << result_tuples_.back().first;
    result_tuples_.pop_back();
  }

  if (fn_scope_ == ROWS && curr_partition_stream_idx_ > 0 &&
      window_.window_end.type == TAnalyticWindowBoundaryType::FOLLOWING) {
    AddResultTuple(curr_partition_stream_idx_ - 1);
  }

  VLOG_ROW << "Reset curr_tuple";
  // Call finalize to release resources; result is not needed but the dst tuple must be
  // a tuple described by result_tuple_desc_.
  AggFnEvaluator::Finalize(evaluators_, fn_ctxs_, curr_tuple_, dummy_result_tuple_);
  // Re-initialize curr_tuple_.
  curr_tuple_->Init(intermediate_tuple_desc_->byte_size());
  AggFnEvaluator::Init(evaluators_, fn_ctxs_, curr_tuple_);

  // Add a result tuple containing values set by Init() (e.g. NULL for sum(), 0 for
  // count()) for output rows that have no input rows in the window. We need to add this
  // result tuple before any input rows are consumed and the evaluators are updated.
  if (fn_scope_ == ROWS &&
      window_.window_end.type == TAnalyticWindowBoundaryType::PRECEDING) {
    AddResultTuple(curr_partition_stream_idx_ - rows_end_idx() - 1);
  }
}

Status AnalyticEvalNode::ProcessChildBatch(RuntimeState* state) {
  // TODO: DCHECK input is sorted (even just first row vs prev_input_row_)
  VLOG_ROW << "ProcessChildBatch: " << DebugEvaluatedRowsString()
           << " result_tuples_ size: " << result_tuples_.size()
           << " input batch size:" << curr_child_batch_->num_rows();
  SCOPED_TIMER(evaluation_timer_);
  // BufferedTupleStream::num_rows() returns the total number of rows that have been
  // inserted into the stream (it does not decrease when we read rows), so the index of
  // the next input row that will be inserted will be the current size of the stream.
  int64_t stream_idx = input_stream_->num_rows();
  for (int i = 0; i < curr_child_batch_->num_rows(); ++i, ++stream_idx) {
    TupleRow* row = curr_child_batch_->GetRow(i);

    // Every row is first compared against the previous row to determine if (a) the row
    // starts a new partition or (b) the row does not share the same values for the
    // ordering exprs. When either of these occurs, the evaluators_ are finalized and
    // the result tuple is added to result_tuples_ so that it may be added to output
    // rows in GetNextOutputBatch(). When a new partition is found (a), a new, empty
    // result tuple is created and initialized over the evaluators_. If the row has
    // different values for the ordering exprs (b), then a new tuple is created but
    // copied from curr_tuple_ because the original is used for one or more previous
    // row(s) but the incremental state still applies to the current row.
    // TODO: Replace TupleRowComparators with predicates (to be generated by the planner)
    bool next_partition = 0 != partition_comparator_->Compare(prev_input_row_, row);
    TryAddResultTupleForPrevRow(next_partition, stream_idx, row);
    if (next_partition) InitPartition(stream_idx);

    // The evaluators_ are updated with the current row.
    VLOG_ROW << "UpdateRow idx=" << stream_idx;
    AggFnEvaluator::Update(evaluators_, fn_ctxs_, row, curr_tuple_);
    TryAddResultTupleForCurrRow(stream_idx, row);

    if (UNLIKELY(!input_stream_->AddRow(row))) {
      // AddRow returns false if an error occurs (available via status()) or there is
      // not enough memory (status() is OK). If there isn't enough memory, we unpin
      // the stream and continue writing/reading in unpinned mode.
      // TODO: Consider re-pinning later if the output stream is fully consumed.
      RETURN_IF_ERROR(input_stream_->status());
      RETURN_IF_ERROR(input_stream_->UnpinStream());
      if (!input_stream_->AddRow(row)) {
        // Rows should be added in unpinned mode unless an error occurs.
        RETURN_IF_ERROR(input_stream_->status());
        DCHECK(false);
      }
    }
    prev_input_row_ = row;
  }

  // TODO: Avoid copying and instead maintain the last two input batches
  DCHECK(prev_input_row_ != NULL);
  prev_input_row_ = prev_input_row_->DeepCopy(child(0)->row_desc().tuple_descriptors(),
      mem_pool_.get());
  // We need to add the results for the last row(s).
  if (input_eos_ && (result_tuples_.empty() ||
      result_tuples_.back().first < stream_idx - 1)) {
    AddResultTuple(stream_idx - 1);
  }

  if (num_tuples_in_curr_result_pool_ >= MAX_NUM_OWNED_RESULT_TUPLES) {
    VLOG_ROW << "Transferring resources from curr result pool to prev pool, num="
             << num_tuples_in_curr_result_pool_ << " prev pool now thru idx:"
             << stream_idx << " was:" << prev_result_tuple_pool_stream_idx_;
    prev_result_tuple_pool_->AcquireData(curr_result_tuple_pool_.get(), false);
    DCHECK(!result_tuples_.empty());
    prev_result_tuple_pool_stream_idx_ = result_tuples_.back().first;
    num_tuples_in_curr_result_pool_ = 0;
  }
  return Status::OK;
}

Status AnalyticEvalNode::GetNextOutputBatch(RowBatch* output_batch, bool* eos) {
  SCOPED_TIMER(evaluation_timer_);
  VLOG_ROW << "GetNextOutputBatch: " << DebugEvaluatedRowsString();
  if (input_stream_->rows_returned() == input_stream_->num_rows()) {
    *eos = true;
    return Status::OK;
  }

  const int num_child_tuples = child(0)->row_desc().tuple_descriptors().size();
  ExprContext** ctxs = &conjunct_ctxs_[0];
  int num_ctxs = conjunct_ctxs_.size();
  while (true) {
    // Copy rows until we hit the limit/capacity or exhaust input_stream_batch_
    while (!ReachedLimit() && !output_batch->AtCapacity() &&
        input_stream_batch_idx_ < input_stream_batch_->num_rows()) {
      DCHECK(!result_tuples_.empty());
      TupleRow* src = input_stream_batch_->GetRow(input_stream_batch_idx_);
      ++input_stream_batch_idx_;
      TupleRow* dest = output_batch->GetRow(output_batch->AddRow());
      // CopyRow works as expected: input_batch tuples form a prefix of output_batch
      // tuples.
      input_stream_batch_->CopyRow(src, dest);
      dest->SetTuple(num_child_tuples, result_tuples_.front().second);

      if (ExecNode::EvalConjuncts(ctxs, num_ctxs, dest)) {
        output_batch->CommitLastRow();
        ++num_rows_returned_;
      }

      // Remove the head of result_tuples_ if all rows using that evaluated tuple
      // have been returned.
      VLOG_ROW << "Output row idx: " << input_stream_idx_ << " "
               << DebugEvaluatedRowsString();
      DCHECK_LE(input_stream_idx_, result_tuples_.front().first);
      if (input_stream_idx_ >= result_tuples_.front().first) result_tuples_.pop_front();
      ++input_stream_idx_;
    }
    if (output_batch->AtCapacity()) return Status::OK;

    input_stream_batch_->TransferResourceOwnership(output_batch);
    input_stream_batch_->Reset();
    input_stream_batch_idx_ = 0;
    if (ReachedLimit() || input_stream_->rows_returned() == input_stream_->num_rows()) {
      *eos = true;
      break;
    }

    // Need more rows
    bool stream_eos = false;
    DCHECK_EQ(input_stream_idx_, input_stream_->rows_returned());
    RETURN_IF_ERROR(input_stream_->GetNext(input_stream_batch_.get(), &stream_eos));
    DCHECK(!stream_eos || input_stream_->rows_returned() == input_stream_->num_rows());
  }
  return Status::OK;
}

inline int64_t AnalyticEvalNode::NumOutputRowsReady() const {
  if (result_tuples_.empty()) return 0;
  int64_t last_evaluated_row_idx = result_tuples_.back().first;
  int64_t rows_to_return = last_evaluated_row_idx - input_stream_idx_;
  if (last_evaluated_row_idx > input_stream_->num_rows()) {
    // This happens when we were able to add a result tuple before consuming child rows,
    // e.g. initializing a new partition with an end bound that is X preceding. The first
    // X rows get the default value and we add that tuple to result_tuples_ before
    // consuming child rows. It's possible the result is negative, and that's fine
    // because this result is only used to determine if the number of rows to return
    // is at least as big as the batch size.
    rows_to_return -= last_evaluated_row_idx - input_stream_->num_rows();
  } else {
    DCHECK_GE(rows_to_return, 0);
  }
  return rows_to_return;
}

Status AnalyticEvalNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(state->CheckQueryState());
  VLOG_ROW << "GetNext: " << DebugEvaluatedRowsString();

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK;
  } else {
    *eos = false;
  }

  // Processes input row batches until there are enough rows that are ready to return.
  while (curr_child_batch_.get() != NULL && NumOutputRowsReady() < state->batch_size()) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->CheckQueryState());
    RETURN_IF_ERROR(ProcessChildBatch(state));
    // TODO: DCHECK that the size of result_tuples_ is bounded. It shouldn't be larger
    // than 2x the batch size unless the end bound has an offset preceding, in which
    // case it may be slightly larger (proportional to the offset but still bounded).
    if (input_eos_) {
      // Already processed the last child batch. Clean up and break.
      curr_child_batch_.reset();
      prev_child_batch_.reset();
      break;
    }
    prev_child_batch_->Reset();
    prev_child_batch_.swap(curr_child_batch_);
    RETURN_IF_ERROR(child(0)->GetNext(state, curr_child_batch_.get(), &input_eos_));
  }

  bool output_eos = false;
  RETURN_IF_ERROR(GetNextOutputBatch(row_batch, &output_eos));
  if (curr_child_batch_.get() == NULL && output_eos) *eos = true;

  // Transfer resources to the output row batch if enough have accumulated and they're
  // no longer needed by output rows to be returned later.
  VLOG_ROW << "Transfer prev_result_tuple_pool_ thru idx:"
           << prev_result_tuple_pool_stream_idx_;
  if (prev_result_tuple_pool_stream_idx_ != -1 &&
      prev_result_tuple_pool_stream_idx_ < input_stream_idx_) {
    VLOG_ROW << "Transfer prev_result_tuple_pool_";
    row_batch->tuple_data_pool()->AcquireData(prev_result_tuple_pool_.get(), !*eos);
    prev_result_tuple_pool_stream_idx_ = -1;
  }

  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK;
}

void AnalyticEvalNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (input_stream_.get() != NULL) input_stream_->Close();

  DCHECK_EQ(evaluators_.size(), fn_ctxs_.size());
  for (int i = 0; i < evaluators_.size(); ++i) {
    // Need to make sure finalize is called in case there is any state to clean up.
    evaluators_[i]->Finalize(fn_ctxs_[i], curr_tuple_, dummy_result_tuple_);
    evaluators_[i]->Close(state);
    fn_ctxs_[i]->impl()->Close();
  }
  partition_exprs_.Close(state);
  ordering_exprs_.Close(state);
  if (prev_child_batch_.get() != NULL) prev_child_batch_.reset();
  if (curr_child_batch_.get() != NULL) curr_child_batch_.reset();
  if (input_stream_batch_.get() != NULL) input_stream_batch_.reset();
  if (curr_result_tuple_pool_.get() != NULL) curr_result_tuple_pool_->FreeAll();
  if (prev_result_tuple_pool_.get() != NULL) prev_result_tuple_pool_->FreeAll();
  if (mem_pool_.get() != NULL) mem_pool_->FreeAll();
  ExecNode::Close(state);
}

inline int64_t AnalyticEvalNode::rows_start_idx() const {
  DCHECK_EQ(fn_scope_, ROWS);
  DCHECK(window_.__isset.window_start);
  DCHECK(window_.window_start.__isset.rows_offset_idx);
  return window_.window_start.rows_offset_idx;
}

inline int64_t AnalyticEvalNode::rows_end_idx() const {
  DCHECK_EQ(fn_scope_, ROWS);
  DCHECK(window_.__isset.window_end);
  DCHECK(window_.window_end.__isset.rows_offset_idx);
  return window_.window_end.rows_offset_idx;
}

void AnalyticEvalNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "AnalyticEvalNode("
       << " window=" << DebugWindowString()
       << " partition_exprs="
       << Expr::DebugString(partition_exprs_.lhs_ordering_expr_ctxs())
       << " ordering_exprs="
       << Expr::DebugString(ordering_exprs_.lhs_ordering_expr_ctxs());
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

}
