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
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "udf/udf-internal.h"

using namespace std;

namespace impala {

AnalyticEvalNode::AnalyticEvalNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    output_tuple_desc_(
        row_desc().tuple_descriptors()[tnode.analytic_node.output_tuple_id]),
    current_tuple_(NULL),
    num_owned_output_tuples_(0),
    prev_input_row_(NULL),
    input_row_idx_(0),
    input_eos_(false),
    evaluation_timer_(NULL) {
  // TODO: Properly handle different intermediate and output tuples.
  DCHECK_EQ(tnode.analytic_node.output_tuple_id,
      tnode.analytic_node.intermediate_tuple_id);
}

Status AnalyticEvalNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(ExecNode::Init(tnode));
  const TAnalyticNode& analytic_node = tnode.analytic_node;
  window_ = analytic_node.window;
  for (int i = 0; i < analytic_node.analytic_functions.size(); ++i) {
    AggFnEvaluator* evaluator;
    RETURN_IF_ERROR(AggFnEvaluator::Create(
          pool_, analytic_node.analytic_functions[i], &evaluator));
    evaluators_.push_back(evaluator);
  }
  RETURN_IF_ERROR(partition_exprs_.Init(analytic_node.partition_exprs, NULL, pool_));
  RETURN_IF_ERROR(ordering_exprs_.Init(analytic_node.order_by_exprs, NULL, pool_));
  return Status::OK;
}

Status AnalyticEvalNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  DCHECK(child(0)->row_desc().IsPrefixOf(row_desc()));
  output_tuple_pool_.reset(new MemPool(mem_tracker()));
  mem_pool_.reset(new MemPool(mem_tracker()));
  evaluation_timer_ = ADD_TIMER(runtime_profile(), "EvaluationTime");
  input_batch_.reset(new RowBatch(child(0)->row_desc(), state->batch_size(),
      mem_tracker()));

  fn_ctxs_.resize(evaluators_.size());
  for (int i = 0; i < evaluators_.size(); ++i) {
    // TODO: These should be different slots once we fully support
    // different intermediate and output tuples.
    RETURN_IF_ERROR(evaluators_[i]->Prepare(state, child(0)->row_desc(),
        output_tuple_desc_->slots()[i], output_tuple_desc_->slots()[i],
        mem_pool_.get(), &fn_ctxs_[i]));
    state->obj_pool()->Add(fn_ctxs_[i]);
  }
  RETURN_IF_ERROR(partition_exprs_.Prepare(state, child(0)->row_desc(), row_descriptor_));
  RETURN_IF_ERROR(ordering_exprs_.Prepare(state, child(0)->row_desc(), row_descriptor_));
  return Status::OK;
}

inline Tuple* AnalyticEvalNode::CreateOutputTuple() {
  Tuple* result = Tuple::Create(output_tuple_desc_->byte_size(),
      output_tuple_pool_.get());
  ++num_owned_output_tuples_;
  DCHECK(current_tuple_ != NULL);
  memcpy(result, current_tuple_, output_tuple_desc_->byte_size());
  return result;
}

Status AnalyticEvalNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(state->CheckQueryState());
  RETURN_IF_ERROR(child(0)->Open(state));
  // Hack to create the BufferedBlockMgr if it doesn't exist (child is not a SortNode).
  // TODO: Remove this once the block mgr is created in a single, shared place
  RETURN_IF_ERROR(state->CreateBlockMgr(mem_tracker()->SpareCapacity() * 0.5));
  RETURN_IF_ERROR(state->block_mgr()->RegisterClient(2, mem_tracker(), &client_));
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

  // Only allocated once, output tuples are allocated from output_tuple_pool_ and copy
  // the intermediate state from current_tuple_.
  current_tuple_ = Tuple::Create(output_tuple_desc_->byte_size(), mem_pool_.get());
  AggFnEvaluator::Init(evaluators_, fn_ctxs_, current_tuple_);

  // Initialize and process the first input batch so that some initial state can be
  // set here to avoid special casing in GetNext().
  RowBatch input_batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
  while (!input_eos_ && prev_input_row_ == NULL) {
    RETURN_IF_ERROR(child(0)->GetNext(state, &input_batch, &input_eos_));
    if (input_batch.num_rows() > 0) {
      prev_input_row_ = input_batch.GetRow(0);
      RETURN_IF_ERROR(ProcessInputBatch(state, &input_batch));
    }
    input_batch.Reset();
  }
  return Status::OK;
}

string AnalyticEvalNode::DebugEvaluatedRowsString() const {
  stringstream ss;
  ss << "stream_idx=" << input_stream_->rows_returned() << " stream_size="
     << input_stream_->num_rows() << " evaluated_tuple_idx=[";
  for (list<pair<int64_t, Tuple*> >::const_iterator it = result_tuples_.begin();
       it != result_tuples_.end(); ++it) {
    ss << it->first;
    if (*it != result_tuples_.back()) ss << ", ";
  }
  ss << "]";
  return ss.str();
}

void AnalyticEvalNode::FinalizeOutputTuple(bool reinitialize_current_tuple) {
  DCHECK(current_tuple_ != NULL);
  Tuple* output_tuple = CreateOutputTuple();
  for (int i = 0; i < evaluators_.size(); ++i) {
    // TODO: Currently assumes UDAs can call Finalize() repeatedly, will need to change
    // for functions that have different intermediate state.
    evaluators_[i]->Finalize(fn_ctxs_[i], output_tuple, output_tuple);
  }
  DCHECK(result_tuples_.empty() ||
      input_stream_->num_rows() > result_tuples_.back().first);
  result_tuples_.push_back(
      pair<int64_t, Tuple*>(input_stream_->num_rows(), output_tuple));

  if (reinitialize_current_tuple) {
    current_tuple_->Init(output_tuple_desc_->byte_size());
    AggFnEvaluator::Init(evaluators_, fn_ctxs_, current_tuple_);
  }
}

Status AnalyticEvalNode::ProcessInputBatch(RuntimeState* state, RowBatch* row_batch) {
  VLOG_ROW << "ProcessInputBatch: " << DebugEvaluatedRowsString()
           << " input num_rows=" << row_batch->num_rows();
  SCOPED_TIMER(evaluation_timer_);
  for (int i = 0; i < row_batch->num_rows(); ++i) {
    TupleRow* row = row_batch->GetRow(i);
    // Every row is first compared against the previous row to determine if (a) the row
    // starts a new partition or (b) the row does not share the same values for the
    // ordering exprs. When either of these occurs, the evaluators_ are finalized and
    // the result tuple is added to result_tuples_ so that it may be added to output
    // rows in GetNextOutputBatch(). When a new partition is found (a), a new, empty
    // output tuple is created and initialized over the evaluators_. If the row has
    // different values for the ordering exprs (b), then a new tuple is created but
    // copied from current_tuple_ because the original is used for one or more previous
    // row(s) but the incremental state still applies to the current row.
    // TODO: Replace TupleRowComparators with predicates (to be generated by the planner)
    bool next_partition = 0 != partition_comparator_->Compare(prev_input_row_, row);
    if (next_partition || (0 != ordering_comparator_->Compare(prev_input_row_, row))) {
      FinalizeOutputTuple(next_partition);
    }

    // The evaluators_ are updated with the current row.
    AggFnEvaluator::Update(evaluators_, fn_ctxs_, row, current_tuple_);

    if (UNLIKELY(!input_stream_->AddRow(row))) {
      // AddRow returns false if an error occurs (available via status()) or there is
      // not enough memory (status() is OK). If there isn't enough memory, we unpin
      // the stream and continue writing/reading in unpinned mode.
      // TODO: Consider re-pinning later if the output stream is fully consumed.
      RETURN_IF_ERROR(input_stream_->status());
      RETURN_IF_ERROR(input_stream_->UnpinAllBlocks());
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
  if (input_eos_) FinalizeOutputTuple(false);
  return Status::OK;
}

Status AnalyticEvalNode::GetNextOutputBatch(RowBatch* output_batch, bool* eos) {
  SCOPED_TIMER(evaluation_timer_);
  VLOG_ROW << "GetNextOutputBatch: " << DebugEvaluatedRowsString();
  if (input_stream_->rows_returned() == input_stream_->num_rows()) {
    DCHECK(result_tuples_.empty());
    *eos = true;
    return Status::OK;
  }

  const int num_child_tuples = child(0)->row_desc().tuple_descriptors().size();
  int64_t stream_idx = input_stream_->rows_returned();
  ExprContext** ctxs = &conjunct_ctxs_[0];
  int num_ctxs = conjunct_ctxs_.size();
  while (true) {
    // Copy rows until we hit the limit/capacity or exhaust input_batch_
    while (!ReachedLimit() && !output_batch->AtCapacity() &&
        input_row_idx_ < input_batch_->num_rows()) { // TODO: num_rows() or capacity()?
      DCHECK(!result_tuples_.empty());
      TupleRow* src = input_batch_->GetRow(input_row_idx_);
      ++input_row_idx_;
      ++stream_idx;
      TupleRow* dest = output_batch->GetRow(output_batch->AddRow());
      // CopyRow works as expected, input_batch rows form a prefix of output_batch rows.
      input_batch_->CopyRow(src, dest);
      dest->SetTuple(num_child_tuples, result_tuples_.front().second);

      if (ExecNode::EvalConjuncts(ctxs, num_ctxs, dest)) {
        output_batch->CommitLastRow();
        ++num_rows_returned_;
      }

      // Remove the head of result_tuples_ if all rows using that evaluated tuple
      // have been returned.
      DCHECK_LE(stream_idx, result_tuples_.front().first);
      if (stream_idx >= result_tuples_.front().first) result_tuples_.pop_front();
    }
    if (output_batch->AtCapacity()) return Status::OK;

    input_batch_->TransferResourceOwnership(output_batch);
    input_batch_->Reset();
    input_row_idx_ = 0;
    if (ReachedLimit() || input_stream_->rows_returned() == input_stream_->num_rows()) {
      *eos = true;
      break;
    }

    // Need more rows
    bool stream_eos = false;
    RETURN_IF_ERROR(input_stream_->GetNext(input_batch_.get(), &stream_eos));
    DCHECK(!stream_eos || input_stream_->rows_returned() == input_stream_->num_rows());
  }

  DCHECK((!*eos) || result_tuples_.empty() || ReachedLimit());
  return Status::OK;
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
  RowBatch input_batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
  while (!input_eos_) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->CheckQueryState());
    int64_t num_rows_to_return = 0;
    if (!result_tuples_.empty()) {
      // Compute the number of rows that are ready to be returned, i.e. rows in
      // input_stream_ that have an output tuple (containing the analytic fn results) in
      // result_tuples_. If there is at least enough rows to return an entire row
      // batch, break out of the input row processing loop to return a row batch.
      int64_t last_evaluated_row_idx = result_tuples_.back().first;
      num_rows_to_return = last_evaluated_row_idx - input_stream_->rows_returned();
      DCHECK_GE(num_rows_to_return, 0);
    }
    if (num_rows_to_return >= state->batch_size()) break;

    RETURN_IF_ERROR(child(0)->GetNext(state, &input_batch, &input_eos_));
    RETURN_IF_ERROR(ProcessInputBatch(state, &input_batch));
    input_batch.Reset();
  }

  bool output_eos = false;
  RETURN_IF_ERROR(GetNextOutputBatch(row_batch, &output_eos));
  if (input_eos_ && output_eos) *eos = true;

  // When the number of tuples allocated from output_tuple_pool_ reaches the row batch
  // size, transfer resources to the output row batch so that resources don't accumulate.
  if (num_owned_output_tuples_ >= state->batch_size()) {
    row_batch->tuple_data_pool()->AcquireData(output_tuple_pool_.get(), !*eos);
    num_owned_output_tuples_ = 0;
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
    evaluators_[i]->Finalize(fn_ctxs_[i], current_tuple_, current_tuple_);
    evaluators_[i]->Close(state);
    fn_ctxs_[i]->impl()->Close();
  }
  partition_exprs_.Close(state);
  ordering_exprs_.Close(state);
  if (input_batch_.get() != NULL) input_batch_.reset();
  if (output_tuple_pool_.get() != NULL) output_tuple_pool_->FreeAll();
  if (mem_pool_.get() != NULL) mem_pool_->FreeAll();
  ExecNode::Close(state);
}

void AnalyticEvalNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "AnalyticEvalNode("
       << " partition_exprs="
       << Expr::DebugString(partition_exprs_.lhs_ordering_expr_ctxs())
       << " ordering_exprs="
       << Expr::DebugString(ordering_exprs_.lhs_ordering_expr_ctxs());
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

}
