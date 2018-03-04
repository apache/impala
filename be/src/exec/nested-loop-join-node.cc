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

#include "exec/nested-loop-join-node.h"

#include <sstream>
#include <gutil/strings/substitute.h>

#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/bitmap.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"
#include "gen-cpp/PlanNodes_types.h"
#include "common/names.h"

using namespace impala;
using namespace strings;

NestedLoopJoinNode::NestedLoopJoinNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
  : BlockingJoinNode("NestedLoopJoinNode", tnode.nested_loop_join_node.join_op, pool,
        tnode, descs),
    build_batches_(NULL),
    current_build_row_idx_(0),
    process_unmatched_build_rows_(false) {
}

NestedLoopJoinNode::~NestedLoopJoinNode() {
  DCHECK(is_closed());
}

Status NestedLoopJoinNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  RETURN_IF_ERROR(BlockingJoinNode::Init(tnode, state));
  DCHECK(tnode.__isset.nested_loop_join_node);
  // join_conjunct_evals_ are evaluated in the context of rows assembled from
  // all inner and outer tuples.
  RowDescriptor full_row_desc(*child(0)->row_desc(), *child(1)->row_desc());
  RETURN_IF_ERROR(ScalarExpr::Create(tnode.nested_loop_join_node.join_conjuncts,
      full_row_desc, state, &join_conjuncts_));
  DCHECK(tnode.nested_loop_join_node.join_op != TJoinOp::CROSS_JOIN ||
      join_conjuncts_.size() == 0) << "Join conjuncts in a cross join";
  return Status::OK();
}

Status NestedLoopJoinNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(BlockingJoinNode::Open(state));
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(join_conjunct_evals_, state));

  // Check for errors and free expr result allocations before opening children.
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  if (child(1)->type() == TPlanNodeType::type::SINGULAR_ROW_SRC_NODE) {
    DCHECK(IsInSubplan());
    // When inside a subplan, open the first child before doing the build such that
    // UnnestNodes on the probe side are opened and project their unnested collection
    // slots. Otherwise, the build might unnecessarily deep-copy those collection slots,
    // and this node would return them in GetNext().
    // TODO: Remove this special-case behavior for subplans once we have proper
    // projection. See UnnestNode for details on the current projection implementation.
    RETURN_IF_ERROR(child(0)->Open(state));
    RETURN_IF_ERROR(ConstructSingularBuildSide(state));
    DCHECK_EQ(builder_->copied_build_batches()->total_num_rows(), 0);
    build_batches_ = builder_->input_build_batches();
  } else {
    RETURN_IF_ERROR(
        BlockingJoinNode::ProcessBuildInputAndOpenProbe(state, builder_.get()));
    build_batches_ = builder_->GetFinalBuildBatches();
    if (matching_build_rows_ != NULL) {
      RETURN_IF_ERROR(ResetMatchingBuildRows(state, build_batches_->total_num_rows()));
    }
  }
  RETURN_IF_ERROR(BlockingJoinNode::GetFirstProbeRow(state));
  ResetForProbe();
  return Status::OK();
}

Status NestedLoopJoinNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(BlockingJoinNode::Prepare(state));
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(join_conjuncts_, state,
      pool_, expr_perm_pool(), expr_results_pool(), &join_conjunct_evals_));
  builder_.reset(new NljBuilder(child(1)->row_desc(), state));
  RETURN_IF_ERROR(builder_->Prepare(state, mem_tracker()));
  runtime_profile()->PrependChild(builder_->profile());

  // For some join modes we need to record the build rows with matches in a bitmap.
  if (join_op_ == TJoinOp::RIGHT_ANTI_JOIN || join_op_ == TJoinOp::RIGHT_SEMI_JOIN ||
      join_op_ == TJoinOp::RIGHT_OUTER_JOIN || join_op_ == TJoinOp::FULL_OUTER_JOIN) {
    if (child(1)->type() == TPlanNodeType::type::SINGULAR_ROW_SRC_NODE) {
      // Allocate a fixed-size bitmap with a single element if we have a singular
      // row source node as our build child.
      int64_t bitmap_mem_usage = Bitmap::MemUsage(1);
      if (!mem_tracker()->TryConsume(bitmap_mem_usage)) {
        return mem_tracker()->MemLimitExceeded(state,
            "Could not allocate bitmap in nested loop join", bitmap_mem_usage);
      }
      matching_build_rows_.reset(new Bitmap(1));
    } else {
      // Allocate empty bitmap to be expanded later.
      matching_build_rows_.reset(new Bitmap(0));
    }
  }
  return Status::OK();
}

Status NestedLoopJoinNode::Reset(RuntimeState* state) {
  builder_->Reset();
  build_batches_ = NULL;
  matched_probe_ = false;
  current_probe_row_ = NULL;
  probe_batch_pos_ = 0;
  process_unmatched_build_rows_ = false;
  return BlockingJoinNode::Reset(state);
}

void NestedLoopJoinNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  ScalarExprEvaluator::Close(join_conjunct_evals_, state);
  ScalarExpr::Close(join_conjuncts_);
  if (builder_ != NULL) {
    // IMPALA-6595: builder must be closed before child.
    DCHECK(builder_->is_closed() || !children_[1]->is_closed());
    builder_->Close(state);
  }
  build_batches_ = NULL;
  if (matching_build_rows_ != NULL) {
    mem_tracker()->Release(matching_build_rows_->MemUsage());
    matching_build_rows_.reset();
  }
  BlockingJoinNode::Close(state);
}

Status NestedLoopJoinNode::ConstructSingularBuildSide(RuntimeState* state) {
  // Optimized path for a common subplan shape with a singular row src node on the build
  // side that avoids expensive timers, virtual function calls, and other overhead.
  DCHECK_EQ(child(1)->type(), TPlanNodeType::type::SINGULAR_ROW_SRC_NODE);
  DCHECK(IsInSubplan());
  RowBatch* batch = builder_->GetNextEmptyBatch();
  bool eos;
  RETURN_IF_ERROR(child(1)->GetNext(state, batch, &eos));
  DCHECK_EQ(batch->num_rows(), 1);
  DCHECK(eos);
  DCHECK(!batch->needs_deep_copy());
  builder_->AddBuildBatch(batch);
  if (matching_build_rows_ != NULL) {
    DCHECK_EQ(matching_build_rows_->num_bits(), 1);
    matching_build_rows_->SetAllBits(false);
  }
  return Status::OK();
}

Status NestedLoopJoinNode::ResetMatchingBuildRows(RuntimeState* state, int64_t num_bits) {
  // Reuse existing bitmap, expanding it if needed.
  if (matching_build_rows_->num_bits() >= num_bits) {
    matching_build_rows_->SetAllBits(false);
  } else {
    // Account for the additional memory used by the bitmap.
    int64_t bitmap_size_increase =
        Bitmap::MemUsage(num_bits) - matching_build_rows_->MemUsage();
    if (!mem_tracker()->TryConsume(bitmap_size_increase)) {
      return mem_tracker()->MemLimitExceeded(state,
          "Could not expand bitmap in nested loop join", bitmap_size_increase);
    }
    matching_build_rows_->Reset(num_bits);
  }
  return Status::OK();
}

void NestedLoopJoinNode::ResetForProbe() {
  DCHECK(build_batches_ != NULL);
  build_row_iterator_ = build_batches_->Iterator();
  current_build_row_idx_ = 0;
  matched_probe_ = false;
}

Status NestedLoopJoinNode::GetNext(RuntimeState* state, RowBatch* output_batch,
    bool* eos) {
  DCHECK(!output_batch->AtCapacity());
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  *eos = false;

  if (!HasValidProbeRow()) {
    RETURN_IF_ERROR(NextProbeRow(state, output_batch));
    if (output_batch->AtCapacity()) goto end;
  }

  switch (join_op_) {
    case TJoinOp::INNER_JOIN:
    case TJoinOp::CROSS_JOIN:
      RETURN_IF_ERROR(GetNextInnerJoin(state, output_batch));
      break;
    case TJoinOp::LEFT_OUTER_JOIN:
      RETURN_IF_ERROR(GetNextLeftOuterJoin(state, output_batch));
      break;
    case TJoinOp::LEFT_SEMI_JOIN:
      RETURN_IF_ERROR(GetNextLeftSemiJoin(state, output_batch));
      break;
    case TJoinOp::LEFT_ANTI_JOIN:
      RETURN_IF_ERROR(GetNextLeftAntiJoin(state, output_batch));
      break;
    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
      RETURN_IF_ERROR(GetNextNullAwareLeftAntiJoin(state, output_batch));
      break;
    case TJoinOp::RIGHT_OUTER_JOIN:
      RETURN_IF_ERROR(GetNextRightOuterJoin(state, output_batch));
      break;
    case TJoinOp::RIGHT_SEMI_JOIN:
      RETURN_IF_ERROR(GetNextRightSemiJoin(state, output_batch));
      break;
    case TJoinOp::RIGHT_ANTI_JOIN:
      RETURN_IF_ERROR(GetNextRightAntiJoin(state, output_batch));
      break;
    case TJoinOp::FULL_OUTER_JOIN:
      RETURN_IF_ERROR(GetNextFullOuterJoin(state, output_batch));
      break;
    default:
      DCHECK(false) << "Unknown join type: " << join_op_;
  }

end:
  if (ReachedLimit()) {
    output_batch->set_num_rows(
        output_batch->num_rows() - (num_rows_returned_ - limit_));
    eos_ = true;
  }
  if (eos_) {
    *eos = true;
    probe_batch_->TransferResourceOwnership(output_batch);
    build_batches_->TransferResourceOwnership(output_batch);
  }
  return Status::OK();
}

Status NestedLoopJoinNode::GetNextInnerJoin(RuntimeState* state,
    RowBatch* output_batch) {
  while (!eos_) {
    DCHECK(HasValidProbeRow());
    bool return_output_batch;
    RETURN_IF_ERROR(
        FindBuildMatches(state, output_batch, &return_output_batch));
    if (return_output_batch) return Status::OK();
    RETURN_IF_ERROR(NextProbeRow(state, output_batch));
    if (output_batch->AtCapacity()) break;
  }
  return Status::OK();
}

Status NestedLoopJoinNode::GetNextLeftOuterJoin(RuntimeState* state,
    RowBatch* output_batch) {
  while (!eos_) {
    DCHECK(HasValidProbeRow());
    bool return_output_batch;
    RETURN_IF_ERROR(
        FindBuildMatches(state, output_batch, &return_output_batch));
    if (return_output_batch) return Status::OK();
    if (!matched_probe_) RETURN_IF_ERROR(ProcessUnmatchedProbeRow(state, output_batch));
    RETURN_IF_ERROR(NextProbeRow(state, output_batch));
    if (output_batch->AtCapacity()) break;
  }
  return Status::OK();
}

Status NestedLoopJoinNode::GetNextLeftSemiJoin(RuntimeState* state,
    RowBatch* output_batch) {
  ScalarExprEvaluator* const* join_conjunct_evals = join_conjunct_evals_.data();
  size_t num_join_conjuncts = join_conjuncts_.size();
  DCHECK_EQ(num_join_conjuncts, join_conjunct_evals_.size());
  const int N = BitUtil::RoundUpToPowerOfTwo(state->batch_size());

  while (!eos_) {
    DCHECK(HasValidProbeRow());
    while (!build_row_iterator_.AtEnd()) {
      DCHECK(HasValidProbeRow());
      CreateOutputRow(semi_join_staging_row_, current_probe_row_,
        build_row_iterator_.GetRow());
      build_row_iterator_.Next();
      ++current_build_row_idx_;
      // This loop can go on for a long time if the conjuncts are very selective. Do
      // expensive query maintenance after every N iterations.
      if ((current_build_row_idx_ & (N - 1)) == 0) {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(QueryMaintenance(state));
      }
      if (!EvalConjuncts(
              join_conjunct_evals, num_join_conjuncts, semi_join_staging_row_)) {
        continue;
      }
      // A match is found. Create the output row from the probe row.
      TupleRow* output_row = output_batch->GetRow(output_batch->AddRow());
      output_batch->CopyRow(current_probe_row_, output_row);
      VLOG_ROW << "match row: " << PrintRow(output_row, *row_desc());
      output_batch->CommitLastRow();
      ++num_rows_returned_;
      if (ReachedLimit()) {
        eos_ = true;
        COUNTER_ADD(rows_returned_counter_, output_batch->num_rows());
        return Status::OK();
      }
      // Stop scanning the build rows for the current probe row. If we reach
      // this point, we already have a match for this probe row.
      break;
    }
    RETURN_IF_ERROR(NextProbeRow(state, output_batch));
    if (output_batch->AtCapacity()) break;
  }
  COUNTER_ADD(rows_returned_counter_, output_batch->num_rows());
  return Status::OK();
}

Status NestedLoopJoinNode::GetNextLeftAntiJoin(RuntimeState* state,
    RowBatch* output_batch) {
  ScalarExprEvaluator* const* join_conjunct_evals = join_conjunct_evals_.data();
  size_t num_join_conjuncts = join_conjuncts_.size();
  DCHECK_EQ(num_join_conjuncts, join_conjunct_evals_.size());
  const int N = BitUtil::RoundUpToPowerOfTwo(state->batch_size());

  while (!eos_) {
    DCHECK(HasValidProbeRow());
    while (!build_row_iterator_.AtEnd()) {
      DCHECK(current_probe_row_ != NULL);
      CreateOutputRow(semi_join_staging_row_, current_probe_row_,
          build_row_iterator_.GetRow());
      build_row_iterator_.Next();
      ++current_build_row_idx_;
      // This loop can go on for a long time if the conjuncts are very selective. Do
      // expensive query maintenance after every N iterations.
      if ((current_build_row_idx_ & (N - 1)) == 0) {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(QueryMaintenance(state));
      }
      if (EvalConjuncts(
              join_conjunct_evals, num_join_conjuncts, semi_join_staging_row_)) {
        // Found a match for the probe row. This row will not be in the result.
        matched_probe_ = true;
        break;
      }
    }
    if (!matched_probe_) RETURN_IF_ERROR(ProcessUnmatchedProbeRow(state, output_batch));
    RETURN_IF_ERROR(NextProbeRow(state, output_batch));
    if (output_batch->AtCapacity()) break;
  }
  return Status::OK();
}

Status NestedLoopJoinNode::GetNextNullAwareLeftAntiJoin(RuntimeState* state,
    RowBatch* output_batch) {
  return Status("Nested-loop join with null-aware anti join mode is not implemented");
}

Status NestedLoopJoinNode::GetNextRightOuterJoin(RuntimeState* state,
    RowBatch* output_batch) {
  while (!eos_ && HasMoreProbeRows()) {
    DCHECK(HasValidProbeRow());
    bool return_output_batch = false;
    RETURN_IF_ERROR(
        FindBuildMatches(state, output_batch, &return_output_batch));
    if (return_output_batch) return Status::OK();
    RETURN_IF_ERROR(NextProbeRow(state, output_batch));
    if (output_batch->AtCapacity()) return Status::OK();
  }
  return ProcessUnmatchedBuildRows(state, output_batch);
}

Status NestedLoopJoinNode::GetNextRightSemiJoin(RuntimeState* state,
    RowBatch* output_batch) {
  ScalarExprEvaluator* const* join_conjunct_evals = join_conjunct_evals_.data();
  size_t num_join_conjuncts = join_conjuncts_.size();
  DCHECK_EQ(num_join_conjuncts, join_conjunct_evals_.size());
  DCHECK(matching_build_rows_ != NULL);
  const int N = BitUtil::RoundUpToPowerOfTwo(state->batch_size());

  while (!eos_) {
    DCHECK(HasValidProbeRow());
    while (!build_row_iterator_.AtEnd()) {
      DCHECK(HasValidProbeRow());
      // This loop can go on for a long time if the conjuncts are very selective.
      // Do query maintenance every N iterations.
      if ((current_build_row_idx_ & (N - 1)) == 0) {
        if (ReachedLimit()) {
          eos_ = true;
          COUNTER_ADD(rows_returned_counter_, output_batch->num_rows());
          return Status::OK();
        }
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(QueryMaintenance(state));
      }

      // Check if we already have a match for the build row.
      if (matching_build_rows_->Get(current_build_row_idx_)) {
        build_row_iterator_.Next();
        ++current_build_row_idx_;
        continue;
      }
      CreateOutputRow(semi_join_staging_row_, current_probe_row_,
          build_row_iterator_.GetRow());
      // Evaluate the join conjuncts on the semi-join staging row.
      if (!EvalConjuncts(
              join_conjunct_evals, num_join_conjuncts, semi_join_staging_row_)) {
        build_row_iterator_.Next();
        ++current_build_row_idx_;
        continue;
      }
      TupleRow* output_row = output_batch->GetRow(output_batch->AddRow());
      matching_build_rows_->Set(current_build_row_idx_, true);
      output_batch->CopyRow(build_row_iterator_.GetRow(), output_row);
      build_row_iterator_.Next();
      ++current_build_row_idx_;
      VLOG_ROW << "match row: " << PrintRow(output_row, *row_desc());
      output_batch->CommitLastRow();
      ++num_rows_returned_;
      if (output_batch->AtCapacity()) {
        COUNTER_ADD(rows_returned_counter_, output_batch->num_rows());
        return Status::OK();
      }
    }
    RETURN_IF_ERROR(NextProbeRow(state, output_batch));
    if (output_batch->AtCapacity()) break;
  }
  COUNTER_ADD(rows_returned_counter_, output_batch->num_rows());
  return Status::OK();
}

Status NestedLoopJoinNode::GetNextRightAntiJoin(RuntimeState* state,
    RowBatch* output_batch) {
  ScalarExprEvaluator* const* join_conjunct_evals = join_conjunct_evals_.data();
  size_t num_join_conjuncts = join_conjuncts_.size();
  DCHECK_EQ(num_join_conjuncts, join_conjunct_evals_.size());
  DCHECK(matching_build_rows_ != NULL);
  const int N = BitUtil::RoundUpToPowerOfTwo(state->batch_size());

  while (!eos_ && HasMoreProbeRows()) {
    DCHECK(HasValidProbeRow());
    while (!build_row_iterator_.AtEnd()) {
      DCHECK(current_probe_row_ != NULL);
      // This loop can go on for a long time if the conjuncts are very selective.
      // Do query maintenance every N iterations.
      if ((current_build_row_idx_ & (N - 1)) == 0) {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(QueryMaintenance(state));
      }

      if (matching_build_rows_->Get(current_build_row_idx_)) {
        build_row_iterator_.Next();
        ++current_build_row_idx_;
        continue;
      }
      CreateOutputRow(semi_join_staging_row_, current_probe_row_,
          build_row_iterator_.GetRow());
      if (EvalConjuncts(
              join_conjunct_evals, num_join_conjuncts, semi_join_staging_row_)) {
        matching_build_rows_->Set(current_build_row_idx_, true);
      }
      build_row_iterator_.Next();
      ++current_build_row_idx_;
    }
    RETURN_IF_ERROR(NextProbeRow(state, output_batch));
    if (output_batch->AtCapacity()) return Status::OK();
  }
  return ProcessUnmatchedBuildRows(state, output_batch);
}

Status NestedLoopJoinNode::GetNextFullOuterJoin(RuntimeState* state,
    RowBatch* output_batch) {
  while (!eos_ && HasMoreProbeRows()) {
    DCHECK(HasValidProbeRow());
    bool return_output_batch;
    RETURN_IF_ERROR(
        FindBuildMatches(state, output_batch, &return_output_batch));
    if (return_output_batch) return Status::OK();
    if (!matched_probe_) {
      RETURN_IF_ERROR(ProcessUnmatchedProbeRow(state, output_batch));
    }
    RETURN_IF_ERROR(NextProbeRow(state, output_batch));
    if (output_batch->AtCapacity()) return Status::OK();
  }
  return ProcessUnmatchedBuildRows(state, output_batch);
}

Status NestedLoopJoinNode::ProcessUnmatchedProbeRow(RuntimeState* state,
    RowBatch* output_batch) {
  DCHECK(!matched_probe_);
  DCHECK(current_probe_row_ != NULL);
  ScalarExprEvaluator* const* conjunct_evals = conjunct_evals_.data();
  size_t num_conjuncts = conjuncts_.size();
  DCHECK_EQ(num_conjuncts, conjunct_evals_.size());
  TupleRow* output_row = output_batch->GetRow(output_batch->AddRow());
  if (join_op_ == TJoinOp::LEFT_OUTER_JOIN || join_op_ == TJoinOp::FULL_OUTER_JOIN) {
    CreateOutputRow(output_row, current_probe_row_, NULL);
  } else {
    DCHECK(join_op_ == TJoinOp::LEFT_ANTI_JOIN) << "Unsupported join operator: " <<
        join_op_;
    output_batch->CopyRow(current_probe_row_, output_row);
  }
  // Evaluate all the other (non-join) conjuncts.
  if (EvalConjuncts(conjunct_evals, num_conjuncts, output_row)) {
    VLOG_ROW << "match row:" << PrintRow(output_row, *row_desc());
    output_batch->CommitLastRow();
    ++num_rows_returned_;
    COUNTER_ADD(rows_returned_counter_, 1);
    if (ReachedLimit()) eos_ = true;
  }
  return Status::OK();
}

Status NestedLoopJoinNode::ProcessUnmatchedBuildRows(
    RuntimeState* state, RowBatch* output_batch) {
  if (!process_unmatched_build_rows_) {
    // Reset the build row iterator to start processing the unmatched build rows.
    build_row_iterator_ = build_batches_->Iterator();
    current_build_row_idx_ = 0;
    process_unmatched_build_rows_ = true;
  }
  ScalarExprEvaluator* const* conjunct_evals = conjunct_evals_.data();
  size_t num_conjuncts = conjuncts_.size();
  DCHECK_EQ(num_conjuncts, conjunct_evals_.size());
  DCHECK(matching_build_rows_ != NULL);

  const int N = BitUtil::RoundUpToPowerOfTwo(state->batch_size());
  while (!build_row_iterator_.AtEnd()) {
    // This loop can go on for a long time if the conjuncts are very selective. Do query
    // maintenance every N iterations.
    if ((current_build_row_idx_ & (N - 1)) == 0) {
      if (ReachedLimit()) {
        eos_ = true;
        COUNTER_ADD(rows_returned_counter_, output_batch->num_rows());
        return Status::OK();
      }
      RETURN_IF_CANCELLED(state);
      RETURN_IF_ERROR(QueryMaintenance(state));
    }

    if (matching_build_rows_->Get(current_build_row_idx_)) {
      build_row_iterator_.Next();
      ++current_build_row_idx_;
      continue;
    }
    // Current build row is unmatched.
    TupleRow* output_row = output_batch->GetRow(output_batch->AddRow());
    if (join_op_ == TJoinOp::FULL_OUTER_JOIN || join_op_ == TJoinOp::RIGHT_OUTER_JOIN) {
      CreateOutputRow(output_row, NULL, build_row_iterator_.GetRow());
    } else {
      DCHECK(join_op_ == TJoinOp::RIGHT_ANTI_JOIN) << "Unsupported join operator: " <<
          join_op_;
      output_batch->CopyRow(build_row_iterator_.GetRow(), output_row);
    }
    build_row_iterator_.Next();
    ++current_build_row_idx_;
    // Evaluate conjuncts that don't affect the matching rows of the join on the
    // result row.
    if (EvalConjuncts(conjunct_evals, num_conjuncts, output_row)) {
      VLOG_ROW << "match row: " << PrintRow(output_row, *row_desc());
      output_batch->CommitLastRow();
      ++num_rows_returned_;
      if (output_batch->AtCapacity()) {
        COUNTER_ADD(rows_returned_counter_, output_batch->num_rows());
        return Status::OK();
      }
    }
  }
  eos_ = true;
  COUNTER_ADD(rows_returned_counter_, output_batch->num_rows());
  return Status::OK();
}

Status NestedLoopJoinNode::FindBuildMatches(
    RuntimeState* state, RowBatch* output_batch, bool* return_output_batch) {
  *return_output_batch = false;
  ScalarExprEvaluator* const* join_conjunct_evals = join_conjunct_evals_.data();
  size_t num_join_conjuncts = join_conjuncts_.size();
  DCHECK_EQ(num_join_conjuncts, join_conjunct_evals_.size());
  ScalarExprEvaluator* const* conjunct_evals = conjunct_evals_.data();
  size_t num_conjuncts = conjuncts_.size();
  DCHECK_EQ(num_conjuncts, conjunct_evals_.size());

  const int N = BitUtil::RoundUpToPowerOfTwo(state->batch_size());
  while (!build_row_iterator_.AtEnd()) {
    DCHECK(current_probe_row_ != NULL);
    TupleRow* output_row = output_batch->GetRow(output_batch->AddRow());
    CreateOutputRow(output_row, current_probe_row_, build_row_iterator_.GetRow());
    build_row_iterator_.Next();
    ++current_build_row_idx_;

    // This loop can go on for a long time if the conjuncts are very selective. Do
    // expensive query maintenance after every N iterations.
    if ((current_build_row_idx_ & (N - 1)) == 0) {
      if (ReachedLimit()) {
        eos_ = true;
        *return_output_batch = true;
        COUNTER_ADD(rows_returned_counter_, output_batch->num_rows());
        return Status::OK();
      }
      RETURN_IF_CANCELLED(state);
      RETURN_IF_ERROR(QueryMaintenance(state));
    }
    if (!EvalConjuncts(join_conjunct_evals, num_join_conjuncts, output_row)) {
      continue;
    }
    matched_probe_ = true;
    if (matching_build_rows_ != NULL) {
      matching_build_rows_->Set(current_build_row_idx_ - 1, true);
    }
    if (!EvalConjuncts(conjunct_evals, num_conjuncts, output_row)) continue;
    VLOG_ROW << "match row: " << PrintRow(output_row, *row_desc());
    output_batch->CommitLastRow();
    ++num_rows_returned_;
    if (output_batch->AtCapacity()) {
      *return_output_batch = true;
      COUNTER_ADD(rows_returned_counter_, output_batch->num_rows());
      return Status::OK();
    }
  }
  COUNTER_ADD(rows_returned_counter_, output_batch->num_rows());
  return Status::OK();
}

Status NestedLoopJoinNode::NextProbeRow(RuntimeState* state, RowBatch* output_batch) {
  current_probe_row_ = NULL;
  matched_probe_ = false;
  while (probe_batch_pos_ == probe_batch_->num_rows()) {
    probe_batch_->TransferResourceOwnership(output_batch);
    probe_batch_pos_ = 0;
    // If output_batch_ is at capacity after acquiring probe_batch_'s resources, we
    // need to pass it up through the execution before getting a new probe batch.
    // Otherwise, subsequent GetNext() calls to the probe input may cause the
    // memory referenced by this batch to be deleted (see IMPALA-2191).
    if (output_batch->AtCapacity()) return Status::OK();
    if (probe_side_eos_) {
      eos_ = !NeedToProcessUnmatchedBuildRows();
      return Status::OK();
    } else {
      RETURN_IF_ERROR(child(0)->GetNext(state, probe_batch_.get(), &probe_side_eos_));
    }
  }
  current_probe_row_ = probe_batch_->GetRow(probe_batch_pos_++);
  // We have a valid probe row; reset the build row iterator.
  build_row_iterator_ = build_batches_->Iterator();
  current_build_row_idx_ = 0;
  VLOG_ROW << "left row: " << GetLeftChildRowString(current_probe_row_);
  return Status::OK();
}
