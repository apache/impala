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

#include "exec/partitioned-hash-join-node.inline.h"

#include <functional>
#include <numeric>
#include <sstream>
#include <gutil/strings/substitute.h>

#include "codegen/llvm-codegen.h"
#include "exec/blocking-join-node.inline.h"
#include "exec/exec-node-util.h"
#include "exec/exec-node.inline.h"
#include "exec/hash-table.inline.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "exprs/slot-ref.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/fragment-state.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/cyclic-barrier.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

static const string PREPARE_FOR_READ_FAILED_ERROR_MSG =
    "Failed to acquire initial read buffer for stream in hash join node $0. Reducing "
    "query concurrency or increasing the memory limit may help this query to complete "
    "successfully.";

using namespace impala;
using strings::Substitute;

Status PartitionedHashJoinPlanNode::Init(
    const TPlanNode& tnode, FragmentState* state) {
  RETURN_IF_ERROR(BlockingJoinPlanNode::Init(tnode, state));
  DCHECK(tnode.__isset.join_node);
  DCHECK(tnode.join_node.__isset.hash_join_node);
  const vector<TEqJoinCondition>& eq_join_conjuncts =
      tnode.join_node.hash_join_node.eq_join_conjuncts;
  for (const TEqJoinCondition& eq_join_conjunct : eq_join_conjuncts) {
    ScalarExpr* probe_expr;
    RETURN_IF_ERROR(ScalarExpr::Create(
        eq_join_conjunct.left, probe_row_desc(), state, &probe_expr));
    probe_exprs_.push_back(probe_expr);
    ScalarExpr* build_expr;
    RETURN_IF_ERROR(ScalarExpr::Create(
        eq_join_conjunct.right, build_row_desc(), state, &build_expr));
    build_exprs_.push_back(build_expr);
    is_not_distinct_from_.push_back(eq_join_conjunct.is_not_distinct_from);
  }

  // other_join_conjuncts_ are evaluated in the context of rows assembled from all build
  // and probe tuples; full_row_desc is not necessarily the same as the output row desc,
  // e.g., because semi joins only return the build xor probe tuples
  RowDescriptor full_row_desc(probe_row_desc(), build_row_desc());
  RETURN_IF_ERROR(ScalarExpr::Create(tnode.join_node.hash_join_node.other_join_conjuncts,
      full_row_desc, state, &other_join_conjuncts_));
  DCHECK(tnode.join_node.join_op != TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN
      || eq_join_conjuncts.size() == 1);
  hash_seed_ = tnode.join_node.hash_join_node.hash_seed;

  hash_table_config_ = state->obj_pool()->Add(new HashTableConfig(build_exprs_,
      probe_exprs_, PhjBuilder::HashTableStoresNulls(join_op_, is_not_distinct_from_),
      is_not_distinct_from_));

  // TODO: IMPALA-12265: create the config only if it is necessary
  RETURN_IF_ERROR(
      PhjBuilderConfig::CreateConfig(state, tnode_->node_id, tnode_->join_node.join_op,
          &build_row_desc(), eq_join_conjuncts, tnode_->runtime_filters,
          tnode_->join_node.hash_join_node.hash_seed, &phj_builder_config_));
  state->CheckAndAddCodegenDisabledMessage(codegen_status_msgs_);
  return Status::OK();
}

void PartitionedHashJoinPlanNode::Close() {
  ScalarExpr::Close(probe_exprs_);
  ScalarExpr::Close(build_exprs_);
  ScalarExpr::Close(other_join_conjuncts_);
  if (phj_builder_config_ != nullptr) phj_builder_config_->Close();
  PlanNode::Close();
}

Status PartitionedHashJoinPlanNode::CreateExecNode(
    RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new PartitionedHashJoinNode(state, *this, state->desc_tbl()));
  return Status::OK();
}

PartitionedHashJoinNode::PartitionedHashJoinNode(RuntimeState* state,
    const PartitionedHashJoinPlanNode& pnode, const DescriptorTbl& descs)
  : BlockingJoinNode("PartitionedHashJoinNode", state->obj_pool(), pnode, descs),
    build_exprs_(pnode.build_exprs_),
    probe_exprs_(pnode.probe_exprs_),
    other_join_conjuncts_(pnode.other_join_conjuncts_),
    hash_table_config_(*pnode.hash_table_config_),
    process_probe_batch_fn_(pnode.process_probe_batch_fn_),
    process_probe_batch_fn_level0_(pnode.process_probe_batch_fn_level0_) {
  memset(hash_tbls_, 0, sizeof(HashTable*) * PARTITION_FANOUT);
}

PartitionedHashJoinNode::~PartitionedHashJoinNode() {
  // Check that we didn't leak any memory.
  DCHECK(null_probe_rows_ == nullptr);
}

Status PartitionedHashJoinNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  RETURN_IF_ERROR(BlockingJoinNode::Prepare(state));
  runtime_state_ = state;
  if (!UseSeparateBuild(state->query_options())) {
    const PhjBuilderConfig& builder_config =
      *static_cast<const PartitionedHashJoinPlanNode&>(plan_node_).phj_builder_config_;
    builder_ = builder_config.CreateSink(buffer_pool_client(),
          resource_profile_.spillable_buffer_size, resource_profile_.max_row_buffer_size,
          state);
    RETURN_IF_ERROR(builder_->Prepare(state, mem_tracker()));
    runtime_profile()->PrependChild(builder_->profile());
  }

  RETURN_IF_ERROR(ScalarExprEvaluator::Create(other_join_conjuncts_, state, pool_,
      expr_perm_pool(), expr_results_pool(), &other_join_conjunct_evals_));

  probe_expr_results_pool_.reset(new MemPool(mem_tracker()));

  // We have to carefully set up expression evaluators in the HashTableCtx to use
  // MemPools with appropriate lifetime.  The values of build exprs are only used
  // temporarily while processing each build batch or when processing a probe row
  // so can be stored in 'expr_results_pool_', which is freed during
  // QueryMaintenance(). Values of probe exprs may need to live longer until the
  // cache is reset so are stored in 'probe_expr_results_pool_', which is cleared
  // manually at the appropriate time.
  RETURN_IF_ERROR(HashTableCtx::Create(pool_, state, hash_table_config_, hash_seed(),
      MAX_PARTITION_DEPTH, build_row_desc().tuple_descriptors().size(), expr_perm_pool(),
      expr_results_pool(), probe_expr_results_pool_.get(), &ht_ctx_));
  if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    null_aware_eval_timer_ = ADD_TIMER(runtime_profile(), "NullAwareAntiJoinEvalTime");
  }

  num_probe_rows_partitioned_ =
      ADD_COUNTER(runtime_profile(), "ProbeRowsPartitioned", TUnit::UNIT);
  return Status::OK();
}

void PartitionedHashJoinPlanNode::Codegen(FragmentState* state) {
  DCHECK(state->ShouldCodegen());
  // Codegen the children nodes.
  PlanNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;

  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != nullptr);

  // Codegen the build side (if integrated into this join node).
  if (!UseSeparateBuild(state->query_options())) {
    DCHECK(phj_builder_config_ != nullptr);
    phj_builder_config_->Codegen(state);
  }

  TPrefetchMode::type prefetch_mode = state->query_options().prefetch_mode;
  AddCodegenStatus(CodegenProcessProbeBatch(codegen, prefetch_mode),  "Probe Side");
}

Status PartitionedHashJoinNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  JoinBuilder* tmp_builder = nullptr;
  RETURN_IF_ERROR(BlockingJoinNode::OpenImpl(state, &tmp_builder));
  if (builder_ == nullptr) {
    DCHECK(UseSeparateBuild(state->query_options()));
    builder_ = dynamic_cast<PhjBuilder*>(tmp_builder);
    DCHECK(builder_ != nullptr);
  }
  RETURN_IF_ERROR(ht_ctx_->Open(state));
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(other_join_conjunct_evals_, state));

  // Check for errors and free expr result allocations before opening children.
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  // The prepare functions of probe expressions may have made result allocations implicitly
  // (e.g. calling UdfBuiltins::Lower()). The probe expressions' expr result allocations need to
  // be cleared now as they don't get cleared again till probing. Other exprs' result allocations
  // are cleared in QueryMaintenance().
  probe_expr_results_pool_->Clear();

  RETURN_IF_ERROR(BlockingJoinNode::ProcessBuildInputAndOpenProbe(state, builder_));

  RETURN_IF_ERROR(
      builder_->BeginInitialProbe(buffer_pool_client(), &build_hash_partitions_));
  RETURN_IF_ERROR(PrepareForPartitionedProbe());

  RETURN_IF_ERROR(BlockingJoinNode::GetFirstProbeRow(state));
  ResetForProbe();
  probe_state_ = ProbeState::PROBING_IN_BATCH;
  DCHECK(null_aware_probe_partition_ == nullptr
      || join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN);
  return Status::OK();
}

Status PartitionedHashJoinNode::AcquireResourcesForBuild(RuntimeState* state) {
  if (!buffer_pool_client()->is_registered()) {
    // Ensure the frontend computed enough reservation for this join to execute.
    pair<int64_t, int64_t> min_reservation = builder_->MinReservation();
    if (UseSeparateBuild(state->query_options())) {
      DCHECK_GE(resource_profile_.min_reservation, min_reservation.first);
    } else {
      DCHECK_GE(resource_profile_.min_reservation,
            min_reservation.first + min_reservation.second);
    }
    RETURN_IF_ERROR(ClaimBufferReservation(state));
  }
  if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    // Initialize these partitions before doing the build so that the build does not
    // use the reservation intended for them.
    RETURN_IF_ERROR(InitNullAwareProbePartition());
    RETURN_IF_ERROR(InitNullProbeRows());
  }
  return Status::OK();
}

Status PartitionedHashJoinNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    null_probe_output_idx_ = -1;
    matched_null_probe_.clear();
  }
  ht_ctx_->set_level(0);
  CloseAndDeletePartitions(row_batch);
  builder_->Reset(IsLeftSemiJoin(join_op_) ? nullptr : row_batch);
  memset(hash_tbls_, 0, sizeof(HashTable*) * PARTITION_FANOUT);
  if (output_unmatched_batch_ != nullptr) {
    output_unmatched_batch_->TransferResourceOwnership(row_batch);
  }
  output_unmatched_batch_.reset();
  output_unmatched_batch_iter_.reset();
  flushed_unattachable_build_buffers_ = false;
  return BlockingJoinNode::Reset(state, row_batch);
}

void PartitionedHashJoinNode::CloseAndDeletePartitions(RowBatch* row_batch) {
  // Close all the partitions and clean up all references to them.
  for (unique_ptr<ProbePartition>& partition : probe_hash_partitions_) {
    if (partition != nullptr) partition->Close(row_batch);
  }
  probe_hash_partitions_.clear();
  for (auto& entry : spilled_partitions_) entry.second->Close(row_batch);
  spilled_partitions_.clear();
  if (input_partition_ != nullptr) {
    input_partition_->Close(row_batch);
    input_partition_.reset();
  }
  if (null_aware_probe_partition_ != nullptr) {
    null_aware_probe_partition_->Close(row_batch);
    null_aware_probe_partition_.reset();
  }
  for (unique_ptr<PhjBuilderPartition>& partition : output_build_partitions_) {
    partition->Close(row_batch);
  }
  output_build_partitions_.clear();
  if (null_probe_rows_ != nullptr) {
    null_probe_rows_->Close(row_batch, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
    null_probe_rows_.reset();
  }
}

void PartitionedHashJoinNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (ht_ctx_ != nullptr) {
    if (builder_ != nullptr) ht_ctx_->StatsCountersAdd(builder_->ht_stats_profile());
    ht_ctx_->Close(state);
    ht_ctx_.reset();
  }
  output_unmatched_batch_.reset();
  output_unmatched_batch_iter_.reset();
  // IMPALA-9737: free batches in case attached attached buffers need to be freed to
  // transfer reservation to 'builder_'.
  if (build_batch_ != nullptr) build_batch_->Reset();
  if (probe_batch_ != nullptr) probe_batch_->Reset();
  CloseAndDeletePartitions(nullptr);
  if (builder_ != nullptr) {
    bool separate_build = UseSeparateBuild(state->query_options());
    if (!separate_build || waited_for_build_) {
      if (separate_build
          && buffer_pool_client()->GetReservation() > resource_profile_.min_reservation) {
        // Transfer back surplus reservation, which we may have borrowed from 'builder_'.
        builder_->ReturnReservation(buffer_pool_client(),
            buffer_pool_client()->GetReservation() - resource_profile_.min_reservation);
      }
      builder_->CloseFromProbe(state);
      waited_for_build_ = false;
    }

    if (builder_->num_probe_threads() > 1) builder_->UnregisterThreadFromBarrier();
  }
  ScalarExprEvaluator::Close(other_join_conjunct_evals_, state);
  if (probe_expr_results_pool_ != nullptr) probe_expr_results_pool_->FreeAll();
  BlockingJoinNode::Close(state);
}

PartitionedHashJoinNode::ProbePartition::ProbePartition(RuntimeState* state,
    PartitionedHashJoinNode* parent, PhjBuilderPartition* build_partition)
  : build_partition_(build_partition),
    probe_rows_(make_unique<BufferedTupleStream>(state, &parent->probe_row_desc(),
        parent->buffer_pool_client(), parent->resource_profile_.spillable_buffer_size,
        parent->resource_profile_.max_row_buffer_size)) {}

PartitionedHashJoinNode::ProbePartition::~ProbePartition() {
  DCHECK(IsClosed());
}

Status PartitionedHashJoinNode::ProbePartition::PrepareForWrite(
    PartitionedHashJoinNode* parent, bool pinned) {
  RETURN_IF_ERROR(probe_rows_->Init(parent->label(), pinned));
  bool got_buffer;
  RETURN_IF_ERROR(probe_rows_->PrepareForWrite(&got_buffer));
  DCHECK(got_buffer) << "Should have already acquired reservation";
  return Status::OK();
}

Status PartitionedHashJoinNode::ProbePartition::PrepareForRead() {
  bool got_read_buffer;
  RETURN_IF_ERROR(probe_rows_->PrepareForRead(true, &got_read_buffer));
  DCHECK(got_read_buffer) << "Accounted in min reservation";
  return Status::OK();
}

void PartitionedHashJoinNode::ProbePartition::Close(RowBatch* batch) {
  if (IsClosed()) return;
  if (probe_rows_ != nullptr) {
    // Flush out the resources to free up memory for subsequent partitions.
    probe_rows_->Close(batch, RowBatch::FlushMode::FLUSH_RESOURCES);
    probe_rows_.reset();
  }
}

Status PartitionedHashJoinNode::NextProbeRowBatch(
    RuntimeState* state, RowBatch* out_batch, bool* eos) {
  DCHECK_ENUM_EQ(probe_state_, ProbeState::PROBING_END_BATCH);
  DCHECK(probe_batch_pos_ == probe_batch_->num_rows() || probe_batch_pos_ == -1);
  if (builder_->state() == HashJoinState::PARTITIONING_PROBE) {
    DCHECK(input_partition_ == nullptr);
    RETURN_IF_ERROR(NextProbeRowBatchFromChild(state, out_batch, eos));
  } else {
    DCHECK(builder_->state() == HashJoinState::REPARTITIONING_PROBE
        || builder_->state() == HashJoinState::PROBING_SPILLED_PARTITION)
        << builder_->DebugString();
    DCHECK(probe_side_eos_);
    DCHECK(input_partition_ != nullptr);
    RETURN_IF_ERROR(NextSpilledProbeRowBatch(state, out_batch, eos));
  }
  // Free expr result allocations of the probe side expressions only after
  // ExprValuesCache has been reset.
  DCHECK(ht_ctx_->expr_values_cache()->AtEnd());
  probe_expr_results_pool_->Clear();
  return Status::OK();
}

Status PartitionedHashJoinNode::NextProbeRowBatchFromChild(
    RuntimeState* state, RowBatch* out_batch, bool* eos) {
  DCHECK_ENUM_EQ(builder_->state(), HashJoinState::PARTITIONING_PROBE);
  DCHECK_ENUM_EQ(probe_state_, ProbeState::PROBING_END_BATCH);
  DCHECK(probe_batch_pos_ == probe_batch_->num_rows() || probe_batch_pos_ == -1);
  *eos = false;
  do {
    // Loop until we find a non-empty row batch.
    probe_batch_->TransferResourceOwnership(out_batch);
    if (out_batch->AtCapacity()) {
      // This out batch is full. Need to return it before getting the next batch.
      probe_batch_pos_ = -1;
      return Status::OK();
    }
    if (probe_side_eos_) {
      current_probe_row_ = nullptr;
      probe_batch_pos_ = -1;
      *eos = true;
      return Status::OK();
    }
    RETURN_IF_ERROR(child(0)->GetNext(state, probe_batch_.get(), &probe_side_eos_));
    COUNTER_ADD(probe_row_counter_, probe_batch_->num_rows());
  } while (probe_batch_->num_rows() == 0);

  ResetForProbe();
  return Status::OK();
}

Status PartitionedHashJoinNode::NextSpilledProbeRowBatch(
    RuntimeState* state, RowBatch* out_batch, bool* eos) {
  DCHECK(input_partition_ != nullptr);
  DCHECK(builder_->state() == HashJoinState::PROBING_SPILLED_PARTITION
      || builder_->state() == HashJoinState::REPARTITIONING_PROBE);
  DCHECK_ENUM_EQ(probe_state_, ProbeState::PROBING_END_BATCH);
  *eos = false;
  probe_batch_->TransferResourceOwnership(out_batch);
  if (out_batch->AtCapacity()) {
    // The out_batch has resources associated with it that will be recycled on the
    // next call to GetNext() on the probe stream. Return this batch now.
    probe_batch_pos_ = -1;
    return Status::OK();
  }
  BufferedTupleStream* probe_rows = input_partition_->probe_rows();
  if (LIKELY(probe_rows->rows_returned() < probe_rows->num_rows())) {
    // Continue from the current probe stream.
    RETURN_IF_ERROR(probe_rows->GetNext(probe_batch_.get(), eos));
    DCHECK_GT(probe_batch_->num_rows(), 0);
    ResetForProbe();
  } else {
    // Finished processing spilled probe rows from this partition.
    current_probe_row_ = nullptr;
    probe_batch_pos_ = -1;
    *eos = true;
  }
  return Status::OK();
}

Status PartitionedHashJoinNode::BeginSpilledProbe() {
  VLOG(2) << "BeginSpilledProbe\n" << NodeDebugString();
  DCHECK(input_partition_ == nullptr);
  DCHECK(build_hash_partitions_.hash_partitions == nullptr);
  DCHECK(probe_hash_partitions_.empty());
  DCHECK(!spilled_partitions_.empty());

  PhjBuilderPartition* build_input_partition;
  bool repartitioned;
  RETURN_IF_ERROR(builder_->BeginSpilledProbe(buffer_pool_client(), runtime_profile(),
      &repartitioned, &build_input_partition, &build_hash_partitions_));

  auto it = spilled_partitions_.find(build_input_partition->id());
  DCHECK(it != spilled_partitions_.end())
      << "All spilled build partitions must have a corresponding probe partition";
  input_partition_ = std::move(it->second);
  spilled_partitions_.erase(it);
  DCHECK_EQ(build_input_partition, input_partition_->build_partition());
  DCHECK_EQ(input_partition_->probe_rows()->BytesPinned(false), 0) << NodeDebugString();

  ht_ctx_->set_level(build_input_partition->level() + (repartitioned ? 1 : 0));
  if (!repartitioned && build_input_partition->hash_tbl() == nullptr) {
    // Build skipped the hash table build, which can only happen if there are no probe
    // rows.
    DCHECK_EQ(0, input_partition_->probe_rows()->num_rows())
        << build_input_partition->DebugString() << endl
        << input_partition_->probe_rows()->DebugString();
    return Status::OK();
  } else if (repartitioned) {
    RETURN_IF_ERROR(PrepareForPartitionedProbe());
  } else {
    RETURN_IF_ERROR(PrepareForUnpartitionedProbe());
  }
  COUNTER_ADD(num_probe_rows_partitioned_, input_partition_->probe_rows()->num_rows());
  return Status::OK();
}

Status PartitionedHashJoinNode::ProcessProbeBatch(RowBatch* out_batch) {
  DCHECK_ENUM_EQ(probe_state_, ProbeState::PROBING_IN_BATCH);
  DCHECK_NE(probe_batch_pos_, -1);
  // Putting SCOPED_TIMER in the IR version of ProcessProbeBatch() causes weird exception
  // handling IR in the xcompiled function, so call it here instead.
  int rows_added = 0;
  Status status;
  TPrefetchMode::type prefetch_mode = runtime_state_->query_options().prefetch_mode;
  SCOPED_TIMER(probe_timer_);

  PartitionedHashJoinPlanNode::ProcessProbeBatchFn process_probe_batch_fn;
  if (ht_ctx_->level() == 0) {
    process_probe_batch_fn = process_probe_batch_fn_level0_.load();
  } else {
    process_probe_batch_fn = process_probe_batch_fn_.load();
  }

  if (process_probe_batch_fn != nullptr) {
      rows_added = process_probe_batch_fn(
          this, prefetch_mode, out_batch, ht_ctx_.get(), &status);
  } else {
    rows_added = ProcessProbeBatch(
        join_op_, prefetch_mode, out_batch, ht_ctx_.get(), &status);
  }

  if (UNLIKELY(rows_added < 0)) {
    DCHECK(!status.ok());
    return status;
  }
  DCHECK(status.ok());
  out_batch->CommitRows(rows_added);
  return Status::OK();
}

int PartitionedHashJoinNode::ProcessProbeBatch(
    const TJoinOp::type join_op, TPrefetchMode::type prefetch_mode,
    RowBatch* out_batch, HashTableCtx* ht_ctx, Status* status) {
  switch (join_op) {
    case TJoinOp::INNER_JOIN:
      return ProcessProbeBatch<TJoinOp::INNER_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    case TJoinOp::LEFT_OUTER_JOIN:
      return ProcessProbeBatch<TJoinOp::LEFT_OUTER_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    case TJoinOp::LEFT_SEMI_JOIN:
      return ProcessProbeBatch<TJoinOp::LEFT_SEMI_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    case TJoinOp::LEFT_ANTI_JOIN:
      return ProcessProbeBatch<TJoinOp::LEFT_ANTI_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
      return ProcessProbeBatch<TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>(prefetch_mode,
          out_batch, ht_ctx, status);
    case TJoinOp::RIGHT_OUTER_JOIN:
      return ProcessProbeBatch<TJoinOp::RIGHT_OUTER_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    case TJoinOp::RIGHT_SEMI_JOIN:
      return ProcessProbeBatch<TJoinOp::RIGHT_SEMI_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    case TJoinOp::RIGHT_ANTI_JOIN:
      return ProcessProbeBatch<TJoinOp::RIGHT_ANTI_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    case TJoinOp::FULL_OUTER_JOIN:
      return ProcessProbeBatch<TJoinOp::FULL_OUTER_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    default:
      DCHECK(false) << "Unknown join type";
      return -1;
  }
}

Status PartitionedHashJoinNode::GetNext(
    RuntimeState* state, RowBatch* out_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  DCHECK(!out_batch->AtCapacity());

  Status status = Status::OK();
  *eos = false;
  // Save the number of rows in case GetNext() is called with a non-empty batch,
  // which can happen in a subplan.
  int num_rows_before = out_batch->num_rows();

  // This loop executes the 'probe_state_' state machine until either a full batch is
  // produced, resources are attached to 'out_batch' that require flushing, or eos
  // is reached (i.e. all rows are returned). The next call into GetNext() will resume
  // execution of the state machine where the current call into GetNext() left off.
  // See the definition of ProbeState for description of the state machine and states.
  do {
    DCHECK(status.ok());
    DCHECK(builder_->state() != HashJoinState::PARTITIONING_BUILD)
        << "Should not be in GetNext() " << static_cast<int>(builder_->state());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    switch (probe_state_) {
      case ProbeState::PROBING_IN_BATCH: {
        // Finish processing rows in the current probe batch.
        RETURN_IF_ERROR(ProcessProbeBatch(out_batch));
        DCHECK(out_batch->AtCapacity() || probe_batch_pos_ == probe_batch_->num_rows()
              || ht_ctx_->expr_values_cache()->AtEnd());
        if (probe_batch_pos_ == probe_batch_->num_rows()
            && current_probe_row_ == nullptr) {
          probe_state_ = ProbeState::PROBING_END_BATCH;
        }
        break;
      }
      case ProbeState::PROBING_END_BATCH: {
        // Try to get the next row batch from the current probe input.
        bool probe_eos;
        RETURN_IF_ERROR(NextProbeRowBatch(state, out_batch, &probe_eos));
        if (probe_batch_pos_ == 0) {
          // Got a batch, need to process it.
          probe_state_ = ProbeState::PROBING_IN_BATCH;
        } else if (probe_eos) {
          DCHECK_EQ(probe_batch_pos_, -1);
          if (UseSeparateBuild(state->query_options())
              && !flushed_unattachable_build_buffers_ && ReturnsBuildData(join_op_)) {
            // Can't attach build-side data because it may be referenced by multiple
            // finstances. Note that this makes the batch AtCapacity(), so we will exit
            // the loop below.
            // TODO: IMPALA-9411: implement shared ownership of buffers to avoid this.
            flushed_unattachable_build_buffers_ = true;
            out_batch->MarkNeedsDeepCopy();
          } else {
            // Finished processing all the probe rows for the current hash partitions.
            // There may be some partitions that need to outpt their unmatched build rows.
            RETURN_IF_ERROR(DoneProbing(state, out_batch));
            probe_state_ = output_build_partitions_.empty() ?
                ProbeState::PROBE_COMPLETE :
                ProbeState::OUTPUTTING_UNMATCHED;
            flushed_unattachable_build_buffers_ = false;
          }
        } else {
          // Got an empty batch with resources that we need to flush before getting the
          // next batch.
          DCHECK_EQ(probe_batch_pos_, -1);
        }
        break;
      }
      case ProbeState::OUTPUTTING_UNMATCHED: {
        DCHECK(!output_build_partitions_.empty());
        DCHECK(build_hash_partitions_.hash_partitions == nullptr);
        DCHECK(probe_hash_partitions_.empty());
        DCHECK(NeedToProcessUnmatchedBuildRows(join_op_));
        // Output the remaining batch of build rows from the current partition.
        RETURN_IF_ERROR(OutputUnmatchedBuild(out_batch));
        DCHECK(out_batch->AtCapacity() || output_build_partitions_.empty());
        if (output_build_partitions_.empty()) probe_state_ = ProbeState::PROBE_COMPLETE;
        break;
      }
      case ProbeState::PROBE_COMPLETE: {
        if (!spilled_partitions_.empty()) {
          // Move to the next spilled partition.
          RETURN_IF_ERROR(BeginSpilledProbe());
          probe_state_ = ProbeState::PROBING_END_BATCH;
        } else if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
          // Null aware anti join outputs additional rows after all the probe input,
          // including spilled partitions, is processed.
          bool has_null_aware_rows;
          RETURN_IF_ERROR(BeginNullAwareProbe(&has_null_aware_rows));
          probe_state_ = has_null_aware_rows ? ProbeState::OUTPUTTING_NULL_AWARE :
                                               ProbeState::OUTPUTTING_NULL_PROBE;
        } else {
          // No more rows to output from GetNext().
          probe_state_ = ProbeState::EOS;
        }
        break;
      }
      case ProbeState::OUTPUTTING_NULL_AWARE: {
        DCHECK_ENUM_EQ(join_op_, TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN);
        DCHECK(null_aware_probe_partition_ != nullptr);
        bool napr_eos;
        RETURN_IF_ERROR(OutputNullAwareProbeRows(state, out_batch, &napr_eos));
        if (napr_eos) probe_state_ = ProbeState::OUTPUTTING_NULL_PROBE;
        break;
      }
      case ProbeState::OUTPUTTING_NULL_PROBE: {
        DCHECK_ENUM_EQ(join_op_, TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN);
        DCHECK_GE(null_probe_output_idx_, 0);
        bool nanp_done;
        RETURN_IF_ERROR(OutputNullAwareNullProbe(state, out_batch, &nanp_done));
        if (nanp_done) probe_state_ = ProbeState::EOS;
        break;
      }
      case ProbeState::EOS: {
        // Ensure that all potential sources of output rows are exhausted.
        DCHECK(probe_side_eos_);
        DCHECK(output_build_partitions_.empty());
        DCHECK(spilled_partitions_.empty());
        DCHECK(null_aware_probe_partition_ == nullptr);
        *eos = true;
        break;
      }
      default:
        DCHECK(false) << "invalid probe_state_" << static_cast<int>(probe_state_);
        break;
    }
  } while (!out_batch->AtCapacity() && !*eos);

  int num_rows_added = out_batch->num_rows() - num_rows_before;
  DCHECK_GE(num_rows_added, 0);

  if (limit_ != -1 && rows_returned() + num_rows_added > limit_) {
    // Truncate the row batch if we went over the limit.
    num_rows_added = limit_ - rows_returned();
    DCHECK_GE(num_rows_added, 0);
    out_batch->set_num_rows(num_rows_before + num_rows_added);
    probe_batch_->TransferResourceOwnership(out_batch);
    *eos = true;
  }

  IncrementNumRowsReturned(num_rows_added);
  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

Status PartitionedHashJoinNode::OutputUnmatchedBuild(RowBatch* out_batch) {
  SCOPED_TIMER(probe_timer_);
  DCHECK(NeedToProcessUnmatchedBuildRows(join_op_));
  DCHECK(!output_build_partitions_.empty());
  DCHECK_ENUM_EQ(probe_state_, ProbeState::OUTPUTTING_UNMATCHED);

  if (output_unmatched_batch_iter_.get() != nullptr) {
    // There were no probe rows so we skipped building the hash table. In this case, all
    // build rows of the partition are unmatched.
    RETURN_IF_ERROR(OutputAllBuild(out_batch));
  } else {
    // We built and processed the hash table, so sweep over it and output unmatched rows.
    OutputUnmatchedBuildFromHashTable(out_batch);
  }

  return Status::OK();
}

Status PartitionedHashJoinNode::OutputAllBuild(RowBatch* out_batch) {
  DCHECK_ENUM_EQ(probe_state_, ProbeState::OUTPUTTING_UNMATCHED);
  // This will only be called for partitions that are added to 'output_build_partitions_'
  // in NextSpilledProbeRowBatch(), which adds one partition that is then processed until
  // it is done by the loop in GetNext(). So, there must be exactly one partition in
  // 'output_build_partitions_' here.
  DCHECK_EQ(output_build_partitions_.size(), 1);
  ScalarExprEvaluator* const* conjunct_evals = conjunct_evals_.data();
  const int num_conjuncts = conjuncts_.size();
  RowBatch::Iterator out_batch_iterator(out_batch, out_batch->num_rows());

  bool eos = false;
  while (!eos && !out_batch->AtCapacity()) {
    if (output_unmatched_batch_iter_->AtEnd()) {
      output_unmatched_batch_->TransferResourceOwnership(out_batch);
      output_unmatched_batch_->Reset();
      // Need to flush any resources attached to 'out_batch' before calling
      // build_rows()->GetNext().  E.g. if the previous call to GetNext() set the
      // 'needs_deep_copy' flag, then calling GetNext() before we return the current
      // batch leave the batch referencing invalid memory (see IMPALA-5815).
      if (out_batch->AtCapacity()) break;

      RETURN_IF_ERROR(output_build_partitions_.front()->build_rows()->GetNext(
          output_unmatched_batch_.get(), &eos));
      output_unmatched_batch_iter_.reset(
          new RowBatch::Iterator(output_unmatched_batch_.get(), 0));
    }

    for (; !output_unmatched_batch_iter_->AtEnd() && !out_batch->AtCapacity();
         output_unmatched_batch_iter_->Next()) {
      OutputBuildRow(out_batch, output_unmatched_batch_iter_->Get(), &out_batch_iterator);
      if (ExecNode::EvalConjuncts(
              conjunct_evals, num_conjuncts, out_batch_iterator.Get())) {
        out_batch->CommitLastRow();
        out_batch_iterator.Next();
      }
    }
  }

  // If we reached eos and finished the last batch, then there are no other unmatched
  // build rows for this partition. In that case we need to close the partition.
  // Otherwise, we reached out_batch capacity and we need to continue to output
  // unmatched build rows, without closing the partition.
  if (eos && output_unmatched_batch_iter_->AtEnd()) {
    output_build_partitions_.front()->Close(out_batch);
    output_build_partitions_.pop_front();
    DCHECK(output_build_partitions_.empty());
    output_unmatched_batch_iter_.reset();
    output_unmatched_batch_->TransferResourceOwnership(out_batch);
    output_unmatched_batch_->Reset();
  }
  return Status::OK();
}

void PartitionedHashJoinNode::OutputUnmatchedBuildFromHashTable(RowBatch* out_batch) {
  DCHECK_ENUM_EQ(probe_state_, ProbeState::OUTPUTTING_UNMATCHED);
  ScalarExprEvaluator* const* conjunct_evals = conjunct_evals_.data();
  const int num_conjuncts = conjuncts_.size();
  RowBatch::Iterator out_batch_iterator(out_batch, out_batch->num_rows());

  while (!out_batch->AtCapacity() && !hash_tbl_iterator_.AtEnd()) {
    // Output remaining unmatched build rows.
    if (!hash_tbl_iterator_.IsMatched()) {
      OutputBuildRow(out_batch, hash_tbl_iterator_.GetRow(), &out_batch_iterator);
      if (ExecNode::EvalConjuncts(
              conjunct_evals, num_conjuncts, out_batch_iterator.Get())) {
        out_batch->CommitLastRow();
        out_batch_iterator.Next();
      }
      hash_tbl_iterator_.SetMatched();
    }
    // Move to the next unmatched entry.
    hash_tbl_iterator_.NextUnmatched();
  }

  // If we reached the end of the hash table, then there are no other unmatched build
  // rows for this partition. In that case we need to close the partition, and move to
  // the next. If we have not reached the end of the hash table, it means that we
  // reached out_batch capacity and we need to continue to output unmatched build rows,
  // without closing the partition.
  if (hash_tbl_iterator_.AtEnd()) {
    output_build_partitions_.front()->Close(out_batch);
    output_build_partitions_.pop_front();
    // Move to the next partition to output unmatched rows.
    if (!output_build_partitions_.empty()) {
      hash_tbl_iterator_ =
          output_build_partitions_.front()->hash_tbl()->FirstUnmatched(ht_ctx_.get());
    }
  }
}

void PartitionedHashJoinNode::OutputBuildRow(
    RowBatch* out_batch, TupleRow* build_row, RowBatch::Iterator* out_batch_iterator) {
  DCHECK(build_row != nullptr);
  if (join_op_ == TJoinOp::RIGHT_ANTI_JOIN) {
    out_batch->CopyRow(build_row, out_batch_iterator->Get());
  } else {
    CreateOutputRow(out_batch_iterator->Get(), nullptr, build_row);
  }
}

Status PartitionedHashJoinNode::PrepareNullAwareNullProbe() {
  DCHECK_EQ(null_probe_output_idx_, -1);
  bool got_read_buffer;
  RETURN_IF_ERROR(null_probe_rows_->PrepareForRead(true, &got_read_buffer));
  if (!got_read_buffer) {
    return mem_tracker()->MemLimitExceeded(
        runtime_state_, Substitute(PREPARE_FOR_READ_FAILED_ERROR_MSG, id_));
  }
  DCHECK_EQ(probe_batch_->num_rows(), 0);
  probe_batch_pos_ = 0;
  null_probe_output_idx_ = 0;
  return Status::OK();
}

Status PartitionedHashJoinNode::OutputNullAwareNullProbe(
    RuntimeState* state, RowBatch* out_batch, bool* done) {
  DCHECK_ENUM_EQ(probe_state_, ProbeState::OUTPUTTING_NULL_PROBE);
  DCHECK(null_aware_probe_partition_ != nullptr);
  DCHECK_NE(probe_batch_pos_, -1);
  *done = false;

  if (probe_batch_pos_ == probe_batch_->num_rows()) {
    probe_batch_pos_ = 0;
    probe_batch_->TransferResourceOwnership(out_batch);
    if (out_batch->AtCapacity()) return Status::OK();
    bool eos;
    RETURN_IF_ERROR(null_probe_rows_->GetNext(probe_batch_.get(), &eos));
    if (probe_batch_->num_rows() == 0 && eos) {
      // All done outputting rows from null-aware partition. Clean everything up.
      null_aware_probe_partition_->Close(out_batch);
      null_aware_probe_partition_.reset();
      // Flush out the resources to free up memory.
      null_probe_rows_->Close(out_batch, RowBatch::FlushMode::FLUSH_RESOURCES);
      null_probe_rows_.reset();
      RETURN_IF_ERROR(builder_->DoneProbingNullAwarePartition());
      *done = true;
      return Status::OK();
    }
  }

  for (; probe_batch_pos_ < probe_batch_->num_rows();
      ++probe_batch_pos_, ++null_probe_output_idx_) {
    if (out_batch->AtCapacity()) break;
    if (matched_null_probe_[null_probe_output_idx_]) continue;
    TupleRow* out_row = out_batch->GetRow(out_batch->AddRow());
    out_batch->CopyRow(probe_batch_->GetRow(probe_batch_pos_), out_row);
    out_batch->CommitLastRow();
  }

  return Status::OK();
}

Status PartitionedHashJoinNode::InitNullAwareProbePartition() {
  null_aware_probe_partition_.reset(
      new ProbePartition(runtime_state_, this, builder_->null_aware_partition()));
  RETURN_IF_ERROR(null_aware_probe_partition_->PrepareForWrite(this, true));
  return Status::OK();
}

Status PartitionedHashJoinNode::InitNullProbeRows() {
  RuntimeState* state = runtime_state_;
  null_probe_rows_ =
      make_unique<BufferedTupleStream>(state, &probe_row_desc(), buffer_pool_client(),
          resource_profile_.spillable_buffer_size, resource_profile_.max_row_buffer_size);
  // Start with stream pinned, unpin later if needed.
  RETURN_IF_ERROR(null_probe_rows_->Init(label(), true));
  bool got_buffer;
  RETURN_IF_ERROR(null_probe_rows_->PrepareForWrite(&got_buffer));
  DCHECK(got_buffer) << "Accounted in min reservation"
                     << buffer_pool_client()->DebugString();
  return Status::OK();
}

Status PartitionedHashJoinNode::BeginNullAwareProbe(bool* has_null_aware_rows) {
  DCHECK_ENUM_EQ(probe_state_, ProbeState::PROBE_COMPLETE);
  DCHECK(builder_->null_aware_partition() != nullptr);
  DCHECK(null_aware_probe_partition_ != nullptr);
  DCHECK_EQ(probe_batch_pos_, -1);
  DCHECK_EQ(probe_batch_->num_rows(), 0);

  BufferedTupleStream* probe_stream = null_aware_probe_partition_->probe_rows();
  if (builder_->null_aware_partition()->build_rows()->num_rows() == 0) {
    // There were no build rows. Nothing to do. Just prepare to output the null
    // probe rows.
    DCHECK_EQ(probe_stream->num_rows(), 0);
    RETURN_IF_ERROR(PrepareNullAwareNullProbe());
    *has_null_aware_rows = false;
    return Status::OK();
  }

  RETURN_IF_ERROR(builder_->BeginNullAwareProbe());

  // Initialize the probe stream for reading.
  bool got_read_buffer;
  RETURN_IF_ERROR(probe_stream->PrepareForRead(true, &got_read_buffer));
  if (!got_read_buffer) {
    return mem_tracker()->MemLimitExceeded(
        runtime_state_, Substitute(PREPARE_FOR_READ_FAILED_ERROR_MSG, id_));
  }
  probe_batch_pos_ = 0;
  *has_null_aware_rows = true;
  return Status::OK();
}

Status PartitionedHashJoinNode::OutputNullAwareProbeRows(
    RuntimeState* state, RowBatch* out_batch, bool* done) {
  DCHECK_ENUM_EQ(probe_state_, ProbeState::OUTPUTTING_NULL_AWARE);
  DCHECK(null_aware_probe_partition_ != nullptr);
  *done = false;
  ScalarExprEvaluator* const* join_conjunct_evals = other_join_conjunct_evals_.data();
  int num_join_conjuncts = other_join_conjuncts_.size();
  DCHECK(probe_batch_ != nullptr);

  BufferedTupleStream* probe_stream = null_aware_probe_partition_->probe_rows();
  if (probe_batch_pos_ == probe_batch_->num_rows()) {
    probe_batch_pos_ = 0;
    probe_batch_->TransferResourceOwnership(out_batch);
    if (out_batch->AtCapacity()) return Status::OK();

    // Get the next probe batch.
    bool eos;
    RETURN_IF_ERROR(probe_stream->GetNext(probe_batch_.get(), &eos));

    if (probe_batch_->num_rows() == 0 && eos) {
      RETURN_IF_ERROR(
          EvaluateNullProbe(state, builder_->null_aware_partition()->build_rows()));
      RETURN_IF_ERROR(PrepareNullAwareNullProbe());
      *done = true;
      return Status::OK();
    }
  }

  RowBatch null_build_batch(&build_row_desc(), state->batch_size(), mem_tracker());
  // For each probe row, iterate over all the build rows and check for rows
  // that did not have any matches.
  for (; probe_batch_pos_ < probe_batch_->num_rows(); ++probe_batch_pos_) {
    if (out_batch->AtCapacity()) break;
    TupleRow* probe_row = probe_batch_->GetRow(probe_batch_pos_);
    bool matched = false;
    BufferedTupleStream* null_build_stream =
        builder_->null_aware_partition()->build_rows();
    DCHECK(null_build_stream->is_pinned());
    BufferedTupleStream::ReadIterator build_itr;
    RETURN_IF_ERROR(null_build_stream->PrepareForPinnedRead(&build_itr));
    bool eos;
    do {
      RETURN_IF_ERROR(null_build_stream->GetNext(&build_itr, &null_build_batch, &eos));
      FOREACH_ROW(&null_build_batch, 0, iter) {
        CreateOutputRow(semi_join_staging_row_, probe_row, iter.Get());
        if (ExecNode::EvalConjuncts(
            join_conjunct_evals, num_join_conjuncts, semi_join_staging_row_)) {
          matched = true;
          break;
        }
      }
      null_build_batch.Reset();
      RETURN_IF_CANCELLED(state);
    } while (!matched && !eos);
    if (!matched) {
      TupleRow* out_row = out_batch->GetRow(out_batch->AddRow());
      out_batch->CopyRow(probe_row, out_row);
      out_batch->CommitLastRow();
    }
  }
  return Status::OK();
}

Status PartitionedHashJoinNode::PrepareForPartitionedProbe() {
  DCHECK(builder_->state() == HashJoinState::PARTITIONING_PROBE
      || builder_->state() == HashJoinState::REPARTITIONING_PROBE)
      << builder_->DebugString();
  DCHECK_EQ(PARTITION_FANOUT, build_hash_partitions_.hash_partitions->size());
  DCHECK(probe_hash_partitions_.empty());
  // Initialize the probe partitions, providing them with probe streams. The reservation
  // for the probe streams was obtained from 'builder_' when BeginInitialProbe()
  // or BeginSpilledProbe() was called.
  vector<unique_ptr<BufferedTupleStream>> probe_streams;
  if (input_partition_ != nullptr) {
    DCHECK_ENUM_EQ(builder_->state(), HashJoinState::REPARTITIONING_PROBE);
    // This is a spilled partition - we need to read the probe rows. Memory was reserved
    // in RepartitionBuildInput() for the input stream's read buffer.
    RETURN_IF_ERROR(input_partition_->PrepareForRead());
  }

  bool have_spilled_hash_partitions;
  RETURN_IF_ERROR(CreateProbeHashPartitions(&have_spilled_hash_partitions));

  // Unpin null-aware probe streams if any partitions spilled: we don't want to waste
  // memory pinning the probe streams that might be needed to process spilled partitions.
  if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN
      && (have_spilled_hash_partitions
             || builder_->null_aware_partition()->is_spilled())) {
    RETURN_IF_ERROR(
        null_probe_rows_->UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT));
    RETURN_IF_ERROR(null_aware_probe_partition_->probe_rows()->UnpinStream(
        BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT));
  }

  // Initialize the hash_tbl_ caching array.
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    hash_tbls_[i] = (*build_hash_partitions_.hash_partitions)[i]->hash_tbl();
  }

  // Validate the state of the partitions.
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    PhjBuilderPartition* build_partition =
        (*build_hash_partitions_.hash_partitions)[i].get();
    ProbePartition* probe_partition = probe_hash_partitions_[i].get();
    if (build_partition->IsClosed()) {
      DCHECK(hash_tbls_[i] == nullptr);
      DCHECK(probe_partition == nullptr);
    } else if (build_partition->is_spilled()) {
      DCHECK(hash_tbls_[i] == nullptr);
      DCHECK(probe_partition != nullptr);
    } else {
      DCHECK(hash_tbls_[i] != nullptr);
      DCHECK(probe_partition == nullptr);
    }
  }
  return Status::OK();
}

Status PartitionedHashJoinNode::CreateProbeHashPartitions(
    bool* have_spilled_hash_partitions) {
  DCHECK_EQ(PARTITION_FANOUT, build_hash_partitions_.hash_partitions->size());
  *have_spilled_hash_partitions = false;
  probe_hash_partitions_.resize(PARTITION_FANOUT);
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    PhjBuilderPartition* build_partition =
        (*build_hash_partitions_.hash_partitions)[i].get();
    if (build_partition->IsClosed() || !build_partition->is_spilled()) continue;
    *have_spilled_hash_partitions = true;
    DCHECK(probe_hash_partitions_[i] == nullptr);
    // Put partition into vector so it will be cleaned up in CloseAndDeletePartitions()
    // if Init() fails.
    probe_hash_partitions_[i] =
        make_unique<ProbePartition>(runtime_state_, this, build_partition);
    RETURN_IF_ERROR(probe_hash_partitions_[i]->PrepareForWrite(this, false));
  }
  return Status::OK();
}

Status PartitionedHashJoinNode::PrepareForUnpartitionedProbe() {
  DCHECK_ENUM_EQ(builder_->state(), HashJoinState::PROBING_SPILLED_PARTITION);
  DCHECK(build_hash_partitions_.hash_partitions == nullptr);
  DCHECK(probe_hash_partitions_.empty());
  DCHECK(input_partition_ != nullptr);
  DCHECK(!input_partition_->build_partition()->is_spilled());
  DCHECK(input_partition_->build_partition()->hash_tbl() != nullptr);

  // This is a spilled partition - we need to read the probe rows. Memory was reserved
  // in builder_->BeginSpilledProbe() for the input stream's read buffer.
  RETURN_IF_ERROR(input_partition_->PrepareForRead());

  // In this case, we did not have to partition the build again, we just built
  // a hash table. This means the probe does not have to be partitioned either.
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    hash_tbls_[i] = input_partition_->build_partition()->hash_tbl();
  }
  return Status::OK();
}

bool PartitionedHashJoinNode::AppendProbeRowSlow(
    BufferedTupleStream* stream, TupleRow* row, Status* status) {
  if (!status->ok()) return false; // Check if AddRow() set status.
  *status = runtime_state_->StartSpilling(mem_tracker());
  if (!status->ok()) return false;
  *status = stream->UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT);
  if (!status->ok()) return false;
  return stream->AddRow(row, status);
}

Status PartitionedHashJoinNode::EvaluateNullProbe(
    RuntimeState* state, BufferedTupleStream* build) {
  DCHECK(build->is_pinned());
  if (null_probe_rows_ == nullptr || null_probe_rows_->num_rows() == 0) {
    return Status::OK();
  }
  DCHECK_EQ(null_probe_rows_->num_rows(), matched_null_probe_.size());
  bool got_read_buffer;
  RETURN_IF_ERROR(null_probe_rows_->PrepareForRead(false, &got_read_buffer));
  DCHECK(got_read_buffer) << "Probe stream should always have a read or write iterator";

  ScalarExprEvaluator* const* join_conjunct_evals = other_join_conjunct_evals_.data();
  int num_join_conjuncts = other_join_conjuncts_.size();
  RowBatch probe_batch(&probe_row_desc(), runtime_state_->batch_size(), mem_tracker());
  RowBatch build_batch(&build_row_desc(), state->batch_size(), mem_tracker());

  // For each probe row, iterate over all rows in the build table.
  SCOPED_TIMER(null_aware_eval_timer_);
  int64_t probe_row_idx = 0;
  bool probe_stream_eos = false;
  while (!probe_stream_eos) {
    RETURN_IF_ERROR(null_probe_rows_->GetNext(&probe_batch, &probe_stream_eos));
    for (int i = 0; i < probe_batch.num_rows(); ++i, ++probe_row_idx) {
      // This loop may run for a long time. Check for cancellation.
      RETURN_IF_CANCELLED(state);
      if (matched_null_probe_[probe_row_idx]) continue;
      BufferedTupleStream::ReadIterator build_itr;
      RETURN_IF_ERROR(build->PrepareForPinnedRead(&build_itr));
      bool build_eos;
      do {
        RETURN_IF_ERROR(build->GetNext(&build_itr, &build_batch, &build_eos));
        FOREACH_ROW(&build_batch, 0, iter) {
          CreateOutputRow(semi_join_staging_row_, probe_batch.GetRow(i), iter.Get());
          if (ExecNode::EvalConjuncts(
              join_conjunct_evals, num_join_conjuncts, semi_join_staging_row_)) {
            matched_null_probe_[probe_row_idx] = true;
            break;
          }
        }
        build_batch.Reset();
        RETURN_IF_CANCELLED(state);
      } while (!matched_null_probe_[probe_row_idx] && !build_eos);
    }
    probe_batch.Reset();
  }
  DCHECK_EQ(probe_row_idx, null_probe_rows_->num_rows());
  return Status::OK();
}

Status PartitionedHashJoinNode::DoneProbing(RuntimeState* state, RowBatch* batch) {
  DCHECK_ENUM_EQ(probe_state_, ProbeState::PROBING_END_BATCH);
  DCHECK_EQ(probe_batch_pos_, -1);
  DCHECK(output_build_partitions_.empty());
  // At this point all the rows have been read from the probe side for all partitions in
  // hash_partitions_.
  VLOG(2) << "Probe Side Consumed\n" << NodeDebugString();
  // Clean up input partition first to free up probe reservation before calling
  // DoneProbing*().
  if (input_partition_ != nullptr) {
    input_partition_->Close(batch);
    input_partition_.reset();
  }
  if (builder_->state() == HashJoinState::PROBING_SPILLED_PARTITION) {
    // Need to clean up single in-memory build partition instead of hash partitions.
    DCHECK(build_hash_partitions_.hash_partitions == nullptr);
    RETURN_IF_ERROR(
        builder_->DoneProbingSinglePartition(buffer_pool_client(), runtime_profile(),
            &output_build_partitions_, IsLeftSemiJoin(join_op_) ? nullptr : batch));
  } else {
    // Walk the partitions that had hash tables built for the probe phase and either
    // close them or move them to 'spilled_partitions_'.
    DCHECK_EQ(build_hash_partitions_.hash_partitions->size(), PARTITION_FANOUT);
    DCHECK_EQ(probe_hash_partitions_.size(), PARTITION_FANOUT);
    int64_t num_spilled_probe_rows[PARTITION_FANOUT] = {0};
    for (int i = 0; i < PARTITION_FANOUT; ++i) {
      ProbePartition* probe_partition = probe_hash_partitions_[i].get();
      PhjBuilderPartition* build_partition =
          (*build_hash_partitions_.hash_partitions)[i].get();
      if (probe_partition == nullptr) {
        // Partition was not spilled.
        if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
          // For NAAJ, we need to try to match the NULL probe rows with this build
          // partition before we are done with it.
          if (!build_partition->IsClosed()) {
            RETURN_IF_ERROR(EvaluateNullProbe(state, build_partition->build_rows()));
          }
        }
      } else if (probe_partition->probe_rows()->num_rows() != 0
          || NeedToProcessUnmatchedBuildRows(join_op_)
          || builder_->num_probe_threads() > 1) {
        num_spilled_probe_rows[i] = probe_partition->probe_rows()->num_rows();
        // Unpin the probe stream to free up more memory. We need to free all memory so we
        // can recurse the algorithm and create new hash partitions from spilled
        // partitions.
        RETURN_IF_ERROR(
            probe_partition->probe_rows()->UnpinStream(BufferedTupleStream::UNPIN_ALL));
        spilled_partitions_.emplace(
            build_partition->id(), std::move(probe_hash_partitions_[i]));
      } else {
        // There's no more processing to do for this partition, and since there were no
        // probe rows we didn't return any rows that reference memory from these
        // partitions, so just free the resources.
        // Avoid doing this for shared builds so that all probe threads have the same
        // number of partitions, which simplifies logic.
        probe_partition->Close(nullptr);
      }
    }
    probe_hash_partitions_.clear();
    build_hash_partitions_.Reset();
    RETURN_IF_ERROR(builder_->DoneProbingHashPartitions(num_spilled_probe_rows,
        buffer_pool_client(), runtime_profile(), &output_build_partitions_,
        IsLeftSemiJoin(join_op_) ? nullptr : batch));
  }
  if (!output_build_partitions_.empty()) {
    DCHECK(output_unmatched_batch_iter_.get() == nullptr);
    PhjBuilderPartition* output_partition = output_build_partitions_.front().get();
    if (output_partition->hash_tbl() != nullptr) {
      hash_tbl_iterator_ = output_partition->hash_tbl()->FirstUnmatched(ht_ctx_.get());
    } else {
      output_unmatched_batch_.reset(new RowBatch(
          &build_row_desc(), runtime_state_->batch_size(), builder_->mem_tracker()));
      output_unmatched_batch_iter_.reset(
          new RowBatch::Iterator(output_unmatched_batch_.get(), 0));
    }
  }
  return Status::OK();
}

void PartitionedHashJoinNode::AddToDebugString(int indent, stringstream* out) const {
  *out << " hash_tbl=";
  *out << string(indent * 2, ' ');
  *out << "HashTbl("
       << " build_exprs=" << ScalarExpr::DebugString(build_exprs_)
       << " probe_exprs=" << ScalarExpr::DebugString(probe_exprs_);
  *out << ")";
}

string PartitionedHashJoinNode::NodeDebugString() const {
  stringstream ss;
  ss << "PartitionedHashJoinNode (id=" << id() << " op=" << join_op_
     << " #spilled_partitions=" << spilled_partitions_.size() << ")" << endl;

  if (builder_ != nullptr) {
    ss << "PhjBuilder: " << builder_->DebugString();
  }

  ss << "Probe hash partitions: " << probe_hash_partitions_.size() << ":" << endl;
  for (int i = 0; i < probe_hash_partitions_.size(); ++i) {
    ProbePartition* probe_partition = probe_hash_partitions_[i].get();
    ss << "  Probe hash partition " << i << ": ";
    if (probe_partition != nullptr) {
      ss << "probe ptr=" << probe_partition;
      BufferedTupleStream* probe_rows = probe_partition->probe_rows();
      if (probe_rows != nullptr) {
        ss << "    Probe Rows: " << probe_rows->num_rows()
           << "    (Bytes total/pinned: " << probe_rows->byte_size() << "/"
           << probe_rows->BytesPinned(false) << ")";
      }
    }
    ss << endl;
  }

  if (!spilled_partitions_.empty()) {
    ss << "SpilledPartitions" << endl;
    for (const auto& entry : spilled_partitions_) {
      ProbePartition* probe_partition = entry.second.get();
      PhjBuilderPartition* build_partition = probe_partition->build_partition();
      DCHECK(build_partition->is_spilled());
      DCHECK(build_partition->hash_tbl() == nullptr);
      int build_rows = build_partition->build_rows() == nullptr ?  -1 :
          build_partition->build_rows()->num_rows();
      int probe_rows = probe_partition->probe_rows() == nullptr ?  -1 :
          probe_partition->probe_rows()->num_rows();
      ss << "  ProbePartition (id=" << entry.first << "):" << probe_partition << endl
         << "   Spilled Build Rows: " << build_rows << endl
         << "   Spilled Probe Rows: " << probe_rows << endl;
    }
  }
  if (input_partition_ != nullptr) {
    DCHECK(input_partition_->probe_rows() != nullptr);
    ss << "InputPartition: " << input_partition_.get() << endl;
    PhjBuilderPartition* build_partition = input_partition_->build_partition();
    if (build_partition->IsClosed()) {
      ss << "   Build Partition Closed" << endl;
    } else {
      ss << "   Spilled Build Rows: " << build_partition->build_rows()->num_rows() << endl;
    }
    ss << "   Spilled Probe Rows: " << input_partition_->probe_rows()->num_rows() << endl;
  } else {
    ss << "InputPartition: NULL" << endl;
  }

  if (null_aware_probe_partition_ != nullptr) {
    ss << "null-aware probe partition ptr=" << null_aware_probe_partition_.get();
    BufferedTupleStream* probe_rows = null_aware_probe_partition_->probe_rows();
    if (probe_rows != nullptr) {
      ss << "    Probe Rows: " << probe_rows->num_rows()
         << "    (Bytes total/pinned: " << probe_rows->byte_size() << "/"
         << probe_rows->BytesPinned(false) << ")" << endl;
    }
  }
  if (null_probe_rows_ != nullptr) {
    ss << "null probe rows ptr=" << null_probe_rows_.get();
    ss << "    Probe Rows: " << null_probe_rows_->num_rows()
       << "    (Bytes total/pinned: " << null_probe_rows_->byte_size() << "/"
       << null_probe_rows_->BytesPinned(false) << ")" << endl;
  }
  return ss.str();
}

// For a left outer join, the IR looks like:
// define void @CreateOutputRow(%"class.impala::BlockingJoinNode"* %this_ptr,
//                              %"class.impala::TupleRow"* %out_arg,
//                              %"class.impala::TupleRow"* %probe_arg,
//                              %"class.impala::TupleRow"* %build_arg) #20 {
// entry:
//   %out = bitcast %"class.impala::TupleRow"* %out_arg to i8**
//   %probe = bitcast %"class.impala::TupleRow"* %probe_arg to i8**
//   %build = bitcast %"class.impala::TupleRow"* %build_arg to i8**
//   %0 = bitcast i8** %out to i8*
//   %1 = bitcast i8** %probe to i8*
//   call void @llvm.memcpy.p0i8.p0i8.i32(i8* %0, i8* %1, i32 8, i32 0, i1 false)
//   %build_dst_ptr = getelementptr i8** %out, i32 1
//   %is_build_null = icmp eq i8** %build, null
//   br i1 %is_build_null, label %build_null, label %build_not_null
//
// build_not_null:                                   ; preds = %entry
//   %2 = bitcast i8** %build_dst_ptr to i8*
//   %3 = bitcast i8** %build to i8*
//   call void @llvm.memcpy.p0i8.p0i8.i32(i8* %2, i8* %3, i32 8, i32 0, i1 false)
//   ret void
//
// build_null:                                       ; preds = %entry
//   %dst_tuple_ptr = getelementptr i8** %out, i32 1
//   store i8* null, i8** %dst_tuple_ptr
//   ret void
// }
Status PartitionedHashJoinPlanNode::CodegenCreateOutputRow(
    LlvmCodeGen* codegen, llvm::Function** fn) {
  llvm::PointerType* tuple_row_ptr_type = codegen->GetStructPtrType<TupleRow>();

  llvm::PointerType* this_ptr_type = codegen->GetStructPtrType<BlockingJoinNode>();

  // TupleRows are really just an array of pointers.  Easier to work with them
  // this way.
  llvm::PointerType* tuple_row_working_type = codegen->ptr_ptr_type();

  // Construct function signature to match CreateOutputRow()
  LlvmCodeGen::FnPrototype prototype(codegen, "CreateOutputRow", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("out_arg", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("probe_arg", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("build_arg", tuple_row_ptr_type));

  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);
  llvm::Value* args[4];
  *fn = prototype.GeneratePrototype(&builder, args);
  llvm::Value* out_row_arg =
      builder.CreateBitCast(args[1], tuple_row_working_type, "out");
  llvm::Value* probe_row_arg =
      builder.CreateBitCast(args[2], tuple_row_working_type, "probe");
  llvm::Value* build_row_arg =
      builder.CreateBitCast(args[3], tuple_row_working_type, "build");

  int num_probe_tuples = probe_row_desc().tuple_descriptors().size();
  int num_build_tuples = build_row_desc().tuple_descriptors().size();
  int probe_tuple_row_size = num_probe_tuples * sizeof(Tuple*);
  int build_tuple_row_size = num_build_tuples * sizeof(Tuple*);

  // Copy probe row
  codegen->CodegenMemcpy(&builder, out_row_arg, probe_row_arg, probe_tuple_row_size);
  llvm::Value* build_row_idx[] = {codegen->GetI32Constant(num_probe_tuples)};
  llvm::Value* build_row_dst =
      builder.CreateInBoundsGEP(out_row_arg, build_row_idx, "build_dst_ptr");

  // Copy build row.
  llvm::BasicBlock* build_not_null_block =
      llvm::BasicBlock::Create(context, "build_not_null", *fn);
  llvm::BasicBlock* build_null_block = nullptr;

  if (join_op_ == TJoinOp::LEFT_ANTI_JOIN || join_op_ == TJoinOp::LEFT_OUTER_JOIN ||
      join_op_ == TJoinOp::FULL_OUTER_JOIN ||
      join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    // build tuple can be null
    build_null_block = llvm::BasicBlock::Create(context, "build_null", *fn);
    llvm::Value* is_build_null = builder.CreateIsNull(build_row_arg, "is_build_null");
    builder.CreateCondBr(is_build_null, build_null_block, build_not_null_block);

    // Set tuple build ptrs to NULL
    // TODO: this should be replaced with memset() but I can't get the llvm intrinsic
    // to work.
    builder.SetInsertPoint(build_null_block);
    for (int i = 0; i < num_build_tuples; ++i) {
      llvm::Value* array_idx[] = {
          codegen->GetI32Constant(i + num_probe_tuples)};
      llvm::Value* dst =
          builder.CreateInBoundsGEP(out_row_arg, array_idx, "dst_tuple_ptr");
      builder.CreateStore(codegen->null_ptr_value(), dst);
    }
    builder.CreateRetVoid();
  } else {
    // build row can't be NULL
    builder.CreateBr(build_not_null_block);
  }

  // Copy build tuple ptrs
  builder.SetInsertPoint(build_not_null_block);
  codegen->CodegenMemcpy(&builder, build_row_dst, build_row_arg, build_tuple_row_size);
  builder.CreateRetVoid();

  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == nullptr) {
    return Status("PartitionedHashJoinNode::CodegenCreateOutputRow(): codegen'd "
        "CreateOutputRow() function failed verification, see log");
  }
  return Status::OK();
}

Status PartitionedHashJoinPlanNode::CodegenProcessProbeBatch(
    LlvmCodeGen* codegen, TPrefetchMode::type prefetch_mode) {
  // Codegen for hashing rows
  llvm::Function* hash_fn;
  llvm::Function* murmur_hash_fn;

  RETURN_IF_ERROR(
      HashTableCtx::CodegenHashRow(codegen, false, *hash_table_config_, &hash_fn));
  RETURN_IF_ERROR(
      HashTableCtx::CodegenHashRow(codegen, true, *hash_table_config_, &murmur_hash_fn));

  // Get cross compiled function
  IRFunction::Type ir_fn = IRFunction::FN_END;
  switch (join_op_) {
    case TJoinOp::INNER_JOIN:
      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_INNER_JOIN;
      break;
    case TJoinOp::LEFT_OUTER_JOIN:
      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_LEFT_OUTER_JOIN;
      break;
    case TJoinOp::LEFT_SEMI_JOIN:
      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_LEFT_SEMI_JOIN;
      break;
    case TJoinOp::LEFT_ANTI_JOIN:
      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_LEFT_ANTI_JOIN;
      break;
    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_NULL_AWARE_LEFT_ANTI_JOIN;
      break;
    case TJoinOp::RIGHT_OUTER_JOIN:
      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_RIGHT_OUTER_JOIN;
      break;
    case TJoinOp::RIGHT_SEMI_JOIN:
      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_RIGHT_SEMI_JOIN;
      break;
    case TJoinOp::RIGHT_ANTI_JOIN:
      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_RIGHT_ANTI_JOIN;
      break;
    case TJoinOp::FULL_OUTER_JOIN:
      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_FULL_OUTER_JOIN;
      break;
    default:
      DCHECK(false);
  }
  llvm::Function* process_probe_batch_fn = codegen->GetFunction(ir_fn, true);
  DCHECK(process_probe_batch_fn != nullptr);
  process_probe_batch_fn->setName("ProcessProbeBatch");

  // Verifies that ProcessProbeBatch() has weak_odr linkage so it's not discarded even
  // if it's not referenced. See http://llvm.org/docs/LangRef.html#linkage-types
  DCHECK(process_probe_batch_fn->getLinkage() == llvm::GlobalValue::WeakODRLinkage)
      << LlvmCodeGen::Print(process_probe_batch_fn);

  // Replace the parameter 'prefetch_mode' with constant.
  llvm::Value* prefetch_mode_arg = codegen->GetArgument(process_probe_batch_fn, 1);
  DCHECK_GE(prefetch_mode, TPrefetchMode::NONE);
  DCHECK_LE(prefetch_mode, TPrefetchMode::HT_BUCKET);
  prefetch_mode_arg->replaceAllUsesWith(codegen->GetI32Constant(prefetch_mode));

  // Codegen HashTable::Equals
  llvm::Function* probe_equals_fn;
  RETURN_IF_ERROR(
      HashTableCtx::CodegenEquals(codegen, false, *hash_table_config_, &probe_equals_fn));

  // Codegen for evaluating probe rows
  llvm::Function* eval_row_fn;
  RETURN_IF_ERROR(
      HashTableCtx::CodegenEvalRow(codegen, false, *hash_table_config_, &eval_row_fn));

  // Codegen CreateOutputRow
  llvm::Function* create_output_row_fn;
  RETURN_IF_ERROR(CodegenCreateOutputRow(codegen, &create_output_row_fn));

  // Codegen evaluating other join conjuncts
  llvm::Function* eval_other_conjuncts_fn;
  RETURN_IF_ERROR(ExecNode::CodegenEvalConjuncts(codegen, other_join_conjuncts_,
      &eval_other_conjuncts_fn, "EvalOtherConjuncts"));

  // Codegen evaluating conjuncts
  llvm::Function* eval_conjuncts_fn;
  RETURN_IF_ERROR(ExecNode::CodegenEvalConjuncts(codegen, conjuncts_,
      &eval_conjuncts_fn));

  // Replace all call sites with codegen version
  int replaced = codegen->ReplaceCallSites(process_probe_batch_fn, eval_row_fn,
      "EvalProbeRow");
  DCHECK_REPLACE_COUNT(replaced, 1);

  replaced = codegen->ReplaceCallSites(process_probe_batch_fn, create_output_row_fn,
      "CreateOutputRow");
  // Depends on join_op_
  // TODO: switch statement
  DCHECK(replaced == 1 || replaced == 2) << replaced;

  replaced = codegen->ReplaceCallSites(process_probe_batch_fn, eval_conjuncts_fn,
      "EvalConjuncts");
  switch (join_op_) {
    case TJoinOp::INNER_JOIN:
    case TJoinOp::LEFT_SEMI_JOIN:
    case TJoinOp::RIGHT_OUTER_JOIN:
    case TJoinOp::RIGHT_SEMI_JOIN:
      DCHECK_REPLACE_COUNT(replaced, 1);
      break;
    case TJoinOp::LEFT_OUTER_JOIN:
    case TJoinOp::FULL_OUTER_JOIN:
      DCHECK_REPLACE_COUNT(replaced, 2);
      break;
    case TJoinOp::LEFT_ANTI_JOIN:
    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
    case TJoinOp::RIGHT_ANTI_JOIN:
      DCHECK_REPLACE_COUNT(replaced, 0);
      break;
    default:
      DCHECK(false);
  }

  replaced = codegen->ReplaceCallSites(process_probe_batch_fn, eval_other_conjuncts_fn,
      "EvalOtherJoinConjuncts");
  DCHECK_REPLACE_COUNT(replaced, 1);

  replaced = codegen->ReplaceCallSites(process_probe_batch_fn, probe_equals_fn, "Equals");
  // Depends on join_op_
  // TODO: switch statement
  DCHECK(replaced == 1 || replaced == 2 || replaced == 3 || replaced == 4) << replaced;

  // Replace hash-table parameters with constants.
  HashTableCtx::HashTableReplacedConstants replaced_constants;
  const bool stores_duplicates = true;
  const int num_build_tuples = build_row_desc().tuple_descriptors().size();
  RETURN_IF_ERROR(HashTableCtx::ReplaceHashTableConstants(codegen, *hash_table_config_,
      stores_duplicates, num_build_tuples, process_probe_batch_fn, &replaced_constants));
  DCHECK_GE(replaced_constants.stores_nulls, 1);
  DCHECK_GE(replaced_constants.finds_some_nulls, 1);
  DCHECK_GE(replaced_constants.stores_duplicates, 1);
  DCHECK_GE(replaced_constants.stores_tuples, 1);
  DCHECK_GE(replaced_constants.quadratic_probing, 1);

  llvm::Function* process_probe_batch_fn_level0 =
      codegen->CloneFunction(process_probe_batch_fn);

  // process_probe_batch_fn_level0 uses CRC hash if available,
  // process_probe_batch_fn uses murmur
  replaced = codegen->ReplaceCallSites(process_probe_batch_fn_level0, hash_fn, "HashRow");
  DCHECK_REPLACE_COUNT(replaced, 1);

  replaced = codegen->ReplaceCallSites(process_probe_batch_fn, murmur_hash_fn, "HashRow");
  DCHECK_REPLACE_COUNT(replaced, 1);

  // Finalize ProcessProbeBatch functions
  process_probe_batch_fn = codegen->FinalizeFunction(process_probe_batch_fn);
  if (process_probe_batch_fn == nullptr) {
    return Status("PartitionedHashJoinNode::CodegenProcessProbeBatch(): codegen'd "
        "ProcessProbeBatch() function failed verification, see log");
  }
  process_probe_batch_fn_level0 =
      codegen->FinalizeFunction(process_probe_batch_fn_level0);
  if (process_probe_batch_fn_level0 == nullptr) {
    return Status("PartitionedHashJoinNode::CodegenProcessProbeBatch(): codegen'd "
        "level-zero ProcessProbeBatch() function failed verification, see log");
  }

  // Register native function pointers
  codegen->AddFunctionToJit(process_probe_batch_fn, &process_probe_batch_fn_);
  codegen->AddFunctionToJit(process_probe_batch_fn_level0,
                            &process_probe_batch_fn_level0_);
  return Status::OK();
}
