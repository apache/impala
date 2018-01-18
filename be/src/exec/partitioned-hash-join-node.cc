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
#include "exec/hash-table.inline.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "exprs/slot-ref.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

static const string PREPARE_FOR_READ_FAILED_ERROR_MSG =
    "Failed to acquire initial read buffer for stream in hash join node $0. Reducing "
    "query concurrency or increasing the memory limit may help this query to complete "
    "successfully.";

using namespace impala;
using strings::Substitute;

PartitionedHashJoinNode::PartitionedHashJoinNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : BlockingJoinNode(
        "PartitionedHashJoinNode", tnode.hash_join_node.join_op, pool, tnode, descs),
    num_probe_rows_partitioned_(NULL),
    null_aware_eval_timer_(NULL),
    state_(PARTITIONING_BUILD),
    output_null_aware_probe_rows_running_(false),
    null_probe_output_idx_(-1),
    process_probe_batch_fn_(NULL),
    process_probe_batch_fn_level0_(NULL) {
  memset(hash_tbls_, 0, sizeof(HashTable*) * PARTITION_FANOUT);
}

PartitionedHashJoinNode::~PartitionedHashJoinNode() {
  // Check that we didn't leak any memory.
  DCHECK(null_probe_rows_ == NULL);
}

Status PartitionedHashJoinNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  RETURN_IF_ERROR(BlockingJoinNode::Init(tnode, state));
  DCHECK(tnode.__isset.hash_join_node);
  const vector<TEqJoinCondition>& eq_join_conjuncts =
      tnode.hash_join_node.eq_join_conjuncts;
  // TODO: allow PhjBuilder to be the sink of a separate fragment. For now, PhjBuilder is
  // owned by this node, but duplicates some state (exprs, etc) in anticipation of it
  // being separated out further.
  builder_.reset(new PhjBuilder(id(), join_op_, child(0)->row_desc(),
        child(1)->row_desc(), state, &buffer_pool_client_,
        resource_profile_.spillable_buffer_size, resource_profile_.max_row_buffer_size));
  RETURN_IF_ERROR(
      builder_->InitExprsAndFilters(state, eq_join_conjuncts, tnode.runtime_filters));

  for (const TEqJoinCondition& eq_join_conjunct : eq_join_conjuncts) {
    ScalarExpr* probe_expr;
    RETURN_IF_ERROR(ScalarExpr::Create(
        eq_join_conjunct.left, *child(0)->row_desc(), state, &probe_expr));
    probe_exprs_.push_back(probe_expr);
    ScalarExpr* build_expr;
    RETURN_IF_ERROR(ScalarExpr::Create(
        eq_join_conjunct.right, *child(1)->row_desc(), state, &build_expr));
    build_exprs_.push_back(build_expr);
  }
  // other_join_conjuncts_ are evaluated in the context of rows assembled from all build
  // and probe tuples; full_row_desc is not necessarily the same as the output row desc,
  // e.g., because semi joins only return the build xor probe tuples
  RowDescriptor full_row_desc(*child(0)->row_desc(), *child(1)->row_desc());
  RETURN_IF_ERROR(ScalarExpr::Create(tnode.hash_join_node.other_join_conjuncts,
      full_row_desc, state, &other_join_conjuncts_));
  DCHECK(join_op_ != TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN || eq_join_conjuncts.size() == 1);
  return Status::OK();
}

Status PartitionedHashJoinNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  RETURN_IF_ERROR(BlockingJoinNode::Prepare(state));
  runtime_state_ = state;

  RETURN_IF_ERROR(builder_->Prepare(state, mem_tracker()));
  runtime_profile()->PrependChild(builder_->profile());

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
  RETURN_IF_ERROR(HashTableCtx::Create(pool_, state, build_exprs_, probe_exprs_,
      builder_->HashTableStoresNulls(), builder_->is_not_distinct_from(),
      state->fragment_hash_seed(), MAX_PARTITION_DEPTH,
      child(1)->row_desc()->tuple_descriptors().size(), expr_perm_pool(),
      expr_results_pool(), probe_expr_results_pool_.get(), &ht_ctx_));
  if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    null_aware_eval_timer_ = ADD_TIMER(runtime_profile(), "NullAwareAntiJoinEvalTime");
  }

  num_probe_rows_partitioned_ =
      ADD_COUNTER(runtime_profile(), "ProbeRowsPartitioned", TUnit::UNIT);
  num_hash_table_builds_skipped_ =
      ADD_COUNTER(runtime_profile(), "NumHashTableBuildsSkipped", TUnit::UNIT);
  AddCodegenDisabledMessage(state);
  return Status::OK();
}

void PartitionedHashJoinNode::Codegen(RuntimeState* state) {
  DCHECK(state->ShouldCodegen());
  // Codegen the children node;
  ExecNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;

  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != NULL);

  // Codegen the build side.
  builder_->Codegen(codegen);

  // Codegen the probe side.
  TPrefetchMode::type prefetch_mode = state->query_options().prefetch_mode;
  Status probe_codegen_status = CodegenProcessProbeBatch(codegen, prefetch_mode);
  runtime_profile()->AddCodegenMsg(
      probe_codegen_status.ok(), probe_codegen_status, "Probe Side");
}

Status PartitionedHashJoinNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(BlockingJoinNode::Open(state));
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

  RETURN_IF_ERROR(BlockingJoinNode::ProcessBuildInputAndOpenProbe(state, builder_.get()));
  RETURN_IF_ERROR(PrepareForProbe());

  UpdateState(PARTITIONING_PROBE);
  RETURN_IF_ERROR(BlockingJoinNode::GetFirstProbeRow(state));
  ResetForProbe();
  DCHECK(null_aware_probe_partition_ == NULL
      || join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN);
  return Status::OK();
}

Status PartitionedHashJoinNode::AcquireResourcesForBuild(RuntimeState* state) {
  DCHECK_GE(resource_profile_.min_reservation, builder_->MinReservation());
  if (!buffer_pool_client_.is_registered()) {
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

Status PartitionedHashJoinNode::Reset(RuntimeState* state) {
  if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    null_probe_output_idx_ = -1;
    matched_null_probe_.clear();
    output_null_aware_probe_rows_running_ = false;
  }
  state_ = PARTITIONING_BUILD;
  ht_ctx_->set_level(0);
  CloseAndDeletePartitions();
  builder_->Reset();
  memset(hash_tbls_, 0, sizeof(HashTable*) * PARTITION_FANOUT);
  output_unmatched_batch_.reset();
  output_unmatched_batch_iter_.reset();
  return ExecNode::Reset(state);
}

void PartitionedHashJoinNode::CloseAndDeletePartitions() {
  // Close all the partitions and clean up all references to them.
  for (unique_ptr<ProbePartition>& partition : probe_hash_partitions_) {
    if (partition != NULL) partition->Close(NULL);
  }
  probe_hash_partitions_.clear();
  for (unique_ptr<ProbePartition>& partition : spilled_partitions_) {
    partition->Close(NULL);
  }
  spilled_partitions_.clear();
  if (input_partition_ != NULL) {
    input_partition_->Close(NULL);
    input_partition_.reset();
  }
  if (null_aware_probe_partition_ != NULL) {
    null_aware_probe_partition_->Close(NULL);
    null_aware_probe_partition_.reset();
  }
  output_build_partitions_.clear();
  if (null_probe_rows_ != NULL) {
    null_probe_rows_->Close(NULL, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
    null_probe_rows_.reset();
  }
}

void PartitionedHashJoinNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (ht_ctx_ != nullptr) ht_ctx_->Close(state);
  ht_ctx_.reset();
  output_null_aware_probe_rows_running_ = false;
  output_unmatched_batch_.reset();
  output_unmatched_batch_iter_.reset();
  CloseAndDeletePartitions();
  if (builder_ != nullptr) builder_->Close(state);
  ScalarExprEvaluator::Close(other_join_conjunct_evals_, state);
  ScalarExpr::Close(build_exprs_);
  ScalarExpr::Close(probe_exprs_);
  ScalarExpr::Close(other_join_conjuncts_);
  if (probe_expr_results_pool_ != nullptr) probe_expr_results_pool_->FreeAll();
  BlockingJoinNode::Close(state);
}

PartitionedHashJoinNode::ProbePartition::ProbePartition(RuntimeState* state,
    PartitionedHashJoinNode* parent, PhjBuilder::Partition* build_partition,
    unique_ptr<BufferedTupleStream> probe_rows)
  : build_partition_(build_partition),
    probe_rows_(std::move(probe_rows)) {
  DCHECK(probe_rows_->has_write_iterator());
}

PartitionedHashJoinNode::ProbePartition::~ProbePartition() {
  DCHECK(IsClosed());
}

Status PartitionedHashJoinNode::ProbePartition::PrepareForRead() {
  bool got_read_buffer;
  RETURN_IF_ERROR(probe_rows_->PrepareForRead(true, &got_read_buffer));
  DCHECK(got_read_buffer) << "Accounted in min reservation";
  return Status::OK();
}

void PartitionedHashJoinNode::ProbePartition::Close(RowBatch* batch) {
  if (IsClosed()) return;
  if (probe_rows_ != NULL) {
    // Flush out the resources to free up memory for subsequent partitions.
    probe_rows_->Close(batch, RowBatch::FlushMode::FLUSH_RESOURCES);
    probe_rows_.reset();
  }
}

Status PartitionedHashJoinNode::NextProbeRowBatch(
    RuntimeState* state, RowBatch* out_batch) {
  DCHECK_EQ(state_, PARTITIONING_PROBE);
  DCHECK(probe_batch_pos_ == probe_batch_->num_rows() || probe_batch_pos_ == -1);
  do {
    // Loop until we find a non-empty row batch.
    probe_batch_->TransferResourceOwnership(out_batch);
    if (out_batch->AtCapacity()) {
      // This out batch is full. Need to return it before getting the next batch.
      probe_batch_pos_ = -1;
      return Status::OK();
    }
    if (probe_side_eos_) {
      current_probe_row_ = NULL;
      probe_batch_pos_ = -1;
      return Status::OK();
    }
    RETURN_IF_ERROR(child(0)->GetNext(state, probe_batch_.get(), &probe_side_eos_));
    COUNTER_ADD(probe_row_counter_, probe_batch_->num_rows());
  } while (probe_batch_->num_rows() == 0);

  ResetForProbe();
  return Status::OK();
}

Status PartitionedHashJoinNode::NextSpilledProbeRowBatch(
    RuntimeState* state, RowBatch* out_batch) {
  DCHECK(input_partition_ != NULL);
  DCHECK(state_ == PROBING_SPILLED_PARTITION || state_ == REPARTITIONING_PROBE);
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
    bool eos = false;
    RETURN_IF_ERROR(probe_rows->GetNext(probe_batch_.get(), &eos));
    DCHECK_GT(probe_batch_->num_rows(), 0);
    ResetForProbe();
  } else {
    // Finished processing spilled probe rows from this partition.
    if (state_ == PROBING_SPILLED_PARTITION && NeedToProcessUnmatchedBuildRows()) {
      // If the build partition was in memory, we are done probing this partition.
      // In case of right-outer, right-anti and full-outer joins, we move this partition
      // to the list of partitions that we need to output their unmatched build rows.
      DCHECK(output_build_partitions_.empty());
      DCHECK(output_unmatched_batch_iter_.get() == NULL);
      if (input_partition_->build_partition()->hash_tbl() != NULL) {
        hash_tbl_iterator_ =
            input_partition_->build_partition()->hash_tbl()->FirstUnmatched(
                ht_ctx_.get());
      } else {
        output_unmatched_batch_.reset(new RowBatch(
            child(1)->row_desc(), runtime_state_->batch_size(), builder_->mem_tracker()));
        output_unmatched_batch_iter_.reset(
            new RowBatch::Iterator(output_unmatched_batch_.get(), 0));
      }
      output_build_partitions_.push_back(input_partition_->build_partition());
    } else {
      // In any other case, just close the input build partition.
      input_partition_->build_partition()->Close(out_batch);
    }
    input_partition_->Close(out_batch);
    input_partition_.reset();
    current_probe_row_ = NULL;
    probe_batch_pos_ = -1;
  }
  return Status::OK();
}

Status PartitionedHashJoinNode::PrepareSpilledPartitionForProbe(
    RuntimeState* state, bool* got_partition) {
  VLOG(2) << "PrepareSpilledPartitionForProbe\n" << NodeDebugString();
  DCHECK(input_partition_ == NULL);
  DCHECK_EQ(builder_->num_hash_partitions(), 0);
  DCHECK(probe_hash_partitions_.empty());
  if (spilled_partitions_.empty()) {
    *got_partition = false;
    return Status::OK();
  }

  input_partition_ = std::move(spilled_partitions_.front());
  spilled_partitions_.pop_front();
  PhjBuilder::Partition* build_partition = input_partition_->build_partition();
  DCHECK(build_partition->is_spilled());
  if (input_partition_->probe_rows()->num_rows() == 0) {
    // If there are no probe rows, there's no need to build the hash table, and
    // only partitions with NeedToProcessUnmatcheBuildRows() will have been added
    // to 'spilled_partitions_' in CleanUpHashPartitions().
    DCHECK(NeedToProcessUnmatchedBuildRows());
    bool got_read_buffer = false;
    RETURN_IF_ERROR(input_partition_->build_partition()->build_rows()->PrepareForRead(
        true, &got_read_buffer));
    if (!got_read_buffer) {
      return mem_tracker()->MemLimitExceeded(
          runtime_state_, Substitute(PREPARE_FOR_READ_FAILED_ERROR_MSG, id_));
    }

    *got_partition = true;
    UpdateState(PROBING_SPILLED_PARTITION);
    COUNTER_ADD(num_hash_table_builds_skipped_, 1);
    return Status::OK();
  }

  // Make sure we have a buffer to read the probe rows before we build the hash table.
  RETURN_IF_ERROR(input_partition_->PrepareForRead());
  ht_ctx_->set_level(build_partition->level());

  // Try to build a hash table for the spilled build partition.
  bool built;
  RETURN_IF_ERROR(build_partition->BuildHashTable(&built));

  if (!built) {
    // This build partition still does not fit in memory, repartition.
    UpdateState(REPARTITIONING_BUILD);

    int next_partition_level = build_partition->level() + 1;
    if (UNLIKELY(next_partition_level >= MAX_PARTITION_DEPTH)) {
      return Status(TErrorCode::PARTITIONED_HASH_JOIN_MAX_PARTITION_DEPTH, id(),
          MAX_PARTITION_DEPTH);
    }
    ht_ctx_->set_level(next_partition_level);

    // Spill to free memory from hash tables and pinned streams for use in new partitions.
    RETURN_IF_ERROR(build_partition->Spill(BufferedTupleStream::UNPIN_ALL));
    // Temporarily free up the probe buffer to use when repartitioning.
    input_partition_->probe_rows()->UnpinStream(BufferedTupleStream::UNPIN_ALL);
    DCHECK_EQ(build_partition->build_rows()->BytesPinned(false), 0) << NodeDebugString();
    DCHECK_EQ(input_partition_->probe_rows()->BytesPinned(false), 0) << NodeDebugString();
    int64_t num_input_rows = build_partition->build_rows()->num_rows();
    RETURN_IF_ERROR(builder_->RepartitionBuildInput(
        build_partition, next_partition_level, input_partition_->probe_rows()));

    // Check if there was any reduction in the size of partitions after repartitioning.
    int64_t largest_partition_rows = builder_->LargestPartitionRows();
    DCHECK_GE(num_input_rows, largest_partition_rows) << "Cannot have a partition with "
                                                         "more rows than the input";
    if (UNLIKELY(num_input_rows == largest_partition_rows)) {
      return Status(TErrorCode::PARTITIONED_HASH_JOIN_REPARTITION_FAILS, id_,
          next_partition_level, num_input_rows, NodeDebugString(),
          buffer_pool_client_.DebugString());
    }

    RETURN_IF_ERROR(PrepareForProbe());
    UpdateState(REPARTITIONING_PROBE);
  } else {
    DCHECK(!input_partition_->build_partition()->is_spilled());
    DCHECK(input_partition_->build_partition()->hash_tbl() != NULL);
    // In this case, we did not have to partition the build again, we just built
    // a hash table. This means the probe does not have to be partitioned either.
    for (int i = 0; i < PARTITION_FANOUT; ++i) {
      hash_tbls_[i] = input_partition_->build_partition()->hash_tbl();
    }
    UpdateState(PROBING_SPILLED_PARTITION);
  }

  COUNTER_ADD(num_probe_rows_partitioned_, input_partition_->probe_rows()->num_rows());
  *got_partition = true;
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

Status PartitionedHashJoinNode::GetNext(RuntimeState* state, RowBatch* out_batch,
    bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  DCHECK(!out_batch->AtCapacity());

  Status status = Status::OK();
  *eos = false;
  // Save the number of rows in case GetNext() is called with a non-empty batch,
  // which can happen in a subplan.
  int num_rows_before = out_batch->num_rows();

  while (true) {
    DCHECK(!*eos);
    DCHECK(status.ok());
    DCHECK_NE(state_, PARTITIONING_BUILD) << "Should not be in GetNext()";
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));

    if (!output_build_partitions_.empty()) {
      DCHECK(NeedToProcessUnmatchedBuildRows());

      // Flush the remaining unmatched build rows of any partitions we are done
      // processing before moving onto the next partition.
      RETURN_IF_ERROR(OutputUnmatchedBuild(out_batch));
      if (out_batch->AtCapacity()) break;

      // Stopped before batch was at capacity: - we must have finished outputting
      // unmatched build rows.
      DCHECK(output_build_partitions_.empty());
      DCHECK_EQ(builder_->num_hash_partitions(), 0);
      DCHECK(probe_hash_partitions_.empty());
    }

    if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
      // In this case, we want to output rows from the null aware partition.
      if (builder_->null_aware_partition() == NULL) {
        DCHECK(null_aware_probe_partition_ == NULL);
        *eos = true;
        break;
      }

      if (null_probe_output_idx_ >= 0) {
        RETURN_IF_ERROR(OutputNullAwareNullProbe(state, out_batch));
        if (out_batch->AtCapacity()) break;
        continue;
      }

      if (output_null_aware_probe_rows_running_) {
        RETURN_IF_ERROR(OutputNullAwareProbeRows(state, out_batch));
        if (out_batch->AtCapacity()) break;
        continue;
      }
    }

    // Finish processing rows in the current probe batch.
    if (probe_batch_pos_ != -1) {
      // Putting SCOPED_TIMER in ProcessProbeBatch() causes weird exception handling IR
      // in the xcompiled function, so call it here instead.
      int rows_added = 0;
      TPrefetchMode::type prefetch_mode = state->query_options().prefetch_mode;
      SCOPED_TIMER(probe_timer_);
      if (process_probe_batch_fn_ == NULL) {
        rows_added = ProcessProbeBatch(join_op_, prefetch_mode, out_batch, ht_ctx_.get(),
            &status);
      } else {
        DCHECK(process_probe_batch_fn_level0_ != NULL);
        if (ht_ctx_->level() == 0) {
          rows_added = process_probe_batch_fn_level0_(this, prefetch_mode, out_batch,
              ht_ctx_.get(), &status);
        } else {
          rows_added = process_probe_batch_fn_(this, prefetch_mode, out_batch,
              ht_ctx_.get(), &status);
        }
      }
      if (UNLIKELY(rows_added < 0)) {
        DCHECK(!status.ok());
        return status;
      }
      DCHECK(status.ok());
      out_batch->CommitRows(rows_added);
      if (out_batch->AtCapacity()) break;

      DCHECK(current_probe_row_ == NULL);
    }

    // Try to continue from the current probe side input.
    if (state_ == PARTITIONING_PROBE) {
      DCHECK(input_partition_ == NULL);
      RETURN_IF_ERROR(NextProbeRowBatch(state, out_batch));
    } else {
      DCHECK(state_ == REPARTITIONING_PROBE || state_ == PROBING_SPILLED_PARTITION)
          << state_;
      DCHECK(probe_side_eos_);
      if (input_partition_ != NULL) {
        RETURN_IF_ERROR(NextSpilledProbeRowBatch(state, out_batch));
      }
    }
    // Free expr result allocations of the probe side expressions only after
    // ExprValuesCache has been reset.
    DCHECK(ht_ctx_->expr_values_cache()->AtEnd());
    probe_expr_results_pool_->Clear();

    // We want to return as soon as we have attached a tuple stream to the out_batch
    // (before preparing a new partition). The attached tuple stream will be recycled
    // by the caller, freeing up more memory when we prepare the next partition.
    if (out_batch->AtCapacity()) break;

    // Got a batch, just keep going.
    if (probe_batch_pos_ == 0) continue;
    DCHECK_EQ(probe_batch_pos_, -1);

    // Finished up all probe rows for 'hash_partitions_'. We may have already cleaned up
    // the hash partitions, e.g. if we had to output some unmatched build rows below.
    if (builder_->num_hash_partitions() != 0) {
      RETURN_IF_ERROR(CleanUpHashPartitions(state, out_batch));
      if (out_batch->AtCapacity()) break;
    }

    // There are some partitions that need to flush their unmatched build rows.
    if (!output_build_partitions_.empty()) continue;

    // We get this far if there is nothing left to return from the current partition.
    // Move to the next spilled partition.
    bool got_partition;
    RETURN_IF_ERROR(PrepareSpilledPartitionForProbe(state, &got_partition));
    if (got_partition) continue; // Probe the spilled partition.

    if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
      // Prepare the null-aware partitions, then resume at the top of the loop to output
      // the rows.
      RETURN_IF_ERROR(PrepareNullAwarePartition());
      continue;
    }
    DCHECK(builder_->null_aware_partition() == NULL);

    *eos = true;
    break;
  }

  int num_rows_added = out_batch->num_rows() - num_rows_before;
  DCHECK_GE(num_rows_added, 0);

  if (limit_ != -1 && num_rows_returned_ + num_rows_added > limit_) {
    // Truncate the row batch if we went over the limit.
    num_rows_added = limit_ - num_rows_returned_;
    DCHECK_GE(num_rows_added, 0);
    out_batch->set_num_rows(num_rows_before + num_rows_added);
    probe_batch_->TransferResourceOwnership(out_batch);
    *eos = true;
  }

  num_rows_returned_ += num_rows_added;
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK();
}

Status PartitionedHashJoinNode::OutputUnmatchedBuild(RowBatch* out_batch) {
  SCOPED_TIMER(probe_timer_);
  DCHECK(NeedToProcessUnmatchedBuildRows());
  DCHECK(!output_build_partitions_.empty());

  if (output_unmatched_batch_iter_.get() != NULL) {
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
  }
  return Status::OK();
}

void PartitionedHashJoinNode::OutputUnmatchedBuildFromHashTable(RowBatch* out_batch) {
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
  DCHECK(build_row != NULL);
  if (join_op_ == TJoinOp::RIGHT_ANTI_JOIN) {
    out_batch->CopyRow(build_row, out_batch_iterator->Get());
  } else {
    CreateOutputRow(out_batch_iterator->Get(), NULL, build_row);
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

Status PartitionedHashJoinNode::OutputNullAwareNullProbe(RuntimeState* state,
    RowBatch* out_batch) {
  DCHECK(builder_->null_aware_partition() != NULL);
  DCHECK(null_aware_probe_partition_ != NULL);
  DCHECK(!output_null_aware_probe_rows_running_);
  DCHECK_NE(probe_batch_pos_, -1);

  if (probe_batch_pos_ == probe_batch_->num_rows()) {
    probe_batch_pos_ = 0;
    probe_batch_->TransferResourceOwnership(out_batch);
    if (out_batch->AtCapacity()) return Status::OK();
    bool eos;
    RETURN_IF_ERROR(null_probe_rows_->GetNext(probe_batch_.get(), &eos));
    if (probe_batch_->num_rows() == 0 && eos) {
      // All done.
      builder_->CloseNullAwarePartition(out_batch);
      null_aware_probe_partition_->Close(out_batch);
      null_aware_probe_partition_.reset();
      // Flush out the resources to free up memory.
      null_probe_rows_->Close(out_batch, RowBatch::FlushMode::FLUSH_RESOURCES);
      null_probe_rows_.reset();
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

// In this case we had a lot of NULLs on either the build/probe side. While this is
// possible to process by re-reading the spilled streams for each row with minimal code
// effort, this would behave very slowly (we'd need to do IO for each row). This seems
// like a reasonable limitation for now.
// TODO: revisit.
Status PartitionedHashJoinNode::NullAwareAntiJoinError(BufferedTupleStream* rows) {
  return Status(Substitute(
      "Unable to perform Null-Aware Anti-Join. Could not get enough reservation to fit "
      "all rows with NULLs from the build side in memory. Memory required for $0 rows "
      "was $1. $2/$3 of the join's reservation was available for the rows.",
      rows->num_rows(), PrettyPrinter::PrintBytes(rows->byte_size()),
      PrettyPrinter::PrintBytes(
          buffer_pool_client_.GetUnusedReservation() + rows->BytesPinned(false)),
      PrettyPrinter::PrintBytes(buffer_pool_client_.GetReservation())));
}

Status PartitionedHashJoinNode::InitNullAwareProbePartition() {
  RuntimeState* state = runtime_state_;
  unique_ptr<BufferedTupleStream> probe_rows = make_unique<BufferedTupleStream>(
      state, child(0)->row_desc(), &buffer_pool_client_,
      resource_profile_.spillable_buffer_size,
      resource_profile_.max_row_buffer_size);
  Status status = probe_rows->Init(id(), true);
  if (!status.ok()) goto error;
  bool got_buffer;
  status = probe_rows->PrepareForWrite(&got_buffer);
  if (!status.ok()) goto error;
  DCHECK(got_buffer)
      << "Accounted in min reservation" << buffer_pool_client_.DebugString();
  null_aware_probe_partition_.reset(new ProbePartition(
      state, this, builder_->null_aware_partition(), std::move(probe_rows)));
  return Status::OK();

error:
  DCHECK(!status.ok());
  // Ensure the temporary 'probe_rows' stream is closed correctly on error.
  probe_rows->Close(NULL, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  return status;
}

Status PartitionedHashJoinNode::InitNullProbeRows() {
  RuntimeState* state = runtime_state_;
  null_probe_rows_ = make_unique<BufferedTupleStream>(state, child(0)->row_desc(),
      &buffer_pool_client_, resource_profile_.spillable_buffer_size,
      resource_profile_.max_row_buffer_size);
  // Start with stream pinned, unpin later if needed.
  RETURN_IF_ERROR(null_probe_rows_->Init(id(), true));
  bool got_buffer;
  RETURN_IF_ERROR(null_probe_rows_->PrepareForWrite(&got_buffer));
  DCHECK(got_buffer)
      << "Accounted in min reservation" << buffer_pool_client_.DebugString();
  return Status::OK();
}

Status PartitionedHashJoinNode::PrepareNullAwarePartition() {
  DCHECK(builder_->null_aware_partition() != NULL);
  DCHECK(null_aware_probe_partition_ != NULL);
  DCHECK(!output_null_aware_probe_rows_running_);
  DCHECK_EQ(probe_batch_pos_, -1);
  DCHECK_EQ(probe_batch_->num_rows(), 0);

  BufferedTupleStream* build_stream = builder_->null_aware_partition()->build_rows();
  BufferedTupleStream* probe_stream = null_aware_probe_partition_->probe_rows();

  if (build_stream->num_rows() == 0) {
    // There were no build rows. Nothing to do. Just prepare to output the null
    // probe rows.
    DCHECK_EQ(probe_stream->num_rows(), 0);
    output_null_aware_probe_rows_running_ = false;
    RETURN_IF_ERROR(PrepareNullAwareNullProbe());
    return Status::OK();
  }

  output_null_aware_probe_rows_running_ = true;
  bool pinned;
  RETURN_IF_ERROR(build_stream->PinStream(&pinned));
  if (!pinned) return NullAwareAntiJoinError(build_stream);

  // Initialize the streams for read.
  bool got_read_buffer;
  RETURN_IF_ERROR(probe_stream->PrepareForRead(true, &got_read_buffer));
  if (!got_read_buffer) {
    return mem_tracker()->MemLimitExceeded(
        runtime_state_, Substitute(PREPARE_FOR_READ_FAILED_ERROR_MSG, id_));
  }
  probe_batch_pos_ = 0;
  return Status::OK();
}

Status PartitionedHashJoinNode::OutputNullAwareProbeRows(RuntimeState* state,
    RowBatch* out_batch) {
  DCHECK(builder_->null_aware_partition() != NULL);
  DCHECK(null_aware_probe_partition_ != NULL);
  DCHECK(output_null_aware_probe_rows_running_);

  ScalarExprEvaluator* const* join_conjunct_evals = other_join_conjunct_evals_.data();
  int num_join_conjuncts = other_join_conjuncts_.size();
  DCHECK(probe_batch_ != NULL);

  BufferedTupleStream* probe_stream = null_aware_probe_partition_->probe_rows();
  if (probe_batch_pos_ == probe_batch_->num_rows()) {
    probe_batch_pos_ = 0;
    probe_batch_->TransferResourceOwnership(out_batch);
    if (out_batch->AtCapacity()) return Status::OK();

    // Get the next probe batch.
    bool eos;
    RETURN_IF_ERROR(probe_stream->GetNext(probe_batch_.get(), &eos));

    if (probe_batch_->num_rows() == 0) {
      RETURN_IF_ERROR(EvaluateNullProbe(
            state, builder_->null_aware_partition()->build_rows()));
      output_null_aware_probe_rows_running_ = false;
      RETURN_IF_ERROR(PrepareNullAwareNullProbe());
      return Status::OK();
    }
  }

  // For each probe row, iterate over all the build rows and check for rows
  // that did not have any matches.
  for (; probe_batch_pos_ < probe_batch_->num_rows(); ++probe_batch_pos_) {
    if (out_batch->AtCapacity()) break;
    TupleRow* probe_row = probe_batch_->GetRow(probe_batch_pos_);
    bool matched = false;
    bool got_reservation;
    BufferedTupleStream* null_build_stream =
        builder_->null_aware_partition()->build_rows();
    RETURN_IF_ERROR(null_build_stream->PrepareForRead(false, &got_reservation));
    DCHECK(got_reservation) << "Should have been pinned";
    RowBatch null_build_batch(child(1)->row_desc(), state->batch_size(), mem_tracker());
    bool eos;
    do {
      RETURN_IF_ERROR(null_build_stream->GetNext(&null_build_batch, &eos));
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

Status PartitionedHashJoinNode::PrepareForProbe() {
  DCHECK_EQ(builder_->num_hash_partitions(), PARTITION_FANOUT);
  DCHECK(probe_hash_partitions_.empty());

  // Initialize the probe partitions, providing them with probe streams.
  vector<unique_ptr<BufferedTupleStream>> probe_streams =
      builder_->TransferProbeStreams();
  probe_hash_partitions_.resize(PARTITION_FANOUT);
  bool have_spilled_hash_partitions = false;
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    PhjBuilder::Partition* build_partition = builder_->hash_partition(i);
    if (build_partition->IsClosed() || !build_partition->is_spilled()) continue;
    have_spilled_hash_partitions = true;
    DCHECK(!probe_streams.empty()) << "Builder should have created enough streams";
    CreateProbePartition(i, std::move(probe_streams.back()));
    probe_streams.pop_back();
  }
  DCHECK(probe_streams.empty()) << "Builder should not have created extra streams";

  // Unpin null-aware probe streams if any partitions spilled: we don't want to waste
  // memory pinning the probe streams that might be needed to process spilled partitions.
  if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN
      && (have_spilled_hash_partitions
             || builder_->null_aware_partition()->is_spilled())) {
    null_probe_rows_->UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT);
    null_aware_probe_partition_->probe_rows()->UnpinStream(
        BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT);
  }

  // Initialize the hash_tbl_ caching array.
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    hash_tbls_[i] = builder_->hash_partition(i)->hash_tbl();
  }

  // Validate the state of the partitions.
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    PhjBuilder::Partition* build_partition = builder_->hash_partition(i);
    ProbePartition* probe_partition = probe_hash_partitions_[i].get();
    if (build_partition->IsClosed()) {
      DCHECK(hash_tbls_[i] == NULL);
      DCHECK(probe_partition == NULL);
    } else if (build_partition->is_spilled()) {
      DCHECK(hash_tbls_[i] == NULL);
      DCHECK(probe_partition != NULL);
    } else {
      DCHECK(hash_tbls_[i] != NULL);
      DCHECK(probe_partition == NULL);
    }
  }
  return Status::OK();
}

void PartitionedHashJoinNode::CreateProbePartition(
    int partition_idx, unique_ptr<BufferedTupleStream> probe_rows) {
  DCHECK_GE(partition_idx, 0);
  DCHECK_LT(partition_idx, probe_hash_partitions_.size());
  DCHECK(probe_hash_partitions_[partition_idx] == NULL);
  probe_hash_partitions_[partition_idx] = make_unique<ProbePartition>(runtime_state_,
      this, builder_->hash_partition(partition_idx), std::move(probe_rows));
}

bool PartitionedHashJoinNode::AppendProbeRowSlow(
    BufferedTupleStream* stream, TupleRow* row, Status* status) {
  if (!status->ok()) return false; // Check if AddRow() set status.
  *status = runtime_state_->StartSpilling(mem_tracker());
  if (!status->ok()) return false;
  stream->UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT);
  return stream->AddRow(row, status);
}

Status PartitionedHashJoinNode::EvaluateNullProbe(
    RuntimeState* state, BufferedTupleStream* build) {
  if (null_probe_rows_ == NULL || null_probe_rows_->num_rows() == 0) {
    return Status::OK();
  }
  DCHECK_EQ(null_probe_rows_->num_rows(), matched_null_probe_.size());
  bool got_read_buffer;
  RETURN_IF_ERROR(null_probe_rows_->PrepareForRead(false, &got_read_buffer));
  DCHECK(got_read_buffer) << "Probe stream should always have a read or write iterator";

  ScalarExprEvaluator* const* join_conjunct_evals = other_join_conjunct_evals_.data();
  int num_join_conjuncts = other_join_conjuncts_.size();
  RowBatch probe_batch(child(0)->row_desc(), runtime_state_->batch_size(), mem_tracker());

  bool pinned;
  RETURN_IF_ERROR(build->PinStream(&pinned));
  if (!pinned) return NullAwareAntiJoinError(build);

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
      bool got_reservation;
      RETURN_IF_ERROR(build->PrepareForRead(false, &got_reservation));
      DCHECK(got_reservation) << "Should have been pinned";
      RowBatch build_batch(child(1)->row_desc(), state->batch_size(), mem_tracker());
      bool build_eos;
      do {
        RETURN_IF_ERROR(build->GetNext(&build_batch, &build_eos));
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

Status PartitionedHashJoinNode::CleanUpHashPartitions(
    RuntimeState* state, RowBatch* batch) {
  DCHECK_EQ(probe_batch_pos_, -1);
  // At this point all the rows have been read from the probe side for all partitions in
  // hash_partitions_.
  VLOG(2) << "Probe Side Consumed\n" << NodeDebugString();

  // Walk the partitions that had hash tables built for the probe phase and close them.
  // In the case of right outer and full outer joins, instead of closing those partitions,
  // add them to the list of partitions that need to output any unmatched build rows.
  // This partition will be closed by the function that actually outputs unmatched build
  // rows.
  DCHECK_EQ(builder_->num_hash_partitions(), PARTITION_FANOUT);
  DCHECK_EQ(probe_hash_partitions_.size(), PARTITION_FANOUT);
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    ProbePartition* probe_partition = probe_hash_partitions_[i].get();
    PhjBuilder::Partition* build_partition = builder_->hash_partition(i);
    if (build_partition->IsClosed()) {
      DCHECK(probe_partition == NULL);
      continue;
    }
    if (build_partition->is_spilled()) {
      DCHECK(probe_partition != NULL);
      DCHECK(build_partition->hash_tbl() == NULL) << NodeDebugString();
      // Unpin the probe stream to free up more memory. We need to free all memory so we
      // can recurse the algorithm and create new hash partitions from spilled partitions.
      // TODO: we shouldn't need to unpin the build stream if we stop spilling
      // while probing.
      build_partition->build_rows()->UnpinStream(BufferedTupleStream::UNPIN_ALL);
      DCHECK_EQ(build_partition->build_rows()->BytesPinned(false), 0);
      probe_partition->probe_rows()->UnpinStream(BufferedTupleStream::UNPIN_ALL);

      if (probe_partition->probe_rows()->num_rows() != 0
          || NeedToProcessUnmatchedBuildRows()) {
        // Push newly created partitions at the front. This means a depth first walk
        // (more finely partitioned partitions are processed first). This allows us
        // to delete blocks earlier and bottom out the recursion earlier.
        spilled_partitions_.push_front(std::move(probe_hash_partitions_[i]));
      } else {
        // There's no more processing to do for this partition, and since there were no
        // probe rows we didn't return any rows that reference memory from these
        // partitions, so just free the resources.
        build_partition->Close(NULL);
        probe_partition->Close(NULL);
        COUNTER_ADD(num_hash_table_builds_skipped_, 1);
      }
    } else {
      DCHECK(probe_partition == NULL);
      if (NeedToProcessUnmatchedBuildRows()) {
        DCHECK(output_unmatched_batch_iter_.get() == NULL);
        if (output_build_partitions_.empty()) {
          hash_tbl_iterator_ = build_partition->hash_tbl()->FirstUnmatched(ht_ctx_.get());
        }
        output_build_partitions_.push_back(build_partition);
      } else if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        // For NAAJ, we need to try to match all the NULL probe rows with this partition
        // before closing it. The NULL probe rows could have come from any partition
        // so we collect them all and match them at the end.
        RETURN_IF_ERROR(EvaluateNullProbe(state, build_partition->build_rows()));
        build_partition->Close(batch);
      } else {
        build_partition->Close(batch);
      }
    }
  }

  builder_->ClearHashPartitions();
  probe_hash_partitions_.clear();
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

void PartitionedHashJoinNode::UpdateState(HashJoinState next_state) {
  // Validate the state transition.
  switch (state_) {
    case PARTITIONING_BUILD: DCHECK_EQ(next_state, PARTITIONING_PROBE); break;
    case PARTITIONING_PROBE:
    case REPARTITIONING_PROBE:
    case PROBING_SPILLED_PARTITION:
      DCHECK(
          next_state == REPARTITIONING_BUILD || next_state == PROBING_SPILLED_PARTITION);
      break;
    case REPARTITIONING_BUILD: DCHECK_EQ(next_state, REPARTITIONING_PROBE); break;
    default: DCHECK(false) << "Invalid state " << state_;
  }
  state_ = next_state;
  VLOG(2) << "Transitioned State:" << endl << NodeDebugString();
}

string PartitionedHashJoinNode::PrintState() const {
  switch (state_) {
    case PARTITIONING_BUILD: return "PartitioningBuild";
    case PARTITIONING_PROBE: return "PartitioningProbe";
    case PROBING_SPILLED_PARTITION: return "ProbingSpilledPartition";
    case REPARTITIONING_BUILD: return "RepartitioningBuild";
    case REPARTITIONING_PROBE: return "RepartitioningProbe";
    default: DCHECK(false);
  }
  return "";
}

string PartitionedHashJoinNode::NodeDebugString() const {
  stringstream ss;
  ss << "PartitionedHashJoinNode (id=" << id() << " op=" << join_op_
     << " state=" << PrintState()
     << " #spilled_partitions=" << spilled_partitions_.size() << ")" << endl;

  if (builder_ != NULL) {
    ss << "PhjBuilder: " << builder_->DebugString();
  }

  ss << "Probe hash partitions: " << probe_hash_partitions_.size() << ":" << endl;
  for (int i = 0; i < probe_hash_partitions_.size(); ++i) {
    ProbePartition* probe_partition = probe_hash_partitions_[i].get();
    ss << "  Probe hash partition " << i << ": ";
    if (probe_partition != NULL) {
      ss << "probe ptr=" << probe_partition;
      BufferedTupleStream* probe_rows = probe_partition->probe_rows();
      if (probe_rows != NULL) {
        ss << "    Probe Rows: " << probe_rows->num_rows()
           << "    (Bytes total/pinned: " << probe_rows->byte_size() << "/"
           << probe_rows->BytesPinned(false) << ")";
      }
    }
    ss << endl;
  }

  if (!spilled_partitions_.empty()) {
    ss << "SpilledPartitions" << endl;
    for (const unique_ptr<ProbePartition>& probe_partition : spilled_partitions_) {
      PhjBuilder::Partition* build_partition = probe_partition->build_partition();
      DCHECK(build_partition->is_spilled());
      DCHECK(build_partition->hash_tbl() == NULL);
      DCHECK(build_partition->build_rows() != NULL);
      DCHECK(probe_partition->probe_rows() != NULL);
      ss << "  Partition=" << probe_partition.get() << endl
         << "   Spilled Build Rows: " << build_partition->build_rows()->num_rows() << endl
         << "   Spilled Probe Rows: " << probe_partition->probe_rows()->num_rows()
         << endl;
    }
  }
  if (input_partition_ != NULL) {
    DCHECK(input_partition_->probe_rows() != NULL);
    ss << "InputPartition: " << input_partition_.get() << endl;
    PhjBuilder::Partition* build_partition = input_partition_->build_partition();
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
    if (probe_rows != NULL) {
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
Status PartitionedHashJoinNode::CodegenCreateOutputRow(
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

  int num_probe_tuples = child(0)->row_desc()->tuple_descriptors().size();
  int num_build_tuples = child(1)->row_desc()->tuple_descriptors().size();

  // Copy probe row
  codegen->CodegenMemcpy(&builder, out_row_arg, probe_row_arg, probe_tuple_row_size_);
  llvm::Value* build_row_idx[] = {codegen->GetI32Constant(num_probe_tuples)};
  llvm::Value* build_row_dst =
      builder.CreateInBoundsGEP(out_row_arg, build_row_idx, "build_dst_ptr");

  // Copy build row.
  llvm::BasicBlock* build_not_null_block =
      llvm::BasicBlock::Create(context, "build_not_null", *fn);
  llvm::BasicBlock* build_null_block = NULL;

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
  codegen->CodegenMemcpy(&builder, build_row_dst, build_row_arg, build_tuple_row_size_);
  builder.CreateRetVoid();

  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == NULL) {
    return Status("PartitionedHashJoinNode::CodegenCreateOutputRow(): codegen'd "
        "CreateOutputRow() function failed verification, see log");
  }
  return Status::OK();
}

Status PartitionedHashJoinNode::CodegenProcessProbeBatch(
    LlvmCodeGen* codegen, TPrefetchMode::type prefetch_mode) {
  // Codegen for hashing rows
  llvm::Function* hash_fn;
  llvm::Function* murmur_hash_fn;
  RETURN_IF_ERROR(ht_ctx_->CodegenHashRow(codegen, false, &hash_fn));
  RETURN_IF_ERROR(ht_ctx_->CodegenHashRow(codegen, true, &murmur_hash_fn));

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
  DCHECK(process_probe_batch_fn != NULL);
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
  RETURN_IF_ERROR(ht_ctx_->CodegenEquals(codegen, false, &probe_equals_fn));

  // Codegen for evaluating probe rows
  llvm::Function* eval_row_fn;
  RETURN_IF_ERROR(ht_ctx_->CodegenEvalRow(codegen, false, &eval_row_fn));

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
  DCHECK_EQ(replaced, 1);

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
      DCHECK_EQ(replaced, 1);
      break;
    case TJoinOp::LEFT_OUTER_JOIN:
    case TJoinOp::FULL_OUTER_JOIN:
      DCHECK_EQ(replaced, 2);
      break;
    case TJoinOp::LEFT_ANTI_JOIN:
    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
    case TJoinOp::RIGHT_ANTI_JOIN:
      DCHECK_EQ(replaced, 0);
      break;
    default:
      DCHECK(false);
  }

  replaced = codegen->ReplaceCallSites(process_probe_batch_fn, eval_other_conjuncts_fn,
      "EvalOtherJoinConjuncts");
  DCHECK_EQ(replaced, 1);

  replaced = codegen->ReplaceCallSites(process_probe_batch_fn, probe_equals_fn, "Equals");
  // Depends on join_op_
  // TODO: switch statement
  DCHECK(replaced == 1 || replaced == 2 || replaced == 3 || replaced == 4) << replaced;

  // Replace hash-table parameters with constants.
  HashTableCtx::HashTableReplacedConstants replaced_constants;
  const bool stores_duplicates = true;
  const int num_build_tuples = child(1)->row_desc()->tuple_descriptors().size();
  RETURN_IF_ERROR(ht_ctx_->ReplaceHashTableConstants(codegen, stores_duplicates,
      num_build_tuples, process_probe_batch_fn, &replaced_constants));
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
  DCHECK_EQ(replaced, 1);

  replaced = codegen->ReplaceCallSites(process_probe_batch_fn, murmur_hash_fn, "HashRow");
  DCHECK_EQ(replaced, 1);

  // Finalize ProcessProbeBatch functions
  process_probe_batch_fn = codegen->FinalizeFunction(process_probe_batch_fn);
  if (process_probe_batch_fn == NULL) {
    return Status("PartitionedHashJoinNode::CodegenProcessProbeBatch(): codegen'd "
        "ProcessProbeBatch() function failed verification, see log");
  }
  process_probe_batch_fn_level0 =
      codegen->FinalizeFunction(process_probe_batch_fn_level0);
  if (process_probe_batch_fn_level0 == NULL) {
    return Status("PartitionedHashJoinNode::CodegenProcessProbeBatch(): codegen'd "
        "level-zero ProcessProbeBatch() function failed verification, see log");
  }

  // Register native function pointers
  codegen->AddFunctionToJit(process_probe_batch_fn,
                            reinterpret_cast<void**>(&process_probe_batch_fn_));
  codegen->AddFunctionToJit(process_probe_batch_fn_level0,
                            reinterpret_cast<void**>(&process_probe_batch_fn_level0_));
  return Status::OK();
}
