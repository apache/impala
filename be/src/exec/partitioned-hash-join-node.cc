// Copyright 2012 Cloudera Inc.
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

#include "exec/partitioned-hash-join-node.inline.h"

#include <sstream>
#include <gutil/strings/substitute.h>

#include "codegen/llvm-codegen.h"
#include "exec/hash-table.inline.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "exprs/slot-ref.h"
#include "runtime/buffered-block-mgr.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

DEFINE_bool(enable_phj_probe_side_filtering, true,
    "Enables pushing PHJ build side filters to probe side");

using namespace impala;
using namespace llvm;
using namespace strings;

PartitionedHashJoinNode::PartitionedHashJoinNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : BlockingJoinNode("PartitionedHashJoinNode", tnode.hash_join_node.join_op,
        pool, tnode, descs),
    block_mgr_client_(NULL),
    partition_build_timer_(NULL),
    null_aware_eval_timer_(NULL),
    using_small_buffers_(true),
    state_(PARTITIONING_BUILD),
    partition_pool_(new ObjectPool()),
    input_partition_(NULL),
    null_aware_partition_(NULL),
    non_empty_build_(false),
    null_probe_rows_(NULL),
    null_probe_output_idx_(-1),
    process_build_batch_fn_(NULL),
    process_build_batch_fn_level0_(NULL),
    process_probe_batch_fn_(NULL),
    process_probe_batch_fn_level0_(NULL) {
  memset(hash_tbls_, 0, sizeof(HashTable*) * PARTITION_FANOUT);
  can_add_probe_filters_ = tnode.hash_join_node.add_probe_filters;
  can_add_probe_filters_ &= FLAGS_enable_phj_probe_side_filtering;
}

Status PartitionedHashJoinNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(BlockingJoinNode::Init(tnode));
  DCHECK(tnode.__isset.hash_join_node);
  const vector<TEqJoinCondition>& eq_join_conjuncts =
      tnode.hash_join_node.eq_join_conjuncts;
  for (int i = 0; i < eq_join_conjuncts.size(); ++i) {
    ExprContext* ctx;
    RETURN_IF_ERROR(Expr::CreateExprTree(pool_, eq_join_conjuncts[i].left, &ctx));
    probe_expr_ctxs_.push_back(ctx);
    RETURN_IF_ERROR(Expr::CreateExprTree(pool_, eq_join_conjuncts[i].right, &ctx));
    build_expr_ctxs_.push_back(ctx);
  }
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool_, tnode.hash_join_node.other_join_conjuncts,
                            &other_join_conjunct_ctxs_));

  DCHECK(join_op_ != TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
      eq_join_conjuncts.size() == 1);
  return Status::OK();
}

Status PartitionedHashJoinNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  // Create the codegen object before preparing conjunct_ctxs_ and children_, so that any
  // ScalarFnCalls will use codegen.
  // TODO: this is brittle and hard to reason about, revisit
  if (state->codegen_enabled()) {
    LlvmCodeGen* codegen;
    RETURN_IF_ERROR(state->GetCodegen(&codegen));
  }

  // Disable probe-side filters if we are inside a subplan because no node
  // inside the subplan can use them.
  if (IsInSubplan()) can_add_probe_filters_ = false;

  RETURN_IF_ERROR(BlockingJoinNode::Prepare(state));
  runtime_state_ = state;

  // build and probe exprs are evaluated in the context of the rows produced by our
  // right and left children, respectively
  RETURN_IF_ERROR(
      Expr::Prepare(build_expr_ctxs_, state, child(1)->row_desc(), expr_mem_tracker()));
  RETURN_IF_ERROR(
      Expr::Prepare(probe_expr_ctxs_, state, child(0)->row_desc(), expr_mem_tracker()));
  // Although ConstructBuildSide() maybe be run in a separate thread, it is safe to free
  // local allocations in QueryMaintenance() since the build thread is not run
  // concurrently with other expr evaluation in this join node.
  AddExprCtxsToFree(probe_expr_ctxs_);
  AddExprCtxsToFree(build_expr_ctxs_);

  // other_join_conjunct_ctxs_ are evaluated in the context of rows assembled from all
  // build and probe tuples; full_row_desc is not necessarily the same as the output row
  // desc, e.g., because semi joins only return the build xor probe tuples
  RowDescriptor full_row_desc(child(0)->row_desc(), child(1)->row_desc());
  RETURN_IF_ERROR(
      Expr::Prepare(other_join_conjunct_ctxs_, state, full_row_desc, expr_mem_tracker()));
  AddExprCtxsToFree(other_join_conjunct_ctxs_);

  RETURN_IF_ERROR(state->block_mgr()->RegisterClient(
      MinRequiredBuffers(), mem_tracker(), state, &block_mgr_client_));

  bool should_store_nulls = join_op_ == TJoinOp::RIGHT_OUTER_JOIN ||
      join_op_ == TJoinOp::RIGHT_ANTI_JOIN || join_op_ == TJoinOp::FULL_OUTER_JOIN;
  ht_ctx_.reset(new HashTableCtx(build_expr_ctxs_, probe_expr_ctxs_,
      should_store_nulls, false, state->fragment_hash_seed(), MAX_PARTITION_DEPTH,
      child(1)->row_desc().tuple_descriptors().size()));

  if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    null_aware_eval_timer_ = ADD_TIMER(runtime_profile(), "NullAwareAntiJoinEvalTime");
  }

  partition_build_timer_ = ADD_TIMER(runtime_profile(), "BuildPartitionTime");
  num_hash_buckets_ =
      ADD_COUNTER(runtime_profile(), "HashBuckets", TUnit::UNIT);
  partitions_created_ =
      ADD_COUNTER(runtime_profile(), "PartitionsCreated", TUnit::UNIT);
  max_partition_level_ = runtime_profile()->AddHighWaterMarkCounter(
      "MaxPartitionLevel", TUnit::UNIT);
  num_build_rows_partitioned_ =
      ADD_COUNTER(runtime_profile(), "BuildRowsPartitioned", TUnit::UNIT);
  num_probe_rows_partitioned_ =
      ADD_COUNTER(runtime_profile(), "ProbeRowsPartitioned", TUnit::UNIT);
  num_repartitions_ =
      ADD_COUNTER(runtime_profile(), "NumRepartitions", TUnit::UNIT);
  num_spilled_partitions_ =
      ADD_COUNTER(runtime_profile(), "SpilledPartitions", TUnit::UNIT);
  largest_partition_percent_ = runtime_profile()->AddHighWaterMarkCounter(
      "LargestPartitionPercent", TUnit::UNIT);

  if (state->codegen_enabled()) {
    // Codegen for hashing rows
    Function* hash_fn = ht_ctx_->CodegenHashCurrentRow(state, false);
    Function* murmur_hash_fn = ht_ctx_->CodegenHashCurrentRow(state, true);
    if (hash_fn != NULL && murmur_hash_fn != NULL) {
      // Codegen for build path
      if (CodegenProcessBuildBatch(state, hash_fn, murmur_hash_fn)) {
        AddRuntimeExecOption("Build Side Codegen Enabled");
      }
      // Codegen for probe path
      if (CodegenProcessProbeBatch(state, hash_fn, murmur_hash_fn)) {
        AddRuntimeExecOption("Probe Side Codegen Enabled");
      }
    }
  }
  return Status::OK();
}

Status PartitionedHashJoinNode::Open(RuntimeState* state) {
  if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    // Since there is only one such NAAJ stream, we don't worry about the memory consumed
    // and always use IO-sized buffers.
    null_aware_partition_ = partition_pool_->Add(new Partition(state, this, 0, false));
    RETURN_IF_ERROR(
        null_aware_partition_->build_rows()->Init(id(), runtime_profile(), false));
    RETURN_IF_ERROR(
        null_aware_partition_->probe_rows()->Init(id(), runtime_profile(), false));

    null_probe_rows_ = partition_pool_->Add(new BufferedTupleStream(
        state, child(0)->row_desc(), state->block_mgr(), block_mgr_client_,
        false /* use small buffers */, false /* delete on read */ ));
    RETURN_IF_ERROR(null_probe_rows_->Init(id(), runtime_profile(), false));
  }
  RETURN_IF_ERROR(BlockingJoinNode::Open(state));
  return Status::OK();
}

Status PartitionedHashJoinNode::Reset(RuntimeState* state) {
  if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    non_empty_build_ = false;
    null_probe_output_idx_ = -1;
    matched_null_probe_.clear();
    // The null_probe_rows_ object is owned by the partition_pool_ which is cleared in
    // ClosePartitions(), so Close() the stream here first.
    if (null_probe_rows_ != NULL) null_probe_rows_->Close();
    null_probe_rows_ = NULL;
    nulls_build_batch_.reset();
  }
  state_ = PARTITIONING_BUILD;
  using_small_buffers_ = true;
  ht_ctx_->set_level(0);
  ClosePartitions();
  memset(hash_tbls_, 0, sizeof(HashTable*) * PARTITION_FANOUT);
  return ExecNode::Reset(state);
}

void PartitionedHashJoinNode::ClosePartitions() {
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    hash_partitions_[i]->Close(NULL);
  }
  hash_partitions_.clear();
  for (list<Partition*>::iterator it = spilled_partitions_.begin();
      it != spilled_partitions_.end(); ++it) {
    (*it)->Close(NULL);
  }
  spilled_partitions_.clear();
  for (list<Partition*>::iterator it = output_build_partitions_.begin();
      it != output_build_partitions_.end(); ++it) {
    (*it)->Close(NULL);
  }
  output_build_partitions_.clear();
  if (input_partition_ != NULL) {
    input_partition_->Close(NULL);
    input_partition_ = NULL;
  }
  if (null_aware_partition_ != NULL) {
    null_aware_partition_->Close(NULL);
    null_aware_partition_ = NULL;
  }
  partition_pool_->Clear();
}

void PartitionedHashJoinNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (ht_ctx_.get() != NULL) ht_ctx_->Close();

  if (null_probe_rows_ != NULL) null_probe_rows_->Close();
  nulls_build_batch_.reset();

  ClosePartitions();

  if (block_mgr_client_ != NULL) {
    state->block_mgr()->ClearReservations(block_mgr_client_);
  }
  Expr::Close(build_expr_ctxs_, state);
  Expr::Close(probe_expr_ctxs_, state);
  Expr::Close(other_join_conjunct_ctxs_, state);
  BlockingJoinNode::Close(state);
}

PartitionedHashJoinNode::Partition::Partition(RuntimeState* state,
        PartitionedHashJoinNode* parent, int level, bool use_small_buffers)
  : parent_(parent),
    is_closed_(false),
    is_spilled_(false),
    level_(level) {
  BufferedTupleStream* build = new BufferedTupleStream(
      state, parent_->child(1)->row_desc(), state->block_mgr(),
      parent_->block_mgr_client_, use_small_buffers);
  DCHECK(build != NULL);
  build_rows_ = state->obj_pool()->Add(build);
  BufferedTupleStream* probe = new BufferedTupleStream(
      state, parent_->child(0)->row_desc(),
      state->block_mgr(), parent_->block_mgr_client_, use_small_buffers);
  DCHECK(probe != NULL);
  probe_rows_ = state->obj_pool()->Add(probe);
}

PartitionedHashJoinNode::Partition::~Partition() {
  DCHECK(is_closed());
}

int64_t PartitionedHashJoinNode::Partition::EstimatedInMemSize() const {
  return build_rows_->byte_size() + HashTable::EstimateSize(build_rows_->num_rows());
}

int64_t PartitionedHashJoinNode::Partition::InMemSize() const {
  DCHECK(hash_tbl_.get() != NULL);
  return build_rows_->byte_size() + hash_tbl_->byte_size();
}

void PartitionedHashJoinNode::Partition::Close(RowBatch* batch) {
  if (is_closed()) return;
  is_closed_ = true;
  if (hash_tbl_.get() != NULL) hash_tbl_->Close();

  // Transfer ownership of build_rows_/probe_rows_ to batch if batch is not NULL.
  // Otherwise, close the stream here.
  if (build_rows_ != NULL) {
    if (batch == NULL) {
      build_rows_->Close();
    } else {
      batch->AddTupleStream(build_rows_);
    }
    build_rows_ = NULL;
  }
  if (probe_rows_ != NULL) {
    if (batch == NULL) {
      probe_rows_->Close();
    } else {
      batch->AddTupleStream(probe_rows_);
    }
    probe_rows_ = NULL;
  }
}

Status PartitionedHashJoinNode::Partition::Spill(bool unpin_all_build) {
  if (!is_spilled_) {
    COUNTER_ADD(parent_->num_spilled_partitions_, 1);
    if (parent_->num_spilled_partitions_->value() == 1) {
      parent_->AddRuntimeExecOption("Spilled");
    }
  }
  is_spilled_ = true;
  if (hash_tbl() != NULL) {
    hash_tbl()->Close();
    hash_tbl_.reset();
  }
  // If it is still using small buffers, then there is nothing else to do, as the small
  // buffers are always kept in memory.
  if (build_rows()->using_small_buffers()) return Status::OK();
  return build_rows()->UnpinStream(unpin_all_build);
}

Status PartitionedHashJoinNode::Partition::BuildHashTable(RuntimeState* state,
    bool* built, const bool add_probe_filters) {
  if (add_probe_filters) {
    return BuildHashTableInternal<true>(state, built);
  } else {
    return BuildHashTableInternal<false>(state, built);
  }
}

template<bool const AddProbeFilters>
Status PartitionedHashJoinNode::Partition::BuildHashTableInternal(
    RuntimeState* state, bool* built) {
  DCHECK(build_rows_ != NULL);
  *built = false;

  // TODO: estimate the entire size of the hash table and reserve all of it from
  // the block mgr.

  // We got the buffers we think we will need, try to build the hash table.
  RETURN_IF_ERROR(build_rows_->PinStream(false, built));
  if (!*built) return Status::OK();
  RETURN_IF_ERROR(build_rows_->PrepareForRead());

  RowBatch batch(parent_->child(1)->row_desc(), state->batch_size(),
      parent_->mem_tracker());
  HashTableCtx* ctx = parent_->ht_ctx_.get();
  // TODO: move the batch and indices as members to avoid reallocating.
  vector<BufferedTupleStream::RowIdx> indices;
  uint32_t seed0 = ctx->seed(0);
  bool eos = false;

  // Allocate the partition-local hash table. Initialize the number of buckets based on
  // the number of build rows (the number of rows is known at this point). This assumes
  // there are no duplicates which can be wrong. However, the upside in the common case
  // (few/no duplicates) is large and the downside when there are is low (a bit more
  // memory; the bucket memory is small compared to the memory needed for all the build
  // side allocations).
  // One corner case is if the stream contains tuples with zero footprint (no materialized
  // slots). If the tuples occupy no space, this implies all rows will be duplicates, so
  // create a small hash table, IMPALA-2256.
  // We always start with small pages in the hash table.
  int64_t estimated_num_buckets = build_rows()->has_tuple_footprint() ?
      HashTable::EstimateNumBuckets(build_rows()->num_rows()) : state->batch_size() * 2;
  hash_tbl_.reset(new HashTable(state, parent_->block_mgr_client_,
      parent_->child(1)->row_desc().tuple_descriptors().size(), build_rows(),
      1 << (32 - NUM_PARTITIONING_BITS), estimated_num_buckets));
  if (!hash_tbl_->Init()) goto not_built;

  if (AddProbeFilters) DCHECK_EQ(level_, 0) << "Should not add filters if repartitioning";
  do {
    RETURN_IF_ERROR(build_rows_->GetNext(&batch, &eos, &indices));
    DCHECK_EQ(batch.num_rows(), indices.size());
    int num_rows = batch.num_rows();
    DCHECK_LE(num_rows, hash_tbl_->EmptyBuckets()) << build_rows()->has_tuple_footprint();
    SCOPED_TIMER(parent_->build_timer_);
    for (int i = 0; i < num_rows; ++i) {
      TupleRow* row = batch.GetRow(i);
      uint32_t hash = 0;
      if (!ctx->EvalAndHashBuild(row, &hash)) continue;
      if (UNLIKELY(!hash_tbl_->Insert(ctx, indices[i], row, hash))) goto not_built;
      if (AddProbeFilters) {
        // Update the probe filters for this row.
        for (int j = 0; j < parent_->probe_filters_.size(); ++j) {
          if (parent_->probe_filters_[j].second == NULL) continue;
          void* e = parent_->build_expr_ctxs_[j]->GetValue(row);
          uint32_t h = RawValue::GetHashValue(e,
              parent_->build_expr_ctxs_[j]->root()->type(), seed0);
          parent_->probe_filters_[j].second->Set<true>(h, true);
        }
      }
    }
    batch.Reset();
  } while (!eos);

  // The hash table fits in memory and is built.
  DCHECK(*built);
  DCHECK(hash_tbl_.get() != NULL);
  is_spilled_ = false;
  COUNTER_ADD(parent_->num_hash_buckets_, hash_tbl_->num_buckets());
  return Status::OK();

not_built:
  *built = false;
  if (hash_tbl_.get() != NULL) {
    hash_tbl_->Close();
    hash_tbl_.reset();
  }
  if (parent_->can_add_probe_filters_) {
    // Disabling probe filter push down because not all rows will be included in the
    // probe filter due to a spilled partition.
    parent_->can_add_probe_filters_ = false;
    VLOG(2) << "Disabling probe filter push down because a partition will spill.";
  }
  return Status::OK();
}

bool PartitionedHashJoinNode::AllocateProbeFilters(RuntimeState* state) {
  if (!can_add_probe_filters_) return false;
  DCHECK(ht_ctx_.get() != NULL);
  DCHECK_EQ(build_expr_ctxs_.size(), probe_expr_ctxs_.size());
  probe_filters_.resize(probe_expr_ctxs_.size());
  for (int i = 0; i < build_expr_ctxs_.size(); ++i) {
    if (probe_expr_ctxs_[i]->root()->is_slotref()) {
      // TODO: Enable probe filters not only for "naked" slotrefs.
      probe_filters_[i].first =
          reinterpret_cast<SlotRef*>(probe_expr_ctxs_[i]->root())->slot_id();
      probe_filters_[i].second = new Bitmap(state->slot_filter_bitmap_size());
    } else {
      probe_filters_[i].second = NULL;
    }
  }
  return true;
}

bool PartitionedHashJoinNode::AttachProbeFilters(RuntimeState* state) {
  if (can_add_probe_filters_) {
    // Add all the bitmaps to the runtime state.
    // TODO: DCHECK(!is_in_subplan());
    AddRuntimeExecOption("Build-Side Filter Pushed Down");
    bool acquired_ownership = false;
    for (int i = 0; i < probe_filters_.size(); ++i) {
      if (probe_filters_[i].second == NULL) continue;
      state->AddBitmapFilter(probe_filters_[i].first, probe_filters_[i].second,
                             &acquired_ownership);
      VLOG(2) << "Bitmap filter added on slot: " << probe_filters_[i].first;
      if (!acquired_ownership) {
        delete probe_filters_[i].second;
        probe_filters_[i].second = NULL;
      }
    }
    return true;
  } else {
    // Make sure there are no memory leaks.
    for (int i = 0; i < probe_filters_.size(); ++i) {
      if (probe_filters_[i].second == NULL) continue;
      delete probe_filters_[i].second;
      probe_filters_[i].second = NULL;
    }
    return false;
  }
}

bool PartitionedHashJoinNode::AppendRowStreamFull(BufferedTupleStream* stream,
    TupleRow* row, Status* status) {
  // TODO: should this (and other AddRow() status checks below) try to continue if
  // status->IsMemLimitExceeded()?
  if (!status->ok()) return false;
  if (using_small_buffers_) {
    *status = ReserveTupleStreamBlocks();
    if (!status->ok()) return false;
    if (stream->AddRow(row, status)) return true;
  }

  // We ran out of memory. Pick a partition to spill.
  while (status->ok()) {
    Partition* spilled_partition;
    *status = SpillPartition(&spilled_partition);
    if (!status->ok()) return false;
    if (stream->AddRow(row, status)) return true;
    // Spilling one partition does not guarantee we can append a row now. Keep
    // spilling until we can append this row.
  }
  return false;
}

// TODO: can we do better with the spilling heuristic.
Status PartitionedHashJoinNode::SpillPartition(Partition** spilled_partition) {
  int64_t max_freed_mem = 0;
  int partition_idx = -1;
  *spilled_partition = NULL;

  // Iterate over the partitions and pick the largest partition to spill.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    Partition* candidate = hash_partitions_[i];
    if (candidate->is_closed()) continue;
    if (candidate->is_spilled()) continue;
    int64_t mem = candidate->build_rows()->bytes_in_mem(false);
    if (candidate->hash_tbl() != NULL) {
      // IMPALA-1488: Do not spill partitions that already had matches, because we
      // are going to lose information and return wrong results.
      if (UNLIKELY(candidate->hash_tbl()->HasMatches())) continue;
      mem += candidate->hash_tbl()->byte_size();
    }
    if (mem > max_freed_mem) {
      max_freed_mem = mem;
      partition_idx = i;
    }
  }

  if (partition_idx == -1) {
    // Could not find a partition to spill. This means the mem limit was just too low.
    return runtime_state_->block_mgr()->MemLimitTooLowError(block_mgr_client_, id());
  }

  VLOG(2) << "Spilling partition: " << partition_idx << endl << NodeDebugString();
  RETURN_IF_ERROR(hash_partitions_[partition_idx]->Spill(false));
  DCHECK(hash_partitions_[partition_idx]->probe_rows()->has_write_block());
  hash_tbls_[partition_idx] = NULL;
  *spilled_partition = hash_partitions_[partition_idx];
  return Status::OK();
}

Status PartitionedHashJoinNode::ConstructBuildSide(RuntimeState* state) {
  RETURN_IF_ERROR(Expr::Open(build_expr_ctxs_, state));
  RETURN_IF_ERROR(Expr::Open(probe_expr_ctxs_, state));
  RETURN_IF_ERROR(Expr::Open(other_join_conjunct_ctxs_, state));
  AllocateProbeFilters(state);

  // Do a full scan of child(1) and partition the rows.
  RETURN_IF_ERROR(child(1)->Open(state));
  RETURN_IF_ERROR(ProcessBuildInput(state, 0));

  AttachProbeFilters(state);
  UpdateState(PROCESSING_PROBE);
  return Status::OK();
}

Status PartitionedHashJoinNode::ProcessBuildInput(RuntimeState* state, int level) {
  if (level >= MAX_PARTITION_DEPTH) {
    Status status = Status::MemLimitExceeded();
    status.SetErrorMsg(ErrorMsg(
        TErrorCode::PARTITIONED_HASH_JOIN_MAX_PARTITION_DEPTH,
        id_, MAX_PARTITION_DEPTH));
    state->SetMemLimitExceeded();
    return status;
  }

  DCHECK(hash_partitions_.empty());
  if (input_partition_ != NULL) {
    DCHECK(input_partition_->build_rows() != NULL);
    DCHECK_EQ(input_partition_->build_rows()->blocks_pinned(), 0) << NodeDebugString();
    RETURN_IF_ERROR(input_partition_->build_rows()->PrepareForRead());
  }

  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    Partition* new_partition = new Partition(state, this, level, using_small_buffers_);
    DCHECK(new_partition != NULL);
    hash_partitions_.push_back(partition_pool_->Add(new_partition));
    RETURN_IF_ERROR(new_partition->build_rows()->Init(id(), runtime_profile(), true));

    // Initialize a buffer for the probe here to make sure why have it if we need it.
    // While this is not strictly necessary (there are some cases where we won't need this
    // buffer), the benefit is low. Not grabbing this buffer means there is an additional
    // buffer that could be used for the build side. However since this is only one
    // buffer, there is only a small range of build input sizes where this is beneficial
    // (an IO buffer size). It makes the logic much more complex to enable this
    // optimization.
    RETURN_IF_ERROR(new_partition->probe_rows()->Init(id(), runtime_profile(), false));
  }
  COUNTER_ADD(partitions_created_, PARTITION_FANOUT);
  COUNTER_SET(max_partition_level_, level);

  RowBatch build_batch(child(1)->row_desc(), state->batch_size(), mem_tracker());
  bool eos = false;
  int64_t total_build_rows = 0;
  while (!eos) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    if (input_partition_ == NULL) {
      // If we are still consuming batches from the build side.
      RETURN_IF_ERROR(child(1)->GetNext(state, &build_batch, &eos));
      COUNTER_ADD(build_row_counter_, build_batch.num_rows());
    } else {
      // If we are consuming batches that have already been partitioned.
      RETURN_IF_ERROR(input_partition_->build_rows()->GetNext(&build_batch, &eos));
    }
    total_build_rows += build_batch.num_rows();

    SCOPED_TIMER(partition_build_timer_);
    if (process_build_batch_fn_ == NULL || ht_ctx_->level() != 0) {
      RETURN_IF_ERROR(ProcessBuildBatch(&build_batch));
    } else {
      DCHECK(process_build_batch_fn_level0_ != NULL);
      if (ht_ctx_->level() == 0) {
        RETURN_IF_ERROR(process_build_batch_fn_level0_(this, &build_batch));
      } else {
        RETURN_IF_ERROR(process_build_batch_fn_(this, &build_batch));
      }
    }
    build_batch.Reset();
    DCHECK(!build_batch.AtCapacity());
  }

  if (input_partition_ != NULL) {
    // Done repartitioning build input, close it now.
    input_partition_->build_rows_->Close();
    input_partition_->build_rows_ = NULL;
  }

  stringstream ss;
  ss << "PHJ(node_id=" << id() << ") partitioned(level="
     << hash_partitions_[0]->level_ << ") "
     << total_build_rows << " rows into:" << endl;
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    Partition* partition = hash_partitions_[i];
    double percent =
        partition->build_rows()->num_rows() * 100 / static_cast<double>(total_build_rows);
    ss << "  " << i << " "  << (partition->is_spilled() ? "spilled" : "not spilled")
       << " (fraction=" << fixed << setprecision(2) << percent << "%)" << endl
       << "    #rows:" << partition->build_rows()->num_rows() << endl;
    COUNTER_SET(largest_partition_percent_, static_cast<int64_t>(percent));
  }
  VLOG(2) << ss.str();

  COUNTER_ADD(num_build_rows_partitioned_, total_build_rows);
  non_empty_build_ |= (total_build_rows > 0);
  RETURN_IF_ERROR(BuildHashTables(state));
  return Status::OK();
}

Status PartitionedHashJoinNode::InitGetNext(TupleRow* first_probe_row) {
  // TODO: Move this reset to blocking-join. Not yet though because of hash-join.
  ResetForProbe();
  return Status::OK();
}

Status PartitionedHashJoinNode::NextProbeRowBatch(
    RuntimeState* state, RowBatch* out_batch) {
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
    RETURN_IF_ERROR(input_partition_->probe_rows()->GetNext(probe_batch_.get(), &eos));
    DCHECK_GT(probe_batch_->num_rows(), 0);
    ResetForProbe();
  } else {
    // Done with this partition.
    if (join_op_ == TJoinOp::RIGHT_OUTER_JOIN || join_op_ == TJoinOp::RIGHT_ANTI_JOIN ||
        join_op_ == TJoinOp::FULL_OUTER_JOIN) {
      // In case of right-outer, right-anti and full-outer joins, we move this partition
      // to the list of partitions that we need to output their unmatched build rows.
      DCHECK(output_build_partitions_.empty());
      BufferedTupleStream* build_rows = input_partition_->build_rows();
      if (build_rows->num_rows() > 0) {
        DCHECK(input_partition_->hash_tbl_.get() != NULL) << " id: " << id_
            << " Build: " << build_rows->num_rows()
            << " Probe: " << probe_rows->num_rows() << endl
            << GetStackTrace();
        hash_tbl_iterator_ =
            input_partition_->hash_tbl_->FirstUnmatched(ht_ctx_.get());
        output_build_partitions_.push_back(input_partition_);
      }
    } else {
      // In any other case, just close the input partition.
      input_partition_->Close(out_batch);
      input_partition_ = NULL;
    }
    current_probe_row_ = NULL;
    probe_batch_pos_ = -1;
  }
  return Status::OK();
}

Status PartitionedHashJoinNode::PrepareNextPartition(RuntimeState* state) {
  DCHECK(input_partition_ == NULL);
  if (spilled_partitions_.empty()) return Status::OK();
  VLOG(2) << "PrepareNextPartition\n" << NodeDebugString();

  input_partition_ = spilled_partitions_.front();
  spilled_partitions_.pop_front();
  DCHECK(input_partition_->is_spilled());

  // Reserve one buffer to read the probe side.
  RETURN_IF_ERROR(input_partition_->probe_rows()->PrepareForRead());
  ht_ctx_->set_level(input_partition_->level_);

  int64_t mem_limit = mem_tracker()->SpareCapacity();
  // Try to build a hash table on top the spilled build rows.
  bool built = false;
  int64_t estimated_memory = input_partition_->EstimatedInMemSize();
  if (estimated_memory < mem_limit) {
    ht_ctx_->set_level(input_partition_->level_);
    // TODO: We disable filter on spilled partitions, but perhaps we can revisit
    // this, especially if the probe side is very big (e.g. has spilled as well).
    RETURN_IF_ERROR(input_partition_->BuildHashTable(state, &built, false));
  } else {
    LOG(INFO) << "In hash join id=" << id_ << " the estimated needed memory ("
        << estimated_memory << ") for partition " << input_partition_ << " with "
        << input_partition_->build_rows()->num_rows() << " build rows is larger "
        << " than the mem_limit (" << mem_limit << ").";
  }

  if (!built) {
    // This build partition still does not fit in memory, repartition.
    DCHECK(input_partition_->is_spilled());
    input_partition_->Spill(false);
    ht_ctx_->set_level(input_partition_->level_ + 1);
    int64_t num_input_rows = input_partition_->build_rows()->num_rows();
    RETURN_IF_ERROR(ProcessBuildInput(state, input_partition_->level_ + 1));

    // Check if there was any reduction in the size of partitions after repartitioning.
    int64_t largest_partition = LargestSpilledPartition();
    DCHECK_GE(num_input_rows, largest_partition) << "Cannot have a partition with "
        "more rows than the input";
    if (num_input_rows == largest_partition) {
      Status status = Status::MemLimitExceeded();
      status.AddDetail(Substitute("Cannot perform hash join at node with id $0. "
          "Repartitioning did not reduce the size of a spilled partition. "
          "Repartitioning level $1. Number of rows $2.",
          id_, input_partition_->level_ + 1, num_input_rows));
      state->SetMemLimitExceeded();
      return status;
    }
    UpdateState(REPARTITIONING);
  } else {
    DCHECK(hash_partitions_.empty());
    DCHECK(!input_partition_->is_spilled());
    DCHECK(input_partition_->hash_tbl() != NULL);
    // In this case, we did not have to partition the build again, we just built
    // a hash table. This means the probe does not have to be partitioned either.
    for (int i = 0; i < PARTITION_FANOUT; ++i) {
      hash_tbls_[i] = input_partition_->hash_tbl();
    }
    UpdateState(PROBING_SPILLED_PARTITION);
  }

  COUNTER_ADD(num_repartitions_, 1);
  COUNTER_ADD(num_probe_rows_partitioned_, input_partition_->probe_rows()->num_rows());
  return Status::OK();
}

int64_t PartitionedHashJoinNode::LargestSpilledPartition() const {
  int64_t max_rows = 0;
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    Partition* partition = hash_partitions_[i];
    DCHECK(partition != NULL) << i << " " << hash_partitions_.size();
    if (partition->is_closed() || !partition->is_spilled()) continue;
    int64_t rows = partition->build_rows()->num_rows();
    rows += partition->probe_rows()->num_rows();
    if (rows > max_rows) max_rows = rows;
  }
  return max_rows;
}

Status PartitionedHashJoinNode::GetNext(RuntimeState* state, RowBatch* out_batch,
    bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  DCHECK(!out_batch->AtCapacity());

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK();
  } else {
    *eos = false;
  }

  Status status = Status::OK();
  while (true) {
    DCHECK(status.ok());
    DCHECK_NE(state_, PARTITIONING_BUILD) << "Should not be in GetNext()";
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));

    if ((join_op_ == TJoinOp::RIGHT_OUTER_JOIN || join_op_ == TJoinOp::RIGHT_ANTI_JOIN ||
         join_op_ == TJoinOp::FULL_OUTER_JOIN) &&
        !output_build_partitions_.empty())  {
      // In case of right-outer, right-anti and full-outer joins, flush the remaining
      // unmatched build rows of any partition we are done processing, before processing
      // the next batch.
      OutputUnmatchedBuild(out_batch);
      if (!output_build_partitions_.empty()) break;

      // Finished to output unmatched build rows, move to next partition.
      DCHECK(hash_partitions_.empty());
      RETURN_IF_ERROR(PrepareNextPartition(state));
      if (input_partition_ == NULL) {
        *eos = true;
        break;
      }
      if (out_batch->AtCapacity()) break;
    }

    if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
      // In this case, we want to output rows from the null aware partition.
      if (null_aware_partition_ == NULL) {
        *eos = true;
        break;
      }

      if (null_probe_output_idx_ >= 0) {
        RETURN_IF_ERROR(OutputNullAwareNullProbe(state, out_batch));
        if (out_batch->AtCapacity()) break;
        continue;
      }

      if (nulls_build_batch_.get() != NULL) {
        RETURN_IF_ERROR(OutputNullAwareProbeRows(state, out_batch));
        if (out_batch->AtCapacity()) break;
        continue;
      }
    }

    // Finish up the current batch.
    {
      // Putting SCOPED_TIMER in ProcessProbeBatch() causes weird exception handling IR
      // in the xcompiled function, so call it here instead.
      int rows_added = 0;
      SCOPED_TIMER(probe_timer_);
      if (process_probe_batch_fn_ == NULL || ht_ctx_->level() != 0) {
        rows_added = ProcessProbeBatch(join_op_, out_batch, ht_ctx_.get(), &status);
      } else {
        DCHECK(process_probe_batch_fn_level0_ != NULL);
        if (ht_ctx_->level() == 0) {
          rows_added = process_probe_batch_fn_level0_(this, out_batch, ht_ctx_.get(),
              &status);
        } else {
          rows_added = process_probe_batch_fn_(this, out_batch, ht_ctx_.get(), &status);
        }
      }
      if (UNLIKELY(rows_added < 0)) {
        DCHECK(!status.ok());
        return status;
      }
      DCHECK(status.ok());
      out_batch->CommitRows(rows_added);
      num_rows_returned_ += rows_added;
      COUNTER_SET(rows_returned_counter_, num_rows_returned_);
    }
    if (out_batch->AtCapacity() || ReachedLimit()) break;
    DCHECK(current_probe_row_ == NULL);

    // Try to continue from the current probe side input.
    if (input_partition_ == NULL) {
      RETURN_IF_ERROR(NextProbeRowBatch(state, out_batch));
    } else {
      RETURN_IF_ERROR(NextSpilledProbeRowBatch(state, out_batch));
    }

    // We want to return as soon as we have attached a tuple stream to the out_batch
    // (before preparing a new partition). The attached tuple stream will be recycled
    // by the caller, freeing up more memory when we prepare the next partition.
    if (out_batch->AtCapacity()) break;

    // Got a batch, just keep going.
    if (probe_batch_pos_ == 0) continue;
    DCHECK_EQ(probe_batch_pos_, -1);

    // Finished up all probe rows for hash_partitions_.
    RETURN_IF_ERROR(CleanUpHashPartitions(out_batch));
    if (out_batch->AtCapacity()) break;

    if ((join_op_ == TJoinOp::RIGHT_OUTER_JOIN || join_op_ == TJoinOp::RIGHT_ANTI_JOIN ||
         join_op_ == TJoinOp::FULL_OUTER_JOIN) &&
        !output_build_partitions_.empty()) {
      // There are some partitions that need to flush their unmatched build rows.
      continue;
    }
    // Move onto the next partition.
    RETURN_IF_ERROR(PrepareNextPartition(state));

    if (input_partition_ == NULL) {
      if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        RETURN_IF_ERROR(PrepareNullAwarePartition());
      }
      if (null_aware_partition_ == NULL) {
        *eos = true;
        break;
      } else {
        *eos = false;
      }
    }
  }

  if (ReachedLimit()) *eos = true;
  return Status::OK();
}

void PartitionedHashJoinNode::OutputUnmatchedBuild(RowBatch* out_batch) {
  SCOPED_TIMER(probe_timer_);
  DCHECK(join_op_ == TJoinOp::RIGHT_OUTER_JOIN || join_op_ == TJoinOp::RIGHT_ANTI_JOIN ||
         join_op_ == TJoinOp::FULL_OUTER_JOIN);
  DCHECK(!output_build_partitions_.empty());
  ExprContext* const* conjunct_ctxs = &conjunct_ctxs_[0];
  const int num_conjuncts = conjunct_ctxs_.size();
  TupleRow* out_row = out_batch->GetRow(out_batch->AddRow());
  const int max_rows = out_batch->capacity() - out_batch->num_rows();
  int num_rows_added = 0;

  while (!out_batch->AtCapacity() && !hash_tbl_iterator_.AtEnd() &&
         num_rows_added < max_rows) {
    // Output remaining unmatched build rows.
    if (!hash_tbl_iterator_.IsMatched()) {
      TupleRow* build_row = hash_tbl_iterator_.GetRow();
      DCHECK(build_row != NULL);
      if (join_op_ == TJoinOp::RIGHT_ANTI_JOIN) {
        out_batch->CopyRow(build_row, out_row);
      } else {
        CreateOutputRow(out_row, NULL, build_row);
      }
      if (ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
        ++num_rows_added;
        out_row = out_row->next_row(out_batch);
      }
      hash_tbl_iterator_.SetMatched();
    }
    // Move to the next unmatched entry.
    hash_tbl_iterator_.NextUnmatched();
  }

  // If we reached the end of the hash table, then there are no other unmatched build
  // rows for this partition. In that case we need to close the partition, and move to
  // the next. If we have not reached the end of the hash table, it means that we reached
  // out_batch capacity and we need to continue to output unmatched build rows, without
  // closing the partition.
  if (hash_tbl_iterator_.AtEnd()) {
    output_build_partitions_.front()->Close(out_batch);
    output_build_partitions_.pop_front();
    // Move to the next partition to output unmatched rows.
    if (!output_build_partitions_.empty()) {
      hash_tbl_iterator_ =
          output_build_partitions_.front()->hash_tbl()->FirstUnmatched(ht_ctx_.get());
    }
  }

  DCHECK_LE(num_rows_added, max_rows);
  out_batch->CommitRows(num_rows_added);
  num_rows_returned_ += num_rows_added;
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
}

Status PartitionedHashJoinNode::PrepareNullAwareNullProbe() {
  DCHECK_EQ(null_probe_output_idx_, -1);
  RETURN_IF_ERROR(null_probe_rows_->PrepareForRead());
  DCHECK_EQ(probe_batch_->num_rows(), 0);
  probe_batch_pos_ = 0;
  null_probe_output_idx_ = 0;
  return Status::OK();
}

Status PartitionedHashJoinNode::OutputNullAwareNullProbe(RuntimeState* state,
    RowBatch* out_batch) {
  DCHECK(null_aware_partition_ != NULL);
  DCHECK(nulls_build_batch_.get() == NULL);
  DCHECK_NE(probe_batch_pos_, -1);

  if (probe_batch_pos_ == probe_batch_->num_rows()) {
    probe_batch_pos_ = 0;
    probe_batch_->TransferResourceOwnership(out_batch);
    if (out_batch->AtCapacity()) return Status::OK();
    bool eos;
    RETURN_IF_ERROR(null_probe_rows_->GetNext(probe_batch_.get(), &eos));
    if (probe_batch_->num_rows() == 0) {
      // All done.
      null_aware_partition_->Close(out_batch);
      null_aware_partition_ = NULL;
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
static Status NullAwareAntiJoinError(bool build) {
  return Status(Substitute("Unable to perform Null-Aware Anti-Join. There are too"
      " many NULLs on the $0 side to perform this join.", build ? "build" : "probe"));
}

Status PartitionedHashJoinNode::PrepareNullAwarePartition() {
  DCHECK(null_aware_partition_ != NULL);
  DCHECK(nulls_build_batch_.get() == NULL);
  DCHECK_EQ(probe_batch_pos_, -1);
  DCHECK_EQ(probe_batch_->num_rows(), 0);

  BufferedTupleStream* build_stream = null_aware_partition_->build_rows();
  BufferedTupleStream* probe_stream = null_aware_partition_->probe_rows();

  if (build_stream->num_rows() == 0) {
    // There were no build rows. Nothing to do. Just prepare to output the null
    // probe rows.
    DCHECK_EQ(probe_stream->num_rows(), 0);
    nulls_build_batch_.reset();
    RETURN_IF_ERROR(PrepareNullAwareNullProbe());
    return Status::OK();
  }

  // Bring the entire spilled build stream into memory and read into a single batch.
  bool got_rows;
  RETURN_IF_ERROR(build_stream->GetRows(&nulls_build_batch_, &got_rows));
  if (!got_rows) return NullAwareAntiJoinError(true);

  // Initialize the streams for read.
  RETURN_IF_ERROR(probe_stream->PrepareForRead());
  probe_batch_pos_ = 0;
  return Status::OK();
}

Status PartitionedHashJoinNode::OutputNullAwareProbeRows(RuntimeState* state,
    RowBatch* out_batch) {
  DCHECK(null_aware_partition_ != NULL);
  DCHECK(nulls_build_batch_.get() != NULL);

  ExprContext* const* join_conjunct_ctxs = &other_join_conjunct_ctxs_[0];
  int num_join_conjuncts = other_join_conjunct_ctxs_.size();
  DCHECK(probe_batch_.get() != NULL);

  BufferedTupleStream* probe_stream = null_aware_partition_->probe_rows();
  if (probe_batch_pos_ == probe_batch_->num_rows()) {
    probe_batch_pos_ = 0;
    probe_batch_->TransferResourceOwnership(out_batch);
    if (out_batch->AtCapacity()) return Status::OK();

    // Get the next probe batch.
    bool eos;
    RETURN_IF_ERROR(probe_stream->GetNext(probe_batch_.get(), &eos));

    if (probe_batch_->num_rows() == 0) {
      RETURN_IF_ERROR(EvaluateNullProbe(null_aware_partition_->build_rows()));
      nulls_build_batch_.reset();
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
    for (int i = 0; i < nulls_build_batch_->num_rows(); ++i) {
      CreateOutputRow(semi_join_staging_row_, probe_row, nulls_build_batch_->GetRow(i));
      if (ExecNode::EvalConjuncts(
          join_conjunct_ctxs, num_join_conjuncts, semi_join_staging_row_)) {
        matched = true;
        break;
      }
    }

    if (!matched) {
      TupleRow* out_row = out_batch->GetRow(out_batch->AddRow());
      out_batch->CopyRow(probe_row, out_row);
      out_batch->CommitLastRow();
    }
  }
  return Status::OK();
}

// When this function is called, we've finished processing the current build input
// (either from child(1) or from repartitioning a spilled partition). The build rows
// have only been partitioned, we still need to build hash tables over them. Some
// of the partitions could have already been spilled and attempting to build hash
// tables over the non-spilled ones can cause them to spill.
//
// At the end of the function we'd like all partitions to either have a hash table
// (and therefore not spilled) or be spilled. Partitions that have a hash table don't
// need to spill on the probe side.
//
// This maps perfectly to a 0-1 knapsack where the weight is the memory to keep the
// build rows and hash table and the value is the expected IO savings.
// For now, we go with a greedy solution.
//
// TODO: implement the knapsack solution.
Status PartitionedHashJoinNode::BuildHashTables(RuntimeState* state) {
  DCHECK_EQ(hash_partitions_.size(), PARTITION_FANOUT);

  // Decide whether probe filters will be built.
  if (input_partition_ == NULL && can_add_probe_filters_) {
    uint64_t num_build_rows = 0;
    BOOST_FOREACH(Partition* partition, hash_partitions_) {
      const uint64_t partition_num_rows = partition->build_rows()->num_rows();
      num_build_rows += partition_num_rows;
    }
    // TODO: Using this simple heuristic where we compare the number of build rows
    // to the size of the slot filter bitmap. This is essentially not a Bloom filter
    // but a 1-1 filter, and probably it is missing oportunities. Should revisit.
    if (num_build_rows >= state->slot_filter_bitmap_size()) {
      can_add_probe_filters_ = false;
      VLOG(2) << "Disabling probe filter push down because build side is too large: "
              << num_build_rows;
    }
  } else {
    can_add_probe_filters_ = false;
  }

  // First loop over the partitions and build hash tables for the partitions that did
  // not already spill.
  BOOST_FOREACH(Partition* partition, hash_partitions_) {
    if (partition->build_rows()->num_rows() == 0) {
      // This partition is empty, no need to do anything else.
      partition->Close(NULL);
      continue;
    }

    if (!partition->is_spilled()) {
      bool built = false;
      DCHECK(partition->build_rows()->is_pinned());
      RETURN_IF_ERROR(partition->BuildHashTable(state, &built, can_add_probe_filters_));
      // If we did not have enough memory to build this hash table, we need to spill this
      // partition (clean up the hash table, unpin build).
      if (!built) RETURN_IF_ERROR(partition->Spill(true));
    }
  }

  // Collect all the spilled partitions that don't have an IO buffer. We need to reserve
  // an IO buffer for those partitions. Reserving an IO buffer can cause more partitions
  // to spill so this process is recursive.
  list<Partition*> spilled_partitions;
  BOOST_FOREACH(Partition* partition, hash_partitions_) {
    if (partition->is_closed()) continue;
    if (partition->is_spilled() && partition->probe_rows()->using_small_buffers()) {
      spilled_partitions.push_back(partition);
    }
  }
  while (!spilled_partitions.empty()) {
    Partition* partition = spilled_partitions.front();
    spilled_partitions.pop_front();

    while (true) {
      bool got_buffer;
      RETURN_IF_ERROR(partition->probe_rows()->SwitchToIoBuffers(&got_buffer));
      if (got_buffer) break;
      Partition* spilled_partition;
      RETURN_IF_ERROR(SpillPartition(&spilled_partition));
      DCHECK(spilled_partition->is_spilled());
      if (spilled_partition->probe_rows()->using_small_buffers()) {
        spilled_partitions.push_back(spilled_partition);
      }
    }

    DCHECK(partition->probe_rows()->has_write_block());
    DCHECK(!partition->probe_rows()->using_small_buffers());
  }

  // At this point, the partition is in one of these states:
  // 1. closed. All done, no buffers in either the build or probe stream.
  // 2. in_mem. The build side is pinned and has a hash table built.
  // 3. spilled. The build side is fully unpinned and the probe side has an io
  //    sized buffer.
  BOOST_FOREACH(Partition* partition, hash_partitions_) {
    if (partition->hash_tbl() != NULL) partition->probe_rows()->Close();
  }

  // TODO: at this point we could have freed enough memory to pin and build some
  // spilled partitions. This can happen, for example is there is a lot of skew.
  // Partition 1: 10GB (pinned initially).
  // Partition 2,3,4: 1GB (spilled during partitioning the build).
  // In the previous step, we could have unpinned 10GB (because there was not enough
  // memory to build a hash table over it) which can now free enough memory to
  // build hash tables over the remaining 3 partitions.
  // We start by spilling the largest partition though so the build input would have
  // to be pretty pathological.
  // Investigate if this is worthwhile.

  // Initialize the hash_tbl_ caching array.
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    hash_tbls_[i] = hash_partitions_[i]->hash_tbl();
  }
  return Status::OK();
}

Status PartitionedHashJoinNode::EvaluateNullProbe(BufferedTupleStream* build) {
  if (null_probe_rows_ == NULL || null_probe_rows_->num_rows() == 0) {
    return Status::OK();
  }
  DCHECK_EQ(null_probe_rows_->num_rows(), matched_null_probe_.size());

  // Bring both the build and probe side into memory and do a pairwise evaluation.
  bool got_rows;
  scoped_ptr<RowBatch> build_rows;
  RETURN_IF_ERROR(build->GetRows(&build_rows, &got_rows));
  if (!got_rows) return NullAwareAntiJoinError(true);
  scoped_ptr<RowBatch> probe_rows;
  RETURN_IF_ERROR(null_probe_rows_->GetRows(&probe_rows, &got_rows));
  if (!got_rows) return NullAwareAntiJoinError(false);

  ExprContext* const* join_conjunct_ctxs = &other_join_conjunct_ctxs_[0];
  int num_join_conjuncts = other_join_conjunct_ctxs_.size();

  DCHECK_LE(probe_rows->num_rows(), matched_null_probe_.size());
  // For each row, iterate over all rows in the build table.
  SCOPED_TIMER(null_aware_eval_timer_);
  for (int i = 0; i < probe_rows->num_rows(); ++i) {
    if (matched_null_probe_[i]) continue;
    for (int j = 0; j < build_rows->num_rows(); ++j) {
      CreateOutputRow(semi_join_staging_row_, probe_rows->GetRow(i),
          build_rows->GetRow(j));
      if (ExecNode::EvalConjuncts(
            join_conjunct_ctxs, num_join_conjuncts, semi_join_staging_row_)) {
        matched_null_probe_[i] = true;
        break;
      }
    }
  }

  return Status::OK();
}

Status PartitionedHashJoinNode::ReserveTupleStreamBlocks() {
  DCHECK(using_small_buffers_);

  // Partitions that are using small buffers need to switch to IO sized buffers. We want
  // this to happen to all partitions at the same time to ensure that all partitions can
  // get at least 1 io buffer (at which point then can append indefinitely). This
  // initial buffer is guaranteed by the reservation for this operator.
  // TODO: this mechanism sucks. Redo.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->is_closed()) continue;
    DCHECK(hash_partitions_[i]->build_rows()->using_small_buffers());
    DCHECK(hash_partitions_[i]->probe_rows()->using_small_buffers());
    bool got_buffer;
    RETURN_IF_ERROR(hash_partitions_[i]->build_rows()->SwitchToIoBuffers(&got_buffer));
    if (got_buffer) {
      RETURN_IF_ERROR(hash_partitions_[i]->probe_rows()->SwitchToIoBuffers(&got_buffer));
    }
    if (!got_buffer) {
      Status status = Status::MemLimitExceeded();
      status.AddDetail("Not enough memory to get the minimum required buffers for "
          "join.");
      return status;
    }
  }
  using_small_buffers_ = false;
  return Status::OK();
}

Status PartitionedHashJoinNode::CleanUpHashPartitions(RowBatch* batch) {
  DCHECK_EQ(probe_batch_pos_, -1);
  // At this point all the rows have been read from the probe side for all partitions in
  // hash_partitions_.
  VLOG(2) << "Probe Side Consumed\n" << NodeDebugString();

  // Walk the partitions that had hash tables built for the probe phase and close them.
  // In the case of right outer and full outer joins, instead of closing those partitions,
  // add them to the list of partitions that need to output any unmatched build rows.
  // This partition will be closed by the function that actually outputs unmatched build
  // rows.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    Partition* partition = hash_partitions_[i];
    if (partition->is_closed()) continue;
    if (partition->is_spilled()) {
      DCHECK(partition->hash_tbl() == NULL) << NodeDebugString();
      // Unpin the build and probe stream to free up more memory. We need to free all
      // memory so we can recurse the algorithm and create new hash partitions from
      // spilled partitions.
      RETURN_IF_ERROR(partition->build_rows()->UnpinStream(true));
      RETURN_IF_ERROR(partition->probe_rows()->UnpinStream(true));

      // Push new created partitions at the front. This means a depth first walk
      // (more finely partitioned partitions are processed first). This allows us
      // to delete blocks earlier and bottom out the recursion earlier.
      spilled_partitions_.push_front(partition);
    } else {
      DCHECK_EQ(partition->probe_rows()->num_rows(), 0)
        << "No probe rows should have been spilled for this partition.";
      if (join_op_ == TJoinOp::RIGHT_OUTER_JOIN ||
          join_op_ == TJoinOp::RIGHT_ANTI_JOIN ||
          join_op_ == TJoinOp::FULL_OUTER_JOIN) {
        if (output_build_partitions_.empty()) {
          hash_tbl_iterator_ = partition->hash_tbl_->FirstUnmatched(ht_ctx_.get());
        }
        output_build_partitions_.push_back(partition);
      } else if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        // For NAAJ, we need to try to match all the NULL probe rows with this partition
        // before closing it. The NULL probe rows could have come from any partition
        // so we collect them all and match them at the end.
        RETURN_IF_ERROR(EvaluateNullProbe(partition->build_rows()));
        partition->Close(batch);
      } else {
        partition->Close(batch);
      }
    }
  }

  // Just finished evaluating the null probe rows with all the non-spilled build
  // partitions. Unpin this now to free this memory for repartitioning.
  if (null_probe_rows_ != NULL) RETURN_IF_ERROR(null_probe_rows_->UnpinStream());

  hash_partitions_.clear();
  input_partition_ = NULL;
  return Status::OK();
}

void PartitionedHashJoinNode::AddToDebugString(int indent, stringstream* out) const {
  *out << " hash_tbl=";
  *out << string(indent * 2, ' ');
  *out << "HashTbl("
       << " build_exprs=" << Expr::DebugString(build_expr_ctxs_)
       << " probe_exprs=" << Expr::DebugString(probe_expr_ctxs_);
  *out << ")";
}

void PartitionedHashJoinNode::UpdateState(State s) {
  state_ = s;
  VLOG(2) << "Transitioned State:" << endl << NodeDebugString();
}

string PartitionedHashJoinNode::PrintState() const {
  switch (state_) {
    case PARTITIONING_BUILD: return "PartitioningBuild";
    case PROCESSING_PROBE: return "ProcessingProbe";
    case PROBING_SPILLED_PARTITION: return "ProbingSpilledPartitions";
    case REPARTITIONING: return "Repartioning";
    default: DCHECK(false);
  }
  return "";
}

string PartitionedHashJoinNode::NodeDebugString() const {
  stringstream ss;
  ss << "PartitionedHashJoinNode (id=" << id() << " op=" << join_op_
     << " state=" << PrintState()
     << " #partitions=" << hash_partitions_.size()
     << " #spilled_partitions=" << spilled_partitions_.size()
     << ")" << endl;

  for (int i = 0; i < hash_partitions_.size(); ++i) {
    ss << i << ": ptr=" << hash_partitions_[i];
    Partition* partition = hash_partitions_[i];
    if (partition->is_closed()) {
      ss << " closed" << endl;
      continue;
    }
    ss << endl
       << "   "
       << (partition->is_spilled() ? "(Spilled)" : "")
       << " Build Rows: " << partition->build_rows()->num_rows()
       << " (Blocks pinned: " << partition->build_rows()->blocks_pinned() << ")"
       << endl;
    if (partition->hash_tbl() != NULL) {
      ss << "   Hash Table Rows: " << partition->hash_tbl()->size() << endl;
    }
    ss << "   (Spilled) Probe Rows: " << partition->probe_rows()->num_rows()
       << " (Blocks pinned: " << partition->probe_rows()->blocks_pinned() << ")"
       << endl;
  }

  if (!spilled_partitions_.empty()) {
    ss << "SpilledPartitions" << endl;
    for (list<Partition*>::const_iterator it = spilled_partitions_.begin();
        it != spilled_partitions_.end(); ++it) {
      DCHECK((*it)->is_spilled());
      DCHECK((*it)->hash_tbl() == NULL);
      ss << "  Partition=" << *it << endl
        << "   Spilled Build Rows: "
        << (*it)->build_rows()->num_rows() << endl
        << "   Spilled Probe Rows: "
        << (*it)->probe_rows()->num_rows() << endl;
    }
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
Function* PartitionedHashJoinNode::CodegenCreateOutputRow(LlvmCodeGen* codegen) {
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  DCHECK(tuple_row_type != NULL);
  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);

  Type* this_type = codegen->GetType(BlockingJoinNode::LLVM_CLASS_NAME);
  DCHECK(this_type != NULL);
  PointerType* this_ptr_type = PointerType::get(this_type, 0);

  // TupleRows are really just an array of pointers.  Easier to work with them
  // this way.
  PointerType* tuple_row_working_type = PointerType::get(codegen->ptr_type(), 0);

  // Construct function signature to match CreateOutputRow()
  LlvmCodeGen::FnPrototype prototype(codegen, "CreateOutputRow", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("out_arg", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("probe_arg", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("build_arg", tuple_row_ptr_type));

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Value* args[4];
  Function* fn = prototype.GeneratePrototype(&builder, args);
  Value* out_row_arg = builder.CreateBitCast(args[1], tuple_row_working_type, "out");
  Value* probe_row_arg = builder.CreateBitCast(args[2], tuple_row_working_type, "probe");
  Value* build_row_arg = builder.CreateBitCast(args[3], tuple_row_working_type, "build");

  int num_probe_tuples = child(0)->row_desc().tuple_descriptors().size();
  int num_build_tuples = child(1)->row_desc().tuple_descriptors().size();

  // Copy probe row
  codegen->CodegenMemcpy(&builder, out_row_arg, probe_row_arg, probe_tuple_row_size_);
  Value* build_row_idx[] = { codegen->GetIntConstant(TYPE_INT, num_probe_tuples) };
  Value* build_row_dst = builder.CreateGEP(out_row_arg, build_row_idx, "build_dst_ptr");

  // Copy build row.
  BasicBlock* build_not_null_block = BasicBlock::Create(context, "build_not_null", fn);
  BasicBlock* build_null_block = NULL;

  if (join_op_ == TJoinOp::LEFT_ANTI_JOIN || join_op_ == TJoinOp::LEFT_OUTER_JOIN ||
      join_op_ == TJoinOp::FULL_OUTER_JOIN ||
      join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    // build tuple can be null
    build_null_block = BasicBlock::Create(context, "build_null", fn);
    Value* is_build_null = builder.CreateIsNull(build_row_arg, "is_build_null");
    builder.CreateCondBr(is_build_null, build_null_block, build_not_null_block);

    // Set tuple build ptrs to NULL
    // TODO: this should be replaced with memset() but I can't get the llvm intrinsic
    // to work.
    builder.SetInsertPoint(build_null_block);
    for (int i = 0; i < num_build_tuples; ++i) {
      Value* array_idx[] =
          { codegen->GetIntConstant(TYPE_INT, i + num_probe_tuples) };
      Value* dst = builder.CreateGEP(out_row_arg, array_idx, "dst_tuple_ptr");
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

  return codegen->FinalizeFunction(fn);
}

bool PartitionedHashJoinNode::CodegenProcessBuildBatch(
    RuntimeState* state, Function* hash_fn, Function* murmur_hash_fn) {
  LlvmCodeGen* codegen;
  if (!state->GetCodegen(&codegen).ok()) return false;
  // Get cross compiled function
  Function* process_build_batch_fn =
      codegen->GetFunction(IRFunction::PHJ_PROCESS_BUILD_BATCH);
  DCHECK(process_build_batch_fn != NULL);

  // Codegen for evaluating build rows
  Function* eval_row_fn = ht_ctx_->CodegenEvalRow(state, true);
  if (eval_row_fn == NULL) return false;

  int replaced = 0;
  // Replace call sites
  process_build_batch_fn = codegen->ReplaceCallSites(process_build_batch_fn, false,
      eval_row_fn, "EvalBuildRow", &replaced);
  DCHECK_EQ(replaced, 1);

  // process_build_batch_fn_level0 uses CRC hash if available,
  // process_build_batch_fn uses murmur
  Function* process_build_batch_fn_level0 = codegen->ReplaceCallSites(
      process_build_batch_fn, false, hash_fn, "HashCurrentRow", &replaced);
  DCHECK_EQ(replaced, 1);

  process_build_batch_fn = codegen->ReplaceCallSites(
      process_build_batch_fn, true, murmur_hash_fn, "HashCurrentRow", &replaced);
  DCHECK_EQ(replaced, 1);

  // Finalize ProcessBuildBatch functions
  process_build_batch_fn = codegen->OptimizeFunctionWithExprs(process_build_batch_fn);
  if (process_build_batch_fn == NULL) return false;
  process_build_batch_fn_level0 =
      codegen->OptimizeFunctionWithExprs(process_build_batch_fn_level0);
  if (process_build_batch_fn_level0 == NULL) return false;

  // Register native function pointers
  codegen->AddFunctionToJit(process_build_batch_fn,
                            reinterpret_cast<void**>(&process_build_batch_fn_));
  codegen->AddFunctionToJit(process_build_batch_fn_level0,
                            reinterpret_cast<void**>(&process_build_batch_fn_level0_));
  return true;
}

bool PartitionedHashJoinNode::CodegenProcessProbeBatch(
    RuntimeState* state, Function* hash_fn, Function* murmur_hash_fn) {
  LlvmCodeGen* codegen;
  if (!state->GetCodegen(&codegen).ok()) return false;

  // Get cross compiled function
  IRFunction::Type ir_fn;
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
      return false;
  }
  Function* process_probe_batch_fn = codegen->GetFunction(ir_fn);
  DCHECK(process_probe_batch_fn != NULL);

  // Clone process_probe_batch_fn so we don't clobber the original for other join nodes
  process_probe_batch_fn = codegen->CloneFunction(process_probe_batch_fn);
  process_probe_batch_fn->setName("ProcessProbeBatch");

  // Since ProcessProbeBatch() is a templated function, it has linkonce_odr linkage, which
  // means the function can be removed if it's not referenced. Change to weak_odr, which
  // has the same semantics except it can't be removed.
  // See http://llvm.org/docs/LangRef.html#linkage-types
  DCHECK(process_probe_batch_fn->getLinkage() == GlobalValue::LinkOnceODRLinkage)
      << LlvmCodeGen::Print(process_probe_batch_fn);
  process_probe_batch_fn->setLinkage(GlobalValue::WeakODRLinkage);

  // Bake in %this pointer argument to process_probe_batch_fn.
  Value* this_arg = codegen->GetArgument(process_probe_batch_fn, 0);
  Value* this_loc = codegen->CastPtrToLlvmPtr(this_arg->getType(), this);
  this_arg->replaceAllUsesWith(this_loc);

  // Bake in %ht_ctx pointer argument to process_probe_batch_fn
  Value* ht_ctx_arg = codegen->GetArgument(process_probe_batch_fn, 2);
  Value* ht_ctx_loc = codegen->CastPtrToLlvmPtr(ht_ctx_arg->getType(), ht_ctx_.get());
  ht_ctx_arg->replaceAllUsesWith(ht_ctx_loc);

  // Codegen HashTable::Equals
  Function* equals_fn = ht_ctx_->CodegenEquals(state);
  if (equals_fn == NULL) return false;

  // Codegen for evaluating probe rows
  Function* eval_row_fn = ht_ctx_->CodegenEvalRow(state, false);
  if (eval_row_fn == NULL) return false;

  // Codegen CreateOutputRow
  Function* create_output_row_fn = CodegenCreateOutputRow(codegen);
  if (create_output_row_fn == NULL) return false;

  // Codegen evaluating other join conjuncts
  Function* eval_other_conjuncts_fn = ExecNode::CodegenEvalConjuncts(
      state, other_join_conjunct_ctxs_, "EvalOtherConjuncts");
  if (eval_other_conjuncts_fn == NULL) return false;

  // Codegen evaluating conjuncts
  Function* eval_conjuncts_fn = ExecNode::CodegenEvalConjuncts(state, conjunct_ctxs_);
  if (eval_conjuncts_fn == NULL) return false;

  // Replace all call sites with codegen version
  int replaced = 0;
  process_probe_batch_fn = codegen->ReplaceCallSites(process_probe_batch_fn, true,
      eval_row_fn, "EvalProbeRow", &replaced);
  DCHECK_EQ(replaced, 1);

  process_probe_batch_fn = codegen->ReplaceCallSites(process_probe_batch_fn, true,
      create_output_row_fn, "CreateOutputRow", &replaced);
  DCHECK(replaced == 1 || replaced == 2) << replaced; // Depends on join_op_

  process_probe_batch_fn = codegen->ReplaceCallSites(process_probe_batch_fn, true,
      eval_conjuncts_fn, "EvalConjuncts", &replaced);
  // Depends on join_op_:
  // INNER_JOIN -> 1
  // LEFT_OUTER_JOIN -> 2
  // LEFT_SEMI_JOIN -> 1
  // LEFT_ANTI_JOIN/NULL_AWARE_ANTI_JOIN -> 2
  // RIGHT_OUTER_JOIN -> 1
  // RIGHT_SEMI_JOIN -> 1
  // RIGHT_ANTI_JOIN -> 0
  // FULL_OUTER_JOIN -> 2
  // CROSS_JOIN -> N/A
  DCHECK(replaced == 0 || replaced == 1 || replaced == 2) << replaced;

  process_probe_batch_fn = codegen->ReplaceCallSites(process_probe_batch_fn, true,
      eval_other_conjuncts_fn, "EvalOtherJoinConjuncts", &replaced);
  DCHECK_EQ(replaced, 1);

  process_probe_batch_fn = codegen->ReplaceCallSites(process_probe_batch_fn, true,
      equals_fn, "Equals", &replaced);
  // Depends on join_op_
  DCHECK(replaced == 1 || replaced == 2 || replaced == 3 || replaced == 4) << replaced;

  // process_probe_batch_fn_level0 uses CRC hash if available,
  // process_probe_batch_fn uses murmur
  Function* process_probe_batch_fn_level0 = codegen->ReplaceCallSites(
      process_probe_batch_fn, false, hash_fn, "HashCurrentRow", &replaced);
  DCHECK_EQ(replaced, 1);

  process_probe_batch_fn = codegen->ReplaceCallSites(
      process_probe_batch_fn, true, murmur_hash_fn, "HashCurrentRow", &replaced);
  DCHECK_EQ(replaced, 1);

  // Finalize ProcessProbeBatch functions
  process_probe_batch_fn = codegen->OptimizeFunctionWithExprs(process_probe_batch_fn);
  if (process_probe_batch_fn == NULL) return false;
  process_probe_batch_fn_level0 =
      codegen->OptimizeFunctionWithExprs(process_probe_batch_fn_level0);
  if (process_probe_batch_fn_level0 == NULL) return false;

  // Register native function pointers
  codegen->AddFunctionToJit(process_probe_batch_fn,
                            reinterpret_cast<void**>(&process_probe_batch_fn_));
  codegen->AddFunctionToJit(process_probe_batch_fn_level0,
                            reinterpret_cast<void**>(&process_probe_batch_fn_level0_));
  return true;
}
