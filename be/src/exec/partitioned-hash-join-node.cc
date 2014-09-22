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

DEFINE_bool(enable_phj_probe_side_filtering, true,
    "Enables pushing PHJ build side filters to probe side");

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;

PartitionedHashJoinNode::PartitionedHashJoinNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : BlockingJoinNode("PartitionedHashJoinNode", tnode.hash_join_node.join_op,
        pool, tnode, descs),
    state_(PARTITIONING_BUILD),
    block_mgr_client_(NULL),
    process_build_batch_fn_(NULL),
    process_build_batch_fn_level0_(NULL),
    process_probe_batch_fn_(NULL),
    process_probe_batch_fn_level0_(NULL),
    input_partition_(NULL),
    null_aware_partition_(NULL) {
  memset(hash_tbls_, 0, sizeof(hash_tbls_));
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

  if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    DCHECK_EQ(eq_join_conjuncts.size(), 1);
  }
  return Status::OK;
}

Status PartitionedHashJoinNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(BlockingJoinNode::Prepare(state));

  // build and probe exprs are evaluated in the context of the rows produced by our
  // right and left children, respectively
  RETURN_IF_ERROR(Expr::Prepare(build_expr_ctxs_, state, child(1)->row_desc()));
  RETURN_IF_ERROR(Expr::Prepare(probe_expr_ctxs_, state, child(0)->row_desc()));

  // other_join_conjunct_ctxs_ are evaluated in the context of the rows produced by this
  // node
  RETURN_IF_ERROR(Expr::Prepare(other_join_conjunct_ctxs_, state, row_descriptor_));

  // We need two output buffer per partition (one for build and one for probe) and
  // and one additional buffer either for the input (while repartitioning).
  // TODO: with more careful reasoning we can turn this to 1 per partition I think.
  int num_reserved_buffers = PARTITION_FANOUT * 2 + 1;
  if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    // We need to maintain two additional buffers in this case to store the build/probe
    // on the null aware partition.
    num_reserved_buffers += 2;
  }
  RETURN_IF_ERROR(state->block_mgr()->RegisterClient(
      num_reserved_buffers, mem_tracker(), state, &block_mgr_client_));

  bool should_store_nulls = join_op_ == TJoinOp::RIGHT_OUTER_JOIN ||
      join_op_ == TJoinOp::RIGHT_ANTI_JOIN || join_op_ == TJoinOp::FULL_OUTER_JOIN;
  ht_ctx_.reset(new HashTableCtx(build_expr_ctxs_, probe_expr_ctxs_,
      should_store_nulls, false, state->fragment_hash_seed(), MAX_PARTITION_DEPTH,
      child(1)->row_desc().tuple_descriptors().size()));

  if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    null_aware_partition_ = pool_->Add(new Partition(state, this, 0));
    RETURN_IF_ERROR(null_aware_partition_->build_rows()->Init(runtime_profile(), false));
    RETURN_IF_ERROR(null_aware_partition_->probe_rows()->Init(runtime_profile(), false));
  }

  partition_build_timer_ = ADD_TIMER(runtime_profile(), "BuildPartitionTime");
  num_hash_buckets_ =
      ADD_COUNTER(runtime_profile(), "HashBuckets", TCounterType::UNIT);
  partitions_created_ =
      ADD_COUNTER(runtime_profile(), "PartitionsCreated", TCounterType::UNIT);
  max_partition_level_ = runtime_profile()->AddHighWaterMarkCounter(
      "MaxPartitionLevel", TCounterType::UNIT);
  num_build_rows_partitioned_ =
      ADD_COUNTER(runtime_profile(), "BuildRowsPartitioned", TCounterType::UNIT);
  num_probe_rows_partitioned_ =
      ADD_COUNTER(runtime_profile(), "ProbeRowsPartitioned", TCounterType::UNIT);
  num_repartitions_ =
      ADD_COUNTER(runtime_profile(), "NumRepartitions", TCounterType::UNIT);
  num_spilled_partitions_ =
      ADD_COUNTER(runtime_profile(), "SpilledPartitions", TCounterType::UNIT);
  largest_partition_percent_ = runtime_profile()->AddHighWaterMarkCounter(
      "LargestPartitionPercent", TCounterType::UNIT);

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

  return Status::OK;
}

void PartitionedHashJoinNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (ht_ctx_.get() != NULL) ht_ctx_->Close();
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    hash_partitions_[i]->Close(NULL);
  }
  for (list<Partition*>::iterator it = spilled_partitions_.begin();
      it != spilled_partitions_.end(); ++it) {
    (*it)->Close(NULL);
  }
  if (input_partition_ != NULL) input_partition_->Close(NULL);
  if (null_aware_partition_ != NULL) null_aware_partition_->Close(NULL);
  nulls_build_batch_.reset();

  if (block_mgr_client_ != NULL) {
    state->block_mgr()->LowerBufferReservation(block_mgr_client_, 0);
  }
  Expr::Close(build_expr_ctxs_, state);
  Expr::Close(probe_expr_ctxs_, state);
  Expr::Close(other_join_conjunct_ctxs_, state);
  BlockingJoinNode::Close(state);
}

PartitionedHashJoinNode::Partition::Partition(RuntimeState* state,
        PartitionedHashJoinNode* parent, int level)
  : parent_(parent),
    is_closed_(false),
    is_spilled_(false),
    level_(level),
    build_rows_(state->obj_pool()->Add(new BufferedTupleStream(
        state, parent_->child(1)->row_desc(), state->block_mgr(),
        parent_->block_mgr_client_))),
    probe_rows_(state->obj_pool()->Add(new BufferedTupleStream(
        state, parent_->child(0)->row_desc(),
        state->block_mgr(), parent_->block_mgr_client_))) {
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
  // First pin the entire build stream in memory.
  RETURN_IF_ERROR(build_rows_->PinStream(built));
  if (!*built) return Status::OK;
  RETURN_IF_ERROR(build_rows_->PrepareForRead());

  // Allocate the partition-local hash table.
  hash_tbl_.reset(new HashTable(state, parent_->block_mgr_client_,
      parent_->child(1)->row_desc().tuple_descriptors().size(), build_rows()));
  if (!hash_tbl_->Init()) {
    *built = false;
    hash_tbl_.reset();
    return Status::OK;
  }

  // TODO: move the batch and indices as members to avoid reallocating.
  bool eos = false;
  RowBatch batch(parent_->child(1)->row_desc(), state->batch_size(),
      parent_->mem_tracker());
  HashTableCtx* ctx = parent_->ht_ctx_.get();
  vector<BufferedTupleStream::RowIdx> indices;
  while (!eos) {
    RETURN_IF_ERROR(build_rows_->GetNext(&batch, &eos, &indices));
    DCHECK_EQ(batch.num_rows(), indices.size());
    SCOPED_TIMER(parent_->build_timer_);
    for (int i = 0; i < batch.num_rows(); ++i) {
      TupleRow* row = batch.GetRow(i);
      uint32_t hash = 0;
      if (!ctx->EvalAndHashBuild(row, &hash)) continue;
      // TODO: If we are going to AddProbeFilters we should do it here.
      if (UNLIKELY(!hash_tbl_->Insert(ctx, indices[i], row, hash))) {
        *built = false;
        break;
      }
    }
    batch.Reset();

    if (!*built) return Status::OK;
  }
  DCHECK(*built);
  DCHECK_NOTNULL(hash_tbl_.get());
  is_spilled_ = false;
  COUNTER_ADD(parent_->num_hash_buckets_, hash_tbl_->num_buckets());

  // TODO: We build the filters after we constructed the hash table.  This is what the
  // old (HashJoinNode) code was doing.  We can do better, because now we know a priori
  // whether we are going to add probe filter or not. For example, we can be building
  // the filters while we are streaming the rows from the batch, and not at the end after
  // the HT has been built.
  if (AddProbeFilters) {
    DCHECK_EQ(level_, 0) << "Should not add filters if repartitioning";
    hash_tbl_->UpdateProbeFilters(ctx, parent_->probe_filters_);
  }
  return Status::OK;
}

bool PartitionedHashJoinNode::AllocateProbeFilters(RuntimeState* state) {
  if (!can_add_probe_filters_) return false;
  DCHECK_NOTNULL(ht_ctx_.get());
  DCHECK_EQ(build_expr_ctxs_.size(), probe_expr_ctxs_.size());
  probe_filters_.resize(probe_expr_ctxs_.size());
  for (int i = 0; i < build_expr_ctxs_.size(); ++i) {
    if (probe_expr_ctxs_[i]->root()->is_slotref()) {
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
      if (probe_filters_[i].second != NULL) {
        delete probe_filters_[i].second;
        probe_filters_[i].second = NULL;
      }
    }
    return false;
  }
}

// TODO: can we do better with the spilling heuristic.
Status PartitionedHashJoinNode::SpillPartition() {
  int64_t max_freed_mem = 0;
  int partition_idx = -1;

  // Iterate over the partitions and pick the largest partition to spill.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->is_closed()) continue;
    if (hash_partitions_[i]->is_spilled()) continue;
    int64_t mem = hash_partitions_[i]->build_rows()->bytes_in_mem(false);
    if (hash_partitions_[i]->hash_tbl() != NULL) {
      mem += hash_partitions_[i]->hash_tbl()->byte_size();
    }
    if (mem > max_freed_mem) {
      max_freed_mem = mem;
      partition_idx = i;
    }
  }

  if (partition_idx == -1) {
    // Could not find a partition to spill. This means the mem limit was just too
    // low. e.g. mem_limit too small that we can't put a buffer in front of each
    // partition.
    Status status = Status::MEM_LIMIT_EXCEEDED;
    status.AddErrorMsg("Mem limit is too low to perform partitioned join. We do not "
        "have enough memory to maintain a buffer per partition.");
    return status;
  }
  VLOG(2) << "Spilling partition: " << partition_idx << endl << DebugString();
  RETURN_IF_ERROR(hash_partitions_[partition_idx]->Spill(false));
  DCHECK(hash_partitions_[partition_idx]->probe_rows()->has_write_block());
  hash_tbls_[partition_idx] = NULL;
  return Status::OK;
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
  return Status::OK;
}

Status PartitionedHashJoinNode::ProcessBuildInput(RuntimeState* state, int level) {
  if (level >= MAX_PARTITION_DEPTH) {
    Status status = Status::MEM_LIMIT_EXCEEDED;
    status.AddErrorMsg("Cannot perform hash aggregation. Partitioned input data too many"
       " times. This could mean there is too much skew in the data or the memory"
       " limit is set too low.");
    state->SetMemLimitExceeded();
    return status;
  }

  DCHECK(hash_partitions_.empty());
  if (input_partition_ != NULL) {
    DCHECK(input_partition_->build_rows() != NULL);
    DCHECK_EQ(input_partition_->build_rows()->blocks_pinned(), 0) << DebugString();
    DCHECK_EQ(input_partition_->build_rows()->blocks_pinned(), 0) << DebugString();
    RETURN_IF_ERROR(input_partition_->build_rows()->PrepareForRead());
  }

  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    hash_partitions_.push_back(pool_->Add(new Partition(state, this, level)));
    RETURN_IF_ERROR(hash_partitions_[i]->build_rows()->Init(runtime_profile()));

    // Initialize a buffer for the probe here to make sure why have it if we
    // need it. While this is not strictly necessary (there are some cases where we
    // won't need this buffer), the benefit is low. Not grabbing this buffer means
    // there is an additional buffer that could be used for the build side. However
    // since this is only one buffer, there is only a small range of build input
    // sizes where this is beneficial (an IO buffer size). It makes the logic
    // much more complex to enable this optimization.
    RETURN_IF_ERROR(hash_partitions_[i]->probe_rows()->Init(runtime_profile(), false));
  }
  COUNTER_ADD(partitions_created_, PARTITION_FANOUT);
  COUNTER_SET(max_partition_level_, level);

  RowBatch build_batch(child(1)->row_desc(), state->batch_size(), mem_tracker());
  bool eos = false;
  int64_t total_build_rows = 0;
  while (!eos) {
    RETURN_IF_ERROR(state->CheckQueryState());
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
      DCHECK_NOTNULL(process_build_batch_fn_level0_);
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
  VLOG_QUERY << ss.str();

  COUNTER_ADD(num_build_rows_partitioned_, total_build_rows);
  RETURN_IF_ERROR(BuildHashTables(state));
  return Status::OK;
}

Status PartitionedHashJoinNode::InitGetNext(TupleRow* first_probe_row) {
  // TODO: Move this reset to blocking-join. Not yet though because of hash-join.
  ResetForProbe();
  return Status::OK;
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
      return Status::OK;
    }
    if (probe_side_eos_) {
      current_probe_row_ = NULL;
      probe_batch_pos_ = -1;
      return Status::OK;
    }
    RETURN_IF_ERROR(child(0)->GetNext(state, probe_batch_.get(), &probe_side_eos_));
    COUNTER_ADD(probe_row_counter_, probe_batch_->num_rows());
  } while (probe_batch_->num_rows() == 0);

  ResetForProbe();
  return Status::OK;
}

Status PartitionedHashJoinNode::NextSpilledProbeRowBatch(
    RuntimeState* state, RowBatch* out_batch) {
  DCHECK(input_partition_ != NULL);
  probe_batch_->TransferResourceOwnership(out_batch);
  if (out_batch->AtCapacity()) {
    // The out_batch has resources associated with it that will be recycled on the
    // next call to GetNext() on the probe stream. Return this batch now.
    probe_batch_pos_ = -1;
    return Status::OK;
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
      DCHECK(input_partition_->hash_tbl_.get() != NULL);
      hash_tbl_iterator_ = input_partition_->hash_tbl_->FirstUnmatched(ht_ctx_.get());
      output_build_partitions_.push_back(input_partition_);
    } else {
      // In any other case, just close the input partition.
      input_partition_->Close(out_batch);
      input_partition_ = NULL;
    }
    current_probe_row_ = NULL;
    probe_batch_pos_ = -1;
  }
  return Status::OK;
}

Status PartitionedHashJoinNode::PrepareNextPartition(RuntimeState* state) {
  DCHECK(input_partition_ == NULL);
  if (spilled_partitions_.empty()) return Status::OK;
  VLOG(2) << "PrepareNextPartition\n" << DebugString();

  input_partition_ = spilled_partitions_.front();
  spilled_partitions_.pop_front();
  DCHECK(input_partition_->is_spilled());

  // Reserve one buffer to read the probe side.
  RETURN_IF_ERROR(input_partition_->probe_rows()->PrepareForRead());
  ht_ctx_->set_level(input_partition_->level_);

  int64_t mem_limit = mem_tracker()->SpareCapacity();
  // Try to build a hash table on top the spilled build rows.
  bool built = false;
  if (input_partition_->EstimatedInMemSize() < mem_limit) {
    ht_ctx_->set_level(input_partition_->level_);
    // TODO: We disable filter on spilled partitions, but perhaps we can revisit
    // this, especially if the probe side is very big (e.g. has spilled as well).
    RETURN_IF_ERROR(input_partition_->BuildHashTable(state, &built, false));
  }

  if (!built) {
    // This build partition still does not fit in memory, repartition.
    DCHECK(input_partition_->is_spilled());
    input_partition_->Spill(false);
    ht_ctx_->set_level(input_partition_->level_ + 1);
    RETURN_IF_ERROR(ProcessBuildInput(state, input_partition_->level_ + 1));
    UpdateState(REPARTITIONING);
  } else {
    DCHECK(hash_partitions_.empty());
    DCHECK(!input_partition_->is_spilled());
    DCHECK_NOTNULL(input_partition_->hash_tbl());
    // In this case, we did not have to partition the build again, we just built
    // a hash table. This means the probe does not have to be partitioned either.
    for (int i = 0; i < PARTITION_FANOUT; ++i) {
      hash_tbls_[i] = input_partition_->hash_tbl();
    }
    UpdateState(PROBING_SPILLED_PARTITION);
  }

  COUNTER_ADD(num_repartitions_, 1);
  COUNTER_ADD(num_probe_rows_partitioned_, input_partition_->probe_rows()->num_rows());
  return Status::OK;
}

Status PartitionedHashJoinNode::GetNext(RuntimeState* state, RowBatch* out_batch,
    bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  DCHECK(!out_batch->AtCapacity());

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK;
  } else {
    *eos = false;
  }

  while (true) {
    DCHECK_NE(state_, PARTITIONING_BUILD) << "Should not be in GetNext()";
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->CheckQueryState());

    if ((join_op_ == TJoinOp::RIGHT_OUTER_JOIN || join_op_ == TJoinOp::RIGHT_ANTI_JOIN ||
         join_op_ == TJoinOp::FULL_OUTER_JOIN) &&
        !output_build_partitions_.empty())  {
      // In case of right-outer, right-anti and full-outer joins, flush the remaining
      // unmatched build rows of any partition we are done processing, before processing
      // the next batch.
      RETURN_IF_ERROR(OutputUnmatchedBuild(out_batch));
      if (!output_build_partitions_.empty()) break;

      // Finished to output unmatched build rows, move to next partition.
      DCHECK(hash_partitions_.empty());
      RETURN_IF_ERROR(PrepareNextPartition(state));
      if (input_partition_ == NULL) {
        *eos = true;
        break;
      }
    }

    if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
      // In this case, we want to output rows from the null aware partition.
      if (null_aware_partition_ == NULL) {
        *eos = true;
        break;
      }
      if (nulls_build_batch_.get() != NULL) {
        RETURN_IF_ERROR(OutputNullAwareProbeRows(state, out_batch));
        if (out_batch->AtCapacity()) return Status::OK;
        continue;
      }
    }

    // Finish up the current batch.
    {
      // Putting SCOPED_TIMER in ProcessProbeBatch() causes weird exception handling IR in
      // the xcompiled function, so call it here instead.
      int rows_added = 0;
      SCOPED_TIMER(probe_timer_);
      if (process_probe_batch_fn_ == NULL || ht_ctx_->level() != 0) {
        rows_added = ProcessProbeBatch(join_op_, out_batch, ht_ctx_.get());
      } else {
        DCHECK_NOTNULL(process_probe_batch_fn_level0_);
        if (ht_ctx_->level() == 0) {
          rows_added = process_probe_batch_fn_level0_(this, out_batch, ht_ctx_.get());
        } else {
          rows_added = process_probe_batch_fn_(this, out_batch, ht_ctx_.get());
        }
      }
      if (UNLIKELY(rows_added < 0)) {
        DCHECK(!status_.ok());
        return status_;
      }
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
      *eos = null_aware_partition_ == NULL;
      if (*eos) break;
    }
  }

  if (ReachedLimit()) *eos = true;
  return Status::OK;
}

Status PartitionedHashJoinNode::OutputUnmatchedBuild(RowBatch* out_batch) {
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
    if (!hash_tbl_iterator_.matched()) {
      hash_tbl_iterator_.set_matched(true);
      TupleRow* build_row = hash_tbl_iterator_.GetRow();
      DCHECK(build_row != NULL);
      CreateOutputRow(out_row, NULL, build_row);
      if (ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
        ++num_rows_added;
        out_row = out_row->next_row(out_batch);
      }
    }
    // Move to the next unmatched entry
    hash_tbl_iterator_.NextUnmatched();
  }

  // If we reached the end of the hash table, then there are no other unmatched build
  // rows for this partition. In that case we need to close the partition, and move to the
  // next. If we have not reached the end of the hash table, it means that we reached
  // out_batch capacity and we need to continue to output unmatched build rows, without
  // closing the partition.
  if (hash_tbl_iterator_.AtEnd()) {
    output_build_partitions_.front()->Close(out_batch);
    output_build_partitions_.pop_front();
    // Move to the next partition to output unmatched rows.
    if (!output_build_partitions_.empty()) {
      hash_tbl_iterator_ =
          output_build_partitions_.front()->hash_tbl_->FirstUnmatched(ht_ctx_.get());
    }
  }

  DCHECK_LE(num_rows_added, max_rows);
  out_batch->CommitRows(num_rows_added);
  num_rows_returned_ += num_rows_added;
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK;
}

Status PartitionedHashJoinNode::PrepareNullAwarePartition() {
  DCHECK_NOTNULL(null_aware_partition_);
  DCHECK(nulls_build_batch_.get() == NULL);
  DCHECK_EQ(probe_batch_pos_, -1);
  DCHECK_EQ(probe_batch_->num_rows(), 0);

  BufferedTupleStream* build_stream = null_aware_partition_->build_rows();
  BufferedTupleStream* probe_stream = null_aware_partition_->probe_rows();

  if (build_stream->num_rows() == 0) {
    // There were no build rows. Nothing to do. Just close this partition.
    DCHECK_EQ(probe_stream->num_rows(), 0);
    null_aware_partition_->Close(NULL);
    null_aware_partition_ = NULL;
    nulls_build_batch_.reset();
    return Status::OK;
  }
  // Bring the entire spilled build stream into memory.
  bool pinned;
  RETURN_IF_ERROR(build_stream->PinStream(&pinned));
  if (!pinned) {
    // In this case we had a lot of NULLs on the build side. While this is possible
    // to process by re-reading the spilled build stream for each probe row with
    // minimal code effort, this would behave very slowly (we'd need to do IO for
    // each probe row). This seems like a reasonable limitation for now.
    // TODO: revisit
    return Status("Too many NULLs on the build side to perform this join.");
  }

  // Initialize the streams for read.
  RETURN_IF_ERROR(build_stream->PrepareForRead());
  RETURN_IF_ERROR(probe_stream->PrepareForRead());

  // Read all the build rows into a single batch.
  nulls_build_batch_.reset(new RowBatch(
      child(1)->row_desc(), build_stream->num_rows(), mem_tracker()));
  bool eos = false;
  RETURN_IF_ERROR(build_stream->GetNext(nulls_build_batch_.get(), &eos));

  probe_batch_pos_ = 0;
  return Status::OK;
}

Status PartitionedHashJoinNode::OutputNullAwareProbeRows(RuntimeState* state,
    RowBatch* out_batch) {
  DCHECK_NOTNULL(null_aware_partition_);
  DCHECK(nulls_build_batch_.get() != NULL);

  BufferedTupleStream* probe_stream = null_aware_partition_->probe_rows();
  if (probe_batch_pos_ == probe_batch_->num_rows()) {
    probe_batch_pos_ = 0;
    probe_batch_->TransferResourceOwnership(out_batch);
    if (out_batch->AtCapacity()) return Status::OK;

    // Get the next probe batch.
    bool eos;
    RETURN_IF_ERROR(probe_stream->GetNext(probe_batch_.get(), &eos));
    if (probe_batch_->num_rows() == 0) {
      // No more probe, all done.
      null_aware_partition_->Close(out_batch);
      null_aware_partition_ = NULL;
      nulls_build_batch_.reset();
      return Status::OK;
    }
  }

  ExprContext* const* join_conjunct_ctxs = &other_join_conjunct_ctxs_[0];
  int num_join_conjuncts = other_join_conjunct_ctxs_.size();
  DCHECK_NOTNULL(probe_batch_.get());
  DCHECK_GT(num_join_conjuncts, 0);

  // For each probe row, iterate over all the build rows and check for rows
  // that did not have any matches.
  for (; probe_batch_pos_ < probe_batch_->num_rows(); ++probe_batch_pos_) {
    if (out_batch->AtCapacity()) break;
    TupleRow* out_row = out_batch->GetRow(out_batch->AddRow());
    TupleRow* probe_row = probe_batch_->GetRow(probe_batch_pos_);

    bool matched = false;
    for (int i = 0; i < nulls_build_batch_->num_rows(); ++i) {
      CreateOutputRow(out_row, probe_row, nulls_build_batch_->GetRow(i));
      if (ExecNode::EvalConjuncts(join_conjunct_ctxs, num_join_conjuncts, out_row)) {
        matched = true;
        break;
      }
    }

    if (!matched) {
      CreateOutputRow(out_row, probe_row, NULL);
      out_batch->CommitLastRow();
    }
  }
  return Status::OK;
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
    // TODO: Should we just give up in case we have any spilled partition?
    uint64_t num_build_rows = 0;
    BOOST_FOREACH(Partition* partition, hash_partitions_) {
      const uint64_t partition_num_rows = partition->build_rows()->num_rows();
      num_build_rows += partition_num_rows;
    }
    // TODO: Using this simple heuristic where we compare the number of build rows
    // to the size of the slot filter bitmap. This is essentially not a Bloom filter
    // but a 1-1 filter, and probably it is missing oportunities. Should revisit.
    can_add_probe_filters_ = (num_build_rows < state->slot_filter_bitmap_size());
    if (can_add_probe_filters_) {
      AddRuntimeExecOption("Build-Side Filter Pushed Down");
    } else {
      VLOG(2) << "Disabling probe filter push down because build side is too large: "
              << num_build_rows;
    }
  } else {
    can_add_probe_filters_ = false;
  }

  // First loop over the partitions and build hash tables for the partitions that
  // didn't already spill.
  BOOST_FOREACH(Partition* partition, hash_partitions_) {
    if (partition->build_rows()->num_rows() == 0) {
      // This partition is empty, no need to do anything else.
      partition->Close(NULL);
      continue;
    }

    bool built = false;
    if (!partition->is_spilled()) {
      DCHECK(partition->build_rows()->is_pinned());
      RETURN_IF_ERROR(partition->BuildHashTable(state, &built, can_add_probe_filters_));
    }

    if (built) {
      // This partition's build is in memory and has a hash table, we won't be needing
      // the probe stream anymore.
      partition->probe_rows()->Close();
    } else {
      RETURN_IF_ERROR(partition->Spill(true));
      DCHECK(partition->probe_rows()->has_write_block());
    }
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

  return Status::OK;
}

Status PartitionedHashJoinNode::CleanUpHashPartitions(RowBatch* batch) {
  DCHECK_EQ(probe_batch_pos_, -1);
  // At this point all the rows have been read from the probe side for all partitions in
  // hash_partitions_.
  VLOG(2) << "Probe Side Consumed\n" << DebugString();

  // Walk the partitions that had hash tables built for the probe phase and close them.
  // In the case of right outer and full outer joins, instead of closing those partitions,
  // add them to the list of partitions that need to output any unmatched build rows.
  // This partition will be closed by the function that actually outputs unmatched build
  // rows.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    Partition* partition = hash_partitions_[i];
    if (partition->is_closed()) continue;
    if (partition->is_spilled()) {
      DCHECK(partition->hash_tbl() == NULL) << DebugString();
      // Unpin the build and probe stream to free up more memory. We need to free all
      // memory so we can recurse the algorithm and create new hash partitions from
      // spilled partitions.
      RETURN_IF_ERROR(partition->build_rows()->UnpinStream(true));
      RETURN_IF_ERROR(partition->probe_rows()->UnpinStream(true));
      spilled_partitions_.push_back(partition);
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
      } else {
        partition->Close(batch);
      }
    }
  }
  hash_partitions_.clear();
  input_partition_ = NULL;
  return Status::OK;
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
  VLOG(2) << "Transitioned State:" << endl << DebugString();
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

string PartitionedHashJoinNode::DebugString() const {
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
  DCHECK(replaced == 2 || replaced == 3 || replaced == 4) << replaced;

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
