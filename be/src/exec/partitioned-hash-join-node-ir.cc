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

#include "codegen/impala-ir.h"
#include "exec/hash-table.inline.h"
#include "runtime/raw-value.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-filter.h"
#include "util/bloom-filter.h"

#include "common/names.h"

namespace impala {

// Wrapper around ExecNode's eval conjuncts with a different function name.
// This lets us distinguish between the join conjuncts vs. non-join conjuncts
// for codegen.
// Note: don't declare this static.  LLVM will pick the fastcc calling convention and
// we will not be able to replace the functions with codegen'd versions.
// TODO: explicitly set the calling convention?
// TODO: investigate using fastcc for all codegen internal functions?
bool IR_NO_INLINE EvalOtherJoinConjuncts(
    ExprContext* const* ctxs, int num_ctxs, TupleRow* row) {
  return ExecNode::EvalConjuncts(ctxs, num_ctxs, row);
}

bool IR_ALWAYS_INLINE PartitionedHashJoinNode::ProcessProbeRowInnerJoin(
    ExprContext* const* other_join_conjunct_ctxs, int num_other_join_conjuncts,
    ExprContext* const* conjunct_ctxs, int num_conjuncts,
    RowBatch::Iterator* out_batch_iterator, int* remaining_capacity) {
  DCHECK(current_probe_row_ != NULL);
  TupleRow* out_row = out_batch_iterator->Get();
  for (; !hash_tbl_iterator_.AtEnd(); hash_tbl_iterator_.NextDuplicate()) {
    TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
    DCHECK(matched_build_row != NULL);

    // Create an output row with all probe/build tuples and evaluate the
    // non-equi-join conjuncts.
    CreateOutputRow(out_row, current_probe_row_, matched_build_row);
    if (!EvalOtherJoinConjuncts(other_join_conjunct_ctxs, num_other_join_conjuncts,
        out_row)) {
      continue;
    }
    if (ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
      --(*remaining_capacity);
      if (*remaining_capacity == 0) {
        hash_tbl_iterator_.NextDuplicate();
        return false;
      }
      out_row = out_batch_iterator->Next();
    }
  }
  return true;
}

template<int const JoinOp>
bool IR_ALWAYS_INLINE PartitionedHashJoinNode::ProcessProbeRowRightSemiJoins(
    ExprContext* const* other_join_conjunct_ctxs, int num_other_join_conjuncts,
    ExprContext* const* conjunct_ctxs, int num_conjuncts,
    RowBatch::Iterator* out_batch_iterator, int* remaining_capacity) {
  DCHECK(current_probe_row_ != NULL);
  DCHECK(JoinOp == TJoinOp::RIGHT_SEMI_JOIN || JoinOp == TJoinOp::RIGHT_ANTI_JOIN);
  TupleRow* out_row = out_batch_iterator->Get();
  for (; !hash_tbl_iterator_.AtEnd(); hash_tbl_iterator_.NextDuplicate()) {
    TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
    DCHECK(matched_build_row != NULL);

    if (hash_tbl_iterator_.IsMatched()) continue;
    // Evaluate the non-equi-join conjuncts against a temp row assembled from all
    // build and probe tuples.
    if (num_other_join_conjuncts > 0) {
      CreateOutputRow(semi_join_staging_row_, current_probe_row_, matched_build_row);
      if (!EvalOtherJoinConjuncts(other_join_conjunct_ctxs,
          num_other_join_conjuncts, semi_join_staging_row_)) {
        continue;
      }
    }
    // Create output row assembled from build tuples.
    out_batch_iterator->parent()->CopyRow(matched_build_row, out_row);
    // Update the hash table to indicate that this entry has been matched.
    hash_tbl_iterator_.SetMatched();
    if (JoinOp == TJoinOp::RIGHT_SEMI_JOIN &&
        ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
      --(*remaining_capacity);
      if (*remaining_capacity == 0) {
        hash_tbl_iterator_.NextDuplicate();
        return false;
      }
      out_row = out_batch_iterator->Next();
    }
  }
  return true;
}

template<int const JoinOp>
bool IR_ALWAYS_INLINE PartitionedHashJoinNode::ProcessProbeRowLeftSemiJoins(
    ExprContext* const* other_join_conjunct_ctxs, int num_other_join_conjuncts,
    ExprContext* const* conjunct_ctxs, int num_conjuncts,
    RowBatch::Iterator* out_batch_iterator, int* remaining_capacity, Status* status) {
  DCHECK(current_probe_row_ != NULL);
  DCHECK(JoinOp == TJoinOp::LEFT_ANTI_JOIN || JoinOp == TJoinOp::LEFT_SEMI_JOIN ||
      JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN);
  TupleRow* out_row = out_batch_iterator->Get();
  for (; !hash_tbl_iterator_.AtEnd(); hash_tbl_iterator_.NextDuplicate()) {
    TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
    DCHECK(matched_build_row != NULL);
    // Evaluate the non-equi-join conjuncts against a temp row assembled from all
    // build and probe tuples.
    if (num_other_join_conjuncts > 0) {
      CreateOutputRow(semi_join_staging_row_, current_probe_row_, matched_build_row);
      if (!EvalOtherJoinConjuncts(other_join_conjunct_ctxs,
          num_other_join_conjuncts, semi_join_staging_row_)) {
        continue;
      }
    }
    // Create output row assembled from probe tuples.
    out_batch_iterator->parent()->CopyRow(current_probe_row_, out_row);
    // A match is found in the hash table. The search is over for this probe row.
    matched_probe_ = true;
    hash_tbl_iterator_.SetAtEnd();
    // Append to output batch for left semi joins if the conjuncts are satisfied.
    if (JoinOp == TJoinOp::LEFT_SEMI_JOIN &&
        ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
      --(*remaining_capacity);
      if (*remaining_capacity == 0) return false;
      out_row = out_batch_iterator->Next();
    }
    // Done with this probe row.
    return true;
  }

  if (JoinOp != TJoinOp::LEFT_SEMI_JOIN && !matched_probe_) {
    if (JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
      // Null aware behavior. The probe row did not match in the hash table so we
      // should interpret the hash table probe as "unknown" if there are nulls on the
      // build side. For those rows, we need to process the remaining join
      // predicates later.
      if (null_aware_partition_->build_rows()->num_rows() != 0) {
        if (num_other_join_conjuncts > 0 &&
            UNLIKELY(!AppendRow(null_aware_partition_->probe_rows(),
                         current_probe_row_, status))) {
          DCHECK(!status->ok());
          return false;
        }
        return true;
      }
    }
    // No match for this current_probe_row_, we need to output it. No need to
    // evaluate the conjunct_ctxs since anti joins cannot have any.
    out_batch_iterator->parent()->CopyRow(current_probe_row_, out_row);
    matched_probe_ = true;
    --(*remaining_capacity);
    if (*remaining_capacity == 0) return false;
    out_row = out_batch_iterator->Next();
  }
  return true;
}

template<int const JoinOp>
bool IR_ALWAYS_INLINE PartitionedHashJoinNode::ProcessProbeRowOuterJoins(
    ExprContext* const* other_join_conjunct_ctxs, int num_other_join_conjuncts,
    ExprContext* const* conjunct_ctxs, int num_conjuncts,
    RowBatch::Iterator* out_batch_iterator, int* remaining_capacity) {
  DCHECK(JoinOp == TJoinOp::LEFT_OUTER_JOIN || JoinOp == TJoinOp::RIGHT_OUTER_JOIN ||
      JoinOp == TJoinOp::FULL_OUTER_JOIN);
  DCHECK(current_probe_row_ != NULL);
  TupleRow* out_row = out_batch_iterator->Get();
  for (; !hash_tbl_iterator_.AtEnd(); hash_tbl_iterator_.NextDuplicate()) {
    TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
    DCHECK(matched_build_row != NULL);
    // Create an output row with all probe/build tuples and evaluate the
    // non-equi-join conjuncts.
    CreateOutputRow(out_row, current_probe_row_, matched_build_row);
    if (!EvalOtherJoinConjuncts(other_join_conjunct_ctxs, num_other_join_conjuncts,
        out_row)) {
      continue;
    }
    // At this point the probe is considered matched.
    matched_probe_ = true;
    if (JoinOp == TJoinOp::RIGHT_OUTER_JOIN || JoinOp == TJoinOp::FULL_OUTER_JOIN) {
      // There is a match for this build row. Mark the Bucket or the DuplicateNode
      // as matched for right/full outer joins.
      hash_tbl_iterator_.SetMatched();
    }
    if (ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
      --(*remaining_capacity);
      if (*remaining_capacity == 0) {
        hash_tbl_iterator_.NextDuplicate();
        return false;
      }
      out_row = out_batch_iterator->Next();
    }
  }

  if (JoinOp != TJoinOp::RIGHT_OUTER_JOIN && !matched_probe_) {
    // No match for this row, we need to output it if it's a left/full outer join.
    CreateOutputRow(out_row, current_probe_row_, NULL);
    if (ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
      matched_probe_ = true;
      --(*remaining_capacity);
      if (*remaining_capacity == 0) return false;
      out_row = out_batch_iterator->Next();
    }
  }
  return true;
}

template<int const JoinOp>
bool IR_ALWAYS_INLINE PartitionedHashJoinNode::ProcessProbeRow(
    ExprContext* const* other_join_conjunct_ctxs, int num_other_join_conjuncts,
    ExprContext* const* conjunct_ctxs, int num_conjuncts,
    RowBatch::Iterator* out_batch_iterator, int* remaining_capacity, Status* status) {
  if (JoinOp == TJoinOp::INNER_JOIN) {
    return ProcessProbeRowInnerJoin(other_join_conjunct_ctxs, num_other_join_conjuncts,
        conjunct_ctxs, num_conjuncts, out_batch_iterator, remaining_capacity);
  } else if (JoinOp == TJoinOp::RIGHT_SEMI_JOIN ||
             JoinOp == TJoinOp::RIGHT_ANTI_JOIN) {
    return ProcessProbeRowRightSemiJoins<JoinOp>(other_join_conjunct_ctxs,
        num_other_join_conjuncts, conjunct_ctxs, num_conjuncts, out_batch_iterator,
        remaining_capacity);
  } else if (JoinOp == TJoinOp::LEFT_SEMI_JOIN ||
             JoinOp == TJoinOp::LEFT_ANTI_JOIN ||
             JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    return ProcessProbeRowLeftSemiJoins<JoinOp>(other_join_conjunct_ctxs,
        num_other_join_conjuncts, conjunct_ctxs, num_conjuncts, out_batch_iterator,
        remaining_capacity, status);
  } else {
    DCHECK(JoinOp == TJoinOp::RIGHT_OUTER_JOIN ||
           JoinOp == TJoinOp::LEFT_OUTER_JOIN || TJoinOp::FULL_OUTER_JOIN);
    return ProcessProbeRowOuterJoins<JoinOp>(other_join_conjunct_ctxs,
        num_other_join_conjuncts, conjunct_ctxs, num_conjuncts, out_batch_iterator,
        remaining_capacity);
  }
}

template<int const JoinOp>
bool IR_ALWAYS_INLINE PartitionedHashJoinNode::NextProbeRow(
    HashTableCtx* ht_ctx, RowBatch::Iterator* probe_batch_iterator,
    int* remaining_capacity, Status* status) {
  HashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();
  while (!expr_vals_cache->AtEnd()) {
    // Establish current_probe_row_ and find its corresponding partition.
    DCHECK(!probe_batch_iterator->AtEnd());
    current_probe_row_ = probe_batch_iterator->Get();
    matched_probe_ = false;

    // True if the current row should be skipped for probing.
    bool skip_row = false;

    // The hash of the expressions results for the current probe row.
    uint32_t hash = expr_vals_cache->ExprValuesHash();
    // Hoist the followings out of the else statement below to speed up non-null case.
    const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
    HashTable* hash_tbl = hash_tbls_[partition_idx];

    // Fetch the hash and expr values' nullness for this row.
    if (expr_vals_cache->IsRowNull()) {
      if (JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && non_empty_build_) {
        const int num_other_join_conjuncts = other_join_conjunct_ctxs_.size();
        // For NAAJ, we need to treat NULLs on the probe carefully. The logic is:
        // 1. No build rows -> Return this row. The check for 'non_empty_build_'
        //    is for this case.
        // 2. Has build rows & no other join predicates, skip row.
        // 3. Has build rows & other join predicates, we need to evaluate against all
        // build rows. First evaluate it against this partition, and if there is not
        // a match, save it to evaluate against other partitions later. If there
        // is a match, the row is skipped.
        if (num_other_join_conjuncts == 0) {
          // Condition 2 above.
          skip_row = true;
        } else if (LIKELY(AppendRow(null_probe_rows_, current_probe_row_, status))) {
          // Condition 3 above.
          matched_null_probe_.push_back(false);
          skip_row = true;
        } else {
          // Condition 3 above but failed to append to 'null_probe_rows_'. Bail out.
          DCHECK(!status->ok());
          return false;
        }
      }
    } else {
      // The build partition is in memory. Return this row for probing.
      if (LIKELY(hash_tbl != NULL)) {
        hash_tbl_iterator_ = hash_tbl->FindProbeRow(ht_ctx);
      } else {
        // The build partition is either empty or spilled.
        Partition* partition = hash_partitions_[partition_idx];
        // This partition is closed, meaning the build side for this partition was empty.
        if (UNLIKELY(partition->is_closed())) {
          DCHECK(state_ == PROCESSING_PROBE || state_ == REPARTITIONING);
        } else {
          // This partition is not in memory, spill the probe row and move to the next row.
          DCHECK(partition->is_spilled());
          DCHECK(partition->probe_rows() != NULL);
          // Skip the current row if we manage to append to the spilled partition's BTS.
          // Otherwise, we need to bail out and report the failure.
          if (UNLIKELY(!AppendRow(partition->probe_rows(), current_probe_row_, status))) {
            DCHECK(!status->ok());
            return false;
          }
          skip_row = true;
        }
      }
    }
    // Move to the next probe row and hash table context's cached value.
    probe_batch_iterator->Next();
    expr_vals_cache->NextRow();
    if (skip_row) continue;
    DCHECK(status->ok());
    return true;
  }
  if (probe_batch_iterator->AtEnd()) {
    // No more probe row.
    current_probe_row_ = NULL;
  }
  return false;
}

void IR_ALWAYS_INLINE PartitionedHashJoinNode::EvalAndHashProbePrefetchGroup(
    TPrefetchMode::type prefetch_mode, HashTableCtx* ht_ctx) {
  RowBatch* probe_batch = probe_batch_.get();
  HashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();
  const int prefetch_size = expr_vals_cache->capacity();
  DCHECK(expr_vals_cache->AtEnd());

  expr_vals_cache->Reset();
  FOREACH_ROW_LIMIT(probe_batch, probe_batch_pos_, prefetch_size, batch_iter) {
    TupleRow* row = batch_iter.Get();
    if (ht_ctx->EvalAndHashProbe(row)) {
      if (prefetch_mode != TPrefetchMode::NONE) {
        uint32_t hash = expr_vals_cache->ExprValuesHash();
        const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
        HashTable* hash_tbl = hash_tbls_[partition_idx];
        if (LIKELY(hash_tbl != NULL)) hash_tbl->PrefetchBucket<true>(hash);
      }
    } else {
      expr_vals_cache->SetRowNull();
    }
    expr_vals_cache->NextRow();
  }
  expr_vals_cache->ResetForRead();
}

// CreateOutputRow, EvalOtherJoinConjuncts, and EvalConjuncts are replaced by codegen.
template<int const JoinOp>
int PartitionedHashJoinNode::ProcessProbeBatch(TPrefetchMode::type prefetch_mode,
    RowBatch* out_batch, HashTableCtx* __restrict__ ht_ctx, Status* __restrict__ status) {
  ExprContext* const* other_join_conjunct_ctxs = &other_join_conjunct_ctxs_[0];
  const int num_other_join_conjuncts = other_join_conjunct_ctxs_.size();
  ExprContext* const* conjunct_ctxs = &conjunct_ctxs_[0];
  const int num_conjuncts = conjunct_ctxs_.size();

  DCHECK(!out_batch->AtCapacity());
  DCHECK_GE(probe_batch_pos_, 0);
  RowBatch::Iterator out_batch_iterator(out_batch, out_batch->AddRow());
  const int max_rows = out_batch->capacity() - out_batch->num_rows();
  // Note that 'probe_batch_pos_' is the row no. of the row after 'current_probe_row_'.
  RowBatch::Iterator probe_batch_iterator(probe_batch_.get(), probe_batch_pos_);
  int remaining_capacity = max_rows;
  bool has_probe_rows = current_probe_row_ != NULL || !probe_batch_iterator.AtEnd();
  HashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();

  // Keep processing more probe rows if there are more to process and the output batch
  // has room and we haven't hit any error yet.
  while (has_probe_rows && remaining_capacity > 0 && status->ok()) {
    // Prefetch for the current hash_tbl_iterator_.
    if (prefetch_mode != TPrefetchMode::NONE) {
      hash_tbl_iterator_.PrefetchBucket<true>();
    }
    // Evaluate and hash more rows if prefetch group is empty. A prefetch group is a cache
    // of probe expressions results, nullness of the expression values and hash values
    // against some consecutive number of rows in the probe batch. Prefetching, if
    // enabled, is interleaved with the rows' evaluation and hashing. If the prefetch
    // group is partially full (e.g. we returned before the current prefetch group was
    // exhausted in the previous iteration), we will proceed with the remaining items in
    // the values cache.
    if (expr_vals_cache->AtEnd()) {
      EvalAndHashProbePrefetchGroup(prefetch_mode, ht_ctx);
    }
    // Process the prefetch group.
    do {
      // 'current_probe_row_' can be NULL on the first iteration through this loop.
      if (current_probe_row_ != NULL) {
        if (!ProcessProbeRow<JoinOp>(other_join_conjunct_ctxs, num_other_join_conjuncts,
            conjunct_ctxs, num_conjuncts, &out_batch_iterator, &remaining_capacity,
            status)) {
          if (status->ok()) DCHECK_EQ(remaining_capacity, 0);
          break;
        }
      }
      // Must have reached the end of the hash table iterator for the current row before
      // moving to the next row.
      DCHECK(hash_tbl_iterator_.AtEnd());
      DCHECK(status->ok());
    } while (NextProbeRow<JoinOp>(ht_ctx, &probe_batch_iterator, &remaining_capacity,
        status));
    // Update whether there are more probe rows to process in the current batch.
    has_probe_rows = current_probe_row_ != NULL;
    if (!has_probe_rows) DCHECK(probe_batch_iterator.AtEnd());
    // Update where we are in the probe batch.
    probe_batch_pos_ = (probe_batch_iterator.Get() - probe_batch_->GetRow(0)) /
        probe_batch_->num_tuples_per_row();
  }

  int num_rows_added;
  if (LIKELY(status->ok())) {
    num_rows_added = max_rows - remaining_capacity;
  } else {
    num_rows_added = -1;
  }
  DCHECK_GE(probe_batch_pos_, 0);
  DCHECK_LE(probe_batch_pos_, probe_batch_->capacity());
  DCHECK_LE(num_rows_added, max_rows);
  return num_rows_added;
}

Status PartitionedHashJoinNode::ProcessBuildBatch(RowBatch* build_batch,
    bool build_filters) {
  HashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx_->expr_values_cache();
  expr_vals_cache->Reset();
  FOREACH_ROW(build_batch, 0, build_batch_iter) {
    DCHECK(build_status_.ok());
    TupleRow* build_row = build_batch_iter.Get();
    if (!ht_ctx_->EvalAndHashBuild(build_row)) {
      if (null_aware_partition_ != NULL) {
        // TODO: remove with codegen/template
        // If we are NULL aware and this build row has NULL in the eq join slot,
        // append it to the null_aware partition. We will need it later.
        if (UNLIKELY(!AppendRow(null_aware_partition_->build_rows(),
                build_row, &build_status_))) {
          return build_status_;
        }
      }
      continue;
    }
    if (build_filters) {
      DCHECK_EQ(ht_ctx_->level(), 0)
          << "Runtime filters should not be built during repartitioning.";
      for (const FilterContext& ctx: filters_) {
        // TODO: codegen expr evaluation and hashing
        if (ctx.local_bloom_filter == NULL) continue;
        void* e = ctx.expr->GetValue(build_row);
        uint32_t filter_hash = RawValue::GetHashValue(e, ctx.expr->root()->type(),
            RuntimeFilterBank::DefaultHashSeed());
        ctx.local_bloom_filter->Insert(filter_hash);
      }
    }
    const uint32_t hash = expr_vals_cache->ExprValuesHash();
    const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
    Partition* partition = hash_partitions_[partition_idx];
    const bool result = AppendRow(partition->build_rows(), build_row, &build_status_);
    if (UNLIKELY(!result)) return build_status_;
  }
  return Status::OK();
}

bool PartitionedHashJoinNode::Partition::InsertBatch(
    TPrefetchMode::type prefetch_mode, HashTableCtx* ht_ctx, RowBatch* batch,
    const vector<BufferedTupleStream::RowIdx>& indices) {
  // Compute the hash values and prefetch the hash table buckets.
  const int num_rows = batch->num_rows();
  HashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();
  const int prefetch_size = expr_vals_cache->capacity();
  const BufferedTupleStream::RowIdx* row_indices = indices.data();
  for (int prefetch_group_row = 0; prefetch_group_row < num_rows;
       prefetch_group_row += prefetch_size) {
    int cur_row = prefetch_group_row;
    expr_vals_cache->Reset();
    FOREACH_ROW_LIMIT(batch, cur_row, prefetch_size, batch_iter) {
      if (ht_ctx->EvalAndHashBuild(batch_iter.Get())) {
        if (prefetch_mode != TPrefetchMode::NONE) {
          hash_tbl_->PrefetchBucket<false>(expr_vals_cache->ExprValuesHash());
        }
      } else {
        expr_vals_cache->SetRowNull();
      }
      expr_vals_cache->NextRow();
    }
    // Do the insertion.
    expr_vals_cache->ResetForRead();
    FOREACH_ROW_LIMIT(batch, cur_row, prefetch_size, batch_iter) {
      TupleRow* row = batch_iter.Get();
      BufferedTupleStream::RowIdx row_idx = row_indices[cur_row];
      if (!expr_vals_cache->IsRowNull() &&
          UNLIKELY(!hash_tbl_->Insert(ht_ctx, row_idx, row))) {
        return false;
      }
      expr_vals_cache->NextRow();
      ++cur_row;
    }
  }
  return true;
}

template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::INNER_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, HashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::LEFT_OUTER_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, HashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::LEFT_SEMI_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, HashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::LEFT_ANTI_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, HashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, HashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::RIGHT_OUTER_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, HashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::RIGHT_SEMI_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, HashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::RIGHT_ANTI_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, HashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::FULL_OUTER_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, HashTableCtx* ht_ctx,
    Status* status);
}
