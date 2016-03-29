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
#include "runtime/row-batch.h"
#include "runtime/raw-value.inline.h"
#include "runtime/runtime-filter.h"
#include "util/bloom-filter.h"

#include "common/names.h"

using namespace impala;

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

// CreateOutputRow, EvalOtherJoinConjuncts, and EvalConjuncts are replaced by
// codegen.
template<int const JoinOp>
int PartitionedHashJoinNode::ProcessProbeBatch(RowBatch* out_batch,
    const HashTableCtx* __restrict__ ht_ctx, Status* __restrict__ status) {
  ExprContext* const* other_join_conjunct_ctxs = &other_join_conjunct_ctxs_[0];
  const int num_other_join_conjuncts = other_join_conjunct_ctxs_.size();
  ExprContext* const* conjunct_ctxs = &conjunct_ctxs_[0];
  const int num_conjuncts = conjunct_ctxs_.size();

  DCHECK(!out_batch->AtCapacity());
  DCHECK_GE(probe_batch_pos_, 0);
  RowBatch::Iterator out_batch_iterator(out_batch, out_batch->AddRow());
  TupleRow* out_row = out_batch_iterator.Get();
  const int max_rows = out_batch->capacity() - out_batch->num_rows();
  // Note that 'probe_batch_pos_' is the row no. of the row after 'current_probe_row_'.
  RowBatch::Iterator probe_batch_iterator(probe_batch_.get(), probe_batch_pos_);
  int num_rows_added = 0;

  while (true) {
    if (current_probe_row_ != NULL) {
      while (!hash_tbl_iterator_.AtEnd()) {
        TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
        DCHECK(matched_build_row != NULL);

        if ((JoinOp == TJoinOp::RIGHT_SEMI_JOIN || JoinOp == TJoinOp::RIGHT_ANTI_JOIN) &&
            hash_tbl_iterator_.IsMatched()) {
          hash_tbl_iterator_.NextDuplicate();
          continue;
        }

        if (JoinOp == TJoinOp::LEFT_ANTI_JOIN || JoinOp == TJoinOp::LEFT_SEMI_JOIN ||
            JoinOp == TJoinOp::RIGHT_ANTI_JOIN || JoinOp == TJoinOp::RIGHT_SEMI_JOIN ||
            JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
          // Evaluate the non-equi-join conjuncts against a temp row assembled from all
          // build and probe tuples.
          if (num_other_join_conjuncts > 0) {
            CreateOutputRow(semi_join_staging_row_, current_probe_row_, matched_build_row);
            if (!EvalOtherJoinConjuncts(other_join_conjunct_ctxs,
                    num_other_join_conjuncts, semi_join_staging_row_)) {
              hash_tbl_iterator_.NextDuplicate();
              continue;
            }
          }

          // Create output row assembled from build xor probe tuples.
          if (JoinOp == TJoinOp::LEFT_ANTI_JOIN || JoinOp == TJoinOp::LEFT_SEMI_JOIN ||
              JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            out_batch->CopyRow(current_probe_row_, out_row);
          } else {
            out_batch->CopyRow(matched_build_row, out_row);
          }
        } else {
          // Not a semi join; create an output row with all probe/build tuples and
          // evaluate the non-equi-join conjuncts.
          CreateOutputRow(out_row, current_probe_row_, matched_build_row);
          if (!EvalOtherJoinConjuncts(other_join_conjunct_ctxs, num_other_join_conjuncts,
                  out_row)) {
            hash_tbl_iterator_.NextDuplicate();
            continue;
          }
        }

        // At this point the probe is considered matched.
        matched_probe_ = true;
        if (JoinOp == TJoinOp::LEFT_ANTI_JOIN ||
            JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
          // We can safely ignore this probe row for left anti joins.
          hash_tbl_iterator_.SetAtEnd();
          goto next_row;
        }

        // Update hash_tbl_iterator.
        if (JoinOp == TJoinOp::LEFT_SEMI_JOIN) {
          hash_tbl_iterator_.SetAtEnd();
        } else {
          if (JoinOp == TJoinOp::RIGHT_OUTER_JOIN || JoinOp == TJoinOp::RIGHT_ANTI_JOIN ||
              JoinOp == TJoinOp::FULL_OUTER_JOIN || JoinOp == TJoinOp::RIGHT_SEMI_JOIN) {
            // There is a match for this build row. Mark the Bucket or the DuplicateNode
            // as matched for right/full joins.
            hash_tbl_iterator_.SetMatched();
          }
          hash_tbl_iterator_.NextDuplicate();
        }

        if (JoinOp != TJoinOp::RIGHT_ANTI_JOIN &&
            ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
          ++num_rows_added;
          if (num_rows_added == max_rows) goto update_probe_batch_pos;
          out_row = out_batch_iterator.Next();
        }
      }

      if (JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && !matched_probe_) {
        // Null aware behavior. The probe row did not match in the hash table so we
        // should interpret the hash table probe as "unknown" if there are nulls on the
        // build size. For those rows, we need to process the remaining join
        // predicates later.
        if (null_aware_partition_->build_rows()->num_rows() != 0) {
          if (num_other_join_conjuncts == 0) goto next_row;
          if (UNLIKELY(!AppendRow(null_aware_partition_->probe_rows(),
                           current_probe_row_, status))) {
            num_rows_added = -1;
            goto update_probe_batch_pos;
          }
          goto next_row;
        }
      }

      if ((JoinOp == TJoinOp::LEFT_OUTER_JOIN || JoinOp == TJoinOp::FULL_OUTER_JOIN) &&
          !matched_probe_) {
        // No match for this row, we need to output it.
        CreateOutputRow(out_row, current_probe_row_, NULL);
        if (ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
          ++num_rows_added;
          matched_probe_ = true;
          if (num_rows_added == max_rows) goto update_probe_batch_pos;
          out_row = out_batch_iterator.Next();
        }
      }
      if ((JoinOp == TJoinOp::LEFT_ANTI_JOIN ||
          JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) &&
          !matched_probe_) {
        // No match for this current_probe_row_, we need to output it. No need to
        // evaluate the conjunct_ctxs since semi joins cannot have any.
        out_batch->CopyRow(current_probe_row_, out_row);
        ++num_rows_added;
        matched_probe_ = true;
        if (num_rows_added == max_rows) goto update_probe_batch_pos;
        out_row = out_batch_iterator.Next();
      }
    }

next_row:
    // Must have reached the end of the hash table iterator for the current row before
    // moving to the next row.
    DCHECK(hash_tbl_iterator_.AtEnd());

    if (UNLIKELY(probe_batch_iterator.AtEnd())) {
      // Finished this batch.
      probe_batch_pos_ = probe_batch_->num_rows();
      current_probe_row_ = NULL;
      goto done;
    }

    // Establish current_probe_row_ and find its corresponding partition.
    current_probe_row_ = probe_batch_iterator.Get();
    probe_batch_iterator.Next();
    matched_probe_ = false;
    uint32_t hash;
    if (!ht_ctx->EvalAndHashProbe(current_probe_row_, &hash)) {
      if (JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        // For NAAJ, we need to treat NULLs on the probe carefully. The logic is:
        // 1. No build rows -> Return this row.
        // 2. Has build rows & no other join predicates, skip row.
        // 3. Has build rows & other join predicates, we need to evaluate against all
        // build rows. First evaluate it against this partition, and if there is not
        // a match, save it to evaluate against other partitions later. If there
        // is a match, the row is skipped.
        if (!non_empty_build_) continue;
        if (num_other_join_conjuncts == 0) goto next_row;
        if (UNLIKELY(!AppendRow(null_probe_rows_, current_probe_row_, status))) {
          num_rows_added = -1;
          goto update_probe_batch_pos;
        }
        matched_null_probe_.push_back(false);
        goto next_row;
      }
      continue;
    }
    const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
    if (LIKELY(hash_tbls_[partition_idx] != NULL)) {
      hash_tbl_iterator_ = hash_tbls_[partition_idx]->FindProbeRow(ht_ctx, hash);
    } else {
      Partition* partition = hash_partitions_[partition_idx];
      if (UNLIKELY(partition->is_closed())) {
        // This partition is closed, meaning the build side for this partition was empty.
        DCHECK(state_ == PROCESSING_PROBE || state_ == REPARTITIONING);
      } else {
        // This partition is not in memory, spill the probe row and move to the next row.
        DCHECK(partition->is_spilled());
        DCHECK(partition->probe_rows() != NULL);
        if (UNLIKELY(!AppendRow(partition->probe_rows(), current_probe_row_, status))) {
          num_rows_added = -1;
          goto update_probe_batch_pos;
        }
        goto next_row;
      }
    }
  }

update_probe_batch_pos:
  probe_batch_pos_ =
      (probe_batch_iterator.Get() - probe_batch_->GetRow(0)) / probe_batch_->num_tuples_per_row();
done:
  DCHECK_GE(probe_batch_pos_, 0);
  DCHECK_LE(probe_batch_pos_, probe_batch_->capacity());
  DCHECK_LE(num_rows_added, max_rows);
  return num_rows_added;
}

int PartitionedHashJoinNode::ProcessProbeBatch(
    const TJoinOp::type join_op, RowBatch* out_batch,
    const HashTableCtx* __restrict__ ht_ctx, Status* __restrict__ status) {
 switch (join_op) {
    case TJoinOp::INNER_JOIN:
      return ProcessProbeBatch<TJoinOp::INNER_JOIN>(out_batch, ht_ctx, status);
    case TJoinOp::LEFT_OUTER_JOIN:
      return ProcessProbeBatch<TJoinOp::LEFT_OUTER_JOIN>(out_batch, ht_ctx, status);
    case TJoinOp::LEFT_SEMI_JOIN:
      return ProcessProbeBatch<TJoinOp::LEFT_SEMI_JOIN>(out_batch, ht_ctx, status);
    case TJoinOp::LEFT_ANTI_JOIN:
      return ProcessProbeBatch<TJoinOp::LEFT_ANTI_JOIN>(out_batch, ht_ctx, status);
    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
      return ProcessProbeBatch<TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>(out_batch, ht_ctx,
          status);
    case TJoinOp::RIGHT_OUTER_JOIN:
      return ProcessProbeBatch<TJoinOp::RIGHT_OUTER_JOIN>(out_batch, ht_ctx, status);
    case TJoinOp::RIGHT_SEMI_JOIN:
      return ProcessProbeBatch<TJoinOp::RIGHT_SEMI_JOIN>(out_batch, ht_ctx, status);
    case TJoinOp::RIGHT_ANTI_JOIN:
      return ProcessProbeBatch<TJoinOp::RIGHT_ANTI_JOIN>(out_batch, ht_ctx, status);
    case TJoinOp::FULL_OUTER_JOIN:
      return ProcessProbeBatch<TJoinOp::FULL_OUTER_JOIN>(out_batch, ht_ctx, status);
    default:
      DCHECK(false) << "Unknown join type";
      return -1;
  }
}

Status PartitionedHashJoinNode::ProcessBuildBatch(RowBatch* build_batch,
    bool build_filters) {
  FOREACH_ROW(build_batch, 0, build_row) {
    DCHECK(build_status_.ok());
    uint32_t hash;
    if (!ht_ctx_->EvalAndHashBuild(build_row, &hash)) {
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
        if (ctx.local_bloom_filter == NULL) continue;
        void* e = ctx.expr->GetValue(build_row);
        uint32_t filter_hash = RawValue::GetHashValue(e, ctx.expr->root()->type(),
            RuntimeFilterBank::DefaultHashSeed());
        ctx.local_bloom_filter->Insert(filter_hash);
      }
    }
    const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
    Partition* partition = hash_partitions_[partition_idx];
    const bool result = AppendRow(partition->build_rows(), build_row, &build_status_);
    if (UNLIKELY(!result)) return build_status_;
  }
  return Status::OK();
}
