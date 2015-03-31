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
int PartitionedHashJoinNode::ProcessProbeBatch(
    RowBatch* out_batch, HashTableCtx* ht_ctx, Status* status) {
  ExprContext* const* other_join_conjunct_ctxs = &other_join_conjunct_ctxs_[0];
  const int num_other_join_conjuncts = other_join_conjunct_ctxs_.size();
  ExprContext* const* conjunct_ctxs = &conjunct_ctxs_[0];
  const int num_conjuncts = conjunct_ctxs_.size();

  DCHECK(!out_batch->AtCapacity());
  TupleRow* out_row = out_batch->GetRow(out_batch->AddRow());
  const int max_rows = out_batch->capacity() - out_batch->num_rows();
  int num_rows_added = 0;

  while (probe_batch_pos_ >= 0) {
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
            CreateOutputRow(semi_join_staging_row_, current_probe_row_,
                matched_build_row);
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

        if ((JoinOp != TJoinOp::RIGHT_ANTI_JOIN) &&
            ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
          ++num_rows_added;
          out_row = out_row->next_row(out_batch);
          if (num_rows_added == max_rows) goto end;
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
            return -1;
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
          out_row = out_row->next_row(out_batch);
          if (num_rows_added == max_rows) goto end;
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
        out_row = out_row->next_row(out_batch);
        if (num_rows_added == max_rows) goto end;
      }
    }

next_row:
    // Must have reached the end of the hash table iterator for the current row before
    // moving to the row.
    DCHECK(hash_tbl_iterator_.AtEnd());

    if (UNLIKELY(probe_batch_pos_ == probe_batch_->num_rows())) {
      // Finished this batch.
      current_probe_row_ = NULL;
      goto end;
    }

    // Establish current_probe_row_ and find its corresponding partition.
    current_probe_row_ = probe_batch_->GetRow(probe_batch_pos_++);
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
          return -1;
        }
        matched_null_probe_.push_back(false);
        goto next_row;
      }
      continue;
    }
    const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
    if (LIKELY(hash_tbls_[partition_idx] != NULL)) {
      hash_tbl_iterator_= hash_tbls_[partition_idx]->Find(ht_ctx, hash);
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
          return -1;
        }
        goto next_row;
      }
    }
  }

end:
  DCHECK_LE(num_rows_added, max_rows);
  return num_rows_added;
}

int PartitionedHashJoinNode::ProcessProbeBatch(
    const TJoinOp::type join_op, RowBatch* out_batch, HashTableCtx* ht_ctx,
    Status* status) {
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

Status PartitionedHashJoinNode::ProcessBuildBatch(RowBatch* build_batch) {
  for (int i = 0; i < build_batch->num_rows(); ++i) {
    DCHECK(buildStatus_.ok());
    TupleRow* build_row = build_batch->GetRow(i);
    uint32_t hash;
    if (!ht_ctx_->EvalAndHashBuild(build_row, &hash)) {
      if (null_aware_partition_ != NULL) {
        // TODO: remove with codegen/template
        // If we are NULL aware and this build row has NULL in the eq join slot,
        // append it to the null_aware partition. We will need it later.
        if (UNLIKELY(!AppendRow(null_aware_partition_->build_rows(),
                                build_row, &buildStatus_))) {
          return buildStatus_;
        }
      }
      continue;
    }
    const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
    Partition* partition = hash_partitions_[partition_idx];
    const bool result = AppendRow(partition->build_rows(), build_row, &buildStatus_);
    if (UNLIKELY(!result)) return buildStatus_;
  }
  return Status::OK();
}
