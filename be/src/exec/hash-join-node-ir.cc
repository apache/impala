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

#include "codegen/impala-ir.h"
#include "exec/hash-join-node.h"
#include "exec/old-hash-table.inline.h"
#include "runtime/row-batch.h"

#include "common/names.h"

using namespace impala;

// Functions in this file are cross compiled to IR with clang.

// Wrapper around ExecNode's eval conjuncts with a different function name.
// This lets us distinguish between the join conjuncts vs. non-join conjuncts
// for codegen.
// Note: don't declare this static.  LLVM will pick the fastcc calling convention and
// we will not be able to replace the functions with codegen'd versions.
// TODO: explicitly set the calling convention?
// TODO: investigate using fastcc for all codegen internal functions?
bool IR_NO_INLINE EvalOtherJoinConjuncts2(
    ExprContext* const* ctxs, int num_ctxs, TupleRow* row) {
  return ExecNode::EvalConjuncts(ctxs, num_ctxs, row);
}

// CreateOutputRow, EvalOtherJoinConjuncts, and EvalConjuncts are replaced by
// codegen.
int HashJoinNode::ProcessProbeBatch(RowBatch* out_batch, RowBatch* probe_batch,
    int max_added_rows) {
  // This path does not handle full outer or right outer joins
  DCHECK(!match_all_build_);

  int row_idx = out_batch->AddRows(max_added_rows);
  uint8_t* out_row_mem = reinterpret_cast<uint8_t*>(out_batch->GetRow(row_idx));
  TupleRow* out_row = reinterpret_cast<TupleRow*>(out_row_mem);

  int rows_returned = 0;
  int probe_rows = probe_batch->num_rows();

  ExprContext* const* other_conjunct_ctxs = &other_join_conjunct_ctxs_[0];
  const int num_other_conjunct_ctxs = other_join_conjunct_ctxs_.size();

  ExprContext* const* conjunct_ctxs = &conjunct_ctxs_[0];
  const int num_conjunct_ctxs = conjunct_ctxs_.size();

  while (true) {
    // Create output row for each matching build row
    while (!hash_tbl_iterator_.AtEnd()) {
      TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
      hash_tbl_iterator_.Next<true>();

      if (join_op_ == TJoinOp::LEFT_SEMI_JOIN) {
        // Evaluate the non-equi-join conjuncts against a temp row assembled from all
        // build and probe tuples.
        if (num_other_conjunct_ctxs > 0) {
          CreateOutputRow(semi_join_staging_row_, current_probe_row_, matched_build_row);
          if (!EvalOtherJoinConjuncts2(other_conjunct_ctxs, num_other_conjunct_ctxs,
                semi_join_staging_row_)) {
            continue;
          }
        }
        out_batch->CopyRow(current_probe_row_, out_row);
      } else {
        CreateOutputRow(out_row, current_probe_row_, matched_build_row);
        if (!EvalOtherJoinConjuncts2(
              other_conjunct_ctxs, num_other_conjunct_ctxs, out_row)) {
          continue;
        }
      }
      matched_probe_ = true;

      if (EvalConjuncts(conjunct_ctxs, num_conjunct_ctxs, out_row)) {
        ++rows_returned;
        // Filled up out batch or hit limit
        if (UNLIKELY(rows_returned == max_added_rows)) goto end;
        // Advance to next out row
        out_row_mem += out_batch->row_byte_size();
        out_row = reinterpret_cast<TupleRow*>(out_row_mem);
      }

      // Handle left semi-join
      if (match_one_build_) {
        hash_tbl_iterator_ = hash_tbl_->End();
        break;
      }
    }

    // Handle left outer-join
    if (!matched_probe_ && match_all_probe_) {
      CreateOutputRow(out_row, current_probe_row_, NULL);
      matched_probe_ = true;
      if (EvalConjuncts(conjunct_ctxs, num_conjunct_ctxs, out_row)) {
        ++rows_returned;
        if (UNLIKELY(rows_returned == max_added_rows)) goto end;
        // Advance to next out row
        out_row_mem += out_batch->row_byte_size();
        out_row = reinterpret_cast<TupleRow*>(out_row_mem);
      }
    }

    if (hash_tbl_iterator_.AtEnd()) {
      // Advance to the next probe row
      if (UNLIKELY(probe_batch_pos_ == probe_rows)) goto end;
      current_probe_row_ = probe_batch->GetRow(probe_batch_pos_++);
      hash_tbl_iterator_ = hash_tbl_->Find(current_probe_row_);
      matched_probe_ = false;
    }
  }

end:
  if (match_one_build_ && matched_probe_) hash_tbl_iterator_ = hash_tbl_->End();
  out_batch->CommitRows(rows_returned);
  return rows_returned;
}

void HashJoinNode::ProcessBuildBatch(RowBatch* build_batch) {
  // insert build row into our hash table
  for (int i = 0; i < build_batch->num_rows(); ++i) {
    hash_tbl_->Insert(build_batch->GetRow(i));
  }
}
