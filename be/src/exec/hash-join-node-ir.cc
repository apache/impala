// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "exec/hash-join-node.h"
#include "runtime/row-batch.h"

using namespace std;
using namespace impala;

// Functions in this file are cross compiled to IR with clang.  

// TODO: CreateOutputRow, HashFn, EqualsFn, EvalConjuncts, EvalOtherJoinConjuncts
// should be codegen'd for this function
int HashJoinNode::ProcessProbeBatch(RowBatch* out_batch, RowBatch* probe_batch, 
    int max_added_rows) {
  // This path does not handle full outer or right outer joins
  DCHECK(!match_all_build_);

  int row_idx = out_batch->AddRows(max_added_rows);
  DCHECK(row_idx != RowBatch::INVALID_ROW_INDEX);
  uint8_t* out_row_mem = reinterpret_cast<uint8_t*>(out_batch->GetRow(row_idx));
  TupleRow* out_row = reinterpret_cast<TupleRow*>(out_row_mem);

  int rows_returned = 0;
  int probe_rows = probe_batch->num_rows();

  while (true) {
    TupleRow* matched_build_row = NULL;
    // Create output row for each matching build row
    while ((matched_build_row = hash_tbl_iterator_.GetNext()) != NULL) {
      CreateOutputRow(out_row, current_probe_row_, matched_build_row);

      if (!EvalOtherJoinConjuncts(out_row)) continue;
      matched_probe_ = true;

      if (EvalConjuncts(conjuncts_, out_row)) {
        ++rows_returned;
        // Filled up out batch or hit limit
        if (UNLIKELY(rows_returned == max_added_rows)) goto end;
        // Advance to next out row
        out_row_mem += out_batch->row_byte_size();
        out_row = reinterpret_cast<TupleRow*>(out_row_mem);
      }

      // Handle left semi-join
      if (match_one_build_) {
        hash_tbl_iterator_.SkipToEnd();
        break;
      }
    }

    // Handle left outer-join
    if (!matched_probe_ && match_all_probe_) {
      CreateOutputRow(out_row, current_probe_row_, NULL);
      matched_probe_ = true;
      if (EvalConjuncts(conjuncts_, out_row)) {
        ++rows_returned;
        if (UNLIKELY(rows_returned == max_added_rows)) goto end;
        // Advance to next out row
        out_row_mem += out_batch->row_byte_size();
        out_row = reinterpret_cast<TupleRow*>(out_row_mem);
      }
    }
    
    if (!hash_tbl_iterator_.HasNext()) {
      // Advance to the next probe row
      if (UNLIKELY(probe_batch_pos_ == probe_rows)) goto end;
      current_probe_row_ = probe_batch->GetRow(probe_batch_pos_++);
      hash_tbl_->Scan(current_probe_row_, &hash_tbl_iterator_);
      matched_probe_ = false;
    }
  }

end:
  if (match_one_build_ && matched_probe_) hash_tbl_iterator_.SkipToEnd();
  out_batch->CommitRows(rows_returned);
  return rows_returned;
}

// TODO: HashFn and EqualsFn should be codegen'd for this function
void HashJoinNode::ProcessBuildBatch(RowBatch* build_batch) {
  // insert build row into our hash table
  for (int i = 0; i < build_batch->num_rows(); ++i) {
    hash_tbl_->Insert(build_batch->GetRow(i));
  }
}

