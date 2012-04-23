// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/aggregation-node.h"

#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

using namespace impala;

// Functions in this file are cross compiled to IR with clang.  These functions
// are modified at runtime with a query specific codegen'd UpdateAggTuple

void AggregationNode::ProcessRowBatchNoGrouping(RowBatch* batch) {
  for (int i = 0; i < batch->num_rows(); ++i) {
    current_row_ = batch->GetRow(i);
    UpdateAggTuple(singleton_output_tuple_, current_row_);
  }
}

void AggregationNode::ProcessRowBatchWithGrouping(RowBatch* batch) {
  for (int i = 0; i < batch->num_rows(); ++i) {
    current_row_ = batch->GetRow(i);
    // Compute and cache the grouping exprs for current_row_
    ComputeGroupingValues();
    AggregationTuple* agg_tuple = NULL; 
    // find(NULL) finds the entry for current_row_
    HashTable::iterator entry = hash_tbl_->find(NULL);
    if (entry == hash_tbl_->end()) {
      // new entry
      agg_tuple = ConstructAggTuple();
      hash_tbl_->insert(reinterpret_cast<Tuple*>(agg_tuple));
    } else {
      agg_tuple = reinterpret_cast<AggregationTuple*>(*entry);
    }
    UpdateAggTuple(agg_tuple, current_row_);
  }
}

