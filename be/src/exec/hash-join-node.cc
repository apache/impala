// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/hash-join-node.h"

#include <sstream>
#include <glog/logging.h>

#include "exec/hash-table.h"
#include "exprs/expr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"

#include "gen-cpp/PlanNodes_types.h"

using namespace impala;
using namespace std;
using namespace boost;

HashJoinNode::HashJoinNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs) {
  // TODO: log errors in runtime state
  Status status = Init(pool, tnode);
  DCHECK(status.ok());
}

HashJoinNode::~HashJoinNode() {
  for (int i = 0; i < build_pools_.size(); ++i) {
    delete build_pools_[i];
  }
}

Status HashJoinNode::Init(ObjectPool* pool, const TPlanNode& tnode) {
  DCHECK(tnode.__isset.hash_join_node);
  const vector<TEqJoinCondition>& join_preds = tnode.hash_join_node.join_predicates;
  for (int i = 0; i < join_preds.size(); ++i) {
    Expr* expr;
    RETURN_IF_ERROR(Expr::CreateExprTree(pool, join_preds[i].left, &expr));
    probe_exprs_.push_back(expr);
    RETURN_IF_ERROR(Expr::CreateExprTree(pool, join_preds[i].right, &expr));
    build_exprs_.push_back(expr);
  }
  return Status::OK;
}

Status HashJoinNode::Prepare(RuntimeState* state) {
  ExecNode::Prepare(state);
  // build and probe exprs are evaluated in the context of the rows produced by our
  // right and left children, respectively
  Expr::Prepare(build_exprs_, state, child(1)->row_desc());
  Expr::Prepare(probe_exprs_, state, child(0)->row_desc());
  // our right child/build input materializes exactly one tuple
  DCHECK_EQ(child(1)->row_desc().tuple_descriptors().size(), 1);
  hash_tbl_.reset(
      new HashTable(build_exprs_, probe_exprs_, child(1)->row_desc(), 0, false));
  TupleDescriptor* build_tuple_desc = child(1)->row_desc().tuple_descriptors()[0];
  build_tuple_idx_ = row_descriptor_.GetTupleIdx(build_tuple_desc->id());
  probe_batch_.reset(
      new RowBatch(row_descriptor_.tuple_descriptors(), state->batch_size()));
  return Status::OK;
}

Status HashJoinNode::Open(RuntimeState* state) {
  eos_ = false;

  // do a full scan of child(1) and store everything in hash_tbl_
  RowBatch build_batch(
      child(1)->row_desc().tuple_descriptors(), state->batch_size());
  RETURN_IF_ERROR(child(1)->Open(state));
  while (true) {
    RETURN_IF_ERROR(child(1)->GetNext(state, &build_batch));
    for (int i = 0; i < build_batch.num_rows(); ++i) {
      hash_tbl_->Insert(build_batch.GetRow(i)->GetTuple(0));
    }
    // hang on to tuple memory until the join is done
    build_batch.ReleaseMemPools(&build_pools_);
    if (build_batch.num_rows() < build_batch.capacity()) break;
  }
  RETURN_IF_ERROR(child(1)->Close(state));

  RETURN_IF_ERROR(child(0)->Open(state));
  // prime probe batch
  RETURN_IF_ERROR(child(0)->GetNext(state, probe_batch_.get()));
  probe_batch_pos_ = 0;
  return Status::OK;
}

Status HashJoinNode::GetNext(RuntimeState* state, RowBatch* out_batch) {
  while (!eos_) {
    Tuple* tuple;
    while (!out_batch->IsFull() && (tuple = hash_tbl_iterator_.GetNext()) != NULL) {
      // copy probe row to output
      int row_idx = out_batch->AddRow();
      TupleRow* out_row = out_batch->GetRow(row_idx);
      out_batch->CopyRow(current_probe_row_, out_row);
      out_row->SetTuple(build_tuple_idx_, tuple);
      if (EvalConjuncts(out_row)) {
        out_batch->CommitLastRow();
      }
    }
    if (out_batch->IsFull()) return Status::OK;

    if (probe_batch_pos_ == probe_batch_->num_rows()) {
      // get new probe batch
      if (probe_batch_->num_rows() < probe_batch_->capacity()) {
        // this was the last probe batch
        eos_ = true;
        return Status::OK;
      }
      // pass on pools, out_batch might still need them
      out_batch->AddMemPools(probe_batch_.get());
      RETURN_IF_ERROR(child(0)->GetNext(state, probe_batch_.get()));
      probe_batch_pos_ = 0;
    }

    // join remaining rows in probe_batch_
    current_probe_row_ = probe_batch_->GetRow(probe_batch_pos_++);
    hash_tbl_->Scan(current_probe_row_, &hash_tbl_iterator_);
  }
  return Status::OK;
}

Status HashJoinNode::Close(RuntimeState* state) {
  RETURN_IF_ERROR(child(0)->Close(state));
  return Status::OK;
}

void HashJoinNode::DebugString(int indentation_level, std::stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "HashJoin(eos=" << (eos_ ? "true" : "false")
       << " probe_batch_pos=" << probe_batch_pos_
       << " hash_tbl=";
  hash_tbl_->DebugString(indentation_level, out);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
