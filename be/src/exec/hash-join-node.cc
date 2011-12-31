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
  : ExecNode(pool, tnode, descs),
    join_op_(tnode.hash_join_node.join_op),
    build_pool_(new MemPool()) {
  // TODO: log errors in runtime state
  Status status = Init(pool, tnode);
  DCHECK(status.ok())
      << "HashJoinNode c'tor: Init() failed:\n"
      << status.GetErrorMsg();

  match_all_probe_ =
    (join_op_ == TJoinOp::LEFT_OUTER_JOIN || join_op_ == TJoinOp::FULL_OUTER_JOIN);
  match_one_build_ = (join_op_ == TJoinOp::LEFT_SEMI_JOIN);
  match_all_build_ =
    (join_op_ == TJoinOp::RIGHT_OUTER_JOIN || join_op_ == TJoinOp::FULL_OUTER_JOIN);
}

Status HashJoinNode::Init(ObjectPool* pool, const TPlanNode& tnode) {
  DCHECK(tnode.__isset.hash_join_node);
  const vector<TEqJoinCondition>& eq_join_conjuncts =
      tnode.hash_join_node.eq_join_conjuncts;
  for (int i = 0; i < eq_join_conjuncts.size(); ++i) {
    Expr* expr;
    RETURN_IF_ERROR(Expr::CreateExprTree(pool, eq_join_conjuncts[i].left, &expr));
    probe_exprs_.push_back(expr);
    RETURN_IF_ERROR(Expr::CreateExprTree(pool, eq_join_conjuncts[i].right, &expr));
    build_exprs_.push_back(expr);
  }
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool, tnode.hash_join_node.other_join_conjuncts,
                            &other_join_conjuncts_));
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
  probe_batch_.reset(new RowBatch(row_descriptor_, state->batch_size()));
  return Status::OK;
}

Status HashJoinNode::Open(RuntimeState* state) {
  eos_ = false;

  // do a full scan of child(1) and store everything in hash_tbl_
  RowBatch build_batch(child(1)->row_desc(), state->batch_size());
  RETURN_IF_ERROR(child(1)->Open(state));
  while (true) {
    bool eos;
    RETURN_IF_ERROR(child(1)->GetNext(state, &build_batch, &eos));
    // take ownership of tuple data of build_batch
    build_pool_->AcquireData(build_batch.tuple_data_pool(), false);

    // insert build tuples into our hash table
    for (int i = 0; i < build_batch.num_rows(); ++i) {
      Tuple* t = build_batch.GetRow(i)->GetTuple(0);
      VLOG(1) << "build tuple " << t << ": "
              << PrintTuple(t, *child(1)->row_desc().tuple_descriptors()[0]);
      hash_tbl_->Insert(t);
      VLOG(1) << hash_tbl_->DebugString();
    }

    if (eos) break;
    build_batch.Reset();
  }
  RETURN_IF_ERROR(child(1)->Close(state));

  VLOG(1) << hash_tbl_->DebugString();

  RETURN_IF_ERROR(child(0)->Open(state));

  // seed probe batch and current_probe_row_, etc.
  bool dummy;
  RETURN_IF_ERROR(child(0)->GetNext(state, probe_batch_.get(), &dummy));
  probe_batch_pos_ = 0;
  if (probe_batch_->num_rows() == 0) {
    eos_ = true;
    return Status::OK;
  }
  matched_probe_ = false;
  current_probe_row_ = probe_batch_->GetRow(probe_batch_pos_++);
  VLOG(1) << "probe row: " << PrintRow(current_probe_row_, child(0)->row_desc());
  matched_probe_ = false;
  hash_tbl_->Scan(current_probe_row_, &hash_tbl_iterator_);

  return Status::OK;
}

inline TupleRow* HashJoinNode::CreateOutputRow(
    RowBatch* out_batch, TupleRow* probe_row, Tuple* build_tuple) {
  DCHECK(!out_batch->IsFull());
  // copy probe row to output
  int row_idx = out_batch->AddRow();
  TupleRow* out_row = out_batch->GetRow(row_idx);
  if (probe_row != NULL) {
    out_batch->CopyRow(probe_row, out_row);
  } else {
    out_batch->ClearRow(out_row);
  }
  out_row->SetTuple(build_tuple_idx_, build_tuple);
  return out_row;
}

Status HashJoinNode::GetNext(RuntimeState* state, RowBatch* out_batch, bool* eos) {
  if (ReachedLimit()) {
    *eos = true;
    return Status::OK;
  }

  while (!eos_) {
    Tuple* tuple;
    // create output rows as long as:
    // 1) we haven't already created an output row for the probe row and are doing
    //    a semi-join;
    // 2) there are more matching build rows
    while (!(matched_probe_ && match_one_build_)
           && (tuple = hash_tbl_iterator_.GetNext()) != NULL) {
      TupleRow* out_row = CreateOutputRow(out_batch, current_probe_row_, tuple);
     if (!EvalConjuncts(other_join_conjuncts_, out_row)) continue;
      // we have a match for the purpose of the (outer?) join as soon as we
      // satisfy the JOIN clause conjuncts
      matched_probe_ = true;
      if (match_all_build_) {
        // remember that we matched this build tuple
        joined_build_tuples_.insert(tuple);
        VLOG(1) << "joined build tuple: " << tuple;
      }
      if (EvalConjuncts(conjuncts_, out_row)) {
        out_batch->CommitLastRow();
        VLOG(1) << "match row: " << PrintRow(out_row, row_desc());
        ++num_rows_returned_;
        if (out_batch->IsFull() || ReachedLimit()) {
          *eos = ReachedLimit();
          return Status::OK;
        }
      }
      if (match_one_build_) break;
    }

    // check whether we need to output the current probe row before
    // getting a new probe batch
    if (match_all_probe_ && !matched_probe_) {
      TupleRow* out_row = CreateOutputRow(out_batch, current_probe_row_, NULL);
      if (EvalConjuncts(conjuncts_, out_row)) {
        out_batch->CommitLastRow();
        VLOG(1) << "match row: " << PrintRow(out_row, row_desc());
        ++num_rows_returned_;
        matched_probe_ = true;
        if (out_batch->IsFull() || ReachedLimit()) {
          *eos = ReachedLimit();
          return Status::OK;
        }
      }
    }

    if (probe_batch_pos_ == probe_batch_->num_rows()) {
      // get new probe batch
      if (probe_batch_->num_rows() < probe_batch_->capacity()) {
        // this was the last probe batch
        eos_ = true;
      } else {
        // pass on pools, out_batch might still need them
        probe_batch_->TransferTupleDataOwnership(out_batch);
        bool dummy;  // we ignore eos and use the # of returned rows instead
        RETURN_IF_ERROR(child(0)->GetNext(state, probe_batch_.get(), &dummy));
        probe_batch_pos_ = 0;
        if (probe_batch_->num_rows() == 0) {
          // we're done; don't exit here, we might still need to finish up an outer join
          eos_ = true;
        }
      }
      // finish up right outer join
      if (eos_ && match_all_build_) hash_tbl_->Scan(NULL, &hash_tbl_iterator_);
    }

    if (eos_) break;

    // join remaining rows in probe_batch_
    current_probe_row_ = probe_batch_->GetRow(probe_batch_pos_++);
    VLOG(1) << "probe row: " << PrintRow(current_probe_row_, child(0)->row_desc());
    matched_probe_ = false;
    hash_tbl_->Scan(current_probe_row_, &hash_tbl_iterator_);
  }

  *eos = true;
  if (match_all_build_) {
    // output remaining unmatched build rows
    Tuple* tuple = NULL;
    while (!out_batch->IsFull() && (tuple = hash_tbl_iterator_.GetNext()) != NULL) {
      if (joined_build_tuples_.find(tuple) != joined_build_tuples_.end()) continue;
      TupleRow* out_row = CreateOutputRow(out_batch, NULL, tuple);
      if (EvalConjuncts(conjuncts_, out_row)) {
        out_batch->CommitLastRow();
        VLOG(1) << "match row: " << PrintRow(out_row, row_desc());
        ++num_rows_returned_;
        if (ReachedLimit()) {
          *eos = true;
          return Status::OK;
        }
      }
    }
    // we're done if there are no more tuples left to check
    *eos = tuple == NULL;
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
