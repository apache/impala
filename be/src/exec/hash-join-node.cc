// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/hash-join-node.h"

#include <sstream>
#include <glog/logging.h>

#include "exec/hash-table.h"
#include "exprs/expr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"

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
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  build_timer_ = 
      ADD_COUNTER(runtime_profile(), "BuildTime", TCounterType::CPU_TICKS);
  probe_timer_ = 
      ADD_COUNTER(runtime_profile(), "ProbeTime", TCounterType::CPU_TICKS);
  build_row_counter_ = 
      ADD_COUNTER(runtime_profile(), "BuildRows", TCounterType::UNIT);
  probe_row_counter_ =
      ADD_COUNTER(runtime_profile(), "ProbeRows", TCounterType::UNIT);

  // build and probe exprs are evaluated in the context of the rows produced by our
  // right and left children, respectively
  Expr::Prepare(build_exprs_, state, child(1)->row_desc());
  Expr::Prepare(probe_exprs_, state, child(0)->row_desc());

  // other_join_conjuncts_ are evaluated in the context of the rows produced by this node
  Expr::Prepare(other_join_conjuncts_, state, row_descriptor_);

  // pre-compute the tuple index of build tuples in the output row
  build_tuple_size_ = child(1)->row_desc().tuple_descriptors().size();
  build_tuple_idx_.reserve(build_tuple_size_);
  for (int i = 0; i < build_tuple_size_; ++i) {
    TupleDescriptor* build_tuple_desc = child(1)->row_desc().tuple_descriptors()[i];
    build_tuple_idx_.push_back(row_descriptor_.GetTupleIdx(build_tuple_desc->id()));
  }

  hash_tbl_.reset(new HashTable(build_exprs_, probe_exprs_, child(1)->row_desc(), false));
  probe_batch_.reset(new RowBatch(row_descriptor_, state->batch_size()));
  return Status::OK;
}

Status HashJoinNode::Open(RuntimeState* state) {
  COUNTER_SCOPED_TIMER(runtime_profile_->total_time_counter());
  eos_ = false;

  // Do a full scan of child(1) and store everything in hash_tbl_
  // The hash join node needs to keep in memory all build tuples, including the tuple
  // row ptrs.  Create a new row batch, passing it the build_tuple_pool from which the
  // tuple ptrs array will be allocated.
  RowBatch build_batch(child(1)->row_desc(), state->batch_size(), build_pool_.get());
  RETURN_IF_ERROR(child(1)->Open(state));
  while (true) {
    COUNTER_SCOPED_TIMER(build_timer_);
    bool eos;
    RETURN_IF_ERROR(child(1)->GetNext(state, &build_batch, &eos));
    // take ownership of tuple data of build_batch
    build_pool_->AcquireData(build_batch.tuple_data_pool(), false);

    // insert build row into our hash table
    for (int i = 0; i < build_batch.num_rows(); ++i) {
      TupleRow* t = build_batch.GetRow(i);
      VLOG(1) << "build row " << t << ": " << PrintRow(t, child(1)->row_desc());
      hash_tbl_->Insert(t);
      VLOG(1) << hash_tbl_->DebugString();
    }

    if (eos) break;

    // allocate TupleRow memory for the next batch from buld_pool_
    build_batch.Reset(build_pool_.get());
  }
  COUNTER_UPDATE(build_row_counter_, hash_tbl_->size());
  RETURN_IF_ERROR(child(1)->Close(state));

  VLOG(1) << hash_tbl_->DebugString();

  COUNTER_SCOPED_TIMER(probe_timer_);
  RETURN_IF_ERROR(child(0)->Open(state));

  // seed probe batch and current_probe_row_, etc.
  bool dummy;
  RETURN_IF_ERROR(child(0)->GetNext(state, probe_batch_.get(), &dummy));
  COUNTER_UPDATE(probe_row_counter_, probe_batch_->num_rows());
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
    RowBatch* out_batch, TupleRow* probe_row, TupleRow* build_row) {
  DCHECK(!out_batch->IsFull());
  // copy probe row to output
  int row_idx = out_batch->AddRow();
  TupleRow* out_row = out_batch->GetRow(row_idx);
  if (probe_row != NULL) {
    out_batch->CopyRow(probe_row, out_row);
  } else {
    out_batch->ClearRow(out_row);
  }

  // TODO : we can eventually codegen these copies, which means we can get rid of the
  // loop and all indices are hardwired, ie, it would pipeline perfectly

  // copy the build row to out_row at build_tuple_idx
  if (build_row != NULL) {
    for (int i = 0; i < build_tuple_size_; ++i) {
      out_row->SetTuple(build_tuple_idx_[i], build_row->GetTuple(i));
    }
  } else {
    for (int i = 0; i < build_tuple_size_; ++i) {
      out_row->SetTuple(build_tuple_idx_[i], NULL);
    }
  }

  return out_row;
}

Status HashJoinNode::GetNext(RuntimeState* state, RowBatch* out_batch, bool* eos) {
  COUNTER_SCOPED_TIMER(runtime_profile_->total_time_counter());
  COUNTER_SCOPED_TIMER(probe_timer_);
  if (ReachedLimit()) {
    *eos = true;
    return Status::OK;
  }

  while (!eos_) {
    TupleRow* matched_build_row;
    // create output rows as long as:
    // 1) we haven't already created an output row for the probe row and are doing
    //    a semi-join;
    // 2) there are more matching build rows
    while (!(matched_probe_ && match_one_build_)
           && (matched_build_row = hash_tbl_iterator_.GetNext()) != NULL) {
      TupleRow* out_row = CreateOutputRow(out_batch, current_probe_row_,
                                          matched_build_row);
     if (!EvalConjuncts(other_join_conjuncts_, out_row)) continue;
      // we have a match for the purpose of the (outer?) join as soon as we
      // satisfy the JOIN clause conjuncts
      matched_probe_ = true;
      if (match_all_build_) {
        // remember that we matched this build row
        joined_build_rows_.insert(matched_build_row);
        VLOG(1) << "joined build row: " << matched_build_row;
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
        probe_batch_->TransferTupleData(out_batch);
        bool dummy;  // we ignore eos and use the # of returned rows instead
        RETURN_IF_ERROR(child(0)->GetNext(state, probe_batch_.get(), &dummy));
        COUNTER_UPDATE(probe_row_counter_, probe_batch_->num_rows());
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
    TupleRow* build_row = NULL;
    while (!out_batch->IsFull() && (build_row = hash_tbl_iterator_.GetNext()) != NULL) {
      if (joined_build_rows_.find(build_row) != joined_build_rows_.end()) {
        continue;
      }
      TupleRow* out_row = CreateOutputRow(out_batch, NULL, build_row);
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
    // we're done if there are no more rows left to check
    *eos = build_row == NULL;
  }
  return Status::OK;
}

Status HashJoinNode::Close(RuntimeState* state) {
  RETURN_IF_ERROR(child(0)->Close(state));
  RETURN_IF_ERROR(ExecNode::Close(state));
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
