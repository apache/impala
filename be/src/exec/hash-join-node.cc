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

#include "exec/hash-join-node.h"

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "exec/hash-table.inline.h"
#include "exprs/expr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"

#include "gen-cpp/PlanNodes_types.h"

DECLARE_string(cgroup_hierarchy_path);

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;

const char* HashJoinNode::LLVM_CLASS_NAME = "class.impala::HashJoinNode";

HashJoinNode::HashJoinNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : BlockingJoinNode("HashJoinNode", tnode.hash_join_node.join_op, pool, tnode, descs),
    codegen_process_build_batch_fn_(NULL),
    process_build_batch_fn_(NULL),
    codegen_process_probe_batch_fn_(NULL),
    process_probe_batch_fn_(NULL) {
  match_all_probe_ =
    (join_op_ == TJoinOp::LEFT_OUTER_JOIN || join_op_ == TJoinOp::FULL_OUTER_JOIN);
  match_one_build_ = (join_op_ == TJoinOp::LEFT_SEMI_JOIN);
  match_all_build_ =
    (join_op_ == TJoinOp::RIGHT_OUTER_JOIN || join_op_ == TJoinOp::FULL_OUTER_JOIN);
}

Status HashJoinNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(BlockingJoinNode::Init(tnode));
  DCHECK(tnode.__isset.hash_join_node);
  const vector<TEqJoinCondition>& eq_join_conjuncts =
      tnode.hash_join_node.eq_join_conjuncts;
  for (int i = 0; i < eq_join_conjuncts.size(); ++i) {
    Expr* expr;
    RETURN_IF_ERROR(Expr::CreateExprTree(pool_, eq_join_conjuncts[i].left, &expr));
    probe_exprs_.push_back(expr);
    RETURN_IF_ERROR(Expr::CreateExprTree(pool_, eq_join_conjuncts[i].right, &expr));
    build_exprs_.push_back(expr);
  }
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool_, tnode.hash_join_node.other_join_conjuncts,
                            &other_join_conjuncts_));
  return Status::OK;
}

Status HashJoinNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(BlockingJoinNode::Prepare(state));

  build_buckets_counter_ =
      ADD_COUNTER(runtime_profile(), "BuildBuckets", TCounterType::UNIT);
  hash_tbl_load_factor_counter_ =
      ADD_COUNTER(runtime_profile(), "LoadFactor", TCounterType::DOUBLE_VALUE);

  // build and probe exprs are evaluated in the context of the rows produced by our
  // right and left children, respectively
  RETURN_IF_ERROR(Expr::Prepare(build_exprs_, state, child(1)->row_desc(), false));
  RETURN_IF_ERROR(Expr::Prepare(probe_exprs_, state, child(0)->row_desc(), false));

  // other_join_conjuncts_ are evaluated in the context of the rows produced by this node
  RETURN_IF_ERROR(Expr::Prepare(other_join_conjuncts_, state, row_descriptor_, false));

  // TODO: default buckets
  bool stores_nulls =
      join_op_ == TJoinOp::RIGHT_OUTER_JOIN || join_op_ == TJoinOp::FULL_OUTER_JOIN;
  hash_tbl_.reset(new HashTable(state, build_exprs_, probe_exprs_, build_tuple_size_,
      stores_nulls, false, id(), mem_tracker()));

  if (state->codegen_enabled()) {
    // Codegen for hashing rows
    Function* hash_fn = hash_tbl_->CodegenHashCurrentRow(state->codegen());
    if (hash_fn == NULL) return Status::OK;

    // Codegen for build path
    codegen_process_build_batch_fn_ =
        CodegenProcessBuildBatch(state->codegen(), hash_fn);
    if (codegen_process_build_batch_fn_ != NULL) {
      state->codegen()->AddFunctionToJit(codegen_process_build_batch_fn_,
          reinterpret_cast<void**>(&process_build_batch_fn_));
      AddRuntimeExecOption("Build Side Codegen Enabled");
    }

    // Codegen for probe path (only for left joins)
    if (!match_all_build_) {
      codegen_process_probe_batch_fn_ =
          CodegenProcessProbeBatch(state->codegen(), hash_fn);
      if (codegen_process_probe_batch_fn_ != NULL) {
        state->codegen()->AddFunctionToJit(codegen_process_probe_batch_fn_,
            reinterpret_cast<void**>(&process_probe_batch_fn_));
        AddRuntimeExecOption("Probe Side Codegen Enabled");
      }
    }
  }

  return Status::OK;
}

void HashJoinNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (hash_tbl_.get() != NULL) hash_tbl_->Close();
  Expr::Close(build_exprs_, state);
  Expr::Close(probe_exprs_, state);
  Expr::Close(other_join_conjuncts_, state);
  BlockingJoinNode::Close(state);
}

Status HashJoinNode::ConstructBuildSide(RuntimeState* state) {
  RETURN_IF_ERROR(Expr::Open(build_exprs_, state));
  RETURN_IF_ERROR(Expr::Open(probe_exprs_, state));
  RETURN_IF_ERROR(Expr::Open(other_join_conjuncts_, state));

  // Do a full scan of child(1) and store everything in hash_tbl_
  // The hash join node needs to keep in memory all build tuples, including the tuple
  // row ptrs.  The row ptrs are copied into the hash table's internal structure so they
  // don't need to be stored in the build_pool_.
  RowBatch build_batch(child(1)->row_desc(), state->batch_size(), mem_tracker());
  RETURN_IF_ERROR(child(1)->Open(state));
  while (true) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->CheckQueryState());
    bool eos;
    RETURN_IF_ERROR(child(1)->GetNext(state, &build_batch, &eos));
    SCOPED_TIMER(build_timer_);
    // take ownership of tuple data of build_batch
    build_pool_->AcquireData(build_batch.tuple_data_pool(), false);
    RETURN_IF_ERROR(state->CheckQueryState());

    // Call codegen version if possible
    if (process_build_batch_fn_ == NULL) {
      ProcessBuildBatch(&build_batch);
    } else {
      process_build_batch_fn_(this, &build_batch);
    }
    VLOG_ROW << hash_tbl_->DebugString(true, &child(1)->row_desc());

    COUNTER_SET(build_row_counter_, hash_tbl_->size());
    COUNTER_SET(build_buckets_counter_, hash_tbl_->num_buckets());
    COUNTER_SET(hash_tbl_load_factor_counter_, hash_tbl_->load_factor());
    build_batch.Reset();
    DCHECK(!build_batch.AtCapacity());
    if (eos) break;
  }
  return Status::OK;
}

void HashJoinNode::InitGetNext(TupleRow* first_probe_row) {
  if (first_probe_row == NULL) {
    hash_tbl_iterator_ = hash_tbl_->Begin();
  } else {
    matched_probe_ = false;
    hash_tbl_iterator_ = hash_tbl_->Find(first_probe_row);
  }
}

Status HashJoinNode::GetNext(RuntimeState* state, RowBatch* out_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(state->CheckQueryState());
  if (ReachedLimit()) {
    *eos = true;
    return Status::OK;
  }

  // These cases are simpler and use a more efficient processing loop
  if (!match_all_build_) {
    if (eos_) {
      *eos = true;
      return Status::OK;
    }
    return LeftJoinGetNext(state, out_batch, eos);
  }

  Expr* const* other_conjuncts = &other_join_conjuncts_[0];
  int num_other_conjuncts = other_join_conjuncts_.size();

  Expr* const* conjuncts = &conjuncts_[0];
  int num_conjuncts = conjuncts_.size();

  // Explicitly manage the timer counter to avoid measuring time in the child
  // GetNext call.
  ScopedTimer<MonotonicStopWatch> probe_timer(left_child_timer_);

  while (!eos_) {
    // create output rows as long as:
    // 1) we haven't already created an output row for the probe row and are doing
    //    a semi-join;
    // 2) there are more matching build rows
    while (!hash_tbl_iterator_.AtEnd()) {
      TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
      hash_tbl_iterator_.Next<true>();

      int row_idx = out_batch->AddRow();
      TupleRow* out_row = out_batch->GetRow(row_idx);
      CreateOutputRow(out_row, current_left_child_row_, matched_build_row);
      if (!EvalConjuncts(other_conjuncts, num_other_conjuncts, out_row)) continue;
      // we have a match for the purpose of the (outer?) join as soon as we
      // satisfy the JOIN clause conjuncts
      matched_probe_ = true;
      if (match_all_build_) {
        // remember that we matched this build row
        joined_build_rows_.insert(matched_build_row);
        VLOG_ROW << "joined build row: " << matched_build_row;
      }
      if (EvalConjuncts(conjuncts, num_conjuncts, out_row)) {
        out_batch->CommitLastRow();
        VLOG_ROW << "match row: " << PrintRow(out_row, row_desc());
        ++num_rows_returned_;
        COUNTER_SET(rows_returned_counter_, num_rows_returned_);
        if (out_batch->AtCapacity() || ReachedLimit()) {
          *eos = ReachedLimit();
          return Status::OK;
        }
      }
    }

    // check whether we need to output the current probe row before
    // getting a new probe batch
    if (match_all_probe_ && !matched_probe_) {
      int row_idx = out_batch->AddRow();
      TupleRow* out_row = out_batch->GetRow(row_idx);
      CreateOutputRow(out_row, current_left_child_row_, NULL);
      if (EvalConjuncts(conjuncts, num_conjuncts, out_row)) {
        out_batch->CommitLastRow();
        VLOG_ROW << "match row: " << PrintRow(out_row, row_desc());
        ++num_rows_returned_;
        COUNTER_SET(rows_returned_counter_, num_rows_returned_);
        matched_probe_ = true;
        if (out_batch->AtCapacity() || ReachedLimit()) {
          *eos = ReachedLimit();
          return Status::OK;
        }
      }
    }

    if (left_batch_pos_ == left_batch_->num_rows()) {
      // pass on resources, out_batch might still need them
      left_batch_->TransferResourceOwnership(out_batch);
      left_batch_pos_ = 0;
      if (out_batch->AtCapacity()) return Status::OK;
      // get new probe batch
      if (!left_side_eos_) {
        while (true) {
          probe_timer.Stop();
          RETURN_IF_ERROR(child(0)->GetNext(state, left_batch_.get(), &left_side_eos_));
          probe_timer.Start();
          if (left_batch_->num_rows() == 0) {
            // Empty batches can still contain IO buffers, which need to be passed up to
            // the caller; transferring resources can fill up out_batch.
            left_batch_->TransferResourceOwnership(out_batch);
            if (left_side_eos_) {
              eos_ = true;
              break;
            }
            if (out_batch->AtCapacity()) return Status::OK;
            continue;
          } else {
            COUNTER_UPDATE(left_child_row_counter_, left_batch_->num_rows());
            break;
          }
        }
      } else {
        eos_ = true;
      }
      // finish up right outer join
      if (eos_ && match_all_build_) {
        hash_tbl_iterator_ = hash_tbl_->Begin();
      }
    }

    if (eos_) break;

    // join remaining rows in probe batch_
    current_left_child_row_ = left_batch_->GetRow(left_batch_pos_++);
    VLOG_ROW << "probe row: " << GetLeftChildRowString(current_left_child_row_);
    matched_probe_ = false;
    hash_tbl_iterator_ = hash_tbl_->Find(current_left_child_row_);
  }

  *eos = true;
  if (match_all_build_) {
    // output remaining unmatched build rows
    TupleRow* build_row = NULL;
    while (!out_batch->AtCapacity() && !hash_tbl_iterator_.AtEnd()) {
      build_row = hash_tbl_iterator_.GetRow();
      hash_tbl_iterator_.Next<false>();
      if (joined_build_rows_.find(build_row) != joined_build_rows_.end()) {
        continue;
      }
      int row_idx = out_batch->AddRow();
      TupleRow* out_row = out_batch->GetRow(row_idx);
      CreateOutputRow(out_row, NULL, build_row);
      if (EvalConjuncts(conjuncts, num_conjuncts, out_row)) {
        out_batch->CommitLastRow();
        VLOG_ROW << "match row: " << PrintRow(out_row, row_desc());
        ++num_rows_returned_;
        COUNTER_SET(rows_returned_counter_, num_rows_returned_);
        if (ReachedLimit()) {
          *eos = true;
          return Status::OK;
        }
      }
    }
    // we're done if there are no more rows left to check
    *eos = hash_tbl_iterator_.AtEnd();
  }
  return Status::OK;
}

Status HashJoinNode::LeftJoinGetNext(RuntimeState* state,
    RowBatch* out_batch, bool* eos) {
  *eos = eos_;

  ScopedTimer<MonotonicStopWatch> probe_timer(left_child_timer_);
  while (!eos_) {
    // Compute max rows that should be added to out_batch
    int64_t max_added_rows = out_batch->capacity() - out_batch->num_rows();
    if (limit() != -1) max_added_rows = min(max_added_rows, limit() - rows_returned());

    // Continue processing this row batch
    if (process_probe_batch_fn_ == NULL) {
      num_rows_returned_ +=
          ProcessProbeBatch(out_batch, left_batch_.get(), max_added_rows);
      COUNTER_SET(rows_returned_counter_, num_rows_returned_);
    } else {
      // Use codegen'd function
      num_rows_returned_ +=
          process_probe_batch_fn_(this, out_batch, left_batch_.get(), max_added_rows);
      COUNTER_SET(rows_returned_counter_, num_rows_returned_);
    }

    if (ReachedLimit() || out_batch->AtCapacity()) {
      *eos = ReachedLimit();
      break;
    }

    // Check to see if we're done processing the current probe batch
    if (hash_tbl_iterator_.AtEnd() && left_batch_pos_ == left_batch_->num_rows()) {
      left_batch_->TransferResourceOwnership(out_batch);
      left_batch_pos_ = 0;
      if (out_batch->AtCapacity()) break;
      if (left_side_eos_) {
        *eos = eos_ = true;
        break;
      } else {
        probe_timer.Stop();
        RETURN_IF_ERROR(child(0)->GetNext(state, left_batch_.get(), &left_side_eos_));
        probe_timer.Start();
        COUNTER_UPDATE(left_child_row_counter_, left_batch_->num_rows());
      }
    }
  }

  return Status::OK;
}

void HashJoinNode::AddToDebugString(int indentation_level, stringstream* out) const {
  *out << " hash_tbl=";
  *out << string(indentation_level * 2, ' ');
  *out << "HashTbl("
       << " build_exprs=" << Expr::DebugString(build_exprs_)
       << " probe_exprs=" << Expr::DebugString(probe_exprs_);
  *out << ")";
}

// This codegen'd function should only be used for left join cases so it assumes that
// the probe row is non-null.  For a left outer join, the IR looks like:
// define void @CreateOutputRow(%"class.impala::BlockingJoinNode"* %this_ptr,
//                              %"class.impala::TupleRow"* %out_arg,
//                              %"class.impala::TupleRow"* %probe_arg,
//                              %"class.impala::TupleRow"* %build_arg) {
// entry:
//   %out = bitcast %"class.impala::TupleRow"* %out_arg to i8**
//   %probe = bitcast %"class.impala::TupleRow"* %probe_arg to i8**
//   %build = bitcast %"class.impala::TupleRow"* %build_arg to i8**
//   %0 = bitcast i8** %out to i8*
//   %1 = bitcast i8** %probe to i8*
//   call void @llvm.memcpy.p0i8.p0i8.i32(i8* %0, i8* %1, i32 16, i32 16, i1 false)
//   %is_build_null = icmp eq i8** %build, null
//   br i1 %is_build_null, label %build_null, label %build_not_null
//
// build_not_null:                                   ; preds = %entry
//   %dst_tuple_ptr1 = getelementptr i8** %out, i32 1
//   %src_tuple_ptr = getelementptr i8** %build, i32 0
//   %2 = load i8** %src_tuple_ptr
//   store i8* %2, i8** %dst_tuple_ptr1
//   ret void
//
// build_null:                                       ; preds = %entry
//   %dst_tuple_ptr = getelementptr i8** %out, i32 1
//   store i8* null, i8** %dst_tuple_ptr
//   ret void
// }
Function* HashJoinNode::CodegenCreateOutputRow(LlvmCodeGen* codegen) {
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

  // Copy probe row
  codegen->CodegenMemcpy(&builder, out_row_arg, probe_row_arg, result_tuple_row_size_);

  // Copy build row.
  BasicBlock* build_not_null_block = BasicBlock::Create(context, "build_not_null", fn);
  BasicBlock* build_null_block = NULL;

  if (match_all_probe_) {
    // build tuple can be null
    build_null_block = BasicBlock::Create(context, "build_null", fn);
    Value* is_build_null = builder.CreateIsNull(build_row_arg, "is_build_null");
    builder.CreateCondBr(is_build_null, build_null_block, build_not_null_block);

    // Set tuple build ptrs to NULL
    builder.SetInsertPoint(build_null_block);
    for (int i = 0; i < build_tuple_size_; ++i) {
      Value* array_idx[] = { codegen->GetIntConstant(TYPE_INT, build_tuple_idx_[i]) };
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
  for (int i = 0; i < build_tuple_size_; ++i) {
    Value* dst_idx[] = { codegen->GetIntConstant(TYPE_INT, build_tuple_idx_[i]) };
    Value* src_idx[] = { codegen->GetIntConstant(TYPE_INT, i) };
    Value* dst = builder.CreateGEP(out_row_arg, dst_idx, "dst_tuple_ptr");
    Value* src = builder.CreateGEP(build_row_arg, src_idx, "src_tuple_ptr");
    builder.CreateStore(builder.CreateLoad(src), dst);
  }
  builder.CreateRetVoid();

  return codegen->FinalizeFunction(fn);
}

Function* HashJoinNode::CodegenProcessBuildBatch(LlvmCodeGen* codegen,
    Function* hash_fn) {
  // Get cross compiled function
  Function* process_build_batch_fn = codegen->GetFunction(
      IRFunction::HASH_JOIN_PROCESS_BUILD_BATCH);
  DCHECK(process_build_batch_fn != NULL);

  // Codegen for evaluating build rows
  Function* eval_row_fn = hash_tbl_->CodegenEvalTupleRow(codegen, true);
  if (eval_row_fn == NULL) return NULL;

  int replaced = 0;
  // Replace call sites
  process_build_batch_fn = codegen->ReplaceCallSites(process_build_batch_fn, false,
      eval_row_fn, "EvalBuildRow", &replaced);
  DCHECK_EQ(replaced, 1);

  process_build_batch_fn = codegen->ReplaceCallSites(process_build_batch_fn, false,
      hash_fn, "HashCurrentRow", &replaced);
  DCHECK_EQ(replaced, 1);

  return codegen->OptimizeFunctionWithExprs(process_build_batch_fn);
}

Function* HashJoinNode::CodegenProcessProbeBatch(LlvmCodeGen* codegen,
    Function* hash_fn) {
  // Get cross compiled function
  Function* process_probe_batch_fn = codegen->GetFunction(
      IRFunction::HASH_JOIN_PROCESS_PROBE_BATCH);
  DCHECK(process_probe_batch_fn != NULL);

  // Codegen HashTable::Equals
  Function* equals_fn = hash_tbl_->CodegenEquals(codegen);
  if (equals_fn == NULL) return NULL;

  // Codegen for evaluating build rows
  Function* eval_row_fn = hash_tbl_->CodegenEvalTupleRow(codegen, false);
  if (eval_row_fn == NULL) return NULL;

  // Codegen CreateOutputRow
  Function* create_output_row_fn = CodegenCreateOutputRow(codegen);
  if (create_output_row_fn == NULL) return NULL;

  // Codegen evaluating other join conjuncts
  Function* join_conjuncts_fn = CodegenEvalConjuncts(codegen, other_join_conjuncts_);
  if (join_conjuncts_fn == NULL) return NULL;

  // Codegen evaluating conjuncts
  Function* conjuncts_fn = CodegenEvalConjuncts(codegen, conjuncts_);
  if (conjuncts_fn == NULL) return NULL;

  // Replace all call sites with codegen version
  int replaced = 0;
  process_probe_batch_fn = codegen->ReplaceCallSites(process_probe_batch_fn, false,
      hash_fn, "HashCurrentRow", &replaced);
  DCHECK_EQ(replaced, 1);

  process_probe_batch_fn = codegen->ReplaceCallSites(process_probe_batch_fn, false,
      eval_row_fn, "EvalProbeRow", &replaced);
  DCHECK_EQ(replaced, 1);

  process_probe_batch_fn = codegen->ReplaceCallSites(process_probe_batch_fn, false,
      create_output_row_fn, "CreateOutputRow", &replaced);
  DCHECK_EQ(replaced, 2);

  process_probe_batch_fn = codegen->ReplaceCallSites(process_probe_batch_fn, false,
      conjuncts_fn, "EvalConjuncts", &replaced);
  DCHECK_EQ(replaced, 2);

  process_probe_batch_fn = codegen->ReplaceCallSites(process_probe_batch_fn, false,
      join_conjuncts_fn, "EvalOtherJoinConjuncts", &replaced);
  DCHECK_EQ(replaced, 1);

  process_probe_batch_fn = codegen->ReplaceCallSites(process_probe_batch_fn, false,
      equals_fn, "Equals", &replaced);
  DCHECK_EQ(replaced, 2);

  return codegen->OptimizeFunctionWithExprs(process_probe_batch_fn);
}
