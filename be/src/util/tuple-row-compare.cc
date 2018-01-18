// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/tuple-row-compare.h"

#include <gutil/strings/substitute.h>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"

using namespace impala;
using namespace strings;

Status TupleRowComparator::Open(ObjectPool* pool, RuntimeState* state,
    MemPool* expr_perm_pool, MemPool* expr_results_pool) {
  if (ordering_expr_evals_lhs_.empty()) {
    RETURN_IF_ERROR(ScalarExprEvaluator::Create(ordering_exprs_, state, pool,
        expr_perm_pool, expr_results_pool, &ordering_expr_evals_lhs_));
    RETURN_IF_ERROR(ScalarExprEvaluator::Open(ordering_expr_evals_lhs_, state));
  }
  DCHECK_EQ(ordering_exprs_.size(), ordering_expr_evals_lhs_.size());
  if (ordering_expr_evals_rhs_.empty()) {
    RETURN_IF_ERROR(ScalarExprEvaluator::Clone(pool, state, expr_perm_pool,
        expr_results_pool, ordering_expr_evals_lhs_, &ordering_expr_evals_rhs_));
  }
  DCHECK_EQ(ordering_expr_evals_lhs_.size(), ordering_expr_evals_rhs_.size());
  return Status::OK();
}

void TupleRowComparator::Close(RuntimeState* state) {
  ScalarExprEvaluator::Close(ordering_expr_evals_rhs_, state);
  ScalarExprEvaluator::Close(ordering_expr_evals_lhs_, state);
}

int TupleRowComparator::CompareInterpreted(
    const TupleRow* lhs, const TupleRow* rhs) const {
  DCHECK_EQ(ordering_exprs_.size(), ordering_expr_evals_lhs_.size());
  DCHECK_EQ(ordering_expr_evals_lhs_.size(), ordering_expr_evals_rhs_.size());
  for (int i = 0; i < ordering_expr_evals_lhs_.size(); ++i) {
    void* lhs_value = ordering_expr_evals_lhs_[i]->GetValue(lhs);
    void* rhs_value = ordering_expr_evals_rhs_[i]->GetValue(rhs);

    // The sort order of NULLs is independent of asc/desc.
    if (lhs_value == NULL && rhs_value == NULL) continue;
    if (lhs_value == NULL && rhs_value != NULL) return nulls_first_[i];
    if (lhs_value != NULL && rhs_value == NULL) return -nulls_first_[i];

    int result = RawValue::Compare(lhs_value, rhs_value, ordering_exprs_[i]->type());
    if (!is_asc_[i]) result = -result;
    if (result != 0) return result;
    // Otherwise, try the next Expr
  }
  return 0; // fully equivalent key
}

Status TupleRowComparator::Codegen(RuntimeState* state) {
  llvm::Function* fn;
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != NULL);
  RETURN_IF_ERROR(CodegenCompare(codegen, &fn));
  codegend_compare_fn_ = state->obj_pool()->Add(new CompareFn);
  codegen->AddFunctionToJit(fn, reinterpret_cast<void**>(codegend_compare_fn_));
  return Status::OK();
}

// Codegens an unrolled version of Compare(). Uses codegen'd key exprs and injects
// nulls_first_ and is_asc_ values.
//
// Example IR for comparing an int column then a float column:
//
// ; Function Attrs: alwaysinline
// define i32 @Compare(%"class.impala::ScalarExprEvaluator"**
//                         %ordering_expr_evals_lhs,
//                     %"class.impala::ScalarExprEvaluator"**
//                         %ordering_expr_evals_rhs,
//                     %"class.impala::TupleRow"* %lhs,
//                     %"class.impala::TupleRow"* %rhs) #20 {
// entry:
//   %type13 = alloca %"struct.impala::ColumnType"
//   %0 = alloca float
//   %1 = alloca float
//   %type = alloca %"struct.impala::ColumnType"
//   %2 = alloca i32
//   %3 = alloca i32
//   %4 = getelementptr %"class.impala::ScalarExprEvaluator"**
//            %ordering_expr_evals_lhs, i32 0
//   %5 = load %"class.impala::ScalarExprEvaluator"** %4
//   %lhs_value = call i64 @GetSlotRef(
//       %"class.impala::ScalarExprEvaluator"* %5, %"class.impala::TupleRow"* %lhs)
//   %6 = getelementptr %"class.impala::ScalarExprEvaluator"**
//            %ordering_expr_evals_rhs, i32 0
//   %7 = load %"class.impala::ScalarExprEvaluator"** %6
//   %rhs_value = call i64 @GetSlotRef(
//       %"class.impala::ScalarExprEvaluator"* %7, %"class.impala::TupleRow"* %rhs)
//   %is_null = trunc i64 %lhs_value to i1
//   %is_null1 = trunc i64 %rhs_value to i1
//   %both_null = and i1 %is_null, %is_null1
//   br i1 %both_null, label %next_key, label %non_null
//
// non_null:                                         ; preds = %entry
//   br i1 %is_null, label %lhs_null, label %lhs_non_null
//
// lhs_null:                                         ; preds = %non_null
//   ret i32 1
//
// lhs_non_null:                                     ; preds = %non_null
//   br i1 %is_null1, label %rhs_null, label %rhs_non_null
//
// rhs_null:                                         ; preds = %lhs_non_null
//   ret i32 -1
//
// rhs_non_null:                                     ; preds = %lhs_non_null
//   %8 = ashr i64 %lhs_value, 32
//   %9 = trunc i64 %8 to i32
//   store i32 %9, i32* %3
//   %10 = bitcast i32* %3 to i8*
//   %11 = ashr i64 %rhs_value, 32
//   %12 = trunc i64 %11 to i32
//   store i32 %12, i32* %2
//   %13 = bitcast i32* %2 to i8*
//   store %"struct.impala::ColumnType" { i32 5, i32 -1, i32 -1, i32 -1,
//                                        %"class.std::vector.44" zeroinitializer,
//                                        %"class.std::vector.49" zeroinitializer },
//         %"struct.impala::ColumnType"* %type
//   %result = call i32 @_ZN6impala8RawValue7CompareEPKvS2_RKNS_10ColumnTypeE(
//       i8* %10, i8* %13, %"struct.impala::ColumnType"* %type)
//   %14 = icmp ne i32 %result, 0
//   br i1 %14, label %result_nonzero, label %next_key
//
// result_nonzero:                                   ; preds = %rhs_non_null
//   ret i32 %result
//
// next_key:                                         ; preds = %rhs_non_null, %entry
//   %15 = getelementptr %"class.impala::ScalarExprEvaluator"**
//             %ordering_expr_evals_lhs, i32 1
//   %16 = load %"class.impala::ScalarExprEvaluator"** %15
//   %lhs_value3 = call i64 @GetSlotRef1(
//       %"class.impala::ScalarExprEvaluator"* %16, %"class.impala::TupleRow"* %lhs)
//   %17 = getelementptr %"class.impala::ScalarExprEvaluator"**
//            %ordering_expr_evals_rhs, i32 1
//   %18 = load %"class.impala::ScalarExprEvaluator"** %17
//   %rhs_value4 = call i64 @GetSlotRef1(
//       %"class.impala::ScalarExprEvaluator"* %18, %"class.impala::TupleRow"* %rhs)
//   %is_null5 = trunc i64 %lhs_value3 to i1
//   %is_null6 = trunc i64 %rhs_value4 to i1
//   %both_null8 = and i1 %is_null5, %is_null6
//   br i1 %both_null8, label %next_key2, label %non_null7
//
// non_null7:                                        ; preds = %next_key
//   br i1 %is_null5, label %lhs_null9, label %lhs_non_null10
//
// lhs_null9:                                        ; preds = %non_null7
//   ret i32 1
//
// lhs_non_null10:                                   ; preds = %non_null7
//   br i1 %is_null6, label %rhs_null11, label %rhs_non_null12
//
// rhs_null11:                                       ; preds = %lhs_non_null10
//   ret i32 -1
//
// rhs_non_null12:                                   ; preds = %lhs_non_null10
//   %19 = ashr i64 %lhs_value3, 32
//   %20 = trunc i64 %19 to i32
//   %21 = bitcast i32 %20 to float
//   store float %21, float* %1
//   %22 = bitcast float* %1 to i8*
//   %23 = ashr i64 %rhs_value4, 32
//   %24 = trunc i64 %23 to i32
//   %25 = bitcast i32 %24 to float
//   store float %25, float* %0
//   %26 = bitcast float* %0 to i8*
//   store %"struct.impala::ColumnType" { i32 7, i32 -1, i32 -1, i32 -1,
//                                        %"class.std::vector.44" zeroinitializer,
//                                        %"class.std::vector.49" zeroinitializer },
//         %"struct.impala::ColumnType"* %type13
//   %result14 = call i32 @_ZN6impala8RawValue7CompareEPKvS2_RKNS_10ColumnTypeE(
//       i8* %22, i8* %26, %"struct.impala::ColumnType"* %type13)
//   %27 = icmp ne i32 %result14, 0
//   br i1 %27, label %result_nonzero15, label %next_key2
//
// result_nonzero15:                                 ; preds = %rhs_non_null12
//   ret i32 %result14
//
// next_key2:                                        ; preds = %rhs_non_null12, %next_key
//   ret i32 0
// }
Status TupleRowComparator::CodegenCompare(LlvmCodeGen* codegen, llvm::Function** fn) {
  SCOPED_TIMER(codegen->codegen_timer());
  llvm::LLVMContext& context = codegen->context();
  const vector<ScalarExpr*>& ordering_exprs = ordering_exprs_;
  llvm::Function* key_fns[ordering_exprs.size()];
  for (int i = 0; i < ordering_exprs.size(); ++i) {
    Status status = ordering_exprs[i]->GetCodegendComputeFn(codegen, &key_fns[i]);
    if (!status.ok()) {
      return Status::Expected(Substitute(
            "Could not codegen TupleRowComparator::Compare(): $0", status.GetDetail()));
    }
  }

  // Construct function signature (note that this is different than the interpreted
  // Compare() function signature):
  // int Compare(ScalarExprEvaluator** ordering_expr_evals_lhs,
  //     ScalarExprEvaluator** ordering_expr_evals_rhs,
  //     TupleRow* lhs, TupleRow* rhs)
  llvm::PointerType* expr_evals_type =
      codegen->GetStructPtrPtrType<ScalarExprEvaluator>();
  llvm::PointerType* tuple_row_type = codegen->GetStructPtrType<TupleRow>();
  LlvmCodeGen::FnPrototype prototype(codegen, "Compare", codegen->i32_type());
  prototype.AddArgument("ordering_expr_evals_lhs", expr_evals_type);
  prototype.AddArgument("ordering_expr_evals_rhs", expr_evals_type);
  prototype.AddArgument("lhs", tuple_row_type);
  prototype.AddArgument("rhs", tuple_row_type);

  LlvmBuilder builder(context);
  llvm::Value* args[4];
  *fn = prototype.GeneratePrototype(&builder, args);
  llvm::Value* lhs_evals_arg = args[0];
  llvm::Value* rhs_evals_arg = args[1];
  llvm::Value* lhs_arg = args[2];
  llvm::Value* rhs_arg = args[3];

  // Unrolled loop over each key expr
  for (int i = 0; i < ordering_exprs.size(); ++i) {
    // The start of the next key expr after this one. Used to implement "continue" logic
    // in the unrolled loop.
    llvm::BasicBlock* next_key_block = llvm::BasicBlock::Create(context, "next_key", *fn);

    // Call key_fns[i](ordering_expr_evals_lhs[i], lhs_arg)
    llvm::Value* lhs_eval = codegen->CodegenArrayAt(&builder, lhs_evals_arg, i);
    llvm::Value* lhs_args[] = {lhs_eval, lhs_arg};
    CodegenAnyVal lhs_value = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
        ordering_exprs[i]->type(), key_fns[i], lhs_args, "lhs_value");

    // Call key_fns[i](ordering_expr_evals_rhs[i], rhs_arg)
    llvm::Value* rhs_eval = codegen->CodegenArrayAt(&builder, rhs_evals_arg, i);
    llvm::Value* rhs_args[] = {rhs_eval, rhs_arg};
    CodegenAnyVal rhs_value = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
        ordering_exprs[i]->type(), key_fns[i], rhs_args, "rhs_value");

    // Handle NULLs if necessary
    llvm::Value* lhs_null = lhs_value.GetIsNull();
    llvm::Value* rhs_null = rhs_value.GetIsNull();
    // if (lhs_value == NULL && rhs_value == NULL) continue;
    llvm::Value* both_null = builder.CreateAnd(lhs_null, rhs_null, "both_null");
    llvm::BasicBlock* non_null_block =
        llvm::BasicBlock::Create(context, "non_null", *fn, next_key_block);
    builder.CreateCondBr(both_null, next_key_block, non_null_block);
    // if (lhs_value == NULL && rhs_value != NULL) return nulls_first_[i];
    builder.SetInsertPoint(non_null_block);
    llvm::BasicBlock* lhs_null_block =
        llvm::BasicBlock::Create(context, "lhs_null", *fn, next_key_block);
    llvm::BasicBlock* lhs_non_null_block =
        llvm::BasicBlock::Create(context, "lhs_non_null", *fn, next_key_block);
    builder.CreateCondBr(lhs_null, lhs_null_block, lhs_non_null_block);
    builder.SetInsertPoint(lhs_null_block);
    builder.CreateRet(builder.getInt32(nulls_first_[i]));
    // if (lhs_value != NULL && rhs_value == NULL) return -nulls_first_[i];
    builder.SetInsertPoint(lhs_non_null_block);
    llvm::BasicBlock* rhs_null_block =
        llvm::BasicBlock::Create(context, "rhs_null", *fn, next_key_block);
    llvm::BasicBlock* rhs_non_null_block =
        llvm::BasicBlock::Create(context, "rhs_non_null", *fn, next_key_block);
    builder.CreateCondBr(rhs_null, rhs_null_block, rhs_non_null_block);
    builder.SetInsertPoint(rhs_null_block);
    builder.CreateRet(builder.getInt32(-nulls_first_[i]));

    // int result = RawValue::Compare(lhs_value, rhs_value, <type>)
    builder.SetInsertPoint(rhs_non_null_block);
    llvm::Value* result = lhs_value.Compare(&rhs_value, "result");

    // if (!is_asc_[i]) result = -result;
    if (!is_asc_[i]) result = builder.CreateSub(builder.getInt32(0), result, "result");
    // if (result != 0) return result;
    // Otherwise, try the next Expr
    llvm::Value* result_nonzero = builder.CreateICmpNE(result, builder.getInt32(0));
    llvm::BasicBlock* result_nonzero_block =
        llvm::BasicBlock::Create(context, "result_nonzero", *fn, next_key_block);
    builder.CreateCondBr(result_nonzero, result_nonzero_block, next_key_block);
    builder.SetInsertPoint(result_nonzero_block);
    builder.CreateRet(result);

    // Get builder ready for next iteration or final return
    builder.SetInsertPoint(next_key_block);
  }
  builder.CreateRet(builder.getInt32(0));
  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == NULL) {
    return Status("Codegen'd TupleRowComparator::Compare() function failed verification, "
        "see log");
  }
  return Status::OK();
}
