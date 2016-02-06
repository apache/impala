// Copyright 2015 Cloudera Inc.
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

#include "util/tuple-row-compare.h"

#include <gutil/strings/substitute.h>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "runtime/runtime-state.h"

using namespace impala;
using namespace llvm;
using namespace strings;

Status TupleRowComparator::Codegen(RuntimeState* state) {
  Function* fn;
  RETURN_IF_ERROR(CodegenCompare(state, &fn));
  LlvmCodeGen* codegen;
  bool got_codegen = state->GetCodegen(&codegen).ok();
  DCHECK(got_codegen);
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
// define i32 @Compare(%"class.impala::ExprContext"** %key_expr_ctxs_lhs,
//                     %"class.impala::ExprContext"** %key_expr_ctxs_rhs,
//                     %"class.impala::TupleRow"* %lhs,
//                     %"class.impala::TupleRow"* %rhs) #20 {
// entry:
//   %type13 = alloca %"struct.impala::ColumnType"
//   %0 = alloca float
//   %1 = alloca float
//   %type = alloca %"struct.impala::ColumnType"
//   %2 = alloca i32
//   %3 = alloca i32
//   %4 = getelementptr %"class.impala::ExprContext"** %key_expr_ctxs_lhs, i32 0
//   %5 = load %"class.impala::ExprContext"** %4
//   %lhs_value = call i64 @GetSlotRef(
//       %"class.impala::ExprContext"* %5, %"class.impala::TupleRow"* %lhs)
//   %6 = getelementptr %"class.impala::ExprContext"** %key_expr_ctxs_rhs, i32 0
//   %7 = load %"class.impala::ExprContext"** %6
//   %rhs_value = call i64 @GetSlotRef(
//       %"class.impala::ExprContext"* %7, %"class.impala::TupleRow"* %rhs)
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
//   %15 = getelementptr %"class.impala::ExprContext"** %key_expr_ctxs_lhs, i32 1
//   %16 = load %"class.impala::ExprContext"** %15
//   %lhs_value3 = call i64 @GetSlotRef1(
//       %"class.impala::ExprContext"* %16, %"class.impala::TupleRow"* %lhs)
//   %17 = getelementptr %"class.impala::ExprContext"** %key_expr_ctxs_rhs, i32 1
//   %18 = load %"class.impala::ExprContext"** %17
//   %rhs_value4 = call i64 @GetSlotRef1(
//       %"class.impala::ExprContext"* %18, %"class.impala::TupleRow"* %rhs)
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
Status TupleRowComparator::CodegenCompare(RuntimeState* state, Function** fn) {
  LlvmCodeGen* codegen;
  RETURN_IF_ERROR(state->GetCodegen(&codegen));
  SCOPED_TIMER(codegen->codegen_timer());
  LLVMContext& context = codegen->context();

  // Get all the key compute functions from key_expr_ctxs_lhs_. The lhs and rhs functions
  // are the same since they're clones, and key_expr_ctxs_rhs_ is not populated until
  // Open() is called.
  DCHECK(key_expr_ctxs_rhs_.empty()) << "rhs exprs should be clones of lhs!";
  Function* key_fns[key_expr_ctxs_lhs_.size()];
  for (int i = 0; i < key_expr_ctxs_lhs_.size(); ++i) {
    Status status =
        key_expr_ctxs_lhs_[i]->root()->GetCodegendComputeFn(state, &key_fns[i]);
    if (!status.ok()) {
      return Status(Substitute("Could not codegen TupleRowComparator::Compare(): %s",
          status.GetDetail()));
    }
  }

  // Construct function signature (note that this is different than the interpreted
  // Compare() function signature):
  // int Compare(ExprContext** key_expr_ctxs_lhs, ExprContext** key_expr_ctxs_rhs,
  //     TupleRow* lhs, TupleRow* rhs)
  PointerType* expr_ctxs_type =
      codegen->GetPtrType(ExprContext::LLVM_CLASS_NAME)->getPointerTo();
  PointerType* tuple_row_type = codegen->GetPtrType(TupleRow::LLVM_CLASS_NAME);
  LlvmCodeGen::FnPrototype prototype(codegen, "Compare", codegen->int_type());
  prototype.AddArgument("key_expr_ctxs_lhs", expr_ctxs_type);
  prototype.AddArgument("key_expr_ctxs_rhs", expr_ctxs_type);
  prototype.AddArgument("lhs", tuple_row_type);
  prototype.AddArgument("rhs", tuple_row_type);

  LlvmCodeGen::LlvmBuilder builder(context);
  Value* args[4];
  *fn = prototype.GeneratePrototype(&builder, args);
  Value* lhs_ctxs_arg = args[0];
  Value* rhs_ctxs_arg = args[1];
  Value* lhs_arg = args[2];
  Value* rhs_arg = args[3];

  // Unrolled loop over each key expr
  for (int i = 0; i < key_expr_ctxs_lhs_.size(); ++i) {
    // The start of the next key expr after this one. Used to implement "continue" logic
    // in the unrolled loop.
    BasicBlock* next_key_block = BasicBlock::Create(context, "next_key", *fn);

    // Call key_fns[i](key_expr_ctxs_lhs[i], lhs_arg)
    Value* lhs_ctx = codegen->CodegenArrayAt(&builder, lhs_ctxs_arg, i);
    Value* lhs_args[] = { lhs_ctx, lhs_arg };
    CodegenAnyVal lhs_value = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
        key_expr_ctxs_lhs_[i]->root()->type(), key_fns[i], lhs_args, "lhs_value");

    // Call key_fns[i](key_expr_ctxs_rhs[i], rhs_arg)
    Value* rhs_ctx = codegen->CodegenArrayAt(&builder, rhs_ctxs_arg, i);
    Value* rhs_args[] = { rhs_ctx, rhs_arg };
    CodegenAnyVal rhs_value = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
        key_expr_ctxs_lhs_[i]->root()->type(), key_fns[i], rhs_args, "rhs_value");

    // Handle NULLs if necessary
    Value* lhs_null = lhs_value.GetIsNull();
    Value* rhs_null = rhs_value.GetIsNull();
    // if (lhs_value == NULL && rhs_value == NULL) continue;
    Value* both_null = builder.CreateAnd(lhs_null, rhs_null, "both_null");
    BasicBlock* non_null_block =
        BasicBlock::Create(context, "non_null", *fn, next_key_block);
    builder.CreateCondBr(both_null, next_key_block, non_null_block);
    // if (lhs_value == NULL && rhs_value != NULL) return nulls_first_[i];
    builder.SetInsertPoint(non_null_block);
    BasicBlock* lhs_null_block =
        BasicBlock::Create(context, "lhs_null", *fn, next_key_block);
    BasicBlock* lhs_non_null_block =
        BasicBlock::Create(context, "lhs_non_null", *fn, next_key_block);
    builder.CreateCondBr(lhs_null, lhs_null_block, lhs_non_null_block);
    builder.SetInsertPoint(lhs_null_block);
    builder.CreateRet(builder.getInt32(nulls_first_[i]));
    // if (lhs_value != NULL && rhs_value == NULL) return -nulls_first_[i];
    builder.SetInsertPoint(lhs_non_null_block);
    BasicBlock* rhs_null_block =
        BasicBlock::Create(context, "rhs_null", *fn, next_key_block);
    BasicBlock* rhs_non_null_block =
        BasicBlock::Create(context, "rhs_non_null", *fn, next_key_block);
    builder.CreateCondBr(rhs_null, rhs_null_block, rhs_non_null_block);
    builder.SetInsertPoint(rhs_null_block);
    builder.CreateRet(builder.getInt32(-nulls_first_[i]));

    // int result = RawValue::Compare(lhs_value, rhs_value, <type>)
    builder.SetInsertPoint(rhs_non_null_block);
    Value* result = lhs_value.Compare(&rhs_value, "result");

    // if (!is_asc_[i]) result = -result;
    if (!is_asc_[i]) result = builder.CreateSub(builder.getInt32(0), result, "result");
    // if (result != 0) return result;
    // Otherwise, try the next Expr
    Value* result_nonzero = builder.CreateICmpNE(result, builder.getInt32(0));
    BasicBlock* result_nonzero_block =
        BasicBlock::Create(context, "result_nonzero", *fn, next_key_block);
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
