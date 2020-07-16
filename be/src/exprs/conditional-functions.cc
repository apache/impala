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

#include "exprs/conditional-functions.h"

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exprs/scalar-expr.inline.h"

using namespace impala;

/// Sample IR output:
/// define i16 @IfExpr(%"class.impala::ScalarExprEvaluator"* %eval,
///                    %"class.impala::TupleRow"* %row) #47 {
/// is_condition_null:
///   %condition = call i16 @"impala::Operators::Gt_BigIntVal_BigIntValWrapper"(
///       %"class.impala::ScalarExprEvaluator"* %eval, %"class.impala::TupleRow"* %row)
///   %is_null = trunc i16 %condition to i1
///   br i1 %is_null, label %return_else, label %eval_condition
///
/// eval_condition:                         ; preds = %is_condition_null
///   %0 = ashr i16 %condition, 8
///   %1 = trunc i16 %0 to i8
///   %val = trunc i8 %1 to i1
///   br i1 %val, label %return_then, label %return_else
///
/// return_then:                            ; preds = %eval_condition
///   %then_val = call i16 @Literal.4(%"class.impala::ScalarExprEvaluator"* %eval,
///                                   %"class.impala::TupleRow"* %row)
///   ret i16 %then_val
///
/// return_else:                            ; preds = %eval_condition, %is_condition_null
///   %else_val = call i16 @NullLiteral(%"class.impala::ScalarExprEvaluator"* %eval,
///                                     %"class.impala::TupleRow"* %row)
///   ret i16 %else_val
/// }
Status IfExpr::GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn) {
  constexpr int IF_NUM_CHILDREN = 3;
  DCHECK_EQ(IF_NUM_CHILDREN, GetNumChildren());

  llvm::Function* child_fns[IF_NUM_CHILDREN];
  for (int i = 0; i < IF_NUM_CHILDREN; ++i) {
    RETURN_IF_ERROR(GetChild(i)->GetCodegendComputeFn(codegen, false, &child_fns[i]));
  }

  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);

  llvm::Value* args[2];
  llvm::Function* function = CreateIrFunctionPrototype("IfExpr", codegen, &args);

  llvm::BasicBlock* is_condition_null_block = llvm::BasicBlock::Create(
      context, "is_condition_null", function);
  llvm::BasicBlock* eval_condition_block = llvm::BasicBlock::Create(
      context, "eval_condition", function);
  llvm::BasicBlock* return_then_block = llvm::BasicBlock::Create(
      context, "return_then", function);
  llvm::BasicBlock* return_else_block = llvm::BasicBlock::Create(
      context, "return_else", function);

  // Check if the condition is a null value.
  builder.SetInsertPoint(is_condition_null_block);
  CodegenAnyVal condition = CodegenAnyVal::CreateCallWrapped(
      codegen, &builder, children()[0]->type(), child_fns[0], args, "condition");
  builder.CreateCondBr(
      condition.GetIsNull(), return_else_block, eval_condition_block);

  // Condition is non-null, branch according to its value.
  builder.SetInsertPoint(eval_condition_block);
  builder.CreateCondBr(condition.GetVal(), return_then_block, return_else_block);

  // Eval and return then value.
  builder.SetInsertPoint(return_then_block);
  llvm::Value* then_val =
      CodegenAnyVal::CreateCall(codegen, &builder, child_fns[1], args, "then_val");
  builder.CreateRet(then_val);

  // Eval and return else value.
  builder.SetInsertPoint(return_else_block);
  llvm::Value* else_val =
      CodegenAnyVal::CreateCall(codegen, &builder, child_fns[2], args, "else_val");
  builder.CreateRet(else_val);

  *fn = codegen->FinalizeFunction(function);
  if (UNLIKELY(*fn == nullptr)) return Status(TErrorCode::IR_VERIFY_FAILED, "IfExpr");
  return Status::OK();
}

/// Sample IR output for three columns:
/// define i64 @CoalesceExpr(%"class.impala::ScalarExprEvaluator"* %eval,
///                          %"class.impala::TupleRow"* %row) #47 {
/// is_child_null:
///   %child = call i64 @GetSlotRef(%"class.impala::ScalarExprEvaluator"* %eval,
///                                 %"class.impala::TupleRow"* %row)
///   %is_null = trunc i64 %child to i1
///   br i1 %is_null, label %is_child_null1, label %return_val
///
/// is_child_null1:                                   ; preds = %is_child_null
///   %child4 = call i64 @GetSlotRef.4(%"class.impala::ScalarExprEvaluator"* %eval,
///                                    %"class.impala::TupleRow"* %row)
///   %is_null5 = trunc i64 %child4 to i1
///   br i1 %is_null5, label %return_last_value, label %return_val3
///
/// return_val:                                       ; preds = %is_child_null
///   ret i64 %child
///
/// return_last_value:                                ; preds = %is_child_null1
///   %last_child = call i64 @GetSlotRef.5(%"class.impala::ScalarExprEvaluator"* %eval,
///                                        %"class.impala::TupleRow"* %row)
///   ret i64 %last_child
///
/// return_val3:                                      ; preds = %is_child_null1
///   ret i64 %child4
/// }
Status CoalesceExpr::GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn) {
  const int num_children = GetNumChildren();
  DCHECK_GE(num_children, 1);
  vector<llvm::Function*> child_fns(num_children, nullptr);
  for (int i = 0; i < num_children; ++i) {
    RETURN_IF_ERROR(GetChild(i)->GetCodegendComputeFn(codegen, false, &child_fns[i]));
  }

  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);
  llvm::Value* args[2];
  llvm::Function* function = CreateIrFunctionPrototype("CoalesceExpr", codegen, &args);

  // The entry block to the function, Checks whether the first child is null.
  // To be filled later in the loop.
  llvm::BasicBlock* is_child_null_block = llvm::BasicBlock::Create(
      context, "is_child_null", function);

  llvm::BasicBlock* current_null_check_block = is_child_null_block;
  llvm::BasicBlock* next_null_check_block = nullptr;

  // Loops through all children except the last. The next block in case of a null value is
  // always a new "is_child_null" block.
  for (int i = 0; i < num_children - 1; ++i) {
    next_null_check_block = llvm::BasicBlock::Create(context, "is_child_null", function);

    // The block that returns the value of the child if it is not null.
    llvm::BasicBlock* return_val_block =
        llvm::BasicBlock::Create(context, "return_val", function);

    builder.SetInsertPoint(current_null_check_block);
    CodegenAnyVal child = CodegenAnyVal::CreateCallWrapped(
        codegen, &builder, children()[i]->type(), child_fns[i], args, "child");
    builder.CreateCondBr(
        child.GetIsNull(), next_null_check_block, return_val_block);

    builder.SetInsertPoint(return_val_block);
    builder.CreateRet(child.GetLoweredValue());

    current_null_check_block = next_null_check_block;
  }

  // Handle the last child. We can return it directly because if it is null, we need to
  // return null anyway.
  llvm::BasicBlock* return_last_value_block = current_null_check_block;
  return_last_value_block->setName("return_last_value");
  builder.SetInsertPoint(return_last_value_block);
  llvm::Value* last_child = CodegenAnyVal::CreateCall(
      codegen, &builder, child_fns[num_children - 1], args, "last_child");
  builder.CreateRet(last_child);

  *fn = codegen->FinalizeFunction(function);
  if (UNLIKELY(*fn == nullptr)) {
    return Status(TErrorCode::IR_VERIFY_FAILED, "CoalesceExpr");
  }
  return Status::OK();
}

/// Sample IR output:
/// define { i8, i64 } @IsNullExpr(%"class.impala::ScalarExprEvaluator"* %eval,
///                                %"class.impala::TupleRow"* %row) #47 {
/// is_first_value_null:
///   %first_value = call { i8, i64 } @GetSlotRef(
///       %"class.impala::ScalarExprEvaluator"* %eval, %"class.impala::TupleRow"* %row)
///   %0 = extractvalue { i8, i64 } %first_value, 0
///   %is_null = trunc i8 %0 to i1
///   br i1 %is_null, label %return_second_value, label %return_first_value
///
/// return_first_value:                               ; preds = %is_first_value_null
///   ret { i8, i64 } %first_value
///
/// return_second_value:                              ; preds = %is_first_value_null
///   %second_value = call { i8, i64 } @Literal(
///       %"class.impala::ScalarExprEvaluator"* %eval, %"class.impala::TupleRow"* %row)
///   ret { i8, i64 } %second_value
/// }
Status IsNullExpr::GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn) {
  constexpr int NUM_CHILDREN = 2;
  DCHECK_EQ(NUM_CHILDREN, GetNumChildren());

  llvm::Function* child_fns[NUM_CHILDREN];
  for (int i = 0; i < NUM_CHILDREN; ++i) {
    RETURN_IF_ERROR(GetChild(i)->GetCodegendComputeFn(codegen, false, &child_fns[i]));
  }

  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);

  llvm::Value* args[2];
  llvm::Function* function = CreateIrFunctionPrototype("IsNullExpr", codegen, &args);

  llvm::BasicBlock* is_first_value_null_block = llvm::BasicBlock::Create(
      context, "is_first_value_null", function);
  llvm::BasicBlock* return_first_value_block = llvm::BasicBlock::Create(
      context, "return_first_value", function);
  llvm::BasicBlock* return_second_value_block = llvm::BasicBlock::Create(
      context, "return_second_value", function);

  // Check if the first child is null.
  builder.SetInsertPoint(is_first_value_null_block);
  CodegenAnyVal first_value = CodegenAnyVal::CreateCallWrapped(
      codegen, &builder, children()[0]->type(), child_fns[0], args, "first_value");
  builder.CreateCondBr(
      first_value.GetIsNull(), return_second_value_block, return_first_value_block);

  builder.SetInsertPoint(return_first_value_block);
  builder.CreateRet(first_value.GetLoweredValue());

  builder.SetInsertPoint(return_second_value_block);
  llvm::Value* second_value = CodegenAnyVal::CreateCall(
      codegen, &builder, child_fns[1], args, "second_value");
  builder.CreateRet(second_value);

  *fn = codegen->FinalizeFunction(function);
  if (UNLIKELY(*fn == nullptr)) return Status(TErrorCode::IR_VERIFY_FAILED, "IsNullExpr");
  return Status::OK();
}

/// Definitions of Get*ValInterpreted functions.
#define IS_NULL_COMPUTE_FUNCTION(type) \
  type IsNullExpr::Get##type##Interpreted( \
      ScalarExprEvaluator* eval, const TupleRow* row) const { \
    DCHECK_EQ(children_.size(), 2); \
    type val = GetChild(0)->Get##type(eval, row);  \
    if (!val.is_null) return val; /* short-circuit */ \
    return GetChild(1)->Get##type(eval, row); \
  }

IS_NULL_COMPUTE_FUNCTION(BooleanVal);
IS_NULL_COMPUTE_FUNCTION(TinyIntVal);
IS_NULL_COMPUTE_FUNCTION(SmallIntVal);
IS_NULL_COMPUTE_FUNCTION(IntVal);
IS_NULL_COMPUTE_FUNCTION(BigIntVal);
IS_NULL_COMPUTE_FUNCTION(FloatVal);
IS_NULL_COMPUTE_FUNCTION(DoubleVal);
IS_NULL_COMPUTE_FUNCTION(StringVal);
IS_NULL_COMPUTE_FUNCTION(TimestampVal);
IS_NULL_COMPUTE_FUNCTION(DecimalVal);
IS_NULL_COMPUTE_FUNCTION(DateVal);

#define IF_COMPUTE_FUNCTION(type) \
  type IfExpr::Get##type##Interpreted( \
      ScalarExprEvaluator* eval, const TupleRow* row) const { \
    DCHECK_EQ(children_.size(), 3); \
    BooleanVal cond = GetChild(0)->GetBooleanVal(eval, row); \
    if (cond.is_null || !cond.val) { \
      return GetChild(2)->Get##type(eval, row); \
    } \
    return GetChild(1)->Get##type(eval, row); \
  }

IF_COMPUTE_FUNCTION(BooleanVal);
IF_COMPUTE_FUNCTION(TinyIntVal);
IF_COMPUTE_FUNCTION(SmallIntVal);
IF_COMPUTE_FUNCTION(IntVal);
IF_COMPUTE_FUNCTION(BigIntVal);
IF_COMPUTE_FUNCTION(FloatVal);
IF_COMPUTE_FUNCTION(DoubleVal);
IF_COMPUTE_FUNCTION(StringVal);
IF_COMPUTE_FUNCTION(TimestampVal);
IF_COMPUTE_FUNCTION(DecimalVal);
IF_COMPUTE_FUNCTION(DateVal);

#define COALESCE_COMPUTE_FUNCTION(type) \
  type CoalesceExpr::Get##type##Interpreted( \
      ScalarExprEvaluator* eval, const TupleRow* row) const { \
    DCHECK_GE(children_.size(), 1); \
    for (int i = 0; i < children_.size(); ++i) { \
      type val = GetChild(i)->Get##type(eval, row); \
      if (!val.is_null) return val; \
    } \
    return type::null(); \
  }

COALESCE_COMPUTE_FUNCTION(BooleanVal);
COALESCE_COMPUTE_FUNCTION(TinyIntVal);
COALESCE_COMPUTE_FUNCTION(SmallIntVal);
COALESCE_COMPUTE_FUNCTION(IntVal);
COALESCE_COMPUTE_FUNCTION(BigIntVal);
COALESCE_COMPUTE_FUNCTION(FloatVal);
COALESCE_COMPUTE_FUNCTION(DoubleVal);
COALESCE_COMPUTE_FUNCTION(StringVal);
COALESCE_COMPUTE_FUNCTION(TimestampVal);
COALESCE_COMPUTE_FUNCTION(DecimalVal);
COALESCE_COMPUTE_FUNCTION(DateVal);
