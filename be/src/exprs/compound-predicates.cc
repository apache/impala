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

#include <sstream>

#include "exprs/compound-predicates.h"
#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "runtime/runtime-state.h"

#include "common/names.h"

using namespace impala;

// (<> && false) is false, (true && NULL) is NULL
BooleanVal AndPredicate::GetBooleanVal(ScalarExprEvaluator* eval,
    const TupleRow* row) const {
  DCHECK_EQ(children_.size(), 2);
  BooleanVal val1 = children_[0]->GetBooleanVal(eval, row);
  if (!val1.is_null && !val1.val) return BooleanVal(false); // short-circuit

  BooleanVal val2 = children_[1]->GetBooleanVal(eval, row);
  if (!val2.is_null && !val2.val) return BooleanVal(false);

  if (val1.is_null || val2.is_null) return BooleanVal::null();
  return BooleanVal(true);
}

string AndPredicate::DebugString() const {
  stringstream out;
  out << "AndPredicate(" << ScalarExpr::DebugString() << ")";
  return out.str();
}

// (<> || true) is true, (false || NULL) is NULL
BooleanVal OrPredicate::GetBooleanVal(ScalarExprEvaluator* eval,
    const TupleRow* row) const {
  DCHECK_EQ(children_.size(), 2);
  BooleanVal val1 = children_[0]->GetBooleanVal(eval, row);
  if (!val1.is_null && val1.val) return BooleanVal(true); // short-circuit

  BooleanVal val2 = children_[1]->GetBooleanVal(eval, row);
  if (!val2.is_null && val2.val) return BooleanVal(true);

  if (val1.is_null || val2.is_null) return BooleanVal::null();
  return BooleanVal(false);
}

string OrPredicate::DebugString() const {
  stringstream out;
  out << "OrPredicate(" << ScalarExpr::DebugString() << ")";
  return out.str();
}

// IR codegen for compound and/or predicates.  Compound predicate has non trivial
// null handling as well as many branches so this is pretty complicated.  The IR
// for x && y is:
//
// define i16 @CompoundPredicate(%"class.impala::ScalarExprEvaluator"* %eval,
//                               %"class.impala::TupleRow"* %row) #20 {
// entry:
//   %lhs_call = call i16 @GetSlotRef1(%"class.impala::ScalarExprEvaluator"* %eval,
//                                     %"class.impala::TupleRow"* %row)
//   %rhs_call = call i16 @Eq_IntVal_IntValWrapper(
//                                %"class.impala::ScalarExprEvaluator"* %eval,
//                                %"class.impala::TupleRow"* %row)
//   %is_null = trunc i16 %lhs_call to i1
//   %is_null1 = trunc i16 %rhs_call to i1
//   %0 = ashr i16 %lhs_call, 8
//   %1 = trunc i16 %0 to i8
//   %val = trunc i8 %1 to i1
//   %2 = ashr i16 %rhs_call, 8
//   %3 = trunc i16 %2 to i8
//   %val2 = trunc i8 %3 to i1
//   %tmp_and = and i1 %val, %val2
//   br i1 %is_null, label %lhs_null, label %lhs_not_null
//
// lhs_null:                                         ; preds = %entry
//   br i1 %is_null1, label %null_block, label %lhs_null_rhs_not_null
//
// lhs_not_null:                                     ; preds = %entry
//   br i1 %is_null1, label %lhs_not_null_rhs_null, label %not_null_block
//
// lhs_null_rhs_not_null:                            ; preds = %lhs_null
//   br i1 %val2, label %null_block, label %not_null_block
//
// lhs_not_null_rhs_null:                            ; preds = %lhs_not_null
//   br i1 %val, label %null_block, label %not_null_block
//
// null_block:                                       ; preds = %lhs_null_rhs_not_null,
//                                                     %lhs_not_null_rhs_null, %lhs_null
//   br label %ret
//
// not_null_block:                                   ; preds = %lhs_null_rhs_not_null,
//                                                   %lhs_not_null_rhs_null, %lhs_not_null
//   %4 = phi i1 [ false, %lhs_null_rhs_not_null ],
//               [ false, %lhs_not_null_rhs_null ],
//               [ %tmp_and, %lhs_not_null ]
//   br label %ret
//
// ret:                                              ; preds = %not_null_block, %null_block
//   %ret3 = phi i1 [ false, %null_block ], [ %4, %not_null_block ]
//   %5 = zext i1 %ret3 to i16
//   %6 = shl i16 %5, 8
//   %7 = or i16 0, %6
//   ret i16 %7
// }
Status CompoundPredicate::CodegenComputeFn(
    bool and_fn, LlvmCodeGen* codegen, llvm::Function** fn) {
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK();
  }

  DCHECK_EQ(GetNumChildren(), 2);
  llvm::Function* lhs_function;
  RETURN_IF_ERROR(children()[0]->GetCodegendComputeFn(codegen, &lhs_function));
  llvm::Function* rhs_function;
  RETURN_IF_ERROR(children()[1]->GetCodegendComputeFn(codegen, &rhs_function));

  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);
  llvm::Value* args[2];
  llvm::Function* function =
      CreateIrFunctionPrototype("CompoundPredicate", codegen, &args);

  llvm::BasicBlock* entry_block = llvm::BasicBlock::Create(context, "entry", function);
  builder.SetInsertPoint(entry_block);

  // Control blocks for aggregating results
  llvm::BasicBlock* lhs_null_block =
      llvm::BasicBlock::Create(context, "lhs_null", function);
  llvm::BasicBlock* lhs_not_null_block =
      llvm::BasicBlock::Create(context, "lhs_not_null", function);
  llvm::BasicBlock* lhs_null_rhs_not_null_block =
      llvm::BasicBlock::Create(context, "lhs_null_rhs_not_null", function);
  llvm::BasicBlock* lhs_not_null_rhs_null_block =
      llvm::BasicBlock::Create(context, "lhs_not_null_rhs_null", function);
  llvm::BasicBlock* null_block =
      llvm::BasicBlock::Create(context, "null_block", function);
  llvm::BasicBlock* not_null_block =
      llvm::BasicBlock::Create(context, "not_null_block", function);
  llvm::BasicBlock* ret_block = llvm::BasicBlock::Create(context, "ret", function);

  // Call lhs
  CodegenAnyVal lhs_result = CodegenAnyVal::CreateCallWrapped(
      codegen, &builder, TYPE_BOOLEAN, lhs_function, args, "lhs_call");
  // Call rhs
  CodegenAnyVal rhs_result = CodegenAnyVal::CreateCallWrapped(
      codegen, &builder, TYPE_BOOLEAN, rhs_function, args, "rhs_call");

  llvm::Value* lhs_is_null = lhs_result.GetIsNull();
  llvm::Value* rhs_is_null = rhs_result.GetIsNull();
  llvm::Value* lhs_value = lhs_result.GetVal();
  llvm::Value* rhs_value = rhs_result.GetVal();

  // Apply predicate
  llvm::Value* compare = NULL;
  if (and_fn) {
    compare = builder.CreateAnd(lhs_value, rhs_value, "tmp_and");
  } else {
    compare = builder.CreateOr(lhs_value, rhs_value, "tmp_or");
  }

  // Branch if lhs is null
  builder.CreateCondBr(lhs_is_null, lhs_null_block, lhs_not_null_block);

  // lhs_is_null block
  builder.SetInsertPoint(lhs_null_block);
  builder.CreateCondBr(rhs_is_null, null_block, lhs_null_rhs_not_null_block);

  // lhs_is_not_null block
  builder.SetInsertPoint(lhs_not_null_block);
  builder.CreateCondBr(rhs_is_null, lhs_not_null_rhs_null_block, not_null_block);

  // lhs_not_null rhs_null block
  builder.SetInsertPoint(lhs_not_null_rhs_null_block);
  if (and_fn) {
    // false && null -> false; true && null -> null
    builder.CreateCondBr(lhs_value, null_block, not_null_block);
  } else {
    // true || null -> true; false || null -> null
    builder.CreateCondBr(lhs_value, not_null_block, null_block);
  }

  // lhs_null rhs_not_null block
  builder.SetInsertPoint(lhs_null_rhs_not_null_block);
  if (and_fn) {
    // null && false -> false; null && true -> null
    builder.CreateCondBr(rhs_value, null_block, not_null_block);
  } else {
    // null || true -> true; null || false -> null
    builder.CreateCondBr(rhs_value, not_null_block, null_block);
  }

  // NULL block
  builder.SetInsertPoint(null_block);
  builder.CreateBr(ret_block);

  // not-NULL block
  builder.SetInsertPoint(not_null_block);
  llvm::PHINode* not_null_phi = builder.CreatePHI(codegen->bool_type(), 3);
  if (and_fn) {
    not_null_phi->addIncoming(codegen->false_value(), lhs_null_rhs_not_null_block);
    not_null_phi->addIncoming(codegen->false_value(), lhs_not_null_rhs_null_block);
    not_null_phi->addIncoming(compare, lhs_not_null_block);
  } else {
    not_null_phi->addIncoming(codegen->true_value(), lhs_null_rhs_not_null_block);
    not_null_phi->addIncoming(codegen->true_value(), lhs_not_null_rhs_null_block);
    not_null_phi->addIncoming(compare, lhs_not_null_block);
  }
  builder.CreateBr(ret_block);

  // Ret/merge block
  builder.SetInsertPoint(ret_block);
  llvm::PHINode* is_null_phi = builder.CreatePHI(codegen->bool_type(), 2, "is_null");
  is_null_phi->addIncoming(codegen->true_value(), null_block);
  is_null_phi->addIncoming(codegen->false_value(), not_null_block);

  llvm::PHINode* val_phi = builder.CreatePHI(codegen->bool_type(), 2, "val");
  val_phi->addIncoming(codegen->false_value(), null_block);
  val_phi->addIncoming(not_null_phi, not_null_block);

  CodegenAnyVal ret(codegen, &builder, TYPE_BOOLEAN, NULL, "ret");
  ret.SetIsNull(is_null_phi);
  ret.SetVal(val_phi);
  builder.CreateRet(ret.GetLoweredValue());

  *fn = codegen->FinalizeFunction(function);
  DCHECK(*fn != NULL);
  ir_compute_fn_ = *fn;
  return Status::OK();
}
