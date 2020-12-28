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

#include "exprs/is-not-empty-predicate.h"

#include <sstream>
#include <codegen/codegen-anyval.h>
#include <codegen/llvm-codegen.h>
#include <llvm/IR/BasicBlock.h>

#include "gen-cpp/Exprs_types.h"

#include "common/names.h"
#include "exprs/null-literal.h"
#include "exprs/scalar-expr.inline.h"
#include "exprs/slot-ref.h"

namespace impala {

const char* IsNotEmptyPredicate::LLVM_CLASS_NAME = "class.impala::IsNotEmptyPredicate";

IsNotEmptyPredicate::IsNotEmptyPredicate(const TExprNode& node) : Predicate(node) {}

BooleanVal IsNotEmptyPredicate::GetBooleanValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  CollectionVal coll = children_[0]->GetCollectionVal(eval, row);
  if (coll.is_null) return BooleanVal::null();
  return BooleanVal(coll.num_tuples != 0);
}

Status IsNotEmptyPredicate::Init(
    const RowDescriptor& row_desc, bool is_entry_point, FragmentState* state) {
  RETURN_IF_ERROR(ScalarExpr::Init(row_desc, is_entry_point, state));
  DCHECK_EQ(children_.size(), 1);
  return Status::OK();
}

// Sample IR output (when child is a SlotRef):
// define i16 @IsNotEmptyPredicate(%"class.impala::ScalarExprEvaluator"* %eval,
//                                 %"class.impala::TupleRow"* %row) #37 {
// entry:
//   %0 = alloca i16
//   %1 = alloca i16
//   %coll_val = call { i64, i8* } @GetSlotRef(
//      %"class.impala::ScalarExprEvaluator"* %eval, %"class.impala::TupleRow"* %row)
//   %2 = extractvalue { i64, i8* } %coll_val, 0
//   %coll_is_null = trunc i64 %2 to i1
//   br i1 %coll_is_null, label %ret_null, label %check_count
//
// check_count:                                      ; preds = %entry
//   %3 = extractvalue { i64, i8* } %coll_val, 0
//   %4 = ashr i64 %3, 32
//   %5 = trunc i64 %4 to i32
//   %6 = icmp ne i32 %5, 0
//   %has_values_result = load i16, i16* %0
//   %7 = zext i1 %6 to i16
//   %8 = shl i16 %7, 8
//   %9 = and i16 %has_values_result, 255
//   %has_values_result1 = or i16 %9, %8
//   ret i16 %has_values_result1
//
// ret_null:                                         ; preds = %entry
//   %null_result = load i16, i16* %1
//   ret i16 1
// }
Status IsNotEmptyPredicate::GetCodegendComputeFnImpl(
    LlvmCodeGen* codegen, llvm::Function** fn) {
  // Create a method with the expected signature.
  llvm::LLVMContext& context = codegen->context();
  llvm::Value* args[2];
  llvm::Function* new_fn =
      CreateIrFunctionPrototype("IsNotEmptyPredicate", codegen, &args);
  llvm::BasicBlock* entry_block = llvm::BasicBlock::Create(context, "entry", new_fn);
  LlvmBuilder builder(entry_block);

  ScalarExpr* child = children_[0]; // The child node, on which to call GetCollectionVal.
  llvm::Function* get_collection_val_fn;
  RETURN_IF_ERROR(child->GetCodegendComputeFn(codegen, false, &get_collection_val_fn));
  DCHECK(get_collection_val_fn != nullptr);

  // Find type for the CollectionVal struct.
  llvm::Type* collection_type = codegen->GetNamedType("struct.impala_udf::CollectionVal");
  DCHECK(collection_type->isStructTy());

  // Construct the call to the evaluation method, and return the result.
  CodegenAnyVal coll_val = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
      child->type(), get_collection_val_fn, args, "coll_val");

  // Find the 'is_null' field of the CollectionVal.
  llvm::Value* is_null = coll_val.GetIsNull("coll_is_null");

  llvm::BasicBlock* check_count =
      llvm::BasicBlock::Create(context, "check_count", new_fn);
  llvm::BasicBlock* ret_null = llvm::BasicBlock::Create(context, "ret_null", new_fn);
  builder.CreateCondBr(is_null, ret_null, check_count);

  // Add code to the block that is executed if is_null was true.
  builder.SetInsertPoint(ret_null);
  // allocate a BooleanVal, set null in it, and return it.
  CodegenAnyVal null_result(
      codegen, &builder, ColumnType(TYPE_BOOLEAN), nullptr, "null_result");
  builder.CreateRet(
      null_result.GetNullVal(codegen, ColumnType(TYPE_BOOLEAN)));

  // Back to the branch where 'is_null' is false.
  builder.SetInsertPoint(check_count);
  // Load the value of 'num_tuples'.
  llvm::Value* num_tuples = coll_val.GetLen();

  llvm::Value* has_values = builder.CreateICmpNE(num_tuples, codegen->GetI32Constant(0));
  CodegenAnyVal has_values_result(
      codegen, &builder, ColumnType(TYPE_BOOLEAN), nullptr, "has_values_result");
  has_values_result.SetVal(has_values);
  builder.CreateRet(has_values_result.GetLoweredValue());

  *fn = codegen->FinalizeFunction(new_fn);
  if (UNLIKELY(*fn == nullptr)) {
    return Status(TErrorCode::IR_VERIFY_FAILED, "IsNotEmptyPredicate");
  }
  return Status::OK();
}

string IsNotEmptyPredicate::DebugString() const {
  return ScalarExpr::DebugString("IsNotEmptyPredicate");
}

} // namespace impala
