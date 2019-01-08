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
#include "exprs/slot-ref.h"

namespace impala {

const char* IsNotEmptyPredicate::LLVM_CLASS_NAME = "class.impala::IsNotEmptyPredicate";

IsNotEmptyPredicate::IsNotEmptyPredicate(const TExprNode& node) : Predicate(node) {}

BooleanVal IsNotEmptyPredicate::GetBooleanVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  CollectionVal coll = children_[0]->GetCollectionVal(eval, row);
  if (coll.is_null) return BooleanVal::null();
  return BooleanVal(coll.num_tuples != 0);
}

Status IsNotEmptyPredicate::Init(const RowDescriptor& row_desc, RuntimeState* state) {
  RETURN_IF_ERROR(ScalarExpr::Init(row_desc, state));
  DCHECK_EQ(children_.size(), 1);
  return Status::OK();
}

// Sample IR output (when child is a SlotRef): // FIXME needs review
//
//   define i16 @IsNotEmptyPredicate(%"class.impala::ScalarExprEvaluator"* %eval,
//     %"class.impala::TupleRow"* %row) #38 {
//   entry:
//   %0 = alloca i16
//   %1 = alloca i16
//   %collection_val = alloca %"struct.impala_udf::CollectionVal"
//   call void @_ZNK6impala7SlotRef16GetCollectionValEPNS_
//     19ScalarExprEvaluatorEPKNS_8TupleRowE(%"struct.impala_udf::CollectionVal"*
//     %collection_val, %"class.impala::SlotRef"* inttoptr (i64 140731643050560
//     to %"class.impala::SlotRef"*),
//     %"class.impala::ScalarExprEvaluator"* %eval, %"class.impala::TupleRow"* %row)
//   %anyval_ptr = getelementptr inbounds %"struct.impala_udf::CollectionVal",
//     %"struct.impala_udf::CollectionVal"* %collection_val, i32 0, i32 0
//   %is_null_ptr = getelementptr inbounds %"struct.impala_udf::AnyVal",
//     %"struct.impala_udf::AnyVal"* %anyval_ptr, i32 0, i32 0
//   %is_null = load i8, i8* %is_null_ptr
//   %is_null_bool = icmp ne i8 %is_null, 0
//   %num_tuples_ptr = getelementptr inbounds %"struct.impala_udf::CollectionVal",
//     %"struct.impala_udf::CollectionVal"* %collection_val, i32 0, i32 3
//   %num_tuples = load i32, i32* %num_tuples_ptr
//   br i1 %is_null_bool, label %ret_null, label %check_count
//
// check_count:                                      ; preds = %entry
//   %4 = icmp ne i32 %num_tuples, 0
//   %ret2 = load i16, i16* %0
//   %5 = zext i1 %4 to i16
//   %6 = shl i16 %5, 8
//   %7 = and i16 %ret2, 255
//   %ret21 = or i16 %7, %6
//   ret i16 %ret21
//
// ret_null:                                         ; preds = %entry
//   %ret = load i16, i16* %1
//   ret i16 1
// }
//
Status IsNotEmptyPredicate::GetCodegendComputeFn(
    LlvmCodeGen* codegen, llvm::Function** fn) {
  if (ir_compute_fn_ != nullptr) {
    *fn = ir_compute_fn_;
    return Status::OK();
  }

  // Create a method with the expected signature.
  llvm::LLVMContext& context = codegen->context();
  llvm::Value* args[2];
  llvm::Function* new_fn =
      CreateIrFunctionPrototype("IsNotEmptyPredicate", codegen, &args);
  llvm::BasicBlock* entry_block = llvm::BasicBlock::Create(context, "entry", new_fn);
  LlvmBuilder builder(entry_block);

  ScalarExpr* child = children_[0]; // The child node, on which to call GetCollectionVal.
  llvm::Value* child_expr; //  a Value for 'child' with the correct subtype of ScalarExpr.
  llvm::Function* get_collection_val_fn; // a type specific GetCollectionVal method

  //  To save a virtual method call, find the type of child and lookup the non-virtual
  //  GetCollection method to call.
  if (dynamic_cast<SlotRef*>(child)) {
    // Lookup SlotRef::GetCollectionVal().
    get_collection_val_fn =
        codegen->GetFunction(IRFunction::SCALAR_EXPR_SLOT_REF_GET_COLLECTION_VAL, false);
    child_expr = codegen->CastPtrToLlvmPtr(
        codegen->GetNamedPtrType(SlotRef::LLVM_CLASS_NAME), child);
  } else if (dynamic_cast<NullLiteral*>(child)) {
    // Lookup NullLiteral::GetCollectionVal().
    get_collection_val_fn = codegen->GetFunction(
        IRFunction::SCALAR_EXPR_NULL_LITERAL_GET_COLLECTION_VAL, false);
    child_expr = codegen->CastPtrToLlvmPtr(
        codegen->GetNamedPtrType(NullLiteral::LLVM_CLASS_NAME), child);
  } else {
    // This may mean someone implemented GetCollectionVal in a new subclass of ScalarExpr.
    DCHECK(false) << "Unknown GetCollectionVal implementation: " << typeid(*child).name();
    return Status("Codegen'd IsNotEmptyPredicate function found unknown GetCollectionVal,"
                  " see log.");
  }
  DCHECK(get_collection_val_fn != nullptr);
  DCHECK(child_expr != nullptr);

  // Find type for the CollectionVal struct.
  llvm::Type* collection_type = codegen->GetNamedType("struct.impala_udf::CollectionVal");
  DCHECK(collection_type->isStructTy());

  // Allocate space for the CollectionVal on the stack
  llvm::Value* collection_val_ptr =
      codegen->CreateEntryBlockAlloca(builder, collection_type, "collection_val");

  // The get_collection_val_fn returns a CollectionVal by value. In llvm we have to pass a
  // pointer as the first argument. The second argument is the object on which the call is
  // made.
  llvm::Value* get_coll_call_args[] = {collection_val_ptr, child_expr, args[0], args[1]};

  // Construct the call to the evaluation method, and return the result.
  llvm::Value* collection = builder.CreateCall(get_collection_val_fn, get_coll_call_args);
  DCHECK(collection != nullptr);

  // Find the 'is_null' field of the CollectionVal.
  llvm::Value* anyval_ptr =
      builder.CreateStructGEP(nullptr, collection_val_ptr, 0, "anyval_ptr");
  llvm::Value* is_null_ptr =
      builder.CreateStructGEP(nullptr, anyval_ptr, 0, "is_null_ptr");
  llvm::Value* is_null = builder.CreateLoad(is_null_ptr, "is_null");

  // Check if 'is_null' is true.
  llvm::Value* is_null_bool =
      builder.CreateICmpNE(is_null, codegen->GetI8Constant(0), "is_null_bool");

  llvm::BasicBlock* check_count =
      llvm::BasicBlock::Create(context, "check_count", new_fn);
  llvm::BasicBlock* ret_null = llvm::BasicBlock::Create(context, "ret_null", new_fn);
  builder.CreateCondBr(is_null_bool, ret_null, check_count);

  // Add code to the block that is executed if is_null was true.
  builder.SetInsertPoint(ret_null);
  // allocate a BooleanVal, set null in it, and return it.
  CodegenAnyVal null_result(codegen, &builder, TYPE_BOOLEAN, nullptr, "null_result");
  builder.CreateRet(null_result.GetNullVal(codegen, TYPE_BOOLEAN));

  // Back to the branch where 'is_null' is false.
  builder.SetInsertPoint(check_count);
  // Load the value of 'num_tuples'
  llvm::Value* num_tuples_ptr =
      builder.CreateStructGEP(nullptr, collection_val_ptr, 3, "num_tuples_ptr");
  llvm::Value* num_tuples = builder.CreateLoad(num_tuples_ptr, "num_tuples");

  llvm::Value* has_values = builder.CreateICmpNE(num_tuples, codegen->GetI32Constant(0));
  CodegenAnyVal has_values_result(
      codegen, &builder, TYPE_BOOLEAN, nullptr, "has_values_result");
  has_values_result.SetVal(has_values);
  builder.CreateRet(has_values_result.GetLoweredValue());

  *fn = codegen->FinalizeFunction(new_fn);
  if (UNLIKELY(*fn == nullptr)) {
    return Status(TErrorCode::IR_VERIFY_FAILED, "IsNotEmptyPredicate");
  }

  ir_compute_fn_ = *fn;

  return Status::OK();
}

string IsNotEmptyPredicate::DebugString() const {
  return ScalarExpr::DebugString("IsNotEmptyPredicate");
}

} // namespace impala
