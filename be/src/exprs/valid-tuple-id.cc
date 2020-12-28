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

#include "exprs/valid-tuple-id.h"

#include <sstream>

#include <codegen/codegen-anyval.h>
#include "codegen/llvm-codegen.h"
#include "common/names.h"
#include "runtime/descriptors.h"
#include "runtime/tuple-row.h"

namespace impala {

const char* ValidTupleIdExpr::LLVM_CLASS_NAME = "class.impala::ValidTupleIdExpr";

ValidTupleIdExpr::ValidTupleIdExpr(const TExprNode& node) : ScalarExpr(node) {}

Status ValidTupleIdExpr::Init(
    const RowDescriptor& row_desc, bool is_entry_point, FragmentState* state) {
  RETURN_IF_ERROR(ScalarExpr::Init(row_desc, is_entry_point, state));
  DCHECK_EQ(0, children_.size());
  tuple_ids_.reserve(row_desc.tuple_descriptors().size());
  for (TupleDescriptor* tuple_desc : row_desc.tuple_descriptors()) {
    tuple_ids_.push_back(tuple_desc->id());
  }
  return Status::OK();
}

int ValidTupleIdExpr::ComputeNonNullCount(const TupleRow* row, int num_tuples) {
  int non_null_count = 0;
  for (int i = 0; i < num_tuples; ++i) non_null_count += (row->GetTuple(i) != nullptr);
  return non_null_count;
}

IntVal ValidTupleIdExpr::GetIntValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  // Validate that exactly one tuple is non-NULL.
  int num_tuples = tuple_ids_.size();
  DCHECK_EQ(1, ComputeNonNullCount(row, num_tuples));
  for (int i = 0; i < num_tuples; ++i) {
    if (row->GetTuple(i) != nullptr) return IntVal(tuple_ids_[i]);
  }
  return IntVal::null();
}

// Sample IR output (with 3 tuple ids in the vector):
//
// define i64 @ValidTupleId(%"class.impala::ScalarExprEvaluator"* %eval,
//   %"class.impala::TupleRow"* %row) #48 {
// entry:
//   %0 = alloca i64
//   %1 = alloca i64
//   %2 = alloca i64
//   %tuple_row_ptr = getelementptr inbounds %"class.impala::TupleRow",
//     %"class.impala::TupleRow"* %row, i32 0
//   %tuple_ptr = bitcast %"class.impala::TupleRow"* %tuple_row_ptr to
//     %"class.impala::Tuple"**
//   %tuple_val = load %"class.impala::Tuple"*, %"class.impala::Tuple"** %tuple_ptr
//   %is_not_null = icmp ne %"class.impala::Tuple"* %tuple_val, null
//   br i1 %is_not_null, label %return, label %next
//
// next:                                             ; preds = %entry
//   %tuple_row_ptr2 = getelementptr inbounds %"class.impala::TupleRow",
//     %"class.impala::TupleRow"* %row, i32 1
//   %tuple_ptr3 = bitcast %"class.impala::TupleRow"* %tuple_row_ptr2 to
//     %"class.impala::Tuple"**
//   %tuple_val4 = load %"class.impala::Tuple"*, %"class.impala::Tuple"** %tuple_ptr3
//   %is_not_null5 = icmp ne %"class.impala::Tuple"* %tuple_val4, null
//   br i1 %is_not_null5, label %return7, label %next6
//
// return:                                           ; preds = %entry
//   %ret = load i64, i64* %2
//   %3 = and i64 %ret, 4294967295
//   %ret1 = or i64 %3, 4294967296    -- 1
//   ret i64 %ret1
//
// next6:                                            ; preds = %next
//   %tuple_row_ptr10 = getelementptr inbounds %"class.impala::TupleRow",
//     %"class.impala::TupleRow"* %row, i32 2
//   %tuple_ptr11 = bitcast %"class.impala::TupleRow"* %tuple_row_ptr10 to
//     %"class.impala::Tuple"**
//   %tuple_val12 = load %"class.impala::Tuple"*, %"class.impala::Tuple"** %tuple_ptr11
//   %is_not_null13 = icmp ne %"class.impala::Tuple"* %tuple_val12, null
//   br i1 %is_not_null13, label %return15, label %next14
//
// return7:                                          ; preds = %next
//   %ret8 = load i64, i64* %1
//   %4 = and i64 %ret8, 4294967295
//   %ret9 = or i64 %4, 12884901888   -- 3
//   ret i64 %ret9
//
// next14:                                           ; preds = %next6
//   ret i64 1
//
// return15:                                         ; preds = %next6
//   %ret16 = load i64, i64* %0
//   %5 = and i64 %ret16, 4294967295
//   %ret17 = or i64 %5, 21474836480   -- 5
//   ret i64 %ret17
// }
//
Status ValidTupleIdExpr::GetCodegendComputeFnImpl(
    LlvmCodeGen* codegen, llvm::Function** fn) {
  // Create a method with the expected signature.
  llvm::Value* args[2];
  llvm::Function* new_fn = CreateIrFunctionPrototype("ValidTupleId", codegen, &args);
  llvm::LLVMContext& context = codegen->context();
  llvm::BasicBlock* entry_block = llvm::BasicBlock::Create(context, "entry", new_fn);
  LlvmBuilder builder(entry_block);
  llvm::Value* row_ptr = args[1];

  // Unroll the loop.
  for (uint32_t i = 0; i < tuple_ids_.size(); i++) {
    // Get the i'th Tuple* from the row.
    llvm::Value* tuple_row_ptr =
        builder.CreateInBoundsGEP(row_ptr, codegen->GetI32Constant(i), "tuple_row_ptr");
    // Cast to Tuple**
    llvm::Type* tuple_ptr_ptr_type =
        codegen->GetPtrType(codegen->GetNamedPtrType(Tuple::LLVM_CLASS_NAME));
    llvm::Value* tuple_ptr_ptr =
        builder.CreateBitCast(tuple_row_ptr, tuple_ptr_ptr_type, "tuple_ptr_ptr");
    // Get the Tuple* and compare to nullptr.
    llvm::Value* tuple_ptr = builder.CreateLoad(tuple_ptr_ptr, "tuple_ptr");
    llvm::Value* is_not_null = builder.CreateIsNotNull(tuple_ptr, "is_not_null");

    // Create a conditional on the result.
    llvm::BasicBlock* next = llvm::BasicBlock::Create(context, "next", new_fn);
    llvm::BasicBlock* ret_block = llvm::BasicBlock::Create(context, "return", new_fn);
    builder.CreateCondBr(is_not_null, ret_block, next);

    // Add code to the block that is executed if the Tuple* was not a nullptr.
    builder.SetInsertPoint(ret_block);
    // Generate code returning an IntVal containing the tuple id.
    CodegenAnyVal result(codegen, &builder, ColumnType(TYPE_INT), nullptr, "ret");
    llvm::Constant* tuple_id =
        codegen->GetI32Constant(static_cast<uint64_t>(tuple_ids_[i]));
    result.SetVal(tuple_id);
    builder.CreateRet(result.GetLoweredValue());

    // Add code to the  block that is executed if the Tuple* was null.
    builder.SetInsertPoint(next);
  }
  // We fell out of the loop, return null.
  builder.CreateRet(CodegenAnyVal::GetNullVal(codegen, type()));

  *fn = codegen->FinalizeFunction(new_fn);
  if (UNLIKELY(*fn == nullptr)) {
    return Status(TErrorCode::IR_VERIFY_FAILED, "ValidTupleId");
  }
  return Status::OK();
}

string ValidTupleIdExpr::DebugString() const {
  return "ValidTupleId()";
}

} // namespace impala
