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

#include "exprs/tuple-is-null-predicate.h"

#include <sstream>

#include "gen-cpp/Exprs_types.h"

#include "common/names.h"
#include "codegen/llvm-codegen.h"
#include "codegen/codegen-anyval.h"
#include "runtime/descriptors.h"
#include "runtime/tuple-row.h"

namespace impala {

BooleanVal TupleIsNullPredicate::GetBooleanValInterpreted(
    ScalarExprEvaluator* evaluator, const TupleRow* row) const {
  int count = 0;
  for (int i = 0; i < tuple_idxs_.size(); ++i) {
    count += row->GetTuple(tuple_idxs_[i]) == NULL;
  }
  // Return true only if all originally specified tuples are NULL. Return false if any
  // tuple is non-nullable.
  return BooleanVal(count == tuple_ids_.size());
}

TupleIsNullPredicate::TupleIsNullPredicate(const TExprNode& node)
  : Predicate(node),
    tuple_ids_(node.tuple_is_null_pred.tuple_ids.begin(),
        node.tuple_is_null_pred.tuple_ids.end()) {}

Status TupleIsNullPredicate::Init(
    const RowDescriptor& row_desc, bool is_entry_point, FragmentState* state) {
  RETURN_IF_ERROR(ScalarExpr::Init(row_desc, is_entry_point, state));
  DCHECK_EQ(0, children_.size());
  // Resolve tuple ids to tuple indexes.
  for (int i = 0; i < tuple_ids_.size(); ++i) {
    int32_t tuple_idx = row_desc.GetTupleIdx(tuple_ids_[i]);
    if (tuple_idx == RowDescriptor::INVALID_IDX) {
      // This should not happen and indicates a planner issue. This code is tricky
      // so rather than crashing, do this as a stop gap.
      // TODO: remove this code and replace with DCHECK.
      return Status("Invalid plan. TupleIsNullPredicate has invalid tuple idx.");
    }
    if (row_desc.TupleIsNullable(tuple_idx)) tuple_idxs_.push_back(tuple_idx);
  }
  return Status::OK();
}

/// For sample IR, see 'FillCodegendComputeFnConstantFalse' and
/// 'FillCodegendComputeFnNonConstant'.
/// If there is at least one non-nullable tuple, we codegen a function that returns a
/// constant false BooleanValue.
Status TupleIsNullPredicate::GetCodegendComputeFnImpl(LlvmCodeGen* codegen,
    llvm::Function** fn) {
  DCHECK_EQ(0, GetNumChildren());

  llvm::Value* args[2];
  llvm::Function* function = CreateIrFunctionPrototype(
      "TupleIsNullPredicate", codegen, &args);

  if (tuple_idxs_.size() < tuple_ids_.size()) {
    FillCodegendComputeFnConstantFalse(codegen, function);
  } else {
    FillCodegendComputeFnNonConstant(codegen, function, args);
  }

  *fn = codegen->FinalizeFunction(function);
  if (UNLIKELY(*fn == nullptr)) {
    return Status(TErrorCode::IR_VERIFY_FAILED, "TupleIsNullPredicate");
  }
  return Status::OK();
}

/// Sample IR:
/// define i16 @TupleIsNullPredicate(%"class.impala::ScalarExprEvaluator"* %eval,
///                                  %"class.impala::TupleRow"* %row) #50 {
/// return_false:
///   ret i16 0
/// }
void TupleIsNullPredicate::FillCodegendComputeFnConstantFalse(
    LlvmCodeGen* codegen, llvm::Function* function) const {
  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);

  llvm::BasicBlock* return_false_block =
      llvm::BasicBlock::Create(context, "return_false", function);
  builder.SetInsertPoint(return_false_block);

  CodegenAnyVal ret_val = CodegenAnyVal::GetNonNullVal(
      codegen, &builder, ColumnType(TYPE_BOOLEAN), "ret_val");
  ret_val.SetVal(false);
  builder.CreateRet(ret_val.GetLoweredValue());
}

/// Sample IR:
/// define i16 @TupleIsNullPredicate(%"class.impala::ScalarExprEvaluator"* %eval,
///                                  %"class.impala::TupleRow"* %row) #51 {
/// entry:
///   br label %check_null
///
/// check_null:                                       ; preds = %entry
///   %tuple_is_null_fn = call i1 @_ZN6impala19TupleRowTupleIsNullEPKNS_8TupleRowEi(
///       %"class.impala::TupleRow"* %row, i32 0)
///   %0 = select i1 %tuple_is_null_fn, i32 1, i32 0
///   %count = add i32 0, %0
///   br label %check_null1
///
/// check_null1:                                      ; preds = %check_null
///   %tuple_is_null_fn2 = call i1 @_ZN6impala19TupleRowTupleIsNullEPKNS_8TupleRowEi(
///       %"class.impala::TupleRow"* %row, i32 1)
///   %1 = select i1 %tuple_is_null_fn2, i32 1, i32 0
///   %count3 = add i32 %count, %1
///   br label %compare_count
///
/// compare_count:                                    ; preds = %check_null1
///   %count_eq_col_num = icmp eq i32 %count3, 2
///   %2 = zext i1 %count_eq_col_num to i16
///   %3 = shl i16 %2, 8
///   %ret_val = or i16 0, %3
///   ret i16 %ret_val
/// }
void TupleIsNullPredicate::FillCodegendComputeFnNonConstant(
    LlvmCodeGen* codegen, llvm::Function* function, llvm::Value* args[2]) const {
  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);

  llvm::BasicBlock* entry_block =
      llvm::BasicBlock::Create(context, "entry", function);
  builder.SetInsertPoint(entry_block);

  // Signature:
  // bool TupleRowTupleIsNull(const TupleRow* row, int index);
  // Returns whether row->GetTuple(index) is nullptr.
  llvm::Function* tuple_is_null_fn =
      codegen->GetFunction(IRFunction::TUPLE_ROW_GET_TUPLE_IS_NULL, false);

  llvm::Value* current_count_val = codegen->GetI32Constant(0);
  for (int i = 0; i < tuple_idxs_.size(); i++) {
    // We create the next block that checks whether the next tuple is null and increases
    // 'count' if it is. While the builder's insertion point is still in the previous
    // block we insert a branch to the new block.
    llvm::BasicBlock* check_null_block
        = llvm::BasicBlock::Create(context, "check_null", function);
    builder.CreateBr(check_null_block);

    builder.SetInsertPoint(check_null_block);
    llvm::Value* tuple_is_null = builder.CreateCall(tuple_is_null_fn,
        {args[1], codegen->GetI32Constant(tuple_idxs_[i])}, "tuple_is_null_fn");
    llvm::Value* tuple_is_null_int = builder.CreateSelect(tuple_is_null,
        codegen->GetI32Constant(1), codegen->GetI32Constant(0));
    current_count_val = builder.CreateAdd(current_count_val, tuple_is_null_int, "count");
  }

  // Create the last block that will compare 'count' with the original number of tuples.
  // If they are equal, return a true value, otherwise a false value. While the builder's
  // insertion point is still in the previous block we insert a branch to the new block.
  llvm::BasicBlock* compare_count_block
      = llvm::BasicBlock::Create(context, "compare_count", function);
  builder.CreateBr(compare_count_block);

  builder.SetInsertPoint(compare_count_block);
  llvm::Constant* tuple_ids_size = codegen->GetI32Constant(tuple_ids_.size());
  llvm::Value* cmp =
      builder.CreateICmpEQ(current_count_val, tuple_ids_size, "count_eq_col_num");
  CodegenAnyVal ret_val = CodegenAnyVal::GetNonNullVal(
      codegen, &builder, ColumnType(TYPE_BOOLEAN), "ret_val");
  ret_val.SetVal(cmp);
  builder.CreateRet(ret_val.GetLoweredValue());
}

string TupleIsNullPredicate::DebugString() const {
  stringstream out;
  out << "TupleIsNullPredicate(tupleids=[";
  for (int i = 0; i < tuple_ids_.size(); ++i) {
    out << (i == 0 ? "" : " ") << tuple_ids_[i];
  }
  out << "])";
  return out.str();
}

}
