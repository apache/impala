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
#include "runtime/fragment-state.h"
#include "runtime/runtime-state.h"
#include "runtime/multi-precision.h"
#include "util/runtime-profile-counters.h"
#include "util/bit-util.h"

using namespace impala;
using namespace strings;

const char* TupleRowComparator::COMPARE_SYMBOL =
    "_ZNK6impala18TupleRowComparator7CompareEPKPNS_19ScalarExprEvaluatorES4_PKNS_"
    "8TupleRowES7_";

const char* TupleRowComparator::LLVM_CLASS_NAME = "class.impala::TupleRowComparator";

TupleRowComparatorConfig::TupleRowComparatorConfig(
    const TSortInfo& tsort_info, const std::vector<ScalarExpr*>& ordering_exprs)
  : sorting_order_(tsort_info.sorting_order),
    ordering_exprs_(ordering_exprs),
    num_lexical_keys_(tsort_info.num_lexical_keys_in_zorder),
    is_asc_(tsort_info.is_asc_order) {
  if (sorting_order_ == TSortingOrder::ZORDER) return;
  for (bool null_first : tsort_info.nulls_first) {
    nulls_first_.push_back(null_first ? -1 : 1);
  }
}

int TupleRowComparator::Compare(ScalarExprEvaluator* const* evaluator_lhs,
    ScalarExprEvaluator* const* evaluator_rhs, const TupleRow* lhs,
    const TupleRow* rhs) const {
  return CompareInterpreted(lhs, rhs);
}

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

Status TupleRowComparatorConfig::Codegen(FragmentState* state, llvm::Function** fn) {
  if (sorting_order_ == TSortingOrder::ZORDER) {
    return Status("Codegen not yet implemented for sorting order: ZORDER");
  }
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != nullptr);
  RETURN_IF_ERROR(CodegenLexicalCompare(codegen, fn));
  codegen->AddFunctionToJit(*fn, &codegend_compare_fn_);
  return Status::OK();
}

int TupleRowLexicalComparator::CompareInterpreted(
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

// Codegens an unrolled version of TupleRowLexicalComparator::Compare(). Uses codegen'd
// key exprs and injects nulls_first_ and is_asc_ values.
//
// Example IR for comparing an int column then a float column:
//
// ; Function Attrs: alwaysinline
// define i32 @Compare(%"class.impala::TupleRowComparator"*,
//                     %"class.impala::ScalarExprEvaluator"**
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
Status TupleRowComparatorConfig::CodegenLexicalCompare(
    LlvmCodeGen* codegen, llvm::Function** fn) {
  llvm::LLVMContext& context = codegen->context();
  const vector<ScalarExpr*>& ordering_exprs = ordering_exprs_;
  llvm::Function* key_fns[ordering_exprs.size()];
  for (int i = 0; i < ordering_exprs.size(); ++i) {
    Status status = ordering_exprs[i]->GetCodegendComputeFn(codegen, false, &key_fns[i]);
    if (!status.ok()) {
      return Status::Expected(Substitute(
            "Could not codegen TupleRowComparator::Compare(): $0", status.GetDetail()));
    }
  }

  // Construct function signature as follows.
  //
  // int Compare(TupleRowComparator*, ScalarExprEvaluator** ordering_expr_evals_lhs,
  //     ScalarExprEvaluator** ordering_expr_evals_rhs,
  //     TupleRow* lhs, TupleRow* rhs)
  //
  // Note that this is different than the interpreted Compare() function signature:
  //
  // int Compare(ScalarExprEvaluator** ordering_expr_evals_lhs,
  //     ScalarExprEvaluator** ordering_expr_evals_rhs,
  //     TupleRow* lhs, TupleRow* rhs)
  //
  llvm::PointerType* tuple_row_comparator_type =
      codegen->GetStructPtrType<TupleRowComparator>();
  llvm::PointerType* expr_evals_type =
      codegen->GetStructPtrPtrType<ScalarExprEvaluator>();
  llvm::PointerType* tuple_row_type = codegen->GetStructPtrType<TupleRow>();
  LlvmCodeGen::FnPrototype prototype(codegen, "Compare", codegen->i32_type());
  prototype.AddArgument("tuple_row_comparator_type", tuple_row_comparator_type);
  prototype.AddArgument("ordering_expr_evals_lhs", expr_evals_type);
  prototype.AddArgument("ordering_expr_evals_rhs", expr_evals_type);
  prototype.AddArgument("lhs", tuple_row_type);
  prototype.AddArgument("rhs", tuple_row_type);

  LlvmBuilder builder(context);
  llvm::Value* args[5];
  *fn = prototype.GeneratePrototype(&builder, args);
  args[0] = nullptr;
  llvm::Value* lhs_evals_arg = args[1];
  llvm::Value* rhs_evals_arg = args[2];
  llvm::Value* lhs_arg = args[3];
  llvm::Value* rhs_arg = args[4];

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

int TupleRowZOrderComparator::CompareInterpreted(const TupleRow* lhs,
    const TupleRow* rhs) const {
  DCHECK_EQ(ordering_exprs_.size(), ordering_expr_evals_lhs_.size());
  DCHECK_EQ(ordering_expr_evals_lhs_.size(), ordering_expr_evals_rhs_.size());

  // Sort the partition keys lexically. The PreInsert SortNode uses ASC order and NULLS
  // LAST. See Planner.createPreInsertSort() in FE.
  for (int i = 0; i < num_lexical_keys_; ++i) {
    void* lhs_value = ordering_expr_evals_lhs_[i]->GetValue(lhs);
    void* rhs_value = ordering_expr_evals_rhs_[i]->GetValue(rhs);

    // The sort order of NULLs is independent of asc/desc.
    if (lhs_value == NULL && rhs_value == NULL) continue;
    if (lhs_value == NULL) return 1;
    if (rhs_value == NULL) return -1;

    int result = RawValue::Compare(lhs_value, rhs_value, ordering_exprs_[i]->type());
    if (result != 0) return result;
    // Otherwise, try the next Expr
  }

  // Sort the remaining keys in Z-order.
  if (max_col_size_ <= 4) {
    return CompareBasedOnSize<uint32_t>(lhs, rhs);
  } else if (max_col_size_ <= 8) {
    return CompareBasedOnSize<uint64_t>(lhs, rhs);
  } else {
    return CompareBasedOnSize<uint128_t>(lhs, rhs);
  }
}

template<typename U>
int TupleRowZOrderComparator::CompareBasedOnSize(const TupleRow* lhs,
    const TupleRow* rhs) const {
  auto less_msb = [](U x, U y) { return x < y && x < (x ^ y); };
  ColumnType type = ordering_exprs_[num_lexical_keys_]->type();
  // Values of the most significant dimension from both sides.
  U msd_lhs = GetSharedRepresentation<U>(
      ordering_expr_evals_lhs_[num_lexical_keys_]->GetValue(lhs), type);
  U msd_rhs = GetSharedRepresentation<U>(
      ordering_expr_evals_rhs_[num_lexical_keys_]->GetValue(rhs), type);
  for (int i = num_lexical_keys_ + 1; i < ordering_exprs_.size(); ++i) {
    type = ordering_exprs_[i]->type();
    void* lhs_v = ordering_expr_evals_lhs_[i]->GetValue(lhs);
    void* rhs_v = ordering_expr_evals_rhs_[i]->GetValue(rhs);

    U lhsi = GetSharedRepresentation<U>(lhs_v, type);
    U rhsi = GetSharedRepresentation<U>(rhs_v, type);

    if (less_msb(msd_lhs ^ msd_rhs, lhsi ^ rhsi)) {
      msd_lhs = lhsi;
      msd_rhs = rhsi;
    }
  }
  return msd_lhs < msd_rhs ? -1 : (msd_lhs > msd_rhs ? 1 : 0);
}

template <typename U>
U TupleRowZOrderComparator::GetSharedRepresentation(void* val, ColumnType type) const {
  // The mask used for setting the sign bit correctly.
  if (val == NULL) return 0;
  constexpr U mask = (U)1 << (sizeof(U) * 8 - 1);
  switch (type.type) {
    case TYPE_NULL:
      return 0;
    case TYPE_BOOLEAN:
      return static_cast<U>(*reinterpret_cast<const bool*>(val)) << (sizeof(U) * 8 - 1);
    case TYPE_TINYINT:
      return GetSharedIntRepresentation<U, int8_t>(
          *reinterpret_cast<const int8_t*>(val), mask);
    case TYPE_SMALLINT:
      return GetSharedIntRepresentation<U, int16_t>(
          *reinterpret_cast<const int16_t*>(val), mask);
    case TYPE_INT:
      return GetSharedIntRepresentation<U, int32_t>(
          *reinterpret_cast<const int32_t*>(val), mask);
    case TYPE_BIGINT:
      return GetSharedIntRepresentation<U, int64_t>(
          *reinterpret_cast<const int64_t*>(val), mask);
    case TYPE_DATE:
      return GetSharedIntRepresentation<U, int32_t>(
          reinterpret_cast<const DateValue*>(val)->Value(), mask);
    case TYPE_FLOAT:
      return GetSharedFloatRepresentation<U, float>(val, mask);
    case TYPE_DOUBLE:
      return GetSharedFloatRepresentation<U, double>(val, mask);
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      const StringValue* string_value = reinterpret_cast<const StringValue*>(val);
      return GetSharedStringRepresentation<U>(string_value->Ptr(), string_value->Len());
    }
    case TYPE_CHAR:
      return GetSharedStringRepresentation<U>(
          reinterpret_cast<const char*>(val), type.len);
    case TYPE_TIMESTAMP: {
      const TimestampValue* ts = reinterpret_cast<const TimestampValue*>(val);
      const uint128_t nanosnds = static_cast<uint128_t>(ts->time().total_nanoseconds());
      const uint128_t days = static_cast<uint128_t>(ts->date().day_number());
      return (days << 64) | nanosnds;
    }
    case TYPE_DECIMAL:
      switch (type.GetByteSize()) {
        case 4:
          return GetSharedIntRepresentation<U, int32_t>(
              reinterpret_cast<const Decimal4Value*>(val)->value(), mask);
        case 8:
          return GetSharedIntRepresentation<U, int64_t>(
              reinterpret_cast<const Decimal8Value*>(val)->value(), mask);
        case 16: // value is of int128_t, big enough that no shifts are needed
          return static_cast<U>(
              reinterpret_cast<const Decimal16Value*>(val)->value()) ^ mask;
        default:
          DCHECK(false) << type;
          return 0;
      }
    default:
      return 0;
  }
}

template <typename U, typename T>
U inline TupleRowZOrderComparator::GetSharedIntRepresentation(const T val, U mask) const {
  uint64_t shift_size = static_cast<uint64_t>(
      std::max(static_cast<int64_t>((sizeof(U) - sizeof(T)) * 8), (int64_t) 0));
  return (static_cast<U>(val) << shift_size) ^ mask;
}

template <typename U, typename T>
U inline TupleRowZOrderComparator::GetSharedFloatRepresentation(void* val, U mask) const {
  int64_t tmp;
  T floating_value = *reinterpret_cast<const T*>(val);
  memcpy(&tmp, &floating_value, sizeof(T));
  if (UNLIKELY(std::isnan(floating_value))) return 0;
  uint64_t shift_size = static_cast<uint64_t>(
      std::max(static_cast<int64_t>((sizeof(U) - sizeof(T)) * 8), (int64_t) 0));
  if (floating_value < 0.0) {
    // Flipping all bits for negative values.
    return static_cast<U>(~tmp) << shift_size;
  } else {
    // Flipping only first bit.
    return (static_cast<U>(tmp) << shift_size) ^ mask;
  }
}

template <typename U>
U inline TupleRowZOrderComparator::GetSharedStringRepresentation(const char* char_ptr,
    int length) const {
  int len = length < sizeof(U) ? length : sizeof(U);
  if (len == 0) return 0;
  U dst = 0;
  // We copy the bytes from the string but swap the bytes because of integer endianness.
  BitUtil::ByteSwap(&dst, char_ptr, len);
  return dst << ((sizeof(U) - len) * 8);
}

