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

#include "exec/filter-context.h"

#include "codegen/codegen-anyval.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/tuple-row.h"
#include "util/min-max-filter.h"
#include "util/runtime-profile-counters.h"
#include "service/hs2-util.h"

using namespace impala;
using namespace strings;

const std::string FilterStats::ROW_GROUPS_KEY = "RowGroups";
const std::string FilterStats::FILES_KEY = "Files";
const std::string FilterStats::SPLITS_KEY = "Splits";
const std::string FilterStats::ROWS_KEY = "Rows";

const char* FilterContext::LLVM_CLASS_NAME = "struct.impala::FilterContext";

FilterStats::FilterStats(RuntimeProfile* runtime_profile) {
  DCHECK(runtime_profile != nullptr);
  profile = runtime_profile;
  RegisterCounterGroup(FilterStats::SPLITS_KEY);
  RegisterCounterGroup(FilterStats::FILES_KEY);
  // TODO: These only apply to Parquet, so only register them in that case.
  RegisterCounterGroup(FilterStats::ROWS_KEY);
  RegisterCounterGroup(FilterStats::ROW_GROUPS_KEY);
}

void FilterStats::IncrCounters(const string& key, int32_t total, int32_t processed,
    int32_t rejected) const {
  CountersMap::const_iterator it = counters.find(key);
  DCHECK(it != counters.end()) << "Tried to increment unknown counter group";
  it->second.total->Add(total);
  it->second.processed->Add(processed);
  it->second.rejected->Add(rejected);
}

/// Adds a new counter group with key 'key'. Not thread safe.
void FilterStats::RegisterCounterGroup(const string& key) {
  CounterGroup counter;
  counter.total =
      ADD_COUNTER(profile, Substitute("$0 total", key), TUnit::UNIT);
  counter.processed =
      ADD_COUNTER(profile, Substitute("$0 processed", key), TUnit::UNIT);
  counter.rejected =
      ADD_COUNTER(profile, Substitute("$0 rejected", key), TUnit::UNIT);
  counters[key] = counter;
}

Status FilterContext::CloneFrom(const FilterContext& from, ObjectPool* pool,
    RuntimeState* state, MemPool* expr_perm_pool, MemPool* expr_results_pool) {
  filter = from.filter;
  stats = from.stats;
  return from.expr_eval->Clone(
      pool, state, expr_perm_pool, expr_results_pool, &expr_eval);
}

bool FilterContext::Eval(TupleRow* row) const noexcept {
  void* val = expr_eval->GetValue(row);
  return filter->Eval(val, expr_eval->root().type());
}

void FilterContext::Insert(TupleRow* row) const noexcept {
  if (filter->is_bloom_filter()) {
    if (local_bloom_filter == nullptr) return;
    void* val = expr_eval->GetValue(row);
    uint32_t filter_hash = RawValue::GetHashValueFastHash32(
        val, expr_eval->root().type(), RuntimeFilterBank::DefaultHashSeed());
    local_bloom_filter->Insert(filter_hash);
  } else if (filter->is_min_max_filter()) {
    if (local_min_max_filter == nullptr || local_min_max_filter->AlwaysTrue()) return;
    void* val = expr_eval->GetValue(row);
    local_min_max_filter->Insert(val);
  } else {
    DCHECK(filter->is_in_list_filter());
    if (local_in_list_filter == nullptr || local_in_list_filter->AlwaysTrue()) return;
    local_in_list_filter->Insert(expr_eval->GetValue(row));
  }
}

void FilterContext::InsertPerCompareOp(TupleRow* row) const noexcept {
  if (filter->getCompareOp() == extdatasource::TComparisonOp::type::EQ) {
    Insert(row);
  } else {
    DCHECK(filter->is_min_max_filter());
    if (local_min_max_filter == nullptr || local_min_max_filter->AlwaysTrue()) return;
    void* val = expr_eval->GetValue(row);
    switch (filter->getCompareOp()) {
      case extdatasource::TComparisonOp::type::LE:
        local_min_max_filter->InsertForLE(val);
        break;
      case extdatasource::TComparisonOp::type::LT:
        local_min_max_filter->InsertForLT(val);
        break;
      case extdatasource::TComparisonOp::type::GE:
        local_min_max_filter->InsertForGE(val);
        break;
      case extdatasource::TComparisonOp::type::GT:
        local_min_max_filter->InsertForGT(val);
        break;
      default:
        DCHECK(false)
            << "Unsupported comparison op in FilterContext::InsertPerCompareOp()";
    }
  }
}

void FilterContext::MaterializeValues() const {
  if (filter->is_min_max_filter() && local_min_max_filter != nullptr) {
    local_min_max_filter->MaterializeValues();
  } else if (filter->is_in_list_filter() && local_in_list_filter != nullptr) {
    local_in_list_filter->MaterializeValues();
  }
}

// An example of the generated code for the following query:
//
// select a.outer_struct, b.small_struct
// from functional_orc_def.complextypes_nested_structs a
//     inner join functional_orc_def.complextypes_structs b
//     on b.small_struct.i = a.outer_struct.inner_struct2.i + 19091;
//
// define i1 @FilterContextEval(%"struct.impala::FilterContext"* %this,
//     %"class.impala::TupleRow"* %row) #50 {
// entry:
//   %0 = alloca i64
//   %expr_eval_ptr = getelementptr inbounds %"struct.impala::FilterContext",
//       %"struct.impala::FilterContext"* %this, i32 0, i32 0
//   %expr_eval_arg = load %"class.impala::ScalarExprEvaluator"*,
//       %"class.impala::ScalarExprEvaluator"** %expr_eval_ptr
//   %result = call { i8, i64 } @"impala::Operators::Add_BigIntVal_BigIntValWrapper"(
//       %"class.impala::ScalarExprEvaluator"* %expr_eval_arg,
//       %"class.impala::TupleRow"* %row)
//   br label %entry1
//
// entry1:                                           ; preds = %entry
//   %1 = extractvalue { i8, i64 } %result, 0
//   %is_null = trunc i8 %1 to i1
//   br i1 %is_null, label %null, label %non_null
//
// non_null:                                         ; preds = %entry1
//   %val = extractvalue { i8, i64 } %result, 1
//   store i64 %val, i64* %0
//   %native_ptr = bitcast i64* %0 to i8*
//   br label %eval_filter
//
// null:                                             ; preds = %entry1
//   br label %eval_filter
//
// eval_filter:                                      ; preds = %non_null, %null
//   %native_ptr_phi = phi i8* [ %native_ptr, %non_null ], [ null, %null ]
//   %filter_ptr = getelementptr inbounds %"struct.impala::FilterContext",
//       %"struct.impala::FilterContext"* %this, i32 0, i32 1
//   %filter_arg = load %"class.impala::RuntimeFilter"*,
//       %"class.impala::RuntimeFilter"** %filter_ptr
//   %passed_filter = call i1 @_ZNK6impala13RuntimeFilter4EvalEPvRKNS_10ColumnTypeE(
//       %"class.impala::RuntimeFilter"* %filter_arg, i8* %native_ptr_phi,
//       %"struct.impala::ColumnType"* @expr_type_arg)
//   ret i1 %passed_filter
// }
Status FilterContext::CodegenEval(
    LlvmCodeGen* codegen, ScalarExpr* filter_expr, llvm::Function** fn) {
  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);

  *fn = nullptr;
  llvm::PointerType* this_type = codegen->GetStructPtrType<FilterContext>();
  llvm::PointerType* tuple_row_ptr_type = codegen->GetStructPtrType<TupleRow>();
  LlvmCodeGen::FnPrototype prototype(codegen, "FilterContextEval",
      codegen->bool_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this", this_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  llvm::Value* args[2];
  llvm::Function* eval_filter_fn = prototype.GeneratePrototype(&builder, args);
  llvm::Value* this_arg = args[0];
  llvm::Value* row_arg = args[1];

  llvm::Function* compute_fn;
  RETURN_IF_ERROR(filter_expr->GetCodegendComputeFn(codegen, false, &compute_fn));
  DCHECK(compute_fn != nullptr);

  // The function for checking against the bloom filter for match.
  llvm::Function* runtime_filter_fn =
      codegen->GetFunction(IRFunction::RUNTIME_FILTER_EVAL, false);
  DCHECK(runtime_filter_fn != nullptr);

  // Load 'expr_eval' from 'this_arg' FilterContext object.
  llvm::Value* expr_eval_ptr =
      builder.CreateStructGEP(nullptr, this_arg, 0, "expr_eval_ptr");
  llvm::Value* expr_eval_arg = builder.CreateLoad(expr_eval_ptr, "expr_eval_arg");

  // Evaluate the row against the filter's expression.
  llvm::Value* compute_fn_args[] = {expr_eval_arg, row_arg};
  CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
      filter_expr->type(), compute_fn, compute_fn_args, "result");

  CodegenAnyValReadWriteInfo rwi = result.ToReadWriteInfo();
  rwi.entry_block().BranchTo(&builder);

  llvm::BasicBlock* eval_filter_block =
      llvm::BasicBlock::Create(context, "eval_filter", eval_filter_fn);

  // Set the pointer to NULL in case it evaluates to NULL.
  builder.SetInsertPoint(rwi.null_block());
  llvm::Value* null_ptr = codegen->null_ptr_value();
  builder.CreateBr(eval_filter_block);

  // Saves 'result' on the stack and passes a pointer to it to 'runtime_filter_fn'.
  builder.SetInsertPoint(rwi.non_null_block());
  llvm::Value* native_ptr = SlotDescriptor::CodegenStoreNonNullAnyValToNewAlloca(rwi);
  native_ptr = builder.CreatePointerCast(native_ptr, codegen->ptr_type(), "native_ptr");
  builder.CreateBr(eval_filter_block);

  // Get the arguments in place to call 'runtime_filter_fn' to see if the row passes.
  builder.SetInsertPoint(eval_filter_block);
  llvm::PHINode* val_ptr_phi = rwi.CodegenNullPhiNode(native_ptr, null_ptr,
      "val_ptr_phi");

  // Create a global constant of the filter expression's ColumnType. It needs to be a
  // constant for constant propagation and dead code elimination in 'runtime_filter_fn'.
  llvm::Type* col_type = codegen->GetStructType<ColumnType>();
  llvm::Constant* expr_type_arg = codegen->ConstantToGVPtr(
      col_type, filter_expr->type().ToIR(codegen), "expr_type_arg");

  // Load 'filter' from 'this_arg' FilterContext object.
  llvm::Value* filter_ptr = builder.CreateStructGEP(nullptr, this_arg, 1, "filter_ptr");
  llvm::Value* filter_arg = builder.CreateLoad(filter_ptr, "filter_arg");

  llvm::Value* run_filter_args[] = {filter_arg, val_ptr_phi, expr_type_arg};
  llvm::Value* passed_filter =
      builder.CreateCall(runtime_filter_fn, run_filter_args, "passed_filter");
  builder.CreateRet(passed_filter);

  *fn = codegen->FinalizeFunction(eval_filter_fn);
  if (*fn == nullptr) {
    return Status("Codegen'ed FilterContext::Eval() fails verification, see log");
  }
  return Status::OK();
}

// An example of the generated code the following query: RF001[min_max] -> a.smallint_col
//
// set minmax_filter_threshold=1.0;
// set minmax_filtering_level=PAGE;
// select straight_join count(a.id) from
// alltypes a join [SHUFFLE] alltypes b
// on a.smallint_col = b.bigint_col;
//
//
// ; Function Attrs: noinline nounwind
// define internal fastcc void @InsertRuntimeFilters(%"struct.impala::FilterContext"*
// nocapture readonly %filter_ctxs, %"class.impala::TupleRow"* nocapture readonly %row)
// unnamed_addr #6 personality i32 (...)* @__gxx_personality_v0 { entry:
//   %local_bloom_filter_ptr.i = getelementptr inbounds %"struct.impala::FilterContext",
//   %"struct.impala::FilterContext"* %filter_ctxs, i64 0, i32 3 %local_bloom_filter_arg.i
//   = load %"class.impala::BloomFilter"*, %"class.impala::BloomFilter"**
//   %local_bloom_filter_ptr.i, align 8 %filter_is_null.i = icmp eq
//   %"class.impala::BloomFilter"* %local_bloom_filter_arg.i, null br i1
//   %filter_is_null.i, label %FilterContextInsert.exit, label %check_val_block.i
//
// check_val_block.i:                                ; preds = %entry
//   %cast_row_ptr.i.i = bitcast %"class.impala::TupleRow"* %row to i8**
//   %tuple_ptr.i.i = load i8*, i8** %cast_row_ptr.i.i, align 8
//   %null_byte_ptr.i.i = getelementptr inbounds i8, i8* %tuple_ptr.i.i, i64 8
//   %null_byte.i.i = load i8, i8* %null_byte_ptr.i.i, align 1
//   %null_mask.i.i = and i8 %null_byte.i.i, 1
//   %is_null.i.i = icmp eq i8 %null_mask.i.i, 0
//   br i1 %is_null.i.i, label %_ZN6impala8HashUtil10FastHash64EPKvlm.exit1.i.i, label
//   %_ZN6impala8RawValue22GetHashValueFastHash32EPKvRKNS_10ColumnTypeEj.exit.i
//
// _ZN6impala8HashUtil10FastHash64EPKvlm.exit1.i.i:  ; preds = %check_val_block.i
//   %val_ptr.i.i = bitcast i8* %tuple_ptr.i.i to i64*
//   %val.i.i = load i64, i64* %val_ptr.i.i, align 8
//   %0 = lshr i64 %val.i.i, 23
//   %1 = xor i64 %0, %val.i.i
//   %2 = mul i64 %1, 2388976653695081527
//   %3 = lshr i64 %2, 47
//   %4 = xor i64 %2, 4619197404915748858
//   %5 = xor i64 %4, %3
//   %6 = mul i64 %5, -8645972361240307355
//   %7 = lshr i64 %6, 23
//   %8 = xor i64 %7, %6
//   %9 = mul i64 %8, 2388976653695081527
//   %10 = lshr i64 %9, 47
//   %11 = xor i64 %10, %9
//   br label %_ZN6impala8RawValue22GetHashValueFastHash32EPKvRKNS_10ColumnTypeEj.exit.i
//
// _ZN6impala8RawValue22GetHashValueFastHash32EPKvRKNS_10ColumnTypeEj.exit.i: ; preds =
// %_ZN6impala8HashUtil10FastHash64EPKvlm.exit1.i.i, %check_val_block.i
//   %12 = phi i64 [ %11, %_ZN6impala8HashUtil10FastHash64EPKvlm.exit1.i.i ], [
//   -1206697893868870850, %check_val_block.i ] %13 = lshr i64 %12, 32 %14 = sub i64 %12,
//   %13 %15 = trunc i64 %14 to i32 %16 = getelementptr inbounds
//   %"class.impala::BloomFilter", %"class.impala::BloomFilter"*
//   %local_bloom_filter_arg.i, i64 0, i32 1 tail call void
//   @_ZN4kudu16BlockBloomFilter6InsertEj(%"class.kudu::BlockBloomFilter"* %16, i32 %15)
//   #9 br label %FilterContextInsert.exit
//
// FilterContextInsert.exit:                         ; preds = %entry,
// %_ZN6impala8RawValue22GetHashValueFastHash32EPKvRKNS_10ColumnTypeEj.exit.i
//   %local_min_max_filter_ptr.i = getelementptr inbounds %"struct.impala::FilterContext",
//   %"struct.impala::FilterContext"* %filter_ctxs, i64 1, i32 4
//   %cast_min_max_filter_ptr.i = bitcast %"class.impala::MinMaxFilter"**
//   %local_min_max_filter_ptr.i to %"class.impala::BigIntMinMaxFilter"**
//   %local_min_max_filter_arg.i = load %"class.impala::BigIntMinMaxFilter"*,
//   %"class.impala::BigIntMinMaxFilter"** %cast_min_max_filter_ptr.i, align 8
//   %filter_is_null.i1 = icmp eq %"class.impala::BigIntMinMaxFilter"*
//   %local_min_max_filter_arg.i, null br i1 %filter_is_null.i1, label
//   %FilterContextInsert.2.exit, label %filters_not_null.i
//
// filters_not_null.i:                               ; preds = %FilterContextInsert.exit
//   %17 = getelementptr inbounds %"class.impala::BigIntMinMaxFilter",
//   %"class.impala::BigIntMinMaxFilter"* %local_min_max_filter_arg.i, i64 0, i32 0, i32 1
//   %18 = load i8, i8* %17, align 8, !tbaa !2, !range !7
//   %19 = icmp eq i8 %18, 0
//   br i1 %19, label %always_true_false_block.i, label %FilterContextInsert.2.exit
//
// always_true_false_block.i:                        ; preds = %filters_not_null.i
//   %cast_row_ptr.i.i3 = bitcast %"class.impala::TupleRow"* %row to i8**
//   %tuple_ptr.i.i4 = load i8*, i8** %cast_row_ptr.i.i3, align 8
//   %null_byte_ptr.i.i5 = getelementptr inbounds i8, i8* %tuple_ptr.i.i4, i64 8
//   %null_byte.i.i6 = load i8, i8* %null_byte_ptr.i.i5, align 1
//   %null_mask.i.i7 = and i8 %null_byte.i.i6, 1
//   %is_null.i.i8 = icmp eq i8 %null_mask.i.i7, 0
//   br i1 %is_null.i.i8, label %20, label %FilterContextInsert.2.exit
//
// ; <label>:20:                                     ; preds = %always_true_false_block.i
//   %val_ptr.i.i9 = bitcast i8* %tuple_ptr.i.i4 to i64*
//   %val.i.i10 = load i64, i64* %val_ptr.i.i9, align 8
//   %21 = getelementptr inbounds %"class.impala::BigIntMinMaxFilter",
//   %"class.impala::BigIntMinMaxFilter"* %local_min_max_filter_arg.i, i64 0, i32 1 %22 =
//   load i64, i64* %21, align 8, !tbaa !8 %23 = icmp slt i64 %val.i.i10, %22 br i1 %23,
//   label %24, label %25, !prof !11
//
// ; <label>:24:                                     ; preds = %20
//   store i64 %val.i.i10, i64* %21, align 8, !tbaa !8
//   br label %25
//
// ; <label>:25:                                     ; preds = %24, %20
//   %26 = getelementptr inbounds %"class.impala::BigIntMinMaxFilter",
//   %"class.impala::BigIntMinMaxFilter"* %local_min_max_filter_arg.i, i64 0, i32 2 %27 =
//   load i64, i64* %26, align 8, !tbaa !12 %28 = icmp sgt i64 %val.i.i10, %27 br i1 %28,
//   label %29, label %FilterContextInsert.2.exit, !prof !11
//
// ; <label>:29:                                     ; preds = %25
//   store i64 %val.i.i10, i64* %26, align 8, !tbaa !12
//   br label %FilterContextInsert.2.exit
//
// FilterContextInsert.2.exit:                       ; preds = %FilterContextInsert.exit,
// %filters_not_null.i, %always_true_false_block.i, %25, %29
//   ret void
// }
Status FilterContext::CodegenInsert(LlvmCodeGen* codegen, ScalarExpr* filter_expr,
    const TRuntimeFilterDesc& filter_desc, llvm::Function** fn) {
  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);

  *fn = nullptr;
  llvm::PointerType* this_type = codegen->GetStructPtrType<FilterContext>();
  llvm::PointerType* tuple_row_ptr_type = codegen->GetStructPtrType<TupleRow>();
  LlvmCodeGen::FnPrototype prototype(
      codegen, "FilterContextInsert", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this", this_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  llvm::Value* args[2];
  llvm::Function* insert_filter_fn = prototype.GeneratePrototype(&builder, args);
  llvm::Value* this_arg = args[0];
  llvm::Value* row_arg = args[1];

  llvm::Value* local_filter_arg;
  // The function for inserting into the in-list filter.
  llvm::Function* insert_in_list_filter_fn = nullptr;
  if (filter_desc.type == TRuntimeFilterType::BLOOM) {
    // Load 'local_bloom_filter' from 'this_arg' FilterContext object.
    llvm::Value* local_bloom_filter_ptr =
        builder.CreateStructGEP(nullptr, this_arg, 3, "local_bloom_filter_ptr");
    local_filter_arg =
        builder.CreateLoad(local_bloom_filter_ptr, "local_bloom_filter_arg");
  } else if (filter_desc.type == TRuntimeFilterType::MIN_MAX) {
    // Load 'local_min_max_filter' from 'this_arg' FilterContext object.
    llvm::Value* local_min_max_filter_ptr =
        builder.CreateStructGEP(nullptr, this_arg, 4, "local_min_max_filter_ptr");
    llvm::PointerType* min_max_filter_type =
        codegen->GetNamedPtrType(MinMaxFilter::GetLlvmClassName(
        filter_expr->type().type))->getPointerTo();
    local_min_max_filter_ptr = builder.CreatePointerCast(
        local_min_max_filter_ptr, min_max_filter_type, "cast_min_max_filter_ptr");
    local_filter_arg =
        builder.CreateLoad(local_min_max_filter_ptr, "local_min_max_filter_arg");
  } else {
    DCHECK(filter_desc.type == TRuntimeFilterType::IN_LIST);
    // Load 'local_in_list_filter' from 'this_arg' FilterContext object.
    llvm::Value* local_in_list_filter_ptr =
        builder.CreateStructGEP(nullptr, this_arg, 5, "local_in_list_filter_ptr");
    switch (filter_expr->type().type) {
      case TYPE_TINYINT:
        insert_in_list_filter_fn = codegen->GetFunction(
            IRFunction::TINYINT_IN_LIST_FILTER_INSERT, false);
        break;
      case TYPE_SMALLINT:
        insert_in_list_filter_fn = codegen->GetFunction(
            IRFunction::SMALLINT_IN_LIST_FILTER_INSERT, false);
        break;
      case TYPE_INT:
        insert_in_list_filter_fn = codegen->GetFunction(
            IRFunction::INT_IN_LIST_FILTER_INSERT, false);
        break;
      case TYPE_BIGINT:
        insert_in_list_filter_fn = codegen->GetFunction(
            IRFunction::BIGINT_IN_LIST_FILTER_INSERT, false);
        break;
      case TYPE_DATE:
        insert_in_list_filter_fn = codegen->GetFunction(
            IRFunction::DATE_IN_LIST_FILTER_INSERT, false);
        break;
      case TYPE_STRING:
        insert_in_list_filter_fn = codegen->GetFunction(
            IRFunction::STRING_IN_LIST_FILTER_INSERT, false);
        break;
      case TYPE_CHAR:
        insert_in_list_filter_fn = codegen->GetFunction(
            IRFunction::CHAR_IN_LIST_FILTER_INSERT, false);
        break;
      case TYPE_VARCHAR:
        insert_in_list_filter_fn = codegen->GetFunction(
            IRFunction::VARCHAR_IN_LIST_FILTER_INSERT, false);
        break;
      default:
        DCHECK(false);
        break;
    }
    // Get type of the InListFilterImpl class from the first arg of the Insert() method.
    // We can't hardcode the class name since it's a template class. The class name will
    // be something like "class.impala::InListFilterImpl.1408". The last number is a
    // unique id appended by LLVM at runtime.
    llvm::Type* filter_impl_type = insert_in_list_filter_fn->arg_begin()->getType();
    llvm::PointerType* in_list_filter_type = codegen->GetPtrType(filter_impl_type);
    local_in_list_filter_ptr = builder.CreatePointerCast(
        local_in_list_filter_ptr, in_list_filter_type, "cast_in_list_filter_ptr");
    local_filter_arg =
        builder.CreateLoad(local_in_list_filter_ptr, "local_in_list_filter_arg");
  }

  // Check if 'local_bloom_filter', 'local_min_max_filter' or 'local_in_list_filter' are
  // NULL (depending on filter desc) and return if so.
  llvm::Value* filter_null = builder.CreateIsNull(local_filter_arg, "filter_is_null");
  llvm::BasicBlock* filter_not_null_block =
      llvm::BasicBlock::Create(context, "filters_not_null", insert_filter_fn);
  llvm::BasicBlock* filter_null_block =
      llvm::BasicBlock::Create(context, "filters_null", insert_filter_fn);
  llvm::BasicBlock* check_val_block =
      llvm::BasicBlock::Create(context, "check_val_block", insert_filter_fn);
  builder.CreateCondBr(filter_null, filter_null_block, filter_not_null_block);
  builder.SetInsertPoint(filter_null_block);
  builder.CreateRetVoid();
  builder.SetInsertPoint(filter_not_null_block);

  // Test whether 'local_min_max_filter->AlwaysTrue()' is true and return if so.
  if (filter_desc.type == TRuntimeFilterType::MIN_MAX) {
    // Get the function for boolean <Type>MinMaxFilter::AlwaysTrue().
    llvm::Function* always_true_member_fn = codegen->GetFunction(
        MinMaxFilter::GetAlwaysTrueIRFunctionType(filter_expr->type()), false);
    DCHECK(always_true_member_fn != nullptr);

    llvm::Value* always_true_result =
        builder.CreateCall(always_true_member_fn, {local_filter_arg});

    llvm::BasicBlock* always_true_true_block =
        llvm::BasicBlock::Create(context, "always_true_true_block", insert_filter_fn);
    llvm::BasicBlock* always_true_false_block =
        llvm::BasicBlock::Create(context, "always_true_false_block", insert_filter_fn);

    builder.CreateCondBr(
        always_true_result, always_true_true_block, always_true_false_block);

    builder.SetInsertPoint(always_true_true_block);
    builder.CreateRetVoid();
    builder.SetInsertPoint(always_true_false_block);
    builder.CreateBr(check_val_block);
  } else {
    builder.CreateBr(check_val_block);
  }
  builder.SetInsertPoint(check_val_block);


  llvm::Function* compute_fn;
  RETURN_IF_ERROR(filter_expr->GetCodegendComputeFn(codegen, false, &compute_fn));
  DCHECK(compute_fn != nullptr);

  // Load 'expr_eval' from 'this_arg' FilterContext object.
  llvm::Value* expr_eval_ptr =
      builder.CreateStructGEP(nullptr, this_arg, 0, "expr_eval_ptr");
  llvm::Value* expr_eval_arg = builder.CreateLoad(expr_eval_ptr, "expr_eval_arg");

  // Evaluate the row against the filter's expression.
  llvm::Value* compute_fn_args[] = {expr_eval_arg, row_arg};
  CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(
      codegen, &builder, filter_expr->type(), compute_fn, compute_fn_args, "result");

  CodegenAnyValReadWriteInfo rwi = result.ToReadWriteInfo();
  rwi.entry_block().BranchTo(&builder);

  llvm::BasicBlock* insert_filter_block =
      llvm::BasicBlock::Create(context, "insert_filter", insert_filter_fn);

  // Set the pointer to NULL in case it evaluates to NULL.
  builder.SetInsertPoint(rwi.null_block());
  llvm::Value* null_ptr = codegen->null_ptr_value();
  builder.CreateBr(insert_filter_block);

  // Saves 'result' on the stack and passes a pointer to it to Insert().
  builder.SetInsertPoint(rwi.non_null_block());
  llvm::Value* native_ptr = SlotDescriptor::CodegenStoreNonNullAnyValToNewAlloca(rwi);
  native_ptr = builder.CreatePointerCast(native_ptr, codegen->ptr_type(), "native_ptr");
  builder.CreateBr(insert_filter_block);

  // Get the arguments in place to call Insert().
  builder.SetInsertPoint(insert_filter_block);
  llvm::PHINode* val_ptr_phi = rwi.CodegenNullPhiNode(native_ptr, null_ptr,
      "val_ptr_phi");

  // Insert into the bloom filter.
  if (filter_desc.type == TRuntimeFilterType::BLOOM) {
    // Create a global constant of the filter expression's ColumnType. It needs to be a
    // constant for constant propagation and dead code elimination in 'get_hash_value_fn'.
    llvm::Type* col_type = codegen->GetStructType<ColumnType>();
    llvm::Constant* expr_type_arg = codegen->ConstantToGVPtr(
        col_type, filter_expr->type().ToIR(codegen), "expr_type_arg");

    // Call RawValue::GetHashValue() on the result of the filter's expression.
    llvm::Value* seed_arg =
        codegen->GetI32Constant(RuntimeFilterBank::DefaultHashSeed());
    llvm::Value* get_hash_value_args[] = {val_ptr_phi, expr_type_arg, seed_arg};
    llvm::Function* get_hash_value_fn =
        codegen->GetFunction(IRFunction::RAW_VALUE_GET_HASH_VALUE_FAST_HASH32, false);
    DCHECK(get_hash_value_fn != nullptr);
    llvm::Value* hash_value =
        builder.CreateCall(get_hash_value_fn, get_hash_value_args, "hash_value");

    // Call Insert() on the bloom filter.
    llvm::Function* insert_bloom_filter_fn =
        codegen->GetFunction(IRFunction::BLOOM_FILTER_INSERT, false);

    DCHECK(insert_bloom_filter_fn != nullptr);

    llvm::Value* insert_args[] = {local_filter_arg, hash_value};
    builder.CreateCall(insert_bloom_filter_fn, insert_args);
  } else if (filter_desc.type == TRuntimeFilterType::MIN_MAX) {
    // The function for inserting into the min-max filter.
    llvm::Function* min_max_insert_fn = codegen->GetFunction(
        MinMaxFilter::GetInsertIRFunctionType(filter_expr->type()), false);
    DCHECK(min_max_insert_fn != nullptr);

    llvm::Value* insert_filter_args[] = {local_filter_arg, val_ptr_phi};
    builder.CreateCall(min_max_insert_fn, insert_filter_args);
  } else {
    DCHECK(filter_desc.type == TRuntimeFilterType::IN_LIST);
    DCHECK(insert_in_list_filter_fn != nullptr);
    llvm::Value* insert_filter_args[] = {local_filter_arg, val_ptr_phi};
    builder.CreateCall(insert_in_list_filter_fn, insert_filter_args);
  }

  builder.CreateRetVoid();

  *fn = codegen->FinalizeFunction(insert_filter_fn);
  if (*fn == nullptr) {
    return Status("Codegen'ed FilterContext::Insert() fails verification, see log");
  }
  return Status::OK();
}

bool FilterContext::CheckForAlwaysFalse(const std::string& stats_name,
    const std::vector<FilterContext>& ctxs) {
  for (const FilterContext& ctx : ctxs) {
    if (ctx.filter->AlwaysFalse()) {
      ctx.stats->IncrCounters(stats_name, 1, 1, 1);
      return true;
    }
  }
  return false;
}

// Return true if both the filter and column min/max stats exist and the overlap of filter
// range [min,max] with column stats range [min, max] is more than 'threshold'. Return
// false otherwise.
bool FilterContext::ShouldRejectFilterBasedOnColumnStats(
    const TRuntimeFilterTargetDesc& desc, MinMaxFilter* minmax_filter, float threshold) {
  if (!desc.is_min_max_value_present) return false;
  DCHECK(minmax_filter) << "Expect a valid minmax_filter";
  const TColumnValue& column_low_value = desc.low_value;
  const TColumnValue& column_high_value = desc.high_value;
  ColumnType col_type = ColumnType::FromThrift(desc.target_expr.nodes[0].type);
  float ratio =
      minmax_filter->ComputeOverlapRatio(col_type, column_low_value, column_high_value);
  return ratio >= threshold;
}
