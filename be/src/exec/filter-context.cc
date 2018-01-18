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
#include "runtime/runtime-filter.inline.h"
#include "runtime/tuple-row.h"
#include "util/min-max-filter.h"
#include "util/runtime-profile-counters.h"

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
    uint32_t filter_hash = RawValue::GetHashValue(
        val, expr_eval->root().type(), RuntimeFilterBank::DefaultHashSeed());
    local_bloom_filter->Insert(filter_hash);
  } else {
    DCHECK(filter->is_min_max_filter());
    if (local_min_max_filter == nullptr) return;
    void* val = expr_eval->GetValue(row);
    local_min_max_filter->Insert(val);
  }
}

void FilterContext::MaterializeValues() const {
  if (filter->is_min_max_filter() && local_min_max_filter != nullptr) {
    local_min_max_filter->MaterializeValues();
  }
}

// An example of the generated code for TPCH-Q2: RF002 -> n_regionkey
//
// @expr_type_arg = constant %"struct.impala::ColumnType" { i32 4, i32 -1, i32 -1,
//     i32 -1, %"class.std::vector.422" zeroinitializer,
//     %"class.std::vector.101" zeroinitializer }
//
// ; Function Attrs: alwaysinline
// define i1 @FilterContextEval(%"struct.impala::FilterContext"* %this,
//                              %"class.impala::TupleRow"* %row) #34 {
// entry:
//   %0 = alloca i16
//   %expr_eval_ptr = getelementptr inbounds %"struct.impala::FilterContext",
//       %"struct.impala::FilterContext"* %this, i32 0, i32 0
//   %expr_eval_arg = load %"class.impala::ExprContext"*,
//       %"class.impala::ExprContext"** %expr_eval_ptr
//   %result = call i32 @GetSlotRef(%"class.impala::ExprContext"* %expr_eval_arg,
//       %"class.impala::TupleRow"* %row)
//   %is_null1 = trunc i32 %result to i1
//   br i1 %is_null1, label %is_null, label %not_null
//
// not_null:                                         ; preds = %entry
//   %1 = ashr i32 %result, 16
//   %2 = trunc i32 %1 to i16
//   store i16 %2, i16* %0
//   %native_ptr = bitcast i16* %0 to i8*
//   br label %eval_filter
//
// is_null:                                          ; preds = %entry
//   br label %eval_filter
//
// eval_filter:                                      ; preds = %not_null, %is_null
//   %val_ptr_phi = phi i8* [ %native_ptr, %not_null ], [ null, %is_null ]
//   %filter_ptr = getelementptr inbounds %"struct.impala::FilterContext",
//       %"struct.impala::FilterContext"* %this, i32 0, i32 1
//   %filter_arg = load %"class.impala::RuntimeFilter"*,
//       %"class.impala::RuntimeFilter"** %filter_ptr
//   %passed_filter = call i1 @_ZNK6impala13RuntimeFilter4EvalEPvRKNS_10ColumnTypeE.3(
//       %"class.impala::RuntimeFilter"* %filter_arg, i8* %val_ptr_phi,
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

  llvm::BasicBlock* not_null_block =
      llvm::BasicBlock::Create(context, "not_null", eval_filter_fn);
  llvm::BasicBlock* is_null_block =
      llvm::BasicBlock::Create(context, "is_null", eval_filter_fn);
  llvm::BasicBlock* eval_filter_block =
      llvm::BasicBlock::Create(context, "eval_filter", eval_filter_fn);

  llvm::Function* compute_fn;
  RETURN_IF_ERROR(filter_expr->GetCodegendComputeFn(codegen, &compute_fn));
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

  // Check if the result is NULL
  llvm::Value* is_null = result.GetIsNull();
  builder.CreateCondBr(is_null, is_null_block, not_null_block);

  // Set the pointer to NULL in case it evaluates to NULL.
  builder.SetInsertPoint(is_null_block);
  llvm::Value* null_ptr = codegen->null_ptr_value();
  builder.CreateBr(eval_filter_block);

  // Saves 'result' on the stack and passes a pointer to it to 'runtime_filter_fn'.
  builder.SetInsertPoint(not_null_block);
  llvm::Value* native_ptr = result.ToNativePtr();
  native_ptr = builder.CreatePointerCast(native_ptr, codegen->ptr_type(), "native_ptr");
  builder.CreateBr(eval_filter_block);

  // Get the arguments in place to call 'runtime_filter_fn' to see if the row passes.
  builder.SetInsertPoint(eval_filter_block);
  llvm::PHINode* val_ptr_phi = builder.CreatePHI(codegen->ptr_type(), 2, "val_ptr_phi");
  val_ptr_phi->addIncoming(native_ptr, not_null_block);
  val_ptr_phi->addIncoming(null_ptr, is_null_block);

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
  if (*fn == NULL) {
    return Status("Codegen'ed FilterContext::Eval() fails verification, see log");
  }
  return Status::OK();
}

// An example of the generated code for TPCH-Q2: RF002 -> n_regionkey
//
// @expr_type_arg = constant %"struct.impala::ColumnType" { i32 4, i32 -1, i32 -1,
//     i32 -1, %"class.std::vector.422" zeroinitializer,
//     %"class.std::vector.101" zeroinitializer }
//
// define void @FilterContextInsert(%"struct.impala::FilterContext"* %this,
//     %"class.impala::TupleRow"* %row) #37 {
// entry:
//   %0 = alloca i16
//   %local_bloom_filter_ptr = getelementptr inbounds %"struct.impala::FilterContext",
//       %"struct.impala::FilterContext"* %this, i32 0, i32 3
//   %local_bloom_filter_arg = load %"class.impala::BloomFilter"*,
//       %"class.impala::BloomFilter"** %local_bloom_filter_ptr
//   %filter_is_null = icmp eq %"class.impala::BloomFilter"* %local_bloom_filter_arg, null
//   br i1 %filter_is_null, label %filters_null, label %filters_not_null
//
// filters_not_null:                                 ; preds = %entry
//   %expr_eval_ptr = getelementptr inbounds %"struct.impala::FilterContext",
//       %"struct.impala::FilterContext"* %this, i32 0, i32 0
//   %expr_eval_arg = load %"class.impala::ScalarExprEvaluator"*,
//       %"class.impala::ScalarExprEvaluator"** %expr_eval_ptr
//   %result = call i32 @GetSlotRef.46(
//       %"class.impala::ScalarExprEvaluator"* %expr_eval_arg,
//       %"class.impala::TupleRow"* %row)
//   %is_null = trunc i32 %result to i1
//   br i1 %is_null, label %val_is_null, label %val_not_null
//
// filters_null:                                     ; preds = %entry
//   ret void
//
// val_not_null:                                     ; preds = %filters_not_null
//   %1 = ashr i32 %result, 16
//   %2 = trunc i32 %1 to i16
//   store i16 %2, i16* %0
//   %native_ptr = bitcast i16* %0 to i8*
//   br label %insert_filter
//
// val_is_null:                                      ; preds = %filters_not_null
//   br label %insert_filter
//
// insert_filter:                                    ; preds = %val_not_null, %val_is_null
//   %val_ptr_phi = phi i8* [ %native_ptr, %val_not_null ], [ null, %val_is_null ]
//   %hash_value = call i32 @_ZN6impala8RawValue12GetHashValueEPKvRKNS_10ColumnTypeEj(
//       i8* %val_ptr_phi, %"struct.impala::ColumnType"* @expr_type_arg, i32 1234)
//   call void @_ZN6impala11BloomFilter10InsertAvx2Ej(
//       %"class.impala::BloomFilter"* %local_bloom_filter_arg, i32 %hash_value)
//   ret void
// }
Status FilterContext::CodegenInsert(LlvmCodeGen* codegen, ScalarExpr* filter_expr,
    FilterContext* ctx, llvm::Function** fn) {
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
  if (ctx->filter->is_bloom_filter()) {
    // Load 'local_bloom_filter' from 'this_arg' FilterContext object.
    llvm::Value* local_bloom_filter_ptr =
        builder.CreateStructGEP(nullptr, this_arg, 3, "local_bloom_filter_ptr");
    local_filter_arg =
        builder.CreateLoad(local_bloom_filter_ptr, "local_bloom_filter_arg");
  } else {
    DCHECK(ctx->filter->is_min_max_filter());
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
  }

  // Check if 'local_bloom_filter' or 'local_min_max_filter' are NULL (depending on
  // filter desc) and return if so.
  llvm::Value* filter_null = builder.CreateIsNull(local_filter_arg, "filter_is_null");
  llvm::BasicBlock* filter_not_null_block =
      llvm::BasicBlock::Create(context, "filters_not_null", insert_filter_fn);
  llvm::BasicBlock* filter_null_block =
      llvm::BasicBlock::Create(context, "filters_null", insert_filter_fn);
  builder.CreateCondBr(filter_null, filter_null_block, filter_not_null_block);
  builder.SetInsertPoint(filter_null_block);
  builder.CreateRetVoid();
  builder.SetInsertPoint(filter_not_null_block);

  llvm::BasicBlock* val_not_null_block =
      llvm::BasicBlock::Create(context, "val_not_null", insert_filter_fn);
  llvm::BasicBlock* val_is_null_block =
      llvm::BasicBlock::Create(context, "val_is_null", insert_filter_fn);
  llvm::BasicBlock* insert_filter_block =
      llvm::BasicBlock::Create(context, "insert_filter", insert_filter_fn);

  llvm::Function* compute_fn;
  RETURN_IF_ERROR(filter_expr->GetCodegendComputeFn(codegen, &compute_fn));
  DCHECK(compute_fn != nullptr);

  // Load 'expr_eval' from 'this_arg' FilterContext object.
  llvm::Value* expr_eval_ptr =
      builder.CreateStructGEP(nullptr, this_arg, 0, "expr_eval_ptr");
  llvm::Value* expr_eval_arg = builder.CreateLoad(expr_eval_ptr, "expr_eval_arg");

  // Evaluate the row against the filter's expression.
  llvm::Value* compute_fn_args[] = {expr_eval_arg, row_arg};
  CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(
      codegen, &builder, filter_expr->type(), compute_fn, compute_fn_args, "result");

  // Check if the result is NULL
  llvm::Value* val_is_null = result.GetIsNull();
  builder.CreateCondBr(val_is_null, val_is_null_block, val_not_null_block);

  // Set the pointer to NULL in case it evaluates to NULL.
  builder.SetInsertPoint(val_is_null_block);
  llvm::Value* null_ptr = codegen->null_ptr_value();
  builder.CreateBr(insert_filter_block);

  // Saves 'result' on the stack and passes a pointer to it to Insert().
  builder.SetInsertPoint(val_not_null_block);
  llvm::Value* native_ptr = result.ToNativePtr();
  native_ptr = builder.CreatePointerCast(native_ptr, codegen->ptr_type(), "native_ptr");
  builder.CreateBr(insert_filter_block);

  // Get the arguments in place to call Insert().
  builder.SetInsertPoint(insert_filter_block);
  llvm::PHINode* val_ptr_phi = builder.CreatePHI(codegen->ptr_type(), 2, "val_ptr_phi");
  val_ptr_phi->addIncoming(native_ptr, val_not_null_block);
  val_ptr_phi->addIncoming(null_ptr, val_is_null_block);

  // Insert into the bloom filter.
  if (ctx->filter->is_bloom_filter()) {
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
        codegen->GetFunction(IRFunction::RAW_VALUE_GET_HASH_VALUE, false);
    DCHECK(get_hash_value_fn != nullptr);
    llvm::Value* hash_value =
        builder.CreateCall(get_hash_value_fn, get_hash_value_args, "hash_value");

    // Call Insert() on the bloom filter.
    llvm::Function* insert_bloom_filter_fn;
    if (CpuInfo::IsSupported(CpuInfo::AVX2)) {
      insert_bloom_filter_fn =
          codegen->GetFunction(IRFunction::BLOOM_FILTER_INSERT_AVX2, false);
    } else {
      insert_bloom_filter_fn =
          codegen->GetFunction(IRFunction::BLOOM_FILTER_INSERT_NO_AVX2, false);
    }
    DCHECK(insert_bloom_filter_fn != nullptr);

    llvm::Value* insert_args[] = {local_filter_arg, hash_value};
    builder.CreateCall(insert_bloom_filter_fn, insert_args);
  } else {
    DCHECK(ctx->filter->is_min_max_filter());
    // The function for inserting into the min-max filter.
    llvm::Function* min_max_insert_fn = codegen->GetFunction(
        MinMaxFilter::GetInsertIRFunctionType(filter_expr->type().type), false);
    DCHECK(min_max_insert_fn != nullptr);

    llvm::Value* insert_filter_args[] = {local_filter_arg, val_ptr_phi};
    builder.CreateCall(min_max_insert_fn, insert_filter_args);
  }

  builder.CreateRetVoid();

  *fn = codegen->FinalizeFunction(insert_filter_fn);
  if (*fn == NULL) {
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
