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
#include "runtime/runtime-filter.h"
#include "runtime/tuple-row.h"
#include "util/runtime-profile-counters.h"

using namespace impala;
using namespace strings;
using namespace llvm;

const std::string FilterStats::ROW_GROUPS_KEY = "RowGroups";
const std::string FilterStats::FILES_KEY = "Files";
const std::string FilterStats::SPLITS_KEY = "Splits";
const std::string FilterStats::ROWS_KEY = "Rows";

const char* FilterContext::LLVM_CLASS_NAME = "struct.impala::FilterContext";

FilterStats::FilterStats(RuntimeProfile* runtime_profile, bool is_partition_filter) {
  DCHECK(runtime_profile != NULL);
  profile = runtime_profile;
  if (is_partition_filter) {
    RegisterCounterGroup(FilterStats::SPLITS_KEY);
    RegisterCounterGroup(FilterStats::FILES_KEY);
  }

  // TODO: These only apply to Parquet, so only register them in that case.
  RegisterCounterGroup(FilterStats::ROWS_KEY);
  if (is_partition_filter) RegisterCounterGroup(FilterStats::ROW_GROUPS_KEY);
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
    RuntimeState* state, MemPool* mem_pool) {
  filter = from.filter;
  stats = from.stats;
  return from.expr_eval->Clone(pool, state, mem_pool, &expr_eval);
}

bool FilterContext::Eval(TupleRow* row) const noexcept {
  void* val = expr_eval->GetValue(row);
  return filter->Eval(val, expr_eval->root().type());
}

void FilterContext::Insert(TupleRow* row) const noexcept {
  if (local_bloom_filter == NULL) return;
  void* val = expr_eval->GetValue(row);
  uint32_t filter_hash = RawValue::GetHashValue(
      val, expr_eval->root().type(), RuntimeFilterBank::DefaultHashSeed());
  local_bloom_filter->Insert(filter_hash);
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
Status FilterContext::CodegenEval(LlvmCodeGen* codegen, ScalarExpr* filter_expr,
    Function** fn) {
  LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);

  *fn = nullptr;
  PointerType* this_type = codegen->GetPtrType(FilterContext::LLVM_CLASS_NAME);
  PointerType* tuple_row_ptr_type = codegen->GetPtrType(TupleRow::LLVM_CLASS_NAME);
  LlvmCodeGen::FnPrototype prototype(codegen, "FilterContextEval",
      codegen->boolean_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this", this_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  Value* args[2];
  Function* eval_filter_fn = prototype.GeneratePrototype(&builder, args);
  Value* this_arg = args[0];
  Value* row_arg = args[1];

  BasicBlock* not_null_block = BasicBlock::Create(context, "not_null", eval_filter_fn);
  BasicBlock* is_null_block = BasicBlock::Create(context, "is_null", eval_filter_fn);
  BasicBlock* eval_filter_block =
      BasicBlock::Create(context, "eval_filter", eval_filter_fn);

  Function* compute_fn;
  RETURN_IF_ERROR(filter_expr->GetCodegendComputeFn(codegen, &compute_fn));
  DCHECK(compute_fn != nullptr);

  // The function for checking against the bloom filter for match.
  Function* runtime_filter_fn =
      codegen->GetFunction(IRFunction::RUNTIME_FILTER_EVAL, false);
  DCHECK(runtime_filter_fn != nullptr);

  // Load 'expr_eval' from 'this_arg' FilterContext object.
  Value* expr_eval_ptr =
      builder.CreateStructGEP(nullptr, this_arg, 0, "expr_eval_ptr");
  Value* expr_eval_arg =
      builder.CreateLoad(expr_eval_ptr, "expr_eval_arg");

  // Evaluate the row against the filter's expression.
  Value* compute_fn_args[] = {expr_eval_arg, row_arg};
  CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
      filter_expr->type(), compute_fn, compute_fn_args, "result");

  // Check if the result is NULL
  Value* is_null = result.GetIsNull();
  builder.CreateCondBr(is_null, is_null_block, not_null_block);

  // Set the pointer to NULL in case it evaluates to NULL.
  builder.SetInsertPoint(is_null_block);
  Value* null_ptr = codegen->null_ptr_value();
  builder.CreateBr(eval_filter_block);

  // Saves 'result' on the stack and passes a pointer to it to 'runtime_filter_fn'.
  builder.SetInsertPoint(not_null_block);
  Value* native_ptr = result.ToNativePtr();
  native_ptr = builder.CreatePointerCast(native_ptr, codegen->ptr_type(), "native_ptr");
  builder.CreateBr(eval_filter_block);

  // Get the arguments in place to call 'runtime_filter_fn' to see if the row passes.
  builder.SetInsertPoint(eval_filter_block);
  PHINode* val_ptr_phi = builder.CreatePHI(codegen->ptr_type(), 2, "val_ptr_phi");
  val_ptr_phi->addIncoming(native_ptr, not_null_block);
  val_ptr_phi->addIncoming(null_ptr, is_null_block);

  // Create a global constant of the filter expression's ColumnType. It needs to be a
  // constant for constant propagation and dead code elimination in 'runtime_filter_fn'.
  Type* col_type = codegen->GetType(ColumnType::LLVM_CLASS_NAME);
  Constant* expr_type_arg = codegen->ConstantToGVPtr(col_type,
      filter_expr->type().ToIR(codegen), "expr_type_arg");

  // Load 'filter' from 'this_arg' FilterContext object.
  Value* filter_ptr = builder.CreateStructGEP(nullptr, this_arg, 1, "filter_ptr");
  Value* filter_arg = builder.CreateLoad(filter_ptr, "filter_arg");

  Value* run_filter_args[] = {filter_arg, val_ptr_phi, expr_type_arg};
  Value* passed_filter =
       builder.CreateCall(runtime_filter_fn, run_filter_args, "passed_filter");
  builder.CreateRet(passed_filter);

  *fn = codegen->FinalizeFunction(eval_filter_fn);
  if (*fn == NULL) {
    return Status("Codegen'ed FilterContext::Eval() fails verification, see log");
  }
  return Status::OK();
}

