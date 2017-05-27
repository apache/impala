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

#include "exec/old-hash-table.inline.h"

#include <functional>
#include <numeric>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/slot-ref.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.inline.h"
#include "runtime/runtime-filter.h"
#include "runtime/runtime-filter-bank.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple-row.h"
#include "util/bloom-filter.h"
#include "runtime/tuple.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/impalad-metrics.h"

#include "common/names.h"

using namespace impala;
using namespace llvm;

const char* OldHashTable::LLVM_CLASS_NAME = "class.impala::OldHashTable";

const float OldHashTable::MAX_BUCKET_OCCUPANCY_FRACTION = 0.75f;
static const int HT_PAGE_SIZE = 8 * 1024 * 1024;

// Put a non-zero constant in the result location for NULL.
// We don't want(NULL, 1) to hash to the same as (0, 1).
// This needs to be as big as the biggest primitive type since the bytes
// get copied directly.
// TODO find a better approach, since primitives like CHAR(N) can be up to 128 bytes
static int64_t NULL_VALUE[] = { HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED };

OldHashTable::OldHashTable(RuntimeState* state,
    const vector<ScalarExpr*>& build_exprs, const vector<ScalarExpr*>& probe_exprs,
    const vector<ScalarExpr*>& filter_exprs, int num_build_tuples, bool stores_nulls,
    const vector<bool>& finds_nulls, int32_t initial_seed, MemTracker* mem_tracker,
    const vector<RuntimeFilter*>& runtime_filters, bool stores_tuples,
    int64_t num_buckets)
  : state_(state),
    build_exprs_(build_exprs),
    probe_exprs_(probe_exprs),
    filter_exprs_(filter_exprs),
    filters_(runtime_filters),
    num_build_tuples_(num_build_tuples),
    stores_nulls_(stores_nulls),
    finds_nulls_(finds_nulls),
    finds_some_nulls_(std::accumulate(
        finds_nulls_.begin(), finds_nulls_.end(), false, std::logical_or<bool>())),
    stores_tuples_(stores_tuples),
    initial_seed_(initial_seed),
    num_filled_buckets_(0),
    num_nodes_(0),
    mem_pool_(new MemPool(mem_tracker)),
    num_data_pages_(0),
    next_node_(NULL),
    node_remaining_current_page_(0),
    mem_tracker_(mem_tracker),
    mem_limit_exceeded_(false) {
  DCHECK(mem_tracker != NULL);
  DCHECK_EQ(build_exprs_.size(), probe_exprs_.size());
  DCHECK_EQ(build_exprs_.size(), finds_nulls_.size());
  DCHECK_EQ((num_buckets & (num_buckets-1)), 0) << "num_buckets must be a power of 2";
  buckets_.resize(num_buckets);
  num_buckets_ = num_buckets;
  num_buckets_till_resize_ = MAX_BUCKET_OCCUPANCY_FRACTION * num_buckets_;
  mem_tracker_->Consume(buckets_.capacity() * sizeof(Bucket));

  // Compute the layout and buffer size to store the evaluated expr results
  results_buffer_size_ = ScalarExpr::ComputeResultsLayout(build_exprs_,
      &expr_values_buffer_offsets_, &var_result_begin_);
  expr_values_buffer_= new uint8_t[results_buffer_size_];
  memset(expr_values_buffer_, 0, sizeof(uint8_t) * results_buffer_size_);
  expr_value_null_bits_ = new uint8_t[build_exprs_.size()];

  GrowNodeArray();
}

Status OldHashTable::Init(ObjectPool* pool, RuntimeState* state) {
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(build_exprs_, state, pool,
      mem_pool_.get(), &build_expr_evals_));
  DCHECK_EQ(build_exprs_.size(), build_expr_evals_.size());
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(probe_exprs_, state, pool,
      mem_pool_.get(), &probe_expr_evals_));
  DCHECK_EQ(probe_exprs_.size(), probe_expr_evals_.size());
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(filter_exprs_, state, pool,
      mem_pool_.get(), &filter_expr_evals_));
  DCHECK_EQ(filter_exprs_.size(), filter_expr_evals_.size());
  return Status::OK();
}

Status OldHashTable::Create(ObjectPool* pool, RuntimeState* state,
    const vector<ScalarExpr*>& build_exprs, const vector<ScalarExpr*>& probe_exprs,
    const vector<ScalarExpr*>& filter_exprs, int num_build_tuples, bool stores_nulls,
    const vector<bool>& finds_nulls, int32_t initial_seed, MemTracker* mem_tracker,
    const vector<RuntimeFilter*>& runtime_filters, scoped_ptr<OldHashTable>* hash_tbl,
    bool stores_tuples, int64_t num_buckets) {
  hash_tbl->reset(new OldHashTable(state, build_exprs, probe_exprs, filter_exprs,
      num_build_tuples, stores_nulls, finds_nulls, initial_seed, mem_tracker,
      runtime_filters, stores_tuples, num_buckets));
  return (*hash_tbl)->Init(pool, state);
}

Status OldHashTable::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(build_expr_evals_, state));
  DCHECK_EQ(build_exprs_.size(), build_expr_evals_.size());
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(probe_expr_evals_, state));
  DCHECK_EQ(probe_exprs_.size(), probe_expr_evals_.size());
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(filter_expr_evals_, state));
  DCHECK_EQ(filter_exprs_.size(), filter_expr_evals_.size());
  return Status::OK();
}

void OldHashTable::Close(RuntimeState* state) {
  // TODO: use tr1::array?
  delete[] expr_values_buffer_;
  delete[] expr_value_null_bits_;
  expr_values_buffer_ = NULL;
  expr_value_null_bits_ = NULL;
  ScalarExprEvaluator::Close(build_expr_evals_, state);
  ScalarExprEvaluator::Close(probe_expr_evals_, state);
  ScalarExprEvaluator::Close(filter_expr_evals_, state);
  mem_pool_->FreeAll();
  if (ImpaladMetrics::HASH_TABLE_TOTAL_BYTES != NULL) {
    ImpaladMetrics::HASH_TABLE_TOTAL_BYTES->Increment(-num_data_pages_ * HT_PAGE_SIZE);
  }
  mem_tracker_->Release(buckets_.capacity() * sizeof(Bucket));
  buckets_.clear();
}

void OldHashTable::FreeLocalAllocations() {
  ScalarExprEvaluator::FreeLocalAllocations(build_expr_evals_);
  ScalarExprEvaluator::FreeLocalAllocations(probe_expr_evals_);
  ScalarExprEvaluator::FreeLocalAllocations(filter_expr_evals_);
}

bool OldHashTable::EvalRow(
    TupleRow* row, const vector<ScalarExprEvaluator*>& evals) {
  bool has_null = false;
  for (int i = 0; i < evals.size(); ++i) {
    void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
    void* val = evals[i]->GetValue(row);
    if (val == NULL) {
      // If the table doesn't store nulls, no reason to keep evaluating
      if (!stores_nulls_) return true;

      expr_value_null_bits_[i] = true;
      val = &NULL_VALUE;
      has_null = true;
    } else {
      expr_value_null_bits_[i] = false;
    }
    RawValue::Write(val, loc, build_exprs_[i]->type(), NULL);
  }
  return has_null;
}

int OldHashTable::AddBloomFilters() {
  int num_enabled_filters = 0;
  vector<BloomFilter*> bloom_filters;
  bloom_filters.resize(filters_.size());
  for (int i = 0; i < filters_.size(); ++i) {
    if (state_->filter_bank()->FpRateTooHigh(filters_[i]->filter_size(), size())) {
      bloom_filters[i] = BloomFilter::ALWAYS_TRUE_FILTER;
    } else {
      bloom_filters[i] =
          state_->filter_bank()->AllocateScratchBloomFilter(filters_[i]->id());
      ++num_enabled_filters;
    }
  }

  OldHashTable::Iterator iter = Begin();
  while (iter != End()) {
    TupleRow* row = iter.GetRow();
    for (int i = 0; i < filters_.size(); ++i) {
      if (bloom_filters[i] == NULL) continue;
      void* e = filter_expr_evals_[i]->GetValue(row);
      uint32_t h = RawValue::GetHashValue(e, filter_exprs_[i]->type(),
          RuntimeFilterBank::DefaultHashSeed());
      bloom_filters[i]->Insert(h);
    }
    iter.Next<false>();
  }

  // Update all the local filters in the filter bank.
  for (int i = 0; i < filters_.size(); ++i) {
    state_->filter_bank()->UpdateFilterFromLocal(filters_[i]->id(), bloom_filters[i]);
  }

  return num_enabled_filters;
}

// Helper function to store a value into the results buffer if the expr
// evaluated to NULL.  We don't want (NULL, 1) to hash to the same as (0,1) so
// we'll pick a more random value.
static void CodegenAssignNullValue(
    LlvmCodeGen* codegen, LlvmBuilder* builder, Value* dst, const ColumnType& type) {
  uint64_t fnv_seed = HashUtil::FNV_SEED;

  if (type.type == TYPE_STRING || type.type == TYPE_VARCHAR) {
    Value* dst_ptr = builder->CreateStructGEP(NULL, dst, 0, "string_ptr");
    Value* dst_len = builder->CreateStructGEP(NULL, dst, 1, "string_len");
    Value* null_len = codegen->GetIntConstant(TYPE_INT, fnv_seed);
    Value* null_ptr = builder->CreateIntToPtr(null_len, codegen->ptr_type());
    builder->CreateStore(null_ptr, dst_ptr);
    builder->CreateStore(null_len, dst_len);
    return;
  } else {
    Value* null_value = NULL;
    int byte_size = type.GetByteSize();
    // Get a type specific representation of fnv_seed
    switch (type.type) {
      case TYPE_BOOLEAN:
        // In results, booleans are stored as 1 byte
        dst = builder->CreateBitCast(dst, codegen->ptr_type());
        null_value = codegen->GetIntConstant(TYPE_TINYINT, fnv_seed);
        break;
      case TYPE_TIMESTAMP: {
        // Cast 'dst' to 'i128*'
        DCHECK_EQ(byte_size, 16);
        PointerType* fnv_seed_ptr_type =
            codegen->GetPtrType(Type::getIntNTy(codegen->context(), byte_size * 8));
        dst = builder->CreateBitCast(dst, fnv_seed_ptr_type);
        null_value = codegen->GetIntConstant(byte_size, fnv_seed, fnv_seed);
        break;
      }
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_BIGINT:
      case TYPE_DECIMAL:
        null_value = codegen->GetIntConstant(byte_size, fnv_seed, fnv_seed);
        break;
      case TYPE_FLOAT: {
        // Don't care about the value, just the bit pattern
        float fnv_seed_float = *reinterpret_cast<float*>(&fnv_seed);
        null_value = ConstantFP::get(codegen->context(), APFloat(fnv_seed_float));
        break;
      }
      case TYPE_DOUBLE: {
        // Don't care about the value, just the bit pattern
        double fnv_seed_double = *reinterpret_cast<double*>(&fnv_seed);
        null_value = ConstantFP::get(codegen->context(), APFloat(fnv_seed_double));
        break;
      }
      default:
        DCHECK(false);
    }
    builder->CreateStore(null_value, dst);
  }
}

// Codegen for evaluating a tuple row over either build_exprs_ or probe_exprs_.
// For the case where we are joining on a single int, the IR looks like
// define i1 @EvaBuildRow(%"class.impala::OldHashTable"* %this_ptr,
//                        %"class.impala::TupleRow"* %row) {
// entry:
//   %null_ptr = alloca i1
//   %0 = bitcast %"class.impala::TupleRow"* %row to i8**
//   %eval = call i32 @SlotRef(i8** %0, i8* null, i1* %null_ptr)
//   %1 = load i1* %null_ptr
//   br i1 %1, label %null, label %not_null
//
// null:                                             ; preds = %entry
//   ret i1 true
//
// not_null:                                         ; preds = %entry
//   store i32 %eval, i32* inttoptr (i64 46146336 to i32*)
//   br label %continue
//
// continue:                                         ; preds = %not_null
//   %2 = zext i1 %1 to i8
//   store i8 %2, i8* inttoptr (i64 46146248 to i8*)
//   ret i1 false
// }
// For each expr, we create 3 code blocks.  The null, not null and continue blocks.
// Both the null and not null branch into the continue block.  The continue block
// becomes the start of the next block for codegen (either the next expr or just the
// end of the function).
Function* OldHashTable::CodegenEvalTupleRow(LlvmCodeGen* codegen, bool build) {
  DCHECK_EQ(build_exprs_.size(), probe_exprs_.size());
  const vector<ScalarExpr*>& exprs = build ? build_exprs_ : probe_exprs_;
  for (int i = 0; i < exprs.size(); ++i) {
    PrimitiveType type = exprs[i]->type().type;
    if (type == TYPE_CHAR) return NULL;
  }

  // Get types to generate function prototype
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  DCHECK(tuple_row_type != NULL);
  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);

  Type* this_type = codegen->GetType(OldHashTable::LLVM_CLASS_NAME);
  DCHECK(this_type != NULL);
  PointerType* this_ptr_type = PointerType::get(this_type, 0);

  LlvmCodeGen::FnPrototype prototype(codegen, build ? "EvalBuildRow" : "EvalProbeRow",
      codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);
  Value* args[2];
  Function* fn = prototype.GeneratePrototype(&builder, args);
  Value* this_ptr = args[0];
  Value* row = args[1];
  Value* has_null = codegen->false_value();

  IRFunction::Type fn_name = build ?
      IRFunction::OLD_HASH_TABLE_GET_BUILD_EXPR_EVALUATORS :
      IRFunction::OLD_HASH_TABLE_GET_PROBE_EXPR_EVALUATORS;
  Function* get_expr_eval_fn = codegen->GetFunction(fn_name, false);
  DCHECK(get_expr_eval_fn != NULL);

  // Aggregation with no grouping exprs also use the hash table interface for
  // code simplicity. In that case, there are no build exprs.
  if (!exprs.empty()) {
    // Load build_expr_evals_.data() / probe_expr_evals_.data()
    Value* eval_vector = codegen->CodegenCallFunction(&builder, build ?
        IRFunction::OLD_HASH_TABLE_GET_BUILD_EXPR_EVALUATORS :
        IRFunction::OLD_HASH_TABLE_GET_PROBE_EXPR_EVALUATORS,
        this_ptr, "eval_vector");

    // Load expr_values_buffer_
    Value* expr_values_buffer = codegen->CodegenCallFunction(&builder,
        IRFunction::OLD_HASH_TABLE_GET_EXPR_VALUES_BUFFER,
        this_ptr, "expr_values_buffer");

    // Load expr_values_null_bits_
    Value* expr_value_null_bits = codegen->CodegenCallFunction(&builder,
        IRFunction::OLD_HASH_TABLE_GET_EXPR_VALUE_NULL_BITS,
        this_ptr, "expr_value_null_bits");

    for (int i = 0; i < exprs.size(); ++i) {
      BasicBlock* null_block = BasicBlock::Create(context, "null", fn);
      BasicBlock* not_null_block = BasicBlock::Create(context, "not_null", fn);
      BasicBlock* continue_block = BasicBlock::Create(context, "continue", fn);

      // loc_addr = expr_values_buffer_ + expr_values_buffer_offsets_[i]
      Value* llvm_loc = builder.CreateInBoundsGEP(NULL, expr_values_buffer,
          codegen->GetIntConstant(TYPE_INT, expr_values_buffer_offsets_[i]), "loc_addr");
      llvm_loc = builder.CreatePointerCast(llvm_loc,
          codegen->GetPtrType(exprs[i]->type()), "loc");

      // Codegen GetValue() for exprs[i]
      Function* expr_fn;
      Status status = exprs[i]->GetCodegendComputeFn(codegen, &expr_fn);
      if (!status.ok()) {
        fn->eraseFromParent(); // deletes function
        VLOG_QUERY << "Failed to codegen EvalTupleRow(): " << status.GetDetail();
        return NULL;
      }

      // Load evals[i] and call GetValue()
      Value* eval_arg =
          codegen->CodegenArrayAt(&builder, eval_vector, i, "eval");
      DCHECK(eval_arg->getType()->isPointerTy());
      CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
          exprs[i]->type(), expr_fn, {eval_arg, row}, "result");
      Value* is_null = result.GetIsNull();

      // Set null-byte result
      Value* null_bits = builder.CreateZExt(is_null, codegen->GetType(TYPE_TINYINT));
      Value* llvm_null_bits_loc = builder.CreateInBoundsGEP(NULL, expr_value_null_bits,
          codegen->GetIntConstant(TYPE_INT, i), "null_bits_loc");
      builder.CreateStore(null_bits, llvm_null_bits_loc);
      builder.CreateCondBr(is_null, null_block, not_null_block);

      // Null block
      builder.SetInsertPoint(null_block);
      if (!stores_nulls_) {
        // hash table doesn't store nulls, no reason to keep evaluating exprs
        builder.CreateRet(codegen->true_value());
      } else {
        CodegenAssignNullValue(codegen, &builder, llvm_loc, exprs[i]->type());
        has_null = codegen->true_value();
        builder.CreateBr(continue_block);
      }

      // Not null block
      builder.SetInsertPoint(not_null_block);
      result.ToNativePtr(llvm_loc);
      builder.CreateBr(continue_block);

      builder.SetInsertPoint(continue_block);
    }
  }
  builder.CreateRet(has_null);
  return codegen->FinalizeFunction(fn);
}

uint32_t OldHashTable::HashVariableLenRow() {
  uint32_t hash = initial_seed_;
  // Hash the non-var length portions (if there are any)
  if (var_result_begin_ != 0) {
    hash = HashUtil::Hash(expr_values_buffer_, var_result_begin_, hash);
  }

  for (int i = 0; i < build_exprs_.size(); ++i) {
    // non-string and null slots are already part of expr_values_buffer
    if (build_exprs_[i]->type().type != TYPE_STRING &&
        build_exprs_[i]->type().type != TYPE_VARCHAR) {
      continue;
    }

    void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
    if (expr_value_null_bits_[i]) {
      // Hash the null random seed values at 'loc'
      hash = HashUtil::Hash(loc, sizeof(StringValue), hash);
    } else {
      // Hash the string
      StringValue* str = reinterpret_cast<StringValue*>(loc);
      hash = HashUtil::Hash(str->ptr, str->len, hash);
    }
  }
  return hash;
}

// Codegen for hashing the current row.  In the case with both string and non-string data
// (group by int_col, string_col), the IR looks like:
// define i32 @HashCurrentRow(%"class.impala::OldHashTable"* %this_ptr) {
// entry:
//   %0 = call i32 @IrCrcHash(i8* inttoptr (i64 51107808 to i8*), i32 16, i32 0)
//   %1 = load i8* inttoptr (i64 29500112 to i8*)
//   %2 = icmp ne i8 %1, 0
//   br i1 %2, label %null, label %not_null
//
// null:                                             ; preds = %entry
//   %3 = call i32 @IrCrcHash(i8* inttoptr (i64 51107824 to i8*), i32 16, i32 %0)
//   br label %continue
//
// not_null:                                         ; preds = %entry
//   %4 = load i8** getelementptr inbounds (
//        %"struct.impala::StringValue"* inttoptr
//          (i64 51107824 to %"struct.impala::StringValue"*), i32 0, i32 0)
//   %5 = load i32* getelementptr inbounds (
//        %"struct.impala::StringValue"* inttoptr
//          (i64 51107824 to %"struct.impala::StringValue"*), i32 0, i32 1)
//   %6 = call i32 @IrCrcHash(i8* %4, i32 %5, i32 %0)
//   br label %continue
//
// continue:                                         ; preds = %not_null, %null
//   %7 = phi i32 [ %6, %not_null ], [ %3, %null ]
//   ret i32 %7
// }
// TODO: can this be cross-compiled?
Function* OldHashTable::CodegenHashCurrentRow(LlvmCodeGen* codegen) {
  for (int i = 0; i < build_exprs_.size(); ++i) {
    // Disable codegen for CHAR
    if (build_exprs_[i]->type().type == TYPE_CHAR) return NULL;
  }

  // Get types to generate function prototype
  Type* this_type = codegen->GetType(OldHashTable::LLVM_CLASS_NAME);
  DCHECK(this_type != NULL);
  PointerType* this_ptr_type = PointerType::get(this_type, 0);

  LlvmCodeGen::FnPrototype prototype(codegen, "HashCurrentRow",
      codegen->GetType(TYPE_INT));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));

  LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);
  Value* this_ptr;
  Function* fn = prototype.GeneratePrototype(&builder, &this_ptr);

  // Load expr_values_buffer_
  Value* expr_values_buffer = codegen->CodegenCallFunction(&builder,
      IRFunction::OLD_HASH_TABLE_GET_EXPR_VALUES_BUFFER, this_ptr, "expr_values_buffer");

  Value* hash_result = codegen->GetIntConstant(TYPE_INT, initial_seed_);
  if (var_result_begin_ == -1) {
    // No variable length slots, just hash what is in 'expr_values_buffer_'
    if (results_buffer_size_ > 0) {
      Function* hash_fn = codegen->GetHashFunction(results_buffer_size_);
      Value* len = codegen->GetIntConstant(TYPE_INT, results_buffer_size_);
      hash_result = builder.CreateCall(hash_fn, {expr_values_buffer, len, hash_result});
    }
  } else {
    if (var_result_begin_ > 0) {
      Function* hash_fn = codegen->GetHashFunction(var_result_begin_);
      Value* len = codegen->GetIntConstant(TYPE_INT, var_result_begin_);
      hash_result = builder.CreateCall(hash_fn, {expr_values_buffer, len, hash_result});
    }

    // Load expr_value_null_bits_
    Value* expr_value_null_bits = codegen->CodegenCallFunction(&builder,
        IRFunction::OLD_HASH_TABLE_GET_EXPR_VALUE_NULL_BITS,
        this_ptr, "expr_value_null_bits");

    // Hash string slots
    for (int i = 0; i < build_exprs_.size(); ++i) {
      if (build_exprs_[i]->type().type != TYPE_STRING
          && build_exprs_[i]->type().type != TYPE_VARCHAR) continue;

      BasicBlock* null_block = NULL;
      BasicBlock* not_null_block = NULL;
      BasicBlock* continue_block = NULL;
      Value* str_null_result = NULL;

      Value* llvm_buffer_loc = builder.CreateInBoundsGEP(NULL, expr_values_buffer,
          codegen->GetIntConstant(TYPE_INT, expr_values_buffer_offsets_[i]), "buffer_loc");

      // If the hash table stores nulls, we need to check if the stringval
      // evaluated to NULL
      if (stores_nulls_) {
        null_block = BasicBlock::Create(context, "null", fn);
        not_null_block = BasicBlock::Create(context, "not_null", fn);
        continue_block = BasicBlock::Create(context, "continue", fn);

        // Load expr_values_null_bits_[i] and check if it's set.
        Value* llvm_null_bits_loc = builder.CreateInBoundsGEP(NULL, expr_value_null_bits,
            codegen->GetIntConstant(TYPE_INT, i), "null_bits_loc");
        Value* null_bits = builder.CreateLoad(llvm_null_bits_loc);
        Value* is_null = builder.CreateICmpNE(null_bits,
            codegen->GetIntConstant(TYPE_TINYINT, 0));
        builder.CreateCondBr(is_null, null_block, not_null_block);

        // For null, we just want to call the hash function on a portion of the data.
        builder.SetInsertPoint(null_block);
        Function* null_hash_fn = codegen->GetHashFunction(sizeof(StringValue));
        Value* len = codegen->GetIntConstant(TYPE_INT, sizeof(StringValue));
        str_null_result = builder.CreateCall(null_hash_fn,
            ArrayRef<Value*>({llvm_buffer_loc, len, hash_result}));
        builder.CreateBr(continue_block);

        builder.SetInsertPoint(not_null_block);
      }

      // Convert expr_values_buffer_ loc to llvm value
      Value* str_val = builder.CreatePointerCast(llvm_buffer_loc,
          codegen->GetPtrType(TYPE_STRING), "str_val");

      Value* ptr = builder.CreateStructGEP(NULL, str_val, 0, "ptr");
      Value* len = builder.CreateStructGEP(NULL, str_val, 1, "len");
      ptr = builder.CreateLoad(ptr);
      len = builder.CreateLoad(len);

      // Call hash(ptr, len, hash_result);
      Function* general_hash_fn = codegen->GetHashFunction();
      Value* string_hash_result =
          builder.CreateCall(general_hash_fn, ArrayRef<Value*>({ptr, len, hash_result}));

      if (stores_nulls_) {
        builder.CreateBr(continue_block);
        builder.SetInsertPoint(continue_block);
        // Use phi node to reconcile that we could have come from the string-null
        // path and string not null paths.
        PHINode* phi_node = builder.CreatePHI(codegen->GetType(TYPE_INT), 2);
        phi_node->addIncoming(string_hash_result, not_null_block);
        phi_node->addIncoming(str_null_result, null_block);
        hash_result = phi_node;
      } else {
        hash_result = string_hash_result;
      }
    }
  }

  builder.CreateRet(hash_result);
  return codegen->FinalizeFunction(fn);
}

bool OldHashTable::Equals(TupleRow* build_row) {
  for (int i = 0; i < build_exprs_.size(); ++i) {
    void* val = build_expr_evals_[i]->GetValue(build_row);
    if (val == NULL) {
      if (!(stores_nulls_ && finds_nulls_[i])) return false;
      if (!expr_value_null_bits_[i]) return false;
      continue;
    } else {
      if (expr_value_null_bits_[i]) return false;
    }

    void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
    if (!RawValue::Eq(loc, val, build_exprs_[i]->type())) {
      return false;
    }
  }
  return true;
}

// Codegen for OldHashTable::Equals.  For a hash table with two exprs (string,int), the
// IR looks like:
//
// define i1 @Equals(%"class.impala::OldHashTable"* %this_ptr,
//                   %"class.impala::TupleRow"* %row) {
// entry:
//   %result = call i64 @GetSlotRef(%"class.impala::ScalarExpr"* inttoptr
//                                  (i64 146381856 to %"class.impala::ScalarExpr"*),
//                                  %"class.impala::TupleRow"* %row)
//   %0 = trunc i64 %result to i1
//   br i1 %0, label %null, label %not_null
//
// false_block:                            ; preds = %not_null2, %null1, %not_null, %null
//   ret i1 false
//
// null:                                             ; preds = %entry
//   br i1 false, label %continue, label %false_block
//
// not_null:                                         ; preds = %entry
//   %1 = load i32* inttoptr (i64 104774368 to i32*)
//   %2 = ashr i64 %result, 32
//   %3 = trunc i64 %2 to i32
//   %cmp_raw = icmp eq i32 %3, %1
//   br i1 %cmp_raw, label %continue, label %false_block
//
// continue:                                         ; preds = %not_null, %null
//   %result4 = call { i64, i8* } @GetSlotRef1(
//       %"class.impala::ScalarExpr"* inttoptr
//       (i64 146381696 to %"class.impala::ScalarExpr"*),
//       %"class.impala::TupleRow"* %row)
//   %4 = extractvalue { i64, i8* } %result4, 0
//   %5 = trunc i64 %4 to i1
//   br i1 %5, label %null1, label %not_null2
//
// null1:                                            ; preds = %continue
//   br i1 false, label %continue3, label %false_block
//
// not_null2:                                        ; preds = %continue
//   %6 = extractvalue { i64, i8* } %result4, 0
//   %7 = ashr i64 %6, 32
//   %8 = trunc i64 %7 to i32
//   %result5 = extractvalue { i64, i8* } %result4, 1
//   %cmp_raw6 = call i1 @_Z11StringValEQPciPKN6impala11StringValueE(
//       i8* %result5, i32 %8, %"struct.impala::StringValue"* inttoptr
//       (i64 104774384 to %"struct.impala::StringValue"*))
//   br i1 %cmp_raw6, label %continue3, label %false_block
//
// continue3:                                        ; preds = %not_null2, %null1
//   ret i1 true
// }
Function* OldHashTable::CodegenEquals(LlvmCodeGen* codegen) {
  for (int i = 0; i < build_exprs_.size(); ++i) {
    // Disable codegen for CHAR
    if (build_exprs_[i]->type().type == TYPE_CHAR) return NULL;
  }

  // Get types to generate function prototype
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  DCHECK(tuple_row_type != NULL);
  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);

  Type* this_type = codegen->GetType(OldHashTable::LLVM_CLASS_NAME);
  DCHECK(this_type != NULL);
  PointerType* this_ptr_type = PointerType::get(this_type, 0);

  LlvmCodeGen::FnPrototype prototype(codegen, "Equals", codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);
  Value* args[2];
  Function* fn = prototype.GeneratePrototype(&builder, args);
  Value* this_ptr = args[0];
  Value* row = args[1];

  if (!build_exprs_.empty()) {
    BasicBlock* false_block = BasicBlock::Create(context, "false_block", fn);

    // Load build_expr_evals_.data()
    Value* eval_vector = codegen->CodegenCallFunction(&builder,
        IRFunction::OLD_HASH_TABLE_GET_BUILD_EXPR_EVALUATORS,
        this_ptr, "eval_vector");

    // Load expr_values_buffer_
    Value* expr_values_buffer = codegen->CodegenCallFunction(&builder,
        IRFunction::OLD_HASH_TABLE_GET_EXPR_VALUES_BUFFER,
        this_ptr, "expr_values_buffer");

    // Load expr_value_null_bits_
    Value* expr_value_null_bits = codegen->CodegenCallFunction(&builder,
        IRFunction::OLD_HASH_TABLE_GET_EXPR_VALUE_NULL_BITS,
        this_ptr, "expr_value_null_bits");

    for (int i = 0; i < build_exprs_.size(); ++i) {
      BasicBlock* null_block = BasicBlock::Create(context, "null", fn);
      BasicBlock* not_null_block = BasicBlock::Create(context, "not_null", fn);
      BasicBlock* continue_block = BasicBlock::Create(context, "continue", fn);

      // Generate GetValue() of build_expr_evals_[i]
      Function* expr_fn;
      Status status = build_exprs_[i]->GetCodegendComputeFn(codegen, &expr_fn);
      if (!status.ok()) {
        fn->eraseFromParent(); // deletes function
        VLOG_QUERY << "Failed to codegen Equals(): " << status.GetDetail();
        return NULL;
      }

      // Call GetValue() on build_expr_evals_[i]
      Value* eval_arg =
          codegen->CodegenArrayAt(&builder, eval_vector, i, "eval");
      CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
          build_exprs_[i]->type(), expr_fn, {eval_arg, row}, "result");
      Value* is_null = result.GetIsNull();

      // Determine if probe is null (i.e. expr_value_null_bits_[i] == true). In
      // the case where the hash table does not store nulls, this is always false.
      Value* probe_is_null = codegen->false_value();
      if (stores_nulls_ && finds_nulls_[i]) {
        Value* llvm_null_bits_loc = builder.CreateInBoundsGEP(NULL, expr_value_null_bits,
            codegen->GetIntConstant(TYPE_INT, i), "null_bits_loc");
        Value* null_bits = builder.CreateLoad(llvm_null_bits_loc, "null_bits");
        probe_is_null = builder.CreateICmpNE(null_bits,
            codegen->GetIntConstant(TYPE_TINYINT, 0));
      }

      // Get llvm value for probe_val from 'expr_values_buffer_'
      Value* probe_val = builder.CreateInBoundsGEP(NULL, expr_values_buffer,
          codegen->GetIntConstant(TYPE_INT, expr_values_buffer_offsets_[i]), "probe_val");
      probe_val = builder.CreatePointerCast(
          probe_val, codegen->GetPtrType(build_exprs_[i]->type()));

      // Branch for GetValue() returning NULL
      builder.CreateCondBr(is_null, null_block, not_null_block);

      // Null block
      builder.SetInsertPoint(null_block);
      builder.CreateCondBr(probe_is_null, continue_block, false_block);

      // Not-null block
      builder.SetInsertPoint(not_null_block);
      if (stores_nulls_) {
        BasicBlock* cmp_block = BasicBlock::Create(context, "cmp", fn);
        // First need to compare that probe_expr[i] is not null
        builder.CreateCondBr(probe_is_null, false_block, cmp_block);
        builder.SetInsertPoint(cmp_block);
      }
      // Check result == probe_val
      Value* is_equal = result.EqToNativePtr(probe_val);
      builder.CreateCondBr(is_equal, continue_block, false_block);

      builder.SetInsertPoint(continue_block);
    }
    builder.CreateRet(codegen->true_value());

    builder.SetInsertPoint(false_block);
    builder.CreateRet(codegen->false_value());
  } else {
    builder.CreateRet(codegen->true_value());
  }
  return codegen->FinalizeFunction(fn);
}

void OldHashTable::ResizeBuckets(int64_t num_buckets) {
  DCHECK_EQ((num_buckets & (num_buckets-1)), 0)
      << "num_buckets=" << num_buckets << " must be a power of 2";

  int64_t old_num_buckets = num_buckets_;
  // This can be a rather large allocation so check the limit before (to prevent
  // us from going over the limits too much).
  int64_t delta_size = (num_buckets - old_num_buckets) * sizeof(Bucket);
  if (!mem_tracker_->TryConsume(delta_size)) {
    MemLimitExceeded(delta_size);
    return;
  }
  buckets_.resize(num_buckets);

  // If we're doubling the number of buckets, all nodes in a particular bucket
  // either remain there, or move down to an analogous bucket in the other half.
  // In order to efficiently check which of the two buckets a node belongs in, the number
  // of buckets must be a power of 2.
  bool doubled_buckets = (num_buckets == old_num_buckets * 2);
  for (int i = 0; i < num_buckets_; ++i) {
    Bucket* bucket = &buckets_[i];
    Bucket* sister_bucket = &buckets_[i + old_num_buckets];
    Node* last_node = NULL;
    Node* node = bucket->node;

    while (node != NULL) {
      Node* next = node->next;
      uint32_t hash = node->hash;

      bool node_must_move;
      Bucket* move_to;
      if (doubled_buckets) {
        node_must_move = ((hash & old_num_buckets) != 0);
        move_to = sister_bucket;
      } else {
        int64_t bucket_idx = hash & (num_buckets - 1);
        node_must_move = (bucket_idx != i);
        move_to = &buckets_[bucket_idx];
      }

      if (node_must_move) {
        MoveNode(bucket, move_to, node, last_node);
      } else {
        last_node = node;
      }

      node = next;
    }
  }

  num_buckets_ = num_buckets;
  num_buckets_till_resize_ = MAX_BUCKET_OCCUPANCY_FRACTION * num_buckets_;
}

void OldHashTable::GrowNodeArray() {
  node_remaining_current_page_ = HT_PAGE_SIZE / sizeof(Node);
  next_node_ = reinterpret_cast<Node*>(mem_pool_->Allocate(HT_PAGE_SIZE));
  ++num_data_pages_;
  if (ImpaladMetrics::HASH_TABLE_TOTAL_BYTES != NULL) {
    ImpaladMetrics::HASH_TABLE_TOTAL_BYTES->Increment(HT_PAGE_SIZE);
  }
  if (mem_tracker_->LimitExceeded()) MemLimitExceeded(HT_PAGE_SIZE);
}

void OldHashTable::MemLimitExceeded(int64_t allocation_size) {
  mem_limit_exceeded_ = true;
  if (state_ != NULL) state_->SetMemLimitExceeded(mem_tracker_, allocation_size);
}

string OldHashTable::DebugString(bool skip_empty, bool show_match,
    const RowDescriptor* desc) {
  stringstream ss;
  ss << endl;
  for (int i = 0; i < buckets_.size(); ++i) {
    Node* node = buckets_[i].node;
    bool first = true;
    if (skip_empty && node == NULL) continue;
    ss << i << ": ";
    while (node != NULL) {
      if (!first) ss << ",";
      ss << node << "(" << node->data << ")";
      if (desc != NULL) ss << " " << PrintRow(GetRow(node), *desc);
      if (show_match) {
        if (node->matched) {
          ss << " [M]";
        } else {
          ss << " [U]";
        }
      }
      node = node->next;
      first = false;
    }
    ss << endl;
  }
  return ss.str();
}
