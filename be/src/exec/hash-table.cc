// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "codegen/llvm-codegen.h"
#include "exec/hash-table.inline.h"
#include "exprs/expr.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"

using namespace impala;
using namespace llvm;
using namespace std;

const char* HashTable::LLVM_CLASS_NAME = "class.impala::HashTable";

const float HashTable::MAX_BUCKET_OCCUPANCY_FRACTION = 0.75f;

HashTable::HashTable(RuntimeState* state, const vector<Expr*>& build_exprs,
    const vector<Expr*>& probe_exprs, int num_build_tuples, bool stores_nulls,
    bool finds_nulls, int32_t initial_seed, MemTracker* mem_tracker, int64_t num_buckets)
  : state_(state),
    build_exprs_(build_exprs),
    probe_exprs_(probe_exprs),
    num_build_tuples_(num_build_tuples),
    stores_nulls_(stores_nulls),
    finds_nulls_(finds_nulls),
    initial_seed_(initial_seed),
    node_byte_size_(sizeof(Node) + sizeof(Tuple*) * num_build_tuples_),
    num_filled_buckets_(0),
    nodes_(NULL),
    num_nodes_(0),
    mem_tracker_(mem_tracker),
    mem_limit_exceeded_(false) {
  DCHECK(mem_tracker != NULL);
  DCHECK_EQ(build_exprs_.size(), probe_exprs_.size());
  DCHECK_EQ((num_buckets & (num_buckets-1)), 0) << "num_buckets must be a power of 2";
  buckets_.resize(num_buckets);
  num_buckets_ = num_buckets;
  num_buckets_till_resize_ = MAX_BUCKET_OCCUPANCY_FRACTION * num_buckets_;

  // Compute the layout and buffer size to store the evaluated expr results
  results_buffer_size_ = Expr::ComputeResultsLayout(build_exprs_,
      &expr_values_buffer_offsets_, &var_result_begin_);
  expr_values_buffer_= new uint8_t[results_buffer_size_];
  memset(expr_values_buffer_, 0, sizeof(uint8_t) * results_buffer_size_);
  expr_value_null_bits_ = new uint8_t[build_exprs_.size()];

  nodes_capacity_ = 1024;
  nodes_ = reinterpret_cast<uint8_t*>(malloc(nodes_capacity_ * node_byte_size_));
  if (ImpaladMetrics::HASH_TABLE_TOTAL_BYTES != NULL) {
    ImpaladMetrics::HASH_TABLE_TOTAL_BYTES->Increment(nodes_capacity_ * node_byte_size_);
  }

  // update mem_limits_
  int64_t allocated_bytes =
      num_buckets * sizeof(Bucket) + nodes_capacity_ * node_byte_size_;
  mem_tracker_->Consume(allocated_bytes);
}

void HashTable::Close() {
  // TODO: use tr1::array?
  delete[] expr_values_buffer_;
  delete[] expr_value_null_bits_;
  free(nodes_);
  if (ImpaladMetrics::HASH_TABLE_TOTAL_BYTES != NULL) {
    ImpaladMetrics::HASH_TABLE_TOTAL_BYTES->Increment(-nodes_capacity_ * node_byte_size_);
  }
  int64_t bytes_freed = nodes_capacity_ * node_byte_size_ +
      buckets_.size() * sizeof(Bucket);
  mem_tracker_->Release(bytes_freed);
}

bool HashTable::EvalRow(TupleRow* row, const vector<Expr*>& exprs) {
  // Put a non-zero constant in the result location for NULL.
  // We don't want(NULL, 1) to hash to the same as (0, 1).
  // This needs to be as big as the biggest primitive type since the bytes
  // get copied directly.
  int64_t null_value[] = { HashUtil::FNV_SEED, HashUtil::FNV_SEED };

  bool has_null = false;
  for (int i = 0; i < exprs.size(); ++i) {
    void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
    void* val = exprs[i]->GetValue(row);
    if (val == NULL) {
      // If the table doesn't store nulls, no reason to keep evaluating
      if (!stores_nulls_) return true;

      expr_value_null_bits_[i] = true;
      val = &null_value;
      has_null = true;
    } else {
      expr_value_null_bits_[i] = false;
    }
    RawValue::Write(val, loc, build_exprs_[i]->type(), NULL);
  }
  return has_null;
}

// Helper function to store a value into the results buffer if the expr
// evaluated to NULL.  We don't want (NULL, 1) to hash to the same as (0,1) so
// we'll pick a more random value.
static void CodegenAssignNullValue(LlvmCodeGen* codegen,
    LlvmCodeGen::LlvmBuilder* builder, Value* dst, const ColumnType& type) {
  int64_t fvn_seed = HashUtil::FNV_SEED;

  if (type.type == TYPE_STRING) {
    Value* dst_ptr = builder->CreateStructGEP(dst, 0, "string_ptr");
    Value* dst_len = builder->CreateStructGEP(dst, 1, "string_len");
    Value* null_len = codegen->GetIntConstant(TYPE_INT, fvn_seed);
    Value* null_ptr = builder->CreateIntToPtr(null_len, codegen->ptr_type());
    builder->CreateStore(null_ptr, dst_ptr);
    builder->CreateStore(null_len, dst_len);
    return;
  } else {
    Value* null_value = NULL;
    // Get a type specific representation of fvn_seed
    switch (type.type) {
      case TYPE_BOOLEAN:
        // In results, booleans are stored as 1 byte
        dst = builder->CreateBitCast(dst, codegen->ptr_type());
        null_value = codegen->GetIntConstant(TYPE_TINYINT, fvn_seed);
        break;
      case TYPE_NULL:
        dst = builder->CreateBitCast(dst, codegen->ptr_type());
        null_value = codegen->GetIntConstant(type, fvn_seed);
        break;
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_BIGINT:
        null_value = codegen->GetIntConstant(type, fvn_seed);
        break;
      case TYPE_FLOAT: {
        // Don't care about the value, just the bit pattern
        float fvn_seed_float = *reinterpret_cast<float*>(&fvn_seed);
        null_value = ConstantFP::get(codegen->context(), APFloat(fvn_seed_float));
        break;
      }
      case TYPE_DOUBLE: {
        // Don't care about the value, just the bit pattern
        double fvn_seed_double = *reinterpret_cast<double*>(&fvn_seed);
        null_value = ConstantFP::get(codegen->context(), APFloat(fvn_seed_double));
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
// define i1 @EvaBuildRow(%"class.impala::HashTable"* %this_ptr,
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
Function* HashTable::CodegenEvalTupleRow(LlvmCodeGen* codegen, bool build) {
  if (!Expr::IsCodegenAvailable(build_exprs_) ||
      !Expr::IsCodegenAvailable(probe_exprs_)) {
    VLOG_QUERY << "Could not codegen EvalTupleRow because one of the exprs "
               << "could not be codegen'd.";
    return NULL;
  }

  // Get types to generate function prototype
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  DCHECK(tuple_row_type != NULL);
  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);

  Type* this_type = codegen->GetType(HashTable::LLVM_CLASS_NAME);
  DCHECK(this_type != NULL);
  PointerType* this_ptr_type = PointerType::get(this_type, 0);

  LlvmCodeGen::FnPrototype prototype(codegen, build ? "EvaBuildRow" : "EvalProbeRow",
      codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Value* args[2];
  Function* fn = prototype.GeneratePrototype(&builder, args);

  Value* has_null = codegen->false_value();

  // Aggregation with no grouping exprs also use the hash table interface for
  // code simplicity.  In that case, there are no build exprs.
  if (!build_exprs_.empty()) {
    Type* tuple_row_llvm_type = PointerType::get(codegen->ptr_type(), 0);
    Value* row = builder.CreateBitCast(args[1], tuple_row_llvm_type);

    LlvmCodeGen::NamedVariable null_var("null_ptr", codegen->boolean_type());
    Value* is_null_ptr = codegen->CreateEntryBlockAlloca(fn, null_var);

    const vector<Expr*>& exprs = build ? build_exprs_ : probe_exprs_;
    for (int i = 0; i < exprs.size(); ++i) {
      // TODO: refactor this to somewhere else?  This is not hash table specific
      // except for the null handling bit and would be used for anyone that needs
      // to materialize a vector of exprs
      // Convert result buffer to llvm ptr type
      void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
      Value* llvm_loc = codegen->CastPtrToLlvmPtr(
          codegen->GetPtrType(exprs[i]->type()), loc);

      BasicBlock* null_block = BasicBlock::Create(context, "null", fn);
      BasicBlock* not_null_block = BasicBlock::Create(context, "not_null", fn);
      BasicBlock* continue_block = BasicBlock::Create(context, "continue", fn);

      // Call expr
      Value* expr_args[] = { row, codegen->null_ptr_value(), is_null_ptr };
      Value* result = builder.CreateCall(exprs[i]->codegen_fn(), expr_args, "eval");
      Value* is_null = builder.CreateLoad(is_null_ptr);

      // Set null-byte result
      Value* null_byte = builder.CreateZExt(is_null, codegen->GetType(TYPE_TINYINT));
      uint8_t* null_byte_loc = &expr_value_null_bits_[i];
      Value* llvm_null_byte_loc =
          codegen->CastPtrToLlvmPtr(codegen->ptr_type(), null_byte_loc);
      builder.CreateStore(null_byte, llvm_null_byte_loc);

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
      codegen->CodegenAssign(&builder, llvm_loc, result, exprs[i]->type());
      builder.CreateBr(continue_block);

      builder.SetInsertPoint(continue_block);
    }
  }
  builder.CreateRet(has_null);

  return codegen->FinalizeFunction(fn);
}


uint32_t HashTable::HashVariableLenRow() {
  uint32_t hash = initial_seed_;
  // Hash the non-var length portions (if there are any)
  if (var_result_begin_ != 0) {
    hash = HashUtil::Hash(expr_values_buffer_, var_result_begin_, hash);
  }

  for (int i = 0; i < build_exprs_.size(); ++i) {
    // non-string and null slots are already part of expr_values_buffer
    if (build_exprs_[i]->type().type != TYPE_STRING) continue;

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

// Codegen for hashing the current row.  In the case with both string and non-string
// data (join on int_col, string_col), the IR looks like:
// define i32 @HashCurrentRow(%"class.impala::HashTable"* %this_ptr) {
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
Function* HashTable::CodegenHashCurrentRow(LlvmCodeGen* codegen) {
  if (!Expr::IsCodegenAvailable(build_exprs_) ||
      !Expr::IsCodegenAvailable(probe_exprs_)) {
    VLOG_QUERY << "Could not codegen HashCurrentRow because one of the exprs "
               << "could not be codegen'd.";
    return NULL;
  }

  // Get types to generate function prototype
  Type* this_type = codegen->GetType(HashTable::LLVM_CLASS_NAME);
  DCHECK(this_type != NULL);
  PointerType* this_ptr_type = PointerType::get(this_type, 0);

  LlvmCodeGen::FnPrototype prototype(codegen, "HashCurrentRow",
      codegen->GetType(TYPE_INT));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Value* this_arg;
  Function* fn = prototype.GeneratePrototype(&builder, &this_arg);

  Value* hash_result = codegen->GetIntConstant(TYPE_INT, initial_seed_);
  Value* data = codegen->CastPtrToLlvmPtr(codegen->ptr_type(), expr_values_buffer_);
  if (var_result_begin_ == -1) {
    // No variable length slots, just hash what is in 'expr_values_buffer_'
    if (results_buffer_size_ > 0) {
      Function* hash_fn = codegen->GetHashFunction(results_buffer_size_);
      Value* len = codegen->GetIntConstant(TYPE_INT, results_buffer_size_);
      hash_result = builder.CreateCall3(hash_fn, data, len, hash_result);
    }
  } else {
    if (var_result_begin_ > 0) {
      Function* hash_fn = codegen->GetHashFunction(var_result_begin_);
      Value* len = codegen->GetIntConstant(TYPE_INT, var_result_begin_);
      hash_result = builder.CreateCall3(hash_fn, data, len, hash_result);
    }

    // Hash string slots
    for (int i = 0; i < build_exprs_.size(); ++i) {
      if (build_exprs_[i]->type().type != TYPE_STRING) continue;

      BasicBlock* null_block = NULL;
      BasicBlock* not_null_block = NULL;
      BasicBlock* continue_block = NULL;
      Value* str_null_result = NULL;

      void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];

      // If the hash table stores nulls, we need to check if the stringval
      // evaluated to NULL
      if (stores_nulls_) {
        null_block = BasicBlock::Create(context, "null", fn);
        not_null_block = BasicBlock::Create(context, "not_null", fn);
        continue_block = BasicBlock::Create(context, "continue", fn);

        uint8_t* null_byte_loc = &expr_value_null_bits_[i];
        Value* llvm_null_byte_loc =
            codegen->CastPtrToLlvmPtr(codegen->ptr_type(), null_byte_loc);
        Value* null_byte = builder.CreateLoad(llvm_null_byte_loc);
        Value* is_null = builder.CreateICmpNE(null_byte,
            codegen->GetIntConstant(TYPE_TINYINT, 0));
        builder.CreateCondBr(is_null, null_block, not_null_block);

        // For null, we just want to call the hash function on the portion of
        // the data
        builder.SetInsertPoint(null_block);
        Function* null_hash_fn = codegen->GetHashFunction(sizeof(StringValue));
        Value* llvm_loc = codegen->CastPtrToLlvmPtr(codegen->ptr_type(), loc);
        Value* len = codegen->GetIntConstant(TYPE_INT, sizeof(StringValue));
        str_null_result = builder.CreateCall3(null_hash_fn, llvm_loc, len, hash_result);
        builder.CreateBr(continue_block);

        builder.SetInsertPoint(not_null_block);
      }

      // Convert expr_values_buffer_ loc to llvm value
      Value* str_val = codegen->CastPtrToLlvmPtr(codegen->GetPtrType(TYPE_STRING), loc);

      Value* ptr = builder.CreateStructGEP(str_val, 0, "ptr");
      Value* len = builder.CreateStructGEP(str_val, 1, "len");
      ptr = builder.CreateLoad(ptr);
      len = builder.CreateLoad(len);

      // Call hash(ptr, len, hash_result);
      Function* general_hash_fn = codegen->GetHashFunction();
      Value* string_hash_result =
          builder.CreateCall3(general_hash_fn, ptr, len, hash_result);

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

bool HashTable::Equals(TupleRow* build_row) {
  for (int i = 0; i < build_exprs_.size(); ++i) {
    void* val = build_exprs_[i]->GetValue(build_row);
    if (val == NULL) {
      if (!stores_nulls_) return false;
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

// Codegen for HashTable::Equals.  For a hash table with two exprs (string,int), the
// IR looks like:
//  define i1 @Equals(%"class.impala::HashTable"* %this_ptr,
//                    %"class.impala::TupleRow"* %row) {
//  entry:
//    %null_ptr = alloca i1
//    %0 = bitcast %"class.impala::TupleRow"* %row to i8**
//    %1 = call %"struct.impala::StringValue"* @SlotRef(i8** %0, i8* null, i1* %null_ptr)
//    %2 = load i1* %null_ptr
//    br i1 %2, label %null, label %not_null
//
//  false_block:                          ; preds = %not_null2, %null1, %not_null, %null
//    ret i1 false
//
//  null:                                             ; preds = %entry
//    br i1 false, label %continue, label %false_block
//
//  not_null:                                         ; preds = %entry
//    %tmp_eq = call i1 @StringValueEQ(%"struct.impala::StringValue"* %1,
//                                     %"struct.impala::StringValue"* inttoptr
//                                     (i64 65844560 to %"struct.impala::StringValue"*))
//    br i1 %tmp_eq, label %continue, label %false_block
//
//  continue:                                         ; preds = %not_null, %null
//    %3 = call i32 @SlotRef1(i8** %0, i8* null, i1* %null_ptr)
//    %4 = load i1* %null_ptr
//    %5 = load i32* inttoptr (i64 65844544 to i32*)
//    br i1 %4, label %null1, label %not_null2
//
//  null1:                                            ; preds = %continue
//    br i1 false, label %continue3, label %false_block
//
//  not_null2:                                        ; preds = %continue
//    %tmp_eq4 = icmp eq i32 %3, %5
//    br i1 %tmp_eq4, label %continue3, label %false_block
//
//  continue3:                                        ; preds = %not_null2, %null1
//    ret i1 true
//  }
Function* HashTable::CodegenEquals(LlvmCodeGen* codegen) {
  if (!Expr::IsCodegenAvailable(build_exprs_) ||
      !Expr::IsCodegenAvailable(probe_exprs_)) {
    VLOG_QUERY << "Could not codegen Equals because one of the exprs "
                << "could not be codegen'd.";
    return NULL;
  }

  // Get types to generate function prototype
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  DCHECK(tuple_row_type != NULL);
  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);

  Type* this_type = codegen->GetType(HashTable::LLVM_CLASS_NAME);
  DCHECK(this_type != NULL);
  PointerType* this_ptr_type = PointerType::get(this_type, 0);

  LlvmCodeGen::FnPrototype prototype(codegen, "Equals", codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Value* args[2];
  Function* fn = prototype.GeneratePrototype(&builder, args);

  if (!build_exprs_.empty()) {
    Type* tuple_row_llvm_type = PointerType::get(codegen->ptr_type(), 0);
    Value* row = builder.CreateBitCast(args[1], tuple_row_llvm_type);

    LlvmCodeGen::NamedVariable null_var("null_ptr", codegen->boolean_type());
    Value* is_null_ptr = codegen->CreateEntryBlockAlloca(fn, null_var);

    BasicBlock* false_block = BasicBlock::Create(context, "false_block", fn);

    for (int i = 0; i < build_exprs_.size(); ++i) {
      BasicBlock* null_block = BasicBlock::Create(context, "null", fn);
      BasicBlock* not_null_block = BasicBlock::Create(context, "not_null", fn);
      BasicBlock* continue_block = BasicBlock::Create(context, "continue", fn);

      // call GetValue on build_exprs[i]
      Value* expr_args[] = { row, codegen->null_ptr_value(), is_null_ptr };
      Value* val = builder.CreateCall(build_exprs_[i]->codegen_fn(), expr_args);
      Value* is_null = builder.CreateLoad(is_null_ptr);

      // Determine if probe is null (i.e. expr_value_null_bits_[i] == true). In
      // the case where the hash table does not store nulls, this is always false.
      Value* probe_is_null = codegen->false_value();
      uint8_t* null_byte_loc = &expr_value_null_bits_[i];
      if (stores_nulls_) {
        Value* llvm_null_byte_loc =
            codegen->CastPtrToLlvmPtr(codegen->ptr_type(), null_byte_loc);
        Value* null_byte = builder.CreateLoad(llvm_null_byte_loc);
        probe_is_null = builder.CreateICmpNE(null_byte,
            codegen->GetIntConstant(TYPE_TINYINT, 0));
      }

      // Get llvm value for probe_val from 'expr_values_buffer_'
      void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
      Value* probe_val = codegen->CastPtrToLlvmPtr(
          codegen->GetPtrType(build_exprs_[i]->type()), loc);
      if (build_exprs_[i]->type().type != TYPE_STRING) {
        probe_val = builder.CreateLoad(probe_val);
      }

      // Branch for GetValue() returning NULL
      builder.CreateCondBr(is_null, null_block, not_null_block);

      // Null block
      builder.SetInsertPoint(null_block);
      builder.CreateCondBr(probe_is_null, continue_block, false_block);

      // Not-null block
      builder.SetInsertPoint(not_null_block);
      if (stores_nulls_) {
        BasicBlock* cmp_block = BasicBlock::Create(context, "cmp", fn);
        // First need to compare that probe expr[i] is not null
        builder.CreateCondBr(probe_is_null, false_block, cmp_block);
        builder.SetInsertPoint(cmp_block);
      }
      // Check val == probe_val
      Value* is_equal = codegen->CodegenEquals(&builder, val, probe_val,
          build_exprs_[i]->type());
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

void HashTable::ResizeBuckets(int64_t num_buckets) {
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
    int node_idx = bucket->node_idx_;

    while (node_idx != -1) {
      Node* node = GetNode(node_idx);
      int64_t next_idx = node->next_idx_;
      uint32_t hash = node->hash_;

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
        MoveNode(bucket, move_to, node_idx, node, last_node);
      } else {
        last_node = node;
      }

      node_idx = next_idx;
    }
  }

  num_buckets_ = num_buckets;
  num_buckets_till_resize_ = MAX_BUCKET_OCCUPANCY_FRACTION * num_buckets_;
}

void HashTable::GrowNodeArray() {
  int64_t old_size = nodes_capacity_ * node_byte_size_;
  int64_t new_capacity = nodes_capacity_ + nodes_capacity_ / 2;
  int64_t new_size = new_capacity * node_byte_size_;

  // This can be a rather large allocation so check the limit before (to prevent
  // us from going over the limits too much).
  if (!mem_tracker_->TryConsume(new_size - old_size)) {
    MemLimitExceeded(new_size - old_size);
    return;
  }
  nodes_capacity_ = new_capacity;
  nodes_ = reinterpret_cast<uint8_t*>(realloc(nodes_, new_size));
  if (ImpaladMetrics::HASH_TABLE_TOTAL_BYTES != NULL) {
    ImpaladMetrics::HASH_TABLE_TOTAL_BYTES->Increment(new_size - old_size);
  }
}

void HashTable::MemLimitExceeded(int64_t allocation_size) {
  DCHECK(!mem_limit_exceeded_);
  mem_limit_exceeded_ = true;
  if (state_ != NULL) state_->SetMemLimitExceeded(mem_tracker_, allocation_size);
}

string HashTable::DebugString(bool skip_empty, const RowDescriptor* desc) {
  stringstream ss;
  ss << endl;
  for (int i = 0; i < buckets_.size(); ++i) {
    int64_t node_idx = buckets_[i].node_idx_;
    bool first = true;
    if (skip_empty && node_idx == -1) continue;
    ss << i << ": ";
    while (node_idx != -1) {
      Node* node = GetNode(node_idx);
      if (!first) {
        ss << ",";
      }
      if (desc == NULL) {
        ss << node_idx << "(" << (void*)node->data() << ")";
      } else {
        ss << (void*)node->data() << " " << PrintRow(node->data(), *desc);
      }
      node_idx = node->next_idx_;
      first = false;
    }
    ss << endl;
  }
  return ss.str();
}
