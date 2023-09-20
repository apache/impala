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

#include "exprs/slot-ref.h"

#include <limits>
#include <sstream>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exprs/scalar-expr-evaluator.h"
#include "gen-cpp/Exprs_types.h"
#include "llvm/IR/BasicBlock.h"
#include "runtime/collection-value.h"
#include "runtime/decimal-value.h"
#include "runtime/fragment-state.h"
#include "runtime/multi-precision.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "runtime/tuple-row.h"

#include "common/names.h"

using namespace impala_udf;

namespace impala {

const char* SlotRef::LLVM_CLASS_NAME = "class.impala::SlotRef";

SlotRef::SlotRef(const TExprNode& node)
  : ScalarExpr(node),
    slot_offset_(-1),  // invalid
    null_indicator_offset_(0, 0),
    slot_id_(node.slot_ref.slot_id) {
    // slot_/null_indicator_offset_ are set in Init()
}

SlotRef::SlotRef(const SlotDescriptor* desc)
  : ScalarExpr(desc->type(), false),
    slot_offset_(-1),
    null_indicator_offset_(0, 0),
    slot_id_(desc->id()) {
    // slot_/null_indicator_offset_ are set in Init()
}

SlotRef::SlotRef(const SlotDescriptor* desc, const ColumnType& type)
  : ScalarExpr(type, false),
    slot_offset_(-1),
    null_indicator_offset_(0, 0),
    slot_id_(desc->id()) {
    // slot_/null_indicator_offset_ are set in Init()
}

SlotRef::SlotRef(const ColumnType& type, int offset, const bool nullable /* = false */)
  : ScalarExpr(type, false),
    tuple_idx_(0),
    slot_offset_(offset),
    null_indicator_offset_(0, nullable ? offset : -1),
    slot_id_(-1) {}

SlotRef* SlotRef::TypeSafeCreate(const SlotDescriptor* desc) {
  if (desc->type().type == TYPE_NULL) {
    // ScalarExprEvaluator requires a non-null type for the expr. It returns nullptr for
    // null values of all types. So replacing TYPE_NULL to an arbitrary type is ok.
    // Here we use TYPE_BOOLEAN for consistency with other places.
    return new SlotRef(desc, ColumnType(TYPE_BOOLEAN));
  }
  return new SlotRef(desc);
}

Status SlotRef::Init(
    const RowDescriptor& row_desc, bool is_entry_point, FragmentState* state) {
  DCHECK(type_.IsStructType() || children_.size() == 0);
  RETURN_IF_ERROR(ScalarExpr::Init(row_desc, is_entry_point, state));
  if (slot_id_ != -1) {
    slot_desc_ = state->desc_tbl().GetSlotDescriptor(slot_id_);
    if (slot_desc_ == NULL) {
      // TODO: create macro MAKE_ERROR() that returns a stream
      stringstream error;
      error << "couldn't resolve slot descriptor " << slot_id_;
      LOG(INFO) << error.str();
      return Status(error.str());
    }
    if (slot_desc_->parent()->isTupleOfStructSlot()) {
      tuple_idx_ = row_desc.GetTupleIdx(slot_desc_->parent()->getMasterTuple()->id());
    } else {
      tuple_idx_ = row_desc.GetTupleIdx(slot_desc_->parent()->id());
    }
    if (tuple_idx_ == RowDescriptor::INVALID_IDX) {
      TupleDescriptor* d =
          state->desc_tbl().GetTupleDescriptor(slot_desc_->parent()->id());
      string error = Substitute("invalid tuple_idx: $0\nparent=$1\nrow=$2",
          slot_desc_->DebugString(), d->DebugString(), row_desc.DebugString());
      LOG(INFO) << error;
      return Status(error);
    }
    DCHECK(tuple_idx_ != RowDescriptor::INVALID_IDX);
    tuple_is_nullable_ = slot_desc_->parent()->isTupleOfStructSlot() ?
        false : row_desc.TupleIsNullable(tuple_idx_);
    slot_offset_ = slot_desc_->tuple_offset();
    null_indicator_offset_ = slot_desc_->null_indicator_offset();
  }
  return Status::OK();
}

int SlotRef::GetSlotIds(vector<SlotId>* slot_ids) const {
  if (slot_ids != nullptr) slot_ids->push_back(slot_id_);
  return 1;
}

string SlotRef::DebugString() const {
  stringstream out;
  out << "SlotRef(slot_id=" << slot_id_
      << " tuple_idx=" << tuple_idx_ << " slot_offset=" << slot_offset_
      << " tuple_is_nullable=" << tuple_is_nullable_
      << " null_indicator=" << null_indicator_offset_
      << ScalarExpr::DebugString() << ")";
  return out.str();
}

void SlotRef::AssignFnCtxIdx(int* next_fn_ctx_idx) {
  if (!type_.IsStructType()) {
    ScalarExpr::AssignFnCtxIdx(next_fn_ctx_idx);
    return;
  }
  fn_ctx_idx_start_ = *next_fn_ctx_idx;
  fn_ctx_idx_ = 0;
  fn_ctx_idx_end_ = 1;
  for (ScalarExpr* child : children()) child->AssignFnCtxIdx(next_fn_ctx_idx);
}

// There are four possible cases we may generate:
//   1. Tuple is non-nullable and slot is non-nullable
//   2. Tuple is non-nullable and slot is nullable
//   3. Tuple is nullable and slot is non-nullable (when the aggregate output is the
//      "nullable" side of an outer join).
//   4. Tuple is nullable and slot is nullable
//
// Resulting IR for a bigint slotref:
// (Note: some of the GEPs that look like no-ops are because certain offsets are 0
// in this slot descriptor.)
//
// define { i8, i64 } @GetSlotRef.22(
//     %"class.impala::ScalarExprEvaluator"* %eval, %"class.impala::TupleRow"* %row) #51 {
// entry:
//   %cast_row_ptr = bitcast %"class.impala::TupleRow"* %row to i8**
//   %tuple_ptr_addr = getelementptr inbounds i8*, i8** %cast_row_ptr, i32 0
//   %tuple_ptr = load i8*, i8** %tuple_ptr_addr
//   br label %check_tuple_null
//
// check_tuple_null:                         ; preds = %entry
//   %tuple_is_null = icmp eq i8* %tuple_ptr, null
//   br i1 %tuple_is_null, label %null_block, label %check_slot_null
//
// check_slot_null:                          ; preds = %check_tuple_null
//   %null_byte_ptr = getelementptr inbounds i8, i8* %tuple_ptr, i32 8
//   %null_byte = load i8, i8* %null_byte_ptr
//   %null_mask = and i8 %null_byte, 1
//   %is_null = icmp ne i8 %null_mask, 0
//   br i1 %is_null, label %null_block, label %read_slot
//
// read_slot:                                ; preds = %check_slot_null
//   %slot_addr = getelementptr inbounds i8, i8* %tuple_ptr, i32 0
//   %val_ptr = bitcast i8* %slot_addr to i64*
//   %val = load i64, i64* %val_ptr
//   br label %produce_value
//
// null_block:                               ; preds = %check_slot_null, %check_tuple_null
//   br label %produce_value
//
// produce_value:                            ; preds = %read_slot, %null_block
//   %is_null_phi = phi i8 [ 1, %null_block ], [ 0, %read_slot ]
//   %val_phi = phi i64 [ 0, %null_block ], [ %val, %read_slot ]
//   %result = insertvalue { i8, i64 } zeroinitializer, i64 %val_phi, 1
//   %result1 = insertvalue { i8, i64 } %result, i8 %is_null_phi, 0
//   ret { i8, i64 } %result1
// }
//
// Produced for the following query:
//   select t1.int_col, t2.bigint_col
//   from functional.alltypestiny t1 left outer join functional.alltypestiny t2
//   on t1.int_col = t2.bigint_col
//   order by bigint_col;
//
// TODO: We could generate a typed struct (and not a char*) for Tuple for llvm.  We know
// the types from the TupleDesc.  It will likely make this code simpler to reason about.
Status SlotRef::GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn) {
  // SlotRefs are based on the slot_id and tuple_idx.  Combine them to make a
  // query-wide unique id. We also need to combine whether the tuple is nullable. For
  // example, in an outer join the scan node could have the same tuple id and slot id
  // as the join node. When the slot is being used in the scan-node, the tuple is
  // non-nullable. Used in the join node (and above in the plan tree), it is nullable.
  // TODO: can we do something better.
  constexpr int64_t TUPLE_NULLABLE_MASK = numeric_limits<int64_t>::min();
  int64_t unique_slot_id = slot_id_ | ((int64_t)tuple_idx_) << 32;
  DCHECK_EQ(unique_slot_id & TUPLE_NULLABLE_MASK, 0);
  if (tuple_is_nullable_) unique_slot_id |= TUPLE_NULLABLE_MASK;
  llvm::Function* ir_compute_fn_ = codegen->GetRegisteredExprFn(unique_slot_id);
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK();
  }

  llvm::Value* args[2];
  *fn = CreateIrFunctionPrototype("GetSlotRef", codegen, &args);
  llvm::Value* eval_ptr = args[0];
  llvm::Value* row_ptr = args[1];

  LlvmBuilder builder(codegen->context());
  CodegenAnyVal result_value = CodegenValue(codegen, &builder, *fn, eval_ptr, row_ptr);
  builder.CreateRet(result_value.GetLoweredValue());

  *fn = codegen->FinalizeFunction(*fn);
  if (UNLIKELY(*fn == NULL)) return Status(TErrorCode::IR_VERIFY_FAILED, "SlotRef");
  codegen->RegisterExprFn(unique_slot_id, *fn);
  return Status::OK();
}

// Generates null checking code: null checking may be generated for the tuple and for the
// slot based on 'tuple_is_nullable' and 'slot_is_nullable'. If any one of the checks
// returns null, control is transferred to 'next_block_if_null', otherwise to
// 'next_block_if_not_null.
void SlotRef::CodegenNullChecking(LlvmCodeGen* codegen, LlvmBuilder* builder,
    llvm::Function* fn, llvm::BasicBlock* next_block_if_null,
    llvm::BasicBlock* next_block_if_not_null, llvm::Value* tuple_ptr) {
  bool slot_is_nullable = null_indicator_offset_.bit_mask != 0;
  llvm::BasicBlock* const check_tuple_null_block = tuple_is_nullable_ ?
    llvm::BasicBlock::Create(codegen->context(), "check_tuple_null",
        fn, /* insert before */ next_block_if_not_null)
    : nullptr;
  llvm::BasicBlock* const check_slot_null_block = slot_is_nullable ?
    llvm::BasicBlock::Create(codegen->context(), "check_slot_null",
        fn, /* insert before */ next_block_if_not_null)
    : nullptr;

  // Check if tuple* is null only if the tuple is nullable
  if (tuple_is_nullable_) {
    builder->CreateBr(check_tuple_null_block);
    builder->SetInsertPoint(check_tuple_null_block);
    llvm::Value* tuple_is_null = builder->CreateIsNull(tuple_ptr, "tuple_is_null");
    // Check slot is null only if the null indicator bit is set
    if (slot_is_nullable) {
      builder->CreateCondBr(tuple_is_null, next_block_if_null, check_slot_null_block);
    } else {
      builder->CreateCondBr(tuple_is_null, next_block_if_null, next_block_if_not_null);
    }
  } else {
    if (slot_is_nullable) {
      builder->CreateBr(check_slot_null_block);
    } else {
      builder->CreateBr(next_block_if_not_null);
    }
  }

  // Branch for tuple* != NULL.  Need to check if null-indicator is set
  if (slot_is_nullable) {
    builder->SetInsertPoint(check_slot_null_block);
    llvm::Value* is_slot_null = SlotDescriptor::CodegenIsNull(
        codegen, builder, null_indicator_offset_, tuple_ptr);
    builder->CreateCondBr(is_slot_null, next_block_if_null, next_block_if_not_null);
  }
}

// Codegens reading the members of a StringVal or a CollectionVal from the slot pointed to
// by 'val_ptr'. Returns the resulting values in '*ptr' and '*len'.
void CodegenReadingStringOrCollectionVal(LlvmCodeGen* codegen, LlvmBuilder* builder,
    const ColumnType& type, llvm::Value* val_ptr, llvm::Value** ptr, llvm::Value** len) {
  if (type.IsVarLenStringType()) {
    llvm::Function* str_ptr_fn = codegen->GetFunction(
        IRFunction::STRING_VALUE_PTR, false);
    llvm::Function* str_len_fn = codegen->GetFunction(
        IRFunction::STRING_VALUE_LEN, false);

    *ptr = builder->CreateCall(str_ptr_fn,
        llvm::ArrayRef<llvm::Value*>({val_ptr}), "ptr");
    *len = builder->CreateCall(str_len_fn,
        llvm::ArrayRef<llvm::Value*>({val_ptr}), "len");
  } else if (type.IsCollectionType()) {
    llvm::Value* ptr_ptr = builder->CreateStructGEP(nullptr, val_ptr, 0, "ptr_ptr");
    *ptr = builder->CreateLoad(ptr_ptr, "ptr");
    llvm::Value* len_ptr = builder->CreateStructGEP(nullptr, val_ptr, 1, "len_ptr");
    *len = builder->CreateLoad(len_ptr, "len");
  } else {
    DCHECK(type.type == TYPE_CHAR || type.type == TYPE_FIXED_UDA_INTERMEDIATE);
    // ptr and len are the slot and its fixed length.
    *ptr = builder->CreateBitCast(val_ptr, codegen->ptr_type());
    *len = codegen->GetI32Constant(type.len);
  }
}

void CodegenReadingTimestamp(LlvmCodeGen* codegen, LlvmBuilder* builder,
    llvm::Value* val_ptr, llvm::Value** time_of_day, llvm::Value** date) {
  llvm::Value* time_of_day_ptr =
    builder->CreateStructGEP(nullptr, val_ptr, 0, "time_of_day_ptr");
  // Cast boost::posix_time::time_duration to i64
  llvm::Value* time_of_day_cast =
    builder->CreateBitCast(time_of_day_ptr, codegen->i64_ptr_type());
  *time_of_day = builder->CreateLoad(time_of_day_cast, "time_of_day");
  llvm::Value* date_ptr = builder->CreateStructGEP(nullptr, val_ptr, 1, "date_ptr");
  // Cast boost::gregorian::date to i32
  llvm::Value* date_cast =
    builder->CreateBitCast(date_ptr, codegen->i32_ptr_type());
  *date = builder->CreateLoad(date_cast, "date");
}

CodegenAnyValReadWriteInfo SlotRef::CodegenReadSlot(LlvmCodeGen* codegen,
    LlvmBuilder* builder,
    llvm::Value* eval_ptr, llvm::Value* row_ptr, llvm::BasicBlock* entry_block,
    llvm::BasicBlock* null_block, llvm::BasicBlock* read_slot_block,
    llvm::Value* tuple_ptr, llvm::Value* slot_offset) {
  builder->SetInsertPoint(read_slot_block);
  llvm::Value* slot_ptr = builder->CreateInBoundsGEP(tuple_ptr, slot_offset, "slot_addr");

  // This is not used for structs because the child expressions have their own slot
  // pointers and we only read through those, not through the struct slot pointer.
  llvm::Value* val_ptr = type_.IsStructType() ? nullptr : builder->CreateBitCast(slot_ptr,
      codegen->GetSlotPtrType(type_), "val_ptr");

  // For structs the code that reads the value consists of multiple basic blocks, so the
  // block that should branch to 'produce_value_block' is not 'read_slot_block'. This
  // variable is set to the appropriate block.
  llvm::BasicBlock* non_null_incoming_block = read_slot_block;

  CodegenAnyValReadWriteInfo res(codegen, builder, type_);

  // Depending on the type, create phi nodes for each value needed to populate the return
  // *Val. The optimizer does a better job when there is a phi node for each value, rather
  // than having read_slot_block generate an AnyVal and having a single phi node over
  // that.
  if (type_.IsStructType()) {
    llvm::Function* fn = builder->GetInsertBlock()->getParent();
    std::size_t num_children = children_.size();
    for (std::size_t i = 0; i < num_children; ++i) {
      ScalarExpr* child_expr = children_[i];
      DCHECK(child_expr != nullptr);
      SlotRef* child_slot_ref = dynamic_cast<SlotRef*>(child_expr);
      DCHECK(child_slot_ref != nullptr);

      llvm::Function* const get_child_eval_fn =
        codegen->GetFunction(IRFunction::GET_CHILD_EVALUATOR, false);

      llvm::BasicBlock* child_entry_block = llvm::BasicBlock::Create(codegen->context(),
          "child_entry", fn);
      builder->SetInsertPoint(child_entry_block);
      llvm::Value* child_eval = builder->CreateCall(get_child_eval_fn,
          {eval_ptr, codegen->GetI32Constant(i)}, "child_eval");
      CodegenAnyValReadWriteInfo codegen_anyval_info =
          child_slot_ref->CreateCodegenAnyValReadWriteInfo(codegen, builder, fn,
           child_eval, row_ptr, child_entry_block);
     res.children().push_back(codegen_anyval_info);
    }
  } else if (type_.IsStringType() || type_.type == TYPE_FIXED_UDA_INTERMEDIATE
      || type_.IsCollectionType()) {
    llvm::Value* ptr;
    llvm::Value* len;
    CodegenReadingStringOrCollectionVal(codegen, builder, type_, val_ptr,
        &ptr, &len);
    DCHECK(ptr != nullptr);
    DCHECK(len != nullptr);
    res.SetPtrAndLen(ptr, len);
  } else if (type_.type == TYPE_TIMESTAMP) {
    llvm::Value* time_of_day;
    llvm::Value* date;
    CodegenReadingTimestamp(codegen, builder, val_ptr, &time_of_day, &date);
    DCHECK(time_of_day != nullptr);
    DCHECK(date != nullptr);
    res.SetTimeAndDate(time_of_day, date);
  } else {
    res.SetSimpleVal(builder->CreateLoad(val_ptr, "val"));
  }

  res.SetFnCtxIdx(fn_ctx_idx_);
  res.SetEval(eval_ptr);
  res.SetBlocks(entry_block, null_block, non_null_incoming_block);
  return res;
}

/// Generates code to read the value corresponding to this 'SlotRef' into a *Val. Returns
/// a 'CodegenAnyVal' containing the read value. No return statement is generated in the
/// code, so this function can be called recursively for structs without putting function
/// calls in the generated code.
///
/// The generated code can be conceptually divided into the following parts:
/// 1. Find and load the tuple address (the entry block)
/// 2. Null checking (blocks 'check_tuple_null', 'check_slot_null' and 'null_block')
/// 3. Reading the actual non-null value from its slot (the 'read_slot' block)
/// 4. Produce a final *Val value, whether it is null or not (the 'produce_value' block)
///
/// Number 1. is straightforward.
///
/// Null checking may involve checking the tuple, checking the null indicators for the
/// slot, both or none, depending on what is nullable. If any null check returns true,
/// control branches to 'null_block'. This basic block will be used in PHI nodes as an
/// incoming branch indicating that the *Val is null.
///
/// If all null checks return false, control is transferred to the 'read_slot' block. Here
/// we actually read the value from the slot. This may involve several loads yielding
/// different parts of the value, for example the pointer and the length of a StringValue.
/// If the value is a struct, we recurse to / read the struct members - this produces
/// additional basic blocks.
///
/// The final block, 'produce_value' is used to create the final *Val. This block unites
/// the null and the non-null paths. It has PHI nodes for each value part (ptr, len etc.)
/// from 'null_block' and 'read_slot' (or one of its descendants in case of structs). If
/// control reaches here from 'null_block', the *Val is set to null, otherwise to the
/// value read from the slot.
CodegenAnyVal SlotRef::CodegenValue(LlvmCodeGen* codegen, LlvmBuilder* builder,
    llvm::Function* fn, llvm::Value* eval_ptr, llvm::Value* row_ptr,
    llvm::BasicBlock* entry_block) {
  CodegenAnyValReadWriteInfo read_write_info = CreateCodegenAnyValReadWriteInfo(codegen,
      builder, fn, eval_ptr, row_ptr, entry_block);
  return CodegenAnyVal::CreateFromReadWriteInfo(read_write_info);
}

CodegenAnyValReadWriteInfo SlotRef::CreateCodegenAnyValReadWriteInfo(
    LlvmCodeGen* codegen, LlvmBuilder* builder, llvm::Function* fn,
    llvm::Value* eval_ptr, llvm::Value* row_ptr, llvm::BasicBlock* entry_block) {
  llvm::IRBuilderBase::InsertPoint ip = builder->saveIP();

  llvm::Value* tuple_offset = codegen->GetI32Constant(tuple_idx_);
  llvm::Value* slot_offset = codegen->GetI32Constant(slot_offset_);

  llvm::LLVMContext& context = codegen->context();

  // Create the necessary basic blocks.
  if (entry_block == nullptr) {
    entry_block = llvm::BasicBlock::Create(context, "entry", fn);
  }

  llvm::BasicBlock* read_slot_block = llvm::BasicBlock::Create(context, "read_slot", fn);

  // We use this block to collect all code paths that lead to the result being null. It
  // does nothing but branches unconditionally to 'produce_value_block' and the PHI nodes
  // there can add this block as an incoming branch for the null case; it is simpler and
  // more readable than having many predeccesor blocks for the null case in
  // 'produce_value_block'.
  llvm::BasicBlock* null_block = llvm::BasicBlock::Create(context, "null_block", fn);

  /// Start generating instructions.
  //### Part 1: find the tuple address.
  builder->SetInsertPoint(entry_block);
  // Get the tuple offset addr from the row
  llvm::Value* cast_row_ptr = builder->CreateBitCast(
      row_ptr, codegen->ptr_ptr_type(), "cast_row_ptr");
  llvm::Value* tuple_ptr_addr =
      builder->CreateInBoundsGEP(cast_row_ptr, tuple_offset, "tuple_ptr_addr");
  // Load the tuple*
  llvm::Value* tuple_ptr = builder->CreateLoad(tuple_ptr_addr, "tuple_ptr");

  //### Part 2: null checking
  CodegenNullChecking(codegen, builder, fn, null_block, read_slot_block, tuple_ptr);

  //### Part 3: read non-null value.
  CodegenAnyValReadWriteInfo res = CodegenReadSlot(codegen, builder, eval_ptr,
      row_ptr, entry_block, null_block, read_slot_block, tuple_ptr, slot_offset);

  builder->restoreIP(ip);
  return res;
}

#define SLOT_REF_GET_FUNCTION(type_lit, type_val, type_c) \
    type_val SlotRef::Get##type_val##Interpreted( \
        ScalarExprEvaluator* eval, const TupleRow* row) const { \
      DCHECK_EQ(type_.type, type_lit); \
      Tuple* t = row->GetTuple(tuple_idx_); \
      if (t == NULL || t->IsNull(null_indicator_offset_)) return type_val::null(); \
      return type_val(*reinterpret_cast<type_c*>(t->GetSlot(slot_offset_))); \
    }

SLOT_REF_GET_FUNCTION(TYPE_BOOLEAN, BooleanVal, bool);
SLOT_REF_GET_FUNCTION(TYPE_TINYINT, TinyIntVal, int8_t);
SLOT_REF_GET_FUNCTION(TYPE_SMALLINT, SmallIntVal, int16_t);
SLOT_REF_GET_FUNCTION(TYPE_INT, IntVal, int32_t);
SLOT_REF_GET_FUNCTION(TYPE_BIGINT, BigIntVal, int64_t);
SLOT_REF_GET_FUNCTION(TYPE_FLOAT, FloatVal, float);
SLOT_REF_GET_FUNCTION(TYPE_DOUBLE, DoubleVal, double);

StringVal SlotRef::GetStringValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(type_.IsStringType() || type_.type == TYPE_FIXED_UDA_INTERMEDIATE);
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == NULL || t->IsNull(null_indicator_offset_)) return StringVal::null();
  StringVal result;
  if (type_.type == TYPE_CHAR || type_.type == TYPE_FIXED_UDA_INTERMEDIATE) {
    result.ptr = reinterpret_cast<uint8_t*>(t->GetSlot(slot_offset_));
    result.len = type_.len;
  } else {
    StringValue* sv = reinterpret_cast<StringValue*>(t->GetSlot(slot_offset_));
    sv->ToStringVal(&result);
  }
  return result;
}

TimestampVal SlotRef::GetTimestampValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP);
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == NULL || t->IsNull(null_indicator_offset_)) return TimestampVal::null();
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(t->GetSlot(slot_offset_));
  TimestampVal result;
  tv->ToTimestampVal(&result);
  return result;
}

DecimalVal SlotRef::GetDecimalValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_DECIMAL);
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == NULL || t->IsNull(null_indicator_offset_)) return DecimalVal::null();
  switch (type_.GetByteSize()) {
    case 4:
      return DecimalVal(*reinterpret_cast<int32_t*>(t->GetSlot(slot_offset_)));
    case 8:
      return DecimalVal(*reinterpret_cast<int64_t*>(t->GetSlot(slot_offset_)));
    case 16:
      // Avoid an unaligned load by using memcpy
      __int128_t val;
      memcpy(&val, t->GetSlot(slot_offset_), sizeof(val));
      return DecimalVal(val);
    default:
      DCHECK(false);
      return DecimalVal::null();
  }
}

DateVal SlotRef::GetDateValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_DATE);
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == nullptr || t->IsNull(null_indicator_offset_)) return DateVal::null();
  const DateValue dv(*reinterpret_cast<int32_t*>(t->GetSlot(slot_offset_)));
  return dv.ToDateVal();
}

CollectionVal SlotRef::GetCollectionValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(type_.IsCollectionType());
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == nullptr || t->IsNull(null_indicator_offset_)) return CollectionVal::null();
  CollectionValue* coll_value =
      reinterpret_cast<CollectionValue*>(t->GetSlot(slot_offset_));
  return CollectionVal(coll_value->ptr, coll_value->num_tuples);
}

StructVal SlotRef::GetStructValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(type_.IsStructType() && children_.size() > 0);
  DCHECK_EQ(children_.size(), eval->GetChildEvaluators().size());
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == nullptr || t->IsNull(null_indicator_offset_)) return StructVal::null();

  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
  DCHECK(fn_ctx != nullptr);
  StructVal struct_val(fn_ctx, children_.size());
  vector<ScalarExprEvaluator*>& child_evaluators = eval->GetChildEvaluators();
  for (int i = 0; i < child_evaluators.size(); ++i) {
    ScalarExpr* child_expr = children_[i];
    ScalarExprEvaluator* child_eval = child_evaluators[i];
    DCHECK(child_eval != nullptr);
    void* child_val = child_eval->GetValue(*child_expr, row);
    struct_val.addChild(child_val, i);
  }
  return struct_val;
}

const TupleDescriptor* SlotRef::GetCollectionTupleDesc() const {
  return slot_desc_->children_tuple_descriptor();
}

} // namespace impala
