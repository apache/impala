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
    // slot_/null_indicator_offset_ are set in Prepare()
}

SlotRef::SlotRef(const SlotDescriptor* desc)
  : ScalarExpr(desc->type(), false),
    slot_offset_(-1),
    null_indicator_offset_(0, 0),
    slot_id_(desc->id()) {
    // slot_/null_indicator_offset_ are set in Prepare()
}

SlotRef::SlotRef(const SlotDescriptor* desc, const ColumnType& type)
  : ScalarExpr(type, false),
    slot_offset_(-1),
    null_indicator_offset_(0, 0),
    slot_id_(desc->id()) {
    // slot_/null_indicator_offset_ are set in Prepare()
}

SlotRef::SlotRef(const ColumnType& type, int offset, const bool nullable /* = false */)
  : ScalarExpr(type, false),
    tuple_idx_(0),
    slot_offset_(offset),
    null_indicator_offset_(0, nullable ? offset : -1),
    slot_id_(-1) {}

Status SlotRef::Init(
    const RowDescriptor& row_desc, bool is_entry_point, FragmentState* state) {
  DCHECK_EQ(children_.size(), 0);
  RETURN_IF_ERROR(ScalarExpr::Init(row_desc, is_entry_point, state));
  if (slot_id_ != -1) {
    const SlotDescriptor* slot_desc = state->desc_tbl().GetSlotDescriptor(slot_id_);
    if (slot_desc == NULL) {
      // TODO: create macro MAKE_ERROR() that returns a stream
      stringstream error;
      error << "couldn't resolve slot descriptor " << slot_id_;
      LOG(INFO) << error.str();
      return Status(error.str());
    }
    tuple_idx_ = row_desc.GetTupleIdx(slot_desc->parent()->id());
    if (tuple_idx_ == RowDescriptor::INVALID_IDX) {
      TupleDescriptor* d =
          state->desc_tbl().GetTupleDescriptor(slot_desc->parent()->id());
      string error = Substitute("invalid tuple_idx: $0\nparent=$1\nrow=$2",
          slot_desc->DebugString(), d->DebugString(), row_desc.DebugString());
      LOG(INFO) << error;
      return Status(error);
    }
    DCHECK(tuple_idx_ != RowDescriptor::INVALID_IDX);
    tuple_is_nullable_ = row_desc.TupleIsNullable(tuple_idx_);
    slot_offset_ = slot_desc->tuple_offset();
    null_indicator_offset_ = slot_desc->null_indicator_offset();
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
// define { i8, i64 } @GetSlotRef(i8** %context, %"class.impala::TupleRow"* %row) {
// entry:
//   %cast_row_ptr = bitcast %"class.impala::TupleRow"* %row to i8**
//   %tuple_addr = getelementptr i8** %cast_row_ptr, i32 0
//   %tuple_ptr = load i8** %tuple_addr
//   br label %check_slot_null
//
// check_slot_null:                                  ; preds = %entry
//   %null_byte_ptr = getelementptr i8* %tuple_ptr, i32 0
//   %null_byte = load i8* %null_ptr
//   %null_byte_set = and i8 %null_byte, 2
//   %is_null = icmp ne i8 %null_byte_set, 0
//   br i1 %is_null, label %ret, label %get_slot
//
// get_slot:                                         ; preds = %check_slot_null
//   %slot_addr = getelementptr i8* %tuple_ptr, i32 8
//   %val_ptr = bitcast i8* %slot_addr to i64*
//   %val = load i64* %val_ptr
//   br label %ret
//
// ret:                                              ; preds = %get_slot, %check_slot_null
//   %is_null_phi = phi i8 [ 1, %check_slot_null ], [ 0, %get_slot ]
//   %val_phi = phi i64 [ 0, %check_slot_null ], [ %val, %get_slot ]
//   %result = insertvalue { i8, i64 } zeroinitializer, i8 %is_null_phi, 0
//   %result1 = insertvalue { i8, i64 } %result, i64 %val_phi, 1
//   ret { i8, i64 } %result1
// }
//
// TODO: We could generate a typed struct (and not a char*) for Tuple for llvm.  We know
// the types from the TupleDesc.  It will likely make this code simpler to reason about.
Status SlotRef::GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn) {
  DCHECK_EQ(GetNumChildren(), 0);
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

  llvm::LLVMContext& context = codegen->context();
  llvm::Value* args[2];
  *fn = CreateIrFunctionPrototype("GetSlotRef", codegen, &args);
  llvm::Value* row_ptr = args[1];

  llvm::Value* tuple_offset = codegen->GetI32Constant(tuple_idx_);
  llvm::Value* slot_offset = codegen->GetI32Constant(slot_offset_);
  llvm::Value* zero = codegen->GetI8Constant(0);
  llvm::Value* one = codegen->GetI8Constant(1);

  llvm::BasicBlock* entry_block = llvm::BasicBlock::Create(context, "entry", *fn);
  bool slot_is_nullable = null_indicator_offset_.bit_mask != 0;
  llvm::BasicBlock* check_slot_null_indicator_block = NULL;
  if (slot_is_nullable) {
    check_slot_null_indicator_block =
        llvm::BasicBlock::Create(context, "check_slot_null", *fn);
  }
  llvm::BasicBlock* get_slot_block = llvm::BasicBlock::Create(context, "get_slot", *fn);
  llvm::BasicBlock* ret_block = llvm::BasicBlock::Create(context, "ret", *fn);

  LlvmBuilder builder(entry_block);
  // Get the tuple offset addr from the row
  llvm::Value* cast_row_ptr = builder.CreateBitCast(
      row_ptr, codegen->ptr_ptr_type(), "cast_row_ptr");
  llvm::Value* tuple_addr =
      builder.CreateInBoundsGEP(cast_row_ptr, tuple_offset, "tuple_addr");
  // Load the tuple*
  llvm::Value* tuple_ptr = builder.CreateLoad(tuple_addr, "tuple_ptr");

  // Check if tuple* is null only if the tuple is nullable
  if (tuple_is_nullable_) {
    llvm::Value* tuple_is_null = builder.CreateIsNull(tuple_ptr, "tuple_is_null");
    // Check slot is null only if the null indicator bit is set
    if (slot_is_nullable) {
      builder.CreateCondBr(tuple_is_null, ret_block, check_slot_null_indicator_block);
    } else {
      builder.CreateCondBr(tuple_is_null, ret_block, get_slot_block);
    }
  } else {
    if (slot_is_nullable) {
      builder.CreateBr(check_slot_null_indicator_block);
    } else {
      builder.CreateBr(get_slot_block);
    }
  }

  // Branch for tuple* != NULL.  Need to check if null-indicator is set
  if (slot_is_nullable) {
    builder.SetInsertPoint(check_slot_null_indicator_block);
    llvm::Value* is_slot_null = SlotDescriptor::CodegenIsNull(
        codegen, &builder, null_indicator_offset_, tuple_ptr);
    builder.CreateCondBr(is_slot_null, ret_block, get_slot_block);
  }

  // Branch for slot != NULL
  builder.SetInsertPoint(get_slot_block);
  llvm::Value* slot_ptr = builder.CreateInBoundsGEP(tuple_ptr, slot_offset, "slot_addr");
  llvm::Value* val_ptr = builder.CreateBitCast(slot_ptr,
      codegen->GetSlotPtrType(type_), "val_ptr");
  // Depending on the type, load the values we need
  llvm::Value* val = NULL;
  llvm::Value* ptr = NULL;
  llvm::Value* len = NULL;
  llvm::Value* time_of_day = NULL;
  llvm::Value* date = NULL;
  if (type_.IsVarLenStringType() || type_.IsCollectionType()) {
    llvm::Value* ptr_ptr = builder.CreateStructGEP(NULL, val_ptr, 0, "ptr_ptr");
    ptr = builder.CreateLoad(ptr_ptr, "ptr");
    llvm::Value* len_ptr = builder.CreateStructGEP(NULL, val_ptr, 1, "len_ptr");
    len = builder.CreateLoad(len_ptr, "len");
  } else if (type_.type == TYPE_CHAR || type_.type == TYPE_FIXED_UDA_INTERMEDIATE) {
    // ptr and len are the slot and its fixed length.
    ptr = builder.CreateBitCast(val_ptr, codegen->ptr_type());
    len = codegen->GetI32Constant(type_.len);
  } else if (type_.type == TYPE_TIMESTAMP) {
    llvm::Value* time_of_day_ptr =
        builder.CreateStructGEP(NULL, val_ptr, 0, "time_of_day_ptr");
    // Cast boost::posix_time::time_duration to i64
    llvm::Value* time_of_day_cast =
        builder.CreateBitCast(time_of_day_ptr, codegen->i64_ptr_type());
    time_of_day = builder.CreateLoad(time_of_day_cast, "time_of_day");
    llvm::Value* date_ptr = builder.CreateStructGEP(NULL, val_ptr, 1, "date_ptr");
    // Cast boost::gregorian::date to i32
    llvm::Value* date_cast =
        builder.CreateBitCast(date_ptr, codegen->i32_ptr_type());
    date = builder.CreateLoad(date_cast, "date");
  } else {
    // val_ptr is a native type
    val = builder.CreateLoad(val_ptr, "val");
  }
  builder.CreateBr(ret_block);

  // Return block
  builder.SetInsertPoint(ret_block);
  llvm::PHINode* is_null_phi =
      builder.CreatePHI(codegen->i8_type(), 2, "is_null_phi");
  if (tuple_is_nullable_) is_null_phi->addIncoming(one, entry_block);
  if (check_slot_null_indicator_block != NULL) {
    is_null_phi->addIncoming(one, check_slot_null_indicator_block);
  }
  is_null_phi->addIncoming(zero, get_slot_block);

  // Depending on the type, create phi nodes for each value needed to populate the return
  // *Val. The optimizer does a better job when there is a phi node for each value, rather
  // than having get_slot_block generate an AnyVal and having a single phi node over that.
  // TODO: revisit this code, can possibly be simplified
  if (type_.IsStringType() || type_.type == TYPE_FIXED_UDA_INTERMEDIATE
      || type_.IsCollectionType()) {
    DCHECK(ptr != NULL);
    DCHECK(len != NULL);
    llvm::PHINode* ptr_phi = builder.CreatePHI(ptr->getType(), 2, "ptr_phi");
    llvm::Value* null = llvm::Constant::getNullValue(ptr->getType());
    if (tuple_is_nullable_) {
      ptr_phi->addIncoming(null, entry_block);
    }
    if (check_slot_null_indicator_block != NULL) {
      ptr_phi->addIncoming(null, check_slot_null_indicator_block);
    }
    ptr_phi->addIncoming(ptr, get_slot_block);

    llvm::PHINode* len_phi = builder.CreatePHI(len->getType(), 2, "len_phi");
    null = llvm::ConstantInt::get(len->getType(), 0);
    if (tuple_is_nullable_) {
      len_phi->addIncoming(null, entry_block);
    }
    if (check_slot_null_indicator_block != NULL) {
      len_phi->addIncoming(null, check_slot_null_indicator_block);
    }
    len_phi->addIncoming(len, get_slot_block);

    CodegenAnyVal result =
        CodegenAnyVal::GetNonNullVal(codegen, &builder, type(), "result");
    result.SetIsNull(is_null_phi);
    result.SetPtr(ptr_phi);
    result.SetLen(len_phi);
    builder.CreateRet(result.GetLoweredValue());
  } else if (type_.type == TYPE_TIMESTAMP) {
    DCHECK(time_of_day != NULL);
    DCHECK(date != NULL);
    llvm::PHINode* time_of_day_phi =
        builder.CreatePHI(time_of_day->getType(), 2, "time_of_day_phi");
    llvm::Value* null = llvm::ConstantInt::get(time_of_day->getType(), 0);
    if (tuple_is_nullable_) {
      time_of_day_phi->addIncoming(null, entry_block);
    }
    if (check_slot_null_indicator_block != NULL) {
      time_of_day_phi->addIncoming(null, check_slot_null_indicator_block);
    }
    time_of_day_phi->addIncoming(time_of_day, get_slot_block);

    llvm::PHINode* date_phi = builder.CreatePHI(date->getType(), 2, "date_phi");
    null = llvm::ConstantInt::get(date->getType(), 0);
    if (tuple_is_nullable_) {
      date_phi->addIncoming(null, entry_block);
    }
    if (check_slot_null_indicator_block != NULL) {
      date_phi->addIncoming(null, check_slot_null_indicator_block);
    }
    date_phi->addIncoming(date, get_slot_block);

    CodegenAnyVal result =
        CodegenAnyVal::GetNonNullVal(codegen, &builder, type(), "result");
    result.SetIsNull(is_null_phi);
    result.SetTimeOfDay(time_of_day_phi);
    result.SetDate(date_phi);
    builder.CreateRet(result.GetLoweredValue());
  } else {
    DCHECK(val != NULL);
    llvm::PHINode* val_phi = builder.CreatePHI(val->getType(), 2, "val_phi");
    llvm::Value* null = llvm::Constant::getNullValue(val->getType());
    if (tuple_is_nullable_) {
      val_phi->addIncoming(null, entry_block);
    }
    if (check_slot_null_indicator_block != NULL) {
      val_phi->addIncoming(null, check_slot_null_indicator_block);
    }
    val_phi->addIncoming(val, get_slot_block);

    CodegenAnyVal result =
        CodegenAnyVal::GetNonNullVal(codegen, &builder, type(), "result");
    result.SetIsNull(is_null_phi);
    result.SetVal(val_phi);
    builder.CreateRet(result.GetLoweredValue());
  }

  *fn = codegen->FinalizeFunction(*fn);
  if (UNLIKELY(*fn == NULL)) return Status(TErrorCode::IR_VERIFY_FAILED, "SlotRef");
  codegen->RegisterExprFn(unique_slot_id, *fn);
  return Status::OK();
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

} // namespace impala
