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

#include "exprs/slot-ref.h"

#include <sstream>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "gen-cpp/Exprs_types.h"
#include "runtime/array-value.h"
#include "runtime/runtime-state.h"

#include "common/names.h"

using namespace impala_udf;
using namespace llvm;

namespace impala {

SlotRef::SlotRef(const TExprNode& node)
  : Expr(node, true),
    slot_offset_(-1),  // invalid
    null_indicator_offset_(0, 0),
    slot_id_(node.slot_ref.slot_id) {
    // slot_/null_indicator_offset_ are set in Prepare()
}

SlotRef::SlotRef(const SlotDescriptor* desc)
  : Expr(desc->type(), true),
    slot_offset_(-1),
    null_indicator_offset_(0, 0),
    slot_id_(desc->id()) {
    // slot_/null_indicator_offset_ are set in Prepare()
}

SlotRef::SlotRef(const SlotDescriptor* desc, const ColumnType& type)
  : Expr(type, true),
    slot_offset_(-1),
    null_indicator_offset_(0, 0),
    slot_id_(desc->id()) {
    // slot_/null_indicator_offset_ are set in Prepare()
}

SlotRef::SlotRef(const ColumnType& type, int offset)
  : Expr(type, true),
    tuple_idx_(0),
    slot_offset_(offset),
    null_indicator_offset_(0, -1),
    slot_id_(-1) {
}

Status SlotRef::Prepare(RuntimeState* state, const RowDescriptor& row_desc,
                        ExprContext* context) {
  DCHECK_EQ(children_.size(), 0);
  if (slot_id_ == -1) return Status::OK();

  const SlotDescriptor* slot_desc  = state->desc_tbl().GetSlotDescriptor(slot_id_);
  if (slot_desc == NULL) {
    // TODO: create macro MAKE_ERROR() that returns a stream
    stringstream error;
    error << "couldn't resolve slot descriptor " << slot_id_;
    LOG(INFO) << error.str();
    return Status(error.str());
  }
  if (!slot_desc->is_materialized()) {
    stringstream error;
    error << "reference to non-materialized slot " << slot_id_;
    return Status(error.str());
  }
  tuple_idx_ = row_desc.GetTupleIdx(slot_desc->parent()->id());
  if (tuple_idx_ == RowDescriptor::INVALID_IDX) {
    TupleDescriptor* d = state->desc_tbl().GetTupleDescriptor(slot_desc->parent()->id());
    LOG(INFO) << "invalid idx: " << slot_desc->DebugString()
              << "\nparent=" << d->DebugString()
              << "\nrow=" << row_desc.DebugString();
    stringstream error;
    error << "invalid tuple_idx";
    return Status(error.str());
  }
  DCHECK(tuple_idx_ != RowDescriptor::INVALID_IDX);
  tuple_is_nullable_ = row_desc.TupleIsNullable(tuple_idx_);
  slot_offset_ = slot_desc->tuple_offset();
  null_indicator_offset_ = slot_desc->null_indicator_offset();
  return Status::OK();
}

int SlotRef::GetSlotIds(vector<SlotId>* slot_ids) const {
  slot_ids->push_back(slot_id_);
  return 1;
}

string SlotRef::DebugString() const {
  stringstream out;
  out << "SlotRef(slot_id=" << slot_id_
      << " tuple_idx=" << tuple_idx_ << " slot_offset=" << slot_offset_
      << " tuple_is_nullable=" << tuple_is_nullable_
      << " null_indicator=" << null_indicator_offset_
      << Expr::DebugString() << ")";
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
//   %null_ptr = getelementptr i8* %tuple_ptr, i32 0
//   %null_byte = load i8* %null_ptr
//   %null_byte_set = and i8 %null_byte, 2
//   %slot_is_null = icmp ne i8 %null_byte_set, 0
//   br i1 %slot_is_null, label %ret, label %get_slot
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
// the types from the TupleDesc.  It will likey make this code simpler to reason about.
Status SlotRef::GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn) {
  if (type_.type == TYPE_CHAR) {
    *fn = NULL;
    return Status("Codegen for Char not supported.");
  }
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK();
  }

  DCHECK_EQ(GetNumChildren(), 0);
  LlvmCodeGen* codegen;
  RETURN_IF_ERROR(state->GetCodegen(&codegen));

  // SlotRefs are based on the slot_id and tuple_idx.  Combine them to make a
  // query-wide unique id. We also need to combine whether the tuple is nullable. For
  // example, in an outer join the scan node could have the same tuple id and slot id
  // as the join node. When the slot is being used in the scan-node, the tuple is
  // non-nullable. Used in the join node (and above in the plan tree), it is nullable.
  // TODO: can we do something better.
  const int64_t TUPLE_NULLABLE_MASK = 1L << 63;
  int64_t unique_slot_id = slot_id_ | ((int64_t)tuple_idx_) << 32;
  DCHECK_EQ(unique_slot_id & TUPLE_NULLABLE_MASK, 0);
  if (tuple_is_nullable_) unique_slot_id |= TUPLE_NULLABLE_MASK;
  Function* ir_compute_fn_ = codegen->GetRegisteredExprFn(unique_slot_id);
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK();
  }

  LLVMContext& context = codegen->context();
  Value* args[2];
  *fn = CreateIrFunctionPrototype(codegen, "GetSlotRef", &args);
  Value* row_ptr = args[1];

  Value* tuple_offset = ConstantInt::get(codegen->int_type(), tuple_idx_);
  Value* null_byte_offset =
    ConstantInt::get(codegen->int_type(), null_indicator_offset_.byte_offset);
  Value* slot_offset = ConstantInt::get(codegen->int_type(), slot_offset_);
  Value* null_mask =
      ConstantInt::get(codegen->tinyint_type(), null_indicator_offset_.bit_mask);
  Value* zero = ConstantInt::get(codegen->GetType(TYPE_TINYINT), 0);
  Value* one = ConstantInt::get(codegen->GetType(TYPE_TINYINT), 1);

  BasicBlock* entry_block = BasicBlock::Create(context, "entry", *fn);
  BasicBlock* check_slot_null_indicator_block = NULL;
  if (null_indicator_offset_.bit_mask != 0) {
    check_slot_null_indicator_block =
        BasicBlock::Create(context, "check_slot_null", *fn);
  }
  BasicBlock* get_slot_block = BasicBlock::Create(context, "get_slot", *fn);
  BasicBlock* ret_block = BasicBlock::Create(context, "ret", *fn);

  LlvmCodeGen::LlvmBuilder builder(entry_block);
  // Get the tuple offset addr from the row
  Value* cast_row_ptr = builder.CreateBitCast(
      row_ptr, PointerType::get(codegen->ptr_type(), 0), "cast_row_ptr");
  Value* tuple_addr = builder.CreateGEP(cast_row_ptr, tuple_offset, "tuple_addr");
  // Load the tuple*
  Value* tuple_ptr = builder.CreateLoad(tuple_addr, "tuple_ptr");

  // Check if tuple* is null only if the tuple is nullable
  if (tuple_is_nullable_) {
    Value* tuple_is_null = builder.CreateIsNull(tuple_ptr, "tuple_is_null");
    // Check slot is null only if the null indicator bit is set
    if (null_indicator_offset_.bit_mask == 0) {
      builder.CreateCondBr(tuple_is_null, ret_block, get_slot_block);
    } else {
      builder.CreateCondBr(tuple_is_null, ret_block, check_slot_null_indicator_block);
    }
  } else {
    if (null_indicator_offset_.bit_mask == 0) {
      builder.CreateBr(get_slot_block);
    } else {
      builder.CreateBr(check_slot_null_indicator_block);
    }
  }

  // Branch for tuple* != NULL.  Need to check if null-indicator is set
  if (check_slot_null_indicator_block != NULL) {
    builder.SetInsertPoint(check_slot_null_indicator_block);
    Value* null_addr = builder.CreateGEP(tuple_ptr, null_byte_offset, "null_ptr");
    Value* null_val = builder.CreateLoad(null_addr, "null_byte");
    Value* slot_null_mask = builder.CreateAnd(null_val, null_mask, "null_byte_set");
    Value* is_slot_null = builder.CreateICmpNE(slot_null_mask, zero, "slot_is_null");
    builder.CreateCondBr(is_slot_null, ret_block, get_slot_block);
  }

  // Branch for slot != NULL
  builder.SetInsertPoint(get_slot_block);
  Value* slot_ptr = builder.CreateGEP(tuple_ptr, slot_offset, "slot_addr");
  Value* val_ptr = builder.CreateBitCast(slot_ptr, codegen->GetPtrType(type_), "val_ptr");
  // Depending on the type, load the values we need
  Value* val = NULL;
  Value* ptr = NULL;
  Value* len = NULL;
  Value* time_of_day = NULL;
  Value* date = NULL;
  if (type_.IsStringType()) {
    Value* ptr_ptr = builder.CreateStructGEP(val_ptr, 0, "ptr_ptr");
    ptr = builder.CreateLoad(ptr_ptr, "ptr");
    Value* len_ptr = builder.CreateStructGEP(val_ptr, 1, "len_ptr");
    len = builder.CreateLoad(len_ptr, "len");
  } else if (type() == TYPE_TIMESTAMP) {
    Value* time_of_day_ptr = builder.CreateStructGEP(val_ptr, 0, "time_of_day_ptr");
    // Cast boost::posix_time::time_duration to i64
    Value* time_of_day_cast =
        builder.CreateBitCast(time_of_day_ptr, codegen->GetPtrType(TYPE_BIGINT));
    time_of_day = builder.CreateLoad(time_of_day_cast, "time_of_day");
    Value* date_ptr = builder.CreateStructGEP(val_ptr, 1, "date_ptr");
    // Cast boost::gregorian::date to i32
    Value* date_cast = builder.CreateBitCast(date_ptr, codegen->GetPtrType(TYPE_INT));
    date = builder.CreateLoad(date_cast, "date");
  } else {
    // val_ptr is a native type
    val = builder.CreateLoad(val_ptr, "val");
  }
  builder.CreateBr(ret_block);

  // Return block
  builder.SetInsertPoint(ret_block);
  PHINode* is_null_phi = builder.CreatePHI(codegen->tinyint_type(), 2, "is_null_phi");
  if (tuple_is_nullable_) is_null_phi->addIncoming(one, entry_block);
  if (check_slot_null_indicator_block != NULL) {
    is_null_phi->addIncoming(one, check_slot_null_indicator_block);
  }
  is_null_phi->addIncoming(zero, get_slot_block);

  // Depending on the type, create phi nodes for each value needed to populate the return
  // *Val. The optimizer does a better job when there is a phi node for each value, rather
  // than having get_slot_block generate an AnyVal and having a single phi node over that.
  // TODO: revisit this code, can possibly be simplified
  if (type().IsVarLenStringType()) {
    DCHECK(ptr != NULL);
    DCHECK(len != NULL);
    PHINode* ptr_phi = builder.CreatePHI(ptr->getType(), 2, "ptr_phi");
    Value* null = Constant::getNullValue(ptr->getType());
    if (tuple_is_nullable_) {
      ptr_phi->addIncoming(null, entry_block);
    }
    if (check_slot_null_indicator_block != NULL) {
      ptr_phi->addIncoming(null, check_slot_null_indicator_block);
    }
    ptr_phi->addIncoming(ptr, get_slot_block);

    PHINode* len_phi = builder.CreatePHI(len->getType(), 2, "len_phi");
    null = ConstantInt::get(len->getType(), 0);
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
    builder.CreateRet(result.value());
  } else if (type() == TYPE_TIMESTAMP) {
    DCHECK(time_of_day != NULL);
    DCHECK(date != NULL);
    PHINode* time_of_day_phi =
        builder.CreatePHI(time_of_day->getType(), 2, "time_of_day_phi");
    Value* null = ConstantInt::get(time_of_day->getType(), 0);
    if (tuple_is_nullable_) {
      time_of_day_phi->addIncoming(null, entry_block);
    }
    if (check_slot_null_indicator_block != NULL) {
      time_of_day_phi->addIncoming(null, check_slot_null_indicator_block);
    }
    time_of_day_phi->addIncoming(time_of_day, get_slot_block);

    PHINode* date_phi = builder.CreatePHI(date->getType(), 2, "date_phi");
    null = ConstantInt::get(date->getType(), 0);
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
    builder.CreateRet(result.value());
  } else {
    DCHECK(val != NULL);
    PHINode* val_phi = builder.CreatePHI(val->getType(), 2, "val_phi");
    Value* null = Constant::getNullValue(val->getType());
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
    builder.CreateRet(result.value());
  }

  *fn = codegen->FinalizeFunction(*fn);
  codegen->RegisterExprFn(unique_slot_id, *fn);
  ir_compute_fn_ = *fn;
  return Status::OK();
}

BooleanVal SlotRef::GetBooleanVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_BOOLEAN);
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == NULL || t->IsNull(null_indicator_offset_)) return BooleanVal::null();
  return BooleanVal(*reinterpret_cast<bool*>(t->GetSlot(slot_offset_)));
}

TinyIntVal SlotRef::GetTinyIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_TINYINT);
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == NULL || t->IsNull(null_indicator_offset_)) return TinyIntVal::null();
  return TinyIntVal(*reinterpret_cast<int8_t*>(t->GetSlot(slot_offset_)));
}

SmallIntVal SlotRef::GetSmallIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_SMALLINT);
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == NULL || t->IsNull(null_indicator_offset_)) return SmallIntVal::null();
  return SmallIntVal(*reinterpret_cast<int16_t*>(t->GetSlot(slot_offset_)));
}

IntVal SlotRef::GetIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_INT);
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == NULL || t->IsNull(null_indicator_offset_)) return IntVal::null();
  return IntVal(*reinterpret_cast<int32_t*>(t->GetSlot(slot_offset_)));
}

BigIntVal SlotRef::GetBigIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_BIGINT);
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == NULL || t->IsNull(null_indicator_offset_)) return BigIntVal::null();
  return BigIntVal(*reinterpret_cast<int64_t*>(t->GetSlot(slot_offset_)));
}

FloatVal SlotRef::GetFloatVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_FLOAT);
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == NULL || t->IsNull(null_indicator_offset_)) return FloatVal::null();
  return FloatVal(*reinterpret_cast<float*>(t->GetSlot(slot_offset_)));
}

DoubleVal SlotRef::GetDoubleVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_DOUBLE);
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == NULL || t->IsNull(null_indicator_offset_)) return DoubleVal::null();
  return DoubleVal(*reinterpret_cast<double*>(t->GetSlot(slot_offset_)));
}

StringVal SlotRef::GetStringVal(ExprContext* context, TupleRow* row) {
  DCHECK(type_.IsStringType());
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == NULL || t->IsNull(null_indicator_offset_)) return StringVal::null();
  StringVal result;
  if (type_.type == TYPE_CHAR) {
    result.ptr = reinterpret_cast<uint8_t*>(
        StringValue::CharSlotToPtr(t->GetSlot(slot_offset_), type_));
    result.len = type_.len;
  } else {
    StringValue* sv = reinterpret_cast<StringValue*>(t->GetSlot(slot_offset_));
    sv->ToStringVal(&result);
  }
  return result;
}

TimestampVal SlotRef::GetTimestampVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP);
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == NULL || t->IsNull(null_indicator_offset_)) return TimestampVal::null();
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(t->GetSlot(slot_offset_));
  TimestampVal result;
  tv->ToTimestampVal(&result);
  return result;
}

DecimalVal SlotRef::GetDecimalVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_DECIMAL);
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == NULL || t->IsNull(null_indicator_offset_)) return DecimalVal::null();
  switch (type_.GetByteSize()) {
    case 4:
      return DecimalVal(*reinterpret_cast<int32_t*>(t->GetSlot(slot_offset_)));
    case 8:
      return DecimalVal(*reinterpret_cast<int64_t*>(t->GetSlot(slot_offset_)));
    case 16:
      return DecimalVal(*reinterpret_cast<int128_t*>(t->GetSlot(slot_offset_)));
    default:
      DCHECK(false);
      return DecimalVal::null();
  }
}

ArrayVal SlotRef::GetArrayVal(ExprContext* context, TupleRow* row) {
  DCHECK(type_.IsCollectionType());
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == NULL || t->IsNull(null_indicator_offset_)) return ArrayVal::null();
  ArrayValue* array_value = reinterpret_cast<ArrayValue*>(t->GetSlot(slot_offset_));
  return ArrayVal(array_value->ptr, array_value->num_tuples);
}

}
