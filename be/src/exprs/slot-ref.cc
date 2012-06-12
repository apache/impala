// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exprs/expr.h"  // contains SlotRef definition

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "gen-cpp/Exprs_types.h"
#include "runtime/runtime-state.h"

using namespace std;
using namespace llvm;

namespace impala {

SlotRef::SlotRef(const TExprNode& node)
  : Expr(node, true),
    slot_offset_(-1),  // invalid
    null_indicator_offset_(0, 0),
    slot_id_(node.slot_ref.slot_id) {
    // slot_/null_indicator_offset_ are set in Prepare()
}

SlotRef::SlotRef(PrimitiveType type, int offset) 
  : Expr(type, true),
  tuple_idx_(0),
  slot_offset_(offset),
  null_indicator_offset_(0, -1),
  slot_id_(-1) {
}

Status SlotRef::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK_EQ(children_.size(), 0);
  if (slot_id_ == -1) return Status::OK;

  const SlotDescriptor* slot_desc  = state->desc_tbl().GetSlotDescriptor(slot_id_);
  if (slot_desc == NULL) {
    // TODO: create macro MAKE_ERROR() that returns a stream
    stringstream error;
    error << "couldn't resolve slot descriptor " << slot_id_;
    return Status(error.str());
  }
  if (!slot_desc->is_materialized()) {
    stringstream error;
    error << "reference to non-materialized slot " << slot_id_;
    return Status(error.str());
  }
  // TODO(marcel): get from runtime state
  tuple_idx_ = row_desc.GetTupleIdx(slot_desc->parent());
  tuple_is_nullable_ = row_desc.TupleIsNullable(tuple_idx_);
  DCHECK(tuple_idx_ != RowDescriptor::INVALID_IDX);
  slot_offset_ = slot_desc->tuple_offset();
  null_indicator_offset_ = slot_desc->null_indicator_offset();
  return Status::OK;
}

int SlotRef::GetSlotIds(vector<SlotId>* slot_ids) const {
  slot_ids->push_back(slot_id_);
  return 1;
}

string SlotRef::DebugString() const {
  stringstream out;
  out << "SlotRef(slot_id=" << slot_id_
      << " tuple_idx=" << tuple_idx_ << " slot_offset=" << slot_offset_
      << " null_indicator=" << null_indicator_offset_
      << " " << Expr::DebugString() << ")";
  return out.str();
}

// IR Generation for SlotRef.  The resulting IR looks like:
// TODO: We could generate a typed struct (and not a char*) for Tuple for llvm.
// We know the types from the TupleDesc.  It will likey make this code simpler
// to reason about.
//
// Note: some of the casts that look like no-ops are because certain offsets are 0
// is this slot descriptor.
//
// There are 4 cases that we've to generate:
//   1. tuple is not nullable and slot is not nullable
//   2. tuple is not nullable and slot is nullable
//   3. tuple is nullable and slot is not nullable (it is the case when the aggregate
//      output is the "nullable" side of an outer join).
//   4. tuple is nullable and slot is nullable
//
// define i8 @SlotRef1(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   %tuple_addr1 = bitcast i8** %row to i8**
//   %tuple_ptr = load i8** %tuple_addr1
//   %tuple_is_null = icmp eq i8* %tuple_ptr, null
//   br i1 %tuple_is_null, label %null_ret, label %check_slot_null
// 
// check_slot_null:                                   ; preds = %entry
//   %null_ptr2 = bitcast i8* %tuple_ptr to i8*
//   %null_byte = load i8* %null_ptr2
//   %null_byte_set = and i8 %null_byte, 1
//   %slot_is_null = icmp ne i8 %null_byte_set, 0
//   br i1 %slot_is_null, label %null_ret, label %get_slot
// 
// get_slot:                                         ; preds = %check_slot_null
//   %slot_addr = getelementptr i8* %tuple_ptr, i32 1
//   %slot_value = load i8* %slot_addr
//   store i1 false, i1* %is_null
//   br label %ret
// 
// null_ret:                                         ; preds = %check_slot_null, %entry
//   store i1 true, i1* %is_null
//   br label %ret
// 
// ret:                                              ; preds = %null_ret, %get_slot
//   %tmp_phi = phi i8 [ 0, %null_ret ], [ %slot_value, %get_slot ]
//   ret i8 %tmp_phi
// }
Function* SlotRef::Codegen(LlvmCodeGen* codegen) {
  DCHECK_EQ(GetNumChildren(), 0);
  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  
  Type* return_type = GetLlvmReturnType(codegen);
  Function* function = CreateComputeFnPrototype(codegen, "SlotRef");

  Function::arg_iterator func_args = function->arg_begin();
  Value* row_ptr = func_args;

  Type* int_type = codegen->GetType(TYPE_INT);
  Value* tuple_offset[] = { 
    ConstantInt::get(int_type, tuple_idx_)
  };
  Value* null_byte_offset[] = { 
    ConstantInt::get(int_type, null_indicator_offset_.byte_offset)
  };
  Value* slot_offset[] = { 
    ConstantInt::get(int_type, slot_offset_)
  };
  Value* null_mask = 
      ConstantInt::get(context, APInt(8, null_indicator_offset_.bit_mask, true));
  Value* zero = ConstantInt::get(codegen->GetType(TYPE_TINYINT), 0);
  Type* result_type = codegen->GetType(type());
  PointerType* result_ptr_type = PointerType::get(result_type, 0);

  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);
  BasicBlock* check_slot_null_indicator_block = NULL;
  if (null_indicator_offset_.bit_mask != 0) {
    check_slot_null_indicator_block =
        BasicBlock::Create(context, "check_slot_null", function);
  }
  BasicBlock* get_slot_block = BasicBlock::Create(context, "get_slot", function);
  BasicBlock* null_ret_block = BasicBlock::Create(context, "null_ret", function);
  BasicBlock* ret_block = BasicBlock::Create(context, "ret", function);

  builder.SetInsertPoint(entry_block);
  // Get the tuple offset addr from the row
  Value* tuple_addr = builder.CreateGEP(row_ptr, tuple_offset, "tuple_addr"); 
  // Load the tuple*
  Value* tuple_ptr = builder.CreateLoad(tuple_addr, "tuple_ptr");

  // Check if tuple* is null only if the tuple is nullable
  if (tuple_is_nullable_) {
    Value* tuple_is_null = builder.CreateIsNull(tuple_ptr, "tuple_is_null");
    // Check slot is null only if the null indicator bit is set
    if (null_indicator_offset_.bit_mask == 0) {
      builder.CreateCondBr(tuple_is_null, null_ret_block, get_slot_block);
    } else {
      builder.CreateCondBr(tuple_is_null, null_ret_block, 
          check_slot_null_indicator_block);
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
    builder.CreateCondBr(is_slot_null, null_ret_block, get_slot_block);
  }

  // Branch for slot != NULL
  builder.SetInsertPoint(get_slot_block);
  Value* slot_ptr = builder.CreateGEP(tuple_ptr, slot_offset, "slot_addr");
  Value* result = NULL;
  Value* slot_cast = builder.CreatePointerCast(slot_ptr, result_ptr_type);
  // For non-native types, we return a pointer to the struct
  if (type() == TYPE_STRING || type() == TYPE_TIMESTAMP) {
    result = slot_cast;
  } else {
    result = builder.CreateLoad(slot_cast, "slot_value");
  }
  CodegenSetIsNullArg(codegen, get_slot_block, false);
  builder.CreateBr(ret_block);

  // Branch to set is_null to true
  builder.SetInsertPoint(null_ret_block);
  CodegenSetIsNullArg(codegen, null_ret_block, true);
  builder.CreateBr(ret_block);

  // Ret block
  builder.SetInsertPoint(ret_block);
  PHINode* phi_node = builder.CreatePHI(return_type, 2, "tmp_phi");
  phi_node->addIncoming(GetNullReturnValue(codegen), null_ret_block);
  phi_node->addIncoming(result, get_slot_block);
  builder.CreateRet(phi_node);

  return codegen->FinalizeFunction(function);
}

}
