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

#include "runtime/tuple.h"

#include <vector>
#include "llvm/IR/Function.h"

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/collection-value.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using namespace llvm;

namespace impala {

const char* Tuple::LLVM_CLASS_NAME = "class.impala::Tuple";

const char* Tuple::MATERIALIZE_EXPRS_SYMBOL = "MaterializeExprsILb0ELb0";
const char* Tuple::MATERIALIZE_EXPRS_NULL_POOL_SYMBOL = "MaterializeExprsILb0ELb1";

int64_t Tuple::TotalByteSize(const TupleDescriptor& desc) const {
  int64_t result = desc.byte_size();
  if (!desc.HasVarlenSlots()) return result;
  result += VarlenByteSize(desc);
  return result;
}

int64_t Tuple::VarlenByteSize(const TupleDescriptor& desc) const {
  int64_t result = 0;
  vector<SlotDescriptor*>::const_iterator slot = desc.string_slots().begin();
  for (; slot != desc.string_slots().end(); ++slot) {
    DCHECK((*slot)->type().IsVarLenStringType());
    if (IsNull((*slot)->null_indicator_offset())) continue;
    const StringValue* string_val = GetStringSlot((*slot)->tuple_offset());
    result += string_val->len;
  }

  slot = desc.collection_slots().begin();
  for (; slot != desc.collection_slots().end(); ++slot) {
    DCHECK((*slot)->type().IsCollectionType());
    if (IsNull((*slot)->null_indicator_offset())) continue;
    const CollectionValue* coll_value = GetCollectionSlot((*slot)->tuple_offset());
    uint8_t* coll_data = coll_value->ptr;
    const TupleDescriptor& item_desc = *(*slot)->collection_item_descriptor();
    for (int i = 0; i < coll_value->num_tuples; ++i) {
      result += reinterpret_cast<Tuple*>(coll_data)->TotalByteSize(item_desc);
      coll_data += item_desc.byte_size();
    }
  }
  return result;
}

Tuple* Tuple::DeepCopy(const TupleDescriptor& desc, MemPool* pool) {
  Tuple* result = reinterpret_cast<Tuple*>(pool->Allocate(desc.byte_size()));
  DeepCopy(result, desc, pool);
  return result;
}

// TODO: the logic is very similar to the other DeepCopy implementation aside from how
// memory is allocated - can we templatise it somehow to avoid redundancy without runtime
// overhead.
void Tuple::DeepCopy(Tuple* dst, const TupleDescriptor& desc, MemPool* pool) {
  memcpy(dst, this, desc.byte_size());
  if (desc.HasVarlenSlots()) dst->DeepCopyVarlenData(desc, pool);
}

void Tuple::DeepCopyVarlenData(const TupleDescriptor& desc, MemPool* pool) {
  // allocate then copy all non-null string and collection slots
  for (vector<SlotDescriptor*>::const_iterator slot = desc.string_slots().begin();
       slot != desc.string_slots().end(); ++slot) {
    DCHECK((*slot)->type().IsVarLenStringType());
    if (IsNull((*slot)->null_indicator_offset())) continue;
    StringValue* string_v = GetStringSlot((*slot)->tuple_offset());
    char* string_copy = reinterpret_cast<char*>(pool->Allocate(string_v->len));
    memcpy(string_copy, string_v->ptr, string_v->len);
    string_v->ptr = string_copy;
  }

  for (vector<SlotDescriptor*>::const_iterator slot = desc.collection_slots().begin();
       slot != desc.collection_slots().end(); ++slot) {
    DCHECK((*slot)->type().IsCollectionType());
    if (IsNull((*slot)->null_indicator_offset())) continue;
    CollectionValue* cv = GetCollectionSlot((*slot)->tuple_offset());
    const TupleDescriptor* item_desc = (*slot)->collection_item_descriptor();
    int coll_byte_size = cv->num_tuples * item_desc->byte_size();
    uint8_t* coll_data = reinterpret_cast<uint8_t*>(pool->Allocate(coll_byte_size));
    memcpy(coll_data, cv->ptr, coll_byte_size);
    cv->ptr = coll_data;
    if (!item_desc->HasVarlenSlots()) continue;

    for (int i = 0; i < cv->num_tuples; ++i) {
      int item_offset = i * item_desc->byte_size();
      Tuple* dst_item = reinterpret_cast<Tuple*>(coll_data + item_offset);
      dst_item->DeepCopyVarlenData(*item_desc, pool);
    }
  }
}

void Tuple::DeepCopy(const TupleDescriptor& desc, char** data, int* offset,
                     bool convert_ptrs) {
  Tuple* dst = reinterpret_cast<Tuple*>(*data);
  memcpy(dst, this, desc.byte_size());
  *data += desc.byte_size();
  *offset += desc.byte_size();
  if (desc.HasVarlenSlots()) dst->DeepCopyVarlenData(desc, data, offset, convert_ptrs);
}

void Tuple::DeepCopyVarlenData(const TupleDescriptor& desc, char** data, int* offset,
    bool convert_ptrs) {
  vector<SlotDescriptor*>::const_iterator slot = desc.string_slots().begin();
  for (; slot != desc.string_slots().end(); ++slot) {
    DCHECK((*slot)->type().IsVarLenStringType());
    if (IsNull((*slot)->null_indicator_offset())) continue;

    StringValue* string_v = GetStringSlot((*slot)->tuple_offset());
    memcpy(*data, string_v->ptr, string_v->len);
    string_v->ptr = convert_ptrs ? reinterpret_cast<char*>(*offset) : *data;
    *data += string_v->len;
    *offset += string_v->len;
  }

  slot = desc.collection_slots().begin();
  for (; slot != desc.collection_slots().end(); ++slot) {
    DCHECK((*slot)->type().IsCollectionType());
    if (IsNull((*slot)->null_indicator_offset())) continue;

    CollectionValue* coll_value = GetCollectionSlot((*slot)->tuple_offset());
    const TupleDescriptor& item_desc = *(*slot)->collection_item_descriptor();
    int coll_byte_size = coll_value->num_tuples * item_desc.byte_size();
    memcpy(*data, coll_value->ptr, coll_byte_size);
    uint8_t* coll_data = reinterpret_cast<uint8_t*>(*data);

    coll_value->ptr = convert_ptrs ? reinterpret_cast<uint8_t*>(*offset) : coll_data;

    *data += coll_byte_size;
    *offset += coll_byte_size;

    // Copy per-tuple varlen data if necessary.
    if (!item_desc.HasVarlenSlots()) continue;
    for (int i = 0; i < coll_value->num_tuples; ++i) {
      reinterpret_cast<Tuple*>(coll_data)->DeepCopyVarlenData(
          item_desc, data, offset, convert_ptrs);
      coll_data += item_desc.byte_size();
    }
  }
}

void Tuple::ConvertOffsetsToPointers(const TupleDescriptor& desc, uint8_t* tuple_data) {
  vector<SlotDescriptor*>::const_iterator slot = desc.string_slots().begin();
  for (; slot != desc.string_slots().end(); ++slot) {
    DCHECK((*slot)->type().IsVarLenStringType());
    if (IsNull((*slot)->null_indicator_offset())) continue;

    StringValue* string_val = GetStringSlot((*slot)->tuple_offset());
    int offset = reinterpret_cast<intptr_t>(string_val->ptr);
    string_val->ptr = reinterpret_cast<char*>(tuple_data + offset);
  }

  slot = desc.collection_slots().begin();
  for (; slot != desc.collection_slots().end(); ++slot) {
    DCHECK((*slot)->type().IsCollectionType());
    if (IsNull((*slot)->null_indicator_offset())) continue;

    CollectionValue* coll_value = GetCollectionSlot((*slot)->tuple_offset());
    int offset = reinterpret_cast<intptr_t>(coll_value->ptr);
    coll_value->ptr = tuple_data + offset;

    uint8_t* coll_data = coll_value->ptr;
    const TupleDescriptor& item_desc = *(*slot)->collection_item_descriptor();
    for (int i = 0; i < coll_value->num_tuples; ++i) {
      reinterpret_cast<Tuple*>(coll_data)->ConvertOffsetsToPointers(
          item_desc, tuple_data);
      coll_data += item_desc.byte_size();
    }
  }
}

template <bool COLLECT_STRING_VALS, bool NO_POOL>
void Tuple::MaterializeExprs(TupleRow* row, const TupleDescriptor& desc,
    ScalarExprEvaluator* const* evals, MemPool* pool,
    StringValue** non_null_string_values, int* total_string_lengths,
    int* num_non_null_string_values) {
  ClearNullBits(desc);
  // Evaluate the materialize_expr_evals and place the results in the tuple.
  for (int i = 0; i < desc.slots().size(); ++i) {
    SlotDescriptor* slot_desc = desc.slots()[i];
    // The FE ensures we don't get any TYPE_NULL expressions by picking an arbitrary type
    // when necessary, but does not do this for slot descs.
    // TODO: revisit this logic in the FE
    DCHECK(slot_desc->type().type == TYPE_NULL ||
        slot_desc->type() == evals[i]->root().type());
    void* src = evals[i]->GetValue(row);
    if (src != NULL) {
      void* dst = GetSlot(slot_desc->tuple_offset());
      RawValue::Write(src, dst, slot_desc->type(), pool);
      if (COLLECT_STRING_VALS && slot_desc->type().IsVarLenStringType()) {
        StringValue* string_val = reinterpret_cast<StringValue*>(dst);
        *(non_null_string_values++) = string_val;
        *total_string_lengths += string_val->len;
        ++(*num_non_null_string_values);
      }
    } else {
      SetNull(slot_desc->null_indicator_offset());
    }
  }
}

// Codegens an unrolled version of MaterializeExprs(). Uses codegen'd exprs and slot
// writes. If 'pool' is non-NULL, string data is copied into it. Note that the generated
// function ignores its 'pool' arg; instead we hardcode the pointer in the IR.
//
// Example IR for materializing a string column with non-NULL 'pool':
//
// ; Function Attrs: alwaysinline
// define void @MaterializeExprs(%"class.impala::Tuple"* %opaque_tuple,
//     %"class.impala::TupleRow"* %row, %"class.impala::TupleDescriptor"* %desc,
//     %"class.impala::ScalarExprEvaluator"** %materialize_expr_evals,
//     %"class.impala::MemPool"* %pool,
//     %"struct.impala::StringValue"** %non_null_string_values,
//     i32* %total_string_lengths, i32* %num_non_null_string_values) #34 {
// entry:
//   %tuple = bitcast %"class.impala::Tuple"* %opaque_tuple to
//       <{ %"struct.impala::StringValue", i8 }>*
//   %int8_ptr = bitcast <{ %"struct.impala::StringValue", i8 }>* %tuple to i8*
//   %null_bytes_ptr = getelementptr inbounds i8, i8* %int8_ptr, i32 16
//   call void @llvm.memset.p0i8.i64(i8* %null_bytes_ptr, i8 0, i64 1, i32 0, i1 false)
//   %0 = getelementptr %"class.impala::ExprContext"*,
//       %"class.impala::ExprContext"** %materialize_expr_ctxs, i32 0
//   %expr_ctx = load %"class.impala::ExprContext"*, %"class.impala::ExprContext"** %0
//   %src = call { i64, i8* } @"impala::StringFunctions::UpperWrapper"(
//        %"class.impala::ExprContext"* %expr_ctx, %"class.impala::TupleRow"* %row)
//   %1 = extractvalue { i64, i8* } %src, 0
//   ; ----- generated by CodegenAnyVal::WriteToSlot() ----------------------------------
//   %is_null = trunc i64 %1 to i1
//   br i1 %is_null, label %null, label %non_null
//
// non_null:                                         ; preds = %entry
//   %slot = getelementptr inbounds <{ %"struct.impala::StringValue", i8 }>,
//       <{ %"struct.impala::StringValue", i8 }>* %tuple, i32 0, i32 0
//   %2 = extractvalue { i64, i8* } %src, 0
//   %3 = ashr i64 %2, 32
//   %4 = trunc i64 %3 to i32
//   %5 = insertvalue %"struct.impala::StringValue" zeroinitializer, i32 %4, 1
//   %6 = sext i32 %4 to i64
//   %new_ptr = call i8* @_ZN6impala7MemPool8AllocateILb0EEEPhli(
//       %"class.impala::MemPool"* %pool, i64 %6, i32 8)
//   %src1 = extractvalue { i64, i8* } %src, 1
//   call void @llvm.memcpy.p0i8.p0i8.i32(
//       i8* %new_ptr, i8* %src1, i32 %4, i32 0, i1 false)
//   %7 = insertvalue %"struct.impala::StringValue" %5, i8* %new_ptr, 0
//   store %"struct.impala::StringValue" %7, %"struct.impala::StringValue"* %slot
//   br label %end_write
//
// null:                                             ; preds = %entry
//   %8 = bitcast <{ %"struct.impala::StringValue", i8 }>* %tuple to i8*
//   %null_byte_ptr = getelementptr inbounds i8, i8* %8, i32 16
//   %null_byte = load i8, i8* %null_byte_ptr
//   %null_bit_set = or i8 %null_byte, 1
//   store i8 %null_bit_set, i8* %null_byte_ptr
//   br label %end_write
//
// end_write:                                        ; preds = %null, %non_null
//   ; ----- end CodegenAnyVal::WriteToSlot() -------------------------------------------
//   ret void
// }
Status Tuple::CodegenMaterializeExprs(LlvmCodeGen* codegen, bool collect_string_vals,
    const TupleDescriptor& desc, const vector<ScalarExpr*>& slot_materialize_exprs,
    bool use_mem_pool, Function** fn) {
  // Only support 'collect_string_vals' == false for now.
  if (collect_string_vals) {
    return Status("CodegenMaterializeExprs() collect_string_vals == true NYI");
  }
  SCOPED_TIMER(codegen->codegen_timer());
  LLVMContext& context = codegen->context();

  // Codegen each compute function from slot_materialize_exprs
  Function* materialize_expr_fns[slot_materialize_exprs.size()];
  for (int i = 0; i < slot_materialize_exprs.size(); ++i) {
    Status status = slot_materialize_exprs[i]->GetCodegendComputeFn(codegen,
        &materialize_expr_fns[i]);
    if (!status.ok()) {
      stringstream ss;
      ss << "Could not codegen CodegenMaterializeExprs: " << status.GetDetail();
      return Status(ss.str());
    }
  }

  // Construct function signature (this must exactly match the actual signature since it's
  // used in xcompiled IR). With 'pool':
  // void MaterializeExprs(Tuple* tuple, TupleRow* row, TupleDescriptor* desc,
  //     ScalarExprEvaluator** slot_materialize_exprs, MemPool* pool,
  //     StringValue** non_null_string_values, int* total_string_lengths)
  PointerType* opaque_tuple_type = codegen->GetPtrType(Tuple::LLVM_CLASS_NAME);
  PointerType* row_type = codegen->GetPtrType(TupleRow::LLVM_CLASS_NAME);
  PointerType* desc_type = codegen->GetPtrType(TupleDescriptor::LLVM_CLASS_NAME);
  PointerType* expr_evals_type =
      codegen->GetPtrType(codegen->GetPtrType(ScalarExprEvaluator::LLVM_CLASS_NAME));
  PointerType* pool_type = codegen->GetPtrType(MemPool::LLVM_CLASS_NAME);
  PointerType* string_values_type =
      codegen->GetPtrType(codegen->GetPtrType(StringValue::LLVM_CLASS_NAME));
  PointerType* int_ptr_type = codegen->GetPtrType(TYPE_INT);
  LlvmCodeGen::FnPrototype prototype(codegen, "MaterializeExprs", codegen->void_type());
  prototype.AddArgument("opaque_tuple", opaque_tuple_type);
  prototype.AddArgument("row", row_type);
  prototype.AddArgument("desc", desc_type);
  prototype.AddArgument("slot_materialize_exprs", expr_evals_type);
  prototype.AddArgument("pool", pool_type);
  prototype.AddArgument("non_null_string_values", string_values_type);
  prototype.AddArgument("total_string_lengths", int_ptr_type);
  prototype.AddArgument("num_non_null_string_values", int_ptr_type);

  LlvmBuilder builder(context);
  Value* args[8];
  *fn = prototype.GeneratePrototype(&builder, args);
  Value* opaque_tuple_arg = args[0];
  Value* row_arg = args[1];
  // Value* desc_arg = args[2]; // unused
  Value* expr_evals_arg = args[3];
  Value* pool_arg = args[4];
  // The followings arguments are unused as 'collect_string_vals' is false.
  // Value* non_null_string_values_arg = args[5]; // unused
  // Value* total_string_lengths_arg = args[6]; // unused
  // Value* num_non_null_string_values_arg = args[7]; // unused

  // Cast the opaque Tuple* argument to the generated struct type
  Type* tuple_struct_type = desc.GetLlvmStruct(codegen);
  if (tuple_struct_type == NULL) {
    return Status("CodegenMaterializeExprs(): failed to generate tuple desc");
  }
  PointerType* tuple_type = codegen->GetPtrType(tuple_struct_type);
  Value* tuple = builder.CreateBitCast(opaque_tuple_arg, tuple_type, "tuple");

  // Clear tuple's null bytes
  codegen->CodegenClearNullBits(&builder, tuple, desc);

  // Evaluate the slot_materialize_exprs and place the results in the tuple.
  for (int i = 0; i < desc.slots().size(); ++i) {
    SlotDescriptor* slot_desc = desc.slots()[i];
    DCHECK(slot_desc->type().type == TYPE_NULL ||
        slot_desc->type() == slot_materialize_exprs[i]->type());

    // Call materialize_expr_fns[i](slot_materialize_exprs[i], row)
    Value* expr_eval =
        codegen->CodegenArrayAt(&builder, expr_evals_arg, i, "expr_eval");
    Value* expr_args[] = { expr_eval, row_arg };
    CodegenAnyVal src = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
        slot_materialize_exprs[i]->type(), materialize_expr_fns[i], expr_args, "src");

    // Write expr result 'src' to slot
    src.WriteToSlot(*slot_desc, tuple, use_mem_pool ? pool_arg : nullptr);
  }
  builder.CreateRetVoid();
  // TODO: if pool != NULL, OptimizeFunctionWithExprs() is inlining the Allocate()
  // call. Investigate if this is a good thing.
  *fn = codegen->FinalizeFunction(*fn);
  return Status::OK();
}

template void Tuple::MaterializeExprs<false, false>(TupleRow*, const TupleDescriptor&,
    ScalarExprEvaluator* const*, MemPool*, StringValue**, int*, int*);
template void Tuple::MaterializeExprs<false, true>(TupleRow*, const TupleDescriptor&,
    ScalarExprEvaluator* const*, MemPool*, StringValue**, int*, int*);
template void Tuple::MaterializeExprs<true, false>(TupleRow*, const TupleDescriptor&,
    ScalarExprEvaluator* const*, MemPool*, StringValue**, int*, int*);
template void Tuple::MaterializeExprs<true, true>(TupleRow*, const TupleDescriptor&,
    ScalarExprEvaluator* const*, MemPool*, StringValue**, int*, int*);
}
