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

#include "runtime/tuple.h"

#include <vector>

#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/array-value.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/tuple-row.h"
#include "runtime/string-value.h"
#include "util/debug-util.h"

#include "common/names.h"

namespace impala {

  const char* Tuple::LLVM_CLASS_NAME = "class.impala::Tuple";

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
    const ArrayValue* array_val = GetCollectionSlot((*slot)->tuple_offset());
    uint8_t* array_data = array_val->ptr;
    const TupleDescriptor& item_desc = *(*slot)->collection_item_descriptor();
    for (int i = 0; i < array_val->num_tuples; ++i) {
      result += reinterpret_cast<Tuple*>(array_data)->TotalByteSize(item_desc);
      array_data += item_desc.byte_size();
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
  dst->CopyVarlenData(desc, pool);
}

void Tuple::CopyVarlenData(const TupleDescriptor& desc, MemPool* pool) {
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
    ArrayValue* av = GetCollectionSlot((*slot)->tuple_offset());
    const TupleDescriptor* item_desc = (*slot)->collection_item_descriptor();
    int array_byte_size = av->num_tuples * item_desc->byte_size();
    uint8_t* array_data = reinterpret_cast<uint8_t*>(pool->Allocate(array_byte_size));
    memcpy(array_data, av->ptr, array_byte_size);
    if (!item_desc->HasVarlenSlots()) continue;
    for (int i = 0; i < av->num_tuples; ++i) {
      int item_offset = i * item_desc->byte_size();
      Tuple* dst_item = reinterpret_cast<Tuple*>(array_data + item_offset);
      dst_item->CopyVarlenData(*item_desc, pool);
    }
  }
}

void Tuple::DeepCopy(const TupleDescriptor& desc, char** data, int* offset,
                     bool convert_ptrs) {
  Tuple* dst = reinterpret_cast<Tuple*>(*data);
  memcpy(dst, this, desc.byte_size());
  *data += desc.byte_size();
  *offset += desc.byte_size();
  if (desc.HasVarlenSlots()) dst->CopyVarlenData(desc, data, offset, convert_ptrs);
}

void Tuple::CopyVarlenData(const TupleDescriptor& desc, char** data, int* offset,
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

    ArrayValue* array_val = GetCollectionSlot((*slot)->tuple_offset());
    const TupleDescriptor& item_desc = *(*slot)->collection_item_descriptor();
    int array_byte_size = array_val->num_tuples * item_desc.byte_size();
    memcpy(*data, array_val->ptr, array_byte_size);
    uint8_t* array_data = reinterpret_cast<uint8_t*>(*data);

    array_val->ptr = convert_ptrs ? reinterpret_cast<uint8_t*>(*offset) : array_data;

    *data += array_byte_size;
    *offset += array_byte_size;

    // Copy per-tuple varlen data if necessary.
    if (!item_desc.HasVarlenSlots()) continue;
    for (int i = 0; i < array_val->num_tuples; ++i) {
      reinterpret_cast<Tuple*>(array_data)->CopyVarlenData(
          item_desc, data, offset, convert_ptrs);
      array_data += item_desc.byte_size();
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

    ArrayValue* array_val = GetCollectionSlot((*slot)->tuple_offset());
    int offset = reinterpret_cast<intptr_t>(array_val->ptr);
    array_val->ptr = tuple_data + offset;

    uint8_t* array_data = array_val->ptr;
    const TupleDescriptor& item_desc = *(*slot)->collection_item_descriptor();
    for (int i = 0; i < array_val->num_tuples; ++i) {
      reinterpret_cast<Tuple*>(array_data)->ConvertOffsetsToPointers(
          item_desc, tuple_data);
      array_data += item_desc.byte_size();
    }
  }
}

template <bool collect_string_vals>
void Tuple::MaterializeExprs(
    TupleRow* row, const TupleDescriptor& desc,
    const vector<ExprContext*>& materialize_expr_ctxs, MemPool* pool,
    vector<StringValue*>* non_null_var_len_values, int* total_var_len) {
  if (collect_string_vals) {
    non_null_var_len_values->clear();
    *total_var_len = 0;
  }
  memset(this, 0, desc.num_null_bytes());
  // Evaluate the output_slot_exprs and place the results in the tuples.
  int mat_expr_index = 0;
  for (int i = 0; i < desc.slots().size(); ++i) {
    SlotDescriptor* slot_desc = desc.slots()[i];
    if (!slot_desc->is_materialized()) continue;
    // The FE ensures we don't get any TYPE_NULL expressions by picking an arbitrary type
    // when necessary, but does not do this for slot descs.
    // TODO: revisit this logic in the FE
    DCHECK(slot_desc->type().type == TYPE_NULL ||
           slot_desc->type() == materialize_expr_ctxs[mat_expr_index]->root()->type());
    void* src = materialize_expr_ctxs[mat_expr_index]->GetValue(row);
    if (src != NULL) {
      void* dst = GetSlot(slot_desc->tuple_offset());
      RawValue::Write(src, dst, slot_desc->type(), pool);
      if (collect_string_vals && slot_desc->type().IsVarLenStringType()) {
        StringValue* string_val = reinterpret_cast<StringValue*>(dst);
        non_null_var_len_values->push_back(string_val);
        *total_var_len += string_val->len;
      }
    } else {
      SetNull(slot_desc->null_indicator_offset());
    }
    ++mat_expr_index;
  }

  DCHECK_EQ(mat_expr_index, materialize_expr_ctxs.size());
}

template void Tuple::MaterializeExprs<false>(TupleRow* row, const TupleDescriptor& desc,
    const vector<ExprContext*>& materialize_expr_ctxs, MemPool* pool,
    vector<StringValue*>* non_null_var_values, int* total_var_len);

template void Tuple::MaterializeExprs<true>(TupleRow* row, const TupleDescriptor& desc,
    const vector<ExprContext*>& materialize_expr_ctxs, MemPool* pool,
    vector<StringValue*>* non_null_var_values, int* total_var_len);
}
