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

#include "runtime/string-value.h"

namespace impala {

// Used to force the compilation of the CodegenTypes struct.
void Tuple::dummy(Tuple::CodegenTypes*) {}

bool Tuple::CopyStrings(const char* err_ctx, RuntimeState* state,
    const SlotOffsets* string_slot_offsets, int num_string_slots, MemPool* pool,
    Status* status) noexcept {
  int64_t total_len = 0;
  for (int i = 0; i < num_string_slots; ++i) {
    if (IsNull(string_slot_offsets[i].null_indicator_offset)) continue;
    StringValue* sv = GetStringSlot(string_slot_offsets[i].tuple_offset);
    if (sv->IsSmall()) continue;
    total_len += sv->Len();
  }
  char* buf = AllocateStrings(err_ctx, state, total_len, pool, status);
  if (UNLIKELY(buf == nullptr)) return false;
  for (int i = 0; i < num_string_slots; ++i) {
    if (IsNull(string_slot_offsets[i].null_indicator_offset)) continue;
    StringValue* sv = GetStringSlot(string_slot_offsets[i].tuple_offset);
    if (sv->IsSmall()) continue;
    StringValue::SimpleString s = sv->ToSimpleString();
    if (s.len == 0) continue;
    memcpy(buf, s.ptr, s.len);
    sv->SetPtr(buf);
    buf += s.len;
  }
  return true;
}

template<class T>
bool TryMemCopy(uint8_t* dst, const uint8_t* dst_end, int size, const T* src) {
  if (UNLIKELY(dst + size > dst_end)) return false;
  memcpy(dst, src, size);
  return true;
}

bool Tuple::TryDeepCopy(uint8_t** dst_start, const uint8_t* dst_end, int* offset_start,
    const TupleDescriptor& desc, bool convert_ptrs) const {
  uint8_t* dst = *dst_start;
  int offset = *offset_start;
  if (!TryMemCopy(dst, dst_end, desc.byte_size(), this)) return false;
  Tuple* dst_tuple = reinterpret_cast<Tuple*>(dst);
  dst += desc.byte_size();
  offset += desc.byte_size();
  if (!dst_tuple->TryDeepCopyStrings(&dst, dst_end, &offset, desc, convert_ptrs)) {
    return false;
  }
  if (!dst_tuple->TryDeepCopyCollections(&dst, dst_end, &offset, desc, convert_ptrs)) {
    return false;
  }
  *dst_start = dst;
  *offset_start = offset;
  return true;
}

bool Tuple::TryDeepCopyStrings(uint8_t** data, const uint8_t* data_end, int* offset,
   const TupleDescriptor& desc, bool convert_ptrs) {
  vector<SlotDescriptor*>::const_iterator slot = desc.string_slots().begin();
  for (; slot != desc.string_slots().end(); ++slot) {
    DCHECK((*slot)->type().IsVarLenStringType());
    if (IsNull((*slot)->null_indicator_offset())) continue;

    StringValue* string_v = GetStringSlot((*slot)->tuple_offset());
    // It is safe to smallify at this point as DeepCopyVarlenData is called on the new
    // tuple which can be modified.
    if (string_v->Smallify()) continue;
    int len = string_v->Len();
    DCHECK_GT(len, 0); // Size 0 should be handled by "smallify" case.
    if (!TryMemCopy(*data, data_end, len, string_v->Ptr())) return false;
    char* new_ptr = convert_ptrs ? reinterpret_cast<char*>(*offset) :
                                   reinterpret_cast<char*>(*data);
    string_v->SetPtr(new_ptr);
    *data += len;
    *offset += len;
  }
  return true;
}

bool Tuple::TryDeepCopyCollections(uint8_t** data, const uint8_t* data_end, int* offset,
    const TupleDescriptor& desc, bool convert_ptrs) {
  vector<SlotDescriptor*>::const_iterator slot = desc.collection_slots().begin();
  for (; slot != desc.collection_slots().end(); ++slot) {
    DCHECK((*slot)->type().IsCollectionType());
    if (IsNull((*slot)->null_indicator_offset())) continue;

    CollectionValue* coll_value = GetCollectionSlot((*slot)->tuple_offset());
    const TupleDescriptor& item_desc = *(*slot)->children_tuple_descriptor();
    int coll_byte_size = coll_value->num_tuples * item_desc.byte_size();
    if (!TryMemCopy(*data, data_end, coll_byte_size, coll_value->ptr)) return false;
    uint8_t* coll_data = reinterpret_cast<uint8_t*>(*data);

    coll_value->ptr = convert_ptrs ? reinterpret_cast<uint8_t*>(*offset) : coll_data;

    *data += coll_byte_size;
    *offset += coll_byte_size;

    // Copy per-tuple varlen data if necessary.
    if (!item_desc.HasVarlenSlots()) continue;
    for (int i = 0; i < coll_value->num_tuples; ++i) {
      Tuple* dst_item = reinterpret_cast<Tuple*>(coll_data);
      if(!dst_item->TryDeepCopyStrings(data, data_end, offset, item_desc, convert_ptrs)) {
        return false;
      }
      if(!dst_item->TryDeepCopyCollections(
          data, data_end, offset, item_desc, convert_ptrs)) {
        return false;
      }
      coll_data += item_desc.byte_size();
    }
  }
  return true;
}

}
