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

#include <sstream>
#include <boost/functional/hash.hpp>

#include "kudu/gutil/strings/escaping.h"
#include "runtime/collection-value.h"
#include "runtime/date-value.h"
#include "runtime/raw-value.h"
#include "runtime/raw-value.inline.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple.h"
#include "udf/udf-internal.h"
#include "util/ubsan.h"

#include "common/names.h"

namespace impala {

using impala_udf::StructVal;

const int RawValue::ASCII_PRECISION;
constexpr double RawValue::CANONICAL_DOUBLE_NAN;
constexpr float RawValue::CANONICAL_FLOAT_NAN;
constexpr double RawValue::CANONICAL_DOUBLE_ZERO;
constexpr float RawValue::CANONICAL_FLOAT_ZERO;

void RawValue::PrintValueAsBytes(const void* value, const ColumnType& type,
                                 stringstream* stream) {
  if (value == NULL) return;

  const char* chars = reinterpret_cast<const char*>(value);
  const StringValue* string_val = NULL;
  switch (type.type) {
    case TYPE_BOOLEAN:
      stream->write(chars, sizeof(bool));
      return;
    case TYPE_TINYINT:
      stream->write(chars, sizeof(int8_t));
      break;
    case TYPE_SMALLINT:
      stream->write(chars, sizeof(int16_t));
      break;
    case TYPE_INT:
      stream->write(chars, sizeof(int32_t));
      break;
    case TYPE_DATE:
      stream->write(chars, sizeof(DateValue));
      break;
    case TYPE_BIGINT:
      stream->write(chars, sizeof(int64_t));
      break;
    case TYPE_FLOAT:
      stream->write(chars, sizeof(float));
      break;
    case TYPE_DOUBLE:
      stream->write(chars, sizeof(double));
      break;
    case TYPE_STRING:
    case TYPE_VARCHAR:
      string_val = reinterpret_cast<const StringValue*>(value);
      stream->write(string_val->Ptr(), string_val->Len());
      break;
    case TYPE_TIMESTAMP:
      stream->write(chars, TimestampValue::Size());
      break;
    case TYPE_CHAR:
      stream->write(chars, type.len);
      break;
    case TYPE_DECIMAL:
      stream->write(chars, type.GetByteSize());
      break;
    default:
      DCHECK(false) << "bad RawValue::PrintValue() type: " << type.DebugString();
  }
}

void RawValue::PrintValue(const void* value, const ColumnType& type, int scale,
                          string* str) {
  if (value == NULL) {
    *str = NullLiteral(true);
    return;
  }

  const StringValue* string_val = NULL;
  bool val;
  string tmp;

  // Special case types that we can print more efficiently without using a stringstream
  switch (type.type) {
    case TYPE_BOOLEAN:
      val = *reinterpret_cast<const bool*>(value);
      *str = (val ? "true" : "false");
      return;
    case TYPE_STRING:
    case TYPE_VARCHAR:
      string_val = reinterpret_cast<const StringValue*>(value);
      tmp.assign(string_val->Ptr(), string_val->Len());
      str->swap(tmp);
      return;
    case TYPE_CHAR:
      *str = string(reinterpret_cast<const char*>(value), type.len);
      return;
    case TYPE_TIMESTAMP:
      *str = reinterpret_cast<const TimestampValue*>(value)->ToString();
      return;
    case TYPE_DATE:
      *str = reinterpret_cast<const DateValue*>(value)->ToString();
      return;
    case TYPE_FIXED_UDA_INTERMEDIATE:
      *str = "Intermediate UDA step, no value printed";
      return;
    default:
      break;
  }

  stringstream out;
  out.precision(ASCII_PRECISION);

  PrintValue(value, type, scale, &out);

  *str = out.str();
}

void RawValue::WriteNonNullPrimitive(const void* value, void* dst, const ColumnType& type,
    MemPool* pool) {
  DCHECK(value != NULL);
  switch (type.type) {
    case TYPE_NULL:
      break;
    case TYPE_BOOLEAN:
      // Unlike the other scalar types, bool has a limited set of valid values, so if
      // 'dst' is uninitialized memory and happens to point to a value that is not a valid
      // bool, then dereferencing it via *reinterpret_cast<bool*>(dst) is undefined
      // behavior.
      memcpy(dst, value, sizeof(bool));
      break;
    case TYPE_TINYINT:
      *reinterpret_cast<int8_t*>(dst) = *reinterpret_cast<const int8_t*>(value);
      break;
    case TYPE_SMALLINT:
      *reinterpret_cast<int16_t*>(dst) = *reinterpret_cast<const int16_t*>(value);
      break;
    case TYPE_INT:
      *reinterpret_cast<int32_t*>(dst) = *reinterpret_cast<const int32_t*>(value);
      break;
    case TYPE_DATE:
      *reinterpret_cast<DateValue*>(dst) = *reinterpret_cast<const DateValue*>(value);
      break;
    case TYPE_BIGINT:
      *reinterpret_cast<int64_t*>(dst) = *reinterpret_cast<const int64_t*>(value);
      break;
    case TYPE_FLOAT:
      *reinterpret_cast<float*>(dst) = *reinterpret_cast<const float*>(value);
      break;
    case TYPE_DOUBLE:
      *reinterpret_cast<double*>(dst) = *reinterpret_cast<const double*>(value);
      break;
    case TYPE_TIMESTAMP:
      *reinterpret_cast<TimestampValue*>(dst) =
          *reinterpret_cast<const TimestampValue*>(value);
      break;
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      const StringValue* src = reinterpret_cast<const StringValue*>(value);
      StringValue* dest = reinterpret_cast<StringValue*>(dst);
      dest->Assign(*src);
      if (type.type == TYPE_VARCHAR) DCHECK_LE(dest->Len(), type.len);
      if (pool != NULL) {
        // Note: if this changes to TryAllocate(), SlotDescriptor::CodegenWriteToSlot()
        // will need to reflect this change as well (the codegen'd Allocate() call is
        // actually generated in SlotDescriptor::CodegenWriteStringOrCollectionToSlot()).
        dest->Assign(reinterpret_cast<char*>(pool->Allocate(dest->Len())), dest->Len());
        Ubsan::MemCpy(dest->Ptr(), src->Ptr(), dest->Len());
      }
      break;
    }
    case TYPE_CHAR:
      DCHECK_EQ(type.type, TYPE_CHAR);
      memcpy(dst, value, type.len);
      break;
    case TYPE_DECIMAL:
      memcpy(dst, value, type.GetByteSize());
      break;
    case TYPE_ARRAY:
    case TYPE_MAP: {
      // Collections should be handled by a different Write() function within this class.
      DCHECK(false);
      break;
    }
    case TYPE_STRUCT: {
      // Structs should be handled by a different Write() function within this class.
      DCHECK(false);
      break;
    }
    default:
      DCHECK(false) << "RawValue::WriteNonNullPrimitive(): bad type: "
          << type.DebugString();
  }
}

void RawValue::Write(const void* value, Tuple* tuple, const SlotDescriptor* slot_desc,
    MemPool* pool) {
  RawValue::Write<false>(value, tuple, slot_desc, pool, nullptr, nullptr);
}

template <bool COLLECT_VAR_LEN_VALS>
void RawValue::Write(const void* value, Tuple* tuple, const SlotDescriptor* slot_desc,
    MemPool* pool, std::vector<StringValue*>* string_values,
    std::vector<std::pair<CollectionValue*, int64_t>>* collection_values) {
  if (value == nullptr) {
    if (slot_desc->type().IsStructType()) {
      tuple->SetStructToNull(slot_desc);
    } else {
      tuple->SetNull(slot_desc->null_indicator_offset());
    }
  } else {
    RawValue::WriteNonNull<COLLECT_VAR_LEN_VALS>(value, tuple, slot_desc, pool,
        string_values, collection_values);
  }
}

template <bool COLLECT_VAR_LEN_VALS>
void RawValue::WriteNonNull(const void* value, Tuple* tuple,
    const SlotDescriptor* slot_desc, MemPool* pool,
    vector<StringValue*>* string_values,
    vector<pair<CollectionValue*, int64_t>>* collection_values) {
  DCHECK(value != nullptr && tuple != nullptr && slot_desc != nullptr);

  if (COLLECT_VAR_LEN_VALS) {
    DCHECK(string_values != nullptr);
    DCHECK(collection_values != nullptr);
  }

  if (slot_desc->type().IsStructType()) {
    WriteStruct<COLLECT_VAR_LEN_VALS>(value, tuple, slot_desc, pool,
        string_values, collection_values);
  } else if (slot_desc->type().IsCollectionType()) {
    WriteCollection<COLLECT_VAR_LEN_VALS>(value, tuple, slot_desc, pool,
        string_values, collection_values);
  } else {
    WritePrimitiveCollectVarlen<COLLECT_VAR_LEN_VALS>(value, tuple, slot_desc, pool,
        string_values);
  }
}

template <bool COLLECT_VAR_LEN_VALS>
void RawValue::WriteStruct(const void* value, Tuple* tuple,
    const SlotDescriptor* slot_desc, MemPool* pool, vector<StringValue*>* string_values,
    vector<pair<CollectionValue*, int64_t>>* collection_values) {
  DCHECK(tuple != nullptr);
  DCHECK(slot_desc->type().IsStructType());
  DCHECK(slot_desc->children_tuple_descriptor() != nullptr);
  if (value == nullptr) {
    tuple->SetStructToNull(slot_desc);
    return;
  }
  const StructVal* src = reinterpret_cast<const StructVal*>(value);
  const TupleDescriptor* children_tuple_desc = slot_desc->children_tuple_descriptor();
  DCHECK_EQ(src->num_children, children_tuple_desc->slots().size());

  for (int i = 0; i < src->num_children; ++i) {
    SlotDescriptor* child_slot = children_tuple_desc->slots()[i];
    uint8_t* src_child = src->ptr[i];
    // TODO IMPALA-12160: Handle collections in structs.
    if (child_slot->type().IsStructType()) {
      // Recursive call in case of nested structs.
      WriteStruct<COLLECT_VAR_LEN_VALS>(src_child, tuple, child_slot, pool,
          string_values, collection_values);
    } else if (src_child == nullptr) {
      tuple->SetNull(child_slot->null_indicator_offset());
    } else {
      WritePrimitiveCollectVarlen<COLLECT_VAR_LEN_VALS>(src_child, tuple, child_slot,
          pool, string_values);
    }
  }
}

template <bool COLLECT_VAR_LEN_VALS>
void RawValue::WriteCollection(const void* value, Tuple* tuple,
    const SlotDescriptor* slot_desc, MemPool* pool, vector<StringValue*>* string_values,
    vector<pair<CollectionValue*, int64_t>>* collection_values) {
  DCHECK(slot_desc->type().IsCollectionType());

  void* dst = tuple->GetSlot(slot_desc->tuple_offset());

  const CollectionValue* src = reinterpret_cast<const CollectionValue*>(value);
  CollectionValue* dest = reinterpret_cast<CollectionValue*>(dst);
  dest->num_tuples = src->num_tuples;

  int64_t byte_size = dest->ByteSize(*slot_desc->children_tuple_descriptor());
  if (pool != nullptr) {
    // If 'dest' and 'src' point to the same address, assigning the address of the newly
    // allocated buffer to 'dest->ptr' will also overwrite 'src->ptr', and the memcpy will
    // be from the destination to the destination.
    DCHECK_NE(dest, src);
    // Note: if this changes to TryAllocate(), SlotDescriptor::CodegenWriteToSlot() will
    // need to reflect this change as well (the codegen'd Allocate() call is actually
    // generated in SlotDescriptor::CodegenWriteStringOrCollectionToSlot()).
    dest->ptr = reinterpret_cast<uint8_t*>(pool->Allocate(byte_size));
    Ubsan::MemCpy(dest->ptr, src->ptr, byte_size);
  } else {
    dest->ptr = src->ptr;
  }

  // We only need to recurse if this is a deep copy (pool != nullptr) OR if we collect
  // var-len values.
  if (pool != nullptr || COLLECT_VAR_LEN_VALS) {
    WriteCollectionChildren<COLLECT_VAR_LEN_VALS>(*dest, *src, *slot_desc, pool,
        string_values, collection_values);
  }

  if (COLLECT_VAR_LEN_VALS) {
    DCHECK(string_values != nullptr);
    DCHECK(collection_values != nullptr);
    collection_values->push_back(std::make_pair(dest, byte_size));
  }
}

template <bool COLLECT_VAR_LEN_VALS>
void RawValue::WriteCollectionChildren(const CollectionValue& dest,
    const CollectionValue& src, const SlotDescriptor& collection_slot_desc, MemPool* pool,
    vector<StringValue*>* string_values,
    vector<pair<CollectionValue*, int64_t>>* collection_values) {
  DCHECK_EQ(src.num_tuples, dest.num_tuples);
  const TupleDescriptor* child_tuple_desc =
      collection_slot_desc.children_tuple_descriptor();
  DCHECK(child_tuple_desc != nullptr);

  for (int i = 0; i < dest.num_tuples; i++) {
    Tuple* child_src_tuple = reinterpret_cast<Tuple*>(
        src.ptr + i * child_tuple_desc->byte_size());
    Tuple* child_dest_tuple = reinterpret_cast<Tuple*>(
        dest.ptr + i * child_tuple_desc->byte_size());

    for (const SlotDescriptor* string_slot_desc : child_tuple_desc->string_slots()) {
      WriteCollectionVarlenChild<COLLECT_VAR_LEN_VALS>(child_dest_tuple, child_src_tuple,
          string_slot_desc, pool, string_values, collection_values);
    }

    for (const SlotDescriptor* collection_slot_desc
        : child_tuple_desc->collection_slots()) {
      WriteCollectionVarlenChild<COLLECT_VAR_LEN_VALS>(child_dest_tuple, child_src_tuple,
          collection_slot_desc, pool, string_values, collection_values);
    }
  }
}

template <bool COLLECT_VAR_LEN_VALS>
void RawValue::WriteCollectionVarlenChild(Tuple* child_dest_tuple, Tuple* child_src_tuple,
    const SlotDescriptor* slot_desc, MemPool* pool, vector<StringValue*>* string_values,
    vector<pair<CollectionValue*, int64_t>>* collection_values ) {
  DCHECK(slot_desc != nullptr);
  DCHECK(slot_desc->type().IsVarLenStringType() || slot_desc->type().IsCollectionType());

  if (!child_dest_tuple->IsNull(slot_desc->null_indicator_offset())) {
    // The fixed length part of the child (the pointer and the length / number of tuples)
    // is already in the destination tuple, copied there as the var-len data of the
    // parent. We continue the recursion for two things:
    //   1. deep-copying the var-len data of the child
    //   2. collecting var-len slots.
    // At least one of these is true, otherwise we never get here. The called recursive
    // function will once again set the length (always unnecessary) and the pointer
    // (unnecessary if we're not deep-copying, only collecting). This is not costly enough
    // to justify complicating the code. Note, however, that although at this point the
    // source and destination slots hold the same value (pointer and length / number of
    // tuples), we take 'child_value', the source in the recursive call, from the source
    // tuple, because in case of deep-copying, the pointer of the destination slot will be
    // re-assigned to the newly allocated buffer, and if we took 'child_value' from the
    // destination slot, the 'source' pointer and the 'destination' pointer would be the
    // same, meaning the 'source' pointer would also be overwritten before we copied the
    // data it pointed to.
    void* child_value = child_src_tuple->GetSlot(slot_desc->tuple_offset());

    WriteNonNull<COLLECT_VAR_LEN_VALS>(child_value, child_dest_tuple,
        slot_desc, pool, string_values, collection_values);
  }
}

template <bool COLLECT_VAR_LEN_VALS>
void RawValue::WritePrimitiveCollectVarlen(const void* value, Tuple* tuple,
    const SlotDescriptor* slot_desc, MemPool* pool, vector<StringValue*>* string_values) {
  DCHECK(value != nullptr && tuple != nullptr && slot_desc != nullptr);

  void* dst = tuple->GetSlot(slot_desc->tuple_offset());
  WriteNonNullPrimitive(value, dst, slot_desc->type(), pool);
  if constexpr (COLLECT_VAR_LEN_VALS) {
    DCHECK(string_values != nullptr);
    if (slot_desc->type().IsVarLenStringType()) {
      StringValue* str_value = reinterpret_cast<StringValue*>(dst);
      if (!str_value->IsSmall()) string_values->push_back(str_value);
    } else if (slot_desc->type().IsCollectionType()) {
      DCHECK(false) << "Collections should be handled in WriteCollection.";
    }
  }
}

void RawValue::PrintValue(
    const void* value, const ColumnType& type, int scale, std::stringstream* stream,
    bool quote_val) {
  if (value == NULL) {
    *stream << NullLiteral(true);
    return;
  }

  int old_precision = stream->precision();
  std::ios_base::fmtflags old_flags = stream->flags();
  if (scale > -1) {
    stream->precision(scale);
    // Setting 'fixed' causes precision to set the number of digits printed after the
    // decimal (by default it sets the maximum number of digits total).
    *stream << std::fixed;
  }

  const StringValue* string_val = NULL;
  switch (type.type) {
    case TYPE_BOOLEAN: {
      bool val = *reinterpret_cast<const bool*>(value);
      *stream << (val ? "true" : "false");
      break;
    }
    case TYPE_TINYINT:
      // Extra casting for chars since they should not be interpreted as ASCII.
      *stream << static_cast<int>(*reinterpret_cast<const int8_t*>(value));
      break;
    case TYPE_SMALLINT: *stream << *reinterpret_cast<const int16_t*>(value); break;
    case TYPE_INT: *stream << *reinterpret_cast<const int32_t*>(value); break;
    case TYPE_BIGINT: *stream << *reinterpret_cast<const int64_t*>(value); break;
    case TYPE_FLOAT: {
      float val = *reinterpret_cast<const float*>(value);
      if (LIKELY(std::isfinite(val))) {
        *stream << val;
      } else if (std::isinf(val)) {
        // 'Infinity' is Java's text representation of inf. By staying close to Java, we
        // allow Hive to read text tables containing non-finite values produced by
        // Impala. (The same logic applies to 'NaN', below).
        *stream << (val < 0 ? "-Infinity" : "Infinity");
      } else if (std::isnan(val)) {
        *stream << "NaN";
      }
    } break;
    case TYPE_DOUBLE: {
      double val = *reinterpret_cast<const double*>(value);
      if (LIKELY(std::isfinite(val))) {
        *stream << val;
      } else if (std::isinf(val)) {
        // See TYPE_FLOAT for rationale.
        *stream << (val < 0 ? "-Infinity" : "Infinity");
      } else if (std::isnan(val)) {
        *stream << "NaN";
      }
    } break;
    case TYPE_VARCHAR:
    case TYPE_STRING:
      string_val = reinterpret_cast<const StringValue*>(value);
      if (type.type == TYPE_VARCHAR) DCHECK(string_val->Len() <= type.len);
      if (quote_val) {
        string str(string_val->Ptr(), string_val->Len());
        str = strings::Utf8SafeCEscape(str);
        *stream << "\"";
        stream->write(str.c_str(), str.size());
        *stream << "\"";
      } else {
        stream->write(string_val->Ptr(), string_val->Len());
      }
      break;
    case TYPE_TIMESTAMP:
      if (quote_val) *stream << "\"";
      *stream << *reinterpret_cast<const TimestampValue*>(value);
      if (quote_val) *stream << "\"";
      break;
    case TYPE_CHAR:
      if (quote_val) {
        string str(reinterpret_cast<const char*>(value), type.len);
        str = strings::Utf8SafeCEscape(str);
        *stream << "\"";
        stream->write(str.c_str(), str.size());
        *stream << "\"";
      } else {
        stream->write(reinterpret_cast<const char*>(value), type.len);
      }
      break;
    case TYPE_DECIMAL:
      switch (type.GetByteSize()) {
        case 4:
          *stream << reinterpret_cast<const Decimal4Value*>(value)->ToString(type);
          break;
        case 8:
          *stream << reinterpret_cast<const Decimal8Value*>(value)->ToString(type);
          break;
        case 16:
          *stream << reinterpret_cast<const Decimal16Value*>(value)->ToString(type);
          break;
        default: DCHECK(false) << type;
      }
      break;
    case TYPE_DATE: {
      if (quote_val) *stream << "\"";
      *stream << *reinterpret_cast<const DateValue*>(value);
      if (quote_val) *stream << "\"";
    } break;
    default: DCHECK(false) << "Unknown type: " << type;
  }
  stream->precision(old_precision);
  // Undo setting stream to fixed
  stream->flags(old_flags);
}

template void RawValue::Write<true>(const void* value, Tuple* tuple,
    const SlotDescriptor* slot_desc, MemPool* pool,
    std::vector<StringValue*>* string_values,
    std::vector<std::pair<CollectionValue*, int64_t>>* collection_values);
template void RawValue::Write<false>(const void* value, Tuple* tuple,
    const SlotDescriptor* slot_desc, MemPool* pool,
    std::vector<StringValue*>* string_values,
    std::vector<std::pair<CollectionValue*, int64_t>>* collection_values);

template void RawValue::WriteNonNull<true>(const void* value, Tuple* tuple,
    const SlotDescriptor* slot_desc, MemPool* pool,
    std::vector<StringValue*>* string_values,
    std::vector<std::pair<CollectionValue*, int64_t>>* collection_values);
template void RawValue::WriteNonNull<false>(const void* value, Tuple* tuple,
    const SlotDescriptor* slot_desc, MemPool* pool,
    std::vector<StringValue*>* string_values,
    std::vector<std::pair<CollectionValue*, int64_t>>* collection_values);

template void RawValue::WriteStruct<true>(const void* value, Tuple* tuple,
    const SlotDescriptor* slot_desc, MemPool* pool,
    std::vector<StringValue*>* string_values,
    std::vector<std::pair<CollectionValue*, int64_t>>* collection_values);
template void RawValue::WriteStruct<false>(const void* value, Tuple* tuple,
    const SlotDescriptor* slot_desc, MemPool* pool,
    std::vector<StringValue*>* string_values,
    std::vector<std::pair<CollectionValue*, int64_t>>* collection_values);

template void RawValue::WritePrimitiveCollectVarlen<true>(const void* value,
    Tuple* tuple, const SlotDescriptor* slot_desc, MemPool* pool,
    std::vector<StringValue*>* string_values);
template void RawValue::WritePrimitiveCollectVarlen<false>(const void* value,
    Tuple* tuple,
    const SlotDescriptor* slot_desc, MemPool* pool,
    std::vector<StringValue*>* string_values);
}
