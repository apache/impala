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

namespace {

// Top level null values are printed as "NULL"; collections and structs are printed in
// JSON format, which requires "null".
constexpr const char* NullLiteral(bool top_level) {
  if (top_level) return "NULL";
  return "null";
}

}

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
      stream->write(string_val->ptr, string_val->len);
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

  stringstream out;
  out.precision(ASCII_PRECISION);
  const StringValue* string_val = NULL;
  string tmp;
  bool val;

  // Special case types that we can print more efficiently without using a stringstream
  switch (type.type) {
    case TYPE_BOOLEAN:
      val = *reinterpret_cast<const bool*>(value);
      *str = (val ? "true" : "false");
      return;
    case TYPE_STRING:
    case TYPE_VARCHAR:
      string_val = reinterpret_cast<const StringValue*>(value);
      tmp.assign(string_val->ptr, string_val->len);
      str->swap(tmp);
      return;
    case TYPE_CHAR:
      *str = string(reinterpret_cast<const char*>(value), type.len);
      return;
    case TYPE_FIXED_UDA_INTERMEDIATE:
      *str = "Intermediate UDA step, no value printed";
      return;
    default:
      PrintValue(value, type, scale, &out);
  }
  *str = out.str();
}

void RawValue::Write(const void* value, void* dst, const ColumnType& type,
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
      dest->len = src->len;
      if (type.type == TYPE_VARCHAR) DCHECK_LE(dest->len, type.len);
      if (pool != NULL) {
        // Note: if this changes to TryAllocate(), CodegenAnyVal::WriteToSlot() will need
        // to reflect this change as well (the codegen'd Allocate() call is actually
        // generated in CodegenAnyVal::StoreToNativePtr()).
        dest->ptr = reinterpret_cast<char*>(pool->Allocate(dest->len));
        Ubsan::MemCpy(dest->ptr, src->ptr, dest->len);
      } else {
        dest->ptr = src->ptr;
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
      DCHECK(pool == NULL) << "RawValue::Write(): deep copy of CollectionValues NYI";
      const CollectionValue* src = reinterpret_cast<const CollectionValue*>(value);
      CollectionValue* dest = reinterpret_cast<CollectionValue*>(dst);
      dest->num_tuples = src->num_tuples;
      dest->ptr = src->ptr;
      break;
    }
    case TYPE_STRUCT: {
      // Structs should be handled by a different Write() function within this class.
      DCHECK(false);
    }
    default:
      DCHECK(false) << "RawValue::Write(): bad type: " << type.DebugString();
  }
}

void RawValue::Write(const void* value, Tuple* tuple, const SlotDescriptor* slot_desc,
                     MemPool* pool) {
  if (value == NULL) {
    tuple->SetNull(slot_desc->null_indicator_offset());
  } else {
    void* slot = tuple->GetSlot(slot_desc->tuple_offset());
    RawValue::Write(value, slot, slot_desc->type(), pool);
  }
}

template <bool COLLECT_STRING_VALS>
void RawValue::Write(const void* value, Tuple* tuple,
    const SlotDescriptor* slot_desc, MemPool* pool,
    vector<StringValue*>* string_values) {
  DCHECK(value != nullptr && tuple != nullptr && slot_desc != nullptr &&
      string_values != nullptr);
  DCHECK(string_values->size() == 0);

  if (slot_desc->type().IsStructType()) {
    WriteStruct<COLLECT_STRING_VALS>(value, tuple, slot_desc, pool, string_values);
  } else {
    WritePrimitive<COLLECT_STRING_VALS>(value, tuple, slot_desc, pool, string_values);
  }
}

template <bool COLLECT_STRING_VALS>
void RawValue::WriteStruct(const void* value, Tuple* tuple,
    const SlotDescriptor* slot_desc, MemPool* pool,
    vector<StringValue*>* string_values) {
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
    if (child_slot->type().IsStructType()) {
      // Recursive call in case of nested structs.
      WriteStruct<COLLECT_STRING_VALS>(src_child, tuple, child_slot, pool,
          string_values);
      continue;
    }
    if (src_child == nullptr) {
      tuple->SetNull(child_slot->null_indicator_offset());
    } else {
      WritePrimitive<COLLECT_STRING_VALS>(src_child, tuple, child_slot, pool,
          string_values);
    }
  }
}

template <bool COLLECT_STRING_VALS>
void RawValue::WritePrimitive(const void* value, Tuple* tuple,
    const SlotDescriptor* slot_desc, MemPool* pool,
    vector<StringValue*>* string_values) {
  DCHECK(value != nullptr && tuple != nullptr && slot_desc != nullptr &&
      string_values != nullptr);

  void* dst = tuple->GetSlot(slot_desc->tuple_offset());
  Write(value, dst, slot_desc->type(), pool);
  if (COLLECT_STRING_VALS && slot_desc->type().IsVarLenStringType()) {
    string_values->push_back(reinterpret_cast<StringValue*>(dst));
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
      return;
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
      if (type.type == TYPE_VARCHAR) DCHECK(string_val->len <= type.len);
      if (quote_val) {
        string str(string_val->ptr, string_val->len);
        str = strings::Utf8SafeCEscape(str);
        *stream << "\"";
        stream->write(str.c_str(), str.size());
        *stream << "\"";
      } else {
        stream->write(string_val->ptr, string_val->len);
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
    std::vector<StringValue*>* string_values);
template void RawValue::Write<false>(const void* value, Tuple* tuple,
    const SlotDescriptor* slot_desc, MemPool* pool,
    std::vector<StringValue*>* string_values);

template void RawValue::WriteStruct<true>(const void* value, Tuple* tuple,
      const SlotDescriptor* slot_desc, MemPool* pool,
      std::vector<StringValue*>* string_values);
template void RawValue::WriteStruct<false>(const void* value, Tuple* tuple,
      const SlotDescriptor* slot_desc, MemPool* pool,
      std::vector<StringValue*>* string_values);

template void RawValue::WritePrimitive<true>(const void* value, Tuple* tuple,
      const SlotDescriptor* slot_desc, MemPool* pool,
      std::vector<StringValue*>* string_values);
template void RawValue::WritePrimitive<false>(const void* value, Tuple* tuple,
      const SlotDescriptor* slot_desc, MemPool* pool,
      std::vector<StringValue*>* string_values);

bool PrintNestedValueIfNull(const SlotDescriptor& slot_desc, Tuple* item,
    stringstream* stream) {
  bool is_null = item->IsNull(slot_desc.null_indicator_offset());
  if (is_null) *stream << NullLiteral(false);
  return is_null;
}

void PrintNonNullNestedCollection(const SlotDescriptor& slot_desc, Tuple* item, int scale,
    stringstream* stream) {
  const CollectionValue* nested_collection_val =
      item->GetCollectionSlot(slot_desc.tuple_offset());
  DCHECK(nested_collection_val != nullptr);
  const TupleDescriptor* child_item_tuple_desc =
      slot_desc.children_tuple_descriptor();
  DCHECK(child_item_tuple_desc != nullptr);
  RawValue::PrintCollectionValue(nested_collection_val, child_item_tuple_desc, scale,
      stream, slot_desc.type().IsMapType());
}

void PrintNonNullNestedPrimitive(const SlotDescriptor& slot_desc, Tuple* item, int scale,
    stringstream* stream) {
  RawValue::PrintValue(item->GetSlot(slot_desc.tuple_offset()), slot_desc.type(), scale,
      stream, true);
}

void PrintNestedValue(const SlotDescriptor& slot_desc, Tuple* item, int scale,
    stringstream* stream) {
  bool is_null = PrintNestedValueIfNull(slot_desc, item, stream);
  if (is_null) return;

  if (slot_desc.type().IsCollectionType()) {
    // The item is also an array or a map, recurse deeper if not NULL.
    PrintNonNullNestedCollection(slot_desc, item, scale, stream);
  } else if (!slot_desc.type().IsComplexType()) {
    // The item is a scalar, print it with the usual PrintValue.
    PrintNonNullNestedPrimitive(slot_desc, item, scale, stream);
  } else {
    DCHECK(false);
  }
}

void RawValue::PrintCollectionValue(const CollectionValue* coll_val,
    const TupleDescriptor* item_tuple_desc, int scale, stringstream *stream,
    bool is_map) {
  DCHECK(item_tuple_desc != nullptr);
  if (coll_val == nullptr) {
    // We only reach this code path if this is a top level collection. Otherwise
    // PrintNestedValue() handles the printing of the NULL literal.
    *stream << NullLiteral(true);
    return;
  }
  int item_byte_size = item_tuple_desc->byte_size();

  const vector<SlotDescriptor*>& slot_descs = item_tuple_desc->slots();
  // TODO: This has to be changed once structs are supported too.
  if (is_map) {
    DCHECK(slot_descs.size() == 2);
    DCHECK(slot_descs[0] != nullptr);
    DCHECK(slot_descs[1] != nullptr);
  } else {
    DCHECK(slot_descs.size() == 1);
    DCHECK(slot_descs[0] != nullptr);
  }

  *stream << (is_map ? "{" : "[");
  for (int i = 0; i < coll_val->num_tuples; ++i) {
    Tuple* item = reinterpret_cast<Tuple*>(coll_val->ptr + i * item_byte_size);

    PrintNestedValue(*slot_descs[0], item, scale, stream);
    if (is_map) {
      *stream << ":";
      PrintNestedValue(*slot_descs[1], item, scale, stream);
    }

    if (i < coll_val->num_tuples - 1) *stream << ",";
  }
  *stream << (is_map ? "}" : "]");
}

}
