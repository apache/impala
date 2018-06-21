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

#include "runtime/collection-value.h"
#include "runtime/date-value.h"
#include "runtime/raw-value.inline.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple.h"
#include "util/ubsan.h"

#include "common/names.h"

namespace impala {

const int RawValue::ASCII_PRECISION = 16; // print 16 digits for double/float
const double RawValue::CANONICAL_DOUBLE_NAN = nan("");
const float RawValue::CANONICAL_FLOAT_NAN = nanf("");

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
    *str = "NULL";
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

void RawValue::PrintValue(
    const void* value, const ColumnType& type, int scale, std::stringstream* stream) {
  if (value == NULL) {
    *stream << "NULL";
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
      stream->write(string_val->ptr, string_val->len);
      break;
    case TYPE_TIMESTAMP:
      *stream << *reinterpret_cast<const TimestampValue*>(value);
      break;
    case TYPE_CHAR:
      stream->write(reinterpret_cast<const char*>(value), type.len);
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
        *stream << *reinterpret_cast<const DateValue*>(value);
      }
      break;
    default: DCHECK(false) << "Unknown type: " << type;
  }
  stream->precision(old_precision);
  // Undo setting stream to fixed
  stream->flags(old_flags);
}
}
