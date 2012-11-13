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

#include <sstream>
#include <boost/functional/hash.hpp>

#include "runtime/raw-value.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple.h"

using namespace boost;
using namespace std;

namespace impala {

const int RawValue::ASCII_PRECISION = 16; // print 16 digits for double/float

void RawValue::PrintValueAsBytes(const void* value, PrimitiveType type,
                                 stringstream* stream) {
  if (value == NULL) return;

  const char* chars = reinterpret_cast<const char*>(value);
  const StringValue* string_val = NULL;
  switch (type) {
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
      string_val = reinterpret_cast<const StringValue*>(value);
      stream->write(static_cast<char*>(string_val->ptr), string_val->len);
      return;
    case TYPE_TIMESTAMP:
      stream->write(chars, TimestampValue::Size());
      break;
    default:
      DCHECK(false) << "bad RawValue::PrintValue() type: " << TypeToString(type);
  }
}

void RawValue::PrintValue(const void* value, PrimitiveType type, stringstream* stream) {
  if (value == NULL) {
    *stream << "NULL";
    return;
  }

  string tmp;
  const StringValue* string_val = NULL;
  switch (type) {
    case TYPE_BOOLEAN: {
      bool val = *reinterpret_cast<const bool*>(value);
      *stream << (val ? "true" : "false");
      return;
    }
    case TYPE_TINYINT:
      // Extra casting for chars since they should not be interpreted as ASCII.
      *stream << static_cast<int>(*reinterpret_cast<const int8_t*>(value));
      break;
    case TYPE_SMALLINT:
      *stream << *reinterpret_cast<const int16_t*>(value);
      break;
    case TYPE_INT:
      *stream << *reinterpret_cast<const int32_t*>(value);
      break;
    case TYPE_BIGINT:
      *stream << *reinterpret_cast<const int64_t*>(value);
      break;
    case TYPE_FLOAT:
      *stream << *reinterpret_cast<const float*>(value);
      break;
    case TYPE_DOUBLE:
      *stream << *reinterpret_cast<const double*>(value);
      break;
    case TYPE_STRING:
      string_val = reinterpret_cast<const StringValue*>(value);
      tmp.assign(static_cast<char*>(string_val->ptr), string_val->len);
      *stream << tmp;
      return;
    case TYPE_TIMESTAMP:
      *stream << *reinterpret_cast<const TimestampValue*>(value);
      break;
    default:
      DCHECK(false) << "bad RawValue::PrintValue() type: " << TypeToString(type);
  }
}

void RawValue::PrintValue(const void* value, PrimitiveType type, string* str) {
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
  switch (type) {
    case TYPE_BOOLEAN:
      val = *reinterpret_cast<const bool*>(value);
      *str = (val ? "true" : "false");
      return;
    case TYPE_STRING:
      string_val = reinterpret_cast<const StringValue*>(value);
      tmp.assign(static_cast<char*>(string_val->ptr), string_val->len);
      str->swap(tmp);
      return;
    default:
      PrintValue(value, type, &out);
  }
  *str = out.str();
}

int RawValue::Compare(const void* v1, const void* v2, PrimitiveType type) {
  const StringValue* string_value1;
  const StringValue* string_value2;
  const TimestampValue* ts_value1;
  const TimestampValue* ts_value2;
  float f1, f2;
  double d1, d2;
  int32_t i1, i2;
  int64_t b1, b2;
  switch (type) {
    case TYPE_BOOLEAN:
      return *reinterpret_cast<const bool*>(v1) - *reinterpret_cast<const bool*>(v2);
    case TYPE_TINYINT:
      return *reinterpret_cast<const int8_t*>(v1) - *reinterpret_cast<const int8_t*>(v2);
    case TYPE_SMALLINT:
      return *reinterpret_cast<const int16_t*>(v1) -
             *reinterpret_cast<const int16_t*>(v2);
    case TYPE_INT:
      i1 = *reinterpret_cast<const int32_t*>(v1);
      i2 = *reinterpret_cast<const int32_t*>(v2);
      return i1 > i2 ? 1 : (i1 < i2 ? -1 : 0);
    case TYPE_BIGINT:
      b1 = *reinterpret_cast<const int64_t*>(v1);
      b2 = *reinterpret_cast<const int64_t*>(v2); 
      return b1 > b2 ? 1 : (b1 < b2 ? -1 : 0);
    case TYPE_FLOAT:
      // TODO: can this be faster? (just returning the difference has underflow problems)
      f1 = *reinterpret_cast<const float*>(v1);
      f2 = *reinterpret_cast<const float*>(v2);
      return f1 > f2 ? 1 : (f1 < f2 ? -1 : 0);
    case TYPE_DOUBLE:
      // TODO: can this be faster?
      d1 = *reinterpret_cast<const double*>(v1);
      d2 = *reinterpret_cast<const double*>(v2);
      return d1 > d2 ? 1 : (d1 < d2 ? -1 : 0);
    case TYPE_STRING:
      string_value1 = reinterpret_cast<const StringValue*>(v1);
      string_value2 = reinterpret_cast<const StringValue*>(v2);
      return string_value1->Compare(*string_value2);
    case TYPE_TIMESTAMP:
      ts_value1 = reinterpret_cast<const TimestampValue*>(v1);
      ts_value2 = reinterpret_cast<const TimestampValue*>(v2);
      return *ts_value1 > *ts_value2 ? 1 : (*ts_value1 < *ts_value2 ? -1 : 0);
    default:
      DCHECK(false) << "invalid type: " << TypeToString(type);
      return 0;
  };
}

void RawValue::Write(const void* value, void* dst, PrimitiveType type, MemPool* pool) {
  DCHECK(value != NULL);
  
  switch (type) {
    case TYPE_BOOLEAN: {
      *reinterpret_cast<bool*>(dst) = *reinterpret_cast<const bool*>(value);
      break;
    }
    case TYPE_TINYINT: {
      *reinterpret_cast<int8_t*>(dst) = *reinterpret_cast<const int8_t*>(value);
      break;
    }
    case TYPE_SMALLINT: {
      *reinterpret_cast<int16_t*>(dst) = *reinterpret_cast<const int16_t*>(value);
      break;
    }
    case TYPE_INT: {
      *reinterpret_cast<int32_t*>(dst) = *reinterpret_cast<const int32_t*>(value);
      break;
    }
    case TYPE_BIGINT: {
      *reinterpret_cast<int64_t*>(dst) = *reinterpret_cast<const int64_t*>(value);
      break;
    }
    case TYPE_FLOAT: {
      *reinterpret_cast<float*>(dst) = *reinterpret_cast<const float*>(value);
      break;
    }
    case TYPE_DOUBLE: {
      *reinterpret_cast<double*>(dst) = *reinterpret_cast<const double*>(value);
      break;
    }
    case TYPE_TIMESTAMP: 
      *reinterpret_cast<TimestampValue*>(dst) =
          *reinterpret_cast<const TimestampValue*>(value);
      break;
    case TYPE_STRING: {
      const StringValue* src = reinterpret_cast<const StringValue*>(value);
      StringValue* dest = reinterpret_cast<StringValue*>(dst);
      dest->len = src->len;
      if (pool != NULL) {
        dest->ptr = reinterpret_cast<char*>(pool->Allocate(dest->len));
        memcpy(dest->ptr, src->ptr, dest->len);
      } else {
        dest->ptr = src->ptr;
      }
      break;
    }
    default:
      DCHECK(false) << "RawValue::Write(): bad type: " << TypeToString(type);
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

}
