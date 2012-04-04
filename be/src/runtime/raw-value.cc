// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>
#include <boost/functional/hash.hpp>

#include "runtime/raw-value.h"
#include "runtime/string-value.h"
#include "runtime/tuple.h"

using namespace boost;
using namespace std;

namespace impala {

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
      stream->write(chars, sizeof(TimestampValue));
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

size_t RawValue::GetHashValue(const void* value, PrimitiveType type) {
  const StringValue* string_value;
  switch (type) {
    case TYPE_BOOLEAN:
      return hash<bool>().operator()(*reinterpret_cast<const bool*>(value));
    case TYPE_TINYINT:
      return hash<int8_t>().operator()(*reinterpret_cast<const int8_t*>(value));
    case TYPE_SMALLINT:
      return hash<int16_t>().operator()(*reinterpret_cast<const int16_t*>(value));
    case TYPE_INT:
      return hash<int32_t>().operator()(*reinterpret_cast<const int32_t*>(value));
    case TYPE_BIGINT:
      return hash<int64_t>().operator()(*reinterpret_cast<const int64_t*>(value));
    case TYPE_FLOAT:
      return hash<float>().operator()(*reinterpret_cast<const float*>(value));
    case TYPE_DOUBLE:
      return hash<double>().operator()(*reinterpret_cast<const double*>(value));
    case TYPE_STRING:
      string_value = reinterpret_cast<const StringValue*>(value);
      return hash_range<char*>(string_value->ptr, string_value->ptr + string_value->len);
    case TYPE_TIMESTAMP:
      return hash<double>().operator()(*reinterpret_cast<const TimestampValue*>(value));
    default:
      DCHECK(false) << "invalid type: " << TypeToString(type);
      return 0;
  };
}

int RawValue::Compare(const void* v1, const void* v2, PrimitiveType type) {
  const StringValue* string_value1;
  const StringValue* string_value2;
  const TimestampValue* ts_value1;
  const TimestampValue* ts_value2;
  float f1, f2;
  double d1, d2;
  switch (type) {
    case TYPE_BOOLEAN:
      return *reinterpret_cast<const bool*>(v1) - *reinterpret_cast<const bool*>(v2);
    case TYPE_TINYINT:
      return *reinterpret_cast<const int8_t*>(v1) - *reinterpret_cast<const int8_t*>(v2);
    case TYPE_SMALLINT:
      return *reinterpret_cast<const int16_t*>(v1) - *reinterpret_cast<const int16_t*>(v2);
    case TYPE_INT:
      return *reinterpret_cast<const int32_t*>(v1) - *reinterpret_cast<const int32_t*>(v2);
    case TYPE_BIGINT:
      // TODO: overflow issues?
      return *reinterpret_cast<const int64_t*>(v1) - *reinterpret_cast<const int64_t*>(v2);
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
      return ts_value1 > ts_value2 ? 1 : (ts_value1 < ts_value2 ? -1 : 0);
    default:
      DCHECK(false) << "invalid type: " << TypeToString(type);
      return 0;
  };
}

void RawValue::Write(const void* value, Tuple* tuple, const SlotDescriptor* slot_desc,
                     MemPool* pool) {
  if (value == NULL) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return;
  }
  switch (slot_desc->type()) {
    case TYPE_BOOLEAN: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<bool*>(slot) = *reinterpret_cast<const bool*>(value);
      break;
    }
    case TYPE_TINYINT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<int8_t*>(slot) = *reinterpret_cast<const int8_t*>(value);
      break;
    }
    case TYPE_SMALLINT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<int16_t*>(slot) = *reinterpret_cast<const int16_t*>(value);
      break;
    }
    case TYPE_INT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<int32_t*>(slot) = *reinterpret_cast<const int32_t*>(value);
      break;
    }
    case TYPE_BIGINT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<int64_t*>(slot) = *reinterpret_cast<const int64_t*>(value);
      break;
    }
    case TYPE_FLOAT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<float*>(slot) = *reinterpret_cast<const float*>(value);
      break;
    }
    case TYPE_DOUBLE: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<double*>(slot) = *reinterpret_cast<const double*>(value);
      break;
    }
    case TYPE_STRING: {
      // Copy the string value.
      const StringValue* src = reinterpret_cast<const StringValue*>(value);
      StringValue* dest = tuple->GetStringSlot(slot_desc->tuple_offset());
      dest->len = src->len;
      if (pool != NULL) {
        dest->ptr = reinterpret_cast<char*>(pool->Allocate(dest->len));
        memcpy(dest->ptr, src->ptr, dest->len);
      } else {
        dest->ptr = src->ptr;
      }
      break;
    }
    case TYPE_TIMESTAMP: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<TimestampValue*>(slot) =
          *reinterpret_cast<const TimestampValue*>(value);
      break;
    }
    case INVALID_TYPE: {
      break;
    }
    default:
      DCHECK(false) << "RawValue::Write(): bad type: "
                    << TypeToString(slot_desc->type());
  }
}

}
