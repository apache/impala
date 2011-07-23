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

void RawValue::PrintValue(const void* value, PrimitiveType type, string* str) {
  if (value == NULL) {
    *str = "NULL";
    return;
  }

  stringstream out;
  const StringValue* string_val = NULL;
  string tmp;
  switch (type) {
    case TYPE_BOOLEAN: {
      bool val = *reinterpret_cast<const bool*>(value);
      *str = (val ? "true" : "false");
      return;
    }
    case TYPE_TINYINT:
      // Extra casting for chars since they should not be interpreted as ASCII.
      out << static_cast<int>(*reinterpret_cast<const int8_t*>(value));
      break;
    case TYPE_SMALLINT:
      out << *reinterpret_cast<const int16_t*>(value);
      break;
    case TYPE_INT:
      out << *reinterpret_cast<const int32_t*>(value);
      break;
    case TYPE_BIGINT:
      out << *reinterpret_cast<const int64_t*>(value);
      break;
    case TYPE_FLOAT:
      out << *reinterpret_cast<const float*>(value);
      break;
    case TYPE_DOUBLE:
      out << *reinterpret_cast<const double*>(value);
      break;
    case TYPE_STRING:
      string_val = reinterpret_cast<const StringValue*>(value);
      tmp.assign(static_cast<char*>(string_val->ptr), string_val->len);
      str->swap(tmp);
      return;
    default:
      DCHECK(false) << "bad RawValue::PrintValue() type: " << TypeToString(type);
  }
  *str = out.str();
}

size_t RawValue::GetHashValue(const void* value, PrimitiveType type) {
  const StringValue* string_value;
  string tmp;
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
      tmp.assign(static_cast<const char*>(string_value->ptr), string_value->len);
      return hash<string>().operator()(tmp);
    default:
      DCHECK(false) << "invalid type: " << TypeToString(type);
      return 0;
  };
}

int RawValue::Compare(const void* v1, const void* v2, PrimitiveType type) {
  const StringValue* string_value1;
  const StringValue* string_value2;
  string tmp1, tmp2;
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
      return *reinterpret_cast<const int64_t*>(v1) - *reinterpret_cast<const int64_t*>(v2);
    case TYPE_FLOAT: 
      return *reinterpret_cast<const float*>(v1) - *reinterpret_cast<const float*>(v2);
    case TYPE_DOUBLE: 
      return *reinterpret_cast<const double*>(v1) - *reinterpret_cast<const double*>(v2);
    case TYPE_STRING:
      string_value1 = reinterpret_cast<const StringValue*>(v1);
      tmp1.assign(static_cast<const char*>(string_value1->ptr), string_value1->len);
      string_value2 = reinterpret_cast<const StringValue*>(v2);
      tmp2.assign(static_cast<const char*>(string_value2->ptr), string_value2->len);
      return tmp1.compare(tmp2);
    default:
      DCHECK(false) << "invalid type: " << TypeToString(type);
      return 0;
  };
}

void RawValue::Write(const void* value, Tuple* tuple, const SlotDescriptor* slot_desc,
                     MemPool* pool) {
  switch (slot_desc->type()) {
    case TYPE_BOOLEAN: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<bool*>(slot) = *reinterpret_cast<const bool*>(value);
      break;
    }
    case TYPE_TINYINT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<char*>(slot) = static_cast<char>(*reinterpret_cast<const long*>(value));
      break;
    }
    case TYPE_SMALLINT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<short*>(slot) = static_cast<short>(*reinterpret_cast<const long*>(value));
      break;
    }
    case TYPE_INT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<int*>(slot) = static_cast<int>(*reinterpret_cast<const long*>(value));
      break;
    }
    case TYPE_BIGINT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<long*>(slot) = *reinterpret_cast<const long*>(value);
      break;
    }
    case TYPE_FLOAT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<int*>(slot) = static_cast<float>(*reinterpret_cast<const double*>(value));
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
    case INVALID_TYPE: {
      break;
    }
    default:
      DCHECK(false) << "RawValue::Write(): bad type: " << TypeToString(slot_desc->type());
  }
}

}
