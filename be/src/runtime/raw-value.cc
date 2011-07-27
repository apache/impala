// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>

#include "runtime/raw-value.h"
#include "runtime/string-value.h"

using namespace std;
using namespace impala;

void RawValue::PrintValue(void* value, PrimitiveType type, string* str) {
  if (value == NULL) {
    *str = "NULL";
    return;
  }

  stringstream out;
  StringValue* string_val = NULL;
  string tmp;
  switch (type) {
    case TYPE_BOOLEAN: {
      bool val = *reinterpret_cast<bool*>(value);
      *str = (val ? "true" : "false");
      return;
    }
    case TYPE_TINYINT:
      // Extra casting for chars since they should not be interpreted as ASCII.
      out << static_cast<int>(*reinterpret_cast<signed char*>(value));
      break;
    case TYPE_SMALLINT:
      out << *reinterpret_cast<short*>(value);
      break;
    case TYPE_INT:
      out << *reinterpret_cast<int*>(value);
      break;
    case TYPE_BIGINT:
      out << *reinterpret_cast<long*>(value);
      break;
    case TYPE_FLOAT:
      out << *reinterpret_cast<float*>(value);
      break;
    case TYPE_DOUBLE:
      out << *reinterpret_cast<double*>(value);
      break;
    case TYPE_STRING:
      string_val = reinterpret_cast<StringValue*>(value);
      tmp.assign(static_cast<char*>(string_val->ptr), string_val->len);
      str->swap(tmp);
      return;
    default:
      DCHECK(false) << "bad RawValue::PrintValue() type: " << TypeToString(type);
  }
  *str = out.str();
}
