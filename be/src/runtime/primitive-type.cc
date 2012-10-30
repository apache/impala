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

#include "runtime/primitive-type.h"

using namespace std;

namespace impala {

PrimitiveType ThriftToType(TPrimitiveType::type ttype) {
  switch (ttype) {
    case TPrimitiveType::INVALID_TYPE: return INVALID_TYPE;
    case TPrimitiveType::BOOLEAN: return TYPE_BOOLEAN;
    case TPrimitiveType::TINYINT: return TYPE_TINYINT;
    case TPrimitiveType::SMALLINT: return TYPE_SMALLINT;
    case TPrimitiveType::INT: return TYPE_INT;
    case TPrimitiveType::BIGINT: return TYPE_BIGINT;
    case TPrimitiveType::FLOAT: return TYPE_FLOAT;
    case TPrimitiveType::DOUBLE: return TYPE_DOUBLE;
    case TPrimitiveType::DATE: return TYPE_DATE;
    case TPrimitiveType::DATETIME: return TYPE_DATETIME;
    case TPrimitiveType::TIMESTAMP: return TYPE_TIMESTAMP;
    case TPrimitiveType::STRING: return TYPE_STRING;
    default: return INVALID_TYPE;
  }
}

TPrimitiveType::type ToThrift(PrimitiveType ptype) {
  switch (ptype) {
    case INVALID_TYPE: return TPrimitiveType::INVALID_TYPE;
    case TYPE_BOOLEAN: return TPrimitiveType::BOOLEAN;
    case TYPE_TINYINT: return TPrimitiveType::TINYINT;
    case TYPE_SMALLINT: return TPrimitiveType::SMALLINT;
    case TYPE_INT: return TPrimitiveType::INT;
    case TYPE_BIGINT: return TPrimitiveType::BIGINT;
    case TYPE_FLOAT: return TPrimitiveType::FLOAT;
    case TYPE_DOUBLE: return TPrimitiveType::DOUBLE;
    case TYPE_DATE: return TPrimitiveType::DATE;
    case TYPE_DATETIME: return TPrimitiveType::DATETIME;
    case TYPE_TIMESTAMP: return TPrimitiveType::TIMESTAMP;
    case TYPE_STRING: return TPrimitiveType::STRING;
    default: return TPrimitiveType::INVALID_TYPE;
  }
}

string TypeToString(PrimitiveType t) {
  switch (t) {
    case INVALID_TYPE: return "INVALID";
    case TYPE_BOOLEAN: return "BOOL";
    case TYPE_TINYINT: return "TINYINT";
    case TYPE_SMALLINT: return "SMALLINT";
    case TYPE_INT: return "INT";
    case TYPE_BIGINT: return "BIGINT";
    case TYPE_FLOAT: return "FLOAT";
    case TYPE_DOUBLE: return "DOUBLE";
    case TYPE_DATE: return "DATE";
    case TYPE_DATETIME: return "DATETIME";
    case TYPE_TIMESTAMP: return "TIMESTAMP";
    case TYPE_STRING: return "STRING";
  };
  return "";
}

string TypeToOdbcString(PrimitiveType t) {
  // ODBC driver requires types in lower case
  switch (t) {
    case INVALID_TYPE: return "invalid";
    case TYPE_BOOLEAN: return "boolean";
    case TYPE_TINYINT: return "tinyint";
    case TYPE_SMALLINT: return "smallint";
    case TYPE_INT: return "int";
    case TYPE_BIGINT: return "bigint";
    case TYPE_FLOAT: return "float";
    case TYPE_DOUBLE: return "double";
    case TYPE_DATE: return "date";
    case TYPE_DATETIME: return "datetime";
    case TYPE_TIMESTAMP: return "timestamp";
    case TYPE_STRING: return "string";
  };
  return "unknown";
}

}
