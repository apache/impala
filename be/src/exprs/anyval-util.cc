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

#include "exprs/anyval-util.h"
#include "codegen/llvm-codegen.h"

#include "common/object-pool.h"

using namespace std;
using namespace impala_udf;

namespace impala {

AnyVal* CreateAnyVal(ObjectPool* pool, const ColumnType& type) {
  return pool->Add(CreateAnyVal(type));
}

AnyVal* CreateAnyVal(const ColumnType& type) {
  switch(type.type) {
    case TYPE_NULL: return new AnyVal;
    case TYPE_BOOLEAN: return new BooleanVal;
    case TYPE_TINYINT: return new TinyIntVal;
    case TYPE_SMALLINT: return new SmallIntVal;
    case TYPE_INT: return new IntVal;
    case TYPE_BIGINT: return new BigIntVal;
    case TYPE_FLOAT: return new FloatVal;
    case TYPE_DOUBLE: return new DoubleVal;
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
      return new StringVal;
    case TYPE_TIMESTAMP: return new TimestampVal;
    case TYPE_DECIMAL: return new DecimalVal;
    default:
      DCHECK(false) << "Unsupported type: " << type;
      return NULL;
  }
}

FunctionContext::TypeDesc AnyValUtil::ColumnTypeToTypeDesc(const ColumnType& type) {
  FunctionContext::TypeDesc out;
  switch (type.type) {
    case TYPE_BOOLEAN:
      out.type = FunctionContext::TYPE_BOOLEAN;
      break;
    case TYPE_TINYINT:
      out.type = FunctionContext::TYPE_TINYINT;
      break;
    case TYPE_SMALLINT:
      out.type = FunctionContext::TYPE_SMALLINT;
      break;
    case TYPE_INT:
      out.type = FunctionContext::TYPE_INT;
      break;
    case TYPE_BIGINT:
      out.type = FunctionContext::TYPE_BIGINT;
      break;
    case TYPE_FLOAT:
      out.type = FunctionContext::TYPE_FLOAT;
      break;
    case TYPE_DOUBLE:
      out.type = FunctionContext::TYPE_DOUBLE;
      break;
    case TYPE_TIMESTAMP:
      out.type = FunctionContext::TYPE_TIMESTAMP;
      break;
    case TYPE_VARCHAR:
      out.type = FunctionContext::TYPE_VARCHAR;
      out.len = type.len;
      break;
    case TYPE_STRING:
      out.type = FunctionContext::TYPE_STRING;
      break;
    case TYPE_CHAR:
      out.type = FunctionContext::TYPE_FIXED_BUFFER;
      out.len = type.len;
      break;
    case TYPE_DECIMAL:
      out.type = FunctionContext::TYPE_DECIMAL;
      out.precision = type.precision;
      out.scale = type.scale;
      break;
    default:
      DCHECK(false) << "Unknown type: " << type;
  }
  return out;
}

ColumnType AnyValUtil::TypeDescToColumnType(const FunctionContext::TypeDesc& type) {
  switch (type.type) {
    case FunctionContext::TYPE_BOOLEAN: return ColumnType(TYPE_BOOLEAN);
    case FunctionContext::TYPE_TINYINT: return ColumnType(TYPE_TINYINT);
    case FunctionContext::TYPE_SMALLINT: return ColumnType(TYPE_SMALLINT);
    case FunctionContext::TYPE_INT: return ColumnType(TYPE_INT);
    case FunctionContext::TYPE_BIGINT: return ColumnType(TYPE_BIGINT);
    case FunctionContext::TYPE_FLOAT: return ColumnType(TYPE_FLOAT);
    case FunctionContext::TYPE_DOUBLE: return ColumnType(TYPE_DOUBLE);
    case FunctionContext::TYPE_TIMESTAMP: return ColumnType(TYPE_TIMESTAMP);
    case FunctionContext::TYPE_STRING: return ColumnType(TYPE_STRING);
    case FunctionContext::TYPE_DECIMAL:
      return ColumnType::CreateDecimalType(type.precision, type.scale);
    case FunctionContext::TYPE_FIXED_BUFFER:
      return ColumnType::CreateCharType(type.len);
    case FunctionContext::TYPE_VARCHAR:
      return ColumnType::CreateVarcharType(type.len);
    default:
      DCHECK(false) << "Unknown type: " << type.type;
      return ColumnType(INVALID_TYPE);
  }
}

}
