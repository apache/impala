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

#include "exprs/anyval-util.h"

#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"

#include "common/names.h"

using namespace impala_udf;

namespace impala {

Status AllocateAnyVal(RuntimeState* state, MemPool* pool, const ColumnType& type,
    const std::string& mem_limit_exceeded_msg, AnyVal** result) {
  const int anyval_size = AnyValUtil::AnyValSize(type);
  const int anyval_alignment = AnyValUtil::AnyValAlignment(type);
  *result =
      reinterpret_cast<AnyVal*>(pool->TryAllocateAligned(anyval_size, anyval_alignment));
  if (*result == NULL) {
    return pool->mem_tracker()->MemLimitExceeded(
        state, mem_limit_exceeded_msg, anyval_size);
  }
  memset(*result, 0, anyval_size);
  return Status::OK();
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
    case TYPE_FIXED_UDA_INTERMEDIATE:
      out.type = FunctionContext::TYPE_FIXED_UDA_INTERMEDIATE;
      out.len = type.len;
      break;
    case TYPE_DECIMAL:
      out.type = FunctionContext::TYPE_DECIMAL;
      out.precision = type.precision;
      out.scale = type.scale;
      break;
    case TYPE_DATE:
      out.type = FunctionContext::TYPE_DATE;
      break;
    case TYPE_STRUCT:
      out.type = FunctionContext::TYPE_STRUCT;
      break;
    default:
      DCHECK(false) << "Unknown type: " << type;
  }
  return out;
}

vector<FunctionContext::TypeDesc> AnyValUtil::ColumnTypesToTypeDescs(
    const vector<ColumnType>& types) {
  vector<FunctionContext::TypeDesc> type_descs;
  type_descs.reserve(types.size());
  for (const ColumnType& type : types) type_descs.push_back(ColumnTypeToTypeDesc(type));
  return type_descs;
}

ColumnType AnyValUtil::TypeDescToColumnType(const FunctionContext::TypeDesc& type) {
  switch (type.type) {
    case FunctionContext::TYPE_BOOLEAN:
      return ColumnType(TYPE_BOOLEAN);
    case FunctionContext::TYPE_TINYINT:
      return ColumnType(TYPE_TINYINT);
    case FunctionContext::TYPE_SMALLINT:
      return ColumnType(TYPE_SMALLINT);
    case FunctionContext::TYPE_INT:
      return ColumnType(TYPE_INT);
    case FunctionContext::TYPE_BIGINT:
      return ColumnType(TYPE_BIGINT);
    case FunctionContext::TYPE_FLOAT:
      return ColumnType(TYPE_FLOAT);
    case FunctionContext::TYPE_DOUBLE:
      return ColumnType(TYPE_DOUBLE);
    case FunctionContext::TYPE_TIMESTAMP:
      return ColumnType(TYPE_TIMESTAMP);
    case FunctionContext::TYPE_STRING:
      return ColumnType(TYPE_STRING);
    case FunctionContext::TYPE_DECIMAL:
      return ColumnType::CreateDecimalType(type.precision, type.scale);
    case FunctionContext::TYPE_FIXED_BUFFER:
      return ColumnType::CreateCharType(type.len);
    case FunctionContext::TYPE_FIXED_UDA_INTERMEDIATE:
      return ColumnType::CreateFixedUdaIntermediateType(type.len);
    case FunctionContext::TYPE_VARCHAR:
      return ColumnType::CreateVarcharType(type.len);
    case FunctionContext::TYPE_DATE:
      return ColumnType(TYPE_DATE);
    default:
      DCHECK(false) << "Unknown type: " << type.type;
      return ColumnType(INVALID_TYPE);
  }
}

}
