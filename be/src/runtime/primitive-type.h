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


#ifndef IMPALA_RUNTIME_PRIMITIVE_TYPE_H
#define IMPALA_RUNTIME_PRIMITIVE_TYPE_H

#include <string>

#include "common/logging.h"
#include "gen-cpp/Types_types.h"  // for TPrimitiveType
#include "gen-cpp/cli_service_types.h"  // for HiveServer2 Type

namespace impala {

enum PrimitiveType {
  INVALID_TYPE = 0,
  TYPE_NULL,
  TYPE_BOOLEAN,
  TYPE_TINYINT,
  TYPE_SMALLINT,
  TYPE_INT,
  TYPE_BIGINT,
  TYPE_FLOAT,
  TYPE_DOUBLE,
  TYPE_TIMESTAMP,
  TYPE_STRING,
  TYPE_DATE,        // Not implemented
  TYPE_DATETIME,    // Not implemented
  TYPE_BINARY,      // Not implemented
  TYPE_DECIMAL,     // Not implemented

  // This is minimally supported currently. It can't be returned to the user or
  // parsed from scan nodes. It can be returned from exprs and must be consumable
  // by exprs.
  TYPE_CHAR,
};

// Returns the size of a slot for 'type'.
inline int GetSlotSize(PrimitiveType type) {
  switch (type) {
    case TYPE_NULL:
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
      return 1;
    case TYPE_SMALLINT:
      return 2;
    case TYPE_INT:
    case TYPE_FLOAT:
      return 4;
    case TYPE_BIGINT:
    case TYPE_DOUBLE:
      return 8;
    case TYPE_STRING:
    case TYPE_TIMESTAMP:
      return 16;
    case TYPE_DATE:
    case INVALID_TYPE:
    default:
      DCHECK(false);
  }
  return 0;
}

// Returns the byte size of 'type'  Returns 0 for variable length types.
inline int GetByteSize(PrimitiveType type) {
  switch (type) {
    case TYPE_STRING:
      return 0;
    case TYPE_NULL:
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
      return 1;
    case TYPE_SMALLINT:
      return 2;
    case TYPE_INT:
    case TYPE_FLOAT:
      return 4;
    case TYPE_BIGINT:
    case TYPE_DOUBLE:
      return 8;
    case TYPE_TIMESTAMP:
      // This is the size of the slot, the actual size of the data is 12.
      return 16;
    case TYPE_DATE:
    case INVALID_TYPE:
    default:
      DCHECK(false);
  }
  return 0;
}

PrimitiveType ThriftToType(TPrimitiveType::type ttype);
TPrimitiveType::type ToThrift(PrimitiveType ptype);
std::string TypeToString(PrimitiveType t);
std::string TypeToOdbcString(PrimitiveType t);
apache::hive::service::cli::thrift::TTypeId::type TypeToHiveServer2Type(PrimitiveType t);

// Wrapper struct to describe a type. Includes the enum and, optionally,
// size information.
// TODO: PrimitiveType should not be used directly in the BE.
struct ColumnType {
  PrimitiveType type;
  /// Only set if type == TYPE_CHAR
  int len;

  ColumnType(PrimitiveType type = INVALID_TYPE, int len = -1)
    : type(type), len(len) {
  }

  ColumnType(const TColumnType& t) {
    type = ThriftToType(t.type);
    if (t.__isset.len) {
      len = t.len;
      DCHECK_EQ(type, TYPE_CHAR);
    } else {
      DCHECK_NE(type, TYPE_CHAR);
      len = -1;
    }
  }

  TColumnType ToThrift() const {
    TColumnType thrift_type;
    thrift_type.type = impala::ToThrift(type);
    if (type == TYPE_CHAR) {
      thrift_type.len = len;
      thrift_type.__isset.len = true;
    }
    // TODO: decimal
    return thrift_type;
  }

  std::string DebugString() const;
};

}

#endif
