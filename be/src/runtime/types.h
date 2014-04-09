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


#ifndef IMPALA_RUNTIME_TYPE_H
#define IMPALA_RUNTIME_TYPE_H

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
  TYPE_DECIMAL,

  // This is minimally supported currently. It can't be returned to the user or
  // parsed from scan nodes. It can be returned from exprs and must be consumable
  // by exprs.
  TYPE_CHAR,
};

PrimitiveType ThriftToType(TPrimitiveType::type ttype);
TPrimitiveType::type ToThrift(PrimitiveType ptype);
std::string TypeToString(PrimitiveType t);
std::string TypeToOdbcString(PrimitiveType t);
apache::hive::service::cli::thrift::TTypeId::type TypeToHiveServer2Type(PrimitiveType t);

// Wrapper struct to describe a type. Includes the enum and, optionally,
// size information.
struct ColumnType {
  PrimitiveType type;
  /// Only set if type == TYPE_CHAR
  int len;

  // Only set if type == TYPE_DECIMAL
  int precision, scale;

  ColumnType(PrimitiveType type = INVALID_TYPE)
    : type(type), len(-1), precision(-1), scale(-1) {
    DCHECK_NE(type, TYPE_CHAR);
    DCHECK_NE(type, TYPE_DECIMAL);
  }

  static ColumnType CreateCharType(int len) {
    DCHECK_GE(len, 0);
    ColumnType ret;
    ret.type = TYPE_CHAR;
    ret.len = len;
    return ret;
  }

  static ColumnType CreateDecimalType(int precision, int scale) {
    DCHECK_GE(precision, 0);
    DCHECK_LE(scale, precision);
    ColumnType ret;
    ret.type = TYPE_DECIMAL;
    ret.precision = precision;
    ret.scale = scale;
    return ret;
  }

  ColumnType(const TColumnType& t) {
    len = precision = scale = -1;
    type = ThriftToType(t.type);
    if (t.__isset.len) {
      DCHECK_EQ(type, TYPE_CHAR);
      len = t.len;
    } else if (t.__isset.precision) {
      DCHECK_EQ(type, TYPE_DECIMAL);
      DCHECK(t.__isset.scale);
      precision = t.precision;
      scale = t.scale;
    } else {
      DCHECK_NE(type, TYPE_DECIMAL);
      DCHECK_NE(type, TYPE_CHAR);
    }
  }

  bool operator==(const ColumnType& o) const {
    if (type != o.type) return false;
    if (type == TYPE_CHAR) return len == o.len;
    if (type == TYPE_DECIMAL) return precision == o.precision && scale == o.scale;
    return true;
  }

  bool operator!=(const ColumnType& other) const {
    return !(*this == other);
  }

  TColumnType ToThrift() const {
    TColumnType thrift_type;
    thrift_type.type = impala::ToThrift(type);
    if (type == TYPE_CHAR) {
      DCHECK_NE(len, -1);
      thrift_type.__set_len(len);
    } else if (type == TYPE_DECIMAL) {
      DCHECK_NE(precision, -1);
      DCHECK_NE(scale, -1);
      thrift_type.__set_precision(precision);
      thrift_type.__set_scale(scale);
    }
    return thrift_type;
  }

  // Returns the byte size of this type.  Returns 0 for variable length types.
  inline int GetByteSize() const {
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
      case TYPE_DECIMAL:
        return GetDecimalByteSize(precision);
      case TYPE_DATE:
      case INVALID_TYPE:
      default:
        DCHECK(false);
    }
    return 0;
  }

  // Returns the size of a slot for this type.
  inline int GetSlotSize() const {
    switch (type) {
      case TYPE_STRING:
        return 16;
      default:
        return GetByteSize();
    }
  }

  static inline int GetDecimalByteSize(int precision) {
    DCHECK_GT(precision, 0);
    if (precision <= 9) return 4;
    if (precision <= 19) return 8;
    return 16;
  }

  std::string DebugString() const;
};

std::ostream& operator<<(std::ostream& os, const ColumnType& type);

}

#endif
