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
#include "gen-cpp/TCLIService_types.h"  // for HiveServer2 Type

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
  TYPE_VARCHAR
};

PrimitiveType ThriftToType(TPrimitiveType::type ttype);
TPrimitiveType::type ToThrift(PrimitiveType ptype);
std::string TypeToString(PrimitiveType t);
std::string TypeToOdbcString(PrimitiveType t);

// Wrapper struct to describe a type. Includes the enum and, optionally,
// size information.
// TODO: Rename to ScalarType and mirror FE type hierarchy after the expr refactoring.
struct ColumnType {
  PrimitiveType type;
  // Only set if type == TYPE_CHAR or type == TYPE_VARCHAR
  int len;
  static const int MAX_VARCHAR_LENGTH = 65355;
  static const int MAX_CHAR_LENGTH = 255;
  static const int MAX_CHAR_INLINE_LENGTH = 128;

  // Only set if type == TYPE_DECIMAL
  int precision, scale;

  // Must be kept in sync with FE's max precision/scale.
  static const int MAX_PRECISION = 38;
  static const int MAX_SCALE = MAX_PRECISION;

  ColumnType(PrimitiveType type = INVALID_TYPE)
    : type(type), len(-1), precision(-1), scale(-1) {
    DCHECK_NE(type, TYPE_CHAR);
    DCHECK_NE(type, TYPE_DECIMAL);
  }

  static ColumnType CreateCharType(int len) {
    DCHECK_GE(len, 1);
    DCHECK_LE(len, MAX_CHAR_LENGTH);
    ColumnType ret;
    ret.type = TYPE_CHAR;
    ret.len = len;
    return ret;
  }

  static ColumnType CreateVarcharType(int len) {
    DCHECK_GE(len, 1);
    DCHECK_LE(len, MAX_VARCHAR_LENGTH);
    ColumnType ret;
    ret.type = TYPE_VARCHAR;
    ret.len = len;
    return ret;
  }

  static ColumnType CreateDecimalType(int precision, int scale) {
    DCHECK_LE(precision, MAX_PRECISION);
    DCHECK_LE(scale, MAX_SCALE);
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
    DCHECK_EQ(1, t.types.size());
    const TTypeNode& node = t.types[0];
    DCHECK(node.__isset.scalar_type);
    const TScalarType scalar_type = node.scalar_type;
    type = ThriftToType(scalar_type.type);
    if (type == TYPE_CHAR || type == TYPE_VARCHAR) {
      DCHECK(scalar_type.__isset.len);
      len = scalar_type.len;
    } else if (type == TYPE_DECIMAL) {
      DCHECK(scalar_type.__isset.precision);
      DCHECK(scalar_type.__isset.scale);
      precision = scalar_type.precision;
      scale = scalar_type.scale;
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
    // TODO: Decimal and complex types.
    TColumnType thrift_type;
    thrift_type.types.push_back(TTypeNode());
    TTypeNode& node = thrift_type.types.back();
    node.type = TTypeNodeType::SCALAR;
    node.__set_scalar_type(TScalarType());
    TScalarType& scalar_type = node.scalar_type;
    scalar_type.__set_type(impala::ToThrift(type));
    if (type == TYPE_CHAR || type == TYPE_VARCHAR) {
      DCHECK_NE(len, -1);
      scalar_type.__set_len(len);
    } else if (type == TYPE_DECIMAL) {
      DCHECK_NE(precision, -1);
      DCHECK_NE(scale, -1);
      scalar_type.__set_precision(precision);
      scalar_type.__set_scale(scale);
    }
    return thrift_type;
  }

  inline bool IsStringType() const {
    return type == TYPE_STRING || type == TYPE_VARCHAR || type == TYPE_CHAR;
  }

  inline bool IsVarLen() const {
    return type == TYPE_STRING || type == TYPE_VARCHAR ||
        (type == TYPE_CHAR && len > MAX_CHAR_INLINE_LENGTH);
  }

  // Returns the byte size of this type.  Returns 0 for variable length types.
  inline int GetByteSize() const {
    switch (type) {
      case TYPE_STRING:
      case TYPE_VARCHAR:
        return 0;
      case TYPE_CHAR:
        if (IsVarLen()) return 0;
        return len;
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
      case TYPE_VARCHAR:
        return 16;
      case TYPE_CHAR:
        if (IsVarLen()) return 16;
        return len;
      default:
        return GetByteSize();
    }
  }

  static inline int GetDecimalByteSize(int precision) {
    DCHECK_GT(precision, 0);
    if (precision <= 9) return 4;
    if (precision <= 18) return 8;
    return 16;
  }

  apache::hive::service::cli::thrift::TTypeEntry ToHs2Type() const;
  std::string DebugString() const;
};

std::ostream& operator<<(std::ostream& os, const ColumnType& type);

}

#endif
