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

namespace llvm {
  class ConstantStruct;
}

namespace impala {

class LlvmCodeGen;

// TODO for 2.3: move into ColumnType, rename to Type, and remove TYPE_ prefix
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

  /// This is minimally supported currently. It can't be returned to the user or
  /// parsed from scan nodes. It can be returned from exprs and must be consumable
  /// by exprs.
  TYPE_CHAR,
  TYPE_VARCHAR,

  TYPE_STRUCT,
  TYPE_ARRAY,
  TYPE_MAP
};

PrimitiveType ThriftToType(TPrimitiveType::type ttype);
TPrimitiveType::type ToThrift(PrimitiveType ptype);
std::string TypeToString(PrimitiveType t);
std::string TypeToOdbcString(PrimitiveType t);

// Describes a type. Includes the enum, children types, and any type-specific metadata
// (e.g. precision and scale for decimals).
// TODO for 2.3: rename to TypeDescriptor
struct ColumnType {
  PrimitiveType type;
  /// Only set if type == TYPE_CHAR or type == TYPE_VARCHAR
  int len;
  static const int MAX_VARCHAR_LENGTH = 65355;
  static const int MAX_CHAR_LENGTH = 255;
  static const int MAX_CHAR_INLINE_LENGTH = 128;

  /// Only set if type == TYPE_DECIMAL
  int precision, scale;

  /// Must be kept in sync with FE's max precision/scale.
  static const int MAX_PRECISION = 38;
  static const int MAX_SCALE = MAX_PRECISION;

  /// The maximum precision representable by a 4-byte decimal (Decimal4Value)
  static const int MAX_DECIMAL4_PRECISION = 9;
  /// The maximum precision representable by a 8-byte decimal (Decimal8Value)
  static const int MAX_DECIMAL8_PRECISION = 18;

  /// Empty for scalar types
  std::vector<ColumnType> children;

  /// Only set if type == TYPE_STRUCT. The field name of each child.
  std::vector<std::string> field_names;

  ColumnType(PrimitiveType type = INVALID_TYPE)
    : type(type), len(-1), precision(-1), scale(-1) {
    DCHECK_NE(type, TYPE_CHAR);
    DCHECK_NE(type, TYPE_VARCHAR);
    DCHECK_NE(type, TYPE_DECIMAL);
    DCHECK_NE(type, TYPE_STRUCT);
    DCHECK_NE(type, TYPE_ARRAY);
    DCHECK_NE(type, TYPE_MAP);
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

  static ColumnType FromThrift(const TColumnType& t) {
    int idx = 0;
    ColumnType result(t.types, &idx);
    DCHECK_EQ(idx, t.types.size() - 1);
    return result;
  }

  bool operator==(const ColumnType& o) const {
    if (type != o.type) return false;
    if (children != o.children) return false;
    if (type == TYPE_CHAR) return len == o.len;
    if (type == TYPE_DECIMAL) return precision == o.precision && scale == o.scale;
    return true;
  }

  bool operator!=(const ColumnType& other) const {
    return !(*this == other);
  }

  TColumnType ToThrift() const {
    TColumnType thrift_type;
    ToThrift(&thrift_type);
    return thrift_type;
  }

  inline bool IsStringType() const {
    return type == TYPE_STRING || type == TYPE_VARCHAR || type == TYPE_CHAR;
  }

  inline bool IsVarLenStringType() const {
    return type == TYPE_STRING || type == TYPE_VARCHAR ||
        (type == TYPE_CHAR && len > MAX_CHAR_INLINE_LENGTH);
  }

  inline bool IsComplexType() const {
    return type == TYPE_STRUCT || type == TYPE_ARRAY || type == TYPE_MAP;
  }

  inline bool IsCollectionType() const {
    return type == TYPE_ARRAY || type == TYPE_MAP;
  }

  inline bool IsVarLenType() const {
    return IsVarLenStringType() || IsCollectionType();
  }

  /// Returns the byte size of this type.  Returns 0 for variable length types.
  inline int GetByteSize() const {
    switch (type) {
      case TYPE_ARRAY:
      case TYPE_MAP:
      case TYPE_STRING:
      case TYPE_VARCHAR:
        return 0;
      case TYPE_CHAR:
        if (IsVarLenStringType()) return 0;
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
        DCHECK(false) << "NYI: " << type;
    }
    return 0;
  }

  /// Returns the size of a slot for this type.
  inline int GetSlotSize() const {
    switch (type) {
      case TYPE_STRING:
      case TYPE_VARCHAR:
        return 16;
      case TYPE_CHAR:
        if (IsVarLenStringType()) return 16;
        return len;
      case TYPE_ARRAY:
      case TYPE_MAP:
        return 16;
      case TYPE_STRUCT:
        DCHECK(false) << "TYPE_STRUCT slot not possible";
      default:
        return GetByteSize();
    }
  }

  static inline int GetDecimalByteSize(int precision) {
    DCHECK_GT(precision, 0);
    if (precision <= MAX_DECIMAL4_PRECISION) return 4;
    if (precision <= MAX_DECIMAL8_PRECISION) return 8;
    return 16;
  }

  /// Returns the IR version of this ColumnType. Only implemented for scalar types. LLVM
  /// optimizer can pull out fields of the returned ConstantStruct for constant folding.
  llvm::ConstantStruct* ToIR(LlvmCodeGen* codegen) const;

  apache::hive::service::cli::thrift::TTypeEntry ToHs2Type() const;
  std::string DebugString() const;

 private:
  /// Used to create a possibly nested type from the flattened Thrift representation.
  ///
  /// 'idx' is an in/out parameter that is initially set to the index of the type in
  /// 'types' being constructed, and is set to the index of the next type in 'types' that
  /// needs to be processed (or the size 'types' if all nodes have been processed).
  ColumnType(const std::vector<TTypeNode>& types, int* idx);

  /// Recursive implementation of ToThrift() that populates 'thrift_type' with the
  /// TTypeNodes for this type and its children.
  void ToThrift(TColumnType* thrift_type) const;

  static const char* LLVM_CLASS_NAME;
};

std::ostream& operator<<(std::ostream& os, const ColumnType& type);

}

#endif
