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


#ifndef IMPALA_RUNTIME_TYPE_H
#define IMPALA_RUNTIME_TYPE_H

#include <string>

#include "common/logging.h"
#include "gen-cpp/Types_types.h"  // for TPrimitiveType

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
  TYPE_DATE,
  TYPE_DATETIME,    // Not implemented
  TYPE_BINARY,      // Not used, see ColumnType::is_binary
  TYPE_DECIMAL,
  TYPE_CHAR,
  TYPE_VARCHAR,
  TYPE_FIXED_UDA_INTERMEDIATE,

  TYPE_STRUCT,
  TYPE_ARRAY,
  TYPE_MAP
};

PrimitiveType ThriftToType(TPrimitiveType::type ttype);
TPrimitiveType::type ToThrift(PrimitiveType ptype);
std::string TypeToString(PrimitiveType t);

std::string TypeToOdbcString(const TColumnType& type);

// Describes a type. Includes the enum, children types, and any type-specific metadata
// (e.g. precision and scale for decimals).
// TODO for 2.3: rename to TypeDescriptor
struct ColumnType {
  PrimitiveType type;

  /// Only set if type one of TYPE_CHAR, TYPE_VARCHAR, TYPE_FIXED_UDA_INTERMEDIATE.
  int len;
  static const int MAX_VARCHAR_LENGTH = (1 << 16) - 1; // 65535
  static const int MAX_CHAR_LENGTH = (1 << 8) - 1; // 255

  /// Only set if type == TYPE_DECIMAL
  int precision, scale;

  /// Must be kept in sync with FE's max precision/scale.
  static const int MAX_PRECISION = 38;
  static const int MAX_SCALE = MAX_PRECISION;
  static const int MIN_ADJUSTED_SCALE = 6;

  /// The maximum precision representable by a 4-byte decimal (Decimal4Value)
  static const int MAX_DECIMAL4_PRECISION = 9;
  /// The maximum precision representable by a 8-byte decimal (Decimal8Value)
  static const int MAX_DECIMAL8_PRECISION = 18;

  /// Empty for scalar types
  std::vector<ColumnType> children;

  /// Only set if type == TYPE_STRUCT. The field name of each child.
  std::vector<std::string> field_names;
  /// Only set if type == TYPE_STRUCT. The field id of each child for Iceberg tables.
  std::vector<int> field_ids;

  static const char* LLVM_CLASS_NAME;

  explicit ColumnType(PrimitiveType type = INVALID_TYPE)
    : type(type), len(-1), precision(-1), scale(-1), is_binary_(false) {
    DCHECK_NE(type, TYPE_CHAR);
    DCHECK_NE(type, TYPE_VARCHAR);
    DCHECK_NE(type, TYPE_BINARY);
    DCHECK_NE(type, TYPE_DECIMAL);
    DCHECK_NE(type, TYPE_STRUCT);
    DCHECK_NE(type, TYPE_ARRAY);
    DCHECK_NE(type, TYPE_MAP);
    DCHECK_NE(type, TYPE_FIXED_UDA_INTERMEDIATE);
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

  static ColumnType CreateFixedUdaIntermediateType(int len) {
    DCHECK_GE(len, 1);
    ColumnType ret;
    ret.type = TYPE_FIXED_UDA_INTERMEDIATE;
    ret.len = len;
    return ret;
  }

  static ColumnType CreateBinaryType() {
    ColumnType ret(TYPE_STRING);
    ret.is_binary_ = true;
    return ret;
  }

  static bool ValidateDecimalParams(int precision, int scale) {
    return precision >= 1 && precision <= MAX_PRECISION && scale >= 0
        && scale <= MAX_SCALE && scale <= precision;
  }

  static ColumnType CreateDecimalType(int precision, int scale) {
    DCHECK(ValidateDecimalParams(precision, scale)) << precision << ", " << scale;
    ColumnType ret;
    ret.type = TYPE_DECIMAL;
    ret.precision = precision;
    ret.scale = scale;
    return ret;
  }

  // Matches the results of createAdjustedDecimalType in front-end code.
  static ColumnType CreateAdjustedDecimalType(int precision, int scale) {
    if (precision > MAX_PRECISION) {
      int min_scale = std::min(scale, MIN_ADJUSTED_SCALE);
      int delta = precision - MAX_PRECISION;
      precision = MAX_PRECISION;
      scale = std::max(scale - delta, min_scale);
    }
    return CreateDecimalType(precision, scale);
  }

  static ColumnType FromThrift(const TColumnType& t) {
    int idx = 0;
    ColumnType result(t.types, &idx);
    DCHECK_EQ(idx, t.types.size() - 1);
    return result;
  }

  static std::vector<ColumnType> FromThrift(const std::vector<TColumnType>& ttypes);

  bool operator==(const ColumnType& o) const {
    if (type != o.type) return false;
    if (children != o.children) return false;
    if (type == TYPE_CHAR || type == TYPE_FIXED_UDA_INTERMEDIATE) return len == o.len;
    if (type == TYPE_DECIMAL) return precision == o.precision && scale == o.scale;
    if (type == TYPE_STRING) return is_binary_ == o.is_binary_;
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

  inline bool IsBooleanType() const { return type == TYPE_BOOLEAN; }

  inline bool IsIntegerType() const {
    return type == TYPE_TINYINT || type == TYPE_SMALLINT || type == TYPE_INT
        || type == TYPE_BIGINT;
  }

  inline bool IsFloatingPointType() const {
    return type == TYPE_FLOAT || type == TYPE_DOUBLE;
  }

  inline bool IsDecimalType() const { return type == TYPE_DECIMAL; }

  inline bool IsStringType() const {
    return type == TYPE_STRING || type == TYPE_VARCHAR || type == TYPE_CHAR;
  }

  inline bool IsTimestampType() const { return type == TYPE_TIMESTAMP; }

  inline bool IsDateType() const { return type == TYPE_DATE; }

  inline bool IsVarLenStringType() const {
    return type == TYPE_STRING || type == TYPE_VARCHAR;
  }

  inline bool IsBinaryType() const { return is_binary_; }

  inline bool IsComplexType() const {
    return type == TYPE_STRUCT || type == TYPE_ARRAY || type == TYPE_MAP;
  }

  inline bool IsStructType() const {
    return type == TYPE_STRUCT;
  }

  inline bool IsCollectionType() const {
    return type == TYPE_ARRAY || type == TYPE_MAP;
  }

  inline bool IsArrayType() const { return type == TYPE_ARRAY; }
  inline bool IsMapType() const { return type == TYPE_MAP; }

  /// Returns the byte size of this type.  Returns 0 for variable length types.
  inline int GetByteSize() const { return GetByteSize(*this); }

  /// Returns the size of a slot for this type.
  inline int GetSlotSize() const { return GetSlotSize(*this); }

  static inline int GetDecimalByteSize(int precision) {
    DCHECK_GT(precision, 0);
    if (precision <= MAX_DECIMAL4_PRECISION) return 4;
    if (precision <= MAX_DECIMAL8_PRECISION) return 8;
    return 16;
  }

  /// Returns the IR version of this ColumnType. Only implemented for scalar types. LLVM
  /// optimizer can pull out fields of the returned ConstantStruct for constant folding.
  llvm::ConstantStruct* ToIR(LlvmCodeGen* codegen) const;

  std::string DebugString() const;

  /// Used to create a possibly nested type from the flattened Thrift representation.
  ///
  /// 'idx' is an in/out parameter that is initially set to the index of the type in
  /// 'types' being constructed, and is set to the index of the next type in 'types' that
  /// needs to be processed (or the size 'types' if all nodes have been processed).
  ColumnType(const std::vector<TTypeNode>& types, int* idx);

 private:
  // Differentiates between STRING and BINARY. As STRING is just a byte array in Impala
  // (no UTF-8 encoding), the two types are practically the same in the backend - only
  // some code parts, e.g. file format readers/writers differentiate between the two.
  // Instead of PrimitiveType::TYPE_BINARY, TYPE_STRING is used for the BINARY type to
  // ensure that everything that works for STRING also works for BINARY.
  //
  // This variable is true if 'type' is TYPE_STRING and this object represents the BINARY
  // type, and false in all other cases.
  bool is_binary_ = false;

  /// Recursive implementation of ToThrift() that populates 'thrift_type' with the
  /// TTypeNodes for this type and its children.
  void ToThrift(TColumnType* thrift_type) const;

  /// Helper function for GetSlotSize() so that struct size could be calculated
  /// recursively.
  static inline int GetSlotSize(const ColumnType& col_type) {
    switch (col_type.type) {
      case TYPE_STRUCT: {
        int struct_size = 0;
        for (const ColumnType& child_type : col_type.children) {
          struct_size += GetSlotSize(child_type);
        }
        return struct_size;
      }
      case TYPE_STRING:
      case TYPE_VARCHAR:
        return 12;
      case TYPE_CHAR:
      case TYPE_FIXED_UDA_INTERMEDIATE:
        return col_type.len;
      case TYPE_ARRAY:
      case TYPE_MAP:
        return 12;
      default:
        return GetByteSize(col_type);
    }
  }

  /// Helper function for GetByteSize()
  static inline int GetByteSize(const ColumnType& col_type) {
    switch (col_type.type) {
      case TYPE_STRUCT: {
        int struct_size = 0;
        for (const ColumnType& child_type : col_type.children) {
          struct_size += GetByteSize(child_type);
        }
        return struct_size;
      }
      case TYPE_ARRAY:
      case TYPE_MAP:
      case TYPE_STRING:
      case TYPE_VARCHAR:
        return 0;
      case TYPE_CHAR:
      case TYPE_FIXED_UDA_INTERMEDIATE:
        return col_type.len;
      case TYPE_NULL:
      case TYPE_BOOLEAN:
      case TYPE_TINYINT:
        return 1;
      case TYPE_SMALLINT:
        return 2;
      case TYPE_INT:
      case TYPE_DATE:
      case TYPE_FLOAT:
        return 4;
      case TYPE_BIGINT:
      case TYPE_DOUBLE:
        return 8;
      case TYPE_TIMESTAMP:
        // This is the size of the slot, the actual size of the data is 12.
        return 16;
      case TYPE_DECIMAL:
        return GetDecimalByteSize(col_type.precision);
      case INVALID_TYPE:
      default:
        DCHECK(false) << "NYI: " << col_type.type;
    }
    return 0;
  }
};

std::ostream& operator<<(std::ostream& os, const ColumnType& type);

}

#endif
