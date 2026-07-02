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

#include "runtime/types.h"

#include <ostream>

#include "codegen/llvm-codegen.h"
#include "gutil/strings/substitute.h"

#include "common/names.h"

using namespace apache::hive::service::cli::thrift;

namespace impala {

const int ColumnType::MAX_PRECISION;
const int ColumnType::MAX_SCALE;
const int ColumnType::MIN_ADJUSTED_SCALE;

const int ColumnType::MAX_DECIMAL4_PRECISION;
const int ColumnType::MAX_DECIMAL8_PRECISION;
const int ColumnType::UUID_BYTE_LENGTH;

const char* ColumnType::LLVM_CLASS_NAME = "struct.impala::ColumnType";

ColumnType::ColumnType(const std::vector<TTypeNode>& types, int* idx)
  : len(-1), precision(-1), scale(-1),
    string_val_subtype_(StringValSubtype::STRING) {
  DCHECK_GE(*idx, 0);
  DCHECK_LT(*idx, types.size());
  const TTypeNode& node = types[*idx];
  switch (node.type) {
    case TTypeNodeType::SCALAR: {
      DCHECK(node.__isset.scalar_type);
      const TScalarType scalar_type = node.scalar_type;
      type = ThriftToType(scalar_type.type);
      if (type == TYPE_CHAR || type == TYPE_VARCHAR
          || type == TYPE_FIXED_UDA_INTERMEDIATE) {
        DCHECK(scalar_type.__isset.len);
        len = scalar_type.len;
      } else if (type == TYPE_UUID) {
        len = UUID_BYTE_LENGTH;
      } else if (type == TYPE_STRING) {
        if (scalar_type.type == TPrimitiveType::BINARY) {
          string_val_subtype_ = StringValSubtype::BINARY;
        } else if (scalar_type.type == TPrimitiveType::GEOMETRY) {
          string_val_subtype_ = StringValSubtype::GEOMETRY;
        }
      } else if (type == TYPE_DECIMAL) {
        DCHECK(scalar_type.__isset.precision);
        DCHECK(scalar_type.__isset.scale);
        precision = scalar_type.precision;
        scale = scalar_type.scale;
      }
      break;
    }
    case TTypeNodeType::STRUCT:
      type = TYPE_STRUCT;
      for (int i = 0; i < node.struct_fields.size(); ++i) {
        ++(*idx);
        children.push_back(ColumnType(types, idx));
        field_names.push_back(node.struct_fields[i].name);
        field_ids.push_back(node.struct_fields[i].field_id);
      }
      break;
    case TTypeNodeType::ARRAY:
      DCHECK(!node.__isset.scalar_type);
      DCHECK_LT(*idx, types.size() - 1);
      type = TYPE_ARRAY;
      ++(*idx);
      children.push_back(ColumnType(types, idx));
      break;
    case TTypeNodeType::MAP:
      DCHECK(!node.__isset.scalar_type);
      DCHECK_LT(*idx, types.size() - 2);
      type = TYPE_MAP;
      ++(*idx);
      children.push_back(ColumnType(types, idx));
      ++(*idx);
      children.push_back(ColumnType(types, idx));
      break;
    case TTypeNodeType::VARIANT:
      type = TYPE_VARIANT;
      for (int i = 0; i < node.variant_fields.size(); ++i) {
        ++(*idx);
        children.push_back(ColumnType(types, idx));
        field_names.push_back(node.variant_fields[i].name);
        field_ids.push_back(node.variant_fields[i].field_id);
      }
      DCheckVariantShape();
      break;
    default:
      DCHECK(false) << node.type;
  }
}

void ColumnType::DCheckVariantShape() const {
  DCHECK_EQ(type, TYPE_VARIANT);
  DCHECK_EQ(children.size(), 2) << "VARIANT must have exactly metadata+value children";
  DCHECK_EQ(field_names.size(), children.size());
  DCHECK_EQ(field_names[0], "metadata") << field_names[0];
  DCHECK_EQ(field_names[1], "value") << field_names[1];
  for (const ColumnType& child : children) {
    // BINARY is represented as TYPE_STRING with string_val_subtype_ == BINARY
    DCHECK_EQ(child.type, TYPE_STRING);
    DCHECK(child.IsBinaryType()) << "VARIANT children must be BINARY";
  }
}

PrimitiveType ThriftToType(TPrimitiveType::type ttype) {
  switch (ttype) {
    case TPrimitiveType::INVALID_TYPE: return INVALID_TYPE;
    case TPrimitiveType::NULL_TYPE: return TYPE_NULL;
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
    case TPrimitiveType::VARCHAR: return TYPE_VARCHAR;
    // BINARY and GEOMETRY are generally handled the same way as STRING by the backend.
    case TPrimitiveType::BINARY: return TYPE_STRING;
    case TPrimitiveType::UUID: return TYPE_UUID;
    case TPrimitiveType::GEOMETRY: return TYPE_STRING;
    case TPrimitiveType::DECIMAL: return TYPE_DECIMAL;
    case TPrimitiveType::CHAR: return TYPE_CHAR;
    case TPrimitiveType::FIXED_UDA_INTERMEDIATE: return TYPE_FIXED_UDA_INTERMEDIATE;
    case TPrimitiveType::VARIANT: return TYPE_VARIANT;
    default: return INVALID_TYPE;
  }
}

TPrimitiveType::type ToThrift(PrimitiveType ptype, StringValSubtype subtype) {
  switch (ptype) {
    case INVALID_TYPE: return TPrimitiveType::INVALID_TYPE;
    case TYPE_NULL: return TPrimitiveType::NULL_TYPE;
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
    case TYPE_STRING:
      switch (subtype) {
        case StringValSubtype::STRING: return TPrimitiveType::STRING;
        case StringValSubtype::BINARY: return TPrimitiveType::BINARY;
        case StringValSubtype::GEOMETRY: return TPrimitiveType::GEOMETRY;
        default:
          DCHECK(false) << "Unexpected string subtype" << subtype;
          return TPrimitiveType::INVALID_TYPE;
      }
    case TYPE_VARCHAR: return TPrimitiveType::VARCHAR;
    case TYPE_BINARY:
      DCHECK(false) << "STRING should be used instead of BINARY in the backend.";
      return TPrimitiveType::INVALID_TYPE;
    case TYPE_DECIMAL: return TPrimitiveType::DECIMAL;
    case TYPE_CHAR: return TPrimitiveType::CHAR;
    case TYPE_UUID: return TPrimitiveType::UUID;
    case TYPE_FIXED_UDA_INTERMEDIATE: return TPrimitiveType::FIXED_UDA_INTERMEDIATE;
    case TYPE_VARIANT: return TPrimitiveType::VARIANT;
    case TYPE_STRUCT:
    case TYPE_ARRAY:
    case TYPE_MAP:
      DCHECK(false) << "NYI: " << ptype;
      [[fallthrough]];
    default: return TPrimitiveType::INVALID_TYPE;
  }
}

string TypeToString(PrimitiveType t) {
  switch (t) {
    case INVALID_TYPE: return "INVALID";
    case TYPE_NULL: return "NULL";
    case TYPE_BOOLEAN: return "BOOLEAN";
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
    case TYPE_VARCHAR: return "VARCHAR";
    case TYPE_BINARY: return "BINARY";
    case TYPE_DECIMAL: return "DECIMAL";
    case TYPE_CHAR: return "CHAR";
    case TYPE_UUID: return "UUID";
    case TYPE_FIXED_UDA_INTERMEDIATE: return "FIXED_UDA_INTERMEDIATE";
    case TYPE_STRUCT: return "STRUCT";
    case TYPE_ARRAY: return "ARRAY";
    case TYPE_MAP: return "MAP";
    case TYPE_VARIANT: return "VARIANT";
  };
  return "";
}

string TypeToOdbcString(const TColumnType& type) {
  DCHECK_EQ(1, type.types.size());
  DCHECK_EQ(TTypeNodeType::SCALAR, type.types[0].type);
  DCHECK(type.types[0].__isset.scalar_type);
  TPrimitiveType::type col_type = type.types[0].scalar_type.type;
  PrimitiveType primitive_type = ThriftToType(col_type);
  // ODBC driver requires types in lower case
  switch (primitive_type) {
    case INVALID_TYPE: return "invalid";
    case TYPE_NULL: return "null";
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
    case TYPE_STRING:
      switch (col_type) {
        case TPrimitiveType::GEOMETRY: return "geometry";
        case TPrimitiveType::BINARY: return "binary";
        default: return "string";
      }
    case TYPE_UUID: return "uuid";
    case TYPE_VARCHAR: return "string";
    case TYPE_DECIMAL: return "decimal";
    case TYPE_CHAR: return "char";
    case TYPE_STRUCT: return "struct";
    case TYPE_ARRAY: return "array";
    case TYPE_MAP: return "map";
    case TYPE_VARIANT: return "variant";
    case TYPE_BINARY:
    case TYPE_FIXED_UDA_INTERMEDIATE:
      // This type is not exposed to clients and should not be returned.
      DCHECK(false);
      break;
  };
  return "unknown";
}

void ColumnType::ToThrift(TColumnType* thrift_type) const {
  thrift_type->types.push_back(TTypeNode());
  TTypeNode& node = thrift_type->types.back();
  if (IsComplexType()) {
    if (type == TYPE_ARRAY) {
      node.type = TTypeNodeType::ARRAY;
    } else if (type == TYPE_MAP) {
      node.type = TTypeNodeType::MAP;
    } else if (type == TYPE_VARIANT) {
      node.type = TTypeNodeType::VARIANT;
      DCheckVariantShape();
      node.__set_variant_fields(vector<TStructField>());
      DCHECK_EQ(field_names.size(), field_ids.size());
      for (int i = 0; i < field_names.size(); i++) {
        node.variant_fields.push_back(TStructField());
        node.variant_fields.back().name = field_names[i];
        node.variant_fields.back().field_id = field_ids[i];
      }
    } else {
      DCHECK_EQ(type, TYPE_STRUCT);
      node.type = TTypeNodeType::STRUCT;
      node.__set_struct_fields(vector<TStructField>());
      DCHECK_EQ(field_names.size(), field_ids.size());
      for (int i=0; i<field_names.size(); i++) {
        node.struct_fields.push_back(TStructField());
        node.struct_fields.back().name = field_names[i];
        node.struct_fields.back().field_id = field_ids[i];
      }
    }
    for (const ColumnType& child: children) {
      child.ToThrift(thrift_type);
    }
  } else {
    node.type = TTypeNodeType::SCALAR;
    node.__set_scalar_type(TScalarType());
    TScalarType& scalar_type = node.scalar_type;
    scalar_type.__set_type(impala::ToThrift(type, string_val_subtype_));
    if (type == TYPE_CHAR || type == TYPE_VARCHAR
        || type == TYPE_FIXED_UDA_INTERMEDIATE) {
      DCHECK_NE(len, -1);
      scalar_type.__set_len(len);
    } else if (type == TYPE_DECIMAL) {
      DCHECK_NE(precision, -1);
      DCHECK_NE(scale, -1);
      scalar_type.__set_precision(precision);
      scalar_type.__set_scale(scale);
    }
  }
}

string ColumnType::DebugString() const {
  switch (type) {
    case TYPE_STRING:
      switch (string_val_subtype_) {
        case StringValSubtype::STRING: return "STRING";
        case StringValSubtype::BINARY: return "BINARY";
        case StringValSubtype::GEOMETRY: return "GEOMETRY";
        default:
          DCHECK(false) << "Unexpected string subtype" << string_val_subtype_;
          return "";
      }
    case TYPE_UUID:
      return "UUID";
    case TYPE_CHAR:
      return Substitute("CHAR($0)", len);
    case TYPE_DECIMAL:
      return Substitute("DECIMAL($0,$1)", precision, scale);
    case TYPE_VARCHAR:
      return Substitute("VARCHAR($0)", len);
    case TYPE_FIXED_UDA_INTERMEDIATE:
      return Substitute("FIXED_UDA_INTERMEDIATE($0)", len);
    default:
      return TypeToString(type);
  }
}

vector<ColumnType> ColumnType::FromThrift(const vector<TColumnType>& ttypes) {
  vector<ColumnType> types;
  types.reserve(ttypes.size());
  for (const TColumnType& ttype : ttypes) types.push_back(FromThrift(ttype));
  return types;
}

ostream& operator<<(ostream& os, const ColumnType& type) {
  os << type.DebugString();
  return os;
}

ostream& operator<<(ostream& os, StringValSubtype subtype) {
  switch (subtype) {
    case StringValSubtype::STRING: os << "STRING"; break;
    case StringValSubtype::BINARY: os << "BINARY"; break;
    default: os << "UNKNOWN(" << static_cast<int>(subtype) << ")"; break;
  }
  return os;
}

llvm::ConstantStruct* ColumnType::ToIR(LlvmCodeGen* codegen) const {
  // ColumnType = { i32, i32, i32, i32, <vector>, <vector>, <vector>, i8, <padding> }
  llvm::StructType* column_type_type = codegen->GetStructType<ColumnType>();

  DCHECK_EQ(sizeof(type), sizeof(int32_t));
  llvm::Constant* type_field = codegen->GetI32Constant(type);

  DCHECK_EQ(sizeof(len), sizeof(int32_t));
  llvm::Constant* len_field = codegen->GetI32Constant(len);
  DCHECK_EQ(sizeof(precision), sizeof(int32_t));
  llvm::Constant* precision_field = codegen->GetI32Constant(precision);
  DCHECK_EQ(sizeof(scale), sizeof(int32_t));
  llvm::Constant* scale_field = codegen->GetI32Constant(scale);

  // Create empty 'children', 'field_names' and 'field_ids' vectors
  DCHECK(children.empty()) << "Nested types NYI";
  DCHECK(field_names.empty()) << "Nested types NYI";
  DCHECK(field_ids.empty()) << "Nested types NYI";
  llvm::Constant* children_field =
      llvm::Constant::getNullValue(column_type_type->getElementType(4));
  llvm::Constant* field_names_field =
      llvm::Constant::getNullValue(column_type_type->getElementType(5));
  llvm::Constant* field_ids_field =
      llvm::Constant::getNullValue(column_type_type->getElementType(6));

  DCHECK_EQ(sizeof(string_val_subtype_), sizeof(uint8_t));
  llvm::Constant* string_val_subtype_field =
      codegen->GetI8Constant(static_cast<uint8_t>(string_val_subtype_));

  llvm::Constant* padding =
      llvm::Constant::getNullValue(column_type_type->getElementType(8));

  return llvm::cast<llvm::ConstantStruct>(
      llvm::ConstantStruct::get(column_type_type, type_field, len_field, precision_field,
        scale_field, children_field, field_names_field, field_ids_field,
        string_val_subtype_field, padding));
}

}
