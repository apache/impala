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
#include <sstream>

#include "gen-cpp/TCLIService_constants.h"
#include "codegen/llvm-codegen.h"

#include "common/names.h"

using namespace apache::hive::service::cli::thrift;

namespace impala {

const int ColumnType::MAX_PRECISION;
const int ColumnType::MAX_SCALE;
const int ColumnType::MIN_ADJUSTED_SCALE;

const int ColumnType::MAX_DECIMAL4_PRECISION;
const int ColumnType::MAX_DECIMAL8_PRECISION;

const char* ColumnType::LLVM_CLASS_NAME = "struct.impala::ColumnType";

ColumnType::ColumnType(const std::vector<TTypeNode>& types, int* idx)
  : len(-1), precision(-1), scale(-1) {
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
    default:
      DCHECK(false) << node.type;
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
    case TPrimitiveType::BINARY: return TYPE_BINARY;
    case TPrimitiveType::DECIMAL: return TYPE_DECIMAL;
    case TPrimitiveType::CHAR: return TYPE_CHAR;
    case TPrimitiveType::FIXED_UDA_INTERMEDIATE: return TYPE_FIXED_UDA_INTERMEDIATE;
    default: return INVALID_TYPE;
  }
}

TPrimitiveType::type ToThrift(PrimitiveType ptype) {
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
    case TYPE_STRING: return TPrimitiveType::STRING;
    case TYPE_VARCHAR: return TPrimitiveType::VARCHAR;
    case TYPE_BINARY: return TPrimitiveType::BINARY;
    case TYPE_DECIMAL: return TPrimitiveType::DECIMAL;
    case TYPE_CHAR: return TPrimitiveType::CHAR;
    case TYPE_FIXED_UDA_INTERMEDIATE: return TPrimitiveType::FIXED_UDA_INTERMEDIATE;
    case TYPE_STRUCT:
    case TYPE_ARRAY:
    case TYPE_MAP:
      DCHECK(false) << "NYI: " << ptype;
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
    case TYPE_FIXED_UDA_INTERMEDIATE: return "FIXED_UDA_INTERMEDIATE";
    case TYPE_STRUCT: return "STRUCT";
    case TYPE_ARRAY: return "ARRAY";
    case TYPE_MAP: return "MAP";
  };
  return "";
}

string TypeToOdbcString(PrimitiveType t) {
  // ODBC driver requires types in lower case
  switch (t) {
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
    case TYPE_STRING: return "string";
    case TYPE_VARCHAR: return "string";
    case TYPE_BINARY: return "binary";
    case TYPE_DECIMAL: return "decimal";
    case TYPE_CHAR: return "char";
    case TYPE_STRUCT: return "struct";
    case TYPE_ARRAY: return "array";
    case TYPE_MAP: return "map";
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
    } else {
      DCHECK_EQ(type, TYPE_STRUCT);
      node.type = TTypeNodeType::STRUCT;
      node.__set_struct_fields(vector<TStructField>());
      for (const string& field_name: field_names) {
        node.struct_fields.push_back(TStructField());
        node.struct_fields.back().name = field_name;
      }
    }
    for (const ColumnType& child: children) {
      child.ToThrift(thrift_type);
    }
  } else {
    node.type = TTypeNodeType::SCALAR;
    node.__set_scalar_type(TScalarType());
    TScalarType& scalar_type = node.scalar_type;
    scalar_type.__set_type(impala::ToThrift(type));
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

TTypeEntry ColumnType::ToHs2Type() const {
  TPrimitiveTypeEntry type_entry;
  switch (type) {
    // Map NULL_TYPE to BOOLEAN, otherwise Hive's JDBC driver won't
    // work for queries like "SELECT NULL" (IMPALA-914).
    case TYPE_NULL:
      type_entry.__set_type(TTypeId::BOOLEAN_TYPE);
      break;
    case TYPE_BOOLEAN:
      type_entry.__set_type(TTypeId::BOOLEAN_TYPE);
      break;
    case TYPE_TINYINT:
      type_entry.__set_type(TTypeId::TINYINT_TYPE);
      break;
    case TYPE_SMALLINT:
      type_entry.__set_type(TTypeId::SMALLINT_TYPE);
      break;
    case TYPE_INT:
      type_entry.__set_type(TTypeId::INT_TYPE);
      break;
    case TYPE_BIGINT:
      type_entry.__set_type(TTypeId::BIGINT_TYPE);
      break;
    case TYPE_FLOAT:
      type_entry.__set_type(TTypeId::FLOAT_TYPE);
      break;
    case TYPE_DOUBLE:
      type_entry.__set_type(TTypeId::DOUBLE_TYPE);
      break;
    case TYPE_TIMESTAMP:
      type_entry.__set_type(TTypeId::TIMESTAMP_TYPE);
      break;
    case TYPE_STRING:
      type_entry.__set_type(TTypeId::STRING_TYPE);
      break;
    case TYPE_BINARY:
      type_entry.__set_type(TTypeId::BINARY_TYPE);
      break;
    case TYPE_DECIMAL: {
      TTypeQualifierValue tprecision;
      tprecision.__set_i32Value(precision);
      TTypeQualifierValue tscale;
      tscale.__set_i32Value(scale);

      TTypeQualifiers type_quals;
      type_quals.qualifiers[g_TCLIService_constants.PRECISION] = tprecision;
      type_quals.qualifiers[g_TCLIService_constants.SCALE] = tscale;
      type_entry.__set_typeQualifiers(type_quals);
      type_entry.__set_type(TTypeId::DECIMAL_TYPE);
      break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
      TTypeQualifierValue tmax_len;
      tmax_len.__set_i32Value(len);

      TTypeQualifiers type_quals;
      type_quals.qualifiers[g_TCLIService_constants.CHARACTER_MAXIMUM_LENGTH] = tmax_len;
      type_entry.__set_typeQualifiers(type_quals);
      type_entry.__set_type(
          (type == TYPE_CHAR) ? TTypeId::CHAR_TYPE : TTypeId::VARCHAR_TYPE);
      break;
    }
    default:
      // HiveServer2 does not have a type for invalid, date, datetime or
      // fixed_uda_intermediate.
      DCHECK(false) << "bad TypeToTValueType() type: " << DebugString();
      type_entry.__set_type(TTypeId::STRING_TYPE);
  };

  TTypeEntry result;
  result.__set_primitiveEntry(type_entry);
  return result;
}

string ColumnType::DebugString() const {
  stringstream ss;
  switch (type) {
    case TYPE_CHAR:
      ss << "CHAR(" << len << ")";
      return ss.str();
    case TYPE_DECIMAL:
      ss << "DECIMAL(" << precision << "," << scale << ")";
      return ss.str();
    case TYPE_VARCHAR:
      ss << "VARCHAR(" << len << ")";
      return ss.str();
    case TYPE_FIXED_UDA_INTERMEDIATE:
      ss << "FIXED_UDA_INTERMEDIATE(" << len << ")";
      return ss.str();
    default:
      return TypeToString(type);
  }
}

vector<ColumnType> ColumnType::FromThrift(const vector<TColumnType>& ttypes) {
  vector<ColumnType> types;
  for (const TColumnType& ttype : ttypes) types.push_back(FromThrift(ttype));
  return types;
}

ostream& operator<<(ostream& os, const ColumnType& type) {
  os << type.DebugString();
  return os;
}

llvm::ConstantStruct* ColumnType::ToIR(LlvmCodeGen* codegen) const {
  // ColumnType = { i32, i32, i32, i32, <vector>, <vector> }
  llvm::StructType* column_type_type = codegen->GetStructType<ColumnType>();

  DCHECK_EQ(sizeof(type), sizeof(int32_t));
  llvm::Constant* type_field = codegen->GetI32Constant(type);
  DCHECK_EQ(sizeof(len), sizeof(int32_t));
  llvm::Constant* len_field = codegen->GetI32Constant(len);
  DCHECK_EQ(sizeof(precision), sizeof(int32_t));
  llvm::Constant* precision_field = codegen->GetI32Constant(precision);
  DCHECK_EQ(sizeof(scale), sizeof(int32_t));
  llvm::Constant* scale_field = codegen->GetI32Constant(scale);

  // Create empty 'children' and 'field_names' vectors
  DCHECK(children.empty()) << "Nested types NYI";
  DCHECK(field_names.empty()) << "Nested types NYI";
  llvm::Constant* children_field =
      llvm::Constant::getNullValue(column_type_type->getElementType(4));
  llvm::Constant* field_names_field =
      llvm::Constant::getNullValue(column_type_type->getElementType(5));

  return llvm::cast<llvm::ConstantStruct>(
      llvm::ConstantStruct::get(column_type_type, type_field, len_field, precision_field,
          scale_field, children_field, field_names_field, NULL));
}

}
