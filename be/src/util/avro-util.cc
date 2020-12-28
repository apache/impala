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

#include "util/avro-util.h"

#include <avro/schema.h>
#include <sstream>

using namespace std;

namespace impala {

const char* AvroSchemaElement::LLVM_CLASS_NAME = "struct.impala::AvroSchemaElement";

bool IsSupportedAvroType(const avro_schema_t& schema) {
  switch (schema->type) {
    case AVRO_STRING:
    case AVRO_BYTES:
    case AVRO_INT32:
    case AVRO_DATE:
    case AVRO_INT64:
    case AVRO_FLOAT:
    case AVRO_DOUBLE:
    case AVRO_BOOLEAN:
    case AVRO_NULL:
    case AVRO_RECORD:
    case AVRO_DECIMAL:
      return true;
    default:
      return false;
  }
}

Status AvroSchemaElement::ConvertSchema(
    const avro_schema_t& schema, AvroSchemaElement* element) {
  element->schema = schema;

  // Look for special case of [<supported type>, "null"] union
  if (element->schema->type == AVRO_UNION) {
    int num_fields = avro_schema_union_size(schema);
    DCHECK_GT(num_fields, 0);
    if (num_fields == 2) {
      const avro_schema_t& child0 = avro_schema_union_branch(schema, 0);
      const avro_schema_t& child1 = avro_schema_union_branch(schema, 1);
      int null_position = -1;
      if (child0->type == AVRO_NULL) {
        null_position = 0;
      } else if (child1->type == AVRO_NULL) {
        null_position = 1;
      }

      if (null_position != -1) {
        const avro_schema_t& non_null_child = null_position == 0 ? child1 : child0;
        AvroSchemaElement child;
        RETURN_IF_ERROR(ConvertSchema(non_null_child, &child));

        // 'schema' is a [<child>, "null"] union. Treat this node as the same type as
        // child except with null_union_position set appropriately.
        DCHECK_EQ(child.null_union_position, -1)
            << "Avro spec does not allow immediately nested unions";
        *element = child;
        element->null_union_position = null_position;
        return Status::OK();
      }
    }
  }

  if (!IsSupportedAvroType(element->schema)) {
    stringstream ss;
    ss << "Avro enum, array, map, union, and fixed types are not supported. "
       << "Got type: " << avro_type_name(element->schema->type);
    return Status(ss.str());
  }

  if (element->schema->type == AVRO_RECORD) {
    int num_fields = avro_schema_record_size(element->schema);
    element->children.resize(num_fields);
    for (int i = 0; i < num_fields; ++i) {
      avro_schema_t field =
          avro_schema_record_field_get_by_index(element->schema, i);
      RETURN_IF_ERROR(ConvertSchema(field, &element->children[i]));
    }
  }
  return Status::OK();
}

ScopedAvroSchemaElement::~ScopedAvroSchemaElement() {
  // avro_schema_decref can handle NULL. If element_.schema is a record or other complex
  // type, it will recursively decref it's children when it's free.
  avro_schema_decref(element_.schema);
}

Status AvroSchemaToColumnType(
    const avro_schema_t& schema, const string& column_name, ColumnType* column_type) {
  switch (schema->type) {
    case AVRO_BYTES:
    case AVRO_STRING:
      *column_type = ColumnType(TYPE_STRING);
      return Status::OK();
    case AVRO_INT32:
      *column_type = ColumnType(TYPE_INT);
      return Status::OK();
    case AVRO_DATE:
      *column_type = ColumnType(TYPE_DATE);
      return Status::OK();
    case AVRO_INT64:
      *column_type = ColumnType(TYPE_BIGINT);
      return Status::OK();
    case AVRO_FLOAT:
      *column_type = ColumnType(TYPE_FLOAT);
      return Status::OK();
    case AVRO_DOUBLE:
      *column_type = ColumnType(TYPE_DOUBLE);
      return Status::OK();
    case AVRO_BOOLEAN:
      *column_type = ColumnType(TYPE_BOOLEAN);
      return Status::OK();
    case AVRO_DECIMAL: {
      int precision = avro_schema_decimal_precision(schema);
      int scale = avro_schema_decimal_scale(schema);
      if (!ColumnType::ValidateDecimalParams(precision, scale)) {
        return Status(TErrorCode::AVRO_INVALID_DECIMAL, column_name, precision, scale);
      }
      *column_type = ColumnType::CreateDecimalType(precision, scale);
      return Status::OK();
    }
    default:
      return Status(TErrorCode::AVRO_UNSUPPORTED_TYPE, column_name, avro_type_name(schema->type));
  }
}
}
