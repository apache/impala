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

#include <algorithm>
#include <sstream>

#include <avro/schema.h>
#include <boost/container/small_vector.hpp>

#include "util/uuid-util.h"

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
    case AVRO_FIXED:
      return avro_schema_fixed_size(schema) == UUID_BYTE_LEN;
    default:
      return false;
  }
}

/// Records enclosing the schema node being converted; its size is the nesting depth.
typedef boost::container::small_vector<avro_schema_t, 8> AvroEnclosingRecords;

static Status ConvertSchemaRecursive(const avro_schema_t& schema,
    AvroSchemaElement* element, AvroEnclosingRecords* enclosing_records) {
  // A reference to an already defined named type is an AVRO_LINK. A link to an
  // enclosing record is a recursive type, which Impala cannot represent.
  if (schema->type == AVRO_LINK) {
    avro_schema_t target = avro_schema_link_target(schema);
    if (std::find(enclosing_records->begin(), enclosing_records->end(), target)
        != enclosing_records->end()) {
      stringstream ss;
      ss << "Recursive Avro schemas are not supported. Got recursive reference to: "
         << avro_schema_name(target);
      return Status(ss.str());
    }
    return ConvertSchemaRecursive(target, element, enclosing_records);
  }

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
        RETURN_IF_ERROR(
            ConvertSchemaRecursive(non_null_child, &child, enclosing_records));

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
    ss << "Avro enum, array, map, unions other than [<type>, \"null\"], and fixed types "
       << "with a size other than " << UUID_BYTE_LEN << " are not supported. Got type: "
       << avro_type_name(element->schema->type);
    if (element->schema->type == AVRO_FIXED) {
      ss << "(" << avro_schema_fixed_size(element->schema) << ")";
    }
    return Status(ss.str());
  }

  if (element->schema->type == AVRO_RECORD) {
    int num_fields = avro_schema_record_size(element->schema);
    element->children.resize(num_fields);
    enclosing_records->push_back(element->schema);
    for (int i = 0; i < num_fields; ++i) {
      avro_schema_t field =
          avro_schema_record_field_get_by_index(element->schema, i);
      RETURN_IF_ERROR(
          ConvertSchemaRecursive(field, &element->children[i], enclosing_records));
    }
    enclosing_records->pop_back();
  }
  return Status::OK();
}

Status AvroSchemaElement::ConvertSchema(
    const avro_schema_t& schema, AvroSchemaElement* element) {
  AvroEnclosingRecords enclosing_records;
  return ConvertSchemaRecursive(schema, element, &enclosing_records);
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
    case AVRO_FIXED: {
      // avro-c drops 'logicalType' from fixed schemas, so a fixed(16) UUID cannot be
      // told apart from a plain fixed(16) or a decimal stored as fixed(16). A file field
      // of this type therefore only resolves against a UUID table column.
      if (avro_schema_fixed_size(schema) == UUID_BYTE_LEN) {
        *column_type = ColumnType::CreateUuidType();
        return Status::OK();
      }
      return Status(TErrorCode::AVRO_UNSUPPORTED_TYPE, column_name,
          avro_type_name(schema->type));
    }
    default:
      return Status(TErrorCode::AVRO_UNSUPPORTED_TYPE, column_name, avro_type_name(schema->type));
  }
}
}
