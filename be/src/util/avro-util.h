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


#ifndef IMPALA_UTIL_AVRO_UTIL_H_
#define IMPALA_UTIL_AVRO_UTIL_H_

#include "common/status.h"
#include "runtime/types.h"

/// From avro/schema.h (copied here to avoid pulling in avro header)
struct avro_obj_t;
typedef struct avro_obj_t* avro_schema_t;

namespace impala {

class SlotDescriptor;

bool IsSupportedAvroType(const avro_schema_t& schema);

/// Internal representation of an Avro schema. The schema for a table or data file is a
/// tree of AvroSchemaElements, with the root element having type AVRO_RECORD.
struct AvroSchemaElement {
  /// The original avro_schema_t of this element
  avro_schema_t schema;

  /// Complex types, e.g. records, may have nested child types
  std::vector<AvroSchemaElement> children;

  /// Avro supports nullable types via unions of the form [<type>, "null"]. We
  /// special-case supported nullable types by storing which position "null" occupies in
  /// the union and setting 'schema' to the non-null type's schema, rather than the union
  /// schema.  null_union_position is set to 0 or 1 accordingly if this type is a union
  /// between a supported type and "null", and -1 otherwise.
  int null_union_position;

  /// The slot descriptor corresponding to this element. NULL if this element does not
  /// correspond to a materialized slot.
  const SlotDescriptor* slot_desc;

  AvroSchemaElement() : schema(NULL), null_union_position(-1), slot_desc(NULL) { }

  bool nullable() const { return null_union_position != -1; }

  /// Maps the Avro library's type representation to our own.
  /// The returned AvroSchemaElement does not have 'slot_desc' populated.
  /// Returns a bad status if an unsupported type is found.
  static Status ConvertSchema(const avro_schema_t& schema, AvroSchemaElement* element);

  static const char* LLVM_CLASS_NAME;
};

/// Wrapper around AvroSchemaElement that handles decrementing the ref count of the
/// avro_schema_t.
struct ScopedAvroSchemaElement {
  ScopedAvroSchemaElement() { }

  ~ScopedAvroSchemaElement();

  AvroSchemaElement* operator->() { return &element_; }
  const AvroSchemaElement* operator->() const { return &element_; }
  AvroSchemaElement* get() { return &element_; }
  const AvroSchemaElement* get() const { return &element_; }

 private:
  AvroSchemaElement element_;

  DISALLOW_COPY_AND_ASSIGN(ScopedAvroSchemaElement);
};

/// Converts an Avro schema column to an Impala type. Returns an error status if the Avro
/// schema does not map to a valid Impala type. 'column_name' is used only for error
/// messages.
Status AvroSchemaToColumnType(
    const avro_schema_t& schema, const std::string& column_name, ColumnType* column_type);
}

#endif
