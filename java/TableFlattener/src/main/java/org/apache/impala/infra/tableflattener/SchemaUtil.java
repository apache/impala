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

package org.apache.impala.infra.tableflattener;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;

public class SchemaUtil {

  // Used to validate unions. This is a substitution map for comparison purposes.
  static final Map<Type, Type> BASE_TYPES = ImmutableMap.<Type, Type>builder()
      .put(Type.STRING, Type.BYTES)
      .put(Type.FIXED, Type.BYTES)
      .put(Type.DOUBLE, Type.INT)
      .put(Type.FLOAT, Type.INT)
      .put(Type.LONG, Type.INT)
      .build();

  static Field createField(String name, Type type) {
    return createField(name, type, null, null);
  }

  static Field createField(String name, Type type, String doc, Object defaultValue) {
    return new Field(name, Schema.createUnion(
        Schema.create(Type.NULL), Schema.create(type)), doc, defaultValue);
  }

  static Field createField(String name, Schema schema) {
    return createField(name, schema, null, null);
  }

  static Field createField(String name, Schema schema, String doc, Object defaultValue) {
    Preconditions.checkState(!schemaHasNesting(schema));
    if (schema.getType() == Type.UNION) {
      return new Field(name, Schema.createUnion(schema.getTypes()), doc, defaultValue);
    }
    return createField(name, schema.getType(), doc, defaultValue);
  }

  static boolean recordHasField(GenericRecord record, String fieldName) {
    return record.getSchema().getField(fieldName) != null;
  }

  static Schema reduceUnionToNonNull(Schema unionSchema) {
    Schema reducedSchema = null;
    Type reducedBaseType = null;
    for (Schema schema : unionSchema.getTypes()) {
      if (schema.getType() == Type.NULL) continue;
      String logicalType = schema.getProp("logicalType");
      Type baseType;
      if (logicalType == null) {
        baseType = BASE_TYPES.containsKey(schema.getType()) ?
            BASE_TYPES.get(schema.getType()) : schema.getType();
      } else {
        Preconditions.checkState(logicalType.equals("decimal"));
        baseType = Type.INT;
      }
      if (reducedBaseType == null) {
        reducedSchema = schema;
        reducedBaseType = baseType;
        continue;
      }
      if (reducedBaseType != baseType) {
        throw new RuntimeException(String.format(
            "Union contains incompatible types: %s",
            Joiner.on(" ,").join(unionSchema.getTypes())));
      }
    }
    if (reducedSchema == null) {
      throw new RuntimeException(String.format(
          "Union schema contains no non-null types: %s",
          Joiner.on(" ,").join(unionSchema.getTypes())));
    }
    return reducedSchema;
  }

  static boolean isNullable(Schema schema) {
    return schema.getType() == Type.NULL
        || (schema.getType() == Type.UNION && unionIsNullable(schema));
  }

  static boolean unionIsNullable(Schema unionSchema) {
    for (Schema schema : unionSchema.getTypes()) {
      if (schema.getType() == Type.NULL) return true;
    }
    return false;
  }

  static boolean isComplexType(Type type) {
    Preconditions.checkState(type != Type.UNION);
    return type == Type.ARRAY || type == Type.MAP || type == Type.RECORD;
  }

  static boolean isSimpleType(Schema schema) {
    if (schema.getType() == Type.UNION) schema = reduceUnionToNonNull(schema);
    return !isComplexType(schema.getType());
  }

  static boolean requiresChildDataset(Schema schema) {
    if (schema.getType() == Type.UNION) schema = reduceUnionToNonNull(schema);
    return schema.getType() == Type.ARRAY || schema.getType() == Type.MAP;
  }

  static boolean schemaHasNesting(Schema schema) {
    if (schema.getType() == Type.UNION) schema = reduceUnionToNonNull(schema);
    return isComplexType(schema.getType());
  }
}
