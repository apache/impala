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

package org.apache.impala.util;

import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.STRING;

import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.TypeDef;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

/**
 * Utility class used to parse Avro schema. Checks that the schema is valid
 * and performs mapping of Avro types to Impala types.
 * Note: This code is loosely based off the parsing code in the Hive AvroSerDe.
 */
public class AvroSchemaParser {
  // Map of Avro to Impala primitive types.
  private static final Map<Schema.Type, Type> avroToImpalaPrimitiveTypeMap_;
  static {
    Map<Schema.Type, Type> typeMap = new Hashtable<Schema.Type, Type>();
    typeMap.put(STRING, Type.STRING);
    typeMap.put(INT, Type.INT);
    typeMap.put(BOOLEAN, Type.BOOLEAN);
    typeMap.put(LONG, Type.BIGINT);
    typeMap.put(FLOAT, Type.FLOAT);
    typeMap.put(DOUBLE, Type.DOUBLE);
    avroToImpalaPrimitiveTypeMap_ = Collections.unmodifiableMap(typeMap);
  }

  /**
   * Parses the Avro schema string literal, mapping the Avro types to Impala types.
   * Returns a list of ColumnDef objects with their name and type info set.
   * Throws an AnalysisException if the Avro type maps to a type that Impala
   * does not yet support.
   * Throws a SchemaParseException if the Avro schema was invalid.
   */
  public static List<ColumnDef> parse(String schemaStr)
      throws SchemaParseException, AnalysisException {
    Schema.Parser avroSchemaParser = new Schema.Parser();
    Schema schema = avroSchemaParser.parse(schemaStr);
    if (!schema.getType().equals(Schema.Type.RECORD)) {
      throw new UnsupportedOperationException("Schema for table must be of type " +
          "RECORD. Received type: " + schema.getType());
    }
    List<ColumnDef> colDefs = Lists.newArrayListWithCapacity(schema.getFields().size());
    for (Schema.Field field: schema.getFields()) {
      Map<ColumnDef.Option, Object> option = Maps.newHashMap();
      String comment = field.doc();
      if (comment != null) option.put(ColumnDef.Option.COMMENT, comment);
      ColumnDef colDef = new ColumnDef(field.name(),
          new TypeDef(getTypeInfo(field.schema(), field.name())), option);
      colDef.analyze(null);
      colDefs.add(colDef);
    }
    return colDefs;
  }

  /**
   * Parses the given Avro schema and returns the matching Impala type
   * for this field. Handles primitive and complex types.
   */
  private static Type getTypeInfo(Schema schema, String colName)
      throws AnalysisException {
    // Avro requires NULLable types to be defined as unions of some type T
    // and NULL.  This is annoying and we're going to hide it from the user.
    if (isNullableType(schema)) {
      return getTypeInfo(getColumnType(schema), colName);
    }

    Schema.Type type = schema.getType();
    if (type == INT) {
      String logicalType = schema.getProp("logicalType");
      // DATE is stored in Avro as INT.
      if (logicalType != null && logicalType.equalsIgnoreCase("date")) return Type.DATE;
    }
    if (avroToImpalaPrimitiveTypeMap_.containsKey(type)) {
      return avroToImpalaPrimitiveTypeMap_.get(type);
    }

    switch(type) {
      case ARRAY:
        Type itemType = getTypeInfo(schema.getElementType(), colName);
        return new ArrayType(itemType);
      case MAP:
        Type valueType = getTypeInfo(schema.getValueType(), colName);
        return new MapType(Type.STRING, valueType);
      case RECORD:
        StructType structType = new StructType();
        for (Schema.Field field: schema.getFields()) {
          Type fieldType = getTypeInfo(field.schema(), colName);
          structType.addField(new StructField(field.name(), fieldType, field.doc()));
        }
        return structType;
      case BYTES:
        String logicalType = schema.getProp("logicalType");
        if (logicalType == null) return Type.BINARY;
        // Decimal is stored in Avro as a BYTE.
        if (logicalType.equalsIgnoreCase("decimal")) {
          return getDecimalType(schema);
        } else {
          throw new AnalysisException(String.format(
            "Unsupported logicalType: '%s' for column '%s' with type BYTES",
             logicalType, colName));
        }
      // TODO: Add support for stored Avro UNIONs by exposing them as STRUCTs in Impala.
      case UNION:
      case ENUM:
      case FIXED:
      case NULL:
      default: {
        throw new AnalysisException(String.format(
            "Unsupported type '%s' of column '%s'", type.getName(), colName));
      }
    }
  }

  /**
   * Returns true if this is a nullable type (a Union[T, Null]), false otherwise.
   */
  private static boolean isNullableType(Schema schema) {
    // [null, null] not allowed, so this check is ok.
    return schema.getType().equals(Schema.Type.UNION) && schema.getTypes().size() == 2 &&
        (schema.getTypes().get(0).getType().equals(Schema.Type.NULL) ||
         schema.getTypes().get(1).getType().equals(Schema.Type.NULL));
  }

  /**
   * If a nullable type, get the schema for the non-nullable type which will
   * provide Impala column type information.
   */
  private static Schema getColumnType(Schema schema) {
    List<Schema> types = schema.getTypes();
    return types.get(0).getType().equals(Schema.Type.NULL) ? types.get(1) : types.get(0);
  }

  /**
   * Attempts to parse decimal type information from the Avro schema, returning
   * a decimal ColumnType if successful.
   * Decimal is defined in Avro as a BYTE type with the logicalType property
   * set to "decimal" and a specified scale/precision.
   * Throws a SchemaParseException if the logicType=decimal, but scale/precision
   * is not specified or in the incorrect format.
   */
  private static Type getDecimalType(Schema schema) {
    Preconditions.checkState(schema.getType() == Schema.Type.BYTES);
    // Parse the scale/precision of the decimal type.
    Integer scale = getDecimalProp(schema, "scale");
    // The Avro spec states that scale should default to zero if not set.
    if (scale == null) scale = 0;

    // Precision is a required property according to the Avro spec.
    Integer precision = getDecimalProp(schema, "precision");
    if (precision == null) {
      throw new SchemaParseException(
          "No 'precision' property specified for 'decimal' logicalType");
    }
    return ScalarType.createDecimalType(precision, scale);
  }

  /**
   * Parses a decimal property and returns the value as an integer, or null
   * if the property isn't set. Used to parse decimal scale/precision.
   * Throws a SchemaParseException if the property doesn't parse to a
   * natural number.
   */
  private static Integer getDecimalProp(Schema schema, String propName) {
    Object node = schema.getObjectProp(propName);
    if (node == null) return null;
    Integer value = -1;
    if (node instanceof Integer) value = (Integer) node;
    if (node instanceof Double) value = ((Double) node).intValue();
    if (value < 0) {
      throw new SchemaParseException(String.format("Invalid decimal '%s' " +
          "property value: %s", propName, node));
    }
    return value;
  }
}
