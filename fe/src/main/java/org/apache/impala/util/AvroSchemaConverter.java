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

import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.VariantType;
import com.google.common.collect.Lists;

/**
 * Utility class to generate an Impala-compatible Avro Schema from other schemas, e.g.,
 * an Impala table, a list of Impala columns, a list of Hive field schemas, etc.
 *
 * Error behavior: These functions throw an UnsupportedOperationException when failing
 * to generate an Impala-compatible Avro schema, e.g., because of an unknown type or a
 * type not supported by Impala.
 *
 * Behavior for TIMESTAMP:
 * A TIMESTAMP column definition maps to an Avro STRING and is created as a STRING column,
 * because Avro has no binary TIMESTAMP representation. As a result, no Avro table may
 * have a TIMESTAMP column.
 */
public class AvroSchemaConverter {
  // Arbitrarily chosen schema name and record prefix. Note that
  // record names must be unique within an Avro schema.
  private static final String DEFAULT_SCHEMA_NAME = "baseRecord";
  private static final String RECORD_NAME_PREFIX = "record_";

  // Constants for Avro logical types, in particular, for DECIMAL and DATE.
  private static final String AVRO_LOGICAL_TYPE = "logicalType";
  private static final String PRECISION_PROP_NAME = "precision";
  private static final String SCALE_PROP_NAME = "scale";
  private static final String AVRO_DECIMAL_TYPE = "decimal";
  private static final String AVRO_DATE_TYPE = "date";
  private static final String AVRO_UUID_LOGICAL_TYPE = "uuid";
  private static final int AVRO_UUID_FIXED_SIZE_BYTES = Type.UUID.getSlotSize();

  // Used to generate unique record names as required by Avro.
  private int recordCounter_ = 0;

  public static Schema convertColumns(
      List<Column> columns, String schemaName) {
    AvroSchemaConverter converter = new AvroSchemaConverter();
    return converter.convertColumnsImpl(columns, schemaName);
  }

  public static Schema convertColumnDefs(
      List<ColumnDef> colDefs, String schemaName) {
    AvroSchemaConverter converter = new AvroSchemaConverter();
    return converter.convertColumnDefsImpl(colDefs, schemaName);
  }

  public static Schema convertFieldSchemas(
      List<FieldSchema> fieldSchemas, String schemaName) {
    AvroSchemaConverter converter = new AvroSchemaConverter();
    return converter.convertFieldSchemasImpl(fieldSchemas, schemaName);
  }

  private Schema convertColumnsImpl(List<Column> columns, String schemaName) {
    List<Schema.Field> avroFields = Lists.newArrayList();
    for (Column column: columns) {
      // The NULL default lets the scanner read files written before a column was added.
      final Schema.Field avroField = new Schema.Field(column.getName(),
          createAvroSchema(column.getType()), column.getComment(), Schema.NULL_VALUE);
      avroFields.add(avroField);
    }
    return createAvroRecord(avroFields, schemaName);
  }

  private Schema convertColumnDefsImpl(List<ColumnDef> colDefs, String schemaName) {
    List<Schema.Field> avroFields = Lists.newArrayList();
    for (ColumnDef colDef: colDefs) {
      final Schema.Field avroField = new Schema.Field(colDef.getColName(),
          createAvroSchema(colDef.getType()), colDef.getComment(), null);
      avroFields.add(avroField);
    }
    return createAvroRecord(avroFields, schemaName);
  }

  private Schema convertFieldSchemasImpl(
      List<FieldSchema> fieldSchemas, String schemaName) {
    List<Schema.Field> avroFields = Lists.newArrayList();
    for (FieldSchema fs: fieldSchemas) {
      Type impalaType = Type.parseColumnType(fs.getType());
      if (impalaType == null) {
        throw new UnsupportedOperationException(
            fs.getType() + " is not a suppported Impala type");
      }
      final Schema.Field avroField = new Schema.Field(fs.getName(),
          createAvroSchema(impalaType), fs.getComment(), Schema.NULL_VALUE);
      avroFields.add(avroField);
    }
    return createAvroRecord(avroFields, schemaName);
  }

  private Schema createAvroRecord(List<Schema.Field> avroFields, String schemaName) {
    // Name is a required property for an Avro Record.
    if (schemaName == null || schemaName.isEmpty()) schemaName = DEFAULT_SCHEMA_NAME;
    Schema schema = Schema.createRecord(schemaName, null, null, false);
    schema.setFields(avroFields);
    return schema;
  }

  private Schema createAvroSchema(Type impalaType) {
    Schema schema = null;
    if (impalaType.isScalarType()) {
      schema = createScalarSchema((ScalarType) impalaType);
    } else if (impalaType.isArrayType()) {
      schema = createArraySchema((ArrayType) impalaType);
    } else if (impalaType.isMapType()) {
      schema = createMapSchema((MapType) impalaType);
    } else if (impalaType.isStructType()) {
      schema = createRecordSchema(((StructType) impalaType).getFields());
    } else if (impalaType.isVariantType()) {
      // Same record{metadata, value} layout as the VARIANT struct stored in HMS.
      schema = createRecordSchema(((VariantType) impalaType).getFields());
    } else {
      throw new UnsupportedOperationException(
          impalaType.toSql() + " cannot be converted to an Avro type");
    }
    // Make the Avro schema nullable.
    Schema nullSchema = Schema.create(Schema.Type.NULL);
    return Schema.createUnion(Arrays.asList(nullSchema, schema));
  }

  private Schema createScalarSchema(ScalarType impalaScalarType) {
    switch (impalaScalarType.getPrimitiveType()) {
      case STRING: return Schema.create(Schema.Type.STRING);
      case CHAR: return Schema.create(Schema.Type.STRING);
      case VARCHAR: return Schema.create(Schema.Type.STRING);
      case BINARY: return Schema.create(Schema.Type.BYTES);
      case TINYINT: return Schema.create(Schema.Type.INT);
      case SMALLINT: return Schema.create(Schema.Type.INT);
      case INT: return Schema.create(Schema.Type.INT);
      case BIGINT: return Schema.create(Schema.Type.LONG);
      case BOOLEAN: return Schema.create(Schema.Type.BOOLEAN);
      case FLOAT: return Schema.create(Schema.Type.FLOAT);
      case DOUBLE: return Schema.create(Schema.Type.DOUBLE);
      case TIMESTAMP: return Schema.create(Schema.Type.STRING);
      case DATE: return createDateSchema();
      case DECIMAL: return createDecimalSchema(impalaScalarType);
      case UUID: return createUuidSchema();
      default:
        throw new UnsupportedOperationException(
            impalaScalarType.toSql() + " cannot be converted to an Avro type");
    }
  }

  private Schema createDateSchema() {
    Schema dateSchema = Schema.create(Schema.Type.INT);
    dateSchema.addProp(AVRO_LOGICAL_TYPE, AVRO_DATE_TYPE);
    return dateSchema;
  }

  private Schema createDecimalSchema(ScalarType impalaDecimalType) {
    Schema decimalSchema = Schema.create(Schema.Type.BYTES);
    decimalSchema.addProp(AVRO_LOGICAL_TYPE, AVRO_DECIMAL_TYPE);
    // precision and scale must be integer values
    decimalSchema.addProp(PRECISION_PROP_NAME, impalaDecimalType.decimalPrecision());
    decimalSchema.addProp(SCALE_PROP_NAME, impalaDecimalType.decimalScale());
    return decimalSchema;
  }

  private Schema createUuidSchema() {
    Schema uuidSchema = Schema.createFixed(
        RECORD_NAME_PREFIX + "uuid_" + recordCounter_++, null, null,
        AVRO_UUID_FIXED_SIZE_BYTES);
    uuidSchema.addProp(AVRO_LOGICAL_TYPE, AVRO_UUID_LOGICAL_TYPE);
    return uuidSchema;
  }

  private Schema createArraySchema(ArrayType impalaArrayType) {
    Schema elementSchema = createAvroSchema(impalaArrayType.getItemType());
    return Schema.createArray(elementSchema);
  }

  private Schema createMapSchema(MapType impalaMapType) {
    // Map keys are always STRING according to the Avro spec.
    Schema valueSchema = createAvroSchema(impalaMapType.getValueType());
    return Schema.createMap(valueSchema);
  }

  private Schema createRecordSchema(List<StructField> fields) {
    List<Schema.Field> schemaFields = Lists.newArrayList();
    for (StructField structField : fields) {
      // The NULL default lets the scanner read files written before a field was added
      // to the struct, as for top-level columns.
      Schema.Field avroField = new Schema.Field(structField.getName(),
          createAvroSchema(structField.getType()), structField.getComment(),
          Schema.NULL_VALUE);
      schemaFields.add(avroField);
    }
    // All Avro records in a table must have the name property.
    Schema structSchema = Schema.createRecord(
        RECORD_NAME_PREFIX + recordCounter_, null, null, false);
    ++recordCounter_;
    structSchema.setFields(schemaFields);
    return structSchema;
  }
}
