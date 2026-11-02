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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.VariantType;
import org.junit.Test;

/**
 * Unit tests for {@link AvroSchemaConverter}, including Iceberg Avro reader schemas
 * built via {@link HdfsTable#buildAvroSchemaFromColumns}.
 */
public class AvroSchemaConverterTest {
  /** Impala Avro reader schemas wrap every column in [null, T]. */
  private static Schema unwrapNullableColumn(Schema fieldSchema) {
    if (fieldSchema.getType() != Schema.Type.UNION) return fieldSchema;
    List<Schema> types = fieldSchema.getTypes();
    assertEquals(2, types.size());
    if (types.get(0).getType() == Schema.Type.NULL) return types.get(1);
    if (types.get(1).getType() == Schema.Type.NULL) return types.get(0);
    fail("Expected [null, T] union");
    return null;
  }

  private static Schema uuidFieldSchema(Schema record) {
    return unwrapNullableColumn(record.getField("uuid_col").schema());
  }

  @Test
  public void testUuidColumnMapsToFixed16WithLogicalType() {
    List<Column> columns = Arrays.asList(
        new Column("uuid_col", Type.UUID, "", 0));
    Schema record = AvroSchemaConverter.convertColumns(columns, "test_table");
    Schema uuidSchema = uuidFieldSchema(record);
    assertEquals(Schema.Type.FIXED, uuidSchema.getType());
    assertEquals(Type.UUID.getSlotSize(), uuidSchema.getFixedSize());
    assertEquals("uuid", uuidSchema.getProp("logicalType"));
  }

  @Test
  public void testBuildAvroSchemaFromColumnsMatchesConverter() {
    List<Column> columns = Arrays.asList(
        new Column("id", Type.INT, "", 0),
        new Column("uuid_col", Type.UUID, "", 1),
        new Column("name", Type.STRING, "", 2));
    String schemaJson = HdfsTable.buildAvroSchemaFromColumns(columns, "db.table");
    Schema record = new Schema.Parser().parse(schemaJson);
    assertEquals(3, record.getFields().size());
    Schema uuidSchema = uuidFieldSchema(record);
    assertEquals(Schema.Type.FIXED, uuidSchema.getType());
    assertEquals("uuid", uuidSchema.getProp("logicalType"));
  }

  @Test
  public void testNestedStructUuidField() {
    StructType structType = new StructType(Arrays.asList(
        new StructField("uuid_col", Type.UUID, ""),
        new StructField("name", Type.STRING, "")));
    List<Column> columns = Arrays.asList(
        new Column("s", structType, "", 0));
    Schema record = AvroSchemaConverter.convertColumns(columns, "test_table");
    Schema structField = unwrapNullableColumn(record.getField("s").schema());
    assertEquals(Schema.Type.RECORD, structField.getType());
    Schema nestedUuid = unwrapNullableColumn(
        structField.getField("uuid_col").schema());
    assertEquals(Schema.Type.FIXED, nestedUuid.getType());
    assertEquals("uuid", nestedUuid.getProp("logicalType"));
  }

  @Test
  public void testVariantColumnMapsToMetadataValueRecord() {
    List<Column> columns = Arrays.asList(new Column("v", new VariantType(), "", 0));
    String schemaJson = HdfsTable.buildAvroSchemaFromColumns(columns, "db.table");
    Schema record = new Schema.Parser().parse(schemaJson);
    Schema variant = unwrapNullableColumn(record.getField("v").schema());
    assertEquals(Schema.Type.RECORD, variant.getType());
    assertEquals(2, variant.getFields().size());
    assertEquals(Schema.Type.BYTES,
        unwrapNullableColumn(variant.getField("metadata").schema()).getType());
    assertEquals(Schema.Type.BYTES,
        unwrapNullableColumn(variant.getField("value").schema()).getType());
  }

  @Test
  public void testColumnsHaveNullDefault() {
    List<Column> columns = Arrays.asList(
        new Column("id", Type.INT, "", 0),
        new Column("uuid_col", Type.UUID, "", 1));
    String schemaJson = HdfsTable.buildAvroSchemaFromColumns(columns, "db.table");
    Schema record = new Schema.Parser().parse(schemaJson);
    for (Schema.Field field : record.getFields()) {
      assertTrue(field.name(), field.hasDefaultValue());
      assertEquals(field.name(), JsonProperties.NULL_VALUE, field.defaultVal());
    }
  }

  @Test
  public void testNestedStructFieldsHaveNullDefault() {
    StructType structType = new StructType(Arrays.asList(
        new StructField("a", Type.INT, ""),
        new StructField("b", Type.STRING, "")));
    List<Column> columns = Arrays.asList(new Column("s", structType, "", 0));
    Schema record = AvroSchemaConverter.convertColumns(columns, "test_table");
    Schema structField = unwrapNullableColumn(record.getField("s").schema());
    assertEquals(2, structField.getFields().size());
    for (Schema.Field field : structField.getFields()) {
      assertTrue(field.name(), field.hasDefaultValue());
      assertEquals(field.name(), JsonProperties.NULL_VALUE, field.defaultVal());
    }
  }
}
