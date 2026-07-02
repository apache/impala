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

import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.types.Types;
import org.junit.Test;

/**
 * Tests for the patched HiveSchemaUtil that adds GEOMETRY/GEOGRAPHY support.
 */
public class IcebergHiveSchemaUtilTest {

  @Test
  public void testGeometryColumnConvertsToBinary() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "geom", Types.GeometryType.crs84()));
    List<FieldSchema> hiveSchema = HiveSchemaUtil.convert(schema);
    assertEquals(2, hiveSchema.size());
    assertEquals("id", hiveSchema.get(0).getName());
    assertEquals("int", hiveSchema.get(0).getType());
    assertEquals("geom", hiveSchema.get(1).getName());
    assertEquals("binary", hiveSchema.get(1).getType());
  }

  @Test
  public void testGeographyColumnConvertsToBinary() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "loc", Types.GeographyType.crs84()));
    List<FieldSchema> hiveSchema = HiveSchemaUtil.convert(schema);
    assertEquals(1, hiveSchema.size());
    assertEquals("loc", hiveSchema.get(0).getName());
    assertEquals("binary", hiveSchema.get(0).getType());
  }

  @Test
  public void testMixedSchemaWithGeometry() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "geom", Types.GeometryType.crs84()),
        Types.NestedField.optional(4, "data", Types.BinaryType.get()));
    List<FieldSchema> hiveSchema = HiveSchemaUtil.convert(schema);
    assertEquals(4, hiveSchema.size());
    assertEquals("int", hiveSchema.get(0).getType());
    assertEquals("string", hiveSchema.get(1).getType());
    assertEquals("binary", hiveSchema.get(2).getType());
    assertEquals("binary", hiveSchema.get(3).getType());
  }
}
