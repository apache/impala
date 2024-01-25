
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

import static org.apache.impala.thrift.TIcebergCatalog.CATALOGS;
import static org.apache.impala.thrift.TIcebergCatalog.HADOOP_CATALOG;
import static org.apache.impala.thrift.TIcebergCatalog.HADOOP_TABLES;
import static org.apache.impala.thrift.TIcebergCatalog.HIVE_CATALOG;
import static org.apache.impala.util.IcebergUtil.getDateTimeTransformValue;
import static org.apache.impala.util.IcebergUtil.getFilePathHash;
import static org.apache.impala.util.IcebergUtil.getIcebergFileFormat;
import static org.apache.impala.util.IcebergUtil.getPartitionTransform;
import static org.apache.impala.util.IcebergUtil.getPartitionTransformParams;
import static org.apache.impala.util.IcebergUtil.isPartitionColumn;
import static org.apache.impala.util.IcebergUtil.toHdfsFileFormat;
import static org.apache.impala.util.IcebergUtil.toTHdfsFileFormat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergPartitionField;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.analysis.IcebergPartitionTransform;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.iceberg.IcebergCatalog;
import org.apache.impala.catalog.iceberg.IcebergCatalogs;
import org.apache.impala.catalog.iceberg.IcebergHadoopCatalog;
import org.apache.impala.catalog.iceberg.IcebergHadoopTables;
import org.apache.impala.catalog.iceberg.IcebergHiveCatalog;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TIcebergPartitionTransformType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

/**
 * Unit tests for Iceberg Utilities.
 */
public class IcebergUtilTest {
  /**
   * Unit test for IcebergUtil.getTIcebergCatalog() and IcebergUtil.getIcebergCatalog().
   */
  @Test
  public void testGetCatalog() throws ImpalaRuntimeException {
    CatalogMapping[] mappings = new CatalogMapping[] {
        new CatalogMapping("hadoop.tables", HADOOP_TABLES, IcebergHadoopTables.class),
        new CatalogMapping("hadoop.catalog", HADOOP_CATALOG, IcebergHadoopCatalog.class),
        new CatalogMapping("hive.catalog", HIVE_CATALOG, IcebergHiveCatalog.class),
        new CatalogMapping(null, HIVE_CATALOG, IcebergHiveCatalog.class),
        new CatalogMapping("other string", CATALOGS, IcebergCatalogs.class),
    };
    for (CatalogMapping testValue : mappings) {
      TIcebergCatalog catalog = IcebergUtil.getTIcebergCatalog(testValue.propertyName);
      assertEquals("err for " + testValue.propertyName, testValue.catalog, catalog);
      IcebergCatalog impl = IcebergUtil.getIcebergCatalog(catalog, "location");
      assertEquals("err for " + testValue.propertyName, testValue.clazz, impl.getClass());
    }
  }

  /**
   * Unit test for IcebergUtil.getIcebergTableIdentifier().
   */
  @Test
  public void testGetIcebergTableIdentifier() {
    // Test a table with no table properties.
    Table table = new Table();
    table.setParameters(new HashMap<>());
    String tableName = "table_name";
    table.setTableName(tableName);
    String dbname = "database_name";
    table.setDbName(dbname);
    TableIdentifier icebergTableIdentifier = IcebergUtil.getIcebergTableIdentifier(table);
    assertEquals(
        TableIdentifier.parse("database_name.table_name"), icebergTableIdentifier);

    // If iceberg.table_identifier is not set then the value of the "name" property
    // is used.
    String nameId = "db.table";
    table.putToParameters(Catalogs.NAME, nameId);
    icebergTableIdentifier = IcebergUtil.getIcebergTableIdentifier(table);
    assertEquals(TableIdentifier.parse(nameId), icebergTableIdentifier);

    // If iceberg.table_identifier is set then that is used.
    String tableId = "foo.bar";
    table.putToParameters(IcebergTable.ICEBERG_TABLE_IDENTIFIER, tableId);
    icebergTableIdentifier = IcebergUtil.getIcebergTableIdentifier(table);
    assertEquals(TableIdentifier.parse(tableId), icebergTableIdentifier);

    // If iceberg.table_identifier set to a simple name, then the default catalog is used.
    table.putToParameters(IcebergTable.ICEBERG_TABLE_IDENTIFIER, "noDatabase");
    icebergTableIdentifier = IcebergUtil.getIcebergTableIdentifier(table);
    assertEquals(TableIdentifier.parse("default.noDatabase"), icebergTableIdentifier);
  }

  /**
   * Unit test for isHiveCatalog().
   */
  @Test
  public void testIsHiveCatalog() {
    CatalogType[] catalogTypes = new CatalogType[] {
        // For hadoop.tables amd hadoop.catalog we are not using Hive Catalog.
        new CatalogType("hadoop.tables", false),
        new CatalogType("hadoop.catalog", false),
        // For all other values of ICEBERG_CATALOG then Hive Catalog is used.
        new CatalogType("hive.catalog", true),
        new CatalogType(null, true),
        new CatalogType("other string", true),
    };
    for (CatalogType testValue : catalogTypes) {
      Table table = new Table();
      table.putToParameters(IcebergTable.ICEBERG_CATALOG, testValue.propertyName);
      assertEquals("err in " + testValue.propertyName, testValue.isHiveCatalog,
          IcebergUtil.isHiveCatalog(table));
    }
  }

  /**
   * Unit test for getIcebergFileFormat(), toHdfsFileFormat() and toTHdfsFileFormat().
   */
  @Test
  public void testToHdfsFileFormat() {
    assertEquals(THdfsFileFormat.ORC, toTHdfsFileFormat(TIcebergFileFormat.ORC));
    assertEquals(THdfsFileFormat.PARQUET, toTHdfsFileFormat(TIcebergFileFormat.PARQUET));
    assertEquals(HdfsFileFormat.ORC, toHdfsFileFormat(TIcebergFileFormat.ORC));
    assertEquals(HdfsFileFormat.PARQUET, toHdfsFileFormat(TIcebergFileFormat.PARQUET));
    assertEquals(HdfsFileFormat.ORC, toHdfsFileFormat("ORC"));
    assertEquals(HdfsFileFormat.PARQUET, toHdfsFileFormat("PARQUET"));
    assertEquals(HdfsFileFormat.PARQUET, toHdfsFileFormat((String) null));
    try {
      toHdfsFileFormat("unknown");
      fail("did not get expected assertion");
    } catch (IllegalArgumentException e) {
      // fall through
    }
    assertEquals(TIcebergFileFormat.ORC, getIcebergFileFormat("ORC"));
    assertEquals(TIcebergFileFormat.PARQUET, getIcebergFileFormat("PARQUET"));
    assertNull(getIcebergFileFormat("unknown"));
  }

  /**
   * Unit test forgetPartitionTransform().
   */
  @Test
  public void testGetPartitionTransform() {
    // Case 1
    // Transforms that work OK.
    PartitionTransform[] goodTransforms = new PartitionTransform[] {
        new PartitionTransform("BUCKET", 5),
        new PartitionTransform("TRUNCATE", 4),
        new PartitionTransform("HOUR", null),
        new PartitionTransform("HOURS", null),
        new PartitionTransform("DAY", null),
        new PartitionTransform("DAYS", null),
        new PartitionTransform("MONTH", null),
        new PartitionTransform("MONTHS", null),
        new PartitionTransform("YEAR", null),
        new PartitionTransform("YEARS", null),
        new PartitionTransform("VOID", null),
        new PartitionTransform("IDENTITY", null),
    };
    for (PartitionTransform partitionTransform : goodTransforms) {
      IcebergPartitionTransform transform = null;
      try {
        transform = getPartitionTransform(
            partitionTransform.transformName, partitionTransform.parameter);
      } catch (ImpalaRuntimeException t) {
        fail("Transform " + partitionTransform + " caught unexpected  " + t);
      }
      assertNotNull(transform);
      try {
        transform.analyze(null);
      } catch (AnalysisException t) {
        fail("Transform " + partitionTransform + " caught unexpected  " + t);
      }
    }

    // Case 2
    // Transforms that get TableLoadingException.
    PartitionTransform[] tableExceptions = new PartitionTransform[] {
        new PartitionTransform("JUNK", -5),
    };
    for (PartitionTransform partitionTransform : tableExceptions) {
      try {
        /* IcebergPartitionTransform transform = */ getPartitionTransform(
            partitionTransform.transformName, partitionTransform.parameter);
        fail("Transform " + partitionTransform + " should have got exception");
      } catch (ImpalaRuntimeException t) {
        // OK, fall through
      }
    }

    // Case 3
    // Transforms that fail analysis.
    PartitionTransform[] failAnalysis = new PartitionTransform[] {
        new PartitionTransform("BUCKET", -5),
        new PartitionTransform("TRUNCATE", -4),
    };
    for (PartitionTransform partitionTransform : failAnalysis) {
      IcebergPartitionTransform transform = null;
      try {
        transform = getPartitionTransform(
            partitionTransform.transformName, partitionTransform.parameter);
      } catch (ImpalaRuntimeException t) {
        fail("Transform " + partitionTransform + " caught unexpected  " + t);
      }
      assertNotNull(transform);
      try {
        transform.analyze(null);
        fail("Transform " + partitionTransform + " should have got exception");
      } catch (AnalysisException t) {
        // OK, fall through
      }
    }
  }

  /**
   * Unit test for getDataFilePathHash().
   */
  @Test
  public void testGetDataFilePathHash() {
    String hash = getFilePathHash(FILE_A);
    assertNotNull(hash);
    String hash2 = getFilePathHash(FILE_A);
    assertEquals(hash, hash2);
  }

  /**
   * Unit test for getPartitionTransformParams().
   */
  @Test
  public void testGetPartitionTransformParams() {
    int numBuckets = 128;
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(SCHEMA).bucket("i", numBuckets).build();
    Map<String, Integer> partitionTransformParams =
        getPartitionTransformParams(partitionSpec);
    assertNotNull(partitionTransformParams);
    String expectedKey = "1_BUCKET";
    assertTrue(partitionTransformParams.containsKey(expectedKey));
    assertEquals(numBuckets, (long) partitionTransformParams.get(expectedKey));
  }

  /**
   * Unit test for isPartitionColumn().
   */
  @Test
  public void testIsPartitionColumn() {
    {
      // Case 1
      // No partition fields: isPartitionColumn() should return false.
      int fieldId = 3;
      IcebergColumn column =
          new IcebergColumn("name", Type.BOOLEAN, "comment", 0, fieldId, 5, 0, true);
      List<IcebergPartitionField> fieldList = new ArrayList<>();
      IcebergPartitionSpec icebergPartitionSpec = new IcebergPartitionSpec(4, fieldList);
      assertFalse(isPartitionColumn(column, icebergPartitionSpec));
    }
    {
      // Case 2
      // A partition field source id matches a column field id: isPartitionColumn() should
      // return true.
      int id = 3;
      IcebergColumn column =
          new IcebergColumn("name", Type.BOOLEAN, "comment", 0, id, 105, 0, true);
      IcebergPartitionTransform icebergPartitionTransform =
          new IcebergPartitionTransform(TIcebergPartitionTransformType.IDENTITY);
      IcebergPartitionField field =
          new IcebergPartitionField(id, 106, "name", "name", icebergPartitionTransform,
              column.getType());
      ImmutableList<IcebergPartitionField> fieldList = ImmutableList.of(field);
      IcebergPartitionSpec icebergPartitionSpec = new IcebergPartitionSpec(4, fieldList);
      assertTrue(isPartitionColumn(column, icebergPartitionSpec));
    }
    {
      // Case 3
      // Partition field source id does not match a column field id: isPartitionColumn()
      // should return false.
      IcebergColumn column =
          new IcebergColumn("name", Type.BOOLEAN, "comment", 0, 108, 105, 0, true);
      IcebergPartitionTransform icebergPartitionTransform =
          new IcebergPartitionTransform(TIcebergPartitionTransformType.IDENTITY);
      IcebergPartitionField field =
          new IcebergPartitionField(107, 106, "name", "name", icebergPartitionTransform,
              column.getType());
      ImmutableList<IcebergPartitionField> fieldList = ImmutableList.of(field);
      IcebergPartitionSpec icebergPartitionSpec = new IcebergPartitionSpec(4, fieldList);
      assertFalse(isPartitionColumn(column, icebergPartitionSpec));
    }
  }

  /**
   * Unit test for getDateTransformValue
   */
  @Test
  public void testGetDateTransformValue() {

    assertThrows(() -> getDateTimeTransformValue(TIcebergPartitionTransformType.IDENTITY,
        "string"));
    assertThrows(
        () -> getDateTimeTransformValue(TIcebergPartitionTransformType.BUCKET, "string"));
    assertThrows(() -> getDateTimeTransformValue(TIcebergPartitionTransformType.TRUNCATE,
        "string"));
    assertThrows(
        () -> getDateTimeTransformValue(TIcebergPartitionTransformType.VOID, "string"));

    assertThrows(
        () -> getDateTimeTransformValue(TIcebergPartitionTransformType.YEAR, "2023-12"));
    assertThrows(
        () -> getDateTimeTransformValue(TIcebergPartitionTransformType.MONTH, "2023"));
    assertThrows(() -> getDateTimeTransformValue(TIcebergPartitionTransformType.DAY,
        "2023-12-12-1"));
    assertThrows(
        () -> getDateTimeTransformValue(TIcebergPartitionTransformType.HOUR, "2023-12"));

    try {
      int yearTransformValidString =
          getDateTimeTransformValue(TIcebergPartitionTransformType.YEAR, "2023");
      int monthTransformValidString =
          getDateTimeTransformValue(TIcebergPartitionTransformType.MONTH, "2023-12");
      int dayTransformValidString =
          getDateTimeTransformValue(TIcebergPartitionTransformType.DAY, "2023-12-12");
      int hourTransformValidString =
          getDateTimeTransformValue(TIcebergPartitionTransformType.HOUR, "2023-12-12-1");

      assertEquals(53, yearTransformValidString);
      assertEquals(647, monthTransformValidString);
      assertEquals(19703, dayTransformValidString);
      assertEquals(472873, hourTransformValidString);

    } catch (ImpalaRuntimeException e) {
      fail(String.format("Unexpected parse error: %s", e));
    }
  }

  interface DateTimeTransformCallable {
    Integer call() throws ImpalaRuntimeException;
  }

  private void assertThrows(DateTimeTransformCallable function) {
    try {
      function.call();
    } catch (ImpalaRuntimeException e) {
      assertEquals(ImpalaRuntimeException.class, e.getClass());
      return;
    }
    fail();
  }

  /**
   * Holder class for testing Partition transforms.
   */
  static class PartitionTransform {
    String transformName;
    Integer parameter;

    PartitionTransform(String transformName, Integer parameter) {
      this.transformName = transformName;
      this.parameter = parameter;
    }

    @Override
    public String toString() {
      return "PartitionTransform{"
          + "transformName='" + transformName + '\'' + ", parameter=" + parameter + '}';
    }
  }

  /**
   * A simple Schema object.
   */
  public static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "i", Types.IntegerType.get()),
          Types.NestedField.required(2, "l", Types.LongType.get()),
          Types.NestedField.required(3, "id", Types.IntegerType.get()),
          Types.NestedField.required(4, "data", Types.StringType.get()));

  /**
   * Partition spec used to create tables.
   */
  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 10).build();

  /**
   * A test DataFile.
   */
  static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0") // Easy way to set partition data for now.
          .withRecordCount(1)
          .build();

  /**
   * Holder class for testing isHiveCatalog().
   */
  static class CatalogType {
    String propertyName;
    boolean isHiveCatalog;

    CatalogType(String propertyName, boolean isHiveCatalog) {
      this.propertyName = propertyName;
      this.isHiveCatalog = isHiveCatalog;
    }
  }

  /**
   * Holder class for test of catalog functions.
   */
  static class CatalogMapping {
    String propertyName;
    TIcebergCatalog catalog;
    Class<?> clazz;

    CatalogMapping(String propertyName, TIcebergCatalog catalog, Class<?> clazz) {
      this.propertyName = propertyName;
      this.catalog = catalog;
      this.clazz = clazz;
    }
  }
}