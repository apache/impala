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

package org.apache.impala.catalog;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.SqlCastException;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.CatalogObjectsConstants;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.THBaseTable;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Test suite to verify proper conversion of Catalog objects to/from Thrift structs.
 */
public class CatalogObjectToFromThriftTest {
  private static CatalogServiceCatalog catalog_;

  @BeforeClass
  public static void setUp() throws Exception {
    catalog_ = CatalogServiceTestCatalog.create();
  }

  @AfterClass
  public static void cleanUp() { catalog_.close(); }

  @Test
  public void TestPartitionedTable() throws CatalogException {
    String[] dbNames = {"functional", "functional_avro", "functional_parquet",
                        "functional_seq"};
    for (String dbName: dbNames) {
      Table table = catalog_.getOrLoadTable(dbName, "alltypes", "test", null);
      Assert.assertEquals(24, ((HdfsTable)table).getPartitions().size());
      Assert.assertEquals(24, ((HdfsTable)table).getPartitionIds().size());

      TTable thriftTable = getThriftTable(table);
      Assert.assertEquals(thriftTable.tbl_name, "alltypes");
      Assert.assertEquals(thriftTable.db_name, dbName);
      Assert.assertTrue(thriftTable.isSetTable_type());
      Assert.assertEquals(thriftTable.getClustering_columns().size(), 2);
      Assert.assertEquals(thriftTable.getTable_type(), TTableType.HDFS_TABLE);
      THdfsTable hdfsTable = thriftTable.getHdfs_table();
      Assert.assertTrue(hdfsTable.hdfsBaseDir != null);

      // The table has 24 partitions.
      Assert.assertEquals(24, hdfsTable.getPartitions().size());
      Assert.assertFalse(hdfsTable.getPartitions().containsKey(
          CatalogObjectsConstants.PROTOTYPE_PARTITION_ID));
      // The prototype partition should be included and set properly.
      Assert.assertTrue(hdfsTable.isSetPrototype_partition());
      Assert.assertEquals(CatalogObjectsConstants.PROTOTYPE_PARTITION_ID,
          hdfsTable.getPrototype_partition().id);
      Assert.assertNull(hdfsTable.getPrototype_partition().location);
      for (Map.Entry<Long, THdfsPartition> kv: hdfsTable.getPartitions().entrySet()) {
        Assert.assertEquals(kv.getValue().getPartitionKeyExprs().size(), 2);
      }

      // Now try to load the thrift struct.
      Table newTable = Table.fromThrift(catalog_.getDb(dbName), thriftTable);
      Assert.assertTrue(newTable instanceof HdfsTable);
      Assert.assertEquals(newTable.name_, thriftTable.tbl_name);
      Assert.assertEquals(newTable.numClusteringCols_, 2);
      // Currently only have table stats on "functional.alltypes"
      if (dbName.equals("functional")) Assert.assertEquals(7300, newTable.getNumRows());

      HdfsTable newHdfsTable = (HdfsTable) newTable;
      Assert.assertEquals(newHdfsTable.getPartitions().size(), 24);
      Assert.assertEquals(newHdfsTable.getPartitionIds().size(), 24);
      Collection<? extends FeFsPartition> parts =
          FeCatalogUtils.loadAllPartitions(newHdfsTable);
      for (FeFsPartition hdfsPart: parts) {
        Assert.assertEquals(hdfsPart.getFileDescriptors().size(), 1);
        Assert.assertTrue(
            hdfsPart.getFileDescriptors().get(0).getNumFileBlocks() > 0);

        // Verify the partition access level is getting set properly. The alltypes_seq
        // table has two partitions that are read_only.
        if (dbName.equals("functional_seq") && (
            hdfsPart.getPartitionName().equals("year=2009/month=1") ||
            hdfsPart.getPartitionName().equals("year=2009/month=3"))) {
          Assert.assertEquals(TAccessLevel.READ_ONLY, hdfsPart.getAccessLevel());
        } else {
          Assert.assertEquals(TAccessLevel.READ_WRITE, hdfsPart.getAccessLevel());
        }
      }
      Assert.assertNotNull(newHdfsTable.prototypePartition_);
      Assert.assertEquals(((HdfsTable)table).prototypePartition_.getParameters(),
          newHdfsTable.prototypePartition_.getParameters());
    }
  }

  /**
   * Validates proper to/fromThrift behavior for a table whose column definition does not
   * match its Avro schema definition. The expected behavior is that the Avro schema
   * definition will override any columns defined in the table.
   */
  @Test
  public void TestMismatchedAvroAndTableSchemas() throws CatalogException {
    Table table = catalog_.getOrLoadTable("functional_avro_snap",
        "schema_resolution_test", "test", null);
    TTable thriftTable = getThriftTable(table);
    Assert.assertEquals(thriftTable.tbl_name, "schema_resolution_test");
    Assert.assertTrue(thriftTable.isSetTable_type());
    Assert.assertEquals(thriftTable.getColumns().size(), 9);
    Assert.assertEquals(thriftTable.getClustering_columns().size(), 0);
    Assert.assertEquals(thriftTable.getTable_type(), TTableType.HDFS_TABLE);

    // Now try to load the thrift struct.
    Table newTable = Table.fromThrift(catalog_.getDb("functional_avro_snap"),
        thriftTable);
    Assert.assertEquals(newTable.getColumns().size(), 9);

    // The table schema does not match the Avro schema - it has only 2 columns.
    Assert.assertEquals(newTable.getMetaStoreTable().getSd().getCols().size(), 2);
  }

  @Test
  public void TestHBaseTables() throws CatalogException {
    String dbName = "functional_hbase";
    Table table = catalog_.getOrLoadTable(dbName, "alltypes", "test", null);
    TTable thriftTable = getThriftTable(table);
    Assert.assertEquals(thriftTable.tbl_name, "alltypes");
    Assert.assertEquals(thriftTable.db_name, dbName);
    Assert.assertTrue(thriftTable.isSetTable_type());
    Assert.assertEquals(thriftTable.getClustering_columns().size(), 1);
    Assert.assertEquals(thriftTable.getTable_type(), TTableType.HBASE_TABLE);
    THBaseTable hbaseTable = thriftTable.getHbase_table();
    Assert.assertEquals(hbaseTable.getFamilies().size(), 13);
    Assert.assertEquals(hbaseTable.getQualifiers().size(), 13);
    Assert.assertEquals(hbaseTable.getBinary_encoded().size(), 13);
    for (boolean isBinaryEncoded: hbaseTable.getBinary_encoded()) {
      // None of the columns should be binary encoded.
      Assert.assertTrue(!isBinaryEncoded);
    }

    Table newTable = Table.fromThrift(catalog_.getDb(dbName), thriftTable);
    Assert.assertTrue(newTable instanceof HBaseTable);
    HBaseTable newHBaseTable = (HBaseTable) newTable;
    Assert.assertEquals(newHBaseTable.getColumns().size(), 13);
    Assert.assertEquals(newHBaseTable.getColumn("double_col").getType(),
        Type.DOUBLE);
    Assert.assertEquals(newHBaseTable.getNumClusteringCols(), 1);
  }

  @Test
  public void TestHBaseTableWithBinaryEncodedCols()
      throws CatalogException {
    String dbName = "functional_hbase";
    Table table = catalog_.getOrLoadTable(dbName, "alltypessmallbinary", "test", null);
    TTable thriftTable = getThriftTable(table);
    Assert.assertEquals(thriftTable.tbl_name, "alltypessmallbinary");
    Assert.assertEquals(thriftTable.db_name, dbName);
    Assert.assertTrue(thriftTable.isSetTable_type());
    Assert.assertEquals(thriftTable.getClustering_columns().size(), 1);
    Assert.assertEquals(thriftTable.getTable_type(), TTableType.HBASE_TABLE);
    THBaseTable hbaseTable = thriftTable.getHbase_table();
    Assert.assertEquals(hbaseTable.getFamilies().size(), 13);
    Assert.assertEquals(hbaseTable.getQualifiers().size(), 13);
    Assert.assertEquals(hbaseTable.getBinary_encoded().size(), 13);

    // Count the number of columns that are binary encoded.
    int numBinaryEncodedCols = 0;
    for (boolean isBinaryEncoded: hbaseTable.getBinary_encoded()) {
      if (isBinaryEncoded) ++numBinaryEncodedCols;
    }
    Assert.assertEquals(numBinaryEncodedCols, 10);

    // Verify that creating a table from this thrift struct results in a valid
    // Table.
    Table newTable = Table.fromThrift(catalog_.getDb(dbName), thriftTable);
    Assert.assertTrue(newTable instanceof HBaseTable);
    HBaseTable newHBaseTable = (HBaseTable) newTable;
    Assert.assertEquals(newHBaseTable.getColumns().size(), 13);
    Assert.assertEquals(newHBaseTable.getColumn("double_col").getType(),
        Type.DOUBLE);
    Assert.assertEquals(newHBaseTable.getNumClusteringCols(), 1);
  }

  @Test
  public void TestTableLoadingErrorsForHive2() throws ImpalaException {
    // run the test only when it is running against Hive-2 since index tables are
    // skipped during data-load against Hive-3
    Assume.assumeTrue(
        "Skipping this test since it is only supported when running against Hive-2",
        TestUtils.getHiveMajorVersion() == 2);
    Table table = catalog_.getOrLoadTable("functional", "hive_index_tbl", "test", null);
    Assert.assertNotNull(table);
    TTable thriftTable = getThriftTable(table);
    Assert.assertEquals(thriftTable.tbl_name, "hive_index_tbl");
    Assert.assertEquals(thriftTable.db_name, "functional");
  }

  @Test
  public void TestTableLoadingErrors() throws ImpalaException {
    Table table = catalog_.getOrLoadTable("functional", "alltypes", "test", null);
    HdfsTable hdfsTable = (HdfsTable) table;
    // Get any partition with valid HMS parameters to create a
    // dummy partition.
    long id = Iterables.getFirst(hdfsTable.getPartitionIds(), -1L);
    HdfsPartition part = (HdfsPartition) FeCatalogUtils.loadPartition(
        hdfsTable, id);
    Assert.assertNotNull(part);;
    // Create a dummy partition with an invalid decimal type.
    try {
      new HdfsPartition.Builder(hdfsTable)
          .setMsPartition(part.toHmsPartition())
          .setPartitionKeyValues(Lists.newArrayList(
              LiteralExpr.createFromUnescapedStr(
                  "11.1", ScalarType.createDecimalType(1, 0)),
              LiteralExpr.createFromUnescapedStr(
                  "11.1", ScalarType.createDecimalType(1, 0))))
          .setAccessLevel(TAccessLevel.READ_WRITE);
      fail("Expected metadata to be malformed.");
    } catch (SqlCastException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Value 11.1 cannot be cast to type DECIMAL(1,0)"));
    }
  }

  @Test
  public void TestView() throws CatalogException {
    Table table = catalog_.getOrLoadTable("functional", "view_view", "test", null);
    TTable thriftTable = getThriftTable(table);
    Assert.assertEquals(thriftTable.tbl_name, "view_view");
    Assert.assertEquals(thriftTable.db_name, "functional");
    Assert.assertFalse(thriftTable.isSetHdfs_table());
    Assert.assertFalse(thriftTable.isSetHbase_table());
    Assert.assertTrue(thriftTable.isSetMetastore_table());
  }

  private TTable getThriftTable(Table table) {
    TTable thriftTable = null;
    table.takeReadLock();
    try {
      thriftTable = table.toThrift();
    } finally {
      table.releaseReadLock();
    }
    return thriftTable;
  }
}
