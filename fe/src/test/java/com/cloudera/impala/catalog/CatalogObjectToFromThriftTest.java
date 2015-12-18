// Copyright 2013 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog;

import static org.junit.Assert.fail;

import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.testutil.CatalogServiceTestCatalog;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.thrift.ImpalaInternalServiceConstants;
import com.cloudera.impala.thrift.TAccessLevel;
import com.cloudera.impala.thrift.THBaseTable;
import com.cloudera.impala.thrift.THdfsPartition;
import com.cloudera.impala.thrift.THdfsTable;
import com.cloudera.impala.thrift.TTable;
import com.cloudera.impala.thrift.TTableType;
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
      Table table = catalog_.getOrLoadTable(dbName, "alltypes");
      TTable thriftTable = table.toThrift();
      Assert.assertEquals(thriftTable.tbl_name, "alltypes");
      Assert.assertEquals(thriftTable.db_name, dbName);
      Assert.assertTrue(thriftTable.isSetTable_type());
      Assert.assertEquals(thriftTable.getClustering_columns().size(), 2);
      Assert.assertEquals(thriftTable.getTable_type(), TTableType.HDFS_TABLE);
      THdfsTable hdfsTable = thriftTable.getHdfs_table();
      Assert.assertTrue(hdfsTable.hdfsBaseDir != null);

      // The table has 24 partitions + the default partition
      Assert.assertEquals(hdfsTable.getPartitions().size(), 25);
      Assert.assertTrue(hdfsTable.getPartitions().containsKey(
          new Long(ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID)));

      for (Map.Entry<Long, THdfsPartition> kv: hdfsTable.getPartitions().entrySet()) {
        if (kv.getKey() == ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID) {
          Assert.assertEquals(kv.getValue().getPartitionKeyExprs().size(), 0);
        } else {
          Assert.assertEquals(kv.getValue().getPartitionKeyExprs().size(), 2);
        }
      }

      // Now try to load the thrift struct.
      Table newTable = Table.fromThrift(catalog_.getDb(dbName), thriftTable);
      Assert.assertTrue(newTable instanceof HdfsTable);
      Assert.assertEquals(newTable.name_, thriftTable.tbl_name);
      Assert.assertEquals(newTable.numClusteringCols_, 2);
      // Currently only have table stats on "functional.alltypes"
      if (dbName.equals("functional")) Assert.assertEquals(7300, newTable.numRows_);

      HdfsTable newHdfsTable = (HdfsTable) newTable;
      Assert.assertEquals(newHdfsTable.getPartitions().size(), 25);
      boolean foundDefaultPartition = false;
      for (HdfsPartition hdfsPart: newHdfsTable.getPartitions()) {
        if (hdfsPart.getId() == ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID) {
          Assert.assertEquals(foundDefaultPartition, false);
          foundDefaultPartition = true;
        } else {
          Assert.assertEquals(hdfsPart.getFileDescriptors().size(), 1);
          Assert.assertTrue(
              hdfsPart.getFileDescriptors().get(0).getFileBlocks().size() > 0);

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
      }
      Assert.assertEquals(foundDefaultPartition, true);
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
        "schema_resolution_test");
    TTable thriftTable = table.toThrift();
    Assert.assertEquals(thriftTable.tbl_name, "schema_resolution_test");
    Assert.assertTrue(thriftTable.isSetTable_type());
    Assert.assertEquals(thriftTable.getColumns().size(), 8);
    Assert.assertEquals(thriftTable.getClustering_columns().size(), 0);
    Assert.assertEquals(thriftTable.getTable_type(), TTableType.HDFS_TABLE);

    // Now try to load the thrift struct.
    Table newTable = Table.fromThrift(catalog_.getDb("functional_avro_snap"),
        thriftTable);
    Assert.assertEquals(newTable.getColumns().size(), 8);

    // The table schema does not match the Avro schema - it has only 2 columns.
    Assert.assertEquals(newTable.getMetaStoreTable().getSd().getCols().size(), 2);
  }

  @Test
  public void TestHBaseTables() throws CatalogException {
    String dbName = "functional_hbase";
    Table table = catalog_.getOrLoadTable(dbName, "alltypes");
    TTable thriftTable = table.toThrift();
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
    Table table = catalog_.getOrLoadTable(dbName, "alltypessmallbinary");
    TTable thriftTable = table.toThrift();
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
  public void TestTableLoadingErrors() throws ImpalaException {
    Table table = catalog_.getOrLoadTable("functional", "hive_index_tbl");
    TTable thriftTable = table.toThrift();
    Assert.assertEquals(thriftTable.tbl_name, "hive_index_tbl");
    Assert.assertEquals(thriftTable.db_name, "functional");

    table = catalog_.getOrLoadTable("functional", "alltypes");
    HdfsTable hdfsTable = (HdfsTable) table;
    // Get any partition with valid HMS parameters to create a
    // dummy partition.
    HdfsPartition part = null;
    for (HdfsPartition partition: hdfsTable.getPartitions()) {
      if (!partition.isDefaultPartition()) {
        part = partition;
        break;
      }
    }
    // Create a dummy partition with an invalid decimal type.
    try {
      HdfsPartition dummyPart = new HdfsPartition(hdfsTable, part.toHmsPartition(),
        Lists.newArrayList(LiteralExpr.create("1.1", ScalarType.createDecimalType(1, 0)),
            LiteralExpr.create("1.1", ScalarType.createDecimalType(1, 0))),
        null, Lists.<HdfsPartition.FileDescriptor>newArrayList(),
        TAccessLevel.READ_WRITE);
      fail("Expected metadata to be malformed.");
    } catch (AnalysisException e) {
      Assert.assertTrue(e.getMessage().contains("invalid DECIMAL(1,0) value: 1.1"));
    }
  }

  @Test
  public void TestView() throws CatalogException {
    Table table = catalog_.getOrLoadTable("functional", "view_view");
    TTable thriftTable = table.toThrift();
    Assert.assertEquals(thriftTable.tbl_name, "view_view");
    Assert.assertEquals(thriftTable.db_name, "functional");
    Assert.assertFalse(thriftTable.isSetHdfs_table());
    Assert.assertFalse(thriftTable.isSetHbase_table());
    Assert.assertTrue(thriftTable.isSetMetastore_table());
  }
}
