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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.hdfs.DFSOpsCountStatistics;
import org.apache.hadoop.hdfs.DFSOpsCountStatistics.OpType;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Reference;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TGetPartitionStatsRequest;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TPrivilegeScope;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.NoOpEventSequence;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CatalogTest {
  private CatalogServiceCatalog catalog_;

  @Before
  public void init() {
    catalog_ = CatalogServiceTestCatalog.create();
  }

  @After
  public void cleanUp() { catalog_.close(); }

  public static void checkTableCols(FeDb db, String tblName, int numClusteringCols,
      String[] colNames, Type[] colTypes) throws TableLoadingException {
    FeTable tbl = db.getTable(tblName);
    assertEquals(tbl.getName(), tblName);
    assertEquals(tbl.getNumClusteringCols(), numClusteringCols);
    List<Column> cols = tbl.getColumns();
    assertEquals(colNames.length, colTypes.length);
    assertEquals(cols.size(), colNames.length);
    Iterator<Column> it = cols.iterator();
    int i = 0;
    while (it.hasNext()) {
      Column col = it.next();
      assertEquals(col.getName(), colNames[i]);
      assertTrue(col.getType().equals(colTypes[i]));
      ++i;
    }
  }

  private void checkHBaseTableCols(Db db, String hiveTableName, String hbaseTableName,
      String[] hiveColNames, String[] colFamilies, String[] colQualifiers,
      Type[] colTypes) throws TableLoadingException{
    checkTableCols(db, hiveTableName, 1, hiveColNames, colTypes);
    HBaseTable tbl = (HBaseTable) db.getTable(hiveTableName);
    assertEquals(tbl.getHBaseTableName(), hbaseTableName);
    List<Column> cols = tbl.getColumns();
    assertEquals(colFamilies.length, colTypes.length);
    assertEquals(colQualifiers.length, colTypes.length);
    Iterator<Column> it = cols.iterator();
    int i = 0;
    while (it.hasNext()) {
      HBaseColumn col = (HBaseColumn)it.next();
      assertEquals(col.getColumnFamily(), colFamilies[i]);
      assertEquals(col.getColumnQualifier(), colQualifiers[i]);
      ++i;
    }
  }

  @Test
  public void TestColSchema() throws CatalogException {
    Db functionalDb = catalog_.getDb("functional");
    assertNotNull(functionalDb);
    assertEquals(functionalDb.getName(), "functional");
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypes", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypes_view", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypes_view_sub", "test",
      null));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypessmall", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypeserror", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypeserrornonulls", "test",
     null ));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypesagg", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypesaggnonulls", "test",
      null));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypesnopart", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypesinsert", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "complex_view", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "testtbl", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "dimtbl", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "jointbl", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "liketbl", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "greptiny", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "rankingssmall", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "uservisitssmall", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "view_view", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "date_tbl", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional", "binary_tbl", "test", null));
    // IMP-163 - table with string partition column does not load if there are partitions
    assertNotNull(
        catalog_.getOrLoadTable("functional", "StringPartitionKey", "test", null));
    // Test non-existent table
    assertNull(catalog_.getOrLoadTable("functional", "nonexistenttable", "test", null));

    // functional_seq contains the same tables as functional
    Db testDb = catalog_.getDb("functional_seq");
    assertNotNull(testDb);
    assertEquals(testDb.getName(), "functional_seq");
    assertNotNull(catalog_.getOrLoadTable("functional_seq", "alltypes", "test", null));
    assertNotNull(catalog_.getOrLoadTable("functional_seq", "testtbl", "test", null));

    Db hbaseDb = catalog_.getDb("functional_hbase");
    assertNotNull(hbaseDb);
    assertEquals(hbaseDb.getName(), "functional_hbase");
    // Loading succeeds for an HBase table that has binary columns and an implicit key
    // column mapping
    assertNotNull(catalog_.getOrLoadTable(hbaseDb.getName(), "alltypessmallbinary",
        "test", null));
    assertNotNull(
        catalog_.getOrLoadTable(hbaseDb.getName(), "alltypessmall", "test", null));
    assertNotNull(catalog_.getOrLoadTable(hbaseDb.getName(), "hbasealltypeserror",
        "test", null));
    assertNotNull(catalog_.getOrLoadTable(hbaseDb.getName(),
        "hbasealltypeserrornonulls", "test", null));
    assertNotNull(
        catalog_.getOrLoadTable(hbaseDb.getName(), "alltypesagg", "test", null));
    assertNotNull(catalog_.getOrLoadTable(hbaseDb.getName(), "stringids", "test", null));

    checkTableCols(functionalDb, "alltypes", 2,
        new String[]
          {"year", "month", "id", "bool_col", "tinyint_col", "smallint_col",
           "int_col", "bigint_col", "float_col", "double_col", "date_string_col",
           "string_col", "timestamp_col"},
        new Type[]
          {Type.INT, Type.INT, Type.INT,
           Type.BOOLEAN, Type.TINYINT, Type.SMALLINT,
           Type.INT, Type.BIGINT, Type.FLOAT,
           Type.DOUBLE, Type.STRING, Type.STRING,
           Type.TIMESTAMP});
    checkTableCols(functionalDb, "testtbl", 0,
        new String[] {"id", "name", "zip"},
        new Type[]
          {Type.BIGINT, Type.STRING, Type.INT});
    checkTableCols(testDb, "testtbl", 0,
        new String[] {"id", "name", "zip"},
        new Type[]
          {Type.BIGINT, Type.STRING, Type.INT});
    checkTableCols(functionalDb, "liketbl", 0,
        new String[] {
            "str_col", "match_like_col", "no_match_like_col", "match_regex_col",
            "no_match_regex_col"},
        new Type[]
          {Type.STRING, Type.STRING, Type.STRING,
           Type.STRING, Type.STRING});
    checkTableCols(functionalDb, "dimtbl", 0,
        new String[] {"id", "name", "zip"},
        new Type[]
          {Type.BIGINT, Type.STRING, Type.INT});
    checkTableCols(functionalDb, "jointbl", 0,
        new String[] {"test_id", "test_name", "test_zip", "alltypes_id"},
        new Type[]
          {Type.BIGINT, Type.STRING, Type.INT,
           Type.INT});

    checkHBaseTableCols(hbaseDb, "alltypessmall", "functional_hbase.alltypessmall",
        new String[]
          {"id", "bigint_col", "bool_col", "date_string_col", "double_col", "float_col",
           "int_col", "month", "smallint_col", "string_col", "timestamp_col",
           "tinyint_col", "year"},
        new String[]
          {":key", "d", "d", "d", "d", "d", "d", "d", "d", "d", "d", "d", "d"},
        new String[]
          {null, "bigint_col", "bool_col", "date_string_col", "double_col", "float_col",
           "int_col", "month", "smallint_col", "string_col", "timestamp_col",
           "tinyint_col", "year"},
        new Type[]
          {Type.INT, Type.BIGINT, Type.BOOLEAN,
           Type.STRING, Type.DOUBLE, Type.FLOAT,
           Type.INT, Type.INT, Type.SMALLINT,
           Type.STRING, Type.TIMESTAMP,
           Type.TINYINT, Type.INT});

    checkHBaseTableCols(hbaseDb, "hbasealltypeserror",
        "functional_hbase.hbasealltypeserror",
        new String[]
          {"id", "bigint_col", "bool_col","date_string_col", "double_col", "float_col",
           "int_col", "smallint_col", "string_col","timestamp_col", "tinyint_col"},
        new String[]
          {":key", "d", "d", "d", "d", "d", "d", "d", "d", "d", "d"},
        new String[]
          {null, "bigint_col", "bool_col","date_string_col", "double_col", "float_col",
           "int_col", "smallint_col", "string_col","timestamp_col", "tinyint_col"},
        new Type[]
          {Type.INT, Type.BIGINT, Type.BOOLEAN,
           Type.STRING, Type.DOUBLE, Type.FLOAT,
           Type.INT, Type.SMALLINT, Type.STRING,
           Type.TIMESTAMP, Type.TINYINT});

    checkHBaseTableCols(hbaseDb, "hbasealltypeserrornonulls",
        "functional_hbase.hbasealltypeserrornonulls",
        new String[]
          {"id", "bigint_col", "bool_col", "date_string_col", "double_col", "float_col",
           "int_col", "smallint_col", "string_col","timestamp_col", "tinyint_col"},
        new String[]
          {":key", "d", "d", "d", "d", "d", "d", "d", "d", "d", "d"},
        new String[]
          {null, "bigint_col", "bool_col", "date_string_col", "double_col", "float_col",
           "int_col", "smallint_col", "string_col","timestamp_col", "tinyint_col"},
        new Type[]
          {Type.INT, Type.BIGINT, Type.BOOLEAN,
           Type.STRING, Type.DOUBLE, Type.FLOAT,
           Type.INT, Type.SMALLINT, Type.STRING,
           Type.TIMESTAMP, Type.TINYINT});

    checkHBaseTableCols(hbaseDb, "alltypesagg", "functional_hbase.alltypesagg",
        new String[]
          {"id", "bigint_col", "bool_col", "date_string_col", "day", "double_col",
           "float_col", "int_col", "month", "smallint_col", "string_col",
           "timestamp_col", "tinyint_col", "year"},
        new String[]
          {":key", "d", "d", "d", "d", "d", "d", "d", "d", "d", "d", "d", "d", "d"},
        new String[]
          {null, "bigint_col", "bool_col", "date_string_col", "day", "double_col",
           "float_col", "int_col", "month", "smallint_col", "string_col",
           "timestamp_col", "tinyint_col", "year"},
        new Type[]
          {Type.INT, Type.BIGINT, Type.BOOLEAN,
           Type.STRING,Type.INT, Type.DOUBLE,
           Type.FLOAT, Type.INT, Type.INT,
           Type.SMALLINT, Type.STRING, Type.TIMESTAMP,
           Type.TINYINT, Type.INT});

    checkHBaseTableCols(hbaseDb, "stringids", "functional_hbase.alltypesagg",
        new String[]
          {"id", "bigint_col", "bool_col", "date_string_col", "day", "double_col",
           "float_col", "int_col", "month", "smallint_col", "string_col",
           "timestamp_col", "tinyint_col", "year"},
        new String[]
          {":key", "d", "d", "d", "d", "d", "d", "d", "d", "d", "d", "d", "d", "d"},
        new String[]
          {null, "bigint_col", "bool_col", "date_string_col", "day", "double_col",
           "float_col", "int_col", "month", "smallint_col", "string_col",
           "timestamp_col", "tinyint_col", "year"},
        new Type[]
          {Type.STRING, Type.BIGINT, Type.BOOLEAN,
           Type.STRING,Type.INT, Type.DOUBLE,
           Type.FLOAT, Type.INT, Type.INT,
           Type.SMALLINT, Type.STRING, Type.TIMESTAMP,
           Type.TINYINT, Type.INT});

    checkTableCols(functionalDb, "greptiny", 0,
        new String[]
          {"field"},
        new Type[]
          {Type.STRING});

    checkTableCols(functionalDb, "rankingssmall", 0,
        new String[]
          {"pagerank", "pageurl", "avgduration"},
        new Type[]
          {Type.INT, Type.STRING, Type.INT});

    checkTableCols(functionalDb, "uservisitssmall", 0,
        new String[]
          {"sourceip", "desturl", "visitdate",  "adrevenue", "useragent",
           "ccode", "lcode", "skeyword", "avgtimeonsite"},
        new Type[]
          {Type.STRING, Type.STRING, Type.STRING,
           Type.FLOAT, Type.STRING, Type.STRING,
           Type.STRING, Type.STRING, Type.INT});

    checkTableCols(functionalDb, "date_tbl", 1,
        new String[] {"date_part", "id_col", "date_col"},
        new Type[] {Type.DATE, Type.INT, Type.DATE});

    checkTableCols(functionalDb, "binary_tbl", 0,
        new String[] {"id", "string_col", "binary_col"},
        new Type[] {Type.INT, Type.STRING, Type.BINARY});

    // case-insensitive lookup
    assertEquals(catalog_.getOrLoadTable("functional", "alltypes", "test", null),
        catalog_.getOrLoadTable("functional", "AllTypes", "test", null));
  }

  // Count of listFiles (list status + blocks) calls
  private static final String LIST_LOCATED_STATUS =
      OpType.LIST_LOCATED_STATUS.getSymbol();
  // Count of listStatus calls
  private static final String LIST_STATUS = OpType.LIST_STATUS.getSymbol();
  // Count of getStatus calls
  private static final String GET_FILE_STATUS = OpType.GET_FILE_STATUS.getSymbol();
  // Count of getFileBlockLocations() calls
  private static final String GET_FILE_BLOCK_LOCS =
      OpType.GET_FILE_BLOCK_LOCATIONS.getSymbol();

  /**
   * Regression test for IMPALA-7320 and IMPALA-7047: we should use batch APIs to fetch
   * file permissions for partitions when loading or reloading.
   */
  @Test
  public void testNumberOfGetFileStatusCalls() throws CatalogException, IOException {
    // Reset the filesystem statistics and load the table, ensuring that it's
    // loaded fresh by invalidating it first.
    GlobalStorageStatistics stats = FileSystem.getGlobalStorageStatistics();
    stats.reset();
    catalog_.invalidateTable(new TTableName("functional", "alltypes"),
        /*tblWasRemoved=*/new Reference<Boolean>(),
        /*dbWasAdded=*/new Reference<Boolean>(), NoOpEventSequence.INSTANCE);

    HdfsTable table = (HdfsTable)catalog_.getOrLoadTable("functional", "AllTypes",
        "test", null);
    StorageStatistics opsCounts = stats.get(DFSOpsCountStatistics.NAME);

    // We expect:
    // - one listLocatedStatus() per partition, to get the file info
    // - one listStatus() for the month=2010/ dir
    // - one listStatus() for the month=2009/ dir
    long expectedCalls = table.getPartitionIds().size() + 2;
    // Due to HDFS-13747, the listStatus calls are incorrectly accounted as
    // op_list_located_status. So, we'll just add up the two to make our
    // assertion resilient to this bug.
    long seenCalls = opsCounts.getLong(LIST_LOCATED_STATUS) +
        opsCounts.getLong(LIST_STATUS);
    assertEquals(expectedCalls, seenCalls);

    // We expect only one getFileStatus call, for the top-level directory.
    assertEquals(1L, (long)opsCounts.getLong(GET_FILE_STATUS));

    // None of the underlying files changed so we should not do any ops for the files.
    assertEquals(0L, (long)opsCounts.getLong(GET_FILE_BLOCK_LOCS));

    // Now test REFRESH on the table...
    stats.reset();
    catalog_.reloadTable(table, "test", NoOpEventSequence.INSTANCE);

    // Again, we expect only one getFileStatus call, for the top-level directory.
    assertEquals(1L, (long)opsCounts.getLong(GET_FILE_STATUS));
    // REFRESH calls listStatus on each of the partitions, but doesn't re-check
    // the permissions of the partition directories themselves.
    seenCalls = opsCounts.getLong(LIST_LOCATED_STATUS) +
        opsCounts.getLong(LIST_STATUS);
    assertEquals(table.getPartitionIds().size(), seenCalls);
    // None of the underlying files changed so we should not do any ops for the files.
    assertEquals(0L, (long)opsCounts.getLong(GET_FILE_BLOCK_LOCS));

    // Reloading a specific partition should not make an RPC per file
    // (regression test for IMPALA-7047).
    stats.reset();
    List<TPartitionKeyValue> partitionSpec = ImmutableList.of(
        new TPartitionKeyValue("year", "2010"),
        new TPartitionKeyValue("month", "10"));
    catalog_.reloadPartition(table, partitionSpec, new Reference<>(false),
        CatalogObject.ThriftObjectType.NONE, "test", NoOpEventSequence.INSTANCE);
    assertEquals(0L, (long)opsCounts.getLong(GET_FILE_BLOCK_LOCS));

    // Loading or reloading an unpartitioned table with some files in it should not make
    // an RPC per file.
    stats.reset();
    HdfsTable unpartTable = (HdfsTable)catalog_.getOrLoadTable(
        "functional", "alltypesaggmultifilesnopart", "test", null);
    assertEquals(0L, (long)opsCounts.getLong(GET_FILE_BLOCK_LOCS));
    stats.reset();
    catalog_.reloadTable(unpartTable, "test", NoOpEventSequence.INSTANCE);
    assertEquals(0L, (long)opsCounts.getLong(GET_FILE_BLOCK_LOCS));

    // Simulate an empty partition, which will trigger the full
    // reload path. Since we can't modify HDFS itself via these tests, we
    // do the next best thing: modify the metadata to revise history as
    // though the partition used above were actually empty.
    HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(
        table.getPartitionFromThriftPartitionSpec(partitionSpec));
    partBuilder.setFileDescriptors(new ArrayList<>());
    table.updatePartition(partBuilder);
    stats.reset();
    catalog_.reloadPartition(table, partitionSpec, new Reference<>(false),
        CatalogObject.ThriftObjectType.NONE, "test", NoOpEventSequence.INSTANCE);

    // Should not scan the directory file-by-file, should use a single
    // listLocatedStatus() to get the whole directory (partition)
    assertEquals(0L, (long)opsCounts.getLong(GET_FILE_BLOCK_LOCS));
    seenCalls = opsCounts.getLong(LIST_LOCATED_STATUS) +
        opsCounts.getLong(LIST_STATUS);
    assertEquals(1, seenCalls);
  }

  @Test
  public void TestPartitions() throws CatalogException {
    HdfsTable table =
        (HdfsTable) catalog_.getOrLoadTable("functional", "AllTypes", "test", null);
    checkAllTypesPartitioning(table);
  }

  /**
   * Test SQL constraints such as primary keys and foreign keys
   */
  @Test
  public void testGetSqlConstraints() throws Exception {
    FeFsTable t = (FeFsTable) catalog_.getOrLoadTable("functional", "parent_table",
        "test", null);
    assertNotNull(t);
    assertTrue(t instanceof FeFsTable);
    List<SQLPrimaryKey> primaryKeys = t.getSqlConstraints().getPrimaryKeys();
    List<SQLForeignKey> foreignKeys = t.getSqlConstraints().getForeignKeys();
    assertEquals(2, primaryKeys.size());
    assertEquals(0, foreignKeys.size());
    for (SQLPrimaryKey pk: primaryKeys) {
      assertEquals("functional", pk.getTable_db());
      assertEquals("parent_table", pk.getTable_name());
    }
    assertEquals("id", primaryKeys.get(0).getColumn_name());
    assertEquals("year", primaryKeys.get(1).getColumn_name());

    // Force load parent_table_2. Required for fetching foreign keys from child_table.
    catalog_.getOrLoadTable("functional", "parent_table_2", "test", null);

    t = (FeFsTable) catalog_.getOrLoadTable("functional", "child_table", "test", null);
    assertNotNull(t);
    assertTrue(t instanceof FeFsTable);
    primaryKeys = t.getSqlConstraints().getPrimaryKeys();
    foreignKeys = t.getSqlConstraints().getForeignKeys();
    assertEquals(1, primaryKeys.size());
    assertEquals(3, foreignKeys.size());
    assertEquals("functional", primaryKeys.get(0).getTable_db());
    assertEquals("child_table",primaryKeys.get(0).getTable_name());
    for (SQLForeignKey fk : foreignKeys) {
      assertEquals("functional", fk.getFktable_db());
      assertEquals("child_table", fk.getFktable_name());
      assertEquals("functional", fk.getPktable_db());
    }
    assertEquals("parent_table", foreignKeys.get(0).getPktable_name());
    assertEquals("parent_table", foreignKeys.get(1).getPktable_name());
    assertEquals("parent_table_2", foreignKeys.get(2).getPktable_name());
    assertEquals("id", foreignKeys.get(0).getPkcolumn_name());
    assertEquals("year", foreignKeys.get(1).getPkcolumn_name());
    assertEquals("a", foreignKeys.get(2).getPkcolumn_name());
    // FK name for the composite primary key (id, year) should be equal.
    assertEquals(foreignKeys.get(0).getFk_name(), foreignKeys.get(1).getFk_name());

    // Check tables without constraints.
    t = (FeFsTable) catalog_.getOrLoadTable("functional", "alltypes",
        "test", null);
    assertNotNull(t);
    assertTrue(t instanceof FeFsTable);
    primaryKeys = t.getSqlConstraints().getPrimaryKeys();
    foreignKeys = t.getSqlConstraints().getForeignKeys();
    assertNotNull(primaryKeys);
    assertNotNull(foreignKeys);
    assertEquals(0, primaryKeys.size());
    assertEquals(0, foreignKeys.size());
  }

  public static void checkAllTypesPartitioning(FeFsTable table) {
    assertEquals(24, table.getPartitionIds().size());
    assertEquals(24, table.getPartitions().size());
    Collection<? extends FeFsPartition> partitions =
        FeCatalogUtils.loadAllPartitions(table);

    // check that partition keys cover the date range 1/1/2009-12/31/2010
    // and that we have one file per partition.
    assertEquals(24, partitions.size());
    Set<HdfsStorageDescriptor> uniqueSds = Collections.newSetFromMap(
        new IdentityHashMap<HdfsStorageDescriptor, Boolean>());
    Set<Long> months = new HashSet<>();
    for (FeFsPartition p: partitions) {
      assertEquals(2, p.getPartitionValues().size());

      LiteralExpr key1Expr = p.getPartitionValues().get(0);
      assertTrue(key1Expr instanceof NumericLiteral);
      long key1 = ((NumericLiteral) key1Expr).getLongValue();
      assertTrue(key1 == 2009 || key1 == 2010);

      LiteralExpr key2Expr = p.getPartitionValues().get(1);
      assertTrue(key2Expr instanceof NumericLiteral);
      long key2 = ((NumericLiteral) key2Expr).getLongValue();
      assertTrue(key2 >= 1 && key2 <= 12);

      months.add(key1 * 100 + key2);
      assertEquals(p.getFileDescriptors().size(), 1);
      uniqueSds.add(p.getInputFormatDescriptor());
    }
    assertEquals(months.size(), 24);

    // We intern storage descriptors, so we should only have a single instance across
    // all of the partitions.
    assertEquals(1, uniqueSds.size());
  }

  @Test
  public void testStats() throws CatalogException {
    // make sure the stats for functional.alltypesagg look correct
    HdfsTable table = (HdfsTable) catalog_.getOrLoadTable("functional", "AllTypesAgg",
        "test", null);

    Column idCol = table.getColumn("id");
    assertEquals(idCol.getStats().getAvgSerializedSize(),
        PrimitiveType.INT.getSlotSize(), 0.0001);
    assertEquals(idCol.getStats().getMaxSize(), PrimitiveType.INT.getSlotSize());
    assertFalse(idCol.getStats().hasNulls());

    Column boolCol = table.getColumn("bool_col");
    assertEquals(boolCol.getStats().getAvgSerializedSize(),
        PrimitiveType.BOOLEAN.getSlotSize(), 0.0001);
    assertEquals(boolCol.getStats().getMaxSize(), PrimitiveType.BOOLEAN.getSlotSize());
    assertFalse(boolCol.getStats().hasNulls());

    Column tinyintCol = table.getColumn("tinyint_col");
    assertEquals(tinyintCol.getStats().getAvgSerializedSize(),
        PrimitiveType.TINYINT.getSlotSize(), 0.0001);
    assertEquals(tinyintCol.getStats().getMaxSize(), PrimitiveType.TINYINT.getSlotSize());
    assertTrue(tinyintCol.getStats().hasNulls());

    Column smallintCol = table.getColumn("smallint_col");
    assertEquals(smallintCol.getStats().getAvgSerializedSize(),
        PrimitiveType.SMALLINT.getSlotSize(), 0.0001);
    assertEquals(smallintCol.getStats().getMaxSize(),
        PrimitiveType.SMALLINT.getSlotSize());
    assertTrue(smallintCol.getStats().hasNulls());

    Column intCol = table.getColumn("int_col");
    assertEquals(intCol.getStats().getAvgSerializedSize(),
        PrimitiveType.INT.getSlotSize(), 0.0001);
    assertEquals(intCol.getStats().getMaxSize(), PrimitiveType.INT.getSlotSize());
    assertTrue(intCol.getStats().hasNulls());

    Column bigintCol = table.getColumn("bigint_col");
    assertEquals(bigintCol.getStats().getAvgSerializedSize(),
        PrimitiveType.BIGINT.getSlotSize(), 0.0001);
    assertEquals(bigintCol.getStats().getMaxSize(), PrimitiveType.BIGINT.getSlotSize());
    assertTrue(bigintCol.getStats().hasNulls());

    Column floatCol = table.getColumn("float_col");
    assertEquals(floatCol.getStats().getAvgSerializedSize(),
        PrimitiveType.FLOAT.getSlotSize(), 0.0001);
    assertEquals(floatCol.getStats().getMaxSize(), PrimitiveType.FLOAT.getSlotSize());
    assertTrue(floatCol.getStats().hasNulls());

    Column doubleCol = table.getColumn("double_col");
    assertEquals(doubleCol.getStats().getAvgSerializedSize(),
        PrimitiveType.DOUBLE.getSlotSize(), 0.0001);
    assertEquals(doubleCol.getStats().getMaxSize(), PrimitiveType.DOUBLE.getSlotSize());
    assertTrue(doubleCol.getStats().hasNulls());

    Column timestampCol = table.getColumn("timestamp_col");
    assertEquals(timestampCol.getStats().getAvgSerializedSize(),
        PrimitiveType.TIMESTAMP.getSlotSize(), 0.0001);
    assertEquals(timestampCol.getStats().getMaxSize(),
        PrimitiveType.TIMESTAMP.getSlotSize());
    assertFalse(timestampCol.getStats().hasNulls());

    Column stringCol = table.getColumn("string_col");
    assertTrue(stringCol.getStats().getAvgSerializedSize() > 0);
    assertTrue(stringCol.getStats().getMaxSize() > 0);
    assertFalse(stringCol.getStats().hasNulls());

    // DATE and BINARY types are missing from alltypesagg, so date_tbl and binary_tbl
    // are also checked.
    HdfsTable dateTable = (HdfsTable) catalog_.getOrLoadTable("functional", "date_tbl",
        "test", null);

    Column dateCol = dateTable.getColumn("date_col");
    assertEquals(dateCol.getStats().getAvgSerializedSize(),
        PrimitiveType.DATE.getSlotSize(), 0.0001);
    assertEquals(dateCol.getStats().getMaxSize(), PrimitiveType.DATE.getSlotSize());
    assertTrue(dateCol.getStats().hasNulls());

    HdfsTable binaryTable = (HdfsTable) catalog_.getOrLoadTable("functional",
         "binary_tbl", "test", null);

    Column binaryCol = binaryTable.getColumn("binary_col");
    assertTrue(binaryCol.getStats().getAvgSerializedSize() > 0);
    assertTrue(binaryCol.getStats().getMaxSize() > 0);
    assertTrue(binaryCol.getStats().hasNulls());
    // NDV is not calculated for BINARY columns
    assertFalse(binaryCol.getStats().hasNumDistinctValues());
  }

  /**
   * Verifies that updating column stats data for a type that isn't compatible with
   * the column type results in the stats being set to "unknown". This is a regression
   * test for IMPALA-588, where this used to result in a Preconditions failure.
   */
  @Test
  public void testColStatsColTypeMismatch() throws Exception {
    // First load a table that has column stats.
    //catalog_.refreshTable("functional", "alltypesagg", false);
    HdfsTable table = (HdfsTable) catalog_.getOrLoadTable("functional", "alltypesagg",
        "test", null);

    // Now attempt to update a column's stats with mismatched stats data and ensure
    // we get the expected results.
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      // Load some string stats data and use it to update the stats of different
      // typed columns.
      ColumnStatisticsData stringColStatsData = MetastoreShim.getTableColumnStatistics(
          client.getHiveClient(), "functional", "alltypesagg",
          Lists.newArrayList("string_col")).get(0).getStatsData();

      assertTrue(!table.getColumn("int_col").updateStats(stringColStatsData));
      assertStatsUnknown(table.getColumn("int_col"));

      assertTrue(!table.getColumn("double_col").updateStats(stringColStatsData));
      assertStatsUnknown(table.getColumn("double_col"));

      assertTrue(!table.getColumn("bool_col").updateStats(stringColStatsData));
      assertStatsUnknown(table.getColumn("bool_col"));

      // Do the same thing, but apply bigint stats to a string column.
      ColumnStatisticsData bigIntCol = MetastoreShim.getTableColumnStatistics(
          client.getHiveClient(), "functional", "alltypes",
          Lists.newArrayList("bigint_col")).get(0).getStatsData();
      assertTrue(!table.getColumn("string_col").updateStats(bigIntCol));
      assertStatsUnknown(table.getColumn("string_col"));

      // Now try to apply a matching column stats data and ensure it succeeds.
      assertTrue(table.getColumn("string_col").updateStats(stringColStatsData));
      assertEquals(963, table.getColumn("string_col").getStats().getNumDistinctValues());
    }
  }

  private void assertStatsUnknown(Column column) {
    assertEquals(-1, column.getStats().getNumDistinctValues());
    assertEquals(-1, column.getStats().getNumNulls());
    assertEquals(-1, column.getStats().getNumTrues());
    assertEquals(-1, column.getStats().getNumFalses());
    double expectedSize = column.getType().isFixedLengthType() ?
        column.getType().getSlotSize() : -1;
    assertEquals(expectedSize, column.getStats().getAvgSerializedSize(), 0.0001);
    assertEquals(expectedSize, column.getStats().getMaxSize(), 0.0001);
  }

  // Fetch partition statistics for dbName.tableName for partitionIds.
  private Map<String, ByteBuffer> getPartitionStatistics(String dbName, String tableName)
      throws CatalogException {
    TGetPartitionStatsRequest req = new TGetPartitionStatsRequest();
    req.setTable_name(new TTableName(dbName, tableName));
    return catalog_.getPartitionStats(req);
  }

  // Expect expCount partitions have statistics (though not incremental statistics).
  private void expectStatistics(String dbName, String tableName, int expCount)
      throws CatalogException {
    Map<String, ByteBuffer> result = getPartitionStatistics(dbName, tableName);
    assertEquals(expCount, result.size());
    for (Map.Entry<String, ByteBuffer> e : result.entrySet()) {
      ByteBuffer compressedBuffer = e.getValue();
      byte[] compressedBytes = new byte[compressedBuffer.remaining()];
      compressedBuffer.get(compressedBytes);
      try {
        TPartitionStats stats =
            PartitionStatsUtil.partStatsFromCompressedBytes(compressedBytes, null);
        assertNotNull(stats);
        assertTrue(!stats.isSetIntermediate_col_stats());
      } catch (ImpalaException ex) {
        throw new CatalogException("Error deserializing partition stats.", ex);
      }
    }
  }

  // Expect an exception whose message prefix-matches msgPrefix when fetching partition
  // statistics.
  private void expectStatisticsException(
      String dbName, String tableName, String msgPrefix) {
    try {
      getPartitionStatistics(dbName, tableName);
      fail("Expected exception.");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith(msgPrefix));
    }
  }

  @Test
  public void testPullIncrementalStats() throws CatalogException {
    // Partitioned table with stats. Load the table prior to fetching.
    catalog_.getOrLoadTable("functional", "alltypesagg", "test", null);
    expectStatistics("functional", "alltypesagg", 11);

    // Partitioned table with stats. Invalidate the table prior to fetching.
    Reference<Boolean> tblWasRemoved = new Reference<Boolean>();
    Reference<Boolean> dbWasAdded = new Reference<Boolean>();
    catalog_.invalidateTable(new TTableName("functional", "alltypesagg"),
        tblWasRemoved, dbWasAdded, NoOpEventSequence.INSTANCE);
    expectStatistics("functional", "alltypesagg", 11);

    // Unpartitioned table with no stats.
    expectStatistics("functional", "table_no_newline", 0);

    // Unpartitioned table with stats.
    expectStatistics("functional", "dimtbl", 0);

    // Bogus table.
    expectStatisticsException("functional", "doesnotexist",
        "Requested partition statistics for table that does not exist");

    // Case of IncompleteTable due to loading error.
    expectStatisticsException("functional", "bad_serde",
        "No statistics available for incompletely loaded table");
  }

  @Test
  public void testInternalHBaseTable() throws CatalogException {
    // Cast will fail if table not an HBaseTable
   HBaseTable table = (HBaseTable)
       catalog_.getOrLoadTable("functional_hbase", "internal_hbase_table", "test", null);
    assertNotNull("functional_hbase.internal_hbase_table was not found", table);
  }

  @Test
  public void testDatabaseDoesNotExist() {
    Db nonExistentDb = catalog_.getDb("doesnotexist");
    assertNull(nonExistentDb);
  }

  @Test
  public void testCreateTableMetadata() throws CatalogException {
    Table table = catalog_.getOrLoadTable("functional", "alltypes", "test", null);
    // Tables are created via Impala so the metadata should have been populated properly.
    // alltypes is an external table.
    assertEquals(System.getProperty("user.name"), table.getMetaStoreTable().getOwner());
    assertEquals(TableType.EXTERNAL_TABLE.toString(),
        table.getMetaStoreTable().getTableType());
  }

  /**
   * In Hive-3 the HMS translation layer converts non-transactional managed
   * table definitions to external tables. This test makes sure that such tables
   * are seen as EXTERNAL tables when loaded in catalog
   * @throws CatalogException
   */
  @Test
  public void testCreateTableMetadataHive3() throws CatalogException {
    Assume.assumeTrue(TestUtils.getHiveMajorVersion() > 2);
    // alltypesinsert is created using CREATE TABLE LIKE and is a MANAGED table
    Table table = catalog_.getOrLoadTable("functional", "alltypesinsert", "test", null);
    assertEquals(System.getProperty("user.name"), table.getMetaStoreTable().getOwner());
    assertEquals(TableType.EXTERNAL_TABLE.toString(),
        table.getMetaStoreTable().getTableType());
    // ACID tables should be loaded as MANAGED tables
    table = catalog_.getOrLoadTable("functional", "insert_only_transactional_table",
        "test", null);
    assertEquals(System.getProperty("user.name"), table.getMetaStoreTable().getOwner());
    assertEquals(TableType.MANAGED_TABLE.toString(),
        table.getMetaStoreTable().getTableType());
  }

  /**
   * In Hive-2 there is no HMS translation which converts non-transactional managed
   * table definitions to external tables. This test makes sure that the such tables
   * are seen as MANAGED tables in catalog
   * @throws CatalogException
   */
  @Test
  public void testCreateTableMetadataHive2() throws CatalogException {
    Assume.assumeTrue(TestUtils.getHiveMajorVersion() <= 2);
    // alltypesinsert is created using CREATE TABLE LIKE and is a MANAGED table
    Table table = catalog_.getOrLoadTable("functional", "alltypesinsert", "test", null);
    assertEquals(System.getProperty("user.name"), table.getMetaStoreTable().getOwner());
    assertEquals(TableType.MANAGED_TABLE.toString(),
        table.getMetaStoreTable().getTableType());
  }

  @Test
  public void testLoadingUnsupportedTblTypesOnHive2() throws CatalogException {
    // run the test only when it is running against Hive-2 since index tables are
    // skipped during data-load against Hive-3
    Assume.assumeTrue(
        "Skipping this test since it is only supported when running against Hive-2",
        TestUtils.getHiveMajorVersion() == 2);
    Table table = catalog_.getOrLoadTable("functional", "hive_index_tbl", "test", null);
    assertTrue(table instanceof IncompleteTable);
    IncompleteTable incompleteTable = (IncompleteTable) table;
    assertTrue(incompleteTable.getCause() instanceof TableLoadingException);
    assertEquals("Unsupported table type 'INDEX_TABLE' for: functional.hive_index_tbl",
        incompleteTable.getCause().getMessage());
  }

  @Test
  public void testLoadingUnsupportedTableTypes() throws CatalogException {
    // Table with unsupported SerDe library.
    Table table = catalog_.getOrLoadTable("functional", "bad_serde", "test", null);
    assertTrue(table instanceof IncompleteTable);
    IncompleteTable incompleteTable = (IncompleteTable) table;
    assertTrue(incompleteTable.getCause() instanceof TableLoadingException);
    assertEquals("Impala does not support tables of this type. REASON: SerDe" +
        " library 'org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe' " +
        "is not supported.", incompleteTable.getCause().getCause().getMessage());

    // Impala does not yet support Hive's LazyBinaryColumnarSerDe which can be
    // used for RCFILE tables.
    table = catalog_.getOrLoadTable("functional_rc", "rcfile_lazy_binary_serde", "test"
        , null);
    assertTrue(table instanceof IncompleteTable);
    incompleteTable = (IncompleteTable) table;
    assertTrue(incompleteTable.getCause() instanceof TableLoadingException);
    assertEquals("Impala does not support tables of this type. REASON: SerDe" +
        " library 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe' " +
        "is not supported.", incompleteTable.getCause().getCause().getMessage());
  }

  private List<String> getFunctionSignatures(String db) throws DatabaseNotFoundException {
    List<Function> fns = catalog_.getFunctions(db);
    List<String> names = new ArrayList<>();
    for (Function fn: fns) {
      names.add(fn.signatureString());
    }
    return names;
  }

  @Test
  public void TestUdf() throws CatalogException {
    List<String> fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 0);

    List<Type> args1 = new ArrayList<>();
    List<Type> args2 = Lists.<Type>newArrayList(Type.INT);
    List<Type> args3 = Lists.<Type>newArrayList(Type.TINYINT);

    catalog_.removeFunction(
        new Function(new FunctionName("default", "Foo"), args1,
            Type.INVALID, false));
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 0);

    ScalarFunction udf1 = ScalarFunction.createForTesting(
        "default", "Foo", args1, Type.INVALID, "/Foo", "Foo.class", null,
        null, TFunctionBinaryType.NATIVE);
    catalog_.addFunction(udf1);
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 1);
    assertTrue(fnNames.contains("foo()"));

    // Same function name, overloaded arguments
    ScalarFunction udf2 = ScalarFunction.createForTesting(
        "default", "Foo", args2, Type.INVALID, "/Foo", "Foo.class", null,
        null, TFunctionBinaryType.NATIVE);
    catalog_.addFunction(udf2);
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo()"));
    assertTrue(fnNames.contains("foo(INT)"));

    // Add a function with a new name
    ScalarFunction udf3 = ScalarFunction.createForTesting(
        "default", "Bar", args2, Type.INVALID, "/Foo", "Foo.class", null,
        null, TFunctionBinaryType.NATIVE);
    catalog_.addFunction(udf3);
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 3);
    assertTrue(fnNames.contains("foo()"));
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop Foo()
    catalog_.removeFunction(Function.createFunction("default", "Foo", args1,
          Type.INVALID, false, TFunctionBinaryType.NATIVE));
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop it again, no-op
    catalog_.removeFunction(Function.createFunction("default", "Foo", args1,
          Type.INVALID, false, TFunctionBinaryType.NATIVE));
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop bar(), no-op
    catalog_.removeFunction(Function.createFunction("default", "Bar", args1,
          Type.INVALID, false, TFunctionBinaryType.NATIVE));
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop bar(tinyint), no-op
    catalog_.removeFunction(Function.createFunction("default", "Bar", args3,
          Type.INVALID, false, TFunctionBinaryType.NATIVE));
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop bar(int)
    catalog_.removeFunction(Function.createFunction("default", "Bar", args2,
          Type.INVALID, false, TFunctionBinaryType.NATIVE));
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 1);
    assertTrue(fnNames.contains("foo(INT)"));

    // Drop foo(int)
    catalog_.removeFunction(Function.createFunction("default", "Foo", args2,
          Type.INVALID, false, TFunctionBinaryType.NATIVE));
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 0);

    // Test to check if catalog can handle loading corrupt udfs
    Map<String, String> dbParams = new HashMap<>();
    String badFnKey = "impala_registered_function_badFn";
    String badFnVal = Base64.getEncoder().encodeToString("badFn".getBytes());
    String dbName = "corrupt_udf_test";
    dbParams.put(badFnKey, badFnVal);
    Db db = catalog_.getDb(dbName);
    assertEquals(db, null);
    db = new Db(dbName,
        new org.apache.hadoop.hive.metastore.api.Database(dbName, "", "", dbParams));
    catalog_.addDb(db);
    db = catalog_.getDb(dbName);
    assertTrue(db != null);
    fnNames = getFunctionSignatures(dbName);
    assertEquals(fnNames.size(), 0);

    // Test large functions that exceed HMS 4K param limit. We try to add a sample udf
    // with a very long name, exceeding the hms imposed limit and this is expected to
    // fail.
    ScalarFunction largeUdf = ScalarFunction.createForTesting(
        "default", Strings.repeat("Foo", 5000), args2, Type.INVALID, "/Foo",
        "Foo.class", null, null, TFunctionBinaryType.NATIVE);
    assertTrue(catalog_.addFunction(largeUdf) == false);
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 0);
  }

  @Test
  public void testAuthorizationCatalog() throws CatalogException {
    AuthorizationPolicy authPolicy = catalog_.getAuthPolicy();

    User user = catalog_.addUser("user1");
    TPrivilege userPrivilege = new TPrivilege();
    userPrivilege.setPrincipal_type(TPrincipalType.USER);
    userPrivilege.setPrincipal_id(user.getId());
    userPrivilege.setCreate_time_ms(-1);
    userPrivilege.setServer_name("server1");
    userPrivilege.setScope(TPrivilegeScope.SERVER);
    userPrivilege.setPrivilege_level(TPrivilegeLevel.ALL);
    catalog_.addUserPrivilege("user1", userPrivilege);
    assertSame(user, authPolicy.getPrincipal("user1", TPrincipalType.USER));
    assertNull(authPolicy.getPrincipal("user2", TPrincipalType.USER));
    assertNull(authPolicy.getPrincipal("user1", TPrincipalType.ROLE));
    // Add the same user, the old user will be deleted.
    user = catalog_.addUser("user1");
    assertSame(user, authPolicy.getPrincipal("user1", TPrincipalType.USER));
    // Delete the user.
    assertSame(user, catalog_.removeUser("user1"));
    assertNull(authPolicy.getPrincipal("user1", TPrincipalType.USER));

    Role role = catalog_.addRole("role1", Sets.newHashSet("group1", "group2"));
    TPrivilege rolePrivilege = new TPrivilege();
    rolePrivilege.setPrincipal_type(TPrincipalType.ROLE);
    rolePrivilege.setPrincipal_id(role.getId());
    rolePrivilege.setCreate_time_ms(-1);
    rolePrivilege.setServer_name("server1");
    rolePrivilege.setScope(TPrivilegeScope.SERVER);
    rolePrivilege.setPrivilege_level(TPrivilegeLevel.ALL);
    catalog_.addRolePrivilege("role1", rolePrivilege);
    assertSame(role, catalog_.getAuthPolicy().getPrincipal("role1", TPrincipalType.ROLE));
    assertNull(catalog_.getAuthPolicy().getPrincipal("role1", TPrincipalType.USER));
    assertNull(catalog_.getAuthPolicy().getPrincipal("role2", TPrincipalType.ROLE));
    // Add the same role, the old role will be deleted.
    role = catalog_.addRole("role1", new HashSet<>());
    assertSame(role, authPolicy.getPrincipal("role1", TPrincipalType.ROLE));
    // Delete the role.
    assertSame(role, catalog_.removeRole("role1"));
    assertNull(authPolicy.getPrincipal("role1", TPrincipalType.ROLE));

    // Assert that principal IDs will be unique between roles and users, e.g. no user and
    // role with the same principal ID. The same name can be used for both user and role.
    int size = 10;
    String prefix = "foo";
    for (int i = 0; i < size; i++) {
      String name = prefix + i;
      catalog_.addUser(name);
      catalog_.addRole(name, new HashSet<>());
    }

    for (int i = 0; i < size; i++) {
      String name = prefix + i;
      Principal u = authPolicy.getPrincipal(name, TPrincipalType.USER);
      Principal r = authPolicy.getPrincipal(name, TPrincipalType.ROLE);
      assertEquals(name, u.getName());
      assertEquals(name, r.getName());
      assertNotEquals(u.getId(), r.getId());
    }

    // Validate getAllUsers vs getAllUserNames
    List<User> allUsers = authPolicy.getAllUsers();
    Set<String> allUserNames = authPolicy.getAllUserNames();
    assertEquals(allUsers.size(), allUserNames.size());
    for (Principal principal: allUsers) {
      assertTrue(allUserNames.contains(principal.getName()));
    }

    // Validate getAllRoles and getAllRoleNames work as expected.
    List<Role> allRoles = authPolicy.getAllRoles();
    Set<String> allRoleNames = authPolicy.getAllRoleNames();
    assertEquals(allRoles.size(), allRoleNames.size());
    for (Principal principal: allRoles) {
      assertTrue(allRoleNames.contains(principal.getName()));
    }
  }
}
