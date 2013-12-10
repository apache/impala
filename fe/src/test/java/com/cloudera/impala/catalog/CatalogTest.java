// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import static com.cloudera.impala.thrift.ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.analysis.HdfsURI;
import com.cloudera.impala.analysis.IntLiteral;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.catalog.HdfsStorageDescriptor.InvalidStorageDescriptorException;
import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.cloudera.impala.thrift.TFunctionType;
import com.cloudera.impala.thrift.TUniqueId;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CatalogTest {
  private static Catalog catalog;

  @BeforeClass
  public static void setUp() throws Exception {
    catalog = new CatalogServiceCatalog(new TUniqueId(0L, 0L));
  }

  @AfterClass
  public static void cleanUp() {
    catalog.close();
  }

  private void checkTableCols(Db db, String tblName, int numClusteringCols,
      String[] colNames, PrimitiveType[] colTypes) throws TableLoadingException {
    Table tbl = db.getTable(tblName);
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
      assertEquals(col.getType(), colTypes[i]);
      ++i;
    }
  }

  private void checkHBaseTableCols(Db db, String hiveTableName, String hbaseTableName,
      String[] hiveColNames, String[] colFamilies, String[] colQualifiers,
      PrimitiveType[] colTypes) throws TableLoadingException{
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
  public void TestColSchema() throws TableLoadingException {
    Db functionalDb = catalog.getDb("functional");
    assertNotNull(functionalDb);
    assertEquals(functionalDb.getName(), "functional");
    assertNotNull(functionalDb.getTable("alltypes"));
    assertNotNull(functionalDb.getTable("alltypes_view"));
    assertNotNull(functionalDb.getTable("alltypes_view_sub"));
    assertNotNull(functionalDb.getTable("alltypessmall"));
    assertNotNull(functionalDb.getTable("alltypeserror"));
    assertNotNull(functionalDb.getTable("alltypeserrornonulls"));
    assertNotNull(functionalDb.getTable("alltypesagg"));
    assertNotNull(functionalDb.getTable("alltypesaggnonulls"));
    assertNotNull(functionalDb.getTable("alltypesnopart"));
    assertNotNull(functionalDb.getTable("alltypesinsert"));
    assertNotNull(functionalDb.getTable("complex_view"));
    assertNotNull(functionalDb.getTable("testtbl"));
    assertNotNull(functionalDb.getTable("dimtbl"));
    assertNotNull(functionalDb.getTable("jointbl"));
    assertNotNull(functionalDb.getTable("liketbl"));
    assertNotNull(functionalDb.getTable("greptiny"));
    assertNotNull(functionalDb.getTable("rankingssmall"));
    assertNotNull(functionalDb.getTable("uservisitssmall"));
    assertNotNull(functionalDb.getTable("view_view"));
    // IMP-163 - table with string partition column does not load if there are partitions
    assertNotNull(functionalDb.getTable("StringPartitionKey"));
    // Test non-existent table
    assertNull(functionalDb.getTable("nonexistenttable"));

    // functional_seq contains the same tables as functional
    Db testDb = catalog.getDb("functional_seq");
    assertNotNull(testDb);
    assertEquals(testDb.getName(), "functional_seq");
    assertNotNull(testDb.getTable("alltypes"));
    assertNotNull(testDb.getTable("testtbl"));

    Db hbaseDb = catalog.getDb("functional_hbase");
    assertNotNull(hbaseDb);
    assertEquals(hbaseDb.getName(), "functional_hbase");
    // Loading succeeds for an HBase table that has binary columns and an implicit key
    // column mapping
    assertNotNull(hbaseDb.getTable("alltypessmallbinary"));
    assertNotNull(hbaseDb.getTable("alltypessmall"));
    assertNotNull(hbaseDb.getTable("hbasealltypeserror"));
    assertNotNull(hbaseDb.getTable("hbasealltypeserrornonulls"));
    assertNotNull(hbaseDb.getTable("alltypesagg"));
    assertNotNull(hbaseDb.getTable("stringids"));

    checkTableCols(functionalDb, "alltypes", 2,
        new String[]
          {"year", "month", "id", "bool_col", "tinyint_col", "smallint_col",
           "int_col", "bigint_col", "float_col", "double_col", "date_string_col",
           "string_col", "timestamp_col"},
        new PrimitiveType[]
          {PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT,
           PrimitiveType.BOOLEAN, PrimitiveType.TINYINT, PrimitiveType.SMALLINT,
           PrimitiveType.INT, PrimitiveType.BIGINT, PrimitiveType.FLOAT,
           PrimitiveType.DOUBLE, PrimitiveType.STRING, PrimitiveType.STRING,
           PrimitiveType.TIMESTAMP});
    checkTableCols(functionalDb, "testtbl", 0,
        new String[] {"id", "name", "zip"},
        new PrimitiveType[]
          {PrimitiveType.BIGINT, PrimitiveType.STRING, PrimitiveType.INT});
    checkTableCols(testDb, "testtbl", 0,
        new String[] {"id", "name", "zip"},
        new PrimitiveType[]
          {PrimitiveType.BIGINT, PrimitiveType.STRING, PrimitiveType.INT});
    checkTableCols(functionalDb, "liketbl", 0,
        new String[] {
            "str_col", "match_like_col", "no_match_like_col", "match_regex_col",
            "no_match_regex_col"},
        new PrimitiveType[]
          {PrimitiveType.STRING, PrimitiveType.STRING, PrimitiveType.STRING,
           PrimitiveType.STRING, PrimitiveType.STRING});
    checkTableCols(functionalDb, "dimtbl", 0,
        new String[] {"id", "name", "zip"},
        new PrimitiveType[]
          {PrimitiveType.BIGINT, PrimitiveType.STRING, PrimitiveType.INT});
    checkTableCols(functionalDb, "jointbl", 0,
        new String[] {"test_id", "test_name", "test_zip", "alltypes_id"},
        new PrimitiveType[]
          {PrimitiveType.BIGINT, PrimitiveType.STRING, PrimitiveType.INT,
           PrimitiveType.INT});

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
        new PrimitiveType[]
          {PrimitiveType.INT, PrimitiveType.BIGINT, PrimitiveType.BOOLEAN,
           PrimitiveType.STRING, PrimitiveType.DOUBLE, PrimitiveType.FLOAT,
           PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.SMALLINT,
           PrimitiveType.STRING, PrimitiveType.TIMESTAMP,
           PrimitiveType.TINYINT, PrimitiveType.INT});

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
        new PrimitiveType[]
          {PrimitiveType.INT, PrimitiveType.BIGINT, PrimitiveType.BOOLEAN,
           PrimitiveType.STRING, PrimitiveType.DOUBLE, PrimitiveType.FLOAT,
           PrimitiveType.INT, PrimitiveType.SMALLINT, PrimitiveType.STRING,
           PrimitiveType.TIMESTAMP, PrimitiveType.TINYINT});

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
        new PrimitiveType[]
          {PrimitiveType.INT, PrimitiveType.BIGINT, PrimitiveType.BOOLEAN,
           PrimitiveType.STRING, PrimitiveType.DOUBLE, PrimitiveType.FLOAT,
           PrimitiveType.INT, PrimitiveType.SMALLINT, PrimitiveType.STRING,
           PrimitiveType.TIMESTAMP, PrimitiveType.TINYINT});

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
        new PrimitiveType[]
          {PrimitiveType.INT, PrimitiveType.BIGINT, PrimitiveType.BOOLEAN,
           PrimitiveType.STRING,PrimitiveType.INT, PrimitiveType.DOUBLE,
           PrimitiveType.FLOAT, PrimitiveType.INT, PrimitiveType.INT,
           PrimitiveType.SMALLINT, PrimitiveType.STRING, PrimitiveType.TIMESTAMP,
           PrimitiveType.TINYINT, PrimitiveType.INT});

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
        new PrimitiveType[]
          {PrimitiveType.STRING, PrimitiveType.BIGINT, PrimitiveType.BOOLEAN,
           PrimitiveType.STRING,PrimitiveType.INT, PrimitiveType.DOUBLE,
           PrimitiveType.FLOAT, PrimitiveType.INT, PrimitiveType.INT,
           PrimitiveType.SMALLINT, PrimitiveType.STRING, PrimitiveType.TIMESTAMP,
           PrimitiveType.TINYINT, PrimitiveType.INT});

    checkTableCols(functionalDb, "greptiny", 0,
        new String[]
          {"field"},
        new PrimitiveType[]
          {PrimitiveType.STRING});

    checkTableCols(functionalDb, "rankingssmall", 0,
        new String[]
          {"pagerank", "pageurl", "avgduration"},
        new PrimitiveType[]
          {PrimitiveType.INT, PrimitiveType.STRING, PrimitiveType.INT});

    checkTableCols(functionalDb, "uservisitssmall", 0,
        new String[]
          {"sourceip", "desturl", "visitdate",  "adrevenue", "useragent",
           "ccode", "lcode", "skeyword", "avgtimeonsite"},
        new PrimitiveType[]
          {PrimitiveType.STRING, PrimitiveType.STRING, PrimitiveType.STRING,
           PrimitiveType.FLOAT, PrimitiveType.STRING, PrimitiveType.STRING,
           PrimitiveType.STRING, PrimitiveType.STRING, PrimitiveType.INT});

    // case-insensitive lookup
    assertEquals(functionalDb.getTable("alltypes"), functionalDb.getTable("AllTypes"));
  }

  @Test public void TestPartitions() throws TableLoadingException {
    HdfsTable table =
        (HdfsTable) catalog.getDb("functional").getTable("AllTypes");
    List<HdfsPartition> partitions = table.getPartitions();

    // check that partition keys cover the date range 1/1/2009-12/31/2010
    // and that we have one file per partition, plus the default partition
    assertEquals(25, partitions.size());
    Set<Long> months = Sets.newHashSet();
    for (HdfsPartition p: partitions) {
      if (p.getId() == DEFAULT_PARTITION_ID) {
        continue;
      }

      assertEquals(2, p.getPartitionValues().size());

      LiteralExpr key1Expr = p.getPartitionValues().get(0);
      assertTrue(key1Expr instanceof IntLiteral);
      long key1 = ((IntLiteral) key1Expr).getValue();
      assertTrue(key1 == 2009 || key1 == 2010);

      LiteralExpr key2Expr = p.getPartitionValues().get(1);
      assertTrue(key2Expr instanceof IntLiteral);
      long key2 = ((IntLiteral) key2Expr).getValue();
      assertTrue(key2 >= 1 && key2 <= 12);

      months.add(key1 * 100 + key2);

      assertEquals(p.getFileDescriptors().size(), 1);
    }
    assertEquals(months.size(), 24);
  }

  @Test
  public void testStats() throws TableLoadingException {
    // make sure the stats for functional.alltypesagg look correct
    HdfsTable table =
        (HdfsTable) catalog.getDb("functional").getTable("AllTypesAgg");

    Column idCol = table.getColumn("id");
    assertEquals(idCol.getStats().getAvgSerializedSize(),
        PrimitiveType.INT.getSlotSize(), 0.0001);
    assertEquals(idCol.getStats().getMaxSize(), PrimitiveType.INT.getSlotSize());
    assertTrue(!idCol.getStats().hasNulls());

    Column boolCol = table.getColumn("bool_col");
    assertEquals(boolCol.getStats().getAvgSerializedSize(),
        PrimitiveType.BOOLEAN.getSlotSize(), 0.0001);
    assertEquals(boolCol.getStats().getMaxSize(), PrimitiveType.BOOLEAN.getSlotSize());
    assertTrue(!boolCol.getStats().hasNulls());

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
    // this does not have nulls, it's not clear why this passes
    // TODO: investigate and re-enable
    //assertTrue(timestampCol.getStats().hasNulls());

    Column stringCol = table.getColumn("string_col");
    assertTrue(
        stringCol.getStats().getAvgSerializedSize() >= PrimitiveType.STRING.getSlotSize());
    assertTrue(stringCol.getStats().getAvgSerializedSize() > 0);
    assertTrue(stringCol.getStats().getMaxSize() > 0);
    assertTrue(!stringCol.getStats().hasNulls());
  }

  /**
   * Verifies that updating column stats data for a type that isn't compatible with
   * the column type results in the stats being set to "unknown". This is a regression
   * test for IMPALA-588, where this used to result in a Preconditions failure.
   */
  @Test
  public void testColStatsColTypeMismatch() throws Exception {
    // First load a table that has column stats.
    catalog.getDb("functional").invalidateTable("functional");
    HdfsTable table = (HdfsTable) catalog.getDb("functional").getTable("alltypesagg");

    // Now attempt to update a column's stats with mismatched stats data and ensure
    // we get the expected results.
    MetaStoreClient client = catalog.getMetaStoreClient();
    try {
      // Load some string stats data and use it to update the stats of different
      // typed columns.
      ColumnStatisticsData stringColStatsData = client.getHiveClient()
          .getTableColumnStatistics("functional", "alltypesagg", "string_col")
          .getStatsObj().get(0).getStatsData();

      assertTrue(!table.getColumn("int_col").updateStats(stringColStatsData));
      assertStatsUnknown(table.getColumn("int_col"));

      assertTrue(!table.getColumn("double_col").updateStats(stringColStatsData));
      assertStatsUnknown(table.getColumn("double_col"));

      assertTrue(!table.getColumn("bool_col").updateStats(stringColStatsData));
      assertStatsUnknown(table.getColumn("bool_col"));

      // Do the same thing, but apply bigint stats to a string column.
      ColumnStatisticsData bigIntCol = client.getHiveClient()
          .getTableColumnStatistics("functional", "alltypes", "bigint_col")
          .getStatsObj().get(0).getStatsData();
      assertTrue(!table.getColumn("string_col").updateStats(bigIntCol));
      assertStatsUnknown(table.getColumn("string_col"));

      // Now try to apply a matching column stats data and ensure it succeeds.
      assertTrue(table.getColumn("string_col").updateStats(stringColStatsData));
      assertEquals(1178, table.getColumn("string_col").getStats().getNumDistinctValues());
    } finally {
      // Make sure to invalidate the metadata so the next test isn't using bad col stats
      catalog.getDb("functional").invalidateTable("functional");
      client.release();
    }
  }

  private void assertStatsUnknown(Column column) {
    assertEquals(-1, column.getStats().getNumDistinctValues());
    assertEquals(-1, column.getStats().getNumNulls());
    double expectedSize = column.getType().isFixedLengthType() ?
        column.getType().getSlotSize() : -1;

    assertEquals(expectedSize, column.getStats().getAvgSerializedSize(), 0.0001);
    assertEquals(expectedSize, column.getStats().getMaxSize(), 0.0001);
  }

  @Test
  public void testInternalHBaseTable() throws TableLoadingException {
    // Cast will fail if table not an HBaseTable
   HBaseTable table = (HBaseTable)
       catalog.getDb("functional_hbase").getTable("internal_hbase_table");
    assertNotNull("functional_hbase.internal_hbase_table was not found", table);
  }

  public void testMapColumnFails() throws TableLoadingException {
    Table table = catalog.getDb("functional").getTable("map_table");
    assertTrue(table instanceof IncompleteTable);
    IncompleteTable incompleteTable = (IncompleteTable) table;
    assertTrue(incompleteTable.getCause() instanceof TableLoadingException);
  }

  public void testMapColumnFailsOnHBaseTable() throws TableLoadingException {
    Table table = catalog.getDb("functional_hbase").getTable("map_table_hbase");
    assertTrue(table instanceof IncompleteTable);
    IncompleteTable incompleteTable = (IncompleteTable) table;
    assertTrue(incompleteTable.getCause() instanceof TableLoadingException);
  }

  public void testArrayColumnFails() throws TableLoadingException {
    Table table = catalog.getDb("functional").getTable("array_table");
    assertTrue(table instanceof IncompleteTable);
    IncompleteTable incompleteTable = (IncompleteTable) table;
    assertTrue(incompleteTable.getCause() instanceof TableLoadingException);
  }

  @Test
  public void testDatabaseDoesNotExist() {
    Db nonExistentDb = catalog.getDb("doesnotexist");
    assertNull(nonExistentDb);
  }

  @Test
  public void testCreateTableMetadata() throws TableLoadingException {
    Table table = catalog.getDb("functional").getTable("alltypes");
    // Tables are created via Impala so the metadata should have been populated properly.
    // alltypes is an external table.
    assertEquals(System.getProperty("user.name"), table.getMetaStoreTable().getOwner());
    assertEquals(TableType.EXTERNAL_TABLE.toString(),
        table.getMetaStoreTable().getTableType());
    // alltypesinsert is created using CREATE TABLE LIKE and is a MANAGED table
    table = catalog.getDb("functional").getTable("alltypesinsert");
    assertEquals(System.getProperty("user.name"), table.getMetaStoreTable().getOwner());
    assertEquals(TableType.MANAGED_TABLE.toString(),
        table.getMetaStoreTable().getTableType());
  }

  @Test
  public void testLoadingUnsupportedTableTypes() throws TableLoadingException {
    Table table = catalog.getDb("functional").getTable("hive_index_tbl");
    assertTrue(table instanceof IncompleteTable);
    IncompleteTable incompleteTable = (IncompleteTable) table;
    assertTrue(incompleteTable.getCause() instanceof TableLoadingException);
    assertEquals("Unsupported table type 'INDEX_TABLE' for: functional.hive_index_tbl",
        incompleteTable.getCause().getMessage());

    // Table with unsupported SerDe library.
    table = catalog.getDb("functional").getTable("bad_serde");
    assertTrue(table instanceof IncompleteTable);
    incompleteTable = (IncompleteTable) table;
    assertTrue(incompleteTable.getCause() instanceof TableLoadingException);
    assertEquals("Failed to load metadata for table: bad_serde",
        incompleteTable.getCause().getMessage());
    assertTrue(incompleteTable.getCause().getCause()
        instanceof InvalidStorageDescriptorException);
    assertEquals("Impala does not support tables of this type. REASON: SerDe" +
        " library 'org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe' " +
        "is not supported.", incompleteTable.getCause().getCause().getMessage());
  }

  // This table has metadata set so the escape is \n, which is also the tuple delim. This
  // test validates that our representation of the catalog fixes this and removes the
  // escape char.
  @Test public void TestTableWithBadEscapeChar() throws TableLoadingException {
    HdfsTable table =
        (HdfsTable) catalog.getDb("functional").getTable("escapechartesttable");
    List<HdfsPartition> partitions = table.getPartitions();
    for (HdfsPartition p: partitions) {
      HdfsStorageDescriptor desc = p.getInputFormatDescriptor();
      assertEquals(desc.getEscapeChar(), HdfsStorageDescriptor.DEFAULT_ESCAPE_CHAR);
    }
  }

  @Test
  public void TestReload() throws CatalogException {
    // Exercise the internal logic of reloading a partitioned table, an unpartitioned
    // table and an HBase table.
    String[] tableNames = {"alltypes", "alltypesnopart"};
    for (String tableName: tableNames) {
      Table table = catalog.getDb("functional").getTable(tableName);
      table = Table.load(catalog.getNextTableId(),
          catalog.getMetaStoreClient().getHiveClient(),
          catalog.getDb("functional"), tableName, table);
    }
    // Test HBase table
    Table table = catalog.getDb("functional_hbase").getTable("alltypessmall");
    table = Table.load(catalog.getNextTableId(),
        catalog.getMetaStoreClient().getHiveClient(),
        catalog.getDb("functional_hbase"), "alltypessmall", table);
  }

  @Test
  public void TestUdf() throws CatalogException {
    List<String> fnNames =
        catalog.getFunctionSignatures(TFunctionType.SCALAR, "default", "");
    assertEquals(fnNames.size(), 0);

    ArrayList<PrimitiveType> args1 = Lists.newArrayList();
    ArrayList<PrimitiveType> args2 = Lists.newArrayList(PrimitiveType.INT);
    ArrayList<PrimitiveType> args3 = Lists.newArrayList(PrimitiveType.TINYINT);

    catalog.removeFunction(
        new Function(new FunctionName("default", "Foo"), args1,
            PrimitiveType.INVALID_TYPE, false));
    fnNames = catalog.getFunctionSignatures(TFunctionType.SCALAR, "default", null);

    assertEquals(fnNames.size(), 0);

    Udf udf1 = new Udf(new FunctionName("default", "Foo"),
        args1, PrimitiveType.INVALID_TYPE, new HdfsURI("/Foo"), "Foo.class");
    catalog.addFunction(udf1);
    fnNames = catalog.getFunctionSignatures(TFunctionType.SCALAR, "default", null);
    assertEquals(fnNames.size(), 1);
    assertTrue(fnNames.contains("foo()"));

    // Same function name, overloaded arguments
    Udf udf2 = new Udf(new FunctionName("default", "Foo"),
        args2, PrimitiveType.INVALID_TYPE, new HdfsURI("/Foo"), "Foo.class");
    catalog.addFunction(udf2);
    fnNames = catalog.getFunctionSignatures(TFunctionType.SCALAR, "default", null);
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo()"));
    assertTrue(fnNames.contains("foo(INT)"));

    // Add a function with a new name
    Udf udf3 = new Udf(new FunctionName("default", "Bar"),
        args2, PrimitiveType.INVALID_TYPE, new HdfsURI("/Foo"), "Foo.class");
    catalog.addFunction(udf3);
    fnNames = catalog.getFunctionSignatures(TFunctionType.SCALAR, "default", null);
    assertEquals(fnNames.size(), 3);
    assertTrue(fnNames.contains("foo()"));
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop Foo()
    catalog.removeFunction(new Function(
        new FunctionName("default", "Foo"), args1, PrimitiveType.INVALID_TYPE, false));
    fnNames = catalog.getFunctionSignatures(TFunctionType.SCALAR, "default", null);
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop it again, no-op
    catalog.removeFunction(new Function(
        new FunctionName("default", "Foo"), args1, PrimitiveType.INVALID_TYPE, false));
    fnNames = catalog.getFunctionSignatures(TFunctionType.SCALAR, "default", null);
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop bar(), no-op
    catalog.removeFunction(new Function(
        new FunctionName("default", "Bar"), args1, PrimitiveType.INVALID_TYPE, false));
    fnNames = catalog.getFunctionSignatures(TFunctionType.SCALAR, "default", null);
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop bar(tinyint), no-op
    catalog.removeFunction(new Function(
        new FunctionName("default", "Bar"), args3, PrimitiveType.INVALID_TYPE, false));
    fnNames = catalog.getFunctionSignatures(TFunctionType.SCALAR, "default", null);
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop bar(int)
    catalog.removeFunction(new Function(
        new FunctionName("default", "Bar"), args2, PrimitiveType.INVALID_TYPE, false));
    fnNames = catalog.getFunctionSignatures(TFunctionType.SCALAR, "default", null);
    assertEquals(fnNames.size(), 1);
    assertTrue(fnNames.contains("foo(INT)"));

    // Drop foo(int)
    catalog.removeFunction(new Function(
        new FunctionName("default", "foo"), args2, PrimitiveType.INVALID_TYPE, false));
    fnNames = catalog.getFunctionSignatures(TFunctionType.SCALAR, "default", null);
    assertEquals(fnNames.size(), 0);
  }
}
