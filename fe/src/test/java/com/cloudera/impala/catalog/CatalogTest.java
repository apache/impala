// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import static com.cloudera.impala.thrift.ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.analysis.HdfsURI;
import com.cloudera.impala.analysis.IntLiteral;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.authorization.ImpalaInternalAdminUser;
import com.cloudera.impala.authorization.Privilege;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CatalogTest {
  private static Catalog catalog;

  @BeforeClass
  public static void setUp() throws Exception {
    catalog = new Catalog();
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
    Db defaultDb = getDb(catalog, "functional");
    Db hbaseDb = getDb(catalog, "functional_hbase");
    Db testDb = getDb(catalog, "functional_seq");

    assertNotNull(defaultDb);
    assertEquals(defaultDb.getName(), "functional");
    assertNotNull(testDb);
    assertEquals(testDb.getName(), "functional_seq");
    assertNotNull(hbaseDb);
    assertEquals(hbaseDb.getName(), "functional_hbase");

    assertNotNull(defaultDb.getTable("alltypes"));
    assertNotNull(defaultDb.getTable("alltypes_view"));
    assertNotNull(defaultDb.getTable("alltypes_view_sub"));
    assertNotNull(defaultDb.getTable("alltypessmall"));
    assertNotNull(defaultDb.getTable("alltypeserror"));
    assertNotNull(defaultDb.getTable("alltypeserrornonulls"));
    assertNotNull(defaultDb.getTable("alltypesagg"));
    assertNotNull(defaultDb.getTable("alltypesaggnonulls"));
    assertNotNull(defaultDb.getTable("alltypesnopart"));
    assertNotNull(defaultDb.getTable("alltypesinsert"));
    assertNotNull(defaultDb.getTable("complex_view"));
    assertNotNull(defaultDb.getTable("testtbl"));
    assertNotNull(defaultDb.getTable("dimtbl"));
    assertNotNull(defaultDb.getTable("jointbl"));
    assertNotNull(defaultDb.getTable("liketbl"));
    assertNotNull(defaultDb.getTable("greptiny"));
    assertNotNull(defaultDb.getTable("rankingssmall"));
    assertNotNull(defaultDb.getTable("uservisitssmall"));
    assertNotNull(defaultDb.getTable("view_view"));
    assertNotNull(hbaseDb.getTable("alltypessmall"));
    assertNotNull(hbaseDb.getTable("hbasealltypeserror"));
    assertNotNull(hbaseDb.getTable("hbasealltypeserrornonulls"));
    assertNotNull(hbaseDb.getTable("alltypesagg"));
    assertNotNull(hbaseDb.getTable("stringids"));

    // IMP-163 - table with string partition column does not load if there are partitions
    assertNotNull(defaultDb.getTable("StringPartitionKey"));

    // functional_seq contains the same tables as functional
    assertNotNull(testDb.getTable("alltypes"));
    assertNotNull(testDb.getTable("testtbl"));

    // Test non-existant table
    assertNull(defaultDb.getTable("nonexistenttable"));

    checkTableCols(defaultDb, "alltypes", 2,
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
    checkTableCols(defaultDb, "testtbl", 0,
        new String[] {"id", "name", "zip"},
        new PrimitiveType[]
          {PrimitiveType.BIGINT, PrimitiveType.STRING, PrimitiveType.INT});
    checkTableCols(testDb, "testtbl", 0,
        new String[] {"id", "name", "zip"},
        new PrimitiveType[]
          {PrimitiveType.BIGINT, PrimitiveType.STRING, PrimitiveType.INT});
    checkTableCols(defaultDb, "liketbl", 0,
        new String[] {
            "str_col", "match_like_col", "no_match_like_col", "match_regex_col",
            "no_match_regex_col"},
        new PrimitiveType[]
          {PrimitiveType.STRING, PrimitiveType.STRING, PrimitiveType.STRING,
           PrimitiveType.STRING, PrimitiveType.STRING});
    checkTableCols(defaultDb, "dimtbl", 0,
        new String[] {"id", "name", "zip"},
        new PrimitiveType[]
          {PrimitiveType.BIGINT, PrimitiveType.STRING, PrimitiveType.INT});
    checkTableCols(defaultDb, "jointbl", 0,
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

    checkTableCols(defaultDb, "greptiny", 0,
        new String[]
          {"field"},
        new PrimitiveType[]
          {PrimitiveType.STRING});

    checkTableCols(defaultDb, "rankingssmall", 0,
        new String[]
          {"pagerank", "pageurl", "avgduration"},
        new PrimitiveType[]
          {PrimitiveType.INT, PrimitiveType.STRING, PrimitiveType.INT});

    checkTableCols(defaultDb, "uservisitssmall", 0,
        new String[]
          {"sourceip", "desturl", "visitdate",  "adrevenue", "useragent",
           "ccode", "lcode", "skeyword", "avgtimeonsite"},
        new PrimitiveType[]
          {PrimitiveType.STRING, PrimitiveType.STRING, PrimitiveType.STRING,
           PrimitiveType.FLOAT, PrimitiveType.STRING, PrimitiveType.STRING,
           PrimitiveType.STRING, PrimitiveType.STRING, PrimitiveType.INT});

    // case-insensitive lookup
    assertEquals(defaultDb.getTable("alltypes"), defaultDb.getTable("AllTypes"));
  }

  @Test public void TestPartitions() throws TableLoadingException {
    HdfsTable table = (HdfsTable) getDb(catalog, "functional").getTable("AllTypes");
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
    HdfsTable table = (HdfsTable) getDb(catalog, "functional").getTable("AllTypesAgg");

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
        stringCol.getStats().getAvgSerializedSize() > PrimitiveType.STRING.getSlotSize());
    assertEquals(stringCol.getStats().getMaxSize(), 3);
    assertTrue(!stringCol.getStats().hasNulls());
  }

  @Test
  public void testInternalHBaseTable() throws TableLoadingException {
    // Cast will fail if table not an HBaseTable
   HBaseTable table =
        (HBaseTable)getDb(catalog, "functional_hbase").getTable("internal_hbase_table");
    assertNotNull("functional_hbase.internal_hbase_table was not found", table);
  }

  @Test(expected = TableLoadingException.class)
  public void testMapColumnFails() throws TableLoadingException {
    Table table = getDb(catalog, "functional").getTable("map_table");
  }

  @Test(expected = TableLoadingException.class)
  public void testMapColumnFailsOnHBaseTable() throws TableLoadingException {
    Table table = getDb(catalog, "functional_hbase").getTable("map_table_hbase");
  }

  @Test(expected = TableLoadingException.class)
  public void testArrayColumnFails() throws TableLoadingException {
    Table table = getDb(catalog, "functional").getTable("array_table");
  }

  @Test
  public void testDatabaseDoesNotExist() {
    Db nonExistentDb = getDb(catalog, "doesnotexist");
    assertNull(nonExistentDb);
  }

  @Test
  public void testCreateTableMetadata() throws TableLoadingException {
    Table table = getDb(catalog, "functional").getTable("alltypes");
    // Tables are created via Impala so the metadata should have been populated properly.
    // alltypes is an external table.
    assertEquals(System.getProperty("user.name"), table.getMetaStoreTable().getOwner());
    assertEquals(TableType.EXTERNAL_TABLE.toString(),
        table.getMetaStoreTable().getTableType());
    // alltypesinsert is created using CREATE TABLE LIKE and is a MANAGED table
    table = getDb(catalog, "functional").getTable("alltypesinsert");
    assertEquals(System.getProperty("user.name"), table.getMetaStoreTable().getOwner());
    assertEquals(TableType.MANAGED_TABLE.toString(),
        table.getMetaStoreTable().getTableType());
  }

  @Test
  public void testLoadingUnsupportedTableTypes() {
    try {
      Table table = getDb(catalog, "functional").getTable("hive_index_tbl");
      fail("Expected TableLoadingException when loading INDEX_TABLE");
    } catch (TableLoadingException e) {
      assertEquals("Unsupported table type 'INDEX_TABLE' for: functional.hive_index_tbl",
          e.getMessage());
    }
  }

  // This table has metadata set so the escape is \n, which is also the tuple delim. This
  // test validates that our representation of the catalog fixes this and removes the
  // escape char.
  @Test public void TestTableWithBadEscapeChar() throws TableLoadingException {
    HdfsTable table =
        (HdfsTable) getDb(catalog, "functional").getTable("escapechartesttable");
    List<HdfsPartition> partitions = table.getPartitions();
    for (HdfsPartition p: partitions) {
      HdfsStorageDescriptor desc = p.getInputFormatDescriptor();
      assertEquals(desc.getEscapeChar(), HdfsStorageDescriptor.DEFAULT_ESCAPE_CHAR);
    }
  }

  @Test
  public void TestHiveMetaStoreClientCreationRetry() throws MetaException {
    HiveConf conf = new HiveConf(CatalogTest.class);
    // Set the Metastore warehouse to an empty string to trigger a MetaException
    conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, "");
    try {
      MetaStoreClientPool pool = new MetaStoreClientPool(1, conf);
      fail("Expected MetaException");
    } catch (IllegalStateException e) {
      assertTrue(e.getCause() instanceof MetaException);
    }

    conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, "/some/valid/path");
    MetaStoreClientPool pool = new MetaStoreClientPool(1, conf);

    // TODO: This doesn't fully validate the retry logic. In the future we
    // could throw an exception when the retry attempts maxed out. This exception
    // would have details on the number of retries, etc. We also need coverage for the
    // case where we we have a few failure/retries and then a success.
  }

  @Test
  public void TestReload() throws CatalogException {
    // Exercise the internal logic of reloading a partitioned table, an unpartitioned
    // table and an HBase table.
    String[] tableNames = {"alltypes", "alltypesnopart"};
    for (String tableName: tableNames) {
      Table table = getDb(catalog, "functional").getTable(tableName);
      table = Table.load(catalog.getNextTableId(),
          catalog.getMetaStoreClient().getHiveClient(),
          getDb(catalog, "functional"), tableName, table);
    }
    // Test HBase table
    Table table = getDb(catalog, "functional_hbase").getTable("alltypessmall");
    table = Table.load(catalog.getNextTableId(),
        catalog.getMetaStoreClient().getHiveClient(),
        getDb(catalog, "functional_hbase"), "alltypessmall", table);
  }

  @Test
  public void TestUdf() throws CatalogException {
    List<String> fnNames = catalog.getUdfNames("default", "");
    assertEquals(fnNames.size(), 0);

    ArrayList<PrimitiveType> args1 = Lists.newArrayList();
    ArrayList<PrimitiveType> args2 = Lists.newArrayList(PrimitiveType.INT);
    ArrayList<PrimitiveType> args3 = Lists.newArrayList(PrimitiveType.TINYINT);

    catalog.removeUdf(
        new Function(new FunctionName("default", "Foo"), args1, PrimitiveType.INVALID_TYPE, false));
    fnNames = catalog.getUdfNames("default", null);
    assertEquals(fnNames.size(), 0);

    Udf udf1 = new Udf(new FunctionName("default", "Foo"),
        args1, PrimitiveType.INVALID_TYPE, new HdfsURI("/Foo"), "Foo.class");
    catalog.addUdf(udf1);
    fnNames = catalog.getUdfNames("default", null);
    assertEquals(fnNames.size(), 1);
    assertTrue(fnNames.contains("foo()"));

    // Same function name, overloaded arguments
    Udf udf2 = new Udf(new FunctionName("default", "Foo"),
        args2, PrimitiveType.INVALID_TYPE, new HdfsURI("/Foo"), "Foo.class");
    catalog.addUdf(udf2);
    fnNames = catalog.getUdfNames("default", null);
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo()"));
    assertTrue(fnNames.contains("foo(INT)"));

    // Add a function with a new name
    Udf udf3 = new Udf(new FunctionName("default", "Bar"),
        args2, PrimitiveType.INVALID_TYPE, new HdfsURI("/Foo"), "Foo.class");
    catalog.addUdf(udf3);
    fnNames = catalog.getUdfNames("default", null);
    assertEquals(fnNames.size(), 3);
    assertTrue(fnNames.contains("foo()"));
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop Foo()
    catalog.removeUdf(new Function(
        new FunctionName("default", "Foo"), args1, PrimitiveType.INVALID_TYPE, false));
    fnNames = catalog.getUdfNames("default", null);
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop it again, no-op
    catalog.removeUdf(new Function(
        new FunctionName("default", "Foo"), args1, PrimitiveType.INVALID_TYPE, false));
    fnNames = catalog.getUdfNames("default", null);
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop bar(), no-op
    catalog.removeUdf(new Function(
        new FunctionName("default", "Bar"), args1, PrimitiveType.INVALID_TYPE, false));
    fnNames = catalog.getUdfNames("default", null);
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop bar(tinyint), no-op
    catalog.removeUdf(new Function(
        new FunctionName("default", "Bar"), args3, PrimitiveType.INVALID_TYPE, false));
    fnNames = catalog.getUdfNames("default", null);
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop bar(int)
    catalog.removeUdf(new Function(
        new FunctionName("default", "Bar"), args2, PrimitiveType.INVALID_TYPE, false));
    fnNames = catalog.getUdfNames("default", null);
    assertEquals(fnNames.size(), 1);
    assertTrue(fnNames.contains("foo(INT)"));

    // Drop foo(int)
    catalog.removeUdf(new Function(
        new FunctionName("default", "foo"), args2, PrimitiveType.INVALID_TYPE, false));
    fnNames = catalog.getUdfNames("default", null);
    assertEquals(fnNames.size(), 0);
  }

  private static Db getDb(Catalog catalog, String dbName) {
    try {
      return catalog.getDb(dbName, ImpalaInternalAdminUser.getInstance(), Privilege.ANY);
    } catch (AuthorizationException e) {
      // Wrap as unchecked exception
      throw new IllegalStateException(e);
    }
  }
}
