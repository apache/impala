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
import org.junit.Test;

import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.analysis.HdfsUri;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.analysis.NumericLiteral;
import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.cloudera.impala.testutil.CatalogServiceTestCatalog;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CatalogTest {
  private static CatalogServiceCatalog catalog_ =
      CatalogServiceTestCatalog.create();

  private void checkTableCols(Db db, String tblName, int numClusteringCols,
      String[] colNames, Type[] colTypes) throws TableLoadingException {
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
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypes"));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypes_view"));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypes_view_sub"));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypessmall"));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypeserror"));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypeserrornonulls"));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypesagg"));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypesaggnonulls"));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypesnopart"));
    assertNotNull(catalog_.getOrLoadTable("functional", "alltypesinsert"));
    assertNotNull(catalog_.getOrLoadTable("functional", "complex_view"));
    assertNotNull(catalog_.getOrLoadTable("functional", "testtbl"));
    assertNotNull(catalog_.getOrLoadTable("functional", "dimtbl"));
    assertNotNull(catalog_.getOrLoadTable("functional", "jointbl"));
    assertNotNull(catalog_.getOrLoadTable("functional", "liketbl"));
    assertNotNull(catalog_.getOrLoadTable("functional", "greptiny"));
    assertNotNull(catalog_.getOrLoadTable("functional", "rankingssmall"));
    assertNotNull(catalog_.getOrLoadTable("functional", "uservisitssmall"));
    assertNotNull(catalog_.getOrLoadTable("functional", "view_view"));
    // IMP-163 - table with string partition column does not load if there are partitions
    assertNotNull(catalog_.getOrLoadTable("functional", "StringPartitionKey"));
    // Test non-existent table
    assertNull(catalog_.getOrLoadTable("functional", "nonexistenttable"));

    // functional_seq contains the same tables as functional
    Db testDb = catalog_.getDb("functional_seq");
    assertNotNull(testDb);
    assertEquals(testDb.getName(), "functional_seq");
    assertNotNull(catalog_.getOrLoadTable("functional_seq", "alltypes"));
    assertNotNull(catalog_.getOrLoadTable("functional_seq", "testtbl"));

    Db hbaseDb = catalog_.getDb("functional_hbase");
    assertNotNull(hbaseDb);
    assertEquals(hbaseDb.getName(), "functional_hbase");
    // Loading succeeds for an HBase table that has binary columns and an implicit key
    // column mapping
    assertNotNull(catalog_.getOrLoadTable(hbaseDb.getName(), "alltypessmallbinary"));
    assertNotNull(catalog_.getOrLoadTable(hbaseDb.getName(), "alltypessmall"));
    assertNotNull(catalog_.getOrLoadTable(hbaseDb.getName(), "hbasealltypeserror"));
    assertNotNull(catalog_.getOrLoadTable(hbaseDb.getName(),
        "hbasealltypeserrornonulls"));
    assertNotNull(catalog_.getOrLoadTable(hbaseDb.getName(), "alltypesagg"));
    assertNotNull(catalog_.getOrLoadTable(hbaseDb.getName(), "stringids"));

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

    // case-insensitive lookup
    assertEquals(catalog_.getOrLoadTable("functional", "alltypes"),
        catalog_.getOrLoadTable("functional", "AllTypes"));
  }

  @Test
  public void TestPartitions() throws CatalogException {
    HdfsTable table =
        (HdfsTable) catalog_.getOrLoadTable("functional", "AllTypes");
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
      assertTrue(key1Expr instanceof NumericLiteral);
      long key1 = ((NumericLiteral) key1Expr).getLongValue();
      assertTrue(key1 == 2009 || key1 == 2010);

      LiteralExpr key2Expr = p.getPartitionValues().get(1);
      assertTrue(key2Expr instanceof NumericLiteral);
      long key2 = ((NumericLiteral) key2Expr).getLongValue();
      assertTrue(key2 >= 1 && key2 <= 12);

      months.add(key1 * 100 + key2);

      assertEquals(p.getFileDescriptors().size(), 1);
    }
    assertEquals(months.size(), 24);
  }

  // TODO: All Hive-stats related tests are temporarily disabled because of an unknown,
  // sporadic issue causing stats of some columns to be absent in Jenkins runs.
  // Investigate this issue further.
  //@Test
  public void testStats() throws TableLoadingException {
    // make sure the stats for functional.alltypesagg look correct
    HdfsTable table =
        (HdfsTable) catalog_.getDb("functional").getTable("AllTypesAgg");

    Column idCol = table.getColumn("id");
    assertEquals(idCol.getStats().getAvgSerializedSize() -
        PrimitiveType.INT.getSlotSize(),
        PrimitiveType.INT.getSlotSize(), 0.0001);
    assertEquals(idCol.getStats().getMaxSize(), PrimitiveType.INT.getSlotSize());
    assertTrue(!idCol.getStats().hasNulls());

    Column boolCol = table.getColumn("bool_col");
    assertEquals(boolCol.getStats().getAvgSerializedSize() -
        PrimitiveType.BOOLEAN.getSlotSize(),
        PrimitiveType.BOOLEAN.getSlotSize(), 0.0001);
    assertEquals(boolCol.getStats().getMaxSize(), PrimitiveType.BOOLEAN.getSlotSize());
    assertTrue(!boolCol.getStats().hasNulls());

    Column tinyintCol = table.getColumn("tinyint_col");
    assertEquals(tinyintCol.getStats().getAvgSerializedSize() -
        PrimitiveType.TINYINT.getSlotSize(),
        PrimitiveType.TINYINT.getSlotSize(), 0.0001);
    assertEquals(tinyintCol.getStats().getMaxSize(),
        PrimitiveType.TINYINT.getSlotSize());
    assertTrue(tinyintCol.getStats().hasNulls());

    Column smallintCol = table.getColumn("smallint_col");
    assertEquals(smallintCol.getStats().getAvgSerializedSize() -
        PrimitiveType.SMALLINT.getSlotSize(),
        PrimitiveType.SMALLINT.getSlotSize(), 0.0001);
    assertEquals(smallintCol.getStats().getMaxSize(),
        PrimitiveType.SMALLINT.getSlotSize());
    assertTrue(smallintCol.getStats().hasNulls());

    Column intCol = table.getColumn("int_col");
    assertEquals(intCol.getStats().getAvgSerializedSize() -
        PrimitiveType.INT.getSlotSize(),
        PrimitiveType.INT.getSlotSize(), 0.0001);
    assertEquals(intCol.getStats().getMaxSize(), PrimitiveType.INT.getSlotSize());
    assertTrue(intCol.getStats().hasNulls());

    Column bigintCol = table.getColumn("bigint_col");
    assertEquals(bigintCol.getStats().getAvgSerializedSize() -
        PrimitiveType.BIGINT.getSlotSize(),
        PrimitiveType.BIGINT.getSlotSize(), 0.0001);
    assertEquals(bigintCol.getStats().getMaxSize(), PrimitiveType.BIGINT.getSlotSize());
    assertTrue(bigintCol.getStats().hasNulls());

    Column floatCol = table.getColumn("float_col");
    assertEquals(floatCol.getStats().getAvgSerializedSize() -
        PrimitiveType.FLOAT.getSlotSize(),
        PrimitiveType.FLOAT.getSlotSize(), 0.0001);
    assertEquals(floatCol.getStats().getMaxSize(), PrimitiveType.FLOAT.getSlotSize());
    assertTrue(floatCol.getStats().hasNulls());

    Column doubleCol = table.getColumn("double_col");
    assertEquals(doubleCol.getStats().getAvgSerializedSize() -
        PrimitiveType.DOUBLE.getSlotSize(),
        PrimitiveType.DOUBLE.getSlotSize(), 0.0001);
    assertEquals(doubleCol.getStats().getMaxSize(), PrimitiveType.DOUBLE.getSlotSize());
    assertTrue(doubleCol.getStats().hasNulls());

    Column timestampCol = table.getColumn("timestamp_col");
    assertEquals(timestampCol.getStats().getAvgSerializedSize() -
        PrimitiveType.TIMESTAMP.getSlotSize(),
        PrimitiveType.TIMESTAMP.getSlotSize(), 0.0001);
    assertEquals(timestampCol.getStats().getMaxSize(),
        PrimitiveType.TIMESTAMP.getSlotSize());
    // this does not have nulls, it's not clear why this passes
    // TODO: investigate and re-enable
    //assertTrue(timestampCol.getStats().hasNulls());

    Column stringCol = table.getColumn("string_col");
    assertTrue(stringCol.getStats().getAvgSerializedSize() >=
        PrimitiveType.STRING.getSlotSize());
    assertTrue(stringCol.getStats().getAvgSerializedSize() > 0);
    assertTrue(stringCol.getStats().getMaxSize() > 0);
    assertTrue(!stringCol.getStats().hasNulls());
  }

  /**
   * Verifies that updating column stats data for a type that isn't compatible with
   * the column type results in the stats being set to "unknown". This is a regression
   * test for IMPALA-588, where this used to result in a Preconditions failure.
   */
  // TODO: All Hive-stats related tests are temporarily disabled because of an unknown,
  // sporadic issue causing stats of some columns to be absent in Jenkins runs.
  // Investigate this issue further.
  //@Test
  public void testColStatsColTypeMismatch() throws Exception {
    // First load a table that has column stats.
    //catalog_.refreshTable("functional", "alltypesagg", false);
    HdfsTable table = (HdfsTable) catalog_.getOrLoadTable("functional", "alltypesagg");

    // Now attempt to update a column's stats with mismatched stats data and ensure
    // we get the expected results.
    MetaStoreClient client = catalog_.getMetaStoreClient();
    try {
      // Load some string stats data and use it to update the stats of different
      // typed columns.
      ColumnStatisticsData stringColStatsData = client.getHiveClient()
          .getTableColumnStatistics("functional", "alltypesagg",
           Lists.newArrayList("string_col")).get(0).getStatsData();

      assertTrue(!table.getColumn("int_col").updateStats(stringColStatsData));
      assertStatsUnknown(table.getColumn("int_col"));

      assertTrue(!table.getColumn("double_col").updateStats(stringColStatsData));
      assertStatsUnknown(table.getColumn("double_col"));

      assertTrue(!table.getColumn("bool_col").updateStats(stringColStatsData));
      assertStatsUnknown(table.getColumn("bool_col"));

      // Do the same thing, but apply bigint stats to a string column.
      ColumnStatisticsData bigIntCol = client.getHiveClient()
          .getTableColumnStatistics("functional", "alltypes",
          Lists.newArrayList("bigint_col")).get(0).getStatsData();
      assertTrue(!table.getColumn("string_col").updateStats(bigIntCol));
      assertStatsUnknown(table.getColumn("string_col"));

      // Now try to apply a matching column stats data and ensure it succeeds.
      assertTrue(table.getColumn("string_col").updateStats(stringColStatsData));
      assertEquals(1178, table.getColumn("string_col").getStats().getNumDistinctValues());
    } finally {
      // Make sure to invalidate the metadata so the next test isn't using bad col stats
      //catalog_.refreshTable("functional", "alltypesagg", false);
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
  public void testInternalHBaseTable() throws CatalogException {
    // Cast will fail if table not an HBaseTable
   HBaseTable table = (HBaseTable)
       catalog_.getOrLoadTable("functional_hbase", "internal_hbase_table");
    assertNotNull("functional_hbase.internal_hbase_table was not found", table);
  }

  @Test
  public void testDatabaseDoesNotExist() {
    Db nonExistentDb = catalog_.getDb("doesnotexist");
    assertNull(nonExistentDb);
  }

  @Test
  public void testCreateTableMetadata() throws CatalogException {
    Table table = catalog_.getOrLoadTable("functional", "alltypes");
    // Tables are created via Impala so the metadata should have been populated properly.
    // alltypes is an external table.
    assertEquals(System.getProperty("user.name"), table.getMetaStoreTable().getOwner());
    assertEquals(TableType.EXTERNAL_TABLE.toString(),
        table.getMetaStoreTable().getTableType());
    // alltypesinsert is created using CREATE TABLE LIKE and is a MANAGED table
    table = catalog_.getOrLoadTable("functional", "alltypesinsert");
    assertEquals(System.getProperty("user.name"), table.getMetaStoreTable().getOwner());
    assertEquals(TableType.MANAGED_TABLE.toString(),
        table.getMetaStoreTable().getTableType());
  }

  @Test
  public void testLoadingUnsupportedTableTypes() throws CatalogException {
    Table table = catalog_.getOrLoadTable("functional", "hive_index_tbl");
    assertTrue(table instanceof IncompleteTable);
    IncompleteTable incompleteTable = (IncompleteTable) table;
    assertTrue(incompleteTable.getCause() instanceof TableLoadingException);
    assertEquals("Unsupported table type 'INDEX_TABLE' for: functional.hive_index_tbl",
        incompleteTable.getCause().getMessage());

    // Table with unsupported SerDe library.
    table = catalog_.getOrLoadTable("functional", "bad_serde");
    assertTrue(table instanceof IncompleteTable);
    incompleteTable = (IncompleteTable) table;
    assertTrue(incompleteTable.getCause() instanceof TableLoadingException);
    assertEquals("Impala does not support tables of this type. REASON: SerDe" +
        " library 'org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe' " +
        "is not supported.", incompleteTable.getCause().getCause().getMessage());

    // Impala does not yet support Hive's LazyBinaryColumnarSerDe which can be
    // used for RCFILE tables.
    table = catalog_.getOrLoadTable("functional_rc", "rcfile_lazy_binary_serde");
    assertTrue(table instanceof IncompleteTable);
    incompleteTable = (IncompleteTable) table;
    assertTrue(incompleteTable.getCause() instanceof TableLoadingException);
    assertEquals("Impala does not support tables of this type. REASON: SerDe" +
        " library 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe' " +
        "is not supported.", incompleteTable.getCause().getCause().getMessage());
  }

  private List<String> getFunctionSignatures(String db) throws DatabaseNotFoundException {
    List<Function> fns = catalog_.getFunctions(db);
    List<String> names = Lists.newArrayList();
    for (Function fn: fns) {
      names.add(fn.signatureString());
    }
    return names;
  }

  @Test
  public void TestUdf() throws CatalogException {
    List<String> fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 0);

    ArrayList<Type> args1 = Lists.newArrayList();
    ArrayList<Type> args2 = Lists.<Type>newArrayList(Type.INT);
    ArrayList<Type> args3 = Lists.<Type>newArrayList(Type.TINYINT);

    catalog_.removeFunction(
        new Function(new FunctionName("default", "Foo"), args1,
            Type.INVALID, false));
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 0);

    ScalarFunction udf1 = new ScalarFunction(new FunctionName("default", "Foo"),
        args1, Type.INVALID, new HdfsUri("/Foo"), "Foo.class", null, null);
    catalog_.addFunction(udf1);
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 1);
    assertTrue(fnNames.contains("foo()"));

    // Same function name, overloaded arguments
    ScalarFunction udf2 = new ScalarFunction(new FunctionName("default", "Foo"),
        args2, Type.INVALID, new HdfsUri("/Foo"), "Foo.class", null, null);
    catalog_.addFunction(udf2);
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo()"));
    assertTrue(fnNames.contains("foo(INT)"));

    // Add a function with a new name
    ScalarFunction udf3 = new ScalarFunction(new FunctionName("default", "Bar"),
        args2, Type.INVALID, new HdfsUri("/Foo"), "Foo.class", null, null);
    catalog_.addFunction(udf3);
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 3);
    assertTrue(fnNames.contains("foo()"));
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop Foo()
    catalog_.removeFunction(new Function(
        new FunctionName("default", "Foo"), args1, Type.INVALID, false));
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop it again, no-op
    catalog_.removeFunction(new Function(
        new FunctionName("default", "Foo"), args1, Type.INVALID, false));
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop bar(), no-op
    catalog_.removeFunction(new Function(
        new FunctionName("default", "Bar"), args1, Type.INVALID, false));
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop bar(tinyint), no-op
    catalog_.removeFunction(new Function(
        new FunctionName("default", "Bar"), args3, Type.INVALID, false));
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 2);
    assertTrue(fnNames.contains("foo(INT)"));
    assertTrue(fnNames.contains("bar(INT)"));

    // Drop bar(int)
    catalog_.removeFunction(new Function(
        new FunctionName("default", "Bar"), args2, Type.INVALID, false));
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 1);
    assertTrue(fnNames.contains("foo(INT)"));

    // Drop foo(int)
    catalog_.removeFunction(new Function(
        new FunctionName("default", "foo"), args2, Type.INVALID, false));
    fnNames = getFunctionSignatures("default");
    assertEquals(fnNames.size(), 0);
  }
}
