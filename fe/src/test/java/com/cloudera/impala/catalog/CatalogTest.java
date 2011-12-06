// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.serde.Constants;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.analysis.IntLiteral;
import com.cloudera.impala.analysis.LiteralExpr;
import com.google.common.collect.Sets;

public class CatalogTest {
  private static Catalog catalog;

  @BeforeClass public static void setUp() throws Exception {
    HiveMetaStoreClient client = TestSchemaUtils.createClient();
    catalog = new Catalog(client);
  }

  private void checkTableCols(Db db, String tblName, int numClusteringCols,
      String[] colNames, PrimitiveType[] colTypes) {
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
      PrimitiveType[] colTypes) {
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

  @Test public void TestColSchema() {
    Collection<Db> dbs = catalog.getDbs();
    Db defaultDb = null;
    Db testDb = null;
    for (Db db: dbs) {
      if (db.getName().equals("default")) {
        defaultDb = db;
      }
      if (db.getName().equals("testdb1")) {
        testDb = db;
      }
    }
    assertNotNull(defaultDb);
    assertEquals(defaultDb.getName(), "default");
    assertNotNull(testDb);
    assertEquals(testDb.getName(), "testdb1");

    assertEquals(25, defaultDb.getTables().size());
    assertNotNull(defaultDb.getTable("alltypes"));
    assertNotNull(defaultDb.getTable("alltypes_rc"));
    assertNotNull(defaultDb.getTable("alltypessmall"));
    assertNotNull(defaultDb.getTable("alltypessmall_rc"));
    assertNotNull(defaultDb.getTable("alltypeserror"));
    assertNotNull(defaultDb.getTable("alltypeserror_rc"));
    assertNotNull(defaultDb.getTable("alltypeserrornonulls"));
    assertNotNull(defaultDb.getTable("alltypeserrornonulls_rc"));
    assertNotNull(defaultDb.getTable("alltypesagg"));
    assertNotNull(defaultDb.getTable("alltypesagg_rc"));
    assertNotNull(defaultDb.getTable("alltypesaggnonulls"));
    assertNotNull(defaultDb.getTable("alltypesaggnonulls_rc"));
    assertNotNull(defaultDb.getTable("alltypesnopart"));
    assertNotNull(defaultDb.getTable("testtbl"));
    assertNotNull(defaultDb.getTable("testtbl_rc"));
    assertNotNull(defaultDb.getTable("dimtbl"));
    assertNotNull(defaultDb.getTable("jointbl"));
    assertNotNull(defaultDb.getTable("liketbl"));
    assertNotNull(defaultDb.getTable("hbasealltypessmall"));
    assertNotNull(defaultDb.getTable("hbasealltypeserror"));
    assertNotNull(defaultDb.getTable("hbasealltypeserrornonulls"));
    assertNotNull(defaultDb.getTable("hbasealltypesagg"));
    assertNotNull(defaultDb.getTable("hbasestringids"));
    assertNotNull(defaultDb.getTable("greptiny"));
    assertNotNull(defaultDb.getTable("grep1gb"));

    // testdb contains tables alltypes and testtbl.
    assertEquals(2, testDb.getTables().size());
    assertNotNull(testDb.getTable("alltypes"));
    assertNotNull(testDb.getTable("testtbl"));

    // We should have failed to load this table.
    assertNull(defaultDb.getTable("delimerrortable"));

    checkTableCols(defaultDb, "alltypes", 2,
        new String[]
          {"year", "month", "id", "bool_col", "tinyint_col", "smallint_col",
           "int_col", "bigint_col", "float_col", "double_col", "date_string_col",
           "string_col"},
        new PrimitiveType[]
          {PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT,
           PrimitiveType.BOOLEAN, PrimitiveType.TINYINT, PrimitiveType.SMALLINT,
           PrimitiveType.INT, PrimitiveType.BIGINT, PrimitiveType.FLOAT,
           PrimitiveType.DOUBLE, PrimitiveType.STRING, PrimitiveType.STRING});
    checkTableCols(testDb, "alltypes", 0,
        new String[]
          {"id", "bool_col", "tinyint_col", "smallint_col", "int_col", "bigint_col",
           "float_col", "double_col", "date_string_col", "string_col"},
        new PrimitiveType[]
          {PrimitiveType.INT, PrimitiveType.BOOLEAN, PrimitiveType.TINYINT, PrimitiveType.SMALLINT,
           PrimitiveType.INT, PrimitiveType.BIGINT, PrimitiveType.FLOAT,
           PrimitiveType.DOUBLE, PrimitiveType.STRING, PrimitiveType.STRING});
    checkTableCols(defaultDb, "testtbl", 0,
        new String[] {"id", "name", "zip"},
        new PrimitiveType[]
          {PrimitiveType.BIGINT, PrimitiveType.STRING, PrimitiveType.INT});
    checkTableCols(testDb, "testtbl", 0,
        new String[] {"id", "name", "birthday"},
        new PrimitiveType[]
          {PrimitiveType.BIGINT, PrimitiveType.STRING, PrimitiveType.STRING});
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

    checkHBaseTableCols(defaultDb, "hbasealltypessmall", "hbasealltypessmall",
        new String[]
          {"id", "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col"},
        new String[]
          {":key", "bools", "floats", "floats", "ints", "ints", "ints", "ints",
           "strings", "strings"},
        new String[]
          {null, "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col"},
        new PrimitiveType[]
          {PrimitiveType.INT,PrimitiveType.BOOLEAN, PrimitiveType.DOUBLE,
           PrimitiveType.FLOAT, PrimitiveType.BIGINT, PrimitiveType.INT,
           PrimitiveType.SMALLINT, PrimitiveType.TINYINT, PrimitiveType.STRING,
           PrimitiveType.STRING});

    checkHBaseTableCols(defaultDb, "hbasealltypeserror", "hbasealltypeserror",
        new String[]
          {"id", "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col"},
        new String[]
          {":key", "bools", "floats", "floats", "ints", "ints", "ints", "ints",
           "strings", "strings"},
        new String[]
          {null, "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col"},
        new PrimitiveType[]
          {PrimitiveType.INT,PrimitiveType.BOOLEAN, PrimitiveType.DOUBLE,
           PrimitiveType.FLOAT, PrimitiveType.BIGINT, PrimitiveType.INT,
           PrimitiveType.SMALLINT, PrimitiveType.TINYINT, PrimitiveType.STRING,
           PrimitiveType.STRING});

    checkHBaseTableCols(defaultDb, "hbasealltypeserrornonulls", "hbasealltypeserrornonulls",
        new String[]
          {"id", "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col"},
        new String[]
          {":key", "bools", "floats", "floats", "ints", "ints", "ints", "ints",
           "strings", "strings"},
        new String[]
          {null, "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col"},
        new PrimitiveType[]
          {PrimitiveType.INT,PrimitiveType.BOOLEAN, PrimitiveType.DOUBLE,
           PrimitiveType.FLOAT, PrimitiveType.BIGINT, PrimitiveType.INT,
           PrimitiveType.SMALLINT, PrimitiveType.TINYINT, PrimitiveType.STRING,
           PrimitiveType.STRING});

    checkHBaseTableCols(defaultDb, "hbasealltypesagg", "hbasealltypesagg",
        new String[]
          {"id", "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col"},
        new String[]
          {":key", "bools", "floats", "floats", "ints", "ints", "ints", "ints",
           "strings", "strings"},
        new String[]
          {null, "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col"},
        new PrimitiveType[]
          {PrimitiveType.INT,PrimitiveType.BOOLEAN, PrimitiveType.DOUBLE,
           PrimitiveType.FLOAT, PrimitiveType.BIGINT, PrimitiveType.INT,
           PrimitiveType.SMALLINT, PrimitiveType.TINYINT, PrimitiveType.STRING,
           PrimitiveType.STRING});

    checkHBaseTableCols(defaultDb, "hbasestringids", "hbasealltypesagg",
        new String[]
          {"id", "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col"},
        new String[]
          {":key", "bools", "floats", "floats", "ints", "ints", "ints", "ints",
           "strings", "strings"},
        new String[]
          {null, "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col"},
        new PrimitiveType[]
          {PrimitiveType.STRING,PrimitiveType.BOOLEAN, PrimitiveType.DOUBLE,
           PrimitiveType.FLOAT, PrimitiveType.BIGINT, PrimitiveType.INT,
           PrimitiveType.SMALLINT, PrimitiveType.TINYINT, PrimitiveType.STRING,
           PrimitiveType.STRING});

    checkTableCols(defaultDb, "greptiny", 0,
        new String[]
          {"field"},
        new PrimitiveType[]
          {PrimitiveType.STRING});

    checkTableCols(defaultDb, "grep1gb", 1,
        new String[]
          {"chunk", "field"},
        new PrimitiveType[]
          {PrimitiveType.INT, PrimitiveType.STRING});

    // case-insensitive lookup
    assertEquals(defaultDb.getTable("alltypes"), defaultDb.getTable("AllTypes"));
  }

  @Test public void TestPartitions() {
    HdfsTable table = (HdfsTable) catalog.getDb("default").getTable("AllTypes");
    List<HdfsTable.Partition> partitions = table.getPartitions();

    // check that partition keys cover the date range 1/1/2009-12/31/2010
    // and that we have one file per partition
    assertEquals(partitions.size(), 24);
    Set<Long> months = Sets.newHashSet();
    for (HdfsTable.Partition p: partitions) {
      assertEquals(p.keyValues.size(), 2);

      LiteralExpr key1Expr = p.keyValues.get(0);
      assertTrue(key1Expr instanceof IntLiteral);
      long key1 = ((IntLiteral) key1Expr).getValue();
      assertTrue(key1 == 2009 || key1 == 2010);

      LiteralExpr key2Expr = p.keyValues.get(1);
      assertTrue(key2Expr instanceof IntLiteral);
      long key2 = ((IntLiteral) key2Expr).getValue();
      assertTrue(key2 >= 1 && key2 <= 12);

      months.add(key1 * 100 + key2);

      assertEquals(p.filePaths.size(), 1);
    }
    assertEquals(months.size(), 24);
  }

  @Test public void TestIgnored() {
    Db defaultDb = catalog.getDb("default");
    Db testDb = catalog.getDb("testdb1");
    // check that tables with unsupported types were ignored
    for (String collectionType : Constants.CollectionTypes) {
      String tableName = TestSchemaUtils.getComplexTypeTableName(collectionType);
      assertNull(defaultDb.getTable(tableName));
      assertNull(testDb.getTable(tableName));
    }
  }
}
