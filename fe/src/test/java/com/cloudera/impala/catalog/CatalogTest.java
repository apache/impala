// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.serde.Constants;
import org.junit.BeforeClass;
import org.junit.Test;

public class CatalogTest {
  private static Catalog catalog;

  @BeforeClass public static void setUp() throws Exception {
    HiveMetaStoreClient client = TestSchemaUtils.createSchemaAndClient();
    catalog = new Catalog(client);
  }

  private void checkTable(Db db, String tblName, String[] colNames,
                          PrimitiveType[] colTypes) {
    Table tbl = db.getTable(tblName);
    assertEquals(tbl.getName(), tblName);
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

  @Test public void TestLoad() {
    Collection<Db> dbs = catalog.getDbs();
    Db defaultDb = null;
    Db testDb = null;
    for (Db db: dbs) {
      System.err.println("dbname=" + db.getName());
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

    // both dbs contain tables alltypes and testtbl
    assertNotNull(defaultDb.getTable("alltypes"));
    assertNotNull(defaultDb.getTable("testtbl"));
    assertNotNull(testDb.getTable("alltypes"));
    assertNotNull(testDb.getTable("testtbl"));

    checkTable(defaultDb, "alltypes",
        new String[]
          {"year", "month", "id", "bool_col", "tinyint_col", "smallint_col",
           "int_col", "bigint_col", "float_col", "double_col", "date_string_col",
           "string_col"},
        new PrimitiveType[]
          {PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT,
           PrimitiveType.BOOLEAN, PrimitiveType.TINYINT, PrimitiveType.SMALLINT,
           PrimitiveType.INT, PrimitiveType.BIGINT, PrimitiveType.FLOAT,
           PrimitiveType.DOUBLE, PrimitiveType.STRING, PrimitiveType.STRING});
    checkTable(testDb, "alltypes",
        new String[]
          {"id", "bool_col", "tinyint_col", "smallint_col", "int_col", "bigint_col",
           "float_col", "double_col", "date_string_col", "string_col"},
        new PrimitiveType[]
          {PrimitiveType.INT, PrimitiveType.BOOLEAN, PrimitiveType.TINYINT, PrimitiveType.SMALLINT,
           PrimitiveType.INT, PrimitiveType.BIGINT, PrimitiveType.FLOAT,
           PrimitiveType.DOUBLE, PrimitiveType.STRING, PrimitiveType.STRING});
    checkTable(defaultDb, "testtbl",
        new String[] {"id", "name", "zip"},
        new PrimitiveType[]
          {PrimitiveType.BIGINT, PrimitiveType.STRING, PrimitiveType.INT});
    checkTable(testDb, "testtbl",
        new String[] {"id", "name", "birthday"},
        new PrimitiveType[]
          {PrimitiveType.BIGINT, PrimitiveType.STRING, PrimitiveType.STRING});

    // case-insensitive lookup
    assertEquals(defaultDb.getTable("alltypes"), defaultDb.getTable("AllTypes"));

    // check that tables with unsupported types were ignored
    for (String collectionType : Constants.CollectionTypes) {
      String tableName = TestSchemaUtils.getComplexTypeTableName(collectionType);
      assertNull(defaultDb.getTable(tableName));
      assertNull(testDb.getTable(tableName));
    }
  }
}
