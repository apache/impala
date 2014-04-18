// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.testutil.ImpalaJdbcClient;

/**
 * JdbcTest
 *
 * Basic JDBC metadata test. It exercises getTables, getCatalogs, getSchemas,
 * getTableTypes, getColumns.
 *
 */
public class JdbcTest {
  private static Connection con_;

  @BeforeClass
  public static void setUp() throws Exception {
    ImpalaJdbcClient client = ImpalaJdbcClient.createClientUsingHiveJdbcDriver();
    client.connect();
    con_ = client.getConnection();
    assertNotNull("Connection is null", con_);
    assertFalse("Connection should not be closed", con_.isClosed());
    Statement stmt = con_.createStatement();
    assertNotNull("Statement is null", stmt);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    con_.close();
    assertTrue("Connection should be closed", con_.isClosed());

    Exception expectedException = null;
    try {
      con_.createStatement();
    } catch (Exception e) {
      expectedException = e;
    }

    assertNotNull("createStatement() on closed connection should throw exception",
        expectedException);
  }

  @Test
  public void testMetaDataGetTables() throws SQLException {
    // map from tablename search pattern to actual table name.
    Map<String, String> tests = new HashMap<String, String>();
    tests.put("alltypes", "alltypes");
    tests.put("%all_ypes", "alltypes");

    String[][] tblTypes = {null, {"TABLE"}};

    for (String tblNamePattern: tests.keySet()) {
      for (String[] tblType: tblTypes) {
        ResultSet rs = con_.getMetaData().getTables("", "functional",
            tblNamePattern, tblType);
        assertTrue(rs.next());

        // TABLE_NAME is the 3rd column.
        String resultTableName = rs.getString("TABLE_NAME");
        assertEquals(rs.getString(3), resultTableName);

        assertEquals("Table mismatch", tests.get(tblNamePattern), resultTableName);
        String tableType = rs.getString("TABLE_TYPE");
        assertEquals("table", tableType.toLowerCase());
        assertFalse(rs.next());
        rs.close();
      }
    }

    for (String[] tblType: tblTypes) {
      ResultSet rs = con_.getMetaData().getTables(null, null, null, tblType);
      // Should return at least one value.
      assertTrue(rs.next());
      rs.close();

      rs = con_.getMetaData().getTables(null, null, null, tblType);
      assertTrue(rs.next());
      rs.close();
    }
  }

  @Test
  public void testMetaDataGetCatalogs() throws SQLException {
    // Hive/Impala does not have catalogs.
    ResultSet rs = con_.getMetaData().getCatalogs();
    ResultSetMetaData resMeta = rs.getMetaData();
    assertEquals(1, resMeta.getColumnCount());
    assertEquals("TABLE_CAT", resMeta.getColumnName(1));
    assertFalse(rs.next());
  }

  @Test
  public void testMetaDataGetSchemas() throws SQLException {
    // There is only one schema: "default".
    ResultSet rs = con_.getMetaData().getSchemas("", "d_f%");
    ResultSetMetaData resMeta = rs.getMetaData();
    assertEquals(2, resMeta.getColumnCount());
    assertEquals("TABLE_SCHEM", resMeta.getColumnName(1));
    assertEquals("TABLE_CATALOG", resMeta.getColumnName(2));
    assertTrue(rs.next());
    assertEquals(rs.getString(1).toLowerCase(), "default");
    assertFalse(rs.next());
    rs.close();
  }

  @Test
  public void testMetaDataGetTableTypes() throws SQLException {
    // There is only one table type: "table".
    ResultSet rs = con_.getMetaData().getTableTypes();
    assertTrue(rs.next());
    assertEquals(rs.getString(1).toLowerCase(), "table");
    assertFalse(rs.next());
    rs.close();
  }

  @Test
  public void testMetaDataGetColumns() throws SQLException {
    // It should return alltypessmall.string_col.
    ResultSet rs = con_.getMetaData().getColumns(null,
        "functional", "alltypessmall", "s%rin%");

    // validate the metadata for the getColumns result set
    ResultSetMetaData rsmd = rs.getMetaData();
    assertEquals("TABLE_CAT", rsmd.getColumnName(1));
    assertTrue(rs.next());
    String columnname = rs.getString("COLUMN_NAME");
    int ordinalPos = rs.getInt("ORDINAL_POSITION");
    assertEquals("Incorrect column name", "string_col", columnname);
    assertEquals("Incorrect ordinal position", 12, ordinalPos);
    assertEquals("Incorrect type", java.sql.Types.VARCHAR, rs.getInt("DATA_TYPE"));
    assertFalse(rs.next());
    rs.close();

    // validate bool_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall", "bool_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", java.sql.Types.BOOLEAN, rs.getInt("DATA_TYPE"));
    assertFalse(rs.next());
    rs.close();

    // validate tinyint_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall",
        "tinyint_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", java.sql.Types.TINYINT, rs.getInt("DATA_TYPE"));
    assertFalse(rs.next());
    rs.close();

    // validate smallint_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall",
        "smallint_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", java.sql.Types.SMALLINT, rs.getInt("DATA_TYPE"));
    assertFalse(rs.next());
    rs.close();

    // validate int_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall", "int_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", java.sql.Types.INTEGER, rs.getInt("DATA_TYPE"));
    assertFalse(rs.next());
    rs.close();

    // validate bigint_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall",
        "bigint_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", java.sql.Types.BIGINT, rs.getInt("DATA_TYPE"));
    assertFalse(rs.next());
    rs.close();

    // validate float_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall", "float_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", java.sql.Types.FLOAT, rs.getInt("DATA_TYPE"));
    assertFalse(rs.next());
    rs.close();

    // validate double_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall",
        "double_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", java.sql.Types.DOUBLE, rs.getInt("DATA_TYPE"));
    assertFalse(rs.next());
    rs.close();

    // validate timestamp_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall",
        "timestamp_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", java.sql.Types.TIMESTAMP, rs.getInt("DATA_TYPE"));
    assertFalse(rs.next());
    rs.close();

    // validate null column name returns all columns.
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall", null);
    int numCols = 0;
    while (rs.next()) {
      ++numCols;
    }
    assertEquals(13, numCols);
    rs.close();
  }

  /**
   * Validate the Metadata for the result set of a metadata getColumns call.
   */
  @Test
  public void testMetaDataGetColumnsMetaData() throws SQLException {
    ResultSet rs = con_.getMetaData().getColumns(null, "functional", "alltypes", null);

    ResultSetMetaData rsmd = rs.getMetaData();
    assertEquals("TABLE_CAT", rsmd.getColumnName(1));
    assertEquals(Types.VARCHAR, rsmd.getColumnType(1));
    assertEquals(Integer.MAX_VALUE, rsmd.getColumnDisplaySize(1));
    assertEquals("ORDINAL_POSITION", rsmd.getColumnName(17));
    assertEquals(Types.INTEGER, rsmd.getColumnType(17));
    assertEquals(11, rsmd.getColumnDisplaySize(17));
  }

  @Test
  public void testMetaDataGetFunctions() throws SQLException {
    // Look up the 'substring' function.
    // We support 4 overloaded version of it.
    ResultSet rs = con_.getMetaData().getFunctions(
        null, null, "substring");
    int numFound = 0;
    while (rs.next()) {
      String funcName = rs.getString("FUNCTION_NAME");
      assertEquals("Incorrect function name", "substring", funcName.toLowerCase());
      String dbName = rs.getString("FUNCTION_SCHEM");
      assertEquals("Incorrect function name", "_impala_builtins", dbName.toLowerCase());
      String fnSignature = rs.getString("SPECIFIC_NAME");
      assertTrue(fnSignature.startsWith("substring("));
      ++numFound;
    }
    assertEquals(numFound, 4);
    rs.close();

    // substring is not in default db
    rs = con_.getMetaData().getFunctions(null, "default", "substring");
    assertFalse(rs.next());
    rs.close();
  }

  @Test
  public void testUtilityFunctions() throws SQLException {
    ResultSet rs = con_.createStatement().executeQuery("select user()");
    try {
      // We expect exactly one result row with a NULL inside the first column.
      // The user() function returns NULL because we currently cannot set the user
      // when establishing the Jdbc connection.
      assertTrue(rs.next());
      assertNull(rs.getString(1));
      assertFalse(rs.next());
    } finally {
      rs.close();
    }
  }

  @Test
  public void testRegressions() throws SQLException {
    // Regression test for IMPALA-914.
    ResultSet rs = con_.createStatement().executeQuery("select NULL");
    // Expect the column to be of type BOOLEAN to be compatible with Hive.
    assertEquals(rs.getMetaData().getColumnType(1), Types.BOOLEAN);
    try {
      // We expect exactly one result row with a NULL inside the first column.
      assertTrue(rs.next());
      assertNull(rs.getString(1));
      assertFalse(rs.next());
    } finally {
      rs.close();
    }
  }
}
