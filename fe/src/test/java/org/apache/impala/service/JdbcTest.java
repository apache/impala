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

package org.apache.impala.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.impala.analysis.CreateTableStmt;
import org.apache.impala.analysis.SqlParser;
import org.apache.impala.analysis.SqlScanner;
import org.apache.impala.testutil.ImpalaJdbcClient;
import com.google.common.collect.Lists;

/**
 * JdbcTest
 *
 * Basic JDBC metadata test. It exercises getTables, getCatalogs, getSchemas,
 * getTableTypes, getColumnNames.
 *
 */
public class JdbcTest {
  private static Connection con_;

  // Test-local list of test tables. These are cleaned up in @After.
  private final List<String> testTableNames_ = Lists.newArrayList();

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

  protected void addTestTable(String createTableSql) throws Exception {
    // Parse the stmt to extract the table name. We do this first to ensure
    // that we do not execute arbitrary SQL here and pollute the test setup.
    SqlScanner input = new SqlScanner(new StringReader(createTableSql));
    SqlParser parser = new SqlParser(input);
    Object result = parser.parse().value;
    if (!(result instanceof CreateTableStmt)) {
      throw new Exception("Given stmt is not a CREATE TABLE stmt: " + createTableSql);
    }

    // Execute the stmt.
    Statement stmt = con_.createStatement();
    try {
      stmt.execute(createTableSql);
    } finally {
      stmt.close();
    }

    // Once the stmt was executed successfully, add the fully-qualified table name
    // for cleanup in @After.
    CreateTableStmt parsedStmt = (CreateTableStmt) result;
    testTableNames_.add(parsedStmt.getTblName().toString());
  }

  protected void dropTestTable(String tableName) throws SQLException {
    Statement stmt = con_.createStatement();
    try {
      stmt.execute("DROP TABLE " + tableName);
    } finally {
      stmt.close();
    }
  }

  @After
  public void testCleanUp() throws SQLException {
    for (String tableName: testTableNames_) {
      dropTestTable(tableName);
    }
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
    ResultSet rs = con_.getMetaData().getTableTypes();
    assertTrue(rs.next());
    assertEquals(rs.getString(1).toLowerCase(), "table");
    assertTrue(rs.next());
    assertEquals(rs.getString(1).toLowerCase(), "view");
    assertFalse(rs.next());
    rs.close();
  }

  @Test
  public void testMetaDataGetColumns() throws Exception {
    // It should return alltypessmall.string_col.
    ResultSet rs = con_.getMetaData().getColumns(null,
        "functional", "alltypessmall", "s%rin%");

    // validate the metadata for the getColumnNames result set
    ResultSetMetaData rsmd = rs.getMetaData();
    assertEquals("TABLE_CAT", rsmd.getColumnName(1));
    assertTrue(rs.next());
    String columnname = rs.getString("COLUMN_NAME");
    int ordinalPos = rs.getInt("ORDINAL_POSITION");
    assertEquals("Incorrect column name", "string_col", columnname);
    assertEquals("Incorrect ordinal position", 12, ordinalPos);
    assertEquals("Incorrect type", Types.VARCHAR, rs.getInt("DATA_TYPE"));
    assertFalse(rs.next());
    rs.close();

    // validate bool_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall", "bool_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.BOOLEAN, rs.getInt("DATA_TYPE"));
    assertFalse(rs.next());
    rs.close();

    // validate tinyint_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall",
        "tinyint_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.TINYINT, rs.getInt("DATA_TYPE"));
    assertEquals(3, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
    assertFalse(rs.next());
    rs.close();

    // validate smallint_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall",
        "smallint_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.SMALLINT, rs.getInt("DATA_TYPE"));
    assertEquals(5, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
    assertFalse(rs.next());
    rs.close();

    // validate int_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall", "int_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.INTEGER, rs.getInt("DATA_TYPE"));
    assertEquals(10, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
    assertFalse(rs.next());
    rs.close();

    // validate bigint_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall",
        "bigint_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.BIGINT, rs.getInt("DATA_TYPE"));
    assertEquals(19, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
    assertFalse(rs.next());
    rs.close();

    // validate float_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall", "float_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.FLOAT, rs.getInt("DATA_TYPE"));
    assertEquals(7, rs.getInt("COLUMN_SIZE"));
    assertEquals(7, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
    assertFalse(rs.next());
    rs.close();

    // validate double_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall",
        "double_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.DOUBLE, rs.getInt("DATA_TYPE"));
    assertEquals(15, rs.getInt("COLUMN_SIZE"));
    assertEquals(15, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
    assertFalse(rs.next());
    rs.close();

    // validate timestamp_col
    rs = con_.getMetaData().getColumns(null, "functional", "alltypessmall",
        "timestamp_col");
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.TIMESTAMP, rs.getInt("DATA_TYPE"));
    assertEquals(29, rs.getInt("COLUMN_SIZE"));
    assertEquals(9, rs.getInt("DECIMAL_DIGITS"));
    // Use getString() to check the value is null (and not 0).
    assertEquals(null, rs.getString("NUM_PREC_RADIX"));
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

    // validate DECIMAL columns
    rs = con_.getMetaData().getColumns(null, "functional", "decimal_tbl", null);
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.DECIMAL, rs.getInt("DATA_TYPE"));
    assertEquals(9, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.DECIMAL, rs.getInt("DATA_TYPE"));
    assertEquals(10, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.DECIMAL, rs.getInt("DATA_TYPE"));
    assertEquals(20, rs.getInt("COLUMN_SIZE"));
    assertEquals(10, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.DECIMAL, rs.getInt("DATA_TYPE"));
    assertEquals(38, rs.getInt("COLUMN_SIZE"));
    assertEquals(38, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.DECIMAL, rs.getInt("DATA_TYPE"));
    assertEquals(10, rs.getInt("COLUMN_SIZE"));
    assertEquals(5, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.DECIMAL, rs.getInt("DATA_TYPE"));
    assertEquals(9, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
    assertFalse(rs.next());
    rs.close();

    // validate CHAR/VARCHAR columns
    rs = con_.getMetaData().getColumns(null, "functional", "chars_tiny", null);
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.CHAR, rs.getInt("DATA_TYPE"));
    assertEquals(5, rs.getInt("COLUMN_SIZE"));
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.CHAR, rs.getInt("DATA_TYPE"));
    assertEquals(140, rs.getInt("COLUMN_SIZE"));
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.VARCHAR, rs.getInt("DATA_TYPE"));
    assertEquals(32, rs.getInt("COLUMN_SIZE"));
    assertFalse(rs.next());
    rs.close();

    // Validate complex types STRUCT/MAP/ARRAY.
    // To be consistent with Hive's behavior, the TYPE_NAME field is populated
    // with the primitive type name for scalar types, and with the full toSql()
    // for complex types. The resulting type names are somewhat inconsistent,
    // because nested types are printed differently than top-level types, e.g.:
    // toSql()                     TYPE_NAME
    // DECIMAL(10,10)         -->  DECIMAL
    // CHAR(10)               -->  CHAR
    // VARCHAR(10)            -->  VARCHAR
    // ARRAY<DECIMAL(10,10)>  -->  ARRAY<DECIMAL(10,10)>
    // ARRAY<CHAR(10)>        -->  ARRAY<CHAR(10)>
    // ARRAY<VARCHAR(10)>     -->  ARRAY<VARCHAR(10)>
    addTestTable("create table default.jdbc_complex_type_test (" +
        "s struct<f1:int,f2:char(4),f3:varchar(5),f4:decimal(10,10)>," +
        "a1 array<int>," +
        "a2 array<char(4)>," +
        "a3 array<varchar(5)>," +
        "a4 array<decimal(10,10)>," +
        "m1 map<int,string>," +
        "m2 map<string,char(4)>," +
        "m3 map<bigint,varchar(5)>," +
        "m4 map<boolean,decimal(10,10)>)");
    rs = con_.getMetaData().getColumns(null, "default", "jdbc_complex_type_test", null);
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.STRUCT, rs.getInt("DATA_TYPE"));
    assertEquals("Incorrect type name",
        "STRUCT<f1:INT,f2:CHAR(4),f3:VARCHAR(5),f4:DECIMAL(10,10)>",
        rs.getString("TYPE_NAME"));
    assertEquals(0, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(0, rs.getInt("NUM_PREC_RADIX"));
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.ARRAY, rs.getInt("DATA_TYPE"));
    assertEquals("Incorrect type name", "ARRAY<INT>", rs.getString("TYPE_NAME"));
    assertEquals(0, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(0, rs.getInt("NUM_PREC_RADIX"));
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.ARRAY, rs.getInt("DATA_TYPE"));
    assertEquals("Incorrect type name", "ARRAY<CHAR(4)>", rs.getString("TYPE_NAME"));
    assertEquals(0, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(0, rs.getInt("NUM_PREC_RADIX"));
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.ARRAY, rs.getInt("DATA_TYPE"));
    assertEquals("Incorrect type name", "ARRAY<VARCHAR(5)>", rs.getString("TYPE_NAME"));
    assertEquals(0, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(0, rs.getInt("NUM_PREC_RADIX"));
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.ARRAY, rs.getInt("DATA_TYPE"));
    assertEquals("Incorrect type name", "ARRAY<DECIMAL(10,10)>",
        rs.getString("TYPE_NAME"));
    assertEquals(0, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(0, rs.getInt("NUM_PREC_RADIX"));
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.ARRAY, rs.getInt("DATA_TYPE"));
    assertEquals("Incorrect type name", "MAP<INT,STRING>", rs.getString("TYPE_NAME"));
    assertEquals(0, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(0, rs.getInt("NUM_PREC_RADIX"));
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.ARRAY, rs.getInt("DATA_TYPE"));
    assertEquals("Incorrect type name", "MAP<STRING,CHAR(4)>",
        rs.getString("TYPE_NAME"));
    assertEquals(0, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(0, rs.getInt("NUM_PREC_RADIX"));
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.ARRAY, rs.getInt("DATA_TYPE"));
    assertEquals("Incorrect type name", "MAP<BIGINT,VARCHAR(5)>",
        rs.getString("TYPE_NAME"));
    assertEquals(0, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(0, rs.getInt("NUM_PREC_RADIX"));
    assertTrue(rs.next());
    assertEquals("Incorrect type", Types.ARRAY, rs.getInt("DATA_TYPE"));
    assertEquals("Incorrect type name", "MAP<BOOLEAN,DECIMAL(10,10)>",
        rs.getString("TYPE_NAME"));
    assertEquals(0, rs.getInt("COLUMN_SIZE"));
    assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    assertEquals(0, rs.getInt("NUM_PREC_RADIX"));
    rs.close();
  }

  @Test
  public void testMetaDataGetColumnComments() throws Exception {
    addTestTable("create table default.jdbc_column_comments_test (" +
         "a int comment 'column comment') comment 'table comment'");

    // If a table is not yet loaded before getTables(), then the 'remarks' field
    // is left empty. getColumns() loads the table metadata, so later getTables()
    // calls will return 'remarks' correctly.
    ResultSet rs = con_.getMetaData().getTables(
        null, "default", "jdbc_column_comments_test", null);
    assertTrue(rs.next());
    assertEquals("Incorrect table name", "jdbc_column_comments_test",
        rs.getString("TABLE_NAME"));
    assertEquals("Incorrect table comment", "", rs.getString("REMARKS"));

    rs = con_.getMetaData().getColumns(
        null, "default", "jdbc_column_comments_test", null);
    assertTrue(rs.next());
    assertEquals("Incorrect column comment", "column comment", rs.getString("REMARKS"));

    rs = con_.getMetaData().getTables(
        null, "default", "jdbc_column_comments_test", null);
    assertTrue(rs.next());
    assertEquals("Incorrect table name", "jdbc_column_comments_test",
        rs.getString("TABLE_NAME"));
    assertEquals("Incorrect table comment", "table comment", rs.getString("REMARKS"));
  }

  @Test
  public void testDecimalGetColumnTypes() throws SQLException {
    // Table has 5 decimal columns
    ResultSet rs = con_.createStatement().executeQuery(
        "select * from functional.decimal_tbl");

    assertEquals(rs.getMetaData().getColumnType(1), Types.DECIMAL);
    assertEquals(rs.getMetaData().getPrecision(1), 9);
    assertEquals(rs.getMetaData().getScale(1), 0);

    assertEquals(rs.getMetaData().getColumnType(2), Types.DECIMAL);
    assertEquals(rs.getMetaData().getPrecision(2), 10);
    assertEquals(rs.getMetaData().getScale(2), 0);

    assertEquals(rs.getMetaData().getColumnType(3), Types.DECIMAL);
    assertEquals(rs.getMetaData().getPrecision(3), 20);
    assertEquals(rs.getMetaData().getScale(3), 10);

    assertEquals(rs.getMetaData().getColumnType(4), Types.DECIMAL);
    assertEquals(rs.getMetaData().getPrecision(4), 38);
    assertEquals(rs.getMetaData().getScale(4), 38);

    assertEquals(rs.getMetaData().getColumnType(5), Types.DECIMAL);
    assertEquals(rs.getMetaData().getPrecision(5), 10);
    assertEquals(rs.getMetaData().getScale(5), 5);

    assertEquals(rs.getMetaData().getColumnType(6), Types.DECIMAL);
    assertEquals(rs.getMetaData().getPrecision(6), 9);
    assertEquals(rs.getMetaData().getScale(6), 0);

    rs.close();
  }

  /**
   * Validate the Metadata for the result set of a metadata getColumnNames call.
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
    // We support 2 overloaded version of it.
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
    assertEquals(numFound, 2);
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
  public void testSelectNull() throws SQLException {
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
