package com.cloudera.impala.jdbc;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.hsqldb.lib.HashSet;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.TestSchemaUtils;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.service.Coordinator;
import com.cloudera.impala.testutil.TestUtils;

public class JdbcDriverTest {

  // For checking metadata retrieved with JDBC driver.
  private static Catalog catalog;

  // For comparing results retrieved via JDBC.
  private static Coordinator coordinator;

  @BeforeClass
  public static void setUp() throws Exception {
    Class.forName("com.cloudera.impala.jdbc.ImpalaDriver");
    HiveMetaStoreClient client = TestSchemaUtils.createClient();
    catalog = new Catalog(client);
    coordinator = new Coordinator(catalog);
  }

  // Expected success when connecting to connSting.
  // Test fails if no registered driver matches connString,
  // or if connString contains an unknown database.
  // returns connection.
  private Connection connectSuccess(String connString, String user, String pass) {
    try {
      if (DriverManager.getDriver(connString) == null) {
        fail("No matching driver found for: " + connString +
            " with user: " + user + " and pass: " + pass);
      }
    } catch (SQLException e) {
      fail("No matching driver found for: " + connString +
          " with user: " + user + " and pass: " + pass);
    }
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(connString, user, pass);
    } catch (SQLException e) {
      fail("Unexpected exception when connecting to: " + connString +
          " with user: " + user + " and pass: " + pass + "\n" + e.getMessage());
    }
    return conn;
  }

  // Expected failure when connecting to connSting.
  // Test Connection if the connection attempt succeeds.
  private void connectFailure(String connString, String user, String pass) {
    try {
      if (DriverManager.getDriver(connString) == null) {
        return;
      }
    } catch (SQLException e) {
      return;
    }
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(connString, user, pass);
    } catch (SQLException e) {
      return;
    }
    System.out.println(conn);
    fail("Unexpected success when connecting to: " + connString +
        " with user: " + user + " and pass: " + pass);
  }

  private void checkColumnMetaData(DatabaseMetaData metaData, String dbName, String tableName)
      throws SQLException {
    ResultSet rs =
      metaData.getColumns(metaData.getConnection().getCatalog(), null, tableName, null);
    ResultSetMetaData rsMetaRs = rs.getMetaData();
    // We expect 3 columns with labels: "TABLE_NAME", "COLUMN_NAME", and "COLUMN_TYPE"
    Assert.assertEquals(rsMetaRs.getColumnCount(), 3);
    Assert.assertEquals(rsMetaRs.getColumnLabel(1), "TABLE_NAME");
    Assert.assertEquals(rsMetaRs.getColumnLabel(2), "COLUMN_NAME");
    Assert.assertEquals(rsMetaRs.getColumnLabel(3), "COLUMN_TYPE");
    // Check that columns retrieved from JDBC driver exist in catalog.
    // Create HashSet of columns to check that all columns in catalog were returned.
    Db db = catalog.getDb(dbName);
    Table table = db.getTable(tableName);
    HashSet jdbcColumns = new HashSet();
    while (rs.next()) {
      // Check table name.
      String currTableName = rs.getString(1);
      if (!currTableName.equals(tableName)) {
        fail("Table names didn't match, expected: " + tableName + " actual: " + currTableName);
      }
      // Check column name.
      String columnName = rs.getString(2);
      Column col = table.getColumn(columnName);
      if (col == null) {
        fail("Column '" + columnName + "' doesn't exist in catalog");
      }
      // Check column type.
      String columnType = rs.getString(3);
      PrimitiveType type = PrimitiveType.valueOf(columnType);
      if (col.getType() != type) {
        fail("Column '" + columnName + "' has wrong type. Expected: " +
            col.getType() + ", actual: " + type);
      }
      jdbcColumns.add(columnName);
    }
    // Check if all columns in table were returned from JDBC.
    ArrayList<Column> columns = table.getColumns();
    for (Column c: columns) {
      if (!jdbcColumns.contains(c.getName())) {
        fail("Column '" + c.getName() + "' exists in catalog but was not returned from JDBC.");
      }
    }
  }

  private void checkTableMetaData(String connString, String user, String pass)
      throws SQLException {
    Connection conn = connectSuccess(connString, "u", "p");
    try {
      DatabaseMetaData metaData = conn.getMetaData();
      ResultSet rs = metaData.getTables(conn.getCatalog(), null, "default", null);
      ResultSetMetaData rsMetaRs = rs.getMetaData();
      // We expect a single column with label "TABLE_NAME".
      Assert.assertEquals(rsMetaRs.getColumnCount(), 1);
      Assert.assertEquals(rsMetaRs.getColumnLabel(1), "TABLE_NAME");
      // Find all tables and verify they exist in catalog.
      // Create HashSet of tables from JDBC driver to verify all tables in catalog were returned.
      HashSet jdbcTables = new HashSet();
      while (rs.next()) {
        String tableName = rs.getString(1);
        if (catalog.getDb("default").getTable(tableName) == null) {
          fail("Found table '" + tableName + "' from JDBC but table doesn't exist in catalog.");
        }
        jdbcTables.add(rs.getString(1));
      }
      // Check if all tables in catalog were returned from JDBC.
      String[] splits = connString.split(":");
      String dbName = splits[splits.length - 1];
      Db db = catalog.getDb(dbName);
      Map<String, Table> catalogTables = db.getTables();
      for (Map.Entry<String, Table> entry : catalogTables.entrySet()) {
        if (!jdbcTables.contains(entry.getKey())) {
          fail("Found table '" + entry.getKey() + "' in catalog but not in JDBC metadata.");
        }
        checkColumnMetaData(metaData, dbName, entry.getKey());
      }
    } finally {
      conn.close();
    }
  }

  private void checkQueryResults(Connection conn, String query)
      throws SQLException, ImpalaException {
    // Execute the query through the JDBC driver.
    Statement stmt = conn.createStatement();
    stmt.execute(query);
    ResultSet rs = stmt.getResultSet();
    ResultSetMetaData rsMetaRs = rs.getMetaData();
    // Fill expected column labels.
    ArrayList<String> expectedColLabels = new ArrayList<String>();
    for (int i = 1; i <= rsMetaRs.getColumnCount(); ++i) {
      expectedColLabels.add(rsMetaRs.getColumnLabel(i));
    }
    // Fill expected results.
    ArrayList<String> expectedResults = new ArrayList<String>();
    while (rs.next()) {
      StringBuilder rowBuilder = new StringBuilder();
      for (int i = 1; i <= rsMetaRs.getColumnCount(); ++i) {
        if (i > 1) {
          rowBuilder.append(',');
        }
        rowBuilder.append(rs.getString(i));
      }
      expectedResults.add(rowBuilder.toString());
    }
    // Execute query via the coordinator and compare column labels, and query results.
    StringBuilder errorLog = new StringBuilder();
    TestUtils.runQuery(
        coordinator, query, 0,
        false, 1000, expectedColLabels, null, expectedResults, null, null, errorLog);
  }

  private void queryFailure(Connection conn, String query) throws ImpalaException {
    try {
      checkQueryResults(conn, query);
    } catch (SQLException e) {
      return;
    }
    fail("Unexpected success when executing query: " + query);
  }

  private void querySuccess(Connection conn, String query) throws ImpalaException {
    try {
      checkQueryResults(conn, query);
    } catch (SQLException e) {
      fail("Unexpected failure when executing query: " + query);
    }
  }

  @Test
  public void TestConnect() throws SQLException {
    // The connection string is accepted if it contains "impala".
    // The field after "impala" names the database.
    // For example, in 'jdbc:impala:mydb' the database would be 'mydb'
    // or, in 'abc:impala:mydb:xyz', the database would be 'mydb'
    // or, in 'abc:impala:impala:xyz', the database would be 'impala'
    // Connect to specific database.
    Connection conn = null;
    conn = connectSuccess("impala:default", "u", "p");
    conn.close();
    conn = connectSuccess("impala:testdb1", "u", "p");
    conn.close();
    // Additional text in the connection string.
    conn = connectSuccess("jdbc:impala:testdb1", "u", "p");
    conn.close();
    conn = connectSuccess("db://jdbc:impala:testdb1", "u", "p");
    conn.close();
    // Different user/pass (currently any are accepted).
    conn = connectSuccess("jdbc:impala:testdb1", "abc", "xyz");
    conn.close();

    // No database specified.
    connectFailure("impala", "u", "p");
    // String does not contain "impala".
    connectFailure("ipala", "u", "p");
    // Unknown database.
    connectFailure("impala:baddb", "u", "p");
    // Database at wrong position.
    connectFailure("impala:testdb1:baddb", "u", "p");
  }

  @Test
  public void TestDatabaseMetaData() throws SQLException {
    checkTableMetaData("impala:default", "u", "p");
    checkTableMetaData("impala:testdb1", "u", "p");
  }

  @Test
  public void TestImpalaStatement() throws SQLException, ImpalaException {
    Connection conn = connectSuccess("impala:default", "u", "p");
    try {
      querySuccess(conn, "select * from alltypessmall");
      querySuccess(conn, "select id, int_col from alltypessmall");
      querySuccess(conn, "select count(*) from alltypessmall");
      querySuccess(conn, "select float_col, string_col from alltypessmall where id > 2");

      // Syntax error.
      queryFailure(conn, "selct * from alltypessmall");
      // Unknown database.
      queryFailure(conn, "select * from baddb.alltypessmall");
      // Unknown table.
      queryFailure(conn, "select * from badtable");
      // Unknown column.
      queryFailure(conn, "select bad_col from alltypessmall");
    } finally {
      conn.close();
    }
  }
}
