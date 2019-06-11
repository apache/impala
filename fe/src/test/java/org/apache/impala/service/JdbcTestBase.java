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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.impala.analysis.CreateTableStmt;
import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.testutil.ImpalaJdbcClient;
import org.junit.After;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;

/**
 * Base class providing utility functions for tests that need a Jdbc connection.
 */
@RunWith(Parameterized.class)
public abstract class JdbcTestBase {
  protected String connectionType_;
  protected Connection con_;

  // Test-local list of test tables. These are cleaned up in @After.
  protected final List<String> testTableNames_ = Lists.newArrayList();

  public JdbcTestBase(String connectionType) { connectionType_ = connectionType; }

  @Parameterized.Parameters
  public static String[] createConnections() {
    return new String[] {"binary", "http"};
  }

  /**
   * Closes 'con_'. Any subclasses that specify their own 'After' will need to call this
   * function there.
   */
  @After
  public void cleanUp() throws Exception {
    for (String tableName : testTableNames_) {
      dropTestTable(tableName);
    }

    if (con_ != null) {
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
  }

  protected static Connection createConnection(String connStr) throws Exception {
    ImpalaJdbcClient client = ImpalaJdbcClient.createClientUsingHiveJdbcDriver(connStr);
    client.connect();
    Connection connection = client.getConnection();

    assertNotNull("Connection is null", connection);
    assertFalse("Connection should not be closed", connection.isClosed());
    Statement stmt = connection.createStatement();
    assertNotNull("Statement is null", stmt);

    return connection;
  }

  /**
   * Runs 'createTableSql', which must be a "CREATE TABLE" sql statement, and stores the
   * name of the created table in 'testTableNames_' so that it can be dropped after the
   * test case by testCleanUp().
   */
  protected void addTestTable(String createTableSql) throws Exception {
    // Parse the stmt to extract the table name. We do this first to ensure
    // that we do not execute arbitrary SQL here and pollute the test setup.
    StatementBase result = Parser.parse(createTableSql);
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
}
