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

package org.apache.impala.testutil;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Basic tool for executing queries and displaying results using Impala
 * over JDBC.
 */
public class ImpalaJdbcClient {
  private static final Logger LOG = Logger.getLogger(ImpalaJdbcClient.class);

  // Hive uses simple SASL by default. The auth configuration 'none' (both for the client
  // and the server) correspond to using simple SASL.
  private final static String SASL_AUTH_SPEC = ";auth=none";

  // As of Hive 0.11 'noSasl' is case sensitive. See HIVE-4232 for more details.
  private final static String NOSASL_AUTH_SPEC = ";auth=noSasl";

  private final static String LDAP_AUTH_SPEC = ";user=%s;password=%s";

  // Connects with HTTP as the transport.
  private final static String HTTP_TRANSPORT_SPEC = ";transportMode=http";

  // HiveServer2 compatible ports on coordinator for BINARY and HTTP based transports.
  private final static int HS2_BINARY_PORT = 21050;
  private final static int HS2_HTTP_PORT = 28000;

  private final String driverName_;
  private final String connString_;
  private Connection conn_;
  private Statement stmt_;

  private ImpalaJdbcClient(String driverName, String connString) {
    this.driverName_ = driverName;
    this.connString_ = connString;
  }

  private void validateConnection() throws SQLException {
    if (conn_ == null) {
      throw new RuntimeException("Connection not initialized.");
    } else if (conn_.isClosed()) {
      throw new RuntimeException("Connection not open.");
    }
    Preconditions.checkNotNull(stmt_);

    // Re-open if the statement if it has been closed.
    if (stmt_.isClosed()) {
      stmt_ = conn_.createStatement();
    }
  }

  public void connect() throws ClassNotFoundException, SQLException,
      ImpalaRuntimeException {
    LOG.info("Using JDBC Driver Name: " + driverName_);
    LOG.info("Connecting to: " + connString_);

    // Make sure the driver can be found, throws a ClassNotFoundException if
    // it is not available.
    Class.forName(driverName_);
    Driver d = DriverManager.getDriver(connString_);
    String driverClassName = d.getClass().getName();
    if (!driverClassName.equals(driverName_)) {
      throw new ImpalaRuntimeException(String.format(
          "Specified driver is %s, but for the given connection string %s the "+
          "selected driver is %s. You might want to specify a different connection " +
          "string with option -c.",
          driverName_, connString_, driverClassName));
    }
    conn_ = DriverManager.getConnection(connString_);
    stmt_ = conn_.createStatement();
  }

  /*
   * Closes the internal Statement and Connection objects. If they are already closed
   * this is a no-op.
   */
  public void close() throws SQLException {
    if (stmt_ != null) {
      stmt_.close();
    }

    if (conn_ != null) {
      conn_.close();
    }
  }

  /*
   * Executes the given query and returns the ResultSet. Will re-open the Statement
   * if needed.
   */
  public ResultSet execQuery(String query) throws SQLException {
    validateConnection();
    LOG.info("Executing: " + query);
    return stmt_.executeQuery(query);
  }

  public void execStatement(String query) throws SQLException {
    validateConnection();
    LOG.info("Executing statement: " + query);
    stmt_.execute(query);
  }

  public void changeDatabase(String db_name) throws SQLException {
    validateConnection();
    LOG.info("Using: " + db_name);
    stmt_.execute("use " + db_name);
  }

  public Connection getConnection() {
    return conn_;
  }

  public Statement getStatement() {
    return stmt_;
  }

  public static ImpalaJdbcClient createClient(String driver, String connString) {
    return new ImpalaJdbcClient(driver, connString);
  }

  public static ImpalaJdbcClient createClientUsingHiveJdbcDriver() {
    return createClient(TestUtils.HIVE_SERVER2_DRIVER_NAME,
        getNoAuthConnectionStr("binary"));
  }

  public static ImpalaJdbcClient createClientUsingHiveJdbcDriver(String connString) {
    return createClient(TestUtils.HIVE_SERVER2_DRIVER_NAME, connString);
  }

  public static ImpalaJdbcClient createHttpClientUsingHiveJdbcDriver() {
    return createClient(TestUtils.HIVE_SERVER2_DRIVER_NAME,
        getNoAuthConnectionStr("http"));
  }

  public static String getNoAuthConnectionStr(String connType) {
    return getConnectionStr(connType, NOSASL_AUTH_SPEC);
  }

  public static String getLdapConnectionStr(
      String connType, String username, String password) {
    return getConnectionStr(connType, String.format(LDAP_AUTH_SPEC, username, password));
  }

  private static String getConnectionStr(String connType, String authStr) {
    String connString = TestUtils.HS2_CONNECTION_TEMPLATE + authStr;
    if (connType == "binary") {
      return String.format(connString, HS2_BINARY_PORT, "default");
    } else {
      Preconditions.checkState(connType == "http");
      return String.format(connString + HTTP_TRANSPORT_SPEC, HS2_HTTP_PORT, "default");
    }
  }

  /**
   * Used to store the execution options passed via command line
   */
  private static class ClientExecOptions {
    private final String connStr;
    private final String query;
    private final String driver;

    public ClientExecOptions(String connStr, String query, String driver) {
      this.connStr = connStr;
      this.query = query;
      this.driver = driver;
    }

    public String getQuery() {
      return query;
    }

    public String getConnStr() {
      return connStr;
    }

    public String getDriver() {
      return driver;
    }
  }

  /**
   * Parses command line options
   */
  private static ClientExecOptions parseOptions(String [] args) throws ParseException {
    Options options = new Options();
    options.addOption("i", true, "host:port of target machine impalad is listening on");
    options.addOption("c", true,
        "Full connection string to use. Overrides host/port value");
    options.addOption("t", true, "SASL/NOSASL, whether to use SASL transport or not");
    options.addOption("q", true, "Query String");
    options.addOption("d", true, "Driver name, default: org.apache.hive.jdbc.HiveDriver");
    options.addOption("help", false, "Help");

    BasicParser optionParser = new BasicParser();
    CommandLine cmdArgs = optionParser.parse(options, args);

    String transportOption = cmdArgs.getOptionValue("t");
    if (transportOption == null) {
      LOG.error("Must specify '-t' option, whether to use SASL transport or not.");
      LOG.error("Using the wrong type of transport will cause the program to hang.");
      LOG.error("Usage: " + options.toString());
      System.exit(1);
    }
    if (!transportOption.equalsIgnoreCase("SASL") &&
        !transportOption.equalsIgnoreCase("NOSASL")) {
      LOG.error("Invalid argument " + transportOption + " to '-t' option.");
      LOG.error("Usage: " + options.toString());
      System.exit(1);
    }
    boolean useSasl = transportOption.equalsIgnoreCase("SASL");

    String connStr = cmdArgs.getOptionValue("c", null);

    // If the user didn't specify a custom connection string, build a connection
    // string using HiveServer 2 JDBC driver and no security.
    if (connStr == null) {
      String hostPort = cmdArgs.getOptionValue("i", "localhost:21050");
      connStr = "jdbc:hive2://" + hostPort + "/";
      // Append appropriate auth option to connection string.
      if (useSasl) {
        connStr = connStr + SASL_AUTH_SPEC;
      } else {
        connStr = connStr + NOSASL_AUTH_SPEC;
      }
    }

    String query = cmdArgs.getOptionValue("q");
    if (query == null) {
      LOG.error("Must specify a query to execute.");
      LOG.error("Usage: " + options.toString());
      System.exit(1);
    }

    String driver = cmdArgs.getOptionValue("d", TestUtils.HIVE_SERVER2_DRIVER_NAME);

    return new ClientExecOptions(connStr, query, driver);
  }

  private static String formatColumnValue(String colVal, String columnType)
      throws NumberFormatException {
    columnType = columnType.toLowerCase();
    if (colVal == null) {
      return columnType.equals("string") ? "'NULL'" : "NULL";
    }

    if (columnType.equals("string")) {
      return "'" + colVal + "'";
    } else if (columnType.equals("float") || columnType.equals("double")) {
      // Fixup formatting of float/double values to match the expected test
      // results
      DecimalFormat df = new DecimalFormat("#.##################################");
      double doubleVal = Double.parseDouble(colVal);
      return df.format(doubleVal);
    }
    return colVal;
  }

  /**
   * Executes one or more queries using the given ImpalaJdbcClient. Multiple queries
   * should be seperated using semi-colons.
   * @throws SQLException
   */
  private static void execQuery(ImpalaJdbcClient client, String queryString)
      throws SQLException, NumberFormatException {

    String[] queries = queryString.trim().split(";");
    for (String query: queries) {
      query = query.trim();
      if (query.indexOf(" ") > -1) {
        if (query.substring(0, query.indexOf(" ")).equalsIgnoreCase("use")) {
          String[] split_query = query.split(" ");
          String db_name = split_query[split_query.length - 1];
          client.changeDatabase(db_name);
          client.getStatement().close();
          continue;
        }
      }
      long startTime = System.currentTimeMillis();
      ResultSet res = client.execQuery(query);
      ResultSetMetaData meta = res.getMetaData();
      ArrayList<String> arrayList = Lists.newArrayList();

      // This token (and the [END] token) are used to help parsing the result output
      // for test verification purposes.
      LOG.info("----[START]----");
      int rowCount = 0;
      while (res.next()) {
        arrayList.clear();
        for (int i = 1; i <= meta.getColumnCount(); ++i) {
          // Format the value based on the column type
          String colVal = formatColumnValue(res.getString(i), meta.getColumnTypeName(i));
          arrayList.add(colVal);
        }
        LOG.info(Joiner.on(",").join(arrayList));
        ++rowCount;
      }
      LOG.info("----[END]----");
      long endTime = System.currentTimeMillis();
      float seconds = (endTime - startTime) / 1000F;
      LOG.info("Returned " + rowCount + " row(s) in " + seconds + "s");

      // Make sure the Statement is closed after every query.
      client.getStatement().close();
    }
  }

  /**
   * Executes a query over JDBC. Multiple queries can be passed in if they are semi-colon
   * separated.
   */
  public static void main(String[] args) throws SQLException, ClassNotFoundException,
        ParseException, ImpalaRuntimeException {
    // Remove all prefixes from the logging output to make it easier to parse and disable
    // the root logger from spewing anything. This is done to make it easier to parse
    // the output.
    PatternLayout layout = new PatternLayout("%m%n");
    ConsoleAppender consoleAppender = new ConsoleAppender(layout);
    LOG.addAppender(consoleAppender);
    LOG.setAdditivity(false);

    ClientExecOptions execOptions = parseOptions(args);

    ImpalaJdbcClient client =
      ImpalaJdbcClient.createClient(execOptions.getDriver(), execOptions.getConnStr());

    try {
      client.connect();
      execQuery(client, execOptions.getQuery());
    } finally {
      client.close();
    }
  }
}
