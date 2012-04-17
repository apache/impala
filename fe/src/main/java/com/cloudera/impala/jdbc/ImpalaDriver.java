// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.jdbc;

import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import com.cloudera.impala.catalog.Catalog;

/**
 * Minimal implementation required to run select queries with sqlline.
 * The implemented methods must return non-null values.
 * Most methods are not implemented because they are not required to make sqlline work.
 * Unimplemented methods throw an UnsupportedOperationException that includes the method name of the
 * called method for easier debugging.
 *
 * This class registers an instance of itself in the DriverManager
 * such that clients can connect to it.
 * When a client requests a connection, the DiverManager goes through all registered drivers
 * to find a driver that accepts the connection URL given by the user.
 * The current driver accepts any url that contains "impala", and accepts any username/password.
 */
public class ImpalaDriver implements java.sql.Driver {

  static {
    try {
      DriverManager.registerDriver(new ImpalaDriver());
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private final Catalog catalog;

  protected ImpalaDriver() throws Exception {
    catalog = new Catalog();
  }

  @Override
  public ImpalaConnection connect(String url, Properties info) throws SQLException {
    String dbName = extractDbName(url);
    if (catalog.getDb(dbName) == null) {
      throw new SQLException("Can't connect to database '" + dbName + "' given in url '"
          + url + "'. Database doesn't exist.");
    }
    return new ImpalaConnection(url, dbName, info, catalog);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    if (url.contains("impala")) {
      String dbName = extractDbName(url);
      if (dbName != null) {
        return true;
      } else {
        return false;
      }
    }
    return false;
  }

  // The field after "impala" names the database.
  // For example, in 'jdbc:impala:mydb' the database would be 'mydb'
  // or, in 'abc:impala:mydb:xyz', the database would be 'mydb'
  // or, in 'abc:impala:impala:xyz', the database would be 'impala'
  private String extractDbName(String url) {
    String[] urlParts = url.split(":");
    boolean firstImpalaFound = false;
    String dbName = null;
    for (String s : urlParts) {
      if (firstImpalaFound) {
        dbName = s;
      }
      if (!firstImpalaFound && s.equals("impala")) {
        firstImpalaFound = true;
      }
    }
    return dbName;
  }

  // Non-essential and unimplemented methods start here.

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }
}
