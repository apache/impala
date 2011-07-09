// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.jdbc;

import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import com.cloudera.impala.catalog.Catalog;

/**
 * Minimal implementation required to run select queries with sqlline.
 * The implemented methods must return non-null values.
 * Most methods are not implemented because they are not required to make sqlline work.
 *
 * This class registers an instance of itself in the DriverManager such that clients can connect to it.
 * When a client requests a connection, the DiverManager goes through all registered drivers to find
 * a driver that accepts the connection URL given by the user.
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
    HiveMetaStoreClient client = new HiveMetaStoreClient(new HiveConf(ImpalaDriver.class));
    catalog = new Catalog(client);
  }

  @Override
  public ImpalaConnection connect(String url, Properties info) throws SQLException {
    return new ImpalaConnection(url, info, catalog);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    if (url.contains("impala")) {
      return true;
    }
    return false;
  }

  // Non-essential and unimplemented methods start here.

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    // TODO Auto-generated method stub
    return null;
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
