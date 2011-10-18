// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.service.Executor;

/**
 * Minimal implementation required to run select queries with sqlline.
 * The implemented methods must return non-null values.
 * Most methods are not implemented because they are not required to make sqlline work.
 * Unimplemented methods throw an UnsupportedOperationException that includes the method name of the
 * called method for easier debugging.
 *
 * This class creates ImpalaStatements and provides ImpalaDatabaseMetaData.
 */
public class ImpalaConnection implements java.sql.Connection {

  // Name of catalog, used as identifier for calls in ImpalaDatabaseMetaData
  public static final String CATALOG_NAME = "ImpalaInMemoryCatalog";
  // Connection url passed in by the user.
  private final String url;
  // The name of the database this connection refers to.
  private final String dbName;
  // Connection properties such as user name.
  private final Properties info;
  // For providing metadata.
  private final Catalog catalog;
  // For executing queries.
  private final Executor coordinator;
  // Connection status.
  private boolean isClosed = true;

  public ImpalaConnection(String url, String dbName, Properties info, Catalog catalog) {
    this.url = url;
    this.dbName = dbName;
    this.info = info;
    this.catalog = catalog;
    coordinator = new Executor(catalog);
    isClosed = false;
  }

  @Override
  public Statement createStatement() throws SQLException {
    return new ImpalaStatement(coordinator, this);
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new ImpalaDatabaseMetaData(this, url, dbName, catalog);
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getCatalog() throws SQLException {
    return CATALOG_NAME;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public void close() throws SQLException {
    isClosed = true;
  }

  // We currently don't support transactions.
  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    // Don't throw to avoid an error message in sqlline.
  }

  // We don't support transactions, so commit doesn't make sense.
  @Override
  public boolean getAutoCommit() throws SQLException {
    return false;
  }

  // We currently don't support transactions.
  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    // Don't throw to avoid an error message in sqlline.
  }

  // We currently don't support transactions. For some reason sqlline still reports REPEATABLE_READ.
  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  // Non-essential and unimplemented methods start here.

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public void commit() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public void rollback() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getHoldability() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public Clob createClob() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }
}
