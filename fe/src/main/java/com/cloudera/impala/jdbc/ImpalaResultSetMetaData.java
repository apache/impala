// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import com.cloudera.impala.catalog.PrimitiveType;

/**
 * Minimal implementation required to run select queries with sqlline.
 * The implemented methods must return non-null values.
 * Most methods are not implemented because they are not required to make sqlline work.
 * Unimplemented methods throw an UnsupportedOperationException that includes the method name of the
 * called method for easier debugging.
 *
 * This class provides metadata about a ResultSet.
 * Currently it only provides column names and types.
 */
public class ImpalaResultSetMetaData implements ResultSetMetaData {

  private final List<PrimitiveType> colTypes;
  private final List<String> colLabels;

  // C'tor for query results.
  public ImpalaResultSetMetaData(List<PrimitiveType> colTypes, List<String> colLabels) {
    this.colTypes = colTypes;
    this.colLabels = colLabels;
  }

  @Override
  public int getColumnCount() throws SQLException {
    return colLabels.size();
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    // Column indexes start from 1.
    return colLabels.get(column - 1);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    // Column indexes start from 1.
    return colLabels.get(column - 1);
  }

  // Unimplemented for now but must not throw. sqlline uses this method when executing queries.
  @Override
  public String getTableName(int column) throws SQLException {
    return null;
  }

  // Non-essential and unimplemented methods start here.

  @Override
  public String getSchemaName(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int isNullable(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getScale(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }
}
