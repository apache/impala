package com.cloudera.impala.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Table;

/**
 * ResultSet that provides column metadata of a specific table.
 * It uses the in-memory catalog to provide this information.
 * Used by ImpalaDatabaseMetaData.
 */
public class ImpalaColumnMetaDataResultSet extends ImpalaMetaDataResultSet {
  private Db db;
  private final String tableName;
  private Table table;
  private Iterator<Column> iter;
  private Column col;
  private static final List<String> colLabels = new ArrayList<String>();
  private static final List<PrimitiveType> colTypes = new ArrayList<PrimitiveType>();
  static {
    colLabels.add("TABLE_NAME");
    colLabels.add("COLUMN_NAME");
    colLabels.add("COLUMN_TYPE");
    colTypes.add(PrimitiveType.STRING);
    colTypes.add(PrimitiveType.STRING);
    colTypes.add(PrimitiveType.STRING);
  }

  public ImpalaColumnMetaDataResultSet(Catalog catalog, String dbName, String tableName) throws SQLException {
    super(catalog, dbName);
    this.tableName = tableName;
    init();
  }

  private void init() throws SQLException {
    // Assumes dbName has already been verified to exist.
    db = catalog.getDb(dbName);
    table = db.getTable(tableName);
    if (table == null) {
      throw new SQLException("Table '" + tableName +
          "' doesn't exist in database '" + dbName + "'.");
    }
    iter = table.getColumns().iterator();
    col = null;
  }

  @Override
  public boolean next() throws SQLException {
    if (iter.hasNext()) {
      col = iter.next();
     return true;
    }
    return false;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new ImpalaResultSetMetaData(colTypes, colLabels);
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    if (col != null && columnIndex > 0 && columnIndex <= colLabels.size()) {
      switch(columnIndex) {
        case 1: return tableName;
        case 2: return col.getName();
        case 3: return col.getType().toString();
        default: return null;
      }
    }
    return null;
  }

  @Override
  public void close() throws SQLException {
    col = null;
  }
}
