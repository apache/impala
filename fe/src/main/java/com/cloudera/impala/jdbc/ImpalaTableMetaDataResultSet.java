package com.cloudera.impala.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Table;

/**
 * ResultSet that provides table metadata of a specific database.
 * It uses the in-memory catalog to provide this information.
 * Used by ImpalaDatabaseMetaData.
 */
public class ImpalaTableMetaDataResultSet extends ImpalaMetaDataResultSet {

  private Db db;
  private final Map<String, Table> sortedTables = new TreeMap<String, Table>();
  private Iterator<Map.Entry<String, Table>> iter;
  private Map.Entry<String, Table> entry;
  private static final List<String> colLabels = new ArrayList<String>();
  private static final List<PrimitiveType> colTypes = new ArrayList<PrimitiveType>();
  static {
    colLabels.add("TABLE_NAME");
    colTypes.add(PrimitiveType.STRING);
  }

  public ImpalaTableMetaDataResultSet(Catalog catalog, String dbName) {
    super(catalog, dbName);
    init();
  }

  private void init() {
    // Assumes dbName has already been verified to exist.
    db = catalog.getDb(dbName);
    // Sort the tables by table name for prettier display.
    sortedTables.putAll(db.getTables());
    iter = sortedTables.entrySet().iterator();
    entry = null;
  }

  @Override
  public boolean next() throws SQLException {
    if (iter.hasNext()) {
     entry = iter.next();
     return true;
    }
    return false;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new ImpalaResultSetMetaData(null, colLabels);
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    // There should only be one column in the table.
    if (entry != null && columnIndex > 0 && columnIndex <= colLabels.size()) {
      return entry.getKey();
    }
    return null;
  }

  @Override
  public void close() throws SQLException {
    entry = null;
  }
}

