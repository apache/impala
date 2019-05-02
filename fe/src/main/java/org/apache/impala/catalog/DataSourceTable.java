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

package org.apache.impala.catalog;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.impala.extdatasource.v1.ExternalDataSource;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TDataSource;
import org.apache.impala.thrift.TDataSourceTable;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.TResultRowBuilder;
import com.google.common.base.Preconditions;

/**
 * All data source properties are stored as table properties (persisted in the
 * metastore) because the DataSource catalog object is not persisted so the
 * DataSource catalog object will not exist if the catalog server is restarted,
 * but the table does not need the DataSource catalog object in order to scan
 * the table. Tables that contain the TBL_PROP_DATA_SRC_NAME table parameter are
 * assumed to be backed by an external data source.
 */
public class DataSourceTable extends Table implements FeDataSourceTable {
  private final static Logger LOG = LoggerFactory.getLogger(DataSourceTable.class);

  /**
   * Table property key for the data source name.
   */
  public static final String TBL_PROP_DATA_SRC_NAME = "__IMPALA_DATA_SOURCE_NAME";

  /**
   * Table property key for the table init string.
   */
  public static final String TBL_PROP_INIT_STRING = "__IMPALA_DATA_SOURCE_INIT_STRING";

  /**
   * Table property key for the data source library HDFS path.
   */
  public static final String TBL_PROP_LOCATION = "__IMPALA_DATA_SOURCE_LOCATION";

  /**
   * Table property key for the class implementing {@link ExternalDataSource}.
   */
  public static final String TBL_PROP_CLASS = "__IMPALA_DATA_SOURCE_CLASS";

  /**
   * Table property key for the API version implemented by the data source.
   */
  public static final String TBL_PROP_API_VER = "__IMPALA_DATA_SOURCE_API_VERSION";

  private String initString_;
  private TDataSource dataSource_;

  protected DataSourceTable(org.apache.hadoop.hive.metastore.api.Table msTable,
      Db db, String name, String owner) {
    super(msTable, db, name, owner);
  }

  /**
   * Gets the the data source.
   */
  @Override // FeDataSourceTable
  public TDataSource getDataSource() { return dataSource_; }

  /**
   * Gets the table init string passed to the data source.
   */
  @Override // FeDataSourceTable
  public String getInitString() { return initString_; }

  @Override // FeDataSourceTable
  public int getNumNodes() { return 1; }

  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.TABLE; }

  /**
   * Returns true if the column type is supported.
   */
  public static boolean isSupportedColumnType(Type colType) {
    Preconditions.checkNotNull(colType);
    return isSupportedPrimitiveType(colType.getPrimitiveType());
  }

  @Override
  public long getWriteId() {
    return -1;
  }

  @Override
  public String getValidWriteIds() {
    return null;
  }

  /**
   * Returns true if the primitive type is supported.
   */
  public static boolean isSupportedPrimitiveType(PrimitiveType primitiveType) {
    Preconditions.checkNotNull(primitiveType);
    switch (primitiveType) {
      case BIGINT:
      case INT:
      case SMALLINT:
      case TINYINT:
      case DOUBLE:
      case FLOAT:
      case BOOLEAN:
      case STRING:
      case TIMESTAMP:
      case DECIMAL:
      case DATE:
        return true;
      case BINARY:
      case CHAR:
      case DATETIME:
      case INVALID_TYPE:
      case NULL_TYPE:
      default:
        return false;
    }
  }

  /**
   * Create columns corresponding to fieldSchemas.
   * Throws a TableLoadingException if the metadata is incompatible with what we
   * support.
   */
  private void loadColumns(List<FieldSchema> fieldSchemas, IMetaStoreClient client)
      throws TableLoadingException {
    int pos = 0;
    for (FieldSchema s: fieldSchemas) {
      Column col = new Column(s.getName(), parseColumnType(s), s.getComment(), pos);
      Preconditions.checkArgument(isSupportedColumnType(col.getType()));
      addColumn(col);
      ++pos;
    }
  }

  @Override
  protected void loadFromThrift(TTable thriftTable) throws TableLoadingException {
    super.loadFromThrift(thriftTable);
    TDataSourceTable dataSourceTable = thriftTable.getData_source_table();
    initString_ = dataSourceTable.getInit_string();
    dataSource_ = dataSourceTable.getData_source();
  }

  @Override
  public void load(boolean reuseMetadata, IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    Preconditions.checkNotNull(msTbl);
    msTable_ = msTbl;
    clearColumns();
    if (LOG.isTraceEnabled()) {
      LOG.trace("load table: " + db_.getName() + "." + name_);
    }
    String dataSourceName = getRequiredTableProperty(msTbl, TBL_PROP_DATA_SRC_NAME, null);
    String location = getRequiredTableProperty(msTbl, TBL_PROP_LOCATION, dataSourceName);
    String className = getRequiredTableProperty(msTbl, TBL_PROP_CLASS, dataSourceName);
    String apiVersionString = getRequiredTableProperty(msTbl, TBL_PROP_API_VER,
        dataSourceName);
    dataSource_ = new TDataSource(dataSourceName, location, className, apiVersionString);
    initString_ = getRequiredTableProperty(msTbl, TBL_PROP_INIT_STRING, dataSourceName);

    if (msTbl.getPartitionKeysSize() > 0) {
      throw new TableLoadingException("Data source table cannot contain clustering " +
          "columns: " + name_);
    }
    numClusteringCols_ = 0;

    try {
      // Create column objects.
      List<FieldSchema> fieldSchemas = getMetaStoreTable().getSd().getCols();
      loadColumns(fieldSchemas, client);

      // Set table stats.
      setTableStats(msTable_);
      refreshLastUsedTime();
    } catch (Exception e) {
      throw new TableLoadingException("Failed to load metadata for data source table: " +
          name_, e);
    }
  }

  private String getRequiredTableProperty(
      org.apache.hadoop.hive.metastore.api.Table msTbl, String key, String dataSourceName)
      throws TableLoadingException {
    String val = msTbl.getParameters().get(key);
    if (val == null) {
      throw new TableLoadingException(String.format("Failed to load table %s produced " +
          "by external data source %s. Missing required metadata: %s", name_,
          dataSourceName == null ? "<unknown>" : dataSourceName, key));
    }
    return val;
  }

  /**
   * Returns statistics on this table as a tabular result set. Used for the
   * SHOW TABLE STATS statement. The schema of the returned TResultSet is set
   * inside this method.
   */
  @Override // FeDataSourceTable
  public TResultSet getTableStats() {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    resultSchema.addToColumns(new TColumn("#Rows", Type.BIGINT.toThrift()));
    result.setSchema(resultSchema);
    TResultRowBuilder rowBuilder = new TResultRowBuilder();
    rowBuilder.add(tableStats_.num_rows);
    result.addToRows(rowBuilder.get());
    return result;
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId, Set<Long> referencedPartitions) {
    TTableDescriptor tableDesc = new TTableDescriptor(tableId,
        TTableType.DATA_SOURCE_TABLE, getTColumnDescriptors(), numClusteringCols_,
        name_, db_.getName());
    tableDesc.setDataSourceTable(getDataSourceTable());
    return tableDesc;
  }

  /**
   * Returns a thrift structure representing the table.
   */
  @Override
  public TTable toThrift() {
    TTable table = super.toThrift();
    table.setTable_type(TTableType.DATA_SOURCE_TABLE);
    table.setData_source_table(getDataSourceTable());
    return table;
  }

  /**
   * Returns a thrift {@link TDataSourceTable} structure for the data source table.
   */
  private TDataSourceTable getDataSourceTable() {
    return new TDataSourceTable(dataSource_, initString_);
  }

  /**
   * True if the Hive {@link org.apache.hadoop.hive.metastore.api.Table} is a
   * data source table by checking for the existance of the
   * TBL_PROP_DATA_SRC_NAME table property.
   */
  public static boolean isDataSourceTable(
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return msTbl.getParameters().containsKey(TBL_PROP_DATA_SRC_NAME);
  }
}
