// Copyright 2014 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.extdatasource.v1.ExternalDataSource;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TColumn;
import com.cloudera.impala.thrift.TDataSource;
import com.cloudera.impala.thrift.TDataSourceTable;
import com.cloudera.impala.thrift.TResultSet;
import com.cloudera.impala.thrift.TResultSetMetadata;
import com.cloudera.impala.thrift.TTable;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.cloudera.impala.thrift.TTableType;
import com.cloudera.impala.util.TResultRowBuilder;
import com.google.common.base.Preconditions;

/**
 * Represents a table backed by an external data source. All data source properties are
 * stored as table properties (persisted in the metastore) because the DataSource catalog
 * object is not persisted so the DataSource catalog object will not exist if the catalog
 * server is restarted, but the table does not need the DataSource catalog object in
 * order to scan the table. Tables that contain the TBL_PROP_DATA_SRC_NAME table
 * parameter are assumed to be backed by an external data source.
 */
public class DataSourceTable extends Table {
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

  protected DataSourceTable(
      TableId id, org.apache.hadoop.hive.metastore.api.Table msTable,
      Db db, String name, String owner) {
    super(id, msTable, db, name, owner);
  }

  /**
   * Gets the the data source.
   */
  public TDataSource getDataSource() { return dataSource_; }

  /**
   * Gets the table init string passed to the data source.
   */
  public String getInitString() { return initString_; }

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
        return true;
      case BINARY:
      case CHAR:
      case DATE:
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
  private void loadColumns(List<FieldSchema> fieldSchemas, HiveMetaStoreClient client)
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
  public void load(boolean reuseMetadata, HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    Preconditions.checkNotNull(msTbl);
    msTable_ = msTbl;
    clearColumns();
    LOG.debug("load table: " + db_.getName() + "." + name_);
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
      numRows_ = getRowCount(super.getMetaStoreTable().getParameters());
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
  public TResultSet getTableStats() {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    resultSchema.addToColumns(new TColumn("#Rows", Type.BIGINT.toThrift()));
    result.setSchema(resultSchema);
    TResultRowBuilder rowBuilder = new TResultRowBuilder();
    rowBuilder.add(numRows_);
    result.addToRows(rowBuilder.get());
    return result;
  }

  @Override
  public TTableDescriptor toThriftDescriptor(Set<Long> referencedPartitions) {
    TTableDescriptor tableDesc = new TTableDescriptor(id_.asInt(),
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
