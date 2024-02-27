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

package org.apache.impala.catalog.local;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeDataSourceTable;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.extdatasource.ApiVersion;
import org.apache.impala.extdatasource.jdbc.conf.JdbcStorageConfig;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TDataSource;
import org.apache.impala.thrift.TDataSourceTable;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.JsonUtil;
import org.apache.impala.util.TResultRowBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * DataSource table instance loaded from {@link LocalCatalog}.
 *
 * All DataSource properties are stored as table properties (persisted in the
 * metastore). Tables that contain the TBL_PROP_DATA_SRC_NAME table parameter are
 * assumed to be backed by an external data source.
 */
public class LocalDataSourceTable extends LocalTable implements FeDataSourceTable {
  private final static Logger LOG = LoggerFactory.getLogger(LocalDataSourceTable.class);

  private String initString_;
  private TDataSource dataSource_;

  public static LocalDataSourceTable load(LocalDb db, Table msTbl, TableMetaRef ref)
      throws TableLoadingException {
    Preconditions.checkNotNull(db);
    Preconditions.checkNotNull(msTbl);
    if (LOG.isTraceEnabled()) {
      LOG.trace("load table: " + msTbl.getDbName() + "." + msTbl.getTableName());
    }
    if (msTbl.getPartitionKeysSize() > 0) {
      throw new TableLoadingException("Data source table cannot contain clustering " +
          "columns: " + msTbl.getTableName());
    }
    return new LocalDataSourceTable(db, msTbl, ref);
  }

  private LocalDataSourceTable(LocalDb db, Table msTbl, TableMetaRef ref)
      throws TableLoadingException {
    super(db, msTbl, ref);

    String dataSourceName = getTableProperty(
        msTbl, DataSourceTable.TBL_PROP_DATA_SRC_NAME, null, true);
    if (dataSourceName.equals(DataSourceTable.IMPALA_BUILTIN_JDBC_DATASOURCE)) {
      // The table is created with "STORED BY JDBC".
      dataSource_ = new TDataSource(dataSourceName, /* location */ "",
          /* className */ DataSourceTable.IMPALA_JDBC_DATA_SRC_CLASSNAME,
          /* apiVersionString */ ApiVersion.V1.name());
      // Serialize table properties to JSON string as initString for data source.
      Map<String, String> tblProperties = new HashMap<String, String>();
      for (JdbcStorageConfig config : JdbcStorageConfig.values()) {
        String propertyValue = getTableProperty(msTbl, config.getPropertyName(),
            DataSourceTable.IMPALA_BUILTIN_JDBC_DATASOURCE, false);
        if (propertyValue != null) {
          tblProperties.put(config.getPropertyName(), propertyValue);
        }
      }
      try {
        initString_ = JsonUtil.convertPropertyMapToJSON(tblProperties);
      } catch (ImpalaRuntimeException e) {
        throw new TableLoadingException(e.getMessage());
      }
    } else {
      // The table is created with "PRODUCED BY DATA SOURCE".
      String location = getTableProperty(
          msTbl, DataSourceTable.TBL_PROP_LOCATION, dataSourceName, true);
      String className = getTableProperty(
          msTbl, DataSourceTable.TBL_PROP_CLASS, dataSourceName, true);
      String apiVersionString = getTableProperty(
          msTbl, DataSourceTable.TBL_PROP_API_VER, dataSourceName, true);
      dataSource_ =
          new TDataSource(dataSourceName, location, className, apiVersionString);
      initString_ = getTableProperty(
          msTbl, DataSourceTable.TBL_PROP_INIT_STRING, dataSourceName, true);
    }
  }

  private String getTableProperty(Table msTbl, String key, String dataSourceName,
      boolean required) throws TableLoadingException {
    String val = msTbl.getParameters().get(key);
    if (val == null && required) {
      if (key.equals(DataSourceTable.TBL_PROP_DATA_SRC_NAME)) {
        throw new TableLoadingException(String.format("Failed to load table %s. " +
            "Missing required metadata: %s", msTbl.getTableName(), key));
      } else if (dataSourceName.equals(DataSourceTable.IMPALA_BUILTIN_JDBC_DATASOURCE)) {
        throw new TableLoadingException(String.format("Failed to load table %s stored " +
            "by JDBC. Missing required metadata: %s", msTbl.getTableName(), key));
      } else {
        throw new TableLoadingException(String.format("Failed to load table %s " +
            "produced by external data source %s. Missing required metadata: %s",
            msTbl.getTableName(), dataSourceName, key));
      }
    }
    return val;
  }

  /**
   * Gets the DataSource object.
   */
  @Override // FeDataSourceTable
  public TDataSource getDataSource() { return dataSource_; }

  /**
   * Gets the table init string passed to the DataSource.
   */
  @Override // FeDataSourceTable
  public String getInitString() { return initString_; }

  @Override // FeDataSourceTable
  public int getNumNodes() { return 1; }

  @Override // FeDataSourceTable
  public boolean isJdbcDataSourceTable() {
    return (dataSource_ != null && dataSource_.name != null &&
        dataSource_.name.equals(DataSourceTable.IMPALA_BUILTIN_JDBC_DATASOURCE));
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
    rowBuilder.add(getNumRows());
    result.addToRows(rowBuilder.get());
    return result;
  }

  @Override
  public TTableDescriptor toThriftDescriptor(
      int tableId, Set<Long> referencedPartitions) {
    TTableDescriptor tableDesc = new TTableDescriptor(tableId,
        TTableType.DATA_SOURCE_TABLE, FeCatalogUtils.getTColumnDescriptors(this),
        getNumClusteringCols(), getName(), getDb().getName());
    tableDesc.setDataSourceTable(getDataSourceTable());
    return tableDesc;
  }

  /**
   * Returns a thrift {@link TDataSourceTable} structure for this DataSource table.
   */
  private TDataSourceTable getDataSourceTable() {
    return new TDataSourceTable(dataSource_, initString_);
  }
}
