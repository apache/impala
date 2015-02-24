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

package com.cloudera.impala.analysis;

import static com.cloudera.impala.catalog.DataSourceTable.TBL_PROP_API_VER;
import static com.cloudera.impala.catalog.DataSourceTable.TBL_PROP_CLASS;
import static com.cloudera.impala.catalog.DataSourceTable.TBL_PROP_DATA_SRC_NAME;
import static com.cloudera.impala.catalog.DataSourceTable.TBL_PROP_INIT_STRING;
import static com.cloudera.impala.catalog.DataSourceTable.TBL_PROP_LOCATION;

import java.util.List;
import java.util.Map;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.DataSource;
import com.cloudera.impala.catalog.DataSourceTable;
import com.cloudera.impala.catalog.RowFormat;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.THdfsFileFormat;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.permission.FsAction;

/**
 * Represents a CREATE TABLE statement for external data sources. Such tables
 * reference an external data source (created with a CREATE DATA SOURCE statement)
 * and the properties of that source are stored in the table properties because
 * the metastore does not store the data sources themselves.
 */
public class CreateTableDataSrcStmt extends CreateTableStmt {

  public CreateTableDataSrcStmt(TableName tableName, List<ColumnDef> columnDefs,
      String dataSourceName, String initString, String comment, boolean ifNotExists) {
    super(tableName, columnDefs, Lists.<ColumnDef>newArrayList(), false, comment,
        RowFormat.DEFAULT_ROW_FORMAT, THdfsFileFormat.TEXT, null, null, ifNotExists,
        createInitialTableProperties(dataSourceName, initString),
        Maps.<String, String>newHashMap());
  }

  /**
   * Creates the initial map of table properties containing the name of the data
   * source and the table init string.
   */
  private static Map<String, String> createInitialTableProperties(
      String dataSourceName, String initString) {
    Preconditions.checkNotNull(dataSourceName);
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(TBL_PROP_DATA_SRC_NAME, dataSourceName.toLowerCase());
    tableProperties.put(TBL_PROP_INIT_STRING, Strings.nullToEmpty(initString));
    return tableProperties;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    String dataSourceName = getTblProperties().get(TBL_PROP_DATA_SRC_NAME);
    DataSource dataSource = analyzer.getCatalog().getDataSource(dataSourceName);
    if (dataSource == null) {
      throw new AnalysisException("Data source does not exist: " + dataSourceName);
    }

    for (ColumnDef col: getColumnDefs()) {
      if (!DataSourceTable.isSupportedColumnType(col.getType())) {
        throw new AnalysisException("Tables produced by an external data source do " +
            "not support the column type: " + col.getType());
      }
    }
    // Add table properties from the DataSource catalog object now that we have access
    // to the catalog. These are stored in the table metadata because DataSource catalog
    // objects are not currently persisted.
    String location = dataSource.getLocation();
    getTblProperties().put(TBL_PROP_LOCATION, location);
    getTblProperties().put(TBL_PROP_CLASS, dataSource.getClassName());
    getTblProperties().put(TBL_PROP_API_VER, dataSource.getApiVersion());
    new HdfsUri(location).analyze(analyzer, Privilege.ALL, FsAction.READ);
    // TODO: check class exists and implements API version
  }
}
