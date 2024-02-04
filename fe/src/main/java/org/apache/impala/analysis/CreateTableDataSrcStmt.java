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

package org.apache.impala.analysis;

import static org.apache.impala.catalog.DataSourceTable.TBL_PROP_API_VER;
import static org.apache.impala.catalog.DataSourceTable.TBL_PROP_CLASS;
import static org.apache.impala.catalog.DataSourceTable.TBL_PROP_DATA_SRC_NAME;
import static org.apache.impala.catalog.DataSourceTable.TBL_PROP_INIT_STRING;
import static org.apache.impala.catalog.DataSourceTable.TBL_PROP_LOCATION;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.catalog.FeDataSource;
import org.apache.impala.common.AnalysisException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.fs.permission.FsAction;

/**
 * Represents a CREATE TABLE statement for external data sources. Such tables
 * reference an external data source (created with a CREATE DATA SOURCE statement)
 * and the properties of that source are stored in the table properties because
 * the metastore does not store the data sources themselves.
 */
public class CreateTableDataSrcStmt extends CreateTableStmt {

  public CreateTableDataSrcStmt(CreateTableStmt createTableStmt, String dataSourceName,
      String initString) {
    super(createTableStmt);
    Preconditions.checkNotNull(dataSourceName);
    getTblProperties().put(TBL_PROP_DATA_SRC_NAME, dataSourceName.toLowerCase());
    getTblProperties().put(TBL_PROP_INIT_STRING, Strings.nullToEmpty(initString));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    String dataSourceName = getTblProperties().get(TBL_PROP_DATA_SRC_NAME);
    FeDataSource dataSource = analyzer.getCatalog().getDataSource(dataSourceName);
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
    // to the catalog. These are stored in the table metadata so that the table could
    // be scanned without the DataSource catalog object.
    String location = dataSource.getLocation();
    getTblProperties().put(TBL_PROP_LOCATION, location != null ? location : "");
    getTblProperties().put(TBL_PROP_CLASS, dataSource.getClassName());
    getTblProperties().put(TBL_PROP_API_VER, dataSource.getApiVersion());
    if (!Strings.isNullOrEmpty(location)) {
      new HdfsUri(location).analyze(analyzer, Privilege.ALL, FsAction.READ);
    }
  }
}
