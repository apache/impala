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

import java.util.List;
import java.util.Map;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.thrift.TAccessEvent;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.service.BackendConfig;

import com.google.common.base.Preconditions;

/**
 * Represents a CREATE VIEW statement.
 */
public class CreateViewStmt extends CreateOrAlterViewStmtBase {
  public CreateViewStmt(boolean ifNotExists, TableName tableName,
      List<ColumnDef> columnDefs, String comment, Map<String, String> tblpropertiesMap,
      QueryStmt viewDefStmt) {
    super(ifNotExists, tableName, columnDefs, comment, tblpropertiesMap, viewDefStmt);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(tableName_ != null && !tableName_.isEmpty());

    analyzer.getFqTableName(tableName_).analyze();
    // Use a child analyzer to let views have complex-typed columns.
    Analyzer viewAnalyzerr = new Analyzer(analyzer);
    // Enforce Hive column labels for view compatibility.
    viewAnalyzerr.setUseHiveColLabels(true);
    viewDefStmt_.analyze(viewAnalyzerr);

    dbName_ = analyzer.getTargetDbName(tableName_);
    owner_ = analyzer.getUserShortName();
    // Set the servername here if authorization is enabled because analyzer_ is not
    // available in the toThrift() method.
    serverName_ = analyzer.getServerName();

    if (analyzer.dbContainsTable(dbName_, tableName_.getTbl(), Privilege.CREATE) &&
        !ifNotExists_) {
      throw new AnalysisException(Analyzer.VIEW_ALREADY_EXISTS_ERROR_MSG +
          String.format("%s.%s", dbName_, tableName_.getTbl()));
    }
    analyzer.addAccessEvent(new TAccessEvent(dbName_ + "." + tableName_.getTbl(),
        TCatalogObjectType.VIEW, Privilege.CREATE.toString()));

    createColumnAndViewDefs(analyzer);
    if (BackendConfig.INSTANCE.getComputeLineage() || RuntimeEnv.INSTANCE.isTestEnv()) {
      computeLineageGraph(analyzer);
    }
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE VIEW ");
    if (ifNotExists_) sb.append("IF NOT EXISTS ");
    if (tableName_.getDb() != null) sb.append(tableName_.getDb() + ".");
    sb.append(tableName_.getTbl());
    if (columnDefs_ != null) sb.append("(" + getColumnNames() + ")");
    if (tblPropertyMap_ != null && !tblPropertyMap_.isEmpty()) {
      sb.append(" TBLPROPERTIES " + getTblProperties());
    }
    sb.append(" AS ");
    sb.append(viewDefStmt_.toSql(options));
    return sb.toString();
  }
}
