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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.thrift.TAccessEvent;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.service.BackendConfig;

import com.google.common.base.Preconditions;

/**
 * Represents an ALTER VIEW AS statement.
 */
public class AlterViewStmt extends CreateOrAlterViewStmtBase {
  public AlterViewStmt(
      TableName tableName, List<ColumnDef> columnDefs, QueryStmt viewDefStmt) {
    super(false, tableName, columnDefs, null, null, viewDefStmt);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // Enforce Hive column labels for view compatibility.
    analyzer.setUseHiveColLabels(true);
    viewDefStmt_.analyze(analyzer);

    Preconditions.checkState(tableName_ != null && !tableName_.isEmpty());
    dbName_ = analyzer.getTargetDbName(tableName_);
    owner_ = analyzer.getUserShortName();
    // Set the servername here if authorization is enabled because analyzer_ is not
    // available in the toThrift() method.
    serverName_ = analyzer.getServerName();

    FeTable table = analyzer.getTable(tableName_, Privilege.ALTER);
    Preconditions.checkNotNull(table);
    if (!(table instanceof FeView)) {
      throw new AnalysisException(String.format(
          "ALTER VIEW not allowed on a table: %s.%s", dbName_, getTbl()));
    }
    analyzer.addAccessEvent(new TAccessEvent(dbName_ + "." + tableName_.getTbl(),
        TCatalogObjectType.VIEW, Privilege.ALTER.toString()));

    // viewDefStmt_ should not contain any references to the view being altered.
    Set<FeView> inlineViews = new HashSet<>();
    viewDefStmt_.collectInlineViews(inlineViews);
    TableRef tblRef = analyzer.resolveTableRef(new TableRef(tableName_.toPath(), null));
    if (inlineViews.contains(((InlineViewRef) tblRef).getView())) {
      throw new AnalysisException(
          String.format("Self-reference not allowed on view: %s", tblRef.toSql()));
    }

    createColumnAndViewDefs(analyzer);
    if (BackendConfig.INSTANCE.getComputeLineage() || RuntimeEnv.INSTANCE.isTestEnv()) {
      computeLineageGraph(analyzer);
    }
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder();
    sb.append("ALTER VIEW ");
    if (tableName_.getDb() != null) {
      sb.append(tableName_.getDb() + ".");
    }
    sb.append(tableName_.getTbl());
    if (columnDefs_ != null) sb.append("(" + getColumnNames() + ")");
    sb.append(" AS " + viewDefStmt_.toSql(options));
    return sb.toString();
  }
}
