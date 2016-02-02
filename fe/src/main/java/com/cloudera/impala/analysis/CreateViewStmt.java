// Copyright 2012 Cloudera Inc.
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

import java.util.ArrayList;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.RuntimeEnv;
import com.cloudera.impala.thrift.TAccessEvent;
import com.cloudera.impala.thrift.TCatalogObjectType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Represents a CREATE VIEW statement.
 */
public class CreateViewStmt extends CreateOrAlterViewStmtBase {

  public CreateViewStmt(boolean ifNotExists, TableName tableName,
      ArrayList<ColumnDef> columnDefs, String comment, QueryStmt viewDefStmt) {
    super(ifNotExists, tableName, columnDefs, comment, viewDefStmt);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(tableName_ != null && !tableName_.isEmpty());

    tableName_.analyze();
    // Use a child analyzer to let views have complex-typed columns.
    Analyzer viewAnalyzerr = new Analyzer(analyzer);
    // Enforce Hive column labels for view compatibility.
    viewAnalyzerr.setUseHiveColLabels(true);
    viewDefStmt_.analyze(viewAnalyzerr);

    dbName_ = analyzer.getTargetDbName(tableName_);
    owner_ = analyzer.getUser().getName();
    if (analyzer.dbContainsTable(dbName_, tableName_.getTbl(), Privilege.CREATE) &&
        !ifNotExists_) {
      throw new AnalysisException(Analyzer.TBL_ALREADY_EXISTS_ERROR_MSG +
          String.format("%s.%s", dbName_, tableName_.getTbl()));
    }
    analyzer.addAccessEvent(new TAccessEvent(dbName_ + "." + tableName_.getTbl(),
        TCatalogObjectType.VIEW, Privilege.CREATE.toString()));

    createColumnAndViewDefs(analyzer);
    if (RuntimeEnv.INSTANCE.computeLineage() || RuntimeEnv.INSTANCE.isTestEnv()) {
      computeLineageGraph(analyzer);
    }
  }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE VIEW ");
    if (ifNotExists_) sb.append("IF NOT EXISTS ");
    if (tableName_.getDb() != null) sb.append(tableName_.getDb() + ".");
    sb.append(tableName_.getTbl() + " (");
    sb.append(Joiner.on(", ").join(columnDefs_));
    sb.append(") AS ");
    sb.append(viewDefStmt_.toSql());
    return sb.toString();
  }
}
