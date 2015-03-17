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

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.RuntimeEnv;

import com.google.common.base.Preconditions;

/**
 * Represents an ALTER VIEW AS statement.
 */
public class AlterViewStmt extends CreateOrAlterViewStmtBase {

  public AlterViewStmt(TableName tableName, QueryStmt viewDefStmt) {
    super(false, tableName, null, null, viewDefStmt);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // Enforce Hive column labels for view compatibility.
    analyzer.setUseHiveColLabels(true);
    viewDefStmt_.analyze(analyzer);

    Preconditions.checkState(tableName_ != null && !tableName_.isEmpty());
    dbName_ = analyzer.getTargetDbName(tableName_);
    owner_ = analyzer.getUser().getName();

    Table table = analyzer.getTable(tableName_, Privilege.ALTER);
    Preconditions.checkNotNull(table);
    if (!(table instanceof View)) {
      throw new AnalysisException(String.format(
          "ALTER VIEW not allowed on a table: %s.%s", dbName_, getTbl()));
    }

    createColumnAndViewDefs(analyzer);
    if (RuntimeEnv.INSTANCE.computeLineage() || RuntimeEnv.INSTANCE.isTestEnv()) {
      computeLineageGraph(analyzer);
    }
  }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder();
    sb.append("ALTER VIEW ");
    if (tableName_.getDb() != null) {
      sb.append(tableName_.getDb() + ".");
    }
    sb.append(tableName_.getTbl());
    sb.append(" AS " + viewDefStmt_.toSql());
    return sb.toString();
  }
}
