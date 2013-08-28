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
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

/**
 * Represents an ALTER VIEW AS statement.
 */
public class AlterViewStmt extends CreateOrAlterViewStmtBase {

  public AlterViewStmt(TableName tableName, QueryStmt viewDefStmt) {
    super(false, tableName, null, null, viewDefStmt);
  }

  @Override
  public void analyze(Analyzer analyzer)
      throws AnalysisException, AuthorizationException {
    // Enforce Hive column labels for view compatibility.
    analyzer.setUseHiveColLabels(true);
    viewDefStmt.analyze(analyzer);

    Preconditions.checkState(tableName != null && !tableName.isEmpty());
    dbName = analyzer.getTargetDbName(tableName);
    owner = analyzer.getUser().getName();

    Table table = analyzer.getTable(tableName, Privilege.ALTER);
    Preconditions.checkNotNull(table);
    if (!(table instanceof View)) {
      throw new AnalysisException(String.format(
          "ALTER VIEW not allowed on a table: %s.%s", dbName, getTbl()));
    }

    createColumnAndViewDefs(analyzer);
  }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder();
    sb.append("ALTER VIEW ");
    if (tableName.getDb() != null) {
      sb.append(tableName.getDb() + ".");
    }
    sb.append(tableName.getTbl());
    sb.append(" AS " + viewDefStmt.toSql());
    return sb.toString();
  }
}
