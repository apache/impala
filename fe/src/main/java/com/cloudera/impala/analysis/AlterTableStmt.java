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
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Preconditions;

/**
 * Base class for all ALTER TABLE statements.
 */
public abstract class AlterTableStmt extends StatementBase {
  private final TableName tableName;

  // Set during analysis.
  private Table table;

  protected AlterTableStmt(TableName tableName) {
    Preconditions.checkState(tableName != null && !tableName.isEmpty());
    this.tableName = tableName;
    this.table = null;
  }

  public String getTbl() {
    return tableName.getTbl();
  }

  /**
   * Can only be called after analysis, returns the parent database name of the target
   * table for this ALTER TABLE statement.
   */
  public String getDb() {
    return getTargetTable().getDb().getName();
  }

  /**
   * Can only be called after analysis, returns the Table object of the target of this
   * ALTER TABLE statement.
   */
  protected Table getTargetTable() {
    Preconditions.checkNotNull(table);
    return table;
  }

  public TAlterTableParams toThrift() {
    TAlterTableParams params = new TAlterTableParams();
    params.setTable_name(new TTableName(getDb(), getTbl()));
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    table = analyzer.getTable(tableName, Privilege.ALTER);
  }
}
