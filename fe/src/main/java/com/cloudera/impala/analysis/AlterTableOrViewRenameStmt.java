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
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAlterTableOrViewRenameParams;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableType;
import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Preconditions;

/**
 * Represents an ALTER TABLE/VIEW RENAME statement.
 */
public class AlterTableOrViewRenameStmt extends AlterTableStmt {
  protected final TableName newTableName;

  // Set during analysis
  protected String newDbName;

  //  True if we are renaming a table. False if we are renaming a view.
  protected final boolean renameTable;

  public AlterTableOrViewRenameStmt(TableName oldTableName, TableName newTableName,
      boolean renameTable) {
    super(oldTableName);
    Preconditions.checkState(newTableName != null && !newTableName.isEmpty());
    this.newTableName = newTableName;
    this.renameTable = renameTable;
  }

  public String getNewTbl() {
    return newTableName.getTbl();
  }

  public String getNewDb() {
    Preconditions.checkNotNull(newDbName);
    return newDbName;
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(
        (renameTable) ? TAlterTableType.RENAME_TABLE : TAlterTableType.RENAME_VIEW);
    TAlterTableOrViewRenameParams renameParams =
        new TAlterTableOrViewRenameParams(new TTableName(getNewDb(), getNewTbl()));
    params.setRename_params(renameParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    table = analyzer.getTable(tableName, Privilege.ALTER);
    if (table instanceof View && renameTable) {
      throw new AnalysisException(String.format(
          "ALTER TABLE not allowed on a view: %s", table.getFullName()));
    }
    if (!(table instanceof View) && !renameTable) {
      throw new AnalysisException(String.format(
          "ALTER VIEW not allowed on a table: %s", table.getFullName()));
    }
    newDbName = analyzer.getTargetDbName(newTableName);
    if (analyzer.dbContainsTable(newDbName, newTableName.getTbl(), Privilege.CREATE)) {
      throw new AnalysisException(Analyzer.TBL_ALREADY_EXISTS_ERROR_MSG +
          String.format("%s.%s", newDbName, getNewTbl()));
    }
  }
}
