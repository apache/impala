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

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeView;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAccessEvent;
import org.apache.impala.thrift.TAlterTableOrViewRenameParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TTableName;
import com.google.common.base.Preconditions;

/**
 * Represents an ALTER TABLE/VIEW RENAME statement.
 */
public class AlterTableOrViewRenameStmt extends AlterTableStmt {
  protected final TableName newTableName_;

  // Set during analysis
  protected String newDbName_;

  //  True if we are renaming a table. False if we are renaming a view.
  protected final boolean renameTable_;

  public AlterTableOrViewRenameStmt(TableName oldTableName, TableName newTableName,
      boolean renameTable) {
    super(oldTableName);
    Preconditions.checkState(newTableName != null && !newTableName.isEmpty());
    newTableName_ = newTableName;
    renameTable_ = renameTable;
  }

  public String getNewTbl() {
    return newTableName_.getTbl();
  }

  public String getNewDb() {
    Preconditions.checkNotNull(newDbName_);
    return newDbName_;
  }

  @Override
  public String getOperation() { return "RENAME"; }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(
        (renameTable_) ? TAlterTableType.RENAME_TABLE : TAlterTableType.RENAME_VIEW);
    TAlterTableOrViewRenameParams renameParams =
        new TAlterTableOrViewRenameParams(new TTableName(getNewDb(), getNewTbl()));
    params.setRename_params(renameParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    analyzer.getFqTableName(newTableName_).analyze();
    table_ = analyzer.getTable(tableName_, Privilege.ALL);
    if (table_ instanceof FeView && renameTable_) {
      throw new AnalysisException(String.format(
          "ALTER TABLE not allowed on a view: %s", table_.getFullName()));
    }
    if (!(table_ instanceof FeView) && !renameTable_) {
      throw new AnalysisException(String.format(
          "ALTER VIEW not allowed on a table: %s", table_.getFullName()));
    }
    newDbName_ = analyzer.getTargetDbName(newTableName_);
    if (analyzer.dbContainsTable(newDbName_, newTableName_.getTbl(), Privilege.CREATE)) {
      throw new AnalysisException(Analyzer.TBL_ALREADY_EXISTS_ERROR_MSG +
          String.format("%s.%s", newDbName_, getNewTbl()));
    }
    analyzer.addAccessEvent(new TAccessEvent(newDbName_ + "." + newTableName_.getTbl(),
        table_.getCatalogObjectType(), Privilege.CREATE.toString()));
  }
}
