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

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableRenameParams;
import com.cloudera.impala.thrift.TAlterTableType;
import com.cloudera.impala.thrift.TTableName;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.google.common.base.Preconditions;

/**
 * Represents an ALTER TABLE RENAME <table> statement.
 */
public class AlterTableRenameStmt extends AlterTableStmt {
  private final TableName newTableName;

  // Set during analysis
  private String dbName;

  public AlterTableRenameStmt(TableName oldTableName, TableName newTableName) {
    super(oldTableName);
    Preconditions.checkState(newTableName != null && !newTableName.isEmpty());
    this.newTableName = newTableName;
  }

  public String getNewTbl() {
    return newTableName.getTbl();
  }

  public String getNewDb() {
    Preconditions.checkNotNull(dbName);
    return dbName;
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.RENAME_TABLE);
    TAlterTableRenameParams renameParams =
        new TAlterTableRenameParams(new TTableName(getNewDb(), getNewTbl()));
    params.setRename_params(renameParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    // If new new table name was not fully qualified, use the current default database.
    dbName = 
        newTableName.isFullyQualified() ? newTableName.getDb() : analyzer.getDefaultDb();

    if (analyzer.getCatalog().getDb(dbName) == null) {
      throw new AnalysisException("Unknown database: " + dbName);
    }

    if (analyzer.getCatalog().containsTable(dbName, getNewTbl())) {
      throw new AnalysisException(String.format("Table already exists: %s.%s",
          dbName, getNewTbl())); 
    }
  }
}
