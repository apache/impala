// Copyright 2013 Cloudera Inc.
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
import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Preconditions;

/**
 * Representation of a SHOW CREATE TABLE statement which returns the "CREATE TABLE ..."
 * string that re-creates the table.
 *
 * Syntax: SHOW CREATE TABLE <table>
 */
public class ShowCreateTableStmt extends StatementBase {
  private TableName tableName_;

  public ShowCreateTableStmt(TableName table) {
    Preconditions.checkNotNull(table);
    this.tableName_ = table;
  }

  @Override
  public String toSql() { return "SHOW CREATE TABLE " + tableName_; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (!tableName_.isFullyQualified()) {
      tableName_ = new TableName(analyzer.getDefaultDb(), tableName_.getTbl());
    }
    Table table = analyzer.getTable(tableName_, Privilege.VIEW_METADATA);
    if (table instanceof View) {
      throw new AnalysisException("SHOW CREATE TABLE not supported on VIEW: " +
          tableName_.toString());
    }
  }

  public TTableName toThrift() {
    TTableName params = new TTableName();
    params.setTable_name(tableName_.getTbl());
    params.setDb_name(tableName_.getDb());
    return params;
  }
}
