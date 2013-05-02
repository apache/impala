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
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TDescribeTableParams;

/**
 * Representation of a DESCRIBE table statement.
 */
public class DescribeStmt extends StatementBase {
  private TableName table;

  public DescribeStmt(TableName table) {
    this.table = table;
  }

  @Override
  public String toSql() {
    return "DESCRIBE " + table;
  }

  public TableName getTable() {
    return table;
  }

  @Override
  public String debugString() {
    return toSql() + table.toString();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (!table.isFullyQualified()) {
      table = new TableName(analyzer.getDefaultDb(), table.getTbl());
    }
    analyzer.getTable(table, Privilege.VIEW_METADATA);
  }

  public TDescribeTableParams toThrift() {
    TDescribeTableParams params = new TDescribeTableParams();
    params.setTable_name(getTable().getTbl());
    params.setDb(getTable().getDb());
    return params;
  }
}
