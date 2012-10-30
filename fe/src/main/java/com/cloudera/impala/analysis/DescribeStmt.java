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
import com.cloudera.impala.common.InternalException;

/**
 * Representation of a DESCRIBE table statement. 
 */
public class DescribeStmt extends ParseNodeBase {
  private TableName table;

  public DescribeStmt(TableName table) {
    this.table = table;
  }

  public String toSql() {
    return "DESCRIBE " + table; 
  }

  public TableName getTable() {
    return table;
  }

  public String debugString() {
    return toSql() + table.toString();
  }

  public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
    if (!table.isFullyQualified()) {
      table = new TableName(analyzer.getDefaultDb(), table.getTbl());
    }
  }
}
