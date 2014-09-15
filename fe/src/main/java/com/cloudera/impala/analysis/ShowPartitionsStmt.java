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

import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

/**
 * Representation of a SHOW PARTITIONS statement for displaying
 * partition information on a given table.
 */
public class ShowPartitionsStmt extends ShowStatsStmt {

  public ShowPartitionsStmt(TableName tableName) {
    super(tableName, false);
  }

  @Override
  public String toSql() {
    return getSqlPrefix() + " " + tableName_.toString();
  }

  @Override
  protected String getSqlPrefix() { return "SHOW PARTITIONS"; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    Preconditions.checkNotNull(table_);
    if (!(table_ instanceof HdfsTable)) {
      throw new AnalysisException(getSqlPrefix() + " must target an HDFS table: " +
          table_.getFullName());
    }
    if (table_.getNumClusteringCols() == 0) {
      throw new AnalysisException(String.format(
          "Table is not partitioned: %s", table_.getFullName()));
    }
  }
}
