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
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TDescribeTableOutputStyle;
import com.cloudera.impala.thrift.TDescribeTableParams;

/**
 * Representation of a DESCRIBE table statement which returns metadata on
 * a specified table:
 * Syntax: DESCRIBE [FORMATTED] <table>
 *
 * If FORMATTED is not specified, the statement only returns info on the given table's
 * column definition (column name, data type, and comment).
 * If FORMATTED is specified, extended metadata on the table is returned
 * (in addition to the column definitions). This metadata includes info about the table
 * properties, SerDe properties, StorageDescriptor properties, and more.
 */
public class DescribeStmt extends StatementBase {
  private final TDescribeTableOutputStyle outputStyle_;
  private TableName tableName_;

  public DescribeStmt(TableName tableName, TDescribeTableOutputStyle outputStyle) {
    tableName_ = tableName;
    outputStyle_ = outputStyle;
  }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder("DESCRIBE ");
    if (outputStyle_ != TDescribeTableOutputStyle.MINIMAL) {
      sb.append(outputStyle_.toString());
    }
    return sb.toString() + tableName_;
  }

  public TableName getTableName() { return tableName_; }
  public TDescribeTableOutputStyle getOutputStyle() { return outputStyle_; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    tableName_ = analyzer.getFqTableName(tableName_);
    if (outputStyle_ == TDescribeTableOutputStyle.MINIMAL) {
      analyzer.getTable(tableName_, Privilege.ANY);
    } else {
      analyzer.getTable(tableName_, Privilege.VIEW_METADATA);
    }
  }

  public TDescribeTableParams toThrift() {
    TDescribeTableParams params = new TDescribeTableParams();
    params.setTable_name(getTableName().getTbl());
    params.setDb(getTableName().getDb());
    params.setOutput_style(outputStyle_);
    return params;
  }
}
