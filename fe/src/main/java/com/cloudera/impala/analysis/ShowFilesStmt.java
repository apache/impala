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

import java.util.List;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TShowFilesParams;
import com.cloudera.impala.thrift.TPartitionKeyValue;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Preconditions;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Representation of a SHOW FILES statement.
 * Acceptable syntax:
 *
 * SHOW FILES IN [dbName.]tableName [PARTITION(key=value,...)]
 *
 */
public class ShowFilesStmt extends StatementBase {
  private TableName tableName_;

  // Show files for all the partitions if this is null.
  private final PartitionSpec partitionSpec_;

  // Set during analysis.
  protected Table table_;

  public ShowFilesStmt(TableName tableName, PartitionSpec partitionSpec) {
    this.tableName_ = tableName;
    this.partitionSpec_ = partitionSpec;
  }

  @Override
  public String toSql() {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append("SHOW FILES IN " + tableName_.toString());
    if (partitionSpec_ != null) strBuilder.append(" " + partitionSpec_.toSql());
    return strBuilder.toString();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (!tableName_.isFullyQualified()) {
      tableName_ = new TableName(analyzer.getDefaultDb(), tableName_.getTbl());
    }
    table_ = analyzer.getTable(tableName_, Privilege.VIEW_METADATA);
    if (!(table_ instanceof HdfsTable)) {
      throw new AnalysisException(String.format(
          "SHOW FILES not applicable to a non hdfs table: %s", table_.getFullName()));
    }

    // Analyze the partition spec, if one was specified.
    if (partitionSpec_ != null) {
      partitionSpec_.setTableName(tableName_);
      partitionSpec_.setPartitionShouldExist();
      partitionSpec_.setPrivilegeRequirement(Privilege.VIEW_METADATA);
      partitionSpec_.analyze(analyzer);
    }
  }

  public TShowFilesParams toThrift() {
    TShowFilesParams params = new TShowFilesParams();
    params.setTable_name(new TTableName(tableName_.getDb(), tableName_.getTbl()));
    if (partitionSpec_ != null) {
      params.setPartition_spec(partitionSpec_.toThrift());
    }
    return params;
  }
}
