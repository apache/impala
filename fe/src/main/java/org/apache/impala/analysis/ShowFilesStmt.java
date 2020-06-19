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

import java.util.List;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TShowFilesParams;
import org.apache.impala.thrift.TTableName;

import com.google.common.base.Preconditions;

/**
 * Representation of a SHOW FILES statement.
 * Acceptable syntax:
 *
 * SHOW FILES IN [dbName.]tableName [PARTITION(key=value,...)]
 *
 */
public class ShowFilesStmt extends StatementBase {
  private final TableName tableName_;

  // Show files for all the partitions if this is null.
  private final PartitionSet partitionSet_;

  // Set during analysis.
  protected FeTable table_;

  public ShowFilesStmt(TableName tableName, PartitionSet partitionSet) {
    tableName_ = Preconditions.checkNotNull(tableName);
    partitionSet_ = partitionSet;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append("SHOW FILES IN " + tableName_.toString());
    if (partitionSet_ != null) strBuilder.append(" " + partitionSet_.toSql(options));
    return strBuilder.toString();
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // Resolve and analyze table ref to register privilege and audit events
    // and to allow us to evaluate partition predicates.
    TableRef tableRef = new TableRef(tableName_.toPath(), null, Privilege.VIEW_METADATA);
    tableRef = analyzer.resolveTableRef(tableRef);
    if (tableRef instanceof InlineViewRef ||
        tableRef instanceof CollectionTableRef) {
      throw new AnalysisException(String.format(
          "SHOW FILES not applicable to a non hdfs table: %s", tableName_));
    }
    table_ = tableRef.getTable();
    Preconditions.checkNotNull(table_);
    if (!(table_ instanceof FeFsTable)) {
      throw new AnalysisException("SHOW FILES is applicable only to a HDFS table");
    }
    tableRef.analyze(analyzer);

    // Analyze the partition spec, if one was specified.
    if (partitionSet_ != null) {
      partitionSet_.setTableName(table_.getTableName());
      partitionSet_.setPartitionShouldExist();
      partitionSet_.setPrivilegeRequirement(Privilege.VIEW_METADATA);
      partitionSet_.analyze(analyzer);
    }
  }

  public TShowFilesParams toThrift() {
    TShowFilesParams params = new TShowFilesParams();
    params.setTable_name(new TTableName(table_.getDb().getName(), table_.getName()));
    if (partitionSet_ != null) {
      params.setPartition_set(partitionSet_.toThrift());
    }
    return params;
  }
}
