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
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.authorization.PrivilegeRequestBuilder;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TTableName;

import com.google.common.base.Preconditions;

/**
 * Representation of a REFRESH/INVALIDATE METADATA statement.
 */
public class ResetMetadataStmt extends StatementBase {
  // Updated during analysis. Null if invalidating the entire catalog.
  private TableName tableName_;

  // true if it is a REFRESH statement.
  private final boolean isRefresh_;

  // not null when refreshing a single partition
  private final PartitionSpec partitionSpec_;

  public ResetMetadataStmt(TableName name, boolean isRefresh,
      PartitionSpec partitionSpec) {
    Preconditions.checkArgument(!isRefresh || name != null);
    Preconditions.checkArgument(isRefresh || partitionSpec == null);
    this.tableName_ = name;
    this.isRefresh_ = isRefresh;
    this.partitionSpec_ = partitionSpec;
    if (partitionSpec_ != null) partitionSpec_.setTableName(tableName_);
  }

  public TableName getTableName() { return tableName_; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (tableName_ != null) {
      String dbName = analyzer.getTargetDbName(tableName_);
      tableName_ = new TableName(dbName, tableName_.getTbl());

      if (isRefresh_) {
        // Verify the user has privileges to access this table. Will throw if the parent
        // database does not exists. Don't call getTable() to avoid loading the table
        // metadata if it is not yet in this impalad's catalog cache.
        if (!analyzer.dbContainsTable(dbName, tableName_.getTbl(), Privilege.ANY)) {
          // Only throw an exception when the table does not exist for refresh statements
          // since 'invalidate metadata' should add/remove tables created/dropped external
          // to Impala.
          throw new AnalysisException(Analyzer.TBL_DOES_NOT_EXIST_ERROR_MSG + tableName_);
        }
        if (partitionSpec_ != null) {
          partitionSpec_.setPrivilegeRequirement(Privilege.ANY);
          partitionSpec_.analyze(analyzer);
        }
      } else {
        // Verify the user has privileges to access this table.
        analyzer.registerPrivReq(new PrivilegeRequestBuilder()
            .onTable(dbName, tableName_.getTbl()).any().toRequest());
      }
    } else {
      analyzer.registerPrivReq(new PrivilegeRequest(Privilege.ALL));
    }
  }

  @Override
  public String toSql() {
    StringBuilder result = new StringBuilder();
    if (isRefresh_) {
      result.append("INVALIDATE METADATA");
    } else {
      result.append("REFRESH");
    }

    if (tableName_ != null) result.append(" ").append(tableName_);
    if (partitionSpec_ != null) result.append(" " + partitionSpec_.toSql());
    return result.toString();
  }

  public TResetMetadataRequest toThrift() {
    TResetMetadataRequest  params = new TResetMetadataRequest();
    params.setIs_refresh(isRefresh_);
    if (tableName_ != null) {
      params.setTable_name(new TTableName(tableName_.getDb(), tableName_.getTbl()));
    }
    if (partitionSpec_ != null) {
      params.setPartition_spec(partitionSpec_.toThrift());
    }
    return params;
  }
}
