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
import com.cloudera.impala.authorization.PrivilegeRequest;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TResetMetadataRequest;
import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Preconditions;

/**
 * Representation of a REFRESH/INVALIDATE METADATA statement.
 */
public class ResetMetadataStmt extends StatementBase {
  // Updated during analysis. Null if invalidating the entire catalog.
  private TableName tableName;

  // true if it is a REFRESH statement.
  private final boolean isRefresh;

  public ResetMetadataStmt(TableName name, boolean isRefresh) {
    Preconditions.checkArgument(!isRefresh || name != null);
    this.tableName = name;
    this.isRefresh = isRefresh;
  }

  public TableName getTableName() { return tableName; }
  public boolean isRefresh() { return isRefresh; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (tableName != null) {
      String dbName = analyzer.getTargetDbName(tableName);
      tableName = new TableName(dbName, tableName.getTbl());
      if (!analyzer.dbContainsTable(dbName, tableName.getTbl(), Privilege.ANY)) {
        throw new AnalysisException(Analyzer.TBL_DOES_NOT_EXIST_ERROR_MSG + tableName);
      }
    } else {
      PrivilegeRequest privilegeRequest = new PrivilegeRequest(Privilege.ALL);
      analyzer.getCatalog().checkAccess(analyzer.getUser(), privilegeRequest);
    }
  }

  @Override
  public String toSql() {
    StringBuilder result = new StringBuilder();
    if (isRefresh) {
      result.append("INVALIDATE METADATA");
    } else {
      result.append("REFRESH");
    }

    if (tableName != null) result.append(" ").append(tableName);
    return result.toString();
  }

  public TResetMetadataRequest toThrift() {
    TResetMetadataRequest  params = new TResetMetadataRequest();
    params.setIs_refresh(isRefresh);
    if (tableName != null) {
      params.setTable_name(new TTableName(tableName.getDb(), tableName.getTbl()));
    }
    return params;
  }
}
