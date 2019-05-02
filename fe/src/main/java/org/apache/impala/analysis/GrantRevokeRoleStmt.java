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

import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TGrantRevokeRoleParams;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Represents a "GRANT/REVOKE ROLE" statement.
 */
public class GrantRevokeRoleStmt extends AuthorizationStmt {
  private final String roleName_;
  private final String groupName_;
  private final boolean isGrantStmt_;

  public GrantRevokeRoleStmt(String roleName, String groupName, boolean isGrantStmt) {
    Preconditions.checkNotNull(roleName);
    Preconditions.checkNotNull(groupName);
    roleName_ = roleName;
    groupName_ = groupName;
    isGrantStmt_ = isGrantStmt;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    return String.format("%s ROLE %s %s GROUP %s",
        isGrantStmt_ ? "GRANT" : "REVOKE", roleName_,
        isGrantStmt_ ? "TO" : "FROM", groupName_);
  }

  public TGrantRevokeRoleParams toThrift() {
    TGrantRevokeRoleParams params = new TGrantRevokeRoleParams();
    params.setRole_names(Lists.newArrayList(roleName_));
    params.setGroup_names(Lists.newArrayList(groupName_));
    params.setIs_grant(isGrantStmt_);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    if (Strings.isNullOrEmpty(roleName_)) {
      throw new AnalysisException("Role name in GRANT/REVOKE ROLE cannot be empty.");
    }
    if (Strings.isNullOrEmpty(groupName_)) {
      throw new AnalysisException("Group name in GRANT/REVOKE ROLE cannot be empty.");
    }
  }
}