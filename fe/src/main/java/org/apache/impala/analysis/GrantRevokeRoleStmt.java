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
  private final String userName_;
  private final boolean isGrantStmt_;
  private final boolean principalIsGroup_;

  public GrantRevokeRoleStmt(String roleName, String groupName,
      String userName, boolean isGrantStmt) {
    Preconditions.checkNotNull(roleName);
    Preconditions.checkArgument((groupName != null && userName == null) ||
        (groupName == null && userName != null));
    roleName_ = roleName;
    groupName_ = groupName;
    userName_ = userName;
    isGrantStmt_ = isGrantStmt;
    // If 'userName_' is null, then the grantee/revokee is a group.
    principalIsGroup_ = (userName_ == null);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    return String.format("%s ROLE %s %s %s %s",
        isGrantStmt_ ? "GRANT" : "REVOKE", roleName_,
        isGrantStmt_ ? "TO" : "FROM",
        principalIsGroup_ ? "GROUP" : "USER",
        principalIsGroup_ ? groupName_ : userName_);
  }

  public TGrantRevokeRoleParams toThrift() {
    TGrantRevokeRoleParams params = new TGrantRevokeRoleParams();
    params.setRole_names(Lists.newArrayList(roleName_));
    if (principalIsGroup_) {
      params.setGroup_names(Lists.newArrayList(groupName_));
      params.setUser_names(Lists.newArrayList());
    } else {
      params.setGroup_names(Lists.newArrayList());
      params.setUser_names(Lists.newArrayList(userName_));
    }
    params.setIs_grant(isGrantStmt_);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    if (Strings.isNullOrEmpty(roleName_)) {
      throw new AnalysisException("Role name in GRANT/REVOKE ROLE cannot be empty.");
    }
    if (principalIsGroup_ && Strings.isNullOrEmpty(groupName_)) {
      throw new AnalysisException(String.format("Group name cannot be empty in " +
              "%s ROLE %s GROUP.", isGrantStmt_ ? "GRANT" : "REVOKE",
          isGrantStmt_ ? "TO" : "FROM"));
    }
    if (!principalIsGroup_ && Strings.isNullOrEmpty(userName_)) {
      throw new AnalysisException(String.format("User name cannot be empty in " +
              "%s ROLE %s USER.", isGrantStmt_ ? "GRANT" : "REVOKE",
          isGrantStmt_ ? "TO" : "FROM"));
    }
  }

  @Override
  public boolean requiresHmsMetadata() { return false; }
}