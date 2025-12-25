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

import org.apache.impala.authorization.User;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TShowRolesParams;
import com.google.common.base.Preconditions;

/**
 * Represents "SHOW [CURRENT] ROLES" and "SHOW ROLE GRANT GROUP <groupName>"
 * statements.
 */
public class ShowRolesStmt extends AuthorizationStmt {
  // If null, all roles will be shown. Otherwise only roles granted to this
  // group will be shown.
  private final String groupName_;
  private final String userName_;
  private final boolean isShowCurrentRoles_;

  // Set during analysis.
  private User requestingUser_;

  public ShowRolesStmt(boolean isShowCurrentRoles, String groupName, String userName) {
    // For SHOW CURRENT ROLES, 'groupName' and 'userName' should be null.
    Preconditions.checkState(!isShowCurrentRoles ||
        (groupName == null && userName == null));
    // 'groupName' and 'userName' should not both be set whether 'isShowCurrentRoles' is
    // true.
    Preconditions.checkState(groupName == null || userName == null);
    // An empty 'groupName' or 'userName' should never be possible since 'groupName' and
    // 'userName' should be an identifier and Impala does not allow empty identifiers.
    Preconditions.checkState(groupName == null || !groupName.isEmpty());
    Preconditions.checkState(userName == null || !userName.isEmpty());
    groupName_ = groupName;
    userName_ = userName;
    isShowCurrentRoles_ = isShowCurrentRoles;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    if (groupName_ == null && userName_ == null) {
      return isShowCurrentRoles_ ? "SHOW CURRENT ROLES" : "SHOW ROLES";
    } else {
      String granteeType = groupName_ != null ? "GROUP" : "USER";
      String granteeName = groupName_ != null ? groupName_ : userName_;
      return "SHOW ROLE GRANT " + granteeType + " " + granteeName;
    }
  }

  public TShowRolesParams toThrift() throws InternalException {
    TShowRolesParams params = new TShowRolesParams();
    params.setRequesting_user(requestingUser_.getShortName());
    params.setIs_show_current_roles(isShowCurrentRoles_);
    if (groupName_ != null) params.setGrant_group(groupName_);
    if (userName_ != null) params.setGrant_user(userName_);
    // Users should always be able to execute SHOW CURRENT ROLES.
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    requestingUser_ = analyzer.getUser();
  }

  @Override
  public boolean requiresHmsMetadata() { return false; }
}
