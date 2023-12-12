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

import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TGrantRevokePrivParams;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TPrivilege;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Represents a "GRANT/REVOKE PRIVILEGE" statement.
 * All privilege checks on catalog objects are skipped when executing
 * GRANT/REVOKE statements. This is because we need to be able to create
 * privileges on an object before any privileges actually exist.
 * The GRANT/REVOKE statement itself will be authorized by the loaded
 * authorization provider.
 */
public class GrantRevokePrivStmt extends AuthorizationStmt {
  private final PrivilegeSpec privilegeSpec_;
  private final String principalName_;
  private final boolean isGrantPrivStmt_;
  private final boolean hasGrantOpt_;
  private final TPrincipalType principalType_;

  public GrantRevokePrivStmt(String roleName, PrivilegeSpec privilegeSpec,
      boolean isGrantPrivStmt, boolean hasGrantOpt, TPrincipalType principalType) {
    Preconditions.checkNotNull(privilegeSpec);
    Preconditions.checkNotNull(roleName);
    privilegeSpec_ = privilegeSpec;
    principalName_ = roleName;
    isGrantPrivStmt_ = isGrantPrivStmt;
    hasGrantOpt_ = hasGrantOpt;
    principalType_ = principalType;
  }

  public TGrantRevokePrivParams toThrift() {
    TGrantRevokePrivParams params = new TGrantRevokePrivParams();
    params.setPrincipal_name(principalName_);
    params.setIs_grant(isGrantPrivStmt_);
    List<TPrivilege> privileges = privilegeSpec_.toThrift();
    for (TPrivilege privilege: privileges) {
      privilege.setPrincipal_type(principalType_);
      privilege.setHas_grant_opt(hasGrantOpt_);
    }
    params.setPrincipal_type(principalType_);
    params.setHas_grant_opt(hasGrantOpt_);
    params.setPrivileges(privileges);
    String ownerName = privilegeSpec_.getOwnerName();
    if (ownerName != null) {
      params.setOwner_name(ownerName);
    }
    return params;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder(isGrantPrivStmt_ ? "GRANT " : "REVOKE ");
    if (!isGrantPrivStmt_ && hasGrantOpt_) sb.append("GRANT OPTION FOR ");
    sb.append(privilegeSpec_.toSql(options));
    sb.append(isGrantPrivStmt_ ? " TO " : " FROM ");
    sb.append(principalType_);
    sb.append(" ");
    sb.append(principalName_);
    if (isGrantPrivStmt_ && hasGrantOpt_) sb.append(" WITH GRANT OPTION");
    return sb.toString();
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    if (privilegeSpec_ != null) privilegeSpec_.collectTableRefs(tblRefs);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    if (Strings.isNullOrEmpty(principalName_)) {
      throw new AnalysisException("Principal name in GRANT/REVOKE privilege cannot be " +
          "empty.");
    }

    privilegeSpec_.analyze(analyzer);
  }
}
