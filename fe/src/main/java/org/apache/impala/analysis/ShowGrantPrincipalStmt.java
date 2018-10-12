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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.impala.catalog.Principal;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TShowGrantPrincipalParams;

import java.util.List;

/**
 * Represents "SHOW GRANT ROLE <role>" [ON <privilegeSpec>]" and
 * "SHOW GRANT USER <user> [ON <privilegeSpec>]" statements.
 */
public class ShowGrantPrincipalStmt extends AuthorizationStmt {
  private final PrivilegeSpec privilegeSpec_;
  private final String name_;
  private final TPrincipalType principalType_;

  // Set/modified during analysis.
  private Principal principal_;

  public ShowGrantPrincipalStmt(String name, TPrincipalType principalType,
      PrivilegeSpec privilegeSpec) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(principalType);
    name_ = name;
    principalType_ = principalType;
    privilegeSpec_ = privilegeSpec;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    if (Strings.isNullOrEmpty(name_)) {
      throw new AnalysisException(String.format("%s name in SHOW GRANT %s cannot be " +
          "empty.", Principal.toString(principalType_),
          Principal.toString(principalType_).toUpperCase()));
    }
    principal_ = analyzer.getCatalog().getAuthPolicy().getPrincipal(name_,
        principalType_);

    // If it's a role, we can determine if it doesn't exist here. For a user
    // it's considered non-existent if it doesn't have any groups, but we cannot
    // access the group information from analysis. The group check for a user
    // is done in AuthorizationPolicy.getPrincipalPrivileges
    if (principal_ == null) {
      switch (principalType_) {
        case ROLE:
          throw new AnalysisException(String.format("%s '%s' does not exist.",
              Principal.toString(principalType_), name_));
        case USER:
          // Create a user object here because it's possible the user does not exist in
          // Sentry, but still exists according to the OS, or Hadoop, or other custom
          // group mapping provider.
          principal_ = Principal.newInstance(name_, principalType_, Sets.newHashSet());
          break;
        default:
          throw new AnalysisException(String.format("Unexpected TPrincipalType: %s",
              Principal.toString(principalType_)));
      }
    }
    if (privilegeSpec_ != null) privilegeSpec_.analyze(analyzer);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder(String.format("SHOW GRANT %s ",
        Principal.toString(principalType_).toUpperCase()));
    sb.append(name_);
    if (privilegeSpec_ != null) sb.append(" " + privilegeSpec_.toSql(options));
    return sb.toString();
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    if (privilegeSpec_ != null) privilegeSpec_.collectTableRefs(tblRefs);
  }

  public TShowGrantPrincipalParams toThrift() throws InternalException {
    TShowGrantPrincipalParams params = new TShowGrantPrincipalParams();
    params.setName(name_);
    params.setPrincipal_type(principalType_);
    params.setRequesting_user(requestingUser_.getShortName());
    if (privilegeSpec_ != null) {
      params.setPrivilege(privilegeSpec_.toThrift().get(0));
      params.getPrivilege().setPrincipal_id(principal_.getId());
    }
    return params;
  }

  public Principal getPrincipal() { return principal_; }
}
