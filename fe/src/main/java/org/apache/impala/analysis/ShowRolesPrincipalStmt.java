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
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TShowRolesParams;
import com.google.common.base.Preconditions;

/**
 * Represents "SHOW ROLE GRANT GROUP <groupName>" and
 * "SHOW ROLE GRANT USER <userName>" statements.
 */
public class ShowRolesPrincipalStmt extends AuthorizationStmt {
  private final TPrincipalType principalType_;
  private final String name_;

  // Set during analysis.
  private User requestingUser_;

  public ShowRolesPrincipalStmt(TPrincipalType principalType, String name) {
    Preconditions.checkNotNull(principalType);
    Preconditions.checkArgument(principalType == TPrincipalType.USER ||
        principalType == TPrincipalType.GROUP);
    // The name should be an identifier and Impala does not allow empty identifiers.
    Preconditions.checkState(name != null && !name.isEmpty());
    principalType_ = principalType;
    name_ = name;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    return "SHOW ROLE GRANT " + principalType_.name() + " " + name_;
  }

  public TShowRolesParams toThrift() throws InternalException {
    TShowRolesParams params = new TShowRolesParams();
    params.setRequesting_user(requestingUser_.getShortName());
    params.setIs_show_current_roles(false);

    if (principalType_ == TPrincipalType.GROUP) {
      params.setGrant_group(name_);
    } else {
      params.setGrant_user(name_);
    }
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
