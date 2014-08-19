// Copyright 2014 Cloudera Inc.
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

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TShowRolesParams;
import com.google.common.base.Preconditions;

/**
 * Represents a "SHOW ROLES"/"SHOW ROLE GRANT GROUP" statement.
 */
public class ShowRolesStmt extends AuthorizationStmt {
  // If null, all roles will be shown. Otherwise only roles granted to this
  // group will be shown.
  private final String groupName_;

  public ShowRolesStmt(String groupName) {
    // An empty group name should never be possible since group name is an identifier
    // and Impala does not allow empty identifiers.
    Preconditions.checkState(groupName == null || !groupName.isEmpty());
    groupName_ = groupName;
  }

  @Override
  public String toSql() {
    if (groupName_ != null) {
      return "SHOW ROLES";
    } else {
      return "SHOW ROLE GRANT GROUP " + groupName_;
    }
  }

  public TShowRolesParams toThrift() {
    TShowRolesParams params = new TShowRolesParams();
    if (groupName_ != null) params.setGrant_group(groupName_);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    // TODO: Verify whether user has privileges to execute SHOW ROLES.
  }
}