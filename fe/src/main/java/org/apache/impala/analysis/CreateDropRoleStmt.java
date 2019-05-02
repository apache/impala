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

import org.apache.impala.catalog.Role;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TCreateDropRoleParams;

import com.google.common.base.Preconditions;

/**
 * Represents a "CREATE ROLE" or "DROP ROLE" statement.
 */
public class CreateDropRoleStmt extends AuthorizationStmt {
  private final String roleName_;
  private final boolean isDropRole_;

  public CreateDropRoleStmt(String roleName, boolean isDropRole) {
    Preconditions.checkNotNull(roleName);
    roleName_ = roleName;
    isDropRole_ = isDropRole;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    return String.format("%s ROLE %s", isDropRole_ ? "DROP" : "CREATE", roleName_);
  }

  public TCreateDropRoleParams toThrift() {
    TCreateDropRoleParams params = new TCreateDropRoleParams();
    params.setRole_name(roleName_);
    params.setIs_drop(isDropRole_);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
  }
}