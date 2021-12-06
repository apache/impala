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
import org.apache.impala.catalog.Principal;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TShowGrantPrincipalParams;

import java.util.HashSet;
import java.util.List;

/**
 * Represents "SHOW GRANT ROLE <role>" [ON <privilegeSpec>]" and
 * "SHOW GRANT USER <user> [ON <privilegeSpec>]" statements.
 */
public class ShowGrantPrincipalStmt extends AuthorizationStmt {
  private final PrivilegeSpec privilegeSpec_;
  private final String name_;
  private final TPrincipalType principalType_;

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

    if (privilegeSpec_ != null) privilegeSpec_.analyze(analyzer);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder(String.format("SHOW GRANT %s ",
        Principal.toString(principalType_).toUpperCase()));
    sb.append(name_);
    if (privilegeSpec_ != null) {
      sb.append(String.format(" ON %s", privilegeSpec_.getScope().name()));
      switch (privilegeSpec_.getScope()) {
        case SERVER:
          break;
        case DATABASE:
          sb.append(String.format(" %s", privilegeSpec_.getDbName()));
          break;
        case TABLE:
          sb.append(String.format(" %s", privilegeSpec_.getTableName()));
          break;
        case URI:
          sb.append(String.format(" '%s'", privilegeSpec_.getUri()));
          break;
        case STORAGE_TYPE:
          sb.append(String.format(" '%s'", privilegeSpec_.getStorageType()));
          break;
        case STORAGEHANDLER_URI:
          sb.append(String.format(" '%s'", privilegeSpec_.getStorageUri()));
          break;
        default:
          throw new IllegalStateException("Unexpected privilege spec scope: " +
              privilegeSpec_.getScope());
      }
    }
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
    }
    return params;
  }
}
