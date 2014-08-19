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

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.Role;
import com.cloudera.impala.catalog.RolePrivilege;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TGrantRevokePrivParams;
import com.cloudera.impala.thrift.TPrivilege;
import com.cloudera.impala.thrift.TPrivilegeLevel;
import com.cloudera.impala.thrift.TPrivilegeScope;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Represents a "GRANT/REVOKE PRIVILEGE" statement.
 * All privilege checks on catalog objects are skipped when executing
 * GRANT/REVOKE statements. This is because we need to be able to create
 * privileges on an object before any privileges actually exist.
 * The GRANT/REVOKE statement itself will be authorized (currently by
 * the Sentry Service).
 */
public class GrantRevokePrivStmt extends AuthorizationStmt {
  private final TPrivilegeLevel privilegeLevel_;
  private final TPrivilegeScope scope_;
  private final String roleName_;
  private final TableName tableName_;
  private final HdfsUri uri_;
  private final boolean isGrantPrivStmt_;

  // Set/modified during analysis
  private String dbName_;
  private String serverName_;
  private Role role_;

  private GrantRevokePrivStmt(TPrivilegeLevel privilegeLevel, TPrivilegeScope scope,
      String roleName, boolean isGrantPrivStmt, String dbName, TableName tableName,
      HdfsUri uri) {
    Preconditions.checkNotNull(privilegeLevel);
    Preconditions.checkNotNull(scope);
    Preconditions.checkNotNull(roleName);
    privilegeLevel_ = privilegeLevel;
    scope_ = scope;
    roleName_ = roleName;
    isGrantPrivStmt_ = isGrantPrivStmt;
    tableName_ = tableName;
    dbName_ = (tableName_ != null ? tableName_.getDb() : dbName);
    uri_ = uri;
  }

  public static GrantRevokePrivStmt createServerScopedStmt(
      TPrivilegeLevel privilegeLevel, String roleName, boolean isGrantPrivStmt) {
    return new GrantRevokePrivStmt(privilegeLevel, TPrivilegeScope.SERVER, roleName,
        isGrantPrivStmt, null, null, null);
  }

  public static GrantRevokePrivStmt createDbScopedStmt(TPrivilegeLevel privilegeLevel,
      String roleName, boolean isGrantPrivStmt, String dbName) {
    return new GrantRevokePrivStmt(privilegeLevel, TPrivilegeScope.DATABASE, roleName,
        isGrantPrivStmt, dbName, null, null);
  }

  public static GrantRevokePrivStmt createTableScopedStmt(TPrivilegeLevel privilegeLevel,
      String roleName, boolean isGrantPrivStmt, TableName tableName) {
    Preconditions.checkNotNull(tableName);
    return new GrantRevokePrivStmt(privilegeLevel, TPrivilegeScope.TABLE, roleName,
        isGrantPrivStmt, null, tableName, null);
  }

  public static GrantRevokePrivStmt createUriScopedStmt(TPrivilegeLevel privilegeLevel,
      String roleName, boolean isGrantPrivStmt, HdfsUri uri) {
    return new GrantRevokePrivStmt(privilegeLevel, TPrivilegeScope.URI, roleName,
        isGrantPrivStmt, null, null, uri);
  }

  public TGrantRevokePrivParams toThrift() {
    TGrantRevokePrivParams params = new TGrantRevokePrivParams();
    params.setRole_name(roleName_);
    TPrivilege privilege = new TPrivilege();
    privilege.setScope(scope_);
    privilege.setRole_id(role_.getId());
    privilege.setServer_name(serverName_);
    privilege.setPrivilege_level(privilegeLevel_);
    if (dbName_ != null) privilege.setDb_name(dbName_);
    if (tableName_ != null) privilege.setTable_name(tableName_.getTbl());
    if (uri_ != null) privilege.setUri(uri_.toString());
    params.setIs_grant(isGrantPrivStmt_);
    params.setPrivileges(Lists.newArrayList(privilege));
    privilege.setPrivilege_name(
        RolePrivilege.buildRolePrivilegeName(privilege));
    return params;
  }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder(isGrantPrivStmt_ ? "GRANT " : "REVOKE ");
    sb.append(privilegeLevel_.toString());
    sb.append(" ON ");
    sb.append(scope_.toString() + " ");
    if (scope_ == TPrivilegeScope.DATABASE) {
      sb.append(dbName_ + " ");
    } else if (scope_ == TPrivilegeScope.TABLE) {
      sb.append(tableName_.toString() + " ");
    } else if (scope_ == TPrivilegeScope.URI) {
      sb.append("'" + uri_.getLocation() + "' ");
    }
    sb.append(isGrantPrivStmt_ ? "TO " : "FROM ");
    sb.append(roleName_);
    return sb.toString();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    if (Strings.isNullOrEmpty(roleName_)) {
      throw new AnalysisException("Role name in GRANT/REVOKE privilege cannot be " +
          "empty.");
    }
    role_ = analyzer.getCatalog().getAuthPolicy().getRole(roleName_);
    if (role_ == null) {
      throw new AnalysisException(String.format("Role '%s' does not exist.", roleName_));
    }
    serverName_ = analyzer.getAuthzConfig().getServerName();
    Preconditions.checkState(!Strings.isNullOrEmpty(serverName_));

    switch (scope_) {
      case SERVER:
        if (privilegeLevel_ != TPrivilegeLevel.ALL) {
          throw new AnalysisException("Only 'ALL' privilege may be GRANTED/REVOKED " +
              "to/from a SERVER.");
        }
        break;
      case DATABASE:
        if (Strings.isNullOrEmpty(dbName_)) {
          throw new AnalysisException("Database name in GRANT/REVOKE privilege cannot " +
              "be empty");
        }
        break;
      case URI:
        if (privilegeLevel_ != TPrivilegeLevel.ALL) {
          throw new AnalysisException("Only 'ALL' privilege may be GRANTED/REVOKED " +
              "to/from a URI.");
        }
        uri_.analyze(analyzer, Privilege.ALL, false);
        break;
      case TABLE:
        if (Strings.isNullOrEmpty(tableName_.getTbl())) {
          throw new AnalysisException("Table name in GRANT/REVOKE privilege cannot be " +
              "empty");
        }
        dbName_ = analyzer.getTargetDbName(tableName_);
        Preconditions.checkNotNull(dbName_);
        break;
      default:
        throw new IllegalStateException("Unknown SCOPE in GRANT/REVOKE privilege " +
            "statement: " + scope_.toString());
    }
  }
}
