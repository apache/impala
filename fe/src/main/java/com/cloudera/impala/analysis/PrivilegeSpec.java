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
import com.cloudera.impala.catalog.RolePrivilege;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TPrivilege;
import com.cloudera.impala.thrift.TPrivilegeLevel;
import com.cloudera.impala.thrift.TPrivilegeScope;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Represents a privilegeSpec from a GRANT/REVOKE statement.
 */
public class PrivilegeSpec implements ParseNode {
  private final TPrivilegeScope scope_;
  private final TPrivilegeLevel privilegeLevel_;
  private final TableName tableName_;
  private final HdfsUri uri_;

  // Set/modified during analysis
  private String dbName_;
  private String serverName_;

  private PrivilegeSpec(TPrivilegeLevel privilegeLevel, TPrivilegeScope scope,
      String dbName, TableName tableName, HdfsUri uri) {
    Preconditions.checkNotNull(scope);
    Preconditions.checkNotNull(privilegeLevel);
    privilegeLevel_ = privilegeLevel;
    scope_ = scope;
    tableName_ = tableName;
    dbName_ = (tableName_ != null ? tableName_.getDb() : dbName);
    uri_ = uri;
  }

  public static PrivilegeSpec createServerScopedPriv(TPrivilegeLevel privilegeLevel) {
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.SERVER, null, null, null);
  }

  public static PrivilegeSpec createDbScopedPriv(TPrivilegeLevel privilegeLevel,
      String dbName) {
    Preconditions.checkNotNull(dbName);
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.DATABASE, dbName, null,
        null);
  }

  public static PrivilegeSpec createTableScopedPriv(TPrivilegeLevel privilegeLevel,
      TableName tableName) {
    Preconditions.checkNotNull(tableName);
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.TABLE, null, tableName,
        null);
  }

  public static PrivilegeSpec createUriScopedPriv(TPrivilegeLevel privilegeLevel,
      HdfsUri uri) {
    Preconditions.checkNotNull(uri);
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.URI, null, null, uri);
  }

  public TPrivilege toThrift() {
    TPrivilege privilege = new TPrivilege();
    privilege.setScope(scope_);
    privilege.setServer_name(serverName_);
    // We don't currently filter on privilege level, so set it to an arbitrary value.
    privilege.setPrivilege_level(privilegeLevel_);
    if (dbName_ != null) privilege.setDb_name(dbName_);
    if (tableName_ != null) privilege.setTable_name(tableName_.getTbl());
    if (uri_ != null) privilege.setUri(uri_.toString());
    privilege.setPrivilege_name(
        RolePrivilege.buildRolePrivilegeName(privilege));
    privilege.setCreate_time_ms(-1);
    return privilege;
  }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder(privilegeLevel_.toString());
    sb.append(" ON ");
    sb.append(scope_.toString());
    if (scope_ == TPrivilegeScope.DATABASE) {
      sb.append(" " + dbName_);
    } else if (scope_ == TPrivilegeScope.TABLE) {
      sb.append(" " + tableName_.toString());
    } else if (scope_ == TPrivilegeScope.URI) {
      sb.append(" '" + uri_.getLocation() + "'");
    }
    return sb.toString();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    serverName_ = analyzer.getAuthzConfig().getServerName();
    Preconditions.checkState(!Strings.isNullOrEmpty(serverName_));

    if (scope_ != null) {
      switch (scope_) {
        case SERVER:
          if (privilegeLevel_ != TPrivilegeLevel.ALL) {
            throw new AnalysisException("Only 'ALL' privilege may be applied at " +
                "SERVER scope in privilege spec.");
          }
          break;
        case DATABASE:
          if (Strings.isNullOrEmpty(dbName_)) {
            throw new AnalysisException("Database name in privilege spec cannot " +
                "be empty");
          }
          break;
        case URI:
          if (privilegeLevel_ != TPrivilegeLevel.ALL) {
            throw new AnalysisException("Only 'ALL' privilege may be applied at " +
                "URI scope in privilege spec.");
          }
          uri_.analyze(analyzer, Privilege.ALL, false);
          break;
        case TABLE:
          if (Strings.isNullOrEmpty(tableName_.getTbl())) {
            throw new AnalysisException("Table name in privilege spec cannot be " +
                "empty");
          }
          dbName_ = analyzer.getTargetDbName(tableName_);
          Preconditions.checkNotNull(dbName_);
          break;
        default:
          throw new IllegalStateException("Unknown TPrivilegeScope in privilege spec: " +
              scope_.toString());
      }
    }
  }
}