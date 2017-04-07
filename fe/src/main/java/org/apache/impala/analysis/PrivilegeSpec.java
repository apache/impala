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

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.catalog.RolePrivilege;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.View;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TPrivilegeScope;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Represents a privilege spec from a GRANT/REVOKE statement.
 * A privilege spec may correspond to one or more privileges. Currently, a privilege spec
 * can represent multiple privileges only at the COLUMN scope.
 */
public class PrivilegeSpec implements ParseNode {
  private final TPrivilegeScope scope_;
  private final TPrivilegeLevel privilegeLevel_;
  private final TableName tableName_;
  private final HdfsUri uri_;
  private final List<String> columnNames_;

  // Set/modified during analysis
  private String dbName_;
  private String serverName_;

  private PrivilegeSpec(TPrivilegeLevel privilegeLevel, TPrivilegeScope scope,
      String serverName, String dbName, TableName tableName, HdfsUri uri,
      List<String> columnNames) {
    Preconditions.checkNotNull(scope);
    Preconditions.checkNotNull(privilegeLevel);
    privilegeLevel_ = privilegeLevel;
    scope_ = scope;
    serverName_ = serverName;
    tableName_ = tableName;
    dbName_ = (tableName_ != null ? tableName_.getDb() : dbName);
    uri_ = uri;
    columnNames_ = columnNames;
  }

  public static PrivilegeSpec createServerScopedPriv(TPrivilegeLevel privilegeLevel) {
    return createServerScopedPriv(privilegeLevel, null);
  }

  public static PrivilegeSpec createServerScopedPriv(TPrivilegeLevel privilegeLevel,
      String serverName) {
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.SERVER, serverName, null,
        null, null, null);
  }

  public static PrivilegeSpec createDbScopedPriv(TPrivilegeLevel privilegeLevel,
      String dbName) {
    Preconditions.checkNotNull(dbName);
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.DATABASE, null, dbName,
        null, null, null);
  }

  public static PrivilegeSpec createTableScopedPriv(TPrivilegeLevel privilegeLevel,
      TableName tableName) {
    Preconditions.checkNotNull(tableName);
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.TABLE, null, null, tableName,
        null, null);
  }

  public static PrivilegeSpec createColumnScopedPriv(TPrivilegeLevel privilegeLevel,
        TableName tableName, List<String> columnNames) {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(columnNames);
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.COLUMN, null, null,
        tableName, null, columnNames);
  }

  public static PrivilegeSpec createUriScopedPriv(TPrivilegeLevel privilegeLevel,
      HdfsUri uri) {
    Preconditions.checkNotNull(uri);
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.URI, null, null, null, uri,
        null);
  }

  public List<TPrivilege> toThrift() {
    List<TPrivilege> privileges = Lists.newArrayList();
    if (scope_ == TPrivilegeScope.COLUMN) {
      // Create a TPrivilege for every referenced column
      for (String column: columnNames_) {
        privileges.add(createTPrivilege(column));
      }
    } else {
      privileges.add(createTPrivilege(null));
    }
    return privileges;
  }

  /**
   * Helper function to construct a TPrivilege from this privilege spec. If the scope is
   * COLUMN, 'columnName' must be a non-null column name. Otherwise, 'columnName' is
   * null.
   */
  private TPrivilege createTPrivilege(String columnName) {
    Preconditions.checkState(columnName == null ^ scope_ == TPrivilegeScope.COLUMN);
    TPrivilege privilege = new TPrivilege();
    privilege.setScope(scope_);
    privilege.setServer_name(serverName_);
    // We don't currently filter on privilege level, so set it to an arbitrary value.
    privilege.setPrivilege_level(privilegeLevel_);
    if (dbName_ != null) privilege.setDb_name(dbName_);
    if (tableName_ != null) privilege.setTable_name(tableName_.getTbl());
    if (uri_ != null) privilege.setUri(uri_.toString());
    if (columnName != null) privilege.setColumn_name(columnName);
    privilege.setCreate_time_ms(-1);
    privilege.setPrivilege_name(RolePrivilege.buildRolePrivilegeName(privilege));
    return privilege;
  }

  /**
   * Return the table path of a COLUMN level privilege. The table path consists
   * of server name, database name and table name.
   */
  public static String getTablePath(TPrivilege privilege) {
    Preconditions.checkState(privilege.getScope() == TPrivilegeScope.COLUMN);
    Joiner joiner = Joiner.on(".");
    return joiner.join(privilege.getServer_name(), privilege.getDb_name(),
        privilege.getTable_name());
  }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder(privilegeLevel_.toString());
    sb.append(" ON ");
    sb.append(scope_.toString());
    if (scope_ == TPrivilegeScope.SERVER && serverName_ != null) {
      sb.append(" " + serverName_);
    } else if (scope_ == TPrivilegeScope.DATABASE) {
      sb.append(" " + dbName_);
    } else if (scope_ == TPrivilegeScope.TABLE) {
      sb.append(" " + tableName_.toString());
    } else if (scope_ == TPrivilegeScope.COLUMN) {
      sb.append("(");
      sb.append(Joiner.on(",").join(columnNames_));
      sb.append(")");
      sb.append(" " + tableName_.toString());
    } else if (scope_ == TPrivilegeScope.URI) {
      sb.append(" '" + uri_.getLocation() + "'");
    }
    return sb.toString();
  }

  public void collectTableRefs(List<TableRef> tblRefs) {
    if (tableName_ != null) tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    String configServerName = analyzer.getAuthzConfig().getServerName();
    if (serverName_ != null && !serverName_.equals(configServerName)) {
      throw new AnalysisException(String.format("Specified server name '%s' does not " +
          "match the configured server name '%s'", serverName_, configServerName));
    }
    serverName_ = configServerName;
    Preconditions.checkState(!Strings.isNullOrEmpty(serverName_));
    Preconditions.checkNotNull(scope_);

    switch (scope_) {
      case SERVER:
        if (privilegeLevel_ != TPrivilegeLevel.ALL) {
          throw new AnalysisException("Only 'ALL' privilege may be applied at " +
              "SERVER scope in privilege spec.");
        }
        break;
      case DATABASE:
        Preconditions.checkState(!Strings.isNullOrEmpty(dbName_));
        try {
          analyzer.getDb(dbName_, true);
        } catch (AnalysisException e) {
          throw new AnalysisException(String.format("Error setting privileges for " +
              "database '%s'. Verify that the database exists and that you have " +
              "permissions to issue a GRANT/REVOKE statement.", dbName_));
        }
        break;
      case URI:
        Preconditions.checkNotNull(uri_);
        if (privilegeLevel_ != TPrivilegeLevel.ALL) {
          throw new AnalysisException("Only 'ALL' privilege may be applied at " +
              "URI scope in privilege spec.");
        }
        uri_.analyze(analyzer, Privilege.ALL, false);
        break;
      case TABLE:
        analyzeTargetTable(analyzer);
        break;
      case COLUMN:
        analyzeColumnPrivScope(analyzer);
        break;
      default:
        throw new IllegalStateException("Unknown TPrivilegeScope in privilege spec: " +
            scope_.toString());
    }
  }

  /**
   * Analyzes a privilege spec at the COLUMN scope.
   * Throws an AnalysisException in the following cases:
   * 1. No columns are specified.
   * 2. Privilege is applied on a view or an external data source.
   * 3. Referenced table and/or columns do not exist.
   * 4. Privilege level is not SELECT.
   */
  private void analyzeColumnPrivScope(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(scope_ == TPrivilegeScope.COLUMN);
    Preconditions.checkNotNull(columnNames_);
    if (columnNames_.isEmpty()) {
      throw new AnalysisException("Empty column list in column privilege spec.");
    }
    if (privilegeLevel_ != TPrivilegeLevel.SELECT) {
      throw new AnalysisException("Only 'SELECT' privileges are allowed " +
          "in a column privilege spec.");
    }
    Table table = analyzeTargetTable(analyzer);
    if (table instanceof View) {
      throw new AnalysisException("Column-level privileges on views are not " +
          "supported.");
    }
    if (table instanceof DataSourceTable) {
      throw new AnalysisException("Column-level privileges on external data " +
          "source tables are not supported.");
    }
    for (String columnName: columnNames_) {
      if (table.getColumn(columnName) == null) {
        // The error message should not reveal the existence or absence of a column.
        throw new AnalysisException(String.format("Error setting column-level " +
            "privileges for table '%s'. Verify that both table and columns exist " +
            "and that you have permissions to issue a GRANT/REVOKE statement.",
            tableName_.toString()));
      }
    }
  }

  /**
   * Verifies that the table referenced in the privilege spec exists in the catalog and
   * returns the catalog object.
   * Throws an AnalysisException in the following cases:
   * 1. The table name is not valid.
   * 2. Table is not loaded in the catalog.
   * 3. Table does not exist.
   */
  private Table analyzeTargetTable(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(scope_ == TPrivilegeScope.TABLE ||
        scope_ == TPrivilegeScope.COLUMN);
    Preconditions.checkState(!Strings.isNullOrEmpty(tableName_.getTbl()));
    Table table = null;
    try {
      dbName_ = analyzer.getTargetDbName(tableName_);
      Preconditions.checkNotNull(dbName_);
      table = analyzer.getTable(dbName_, tableName_.getTbl());
    } catch (TableLoadingException e) {
      throw new AnalysisException(e.getMessage(), e);
    } catch (AnalysisException e) {
      throw new AnalysisException(String.format("Error setting privileges for " +
          "table '%s'. Verify that the table exists and that you have permissions " +
          "to issue a GRANT/REVOKE statement.", tableName_.toString()));
    }
    Preconditions.checkNotNull(table);
    return table;
  }
}
