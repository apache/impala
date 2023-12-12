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

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeDataSourceTable;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TPrivilegeScope;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Represents a privilege spec from a GRANT/REVOKE statement.
 * A privilege spec may correspond to one or more privileges. Currently, a privilege spec
 * can represent multiple privileges only at the COLUMN scope.
 */
public class PrivilegeSpec extends StmtNode {
  private final TPrivilegeScope scope_;
  private final TPrivilegeLevel privilegeLevel_;
  private final TableName tableName_;
  private final HdfsUri uri_;
  private final String storageType_;
  private final String storageUri_;
  private final List<String> columnNames_;
  private final FunctionName functionName_;

  // Set/modified during analysis
  private String dbName_;
  private String serverName_;
  private String ownerName_;

  private PrivilegeSpec(TPrivilegeLevel privilegeLevel, TPrivilegeScope scope,
      String serverName, String dbName, TableName tableName, HdfsUri uri,
      String storageType, String storageUri, List<String> columnNames,
      FunctionName functionName) {
    Preconditions.checkNotNull(scope);
    Preconditions.checkNotNull(privilegeLevel);
    privilegeLevel_ = privilegeLevel;
    scope_ = scope;
    serverName_ = serverName;
    tableName_ = tableName;
    dbName_ = (tableName_ != null ? tableName_.getDb() : dbName);
    uri_ = uri;
    storageType_ = storageType;
    storageUri_ = storageUri;
    columnNames_ = columnNames;
    functionName_= functionName;
  }

  public static PrivilegeSpec createServerScopedPriv(TPrivilegeLevel privilegeLevel) {
    return createServerScopedPriv(privilegeLevel, null);
  }

  public static PrivilegeSpec createServerScopedPriv(TPrivilegeLevel privilegeLevel,
      String serverName) {
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.SERVER, serverName, null,
        null, null, null, null, null, null);
  }

  public static PrivilegeSpec createDbScopedPriv(TPrivilegeLevel privilegeLevel,
      String dbName) {
    Preconditions.checkNotNull(dbName);
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.DATABASE, null, dbName,
        null, null, null, null, null, null);
  }

  public static PrivilegeSpec createTableScopedPriv(TPrivilegeLevel privilegeLevel,
      TableName tableName) {
    Preconditions.checkNotNull(tableName);
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.TABLE, null, null,
        tableName, null, null, null, null, null);
  }

  public static PrivilegeSpec createUdfScopedPriv(TPrivilegeLevel privilegeLevel,
      FunctionName functionName) {
    Preconditions.checkNotNull(functionName);
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.USER_DEFINED_FN, null, null,
        null, null, null, null, null, functionName);
  }

  public static PrivilegeSpec createColumnScopedPriv(TPrivilegeLevel privilegeLevel,
        TableName tableName, List<String> columnNames) {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(columnNames);
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.COLUMN, null, null,
        tableName, null, null, null, columnNames, null);
  }

  public static PrivilegeSpec createUriScopedPriv(TPrivilegeLevel privilegeLevel,
      HdfsUri uri) {
    Preconditions.checkNotNull(uri);
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.URI, null, null, null, uri,
        null, null, null, null);
  }

  public static PrivilegeSpec createStorageHandlerUriScopedPriv(
      TPrivilegeLevel privilegeLevel, StorageHandlerUri storageHandlerUri) {
    Preconditions.checkNotNull(storageHandlerUri);
    return new PrivilegeSpec(privilegeLevel, TPrivilegeScope.STORAGEHANDLER_URI, null,
        null, null, null, storageHandlerUri.getStorageType(),
        storageHandlerUri.getStoreUrl(), null, null);
  }

  public List<TPrivilege> toThrift() {
    List<TPrivilege> privileges = new ArrayList<>();
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
   *
   * Recall that for a GRANT/REVOKE statement, Frontend#createCatalogOpRequest() is
   * called to create a TCatalogOpRequest which will be sent from the coordinator to the
   * catalog daemon and that TPrivilege is one of the objects that has to be set up.
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
    if (storageType_ != null) privilege.setStorage_type(storageType_);
    if (storageUri_ != null) privilege.setStorage_url(storageUri_);
    if (columnName != null) privilege.setColumn_name(columnName);
    if (functionName_ != null) privilege.setFn_name(functionName_.getFunction());
    privilege.setCreate_time_ms(-1);
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
  public final String toSql() {
    return toSql(DEFAULT);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder(privilegeLevel_.toString());
    if (scope_ != TPrivilegeScope.COLUMN) {
      sb.append(" ON ");
      sb.append(scope_.toString());
    }
    if (scope_ == TPrivilegeScope.SERVER && serverName_ != null) {
      sb.append(" " + serverName_);
    } else if (scope_ == TPrivilegeScope.DATABASE) {
      sb.append(" " + dbName_);
    } else if (scope_ == TPrivilegeScope.TABLE) {
      sb.append(" " + tableName_.toString());
    } else if (scope_ == TPrivilegeScope.USER_DEFINED_FN) {
      sb.append(" " + functionName_.toString());
    } else if (scope_ == TPrivilegeScope.COLUMN) {
      sb.append(" (");
      sb.append(Joiner.on(",").join(columnNames_));
      sb.append(")");
      sb.append(" ON TABLE " + tableName_.toString());
    } else if (scope_ == TPrivilegeScope.URI) {
      sb.append(" '" + uri_.getLocation() + "'");
    } else if (scope_ == TPrivilegeScope.STORAGEHANDLER_URI) {
      sb.append(" '" + storageType_ + "://" + storageUri_ + "'");
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
        break;
      case DATABASE:
        analyzeTargetDatabase(analyzer);
        break;
      case URI:
        Preconditions.checkNotNull(uri_);
        if (privilegeLevel_ != TPrivilegeLevel.ALL) {
          throw new AnalysisException("Only 'ALL' privilege may be applied at " +
              "URI scope in privilege spec.");
        }
        uri_.analyze(analyzer, Privilege.ALL, false);
        break;
      case STORAGEHANDLER_URI:
        if (privilegeLevel_ != TPrivilegeLevel.RWSTORAGE) {
          throw new AnalysisException("Only 'RWSTORAGE' privilege may be applied at " +
              "storage handler URI scope in privilege spec.");
        }
        break;
      case TABLE:
        analyzeTargetTable(analyzer);
        break;
      case COLUMN:
        analyzeColumnPrivScope(analyzer);
        break;
      case USER_DEFINED_FN:
        analyzeUdf(analyzer);
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
    FeTable table = analyzeTargetTable(analyzer);
    if (table instanceof FeDataSourceTable) {
      throw new AnalysisException("Column-level privileges on external data " +
          "source tables are not supported.");
    }
    for (String columnName: columnNames_) {
      if (table.getColumn(columnName) == null) {
        // The error message should not reveal the existence or absence of a column.
        throw new AnalysisException(String.format("Error setting/showing column-level " +
            "privileges for table '%s'. Verify that both table and columns exist " +
            "and that you have permissions to issue a GRANT/REVOKE/SHOW GRANT statement.",
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
   * 4. The privilege level is not supported on tables, e.g. CREATE.
   */
  private FeTable analyzeTargetTable(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(scope_ == TPrivilegeScope.TABLE ||
        scope_ == TPrivilegeScope.COLUMN);
    Preconditions.checkState(!Strings.isNullOrEmpty(tableName_.getTbl()));
    if (privilegeLevel_ == TPrivilegeLevel.CREATE) {
      throw new AnalysisException("Create-level privileges on tables are not supported.");
    }
    FeTable table = null;
    try {
      dbName_ = analyzer.getTargetDbName(tableName_);
      Preconditions.checkNotNull(dbName_);
      table = analyzer.getTable(dbName_, tableName_.getTbl(), /* must_exist */ true);
      ownerName_ = table.getOwnerUser();
    } catch (TableLoadingException e) {
      throw new AnalysisException(e.getMessage(), e);
    } catch (AnalysisException e) {
      throw new AnalysisException(String.format("Error setting/showing privileges for " +
          "table '%s'. Verify that the table exists and that you have permissions " +
          "to issue a GRANT/REVOKE/SHOW GRANT statement.", tableName_.toString()));
    }
    Preconditions.checkNotNull(table);
    return table;
  }

  private void analyzeTargetDatabase(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(!Strings.isNullOrEmpty(dbName_));
    try {
      FeDb db = analyzer.getDb(dbName_, /* throwIfDoesNotExist */ true);
      ownerName_ = db.getOwnerUser();
    } catch (AnalysisException e) {
      throw new AnalysisException(String.format("Error setting/showing privileges " +
          "for database '%s'. Verify that the database exists and that you have " +
          "permissions to issue a GRANT/REVOKE/SHOW GRANT statement.", dbName_));
    }
  }

  private void analyzeUdf(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(scope_ == TPrivilegeScope.USER_DEFINED_FN);

    // Wildcard function names in the forms of `*`.`*`, <db_name>.`*`, or `*`.<fn_name>
    // are supported.
    List<String> path = functionName_.getFnNamePath();
    if (path.size() == 2 &&
        (path.get(0).equals("*") || path.get(1).equals("*"))) {
      dbName_ = path.get(0);
      functionName_.setDb(path.get(0));
      functionName_.setFunction(path.get(1));
      return;
    }

    // Call analyze() to perform path resolution so that 'db_' and 'fn_' of
    // 'functionName_' will be set up.
    functionName_.analyze(analyzer, /* preferBuiltinsDb */ false);
    Preconditions.checkArgument(!functionName_.getDb().equals("*"));
    Preconditions.checkArgument(!functionName_.getFunction().equals("*"));
    // Need to set up 'dbName_', which in turn is used to set up 'db_name' of the
    // TPrivilege in createTPrivilege().
    dbName_ = analyzer.getTargetDbName(functionName_);
  }

  public TPrivilegeScope getScope() { return scope_; }

  public TableName getTableName() { return tableName_; }

  public HdfsUri getUri() { return uri_; }

  public String getStorageType() { return storageType_; }

  public String getStorageUri() { return storageUri_; }

  public String getDbName() { return dbName_; }

  public String getServerName() { return serverName_; }

  public String getOwnerName() { return ownerName_; }
}
