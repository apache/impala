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

package org.apache.impala.authorization;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.View;
import org.apache.impala.catalog.local.LocalView;
import org.apache.impala.common.RuntimeEnv;

/**
 * Class that helps build PrivilegeRequest objects.
 *
 * For example:
 * PrivilegeRequestBuilder builder = new PrivilegeRequestBuilder(
 *     new AuthorizableFactory(AuthorizationProvider.RANGER));
 * PrivilegeRequest = builder.allOf(Privilege.SELECT).onTable("db", "tbl").build();
 */
public class PrivilegeRequestBuilder {
  private final AuthorizableFactory authzFactory_;
  private Authorizable authorizable_;
  private Privilege privilege_;
  private EnumSet<Privilege> privilegeSet_;
  private boolean grantOption_ = false;

  public PrivilegeRequestBuilder(AuthorizableFactory authzFactory) {
    Preconditions.checkNotNull(authzFactory);
    authzFactory_ = authzFactory;
  }

  /**
   * Sets the authorizable object to be a function.
   */
  public PrivilegeRequestBuilder onFunction(String dbName, String fnName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = authzFactory_.newFunction(dbName, fnName);
    return this;
  }

  /**
   * Sets the authorizable object to be a URI.
   */
  public PrivilegeRequestBuilder onUri(String uriName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = authzFactory_.newUri(uriName);
    return this;
  }

  public PrivilegeRequestBuilder onStorageHandlerUri(String storageType,
      String storageUri) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = authzFactory_.newStorageHandlerUri(storageType, storageUri);
    return this;
  }

  /**
   * Determines whether the given FeTable corresponds to a view that was created by a
   * non-superuser in HiveMetaStore.
   */
  public static boolean isViewCreatedByNonSuperuser(FeTable table) {
    // In the FE test environment, there is no metastore.api.Table object created and
    // thus 'view' would be null. To prevent a NullPointerException from being thrown
    // due to the call to getParameters(), we only check the value of the key
    // 'Authorized' when we are not in the FE test environment and we return false in
    // the FE test environment assuming that the view was authorized at creation.
    // In addition, when 'table' is null, we also assume that 'table' does not correspond
    // to a view not authorized at the creation time. 'table' could be null if it is
    // returned from a method call to Analyzer#getTableNoThrow() and the corresponding
    // FeTable is not currently cached in the local Catalog.
    // Note that we need to consider LocalView as well because a view stored in the
    // catalog daemon would be instantiated as a LocalView if coordinator's local catalog
    // is enabled.
    if (!(table instanceof View || table instanceof LocalView) ||
        RuntimeEnv.INSTANCE.isTestEnv()) {
      return false;
    }
    org.apache.hadoop.hive.metastore.api.Table view = table.getMetaStoreTable();
    Preconditions.checkNotNull(view);
    Map<String, String> parameters = view.getParameters();
    return (parameters != null && parameters.get("Authorized") != null &&
        parameters.get("Authorized").equalsIgnoreCase("false"));
  }

  /**
   * Sets the authorizable object to be a table.
   */
  public PrivilegeRequestBuilder onTable(FeTable table) {
    Preconditions.checkNotNull(table);
    String dbName = Preconditions.checkNotNull(table.getTableName().getDb());
    String tblName = Preconditions.checkNotNull(table.getTableName().getTbl());
    return onTable(dbName, tblName, table.getOwnerUser());
  }

  /**
   * Sets the authorizable object to be a table. This method is only called by
   * onTable(FeTable table) when 'table' corresponds to a view whose creation was not
   * authorized.
   */
  public PrivilegeRequestBuilder onTable(
      String dbName, String tableName, String ownerUser) {
    Preconditions.checkState(authorizable_ == null);
    // Convert 'dbName' and 'tableName' to lowercase to avoid duplicate audit log entries
    // for the PrivilegeRequest of ALTER.
    authorizable_ = authzFactory_.newTable(dbName.toLowerCase(), tableName.toLowerCase(),
        ownerUser);
    return this;
  }

  public PrivilegeRequestBuilder onTableUnknownOwner(String dbName, String tableName) {
    // Useful when owner cannot be determined because the table does not exist.
    // This call path is specifically meant for cases that try to mask the
    // TableNotFound AnalysisExceptions and instead propagate that as an
    // AuthorizationException.
    return onTable(dbName, tableName, null);
  }

  /**
   * Sets the authorizable object to be a server.
   */
  public PrivilegeRequestBuilder onServer(String serverName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = authzFactory_.newServer(serverName);
    return this;
  }

  /**
   * Sets the authorizable object to be a database.
   */
  public PrivilegeRequestBuilder onDb(FeDb db) {
    Preconditions.checkState(authorizable_ == null);
    Preconditions.checkNotNull(db);
    return onDb(db.getName(), db.getMetaStoreDb().getOwnerName());
  }

  /**
   * Sets the authorizable object to be a database.
   */
  public PrivilegeRequestBuilder onDb(String dbName, String ownerUser) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = authzFactory_.newDatabase(dbName, ownerUser);
    return this;
  }

  /**
   * Sets the authorizable object to be a column.
   */
  public PrivilegeRequestBuilder onColumn(String dbName, String tableName,
      String columnName, String tblOwnerUser) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ =
        authzFactory_.newColumnInTable(dbName, tableName, columnName, tblOwnerUser);
    return this;
  }

  /**
   * Specifies that permissions on any column in the given table.
   */
  public PrivilegeRequestBuilder onAnyColumn(
      String dbName, String tableName, String tblOwnerUser) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = authzFactory_.newColumnInTable(dbName, tableName, tblOwnerUser);
    return this;
  }

  /**
   * Specifies that permissions on any column in any table.
   */
  public PrivilegeRequestBuilder onAnyColumn(String dbName, String dbOwnerUser) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = authzFactory_.newColumnAllTbls(dbName, dbOwnerUser);
    return this;
  }

  /**
   * Specifies the privilege the user needs to have.
   */
  public PrivilegeRequestBuilder allOf(Privilege privilege) {
    privilege_ = privilege;
    return this;
  }

  /**
   * Specifies any of the privileges the user needs to have.
   */
  public PrivilegeRequestBuilder anyOf(EnumSet<Privilege> privileges) {
    privilegeSet_ = privileges;
    return this;
  }

  /**
   * Specifies the user needs "ALL" privileges
   */
  public PrivilegeRequestBuilder all() {
    privilege_ = Privilege.ALL;
    return this;
  }

  public PrivilegeRequestBuilder rwstorage() {
    privilege_ = Privilege.RWSTORAGE;
    return this;
  }

  /**
   * Specifies that any privileges are sufficient.
   */
  public PrivilegeRequestBuilder any() {
    privilege_ = Privilege.ANY;
    return this;
  }

  /**
   * Specifies that grant option is required.
   */
  public PrivilegeRequestBuilder grantOption() {
    grantOption_ = true;
    return this;
  }

  /**
   * Builds a PrivilegeRequest object based on the current Authorizable object
   * and privilege settings.
   */
  public PrivilegeRequest build() {
    Preconditions.checkNotNull(authorizable_);
    Preconditions.checkNotNull(privilege_);
    return new PrivilegeRequest(authorizable_, privilege_, grantOption_);
  }

  public Set<PrivilegeRequest> buildSet() {
    Preconditions.checkNotNull(authorizable_);
    Preconditions.checkNotNull(privilegeSet_);
    Set<PrivilegeRequest> privileges = Sets.newHashSet();
    for (Privilege p : privilegeSet_) {
      privileges.add(new PrivilegeRequest(authorizable_, p, grantOption_));
    }
    return privileges;
  }
}
