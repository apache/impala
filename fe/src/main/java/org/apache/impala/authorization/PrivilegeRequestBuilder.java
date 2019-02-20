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

import com.google.common.base.Preconditions;

/**
 * Class that helps build PrivilegeRequest objects.
 *
 * For example:
 * PrivilegeRequestBuilder builder = new PrivilegeRequestBuilder(
 *     new AuthorizableFactory(AuthorizationProvider.SENTRY));
 * PrivilegeRequest = builder.allOf(Privilege.SELECT).onTable("db", "tbl").build();
 */
public class PrivilegeRequestBuilder {
  private final AuthorizableFactory authzFactory_;
  private Authorizable authorizable_;
  private Privilege privilege_;
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

  /**
   * Sets the authorizable object to be a column.
   */
  public PrivilegeRequestBuilder onColumn(String dbName, String tableName,
      String columnName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = authzFactory_.newColumn(dbName, tableName, columnName);
    return this;
  }

  /**
   * Sets the authorizable object to be a table.
   */
  public PrivilegeRequestBuilder onTable(String dbName, String tableName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = authzFactory_.newTable(dbName, tableName);
    return this;
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
  public PrivilegeRequestBuilder onDb(String dbName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = authzFactory_.newDatabase(dbName);
    return this;
  }

  /**
   * Specifies that permissions on any column in the given table.
   */
  public PrivilegeRequestBuilder onAnyColumn(String dbName, String tableName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = authzFactory_.newColumn(dbName, tableName);
    return this;
  }

  /**
   * Specifies that permissions on any column in any table.
   */
  public PrivilegeRequestBuilder onAnyColumn(String dbName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = authzFactory_.newColumn(dbName);
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
   * Specifies the user needs "ALL" privileges
   */
  public PrivilegeRequestBuilder all() {
    privilege_ = Privilege.ALL;
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
}
