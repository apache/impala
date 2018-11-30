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
import org.apache.impala.authorization.sentry.SentryAuthorizableColumn;
import org.apache.impala.authorization.sentry.SentryAuthorizableDb;
import org.apache.impala.authorization.sentry.SentryAuthorizableFn;
import org.apache.impala.authorization.sentry.SentryAuthorizableServer;
import org.apache.impala.authorization.sentry.SentryAuthorizableTable;
import org.apache.impala.authorization.sentry.SentryAuthorizableUri;

/**
 * Class that helps build PrivilegeRequest objects.
 * For example:
 * PrivilegeRequestBuilder builder = new PrivilegeRequestBuilder();
 * PrivilegeRequest = builder.allOf(Privilege.SELECT).onTable("db", "tbl").build();
 *
 * TODO: In the future, this class could be extended to provide the option to specify
 * multiple permissions. For example:
 * builder.allOf(SELECT, INSERT).onTable(..);
 * It could also be extended to support an "anyOf" to check if the user has any of the
 * permissions specified:
 * builder.anyOf(SELECT, INSERT).onTable(...);
 */
public class PrivilegeRequestBuilder {
  private Authorizable authorizable_;
  private Privilege privilege_;
  private boolean grantOption_ = false;

  /**
   * Sets the authorizable object to be a URI.
   */
  public PrivilegeRequestBuilder onFunction(String dbName, String fnName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = new SentryAuthorizableFn(dbName, fnName);
    return this;
  }

  /**
   * Sets the authorizable object to be a URI.
   */
  public PrivilegeRequestBuilder onUri(String uriName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = new SentryAuthorizableUri(uriName);
    return this;
  }

  /**
   * Sets the authorizable object to be a column.
   */
  public PrivilegeRequestBuilder onColumn(String dbName, String tableName,
      String columnName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = new SentryAuthorizableColumn(dbName, tableName, columnName);
    return this;
  }

  /**
   * Sets the authorizable object to be a table.
   */
  public PrivilegeRequestBuilder onTable(String dbName, String tableName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = new SentryAuthorizableTable(dbName, tableName);
    return this;
  }

  /**
   * Sets the authorizable object to be a server.
   */
  public PrivilegeRequestBuilder onServer(String serverName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = new SentryAuthorizableServer(serverName);
    return this;
  }

  /**
   * Sets the authorizable object to be a database.
   */
  public PrivilegeRequestBuilder onDb(String dbName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = new SentryAuthorizableDb(dbName);
    return this;
  }

  /**
   * Specifies that permissions on any table in the given database.
   */
  public PrivilegeRequestBuilder onAnyTable(String dbName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = new SentryAuthorizableTable(dbName);
    return this;
  }

  /**
   * Specifies that permissions on any column in the given table.
   */
  public PrivilegeRequestBuilder onAnyColumn(String dbName, String tableName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = new SentryAuthorizableColumn(dbName, tableName);
    return this;
  }

  /**
   * Specifies that permissions on any column in any table.
   */
  public PrivilegeRequestBuilder onAnyColumn(String dbName) {
    Preconditions.checkState(authorizable_ == null);
    authorizable_ = new SentryAuthorizableColumn(dbName);
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
