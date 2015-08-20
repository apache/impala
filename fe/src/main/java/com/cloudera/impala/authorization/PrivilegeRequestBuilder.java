// Copyright 2013 Cloudera Inc.
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

package com.cloudera.impala.authorization;

import com.google.common.base.Preconditions;

/**
 * Class that helps build PrivilegeRequest objects.
 * For example:
 * PrivilegeRequestBuilder builder = new PrivilegeRequestBuilder();
 * PrivilegeRequest = builder.allOf(Privilege.SELECT).onTable("db", "tbl").toRequest();
 *
 * TODO: In the future, this class could be extended to provide the option to specify
 * multiple permissions. For example:
 * builder.allOf(SELECT, INSERT).onTable(..);
 * It could also be extended to support an "anyOf" to check if the user has any of the
 * permissions specified:
 * builder.anyOf(SELECT, INSERT).onTable(...);
 */
public class PrivilegeRequestBuilder {
  Authorizeable authorizeable_;
  Privilege privilege_;

  /**
   * Sets the authorizeable object to be a column.
   */
  public PrivilegeRequestBuilder onColumn(String dbName, String tableName,
      String columnName) {
    authorizeable_ = new AuthorizeableColumn(dbName, tableName, columnName);
    return this;
  }

  /**
   * Sets the authorizeable object to be a table.
   */
  public PrivilegeRequestBuilder onTable(String dbName, String tableName) {
    authorizeable_ = new AuthorizeableTable(dbName, tableName);
    return this;
  }

  /**
   * Sets the authorizeable object to be a database.
   */
  public PrivilegeRequestBuilder onDb(String dbName) {
    authorizeable_ = new AuthorizeableDb(dbName);
    return this;
  }

  /**
   * Sets the authorizeable object to be a URI.
   */
  public PrivilegeRequestBuilder onURI(String uriName) {
    authorizeable_ = new AuthorizeableUri(uriName);
    return this;
  }

  /**
   * Specifies that permissions on any table in the given database.
   */
  public PrivilegeRequestBuilder onAnyTable(String dbName) {
    return onTable(dbName, AuthorizeableTable.ANY_TABLE_NAME);
  }

  /**
   * Specifies that permissions on any column in the given table.
   */
  public PrivilegeRequestBuilder onAnyColumn(String dbName, String tableName) {
    return onColumn(dbName, tableName, AuthorizeableColumn.ANY_COLUMN_NAME);
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
   * Builds a PrivilegeRequest object based on the current Authorizeable object
   * and privilege settings.
   */
  public PrivilegeRequest toRequest() {
    Preconditions.checkNotNull(authorizeable_);
    Preconditions.checkNotNull(privilege_);
    return new PrivilegeRequest(authorizeable_, privilege_);
  }
}
