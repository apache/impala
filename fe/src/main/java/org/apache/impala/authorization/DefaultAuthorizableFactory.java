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
 * A default implementation of {@link AuthorizableFactory}.
 */
public class DefaultAuthorizableFactory implements AuthorizableFactory {
  public static final String ALL = "*";

  @Override
  public Authorizable newServer(String serverName) {
    return new AuthorizableServer(serverName);
  }

  @Override
  public Authorizable newDatabase(String dbName, String ownerUser) {
    Preconditions.checkNotNull(dbName);
    return new AuthorizableDb(dbName, ownerUser);
  }

  @Override
  public Authorizable newTable(String dbName, String tableName, String ownerUser) {
    Preconditions.checkNotNull(dbName);
    Preconditions.checkNotNull(tableName);
    return new AuthorizableTable(dbName, tableName, ownerUser);
  }

  @Override
  public Authorizable newColumnAllTbls(String dbName, String dbOwnerUser) {
    Preconditions.checkNotNull(dbName);
    return new AuthorizableColumn(dbName, ALL, ALL, dbOwnerUser);
  }

  @Override
  public Authorizable newColumnInTable(
      String dbName, String tableName, String tblOwnerUser) {
    Preconditions.checkNotNull(dbName);
    Preconditions.checkNotNull(tableName);
    return new AuthorizableColumn(dbName, tableName, ALL, tblOwnerUser);
  }

  @Override
  public Authorizable newColumnInTable(
      String dbName, String tableName, String columnName, String tblOwnerUser) {
    Preconditions.checkNotNull(dbName);
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(columnName);
    return new AuthorizableColumn(dbName, tableName, columnName, tblOwnerUser);
  }

  @Override
  public Authorizable newUri(String uri) {
    Preconditions.checkNotNull(uri);
    return new AuthorizableUri(uri);
  }

  @Override
  public Authorizable newFunction(String dbName, String fnName) {
    Preconditions.checkNotNull(dbName);
    Preconditions.checkNotNull(fnName);
    return new AuthorizableFn(dbName, fnName);
  }

  @Override
  public Authorizable newStorageHandlerUri(String storageType, String storageUri) {
    Preconditions.checkNotNull(storageType);
    Preconditions.checkNotNull(storageUri);
    return new AuthorizableStorageHandlerUri(storageType, storageUri);
  }
}
