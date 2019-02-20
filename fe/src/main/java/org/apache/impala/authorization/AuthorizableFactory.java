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

/**
 * An interface to create a factory class for creating instances of
 * {@link Authorizable}s.
 */
public interface AuthorizableFactory {
  /**
   * Creates a new instance of server {@link Authorizable} for a given server name.
   * Server name can be null.
   */
  Authorizable newServer(String serverName);

  /**
   * Creates a new instance of database {@link Authorizable} for a given database name.
   */
  Authorizable newDatabase(String dbName);

  /**
   * Creates a new instance of table {@link Authorizable} for given database and table
   * names.
   */
  Authorizable newTable(String dbName, String tableName);

  /**
   * Creates a new instance of column {@link Authorizable} for a given database name and
   * gives access to all tables and columns.
   */
  Authorizable newColumn(String dbName);

  /**
   * Creates a new instance of column {@link Authorizable} for given database and table
   * names and gives access to all columns.
   */
  Authorizable newColumn(String dbName, String tableName);

  /**
   * Creates a new instance of column {@link Authorizable} for given database, table, and
   * column names.
   */
  Authorizable newColumn(String dbName, String tableName, String columnName);

  /**
   * Creates a new instance of URI {@link Authorizable} for a given URI.
   */
  Authorizable newUri(String uri);

  /**
   * Creates a new instance of function {@link Authorizable} for given database and
   * function names.
   */
  Authorizable newFunction(String dbName, String fnName);
}
