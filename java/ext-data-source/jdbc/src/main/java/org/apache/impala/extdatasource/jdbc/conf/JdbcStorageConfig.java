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

package org.apache.impala.extdatasource.jdbc.conf;


public enum JdbcStorageConfig {
  // Table properties specified in the create table statement.
  // The database from which the external table comes, such as MySQL, ORACLE, POSTGRES,
  // and MSSQL, etc.
  DATABASE_TYPE("database.type", true),
  // JDBC connection string, including the database type, IP address, port number, and
  // database name. For example, "jdbc:postgresql://127.0.0.1:5432/functional
  JDBC_URL("jdbc.url", true),
  // Class name of JDBC driver. For example, "org.postgresql.Driver"
  JDBC_DRIVER_CLASS("jdbc.driver", true),
  // Driver URL for downloading the Jar file package that is used to access the external
  // database.
  JDBC_DRIVER_URL("driver.url", true),
  // Username for accessing the external database.
  DBCP_USERNAME("dbcp.username", false),
  // Password of the user.
  DBCP_PASSWORD("dbcp.password", false),
  // Number of rows to fetch in a batch.
  JDBC_FETCH_SIZE("jdbc.fetch.size", false),
  // SQL query which specify how to get data from external database.
  // User need to specify either “table” or “query” in the create table statement.
  QUERY("query", false),
  // Name of the external table to be mapped in Impala.
  TABLE("table", true),
  // Mapping of column names between external table and Impala.
  COLUMN_MAPPING("column.mapping", false);

  private final String propertyName;
  private boolean required = false;


  JdbcStorageConfig(String propertyName, boolean required) {
    this.propertyName = propertyName;
    this.required = required;
  }


  public String getPropertyName() {
    return propertyName;
  }


  public boolean isRequired() {
    return required;
  }

}

