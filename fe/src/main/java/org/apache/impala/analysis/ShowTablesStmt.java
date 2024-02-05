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

/**
 * Representation of a SHOW TABLES [pattern] statement.
 * Acceptable syntax:
 *
 * SHOW TABLES
 * SHOW TABLES "pattern"
 * SHOW TABLES LIKE "pattern"
 * SHOW TABLES IN database
 * SHOW TABLES IN database "pattern"
 * SHOW TABLES IN database LIKE "pattern"
 *
 * As in Hive, the 'LIKE' is optional. In Hive, also SHOW TABLES unquotedpattern is
 * accepted by the parser but returns no results. We don't support that syntax.
 */
public class ShowTablesStmt extends ShowTablesOrViewsStmt {
  public ShowTablesStmt() { super(null, null); }

  public ShowTablesStmt(String pattern) { super(null, pattern); }

  public ShowTablesStmt(String database, String pattern) { super(database, pattern); }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("SHOW TABLES");

    String parsedDb = getParsedDb();
    if (parsedDb != null) {
      sb.append(" IN ").append(parsedDb);
    }

    String pattern = getPattern();
    if (pattern != null) {
      sb.append(" LIKE '").append(pattern).append("'");
    }

    return sb.toString();
  }
}
