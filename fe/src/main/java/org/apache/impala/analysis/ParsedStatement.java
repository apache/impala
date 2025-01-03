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

import java.util.Set;

/**
 * ParsedStatement interface that holds the parsed query statement.
 *
 * This is currently implemented by the original Impala parser and the
 * Calcite parser.
 */
public interface ParsedStatement {

  // Retrieve all the tables/views referenced in the parsed query.
  public Set<TableName> getTablesInQuery(StmtMetadataLoader loader);

  // return the wrapped statement object.
  public Object getTopLevelNode();

  // true if this is an explain statement
  public boolean isExplain();

  // true if this is a query (select) statement
  public boolean isQueryStmt();

  // returns the sql string (could be rewritten)
  public String toSql();
}
