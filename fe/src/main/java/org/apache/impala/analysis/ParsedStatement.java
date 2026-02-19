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

import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.ColumnLineageGraph.OperationType;

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

  // true if this is an alter table ddl statement
  public boolean isAlterTableStmt();

  // true if this is a compute stats statement
  public boolean isComputeStatsStmt();

  // true if this is a create database ddl statement
  public boolean isCreateDbStmt();

  // true if this is a create table as select ddl statement
  public boolean isCreateTableAsSelectStmt();

  // true if this is a create table like ddl statement
  public boolean isCreateTableLikeStmt();

  // true if this is a create table ddl statement
  public boolean isCreateTableStmt();

  // true if this is a create view ddl statement
  public boolean isCreateViewStmt();

  // true if this is a delete dml statement
  public boolean isDeleteStmt();

  // true if this is a drop database ddl statement
  public boolean isDropDbStmt();

  // true if this is a drop table or view ddl statement
  public boolean isDropTableOrViewStmt();

  // true if this is an insert dml statement
  public boolean isInsertStmt();

  // true if this is an invalidate metadata statement
  public boolean isInvalidateMetadata();

  // true if this is an update dml statement
  public boolean isUpdateStmt();

  // true if this is a statement where the first keyword is "values"
  public boolean isValuesStmt();

  // returns the sql string (could be rewritten)
  public String toSql();

  // Could be overridden to handle an AuthorizationException thrown for a registered
  // masked privilege request.
  public void handleAuthorizationException(AnalysisResult analysisResult);

  // Returns the type of the operation that will be used to produce the column lineage
  // graph when applicable.
  public OperationType getOperationType();
}
