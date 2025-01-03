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

import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;

/**
 * Implementation of the ParsedStatement for the original Impala planner. On
 * construction, the sql statement will be parsed.
 */
public class ParsedStatementImpl implements ParsedStatement {

  // the wrapped sql statement object. Note, this is not final because Impala
  // allows the statement to be rewritten at analysis time (which is kinda
  // hacky)
  private final StatementBase stmt_ ;

  public ParsedStatementImpl(TQueryCtx queryCtx) throws ImpalaException {
    stmt_ = Parser.parse(queryCtx.client_request.stmt,
        queryCtx.client_request.query_options);
  }

  public ParsedStatementImpl(String stmt) throws ImpalaException {
    stmt_ = Parser.parse(stmt);
  }

  public ParsedStatementImpl(String stmt, TQueryOptions queryOpt) throws ImpalaException {
    stmt_ = Parser.parse(stmt, queryOpt);
  }

  public ParsedStatementImpl(StatementBase stmt) {
    stmt_ = stmt;
  }

  @Override
  public Set<TableName> getTablesInQuery(StmtMetadataLoader loader) {
    return loader.collectTableCandidates(stmt_);
  }

  // Retrieves the wrapped sql object
  @Override
  public Object getTopLevelNode() {
    return stmt_;
  }

  @Override
  public boolean isExplain() {
    return stmt_.isExplain();
  }

  @Override
  public boolean isQueryStmt() {
    return stmt_ instanceof QueryStmt;
  }

  @Override
  public String toSql() {
    return stmt_.toSql();
  }
}
