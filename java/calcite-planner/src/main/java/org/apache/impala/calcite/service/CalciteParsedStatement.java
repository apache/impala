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

package org.apache.impala.calcite.service;

import java.util.Set;

import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlNode;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisDriver;
import org.apache.impala.analysis.ParsedStatement;
import org.apache.impala.analysis.TableName;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TQueryCtx;

/**
 * Implemntation of ParsedStatement hook that holds the AST that
 * is parsed from the sql String.
 */
public class CalciteParsedStatement implements ParsedStatement {
  private final SqlNode parsedNode_;
  private final boolean isExplain_;
  private final String sql_;
  private final Set<TableName> tableNames_;

  public CalciteParsedStatement(TQueryCtx queryCtx) throws ImpalaException {
    sql_ = queryCtx.client_request.stmt;
    CalciteQueryParser queryParser = new CalciteQueryParser(sql_);
    SqlNode parsedSqlNode = queryParser.parse();
    isExplain_ = parsedSqlNode instanceof SqlExplain;
    if (isExplain_) {
      parsedSqlNode = ((SqlExplain) parsedSqlNode).getExplicandum();
    }
    parsedNode_ = parsedSqlNode;

    CalciteMetadataHandler.TableVisitor tableVisitor =
        new CalciteMetadataHandler.TableVisitor(queryCtx.session.database);

    parsedNode_.accept(tableVisitor);

    tableNames_ = tableVisitor.tableNames_;
  }

  @Override
  public Set<TableName> getTablesInQuery(StmtMetadataLoader loader) {
    return tableNames_;
  }

  @Override
  public Object getTopLevelNode() {
    return parsedNode_;
  }

  @Override
  public boolean isExplain() {
    return isExplain_;
  }

  @Override
  public boolean isQueryStmt() {
    return true;
  }

  @Override
  public String toSql() {
    return sql_;
  }

  public SqlNode getParsedSqlNode() {
    return parsedNode_;
  }
}
