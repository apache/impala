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

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TMergeCaseType;
import org.apache.impala.thrift.TMergeMatchType;

/**
 * Base class for different merge cases in MERGE statements. Each merge case
 * could contain additional filtering predicates, for example: 'WHEN MATCHED AND
 * source.id > 100' maps 'source.id > 100' predicate as an additional filter
 * predicate. The result expressions are created by the specialized cases,
 * they must contain the same amount of expressions as the target tables has.
 */
public abstract class MergeCase extends StatementBase {
  protected List<Expr> resultExprs_;
  private List<Expr> filterExprs_;
  protected TableName targetTableName_;
  protected List<Column> targetTableColumns_;
  protected TableRef targetTableRef_;
  protected TableRef sourceTableRef_;
  protected TMergeMatchType matchType_;

  protected MergeCase() {
    matchType_ = TMergeMatchType.MATCHED;
    filterExprs_ = Collections.emptyList();
    resultExprs_ = Collections.emptyList();
  }

  protected MergeCase(List<Expr> resultExprs, List<Expr> filterExprs,
      TableName targetTableName, List<Column> targetTableColumns,
      TableRef targetTableRef, TMergeMatchType matchType, TableRef sourceTableRef) {
    targetTableName_ = targetTableName;
    targetTableColumns_ = targetTableColumns;
    targetTableRef_ = targetTableRef;
    sourceTableRef_ = sourceTableRef;
    resultExprs_ = resultExprs;
    filterExprs_ = filterExprs;
    matchType_ = matchType;
  }

  public List<Expr> getFilterExprs() { return filterExprs_; }

  public void setFilterExprs(List<Expr> exprs) { filterExprs_ = exprs; }
  public void setMatchType(TMergeMatchType matchType) { matchType_ = matchType; }

  public void setParent(MergeStmt parent) {
    targetTableName_ = parent.getTargetTable().getTableName();
    targetTableColumns_ = parent.getTargetTable().getColumns();
    targetTableRef_ = parent.getTargetTableRef();
    sourceTableRef_ = parent.getSourceTableRef();
  }

  protected List<String> getSourceColumnLabels() {
    if (sourceTableRef_ instanceof InlineViewRef) {
      InlineViewRef source = (InlineViewRef) sourceTableRef_;
      return source.getColLabels();
    } else {
      return sourceTableRef_.getTable().getColumnNames();
    }
  }

  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    filterExprs_ = Expr.substituteList(filterExprs_, smap, analyzer, true);
    resultExprs_ = Expr.substituteList(resultExprs_, smap, analyzer, true);
  }

  public List<String> getExplainStrings(TExplainLevel explainLevel) {
    List<String> details = Lists.newArrayList();
    if (!filterExprs_.isEmpty()) {
      details.add(String.format(
          "filter predicates: %s", Expr.getExplainString(filterExprs_, explainLevel)));
    }
    details.add(String.format(
        "result expressions: %s", Expr.getExplainString(resultExprs_, explainLevel)));
    details.add(String.format("type: %s", caseType()));
    return details;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);
    for (Expr expr : filterExprs_) {
      expr.analyze(analyzer);
      if (expr.type_ != Type.BOOLEAN) {
        throw new AnalysisException(String.format(
            "Filter expression requires return type '%s'. Actual type is '%s'",
            Type.BOOLEAN, expr.type_));
      }
    }
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder builder = new StringBuilder();
    builder.append("WHEN ");
    builder.append(matchTypeAsString());
    if (!filterExprs_.isEmpty()) {
      StringJoiner expressionJoiner = new StringJoiner(" AND ");
      builder.append(" AND ");
      for (Expr filterExpr : filterExprs_) {
        expressionJoiner.add(filterExpr.toSql(options));
      }
      builder.append(expressionJoiner);
    }
    builder.append(" THEN ");
    return builder.toString();
  }

  @Override
  public void reset() {
    super.reset();
    resultExprs_ = Collections.emptyList();
    for(Expr expr : filterExprs_) {
      expr.reset();
    }
  }

  @Override
  public abstract MergeCase clone();

  @Override
  public List<Expr> getResultExprs() { return resultExprs_; }

  public TMergeMatchType matchType() {
    return matchType_;
  }
  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    rewriter.rewriteList(filterExprs_, analyzer_);
    rewriter.rewriteList(resultExprs_, analyzer_);
  }

  public String matchTypeAsString() {
    switch (matchType_) {
      case MATCHED:
        return "MATCHED";
      case NOT_MATCHED_BY_TARGET:
        return "NOT MATCHED BY TARGET";
      case NOT_MATCHED_BY_SOURCE:
        return "NOT MATCHED BY SOURCE";
      default:
        throw new IllegalStateException(
            String.format("Invalid TMergeMatchType value: %s", matchType_));
    }
  }

  public abstract TMergeCaseType caseType();
}
