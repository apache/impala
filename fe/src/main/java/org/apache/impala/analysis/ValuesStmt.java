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

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.rewrite.BetweenToCompoundRule;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.rewrite.ExtractCompoundVerticalBarExprRule;

/**
 * Representation of a values() statement with a list of constant-expression lists.
 * ValuesStmt is a special case of a UnionStmt with the following restrictions:
 * - Operands are only constant selects
 * - Operands are connected by UNION ALL
 * - No nesting of ValuesStmts
 */
public class ValuesStmt extends UnionStmt {
  public ValuesStmt(List<SetOperand> operands, List<OrderByElement> orderByElements,
      LimitElement limitElement) {
    super(operands, orderByElements, limitElement);
  }

  /**
   * C'tor for cloning.
   */
  private ValuesStmt(ValuesStmt other) { super(other); }

  @Override
  protected String queryStmtToSql(QueryStmt queryStmt) {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append("(");
    appendSelectList((SelectStmt) queryStmt, strBuilder, DEFAULT);
    strBuilder.append(")");
    return strBuilder.toString();
  }

  @Override
  public String toSql(ToSqlOptions options) {
    if (options.showRewritten()) return super.toSql(options);
    StringBuilder strBuilder = new StringBuilder();
    if (withClause_ != null) {
      strBuilder.append(withClause_.toSql(options));
      strBuilder.append(" ");
    }
    Preconditions.checkState(operands_.size() > 0);
    strBuilder.append("VALUES(");
    for (int i = 0; i < operands_.size(); ++i) {
      if (operands_.size() != 1) strBuilder.append("(");
      appendSelectList((SelectStmt) operands_.get(i).getQueryStmt(), strBuilder, options);
      if (operands_.size() != 1) strBuilder.append(")");
      strBuilder.append((i+1 != operands_.size()) ? ", " : "");
    }
    strBuilder.append(")");
    return strBuilder.toString();
  }

  private void appendSelectList(
      SelectStmt select, StringBuilder strBuilder, ToSqlOptions options) {
    SelectList selectList = select.getSelectList();
    for (int j = 0; j < selectList.getItems().size(); ++j) {
      strBuilder.append(selectList.getItems().get(j).toSql(options));
      strBuilder.append((j+1 != selectList.getItems().size()) ? ", " : "");
    }
  }

  @Override
  public ValuesStmt clone() { return new ValuesStmt(this); }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    // IMPALA-11284: Expression rewrites for VALUES() could result in performance
    // regression since overhead can be huge and there is virtually no benefit of
    // rewrite if the expression will only ever be evaluated once (IMPALA-6590).
    // The following code only does the non-optional rewrites for || and BETWEEN
    // operator as the backend cannot execute them directly.
    ExprRewriter mandatoryRewriter = new ExprRewriter(Arrays.asList(
        BetweenToCompoundRule.INSTANCE, ExtractCompoundVerticalBarExprRule.INSTANCE));
    super.rewriteExprs(mandatoryRewriter);
    rewriter.addNumChanges(mandatoryRewriter);
  }

  @Override
  protected boolean shouldAvoidLossyCharPadding(Analyzer analyzer) {
    return analyzer.getQueryCtx().client_request
        .query_options.values_stmt_avoid_lossy_char_padding;
  }
}
