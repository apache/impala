// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.List;

import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

/**
 * CaseExpr represents the SQL expression
 * CASE [expr] WHEN expr THEN expr [WHEN expr THEN expr ...] [ELSE expr] END
 * Each expr is stored as a child, the first one at children[0], etc., and each
 * When/Then clause occupying two child slots..
 *
 */
public class CaseExpr extends Expr {
  private boolean hasCaseExpr;
  private boolean hasElseExpr;

  public CaseExpr(Expr caseExpr, List<CaseWhenClause> whenClauses, Expr elseExpr) {
    super();
    if (caseExpr != null) {
      children.add(caseExpr);
    }
    for (CaseWhenClause whenClause: whenClauses) {
      Preconditions.checkNotNull(whenClause.getWhenExpr());
      children.add(whenClause.getWhenExpr());
      Preconditions.checkNotNull(whenClause.getThenExpr());
      children.add(whenClause.getThenExpr());
    }
    if (elseExpr != null) {
      children.add(elseExpr);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    CaseExpr expr = (CaseExpr) obj;
    return hasCaseExpr == expr.hasCaseExpr && hasElseExpr == expr.hasElseExpr;
  }

  public String toSql() {
    StringBuilder output = new StringBuilder("CASE");
    int childIdx = 0;
    if (hasCaseExpr) {
      output.append(children.get(childIdx++).toSql());
    }
    while (childIdx + 2 <= children.size()) {
      output.append(" WHEN " + children.get(childIdx++).toSql());
      output.append(" THEN " + children.get(childIdx++).toSql());
    }
    if (hasElseExpr) {
      output.append(" ELSE " + children.get(childIdx).toSql());
    }
    output.append(" END");
    return output.toString();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    throw new AnalysisException("CASE not supported");
  }
}
