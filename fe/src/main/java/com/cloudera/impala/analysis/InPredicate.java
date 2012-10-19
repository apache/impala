// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.List;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TInPredicate;

public class InPredicate extends Predicate {

  private final boolean isNotIn;

  // First child is the comparison expr for which we
  // should check membership in the inList (the remaining children).
  public InPredicate(Expr compareExpr, List<Expr> inList, boolean isNotIn) {
    children.add(compareExpr);
    children.addAll(inList);
    this.isNotIn = isNotIn;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    analyzer.castAllToCompatibleType(children);
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.in_predicate = new TInPredicate(isNotIn);
    msg.node_type = TExprNodeType.IN_PRED;
    msg.setOpcode(opcode);
  }

  @Override
  public String toSql() {
    StringBuilder strBuilder = new StringBuilder();
    String notStr = (isNotIn) ? "NOT " : "";
    strBuilder.append(getChild(0).toSql() + " " + notStr + "IN (");
    for (int i = 1; i < children.size(); ++i) {
      strBuilder.append(getChild(i).toSql());
      strBuilder.append((i+1 != children.size()) ? ", " : "");
    }
    strBuilder.append(")");
    return strBuilder.toString();
  }
}
