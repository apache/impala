// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.List;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Joiner;

public class FunctionCallExpr extends Expr {
  private final String functionName;

  public FunctionCallExpr(String functionName, List<Expr> params) {
    super();
    this.functionName = functionName;
    children.addAll(params);
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((FunctionCallExpr) obj).functionName.equals(functionName);
  }

  @Override
  public String toSql() {
    return functionName + "(" + Joiner.on(", ").join(childrenToSql()) + ")";
  }

  // TODO: we need to encode the actual function opcodes;
  // this ties in with replacing TExpr.op with an opcode
  // that resolves to a single compute function for the backend
  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.FUNCTION_CALL;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    throw new AnalysisException("CAST not supported");
  }
}
