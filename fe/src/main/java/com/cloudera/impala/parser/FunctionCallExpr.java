// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import java.util.List;

import com.cloudera.impala.common.AnalysisException;

class FunctionCallExpr extends Expr {
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
  public void analyze(Analyzer analyzer) throws AnalysisException {
    throw new AnalysisException("CAST not supported");
  }
}
