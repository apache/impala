// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

class CastExpr extends Expr {
  private final PrimitiveType targetType;

  public CastExpr(PrimitiveType targetType, Expr e) {
    super();
    Preconditions.checkArgument(targetType != PrimitiveType.INVALID_TYPE);
    this.targetType = targetType;
    Preconditions.checkNotNull(e);
    children.add(e);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    throw new AnalysisException("CAST not supported");
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    CastExpr expr = (CastExpr) obj;
    return targetType == expr.targetType;
  }
}
