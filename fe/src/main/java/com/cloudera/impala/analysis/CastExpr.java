// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

public class CastExpr extends Expr {
  private final PrimitiveType targetType;
  /** true if this is a "pre-analyzed" implicit cast */
  private final boolean isImplicit;

  public CastExpr(PrimitiveType targetType, Expr e, boolean isImplicit) {
    super();
    Preconditions.checkArgument(targetType != PrimitiveType.INVALID_TYPE);
    this.targetType = targetType;
    this.isImplicit = isImplicit;
    Preconditions.checkNotNull(e);
    children.add(e);
    if (isImplicit) {
      type = targetType;
    }
  }

  @Override
  public String toSql() {
    return "CAST(" + getChild(0).toSql() + " AS " + targetType.toString() + ")";
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);

    if (!isImplicit) {
      // cast was asked for in the query, check for validity of cast
      PrimitiveType childType = getChild(0).getType();
      PrimitiveType resultType =
        PrimitiveType.getAssignmentCompatibleType(childType, targetType);

      if (!resultType.isValid()) {
        throw new AnalysisException("Invalid type cast from: " + childType.toString() +
            " to " + targetType);
      }

      // this cast may result in loss of precision, but the user requested it
      this.type = targetType;
    }
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
