// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.opcode.FunctionOperator;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
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
      OpcodeRegistry.Signature match = OpcodeRegistry.instance().getFunctionInfo(
          FunctionOperator.CAST, getChild(0).getType(), type);
      Preconditions.checkState(match != null);
      Preconditions.checkState(match.returnType == type);
      this.opcode = match.opcode;
    }
  }

  @Override
  public String toSql() {
    if (isImplicit) {
      return getChild(0).toSql();
    }
    return "CAST(" + getChild(0).toSql() + " AS " + targetType.toString() + ")";
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.CAST_EXPR;
    msg.setOpcode(opcode);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);

    if (isImplicit) {
      return;
    }

    // cast was asked for in the query, check for validity of cast
    PrimitiveType childType = getChild(0).getType();

    // this cast may result in loss of precision, but the user requested it
    this.type = targetType;
    OpcodeRegistry.Signature match = OpcodeRegistry.instance().getFunctionInfo(
        FunctionOperator.CAST, getChild(0).getType(), type);
    if (match == null)
      throw new AnalysisException("Invalid type cast of " + getChild(0).toSql() +
          " from " + childType + " to " + targetType);
    Preconditions.checkState(match.returnType == targetType);
    this.opcode = match.opcode;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    CastExpr expr = (CastExpr) obj;
    return this.opcode == expr.opcode;
  }
}
