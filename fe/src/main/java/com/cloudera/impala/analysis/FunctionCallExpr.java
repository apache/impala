// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.List;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.opcode.FunctionOperator;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Joiner;

public class FunctionCallExpr extends Expr {
  private final String functionName;

  public FunctionCallExpr(String functionName, List<Expr> params) {
    super();
    this.functionName = functionName.toLowerCase();
    children.addAll(params);
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((FunctionCallExpr) obj).opcode == this.opcode;
  }

  @Override
  public String toSql() {
    return functionName + "(" + Joiner.on(", ").join(childrenToSql()) + ")";
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.FUNCTION_CALL;
    msg.setOpcode(opcode);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    FunctionOperator op = OpcodeRegistry.instance().getFunctionOperator(functionName);
    if (op == FunctionOperator.INVALID_OPERATOR) {
      throw new AnalysisException(functionName + " unknown");
    }

    PrimitiveType[] argTypes = new PrimitiveType[this.children.size()];
    for (int i = 0; i < this.children.size(); ++i) {
      this.children.get(i).analyze(analyzer);
      argTypes[i] = this.children.get(i).getType();
    }
    OpcodeRegistry.Signature match =
      OpcodeRegistry.instance().getFunctionInfo(op, argTypes);
    if (match == null) {
      String error = "No matching function with those arguments: " + functionName
        + Joiner.on(", ").join(argTypes) + ")";
      throw new AnalysisException(error);
    }
    this.opcode = match.opcode;
    this.type = match.returnType;

    // Implicitly cast all the children to match the function if necessary
    for (int i = 0; i < argTypes.length; ++i) {
      // For varargs, we must compare with the last type in match.argTypes.
      int ix = Math.min(match.argTypes.length - 1, i);
      if (argTypes[i] != match.argTypes[ix]) {
        castChild(match.argTypes[ix], i);
      }
    }
  }
}
