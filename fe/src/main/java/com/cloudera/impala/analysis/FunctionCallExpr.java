// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
  public String toSqlImpl() {
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
      OpcodeRegistry.instance().getFunctionInfo(op, true, argTypes);
    if (match == null) {
      String error = String.format("No matching function with those arguments: %s (%s)",
          functionName, Joiner.on(", ").join(argTypes));
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
