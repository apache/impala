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

import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Udf;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.opcode.FunctionOperator;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TUdfCallExpr;
import com.google.common.base.Joiner;

public class FunctionCallExpr extends Expr {
  private final FunctionName functionName_;

  // The udf function if this function call is for a UDF.
  private Udf udf_;

  public FunctionCallExpr(FunctionName functionName, List<Expr> params) {
    super();
    this.functionName_ = functionName;
    children.addAll(params);
  }

  public FunctionCallExpr(String functionName, List<Expr> params) {
    this(new FunctionName(functionName), params);
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
    return functionName_ + "(" + Joiner.on(", ").join(childrenToSql()) + ")";
  }

  @Override
  protected void toThrift(TExprNode msg) {
    if (udf_ != null) {
      msg.node_type = TExprNodeType.UDF_CALL;
      msg.setUdf_call_expr(new TUdfCallExpr());
      msg.udf_call_expr.setBinary_location(udf_.getLocation().toString());
      msg.udf_call_expr.setSymbol_name(udf_.getSymbolName());
      msg.udf_call_expr.setBinary_type(udf_.getBinaryType());
    } else {
      msg.node_type = TExprNodeType.FUNCTION_CALL;
      msg.setOpcode(opcode);
    }
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    PrimitiveType[] argTypes = new PrimitiveType[this.children.size()];
    for (int i = 0; i < this.children.size(); ++i) {
      this.children.get(i).analyze(analyzer);
      argTypes[i] = this.children.get(i).getType();
    }

    Function fnDesc = null;

    // First check if this is a builtin
    FunctionOperator op = OpcodeRegistry.instance().getFunctionOperator(
        functionName_.getFunction());
    if (op != FunctionOperator.INVALID_OPERATOR) {
      OpcodeRegistry.BuiltinFunction match =
          OpcodeRegistry.instance().getFunctionInfo(op, true, argTypes);
      if (match != null) {
        this.opcode = match.opcode;
        fnDesc = match;
      }
    } else {
      // Next check if it is a UDF
      String dbName = analyzer.getTargetDbName(functionName_);
      functionName_.setDb(dbName);

      if (!analyzer.getCatalog().functionExists(functionName_)) {
        throw new AnalysisException(functionName_ + "() unknown");
      }

      Function searchDesc = new Function(functionName_,
          argTypes, PrimitiveType.INVALID_TYPE, false);

      Function fn = analyzer.getCatalog().getFunction(searchDesc, false);
      if (fn != null) {
        if (fn instanceof Udf) {
          udf_ = (Udf)fn;
          fnDesc = fn;
        } else {
          throw new AnalysisException(functionName_ + "() is not a UDF");
        }
      }
    }

    if (fnDesc != null) {
      PrimitiveType[] args = fnDesc.getArgs();
      // Implicitly cast all the children to match the function if necessary
      for (int i = 0; i < argTypes.length; ++i) {
        // For varargs, we must compare with the last type in callArgs.argTypes.
        int ix = Math.min(args.length - 1, i);
        if (argTypes[i] != args[ix]) {
          castChild(args[ix], i);
        }
      }
      this.type = fnDesc.getReturnType();
    } else {
      String error = String.format("No matching function with those arguments: %s(%s)",
          functionName_, Joiner.on(", ").join(argTypes));
      throw new AnalysisException(error);
    }
  }
}
