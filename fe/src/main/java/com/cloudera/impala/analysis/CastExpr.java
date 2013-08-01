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

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.opcode.FunctionOperator;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Preconditions;

public class CastExpr extends Expr {

  private final PrimitiveType targetType;
  /** true if this is a "pre-analyzed" implicit cast */
  private final boolean isImplicit;

  // True if this cast does not change the type.
  private boolean noOp = false;

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
          FunctionOperator.CAST, true, getChild(0).getType(), type);
      Preconditions.checkState(match != null);
      Preconditions.checkState(match.returnType == type);
      this.opcode = match.opcode;
    }
  }

  @Override
  public String toSqlImpl() {
    if (isImplicit) {
      return getChild(0).toSql();
    }
    return "CAST(" + getChild(0).toSql() + " AS " + targetType.toString() + ")";
  }

  @Override
  protected void treeToThriftHelper(TExpr container) {
    if (noOp) {
      getChild(0).treeToThriftHelper(container);
      return;
    }
    super.treeToThriftHelper(container);
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.CAST_EXPR;
    msg.setOpcode(opcode);
  }

  public boolean isImplicit() {
    return isImplicit;
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

    if (childType.equals(targetType)) {
      noOp = true;
      return;
    }

    OpcodeRegistry.Signature match = OpcodeRegistry.instance().getFunctionInfo(
        FunctionOperator.CAST, childType.isNull(), getChild(0).getType(), type);
    if (match == null) {
      throw new AnalysisException("Invalid type cast of " + getChild(0).toSql() +
          " from " + childType + " to " + targetType);
    }
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

  /**
   * Returns child expr if this expr is an implicit cast, otherwise returns 'this'.
   */
  @Override
  public Expr ignoreImplicitCast() {
    if (isImplicit) {
      // we don't expect to see to consecutive implicit casts
      Preconditions.checkState(
          !(getChild(0) instanceof CastExpr) || !((CastExpr) getChild(0)).isImplicit());
      return getChild(0);
    } else {
      return this;
    }
  }

}
