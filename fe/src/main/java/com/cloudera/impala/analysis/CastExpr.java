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

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.opcode.FunctionOperator;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class CastExpr extends Expr {

  private final ColumnType targetType_;

  // true if this is a "pre-analyzed" implicit cast
  private final boolean isImplicit_;

  // True if this cast does not change the type.
  private boolean noOp_ = false;

  public CastExpr(ColumnType targetType, Expr e, boolean isImplicit) {
    super();
    Preconditions.checkArgument(targetType.isValid());
    this.targetType_ = targetType;
    this.isImplicit_ = isImplicit;
    Preconditions.checkNotNull(e);
    children_.add(e);
    if (isImplicit) {
      type_ = targetType;
      OpcodeRegistry.BuiltinFunction match = OpcodeRegistry.instance().getFunctionInfo(
          FunctionOperator.CAST, true, getChild(0).getType(), type_);
      Preconditions.checkState(match != null);
      Preconditions.checkState(match.getReturnType().equals(type_));
      this.opcode_ = match.opcode;
    }
  }

  @Override
  public String toSqlImpl() {
    if (isImplicit_) return getChild(0).toSql();
    return "CAST(" + getChild(0).toSql() + " AS " + targetType_.toString() + ")";
  }

  @Override
  protected void treeToThriftHelper(TExpr container) {
    if (noOp_) {
      getChild(0).treeToThriftHelper(container);
      return;
    }
    super.treeToThriftHelper(container);
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.CAST_EXPR;
    msg.setOpcode(opcode_);
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("isImplicit", isImplicit_)
        .add("target", targetType_)
        .addValue(super.debugString())
        .toString();
  }

  public boolean isImplicit() { return isImplicit_; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);

    if (isImplicit_) return;

    // cast was asked for in the query, check for validity of cast
    // this cast may result in loss of precision, but the user requested it
    ColumnType childType = getChild(0).getType();
    this.type_ = targetType_;

    if (childType.equals(targetType_)) {
      noOp_ = true;
      return;
    }

    OpcodeRegistry.BuiltinFunction match = OpcodeRegistry.instance().getFunctionInfo(
        FunctionOperator.CAST, childType.isNull(), getChild(0).getType(), type_);
    if (match == null) {
      throw new AnalysisException("Invalid type cast of " + getChild(0).toSql() +
          " from " + childType + " to " + targetType_);
    }
    Preconditions.checkState(match.getReturnType().equals(targetType_));
    this.opcode_ = match.opcode;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    CastExpr expr = (CastExpr) obj;
    return this.opcode_ == expr.opcode_;
  }

  /**
   * Returns child expr if this expr is an implicit cast, otherwise returns 'this'.
   */
  @Override
  public Expr ignoreImplicitCast() {
    if (isImplicit_) {
      // we don't expect to see to consecutive implicit casts
      Preconditions.checkState(
          !(getChild(0) instanceof CastExpr) || !((CastExpr) getChild(0)).isImplicit());
      return getChild(0);
    } else {
      return this;
    }
  }

}
