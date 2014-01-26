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
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.Function.CompareMode;
import com.cloudera.impala.catalog.ScalarFunction;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class CastExpr extends Expr {

  private final ColumnType targetType_;

  // true if this is a "pre-analyzed" implicit cast
  private final boolean isImplicit_;

  // True if this cast does not change the type.
  private boolean noOp_ = false;

  private static final String CAST_FN_NAME = "cast";

  public CastExpr(ColumnType targetType, Expr e, boolean isImplicit) {
    super();
    Preconditions.checkArgument(targetType.isValid());
    this.targetType_ = targetType;
    this.isImplicit_ = isImplicit;
    Preconditions.checkNotNull(e);
    children_.add(e);
    if (isImplicit) {
      // Implicit casts don't call analyze()
      // TODO: this doesn't seem like the cleanest approach but there are places
      // we generate these (e.g. table loading) where there is no analyzer object.
      try {
        analyze();
        computeNumDistinctValues();
      } catch (AnalysisException ex) {
        Preconditions.checkState(false,
          "Implicit casts should never throw analysis exception.");
      }
      isAnalyzed_ = true;
    }
  }

  public static void initBuiltins(Db db) {
    for (ColumnType t1: ColumnType.getSupportedTypes()) {
      if (t1.isNull()) continue;
      for (ColumnType t2: ColumnType.getSupportedTypes()) {
        if (t2.isNull()) continue;
        // For some reason we don't allow string->bool.
        // TODO: revisit
        if (t1.isStringType() && t2.isBoolean()) continue;
        db.addBuiltin(ScalarFunction.createBuiltinOperator(
            CAST_FN_NAME, Lists.newArrayList(t1, t2), t2));
      }
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
    analyze();
  }

  private void analyze() throws AnalysisException {
    // Our cast fn currently takes two arguments. The first is the value to cast and the
    // second is a dummy of the type to cast to. We need this to be able to resolve the
    // proper function.
    //  e.g. to differentiate between cast(bool, int) and cast(bool, smallint).
    // TODO: this is not very intuitive. We could also call the functions castToInt(*)
    ColumnType[] args = new ColumnType[2];
    args[0] = children_.get(0).type_;
    args[1] = targetType_;
    if (args[0].equals(args[1])) {
      noOp_ = true;
      type_ = targetType_;
      return;
    }

    FunctionName fnName = new FunctionName(Catalog.BUILTINS_DB, CAST_FN_NAME);
    Function searchDesc = new Function(fnName, args, ColumnType.INVALID, false);
    if (isImplicit_) {
      fn_ = Catalog.getBuiltin(searchDesc, CompareMode.IS_SUBTYPE);
      Preconditions.checkState(fn_ != null);
    } else {
      fn_ = Catalog.getBuiltin(searchDesc,
          args[0].isNull() ? CompareMode.IS_SUBTYPE : CompareMode.IS_IDENTICAL);
      if (fn_ == null) {
        throw new AnalysisException("Invalid type cast of " + getChild(0).toSql() +
            " from " + args[0] + " to " + args[1]);
      }
    }
    Preconditions.checkState(fn_.getReturnType().equals(targetType_));
    type_ = targetType_;
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
