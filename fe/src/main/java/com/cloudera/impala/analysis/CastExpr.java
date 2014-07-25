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

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.Function.CompareMode;
import com.cloudera.impala.catalog.ScalarFunction;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class CastExpr extends Expr {

  private final Type targetType_;

  // true if this is a "pre-analyzed" implicit cast
  private final boolean isImplicit_;

  // True if this cast does not change the type.
  private boolean noOp_ = false;

  private static final String CAST_FN_NAME = "cast";

  public CastExpr(Type targetType, Expr e, boolean isImplicit) {
    super();
    Preconditions.checkArgument(targetType.isValid());
    this.targetType_ = targetType;
    this.isImplicit_ = isImplicit;
    Preconditions.checkNotNull(e);
    if (isImplicit) {
      // replace existing implicit casts
      if (e instanceof CastExpr) {
        CastExpr castExpr = (CastExpr) e;
        if (castExpr.isImplicit()) e = castExpr.getChild(0);
      }
      children_.add(e);

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
    } else {
      children_.add(e);
    }
  }

  /**
   * Copy c'tor used in clone().
   */
  protected CastExpr(CastExpr other) {
    super(other);
    targetType_ = other.targetType_;
    isImplicit_ = other.isImplicit_;
    noOp_ = other.noOp_;
  }

  private static String getFnName(Type targetType) {
    return "castTo" + targetType.getPrimitiveType().toString();
  }

  // TODO: this function can be simplified once the expr refactoring goes in
  public static void initBuiltins(Db db) {
    for (Type t1: Type.getSupportedTypes()) {
      if (t1.isDecimalOrNull()) continue;
      for (Type t2: Type.getSupportedTypes()) {
        if (t2.isDecimalOrNull()) continue;
        // For some reason we don't allow string->bool.
        // TODO: revisit
        if (t1.isStringType() && t2.isBoolean()) continue;
        db.addBuiltin(ScalarFunction.createBuiltinOperator(
            CAST_FN_NAME, Lists.newArrayList(t1, t2), t2));
      }
    }

    // Decimal cast operators are implemented with the UDF interface
    for (Type t : Type.getSupportedTypes()) {
      if (t.isNull()) continue;
      // Cast from decimal
      db.addBuiltin(ScalarFunction.createBuiltin(getFnName(t),
          Lists.newArrayList((Type)Type.DECIMAL), false, t,
          "impala::DecimalOperators::CastTo" + Function.getUdfType(t),
          null, null, true, true));
      // Cast to decimal
      // Disable casting from boolean/timestamp to decimal, and from registering decimal
      // to decimal twice
      if (t.isBoolean() || t.isDateType() || t.isDecimal()) continue;
      db.addBuiltin(ScalarFunction.createBuiltin(getFnName(Type.DECIMAL),
          Lists.newArrayList(t), false, Type.DECIMAL,
          "impala::DecimalOperators::CastToDecimalVal",
          null, null, true, true));
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
    if (targetType_.isDecimal() || children_.get(0).type_.isDecimal()) {
      msg.node_type = TExprNodeType.FUNCTION_CALL;
    } else {
      msg.node_type = TExprNodeType.CAST_EXPR;
    }
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
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    analyze();
  }

  private void analyze() throws AnalysisException {
    targetType_.analyze();
    if (targetType_.isComplexType()) {
      throw new AnalysisException(
          "Unsupported cast to complex type: " + targetType_.toSql());
    }

    if (children_.get(0) instanceof NumericLiteral &&
        targetType_.isFloatingPointType()) {
      // Special case casting a decimal literal to a floating point number. The
      // decimal literal can be interpreted as either and we want to avoid casts
      // since that can result in loss of accuracy.
      ((NumericLiteral)children_.get(0)).explicitlyCastToFloat(targetType_);
    }

    if (children_.get(0).getType().isNull()) {
      // Make sure BE never sees TYPE_NULL
      uncheckedCastChild(targetType_, 0);
    }

    // Ensure child has non-null type (even if it's a null literal). This is required
    // for the UDF interface.
    if (children_.get(0) instanceof NullLiteral) {
      NullLiteral nullChild = (NullLiteral)(children_.get(0));
      nullChild.uncheckedCastTo(targetType_);
    }

    Type childType = children_.get(0).type_;
    Preconditions.checkState(!childType.isNull());
    if (childType.equals(targetType_)) {
      noOp_ = true;
      type_ = targetType_;
      return;
    }

    FunctionName fnName;
    Type[] args;
    if (childType.isDecimal() || targetType_.isDecimal()) {
      fnName = new FunctionName(Catalog.BUILTINS_DB, getFnName(targetType_));
      args = new Type[1];
      args[0] = childType;
    } else {
      fnName = new FunctionName(Catalog.BUILTINS_DB, CAST_FN_NAME);

      // Our cast fn currently takes two arguments. The first is the value to cast and the
      // second is a dummy of the type to cast to. We need this to be able to resolve the
      // proper function.
      //  e.g. to differentiate between cast(bool, int) and cast(bool, smallint).
      // TODO: remove this when all casts use the UDF interface
      args = new Type[2];
      args[0] = childType;
      args[1] = targetType_;
    }

    Function searchDesc = new Function(fnName, args, Type.INVALID, false);
    if (isImplicit_) {
      fn_ = Catalog.getBuiltin(searchDesc, CompareMode.IS_SUPERTYPE_OF);
      Preconditions.checkState(fn_ != null);
    } else {
      fn_ = Catalog.getBuiltin(searchDesc, CompareMode.IS_IDENTICAL);
    }
    if (fn_ == null) {
      throw new AnalysisException("Invalid type cast of " + getChild(0).toSql() +
          " from " + childType + " to " + targetType_);
    }
    Preconditions.checkState(targetType_.matchesType(fn_.getReturnType()),
        targetType_ + " != " + fn_.getReturnType());
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

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj instanceof CastExpr) {
      CastExpr other = (CastExpr) obj;
      return isImplicit_ == other.isImplicit_
          && targetType_.equals(other.targetType_)
          && super.equals(obj);
    }
    // Ignore implicit casts when comparing expr trees.
    if (isImplicit_) return getChild(0).equals(obj);
    return false;
  }

  @Override
  public Expr clone() { return new CastExpr(this); }
}
