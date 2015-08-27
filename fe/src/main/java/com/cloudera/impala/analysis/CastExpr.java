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
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.ScalarFunction;
import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class CastExpr extends Expr {
  // Only set for explicit casts. Null for implicit casts.
  private final TypeDef targetTypeDef_;

  // True if this is a "pre-analyzed" implicit cast.
  private final boolean isImplicit_;

  // True if this cast does not change the type.
  private boolean noOp_ = false;

  /**
   * C'tor for "pre-analyzed" implicit casts.
   */
  public CastExpr(Type targetType, Expr e) {
    super();
    Preconditions.checkState(targetType.isValid());
    Preconditions.checkNotNull(e);
    type_ = targetType;
    targetTypeDef_ = null;
    isImplicit_ = true;
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
  }

  /**
   * C'tor for explicit casts.
   */
  public CastExpr(TypeDef targetTypeDef, Expr e) {
    Preconditions.checkNotNull(targetTypeDef);
    Preconditions.checkNotNull(e);
    isImplicit_ = false;
    targetTypeDef_ = targetTypeDef;
    children_.add(e);
  }

  /**
   * Copy c'tor used in clone().
   */
  protected CastExpr(CastExpr other) {
    super(other);
    targetTypeDef_ = other.targetTypeDef_;
    isImplicit_ = other.isImplicit_;
    noOp_ = other.noOp_;
  }

  private static String getFnName(Type targetType) {
    return "castTo" + targetType.getPrimitiveType().toString();
  }

  public static void initBuiltins(Db db) {
    for (Type fromType : Type.getSupportedTypes()) {
      if (fromType.isNull()) continue;
      for (Type toType : Type.getSupportedTypes()) {
        if (toType.isNull()) continue;
        // Disable casting from string to boolean
        if (fromType.isStringType() && toType.isBoolean()) continue;
        // Disable casting from boolean/timestamp to decimal
        if ((fromType.isBoolean() || fromType.isDateType()) && toType.isDecimal()) {
          continue;
        }
        if (fromType.getPrimitiveType() == PrimitiveType.STRING
            && toType.getPrimitiveType() == PrimitiveType.CHAR) {
          // Allow casting from String to Char(N)
          String beSymbol = "impala::CastFunctions::CastToChar";
          db.addBuiltin(ScalarFunction.createBuiltin(getFnName(ScalarType.CHAR),
              Lists.newArrayList((Type) ScalarType.STRING), false, ScalarType.CHAR,
              beSymbol, null, null, true));
          continue;
        }
        if (fromType.getPrimitiveType() == PrimitiveType.CHAR
            && toType.getPrimitiveType() == PrimitiveType.CHAR) {
          // Allow casting from CHAR(N) to Char(N)
          String beSymbol = "impala::CastFunctions::CastToChar";
          db.addBuiltin(ScalarFunction.createBuiltin(getFnName(ScalarType.CHAR),
              Lists.newArrayList((Type) ScalarType.createCharType(-1)), false,
              ScalarType.CHAR, beSymbol, null, null, true));
          continue;
        }
        if (fromType.getPrimitiveType() == PrimitiveType.VARCHAR
            && toType.getPrimitiveType() == PrimitiveType.VARCHAR) {
          // Allow casting from VARCHAR(N) to VARCHAR(M)
          String beSymbol = "impala::CastFunctions::CastToStringVal";
          db.addBuiltin(ScalarFunction.createBuiltin(getFnName(ScalarType.VARCHAR),
              Lists.newArrayList((Type) ScalarType.VARCHAR), false, ScalarType.VARCHAR,
              beSymbol, null, null, true));
          continue;
        }
        if (fromType.getPrimitiveType() == PrimitiveType.VARCHAR
            && toType.getPrimitiveType() == PrimitiveType.CHAR) {
          // Allow casting from VARCHAR(N) to CHAR(M)
          String beSymbol = "impala::CastFunctions::CastToChar";
          db.addBuiltin(ScalarFunction.createBuiltin(getFnName(ScalarType.CHAR),
              Lists.newArrayList((Type) ScalarType.VARCHAR), false, ScalarType.CHAR,
              beSymbol, null, null, true));
          continue;
        }
        if (fromType.getPrimitiveType() == PrimitiveType.CHAR
            && toType.getPrimitiveType() == PrimitiveType.VARCHAR) {
          // Allow casting from CHAR(N) to VARCHAR(M)
          String beSymbol = "impala::CastFunctions::CastToStringVal";
          db.addBuiltin(ScalarFunction.createBuiltin(getFnName(ScalarType.VARCHAR),
              Lists.newArrayList((Type) ScalarType.CHAR), false, ScalarType.VARCHAR,
              beSymbol, null, null, true));
          continue;
        }
        // Disable no-op casts
        if (fromType.equals(toType) && !fromType.isDecimal()) continue;
        String beClass = toType.isDecimal() || fromType.isDecimal() ?
            "DecimalOperators" : "CastFunctions";
        String beSymbol = "impala::" + beClass + "::CastTo" + Function.getUdfType(toType);
        db.addBuiltin(ScalarFunction.createBuiltin(getFnName(toType),
            Lists.newArrayList(fromType), false, toType, beSymbol,
            null, null, true));
      }
    }
  }

  @Override
  public String toSqlImpl() {
    if (isImplicit_) return getChild(0).toSql();
    return "CAST(" + getChild(0).toSql() + " AS " + targetTypeDef_.toString() + ")";
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
    msg.node_type = TExprNodeType.FUNCTION_CALL;
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("isImplicit", isImplicit_)
        .add("target", type_)
        .addValue(super.debugString())
        .toString();
  }

  public boolean isImplicit() { return isImplicit_; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    Preconditions.checkState(!isImplicit_);
    super.analyze(analyzer);
    targetTypeDef_.analyze(analyzer);
    type_ = targetTypeDef_.getType();
    analyze();
  }

  private void analyze() throws AnalysisException {
    Preconditions.checkNotNull(type_);
    if (type_.isComplexType()) {
      throw new AnalysisException(
          "Unsupported cast to complex type: " + type_.toSql());
    }

    boolean readyForCharCast =
        children_.get(0).getType().getPrimitiveType() == PrimitiveType.STRING ||
        children_.get(0).getType().getPrimitiveType() == PrimitiveType.CHAR;
    if (type_.getPrimitiveType() == PrimitiveType.CHAR && !readyForCharCast) {
      // Back end functions only exist to cast string types to CHAR, there is not a cast
      // for every type since it is redundant with STRING. Casts to go through 2 casts:
      // (1) cast to string, to stringify the value
      // (2) cast to CHAR, to truncate or pad with spaces
      CastExpr tostring = new CastExpr(ScalarType.STRING, children_.get(0));
      tostring.analyze();
      children_.set(0, tostring);
    }

    if (children_.get(0) instanceof NumericLiteral && type_.isFloatingPointType()) {
      // Special case casting a decimal literal to a floating point number. The
      // decimal literal can be interpreted as either and we want to avoid casts
      // since that can result in loss of accuracy.
      ((NumericLiteral)children_.get(0)).explicitlyCastToFloat(type_);
    }

    if (children_.get(0).getType().isNull()) {
      // Make sure BE never sees TYPE_NULL
      uncheckedCastChild(type_, 0);
    }

    // Ensure child has non-null type (even if it's a null literal). This is required
    // for the UDF interface.
    if (children_.get(0) instanceof NullLiteral) {
      NullLiteral nullChild = (NullLiteral)(children_.get(0));
      nullChild.uncheckedCastTo(type_);
    }

    Type childType = children_.get(0).type_;
    Preconditions.checkState(!childType.isNull());
    if (childType.equals(type_)) {
      noOp_ = true;
      return;
    }

    FunctionName fnName = new FunctionName(Catalog.BUILTINS_DB, getFnName(type_));
    Type[] args = { childType };
    Function searchDesc = new Function(fnName, args, Type.INVALID, false);
    if (isImplicit_) {
      fn_ = Catalog.getBuiltin(searchDesc, CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
      Preconditions.checkState(fn_ != null);
    } else {
      fn_ = Catalog.getBuiltin(searchDesc, CompareMode.IS_IDENTICAL);
      if (fn_ == null) {
        // allow for promotion from CHAR to STRING; only if no exact match is found
        fn_ = Catalog.getBuiltin(searchDesc.promoteCharsToStrings(),
            CompareMode.IS_IDENTICAL);
      }
    }
    if (fn_ == null) {
      throw new AnalysisException("Invalid type cast of " + getChild(0).toSql() +
          " from " + childType + " to " + type_);
    }

    Preconditions.checkState(type_.matchesType(fn_.getReturnType()),
        type_ + " != " + fn_.getReturnType());
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
          && type_.equals(other.type_)
          && super.equals(obj);
    }
    // Ignore implicit casts when comparing expr trees.
    if (isImplicit_) return getChild(0).equals(obj);
    return false;
  }

  @Override
  public Expr clone() { return new CastExpr(this); }
}
