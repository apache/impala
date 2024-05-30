// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.analysis;

import java.util.Objects;

import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.TypeCompatibility;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ThriftSerializationCtx;
import org.apache.impala.thrift.TCastExpr;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class CastExpr extends Expr {
  // Only set for explicit casts. Null for implicit casts.
  private final TypeDef targetTypeDef_;

  // True if this is a "pre-analyzed" implicit cast.
  private final boolean isImplicit_;

  // True if this cast does not change the type.
  private boolean noOp_ = false;

  // Prefix for naming cast functions.
  protected final static String CAST_FUNCTION_PREFIX = "castto";
  private final static String CAST_TO_CHAR_FN = "impala::CastFunctions::CastToChar";
  private final static String CAST_TO_VARCHAR_FN = "impala::CastFunctions::CastToVarchar";

  // Stores the value of the FORMAT clause.
  private final String castFormat_;

  // Stores the compatibility level with which the cast was defined.
  private final TypeCompatibility compatibility_;

  /**
   * C'tor for "pre-analyzed" implicit casts.
   */
  public CastExpr(
      Type targetType, Expr e, String format, TypeCompatibility compatibility) {
    super();
    Preconditions.checkState(targetType.isValid());
    Preconditions.checkNotNull(e);
    type_ = targetType;
    targetTypeDef_ = null;
    isImplicit_ = true;
    castFormat_ = format;
    compatibility_ = compatibility;
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
      evalCost_ = computeEvalCost();
    } catch (AnalysisException ex) {
      Preconditions.checkState(false,
          "Implicit casts should never throw analysis exception.");
    }
    analysisDone();
  }

  public CastExpr(Type targetType, Expr e) {
    this(targetType, e, null, TypeCompatibility.DEFAULT);
  }

  public CastExpr(Type targetType, Expr e, TypeCompatibility compatibility) {
    this(targetType, e, null, compatibility);
  }

  /**
   * C'tor for explicit casts.
   */
  public CastExpr(TypeDef targetTypeDef, Expr e) {
    this(targetTypeDef, e, null);
  }

  public CastExpr(TypeDef targetTypeDef, Expr e, String format) {
    Preconditions.checkNotNull(targetTypeDef);
    Preconditions.checkNotNull(e);
    isImplicit_ = false;
    targetTypeDef_ = targetTypeDef;
    children_.add(e);
    castFormat_ = format;
    compatibility_ = TypeCompatibility.DEFAULT;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected CastExpr(CastExpr other) {
    super(other);
    targetTypeDef_ = other.targetTypeDef_;
    isImplicit_ = other.isImplicit_;
    noOp_ = other.noOp_;
    castFormat_ = other.castFormat_;
    compatibility_ = other.compatibility_;
  }

  private static String getFnName(Type targetType) {
    return CAST_FUNCTION_PREFIX + targetType.getPrimitiveType().toString();
  }

  public static void initBuiltins(Db db) {
    for (Type fromType : Type.getSupportedTypes()) {
      if (fromType.isNull()) continue;
      for (Type toType : Type.getSupportedTypes()) {
        if (toType.isNull()) continue;
        // Disable casting from string to boolean
        if (fromType.isStringType() && toType.isBoolean()) continue;
        // Casting from date is only allowed when to-type is timestamp or string.
        if (fromType.isDate() && !toType.isTimestamp() && !toType.isStringType()) {
          continue;
        }
        // Casting to date is only allowed when from-type is timestamp or string.
        if (toType.isDate() && !fromType.isTimestamp() && !fromType.isStringType()) {
          continue;
        }
        // Disable casting from boolean/timestamp/date to decimal
        if ((fromType.isBoolean() || fromType.isDateOrTimeType()) && toType.isDecimal()) {
          continue;
        }
        if (fromType.getPrimitiveType() == PrimitiveType.STRING
            && toType.getPrimitiveType() == PrimitiveType.CHAR) {
          // Allow casting from String to Char(N)
          db.addBuiltin(ScalarFunction.createBuiltin(getFnName(ScalarType.CHAR),
              Lists.newArrayList((Type) ScalarType.STRING), false, ScalarType.CHAR,
              CAST_TO_CHAR_FN, null, null, true));
          continue;
        }
        if (fromType.getPrimitiveType() == PrimitiveType.CHAR
            && toType.getPrimitiveType() == PrimitiveType.CHAR) {
          // Allow casting from CHAR(N) to Char(N)
          db.addBuiltin(ScalarFunction.createBuiltin(getFnName(ScalarType.CHAR),
              Lists.newArrayList((Type) ScalarType.createCharType(-1)), false,
              ScalarType.CHAR, CAST_TO_CHAR_FN, null, null, true));
          continue;
        }
        if (fromType.getPrimitiveType() == PrimitiveType.VARCHAR
            && toType.getPrimitiveType() == PrimitiveType.VARCHAR) {
          // Allow casting from VARCHAR(N) to VARCHAR(M)
          db.addBuiltin(ScalarFunction.createBuiltin(getFnName(ScalarType.VARCHAR),
              Lists.newArrayList((Type) ScalarType.VARCHAR), false, ScalarType.VARCHAR,
              CAST_TO_VARCHAR_FN, null, null, true));
          continue;
        }
        if (fromType.getPrimitiveType() == PrimitiveType.VARCHAR
            && toType.getPrimitiveType() == PrimitiveType.CHAR) {
          // Allow casting from VARCHAR(N) to CHAR(M)
          db.addBuiltin(ScalarFunction.createBuiltin(getFnName(ScalarType.CHAR),
              Lists.newArrayList((Type) ScalarType.VARCHAR), false, ScalarType.CHAR,
              CAST_TO_CHAR_FN, null, null, true));
          continue;
        }
        if (fromType.getPrimitiveType() == PrimitiveType.CHAR
            && toType.getPrimitiveType() == PrimitiveType.VARCHAR) {
          // Allow casting from CHAR(N) to VARCHAR(M)
          db.addBuiltin(ScalarFunction.createBuiltin(getFnName(ScalarType.VARCHAR),
              Lists.newArrayList((Type) ScalarType.CHAR), false, ScalarType.VARCHAR,
              CAST_TO_VARCHAR_FN, null, null, true));
          continue;
        }
       if (fromType.getPrimitiveType() == PrimitiveType.STRING
            && toType.getPrimitiveType() == PrimitiveType.VARCHAR) {
          // Allow casting from STRING to VARCHAR(M)
          db.addBuiltin(ScalarFunction.createBuiltin(getFnName(ScalarType.VARCHAR),
              Lists.newArrayList((Type) ScalarType.STRING), false, ScalarType.VARCHAR,
              CAST_TO_VARCHAR_FN, null, null, true));
          continue;
        }
        // Disable binary<->non-string casts.
        // TODO(IMPALA-7998): invalid cases could be identified in ScalarType's
        //                    compatibility matrix
        if (fromType.isBinary() && !toType.isString()) {
          continue;
        }
        if (toType.isBinary() && !fromType.isString()) {
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

  public String getCastFormatWithEscapedSingleQuotes() {
    Preconditions.checkNotNull(castFormat_);
    Preconditions.checkState(!castFormat_.isEmpty());
    StringBuilder result = new StringBuilder();
    for(int i = 0; i < castFormat_.length(); ++i) {
      char currentChar = castFormat_.charAt(i);
      if (currentChar == '\'') {
        // Count the preceeding backslashes
        int backslashCount = 0;
        int j = i - 1;
        while (j >= 0 && castFormat_.charAt(j) == '\\') {
          ++backslashCount;
          --j;
        }
        // If the single quote is not escaped then adds an extra backslash to escape it.
        if (backslashCount % 2 == 0) result.append('\\');
      }
      result.append(currentChar);
    }
    return result.toString();
  }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    if (isImplicit_) {
      if (options.showImplictCasts()) {
        // for implicit casts, targetTypeDef_ and castFormat_ are null
        return "CAST(" + getChild(0).toSql(options) + " AS " + type_.toSql() + ")";
      } else {
        return getChild(0).toSql(options);
      }
    }
    String formatClause = "";
    if (castFormat_ != null && !castFormat_.isEmpty()) {
      formatClause = " FORMAT '" + getCastFormatWithEscapedSingleQuotes() + "'";
    }
    return "CAST(" + getChild(0).toSql(options) + " AS " + targetTypeDef_.toString()
        + formatClause + ")";
  }

  @Override
  protected void treeToThriftHelper(TExpr container, ThriftSerializationCtx serialCtx) {
    if (noOp_) {
      getChild(0).treeToThriftHelper(container, serialCtx);
      return;
    }
    super.treeToThriftHelper(container, serialCtx);
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.FUNCTION_CALL;
    // Sets cast_expr in case FORMAT clause was provided and this is a cast between a
    // datetime and a string.
    if (null != castFormat_ &&
        (type_.isDateOrTimeType() && getChild(0).getType().isStringType() ||
         type_.isStringType() && getChild(0).getType().isDateOrTimeType())) {
      msg.cast_expr = new TCastExpr(castFormat_);
    }
  }

  @Override
  public String debugString() {
    return MoreObjects.toStringHelper(this)
        .add("isImplicit", isImplicit_)
        .add("target", type_)
        .add("format", castFormat_)
        .addValue(super.debugString())
        .toString();
  }

  public boolean isImplicit() { return isImplicit_; }

  public TypeCompatibility getCompatibility() { return compatibility_; }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(!isImplicit_);
    targetTypeDef_.analyze(analyzer);
    type_ = targetTypeDef_.getType();
    analyze();
  }

  @Override
  protected float computeEvalCost() {
    // By default, assume that casting requires some non-trivial logic - i.e. is similar
    // to calling an arbitrary function.
    float castCost = FUNCTION_CALL_COST;
    Type inType = children_.get(0).getType();
    if ((type_.isVarchar() || type_.getPrimitiveType() == PrimitiveType.STRING) &&
        (inType.isVarchar() || inType.getPrimitiveType() == PrimitiveType.STRING)) {
      // Casting between variable-length string types requires at most adjusting the
      // length field.
      castCost = ARITHMETIC_OP_COST;
    } else if (
        (type_.isFloatingPointType() || type_.isIntegerType() || type_.isBoolean()) &&
        (inType.isFloatingPointType() || inType.isIntegerType() ||  inType.isBoolean())) {
      // Casting between machine primitive types can be done cheaply, e.g. with a
      // machine instruction or two.
      castCost = ARITHMETIC_OP_COST;
    }
    return getChild(0).hasCost() ? getChild(0).getCost() + castCost : UNKNOWN_COST;
  }

  private void analyze() throws AnalysisException {
    Preconditions.checkNotNull(type_);
    if (type_.isComplexType()) {
      throw new AnalysisException(
          "Unsupported cast to complex type: " + type_.toSql());
    }

    boolean twoStepCastNeeded =
        type_.getPrimitiveType() == PrimitiveType.CHAR &&
        children_.get(0).getType().getPrimitiveType() != PrimitiveType.STRING &&
        children_.get(0).getType().getPrimitiveType() != PrimitiveType.CHAR;
    if (twoStepCastNeeded) {
      // Back end functions only exist to cast string types to CHAR, there is not a cast
      // for every type since it is redundant with STRING. Casts to go through 2 casts:
      // (1) cast to string, to stringify the value
      // (2) cast to CHAR, to truncate or pad with spaces
      CastExpr tostring =
          new CastExpr(ScalarType.STRING, children_.get(0), castFormat_, compatibility_);
      tostring.analyze();
      children_.set(0, tostring);
    }

    if (null != castFormat_ && !twoStepCastNeeded) {
      if (!(type_.isDateOrTimeType() && getChild(0).getType().isStringType()) &&
          !(type_.isStringType() && getChild(0).getType().isDateOrTimeType())) {
        // FORMAT clause works only for casting between date types and string types
        throw new AnalysisException("FORMAT clause is not applicable from " +
            getChild(0).getType() + " to " + type_);
      }
      if (castFormat_.isEmpty()) {
        throw new AnalysisException("FORMAT clause can't be empty");
      }
    }

    if (children_.get(0) instanceof NumericLiteral && type_.isFloatingPointType()) {
      // Special case casting a decimal literal to a floating point number. The decimal
      // literal can be interpreted as either and we want to avoid casts since that can
      // result in loss of accuracy. However, if 'type_' is FLOAT and the value does not
      // fit in a FLOAT, we do not do an unchecked conversion
      // ('NumericLiteral.explicitlyCastToFloat()') here but let the conversion fail in
      // the BE.
      NumericLiteral child = (NumericLiteral) children_.get(0);
      final boolean isOverflow = NumericLiteral.isOverflow(child.getValue(), type_);
      if (type_.isScalarType(PrimitiveType.FLOAT)) {
        if (!isOverflow) {
          ((NumericLiteral)children_.get(0)).explicitlyCastToFloat(type_);
        }
      } else {
        Preconditions.checkState(type_.isScalarType(PrimitiveType.DOUBLE));
        Preconditions.checkState(!isOverflow);
        ((NumericLiteral)children_.get(0)).explicitlyCastToFloat(type_);
      }
    }

    if (children_.get(0).getType().isNull()) {
      // Make sure BE never sees TYPE_NULL
      uncheckedCastChild(type_, 0);
    }

    // Ensure child has non-null type (even if it's a null literal). This is required
    // for the UDF interface.
    if (Expr.IS_NULL_LITERAL.apply(children_.get(0))) {
      NullLiteral nullChild = (NullLiteral)(children_.get(0));
      nullChild.uncheckedCastTo(type_);
    }

    Type childType = children_.get(0).type_;
    Preconditions.checkState(!childType.isNull());

    // IMPALA-4550: We always need to set noOp_ to the correct value, since we could
    // be performing a subsequent analysis run and its correct value might have changed.
    // This can happen if the child node gets substituted and its type changes.
    noOp_ = childType.equals(type_);
    if (noOp_) return;

    FunctionName fnName = new FunctionName(BuiltinsDb.NAME, getFnName(type_));
    Type[] args = { childType };
    Function searchDesc = new Function(fnName, args, Type.INVALID, false);
    if (isImplicit_) {
      fn_ = BuiltinsDb.getInstance().getFunction(searchDesc,
          CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
      Preconditions.checkState(fn_ != null);
    } else {
      fn_ = BuiltinsDb.getInstance().getFunction(searchDesc,
          CompareMode.IS_IDENTICAL);
      if (fn_ == null) {
        // allow for promotion from CHAR to STRING; only if no exact match is found
        fn_ =  BuiltinsDb.getInstance().getFunction(
            searchDesc.promoteCharsToStrings(), CompareMode.IS_IDENTICAL);
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
  public boolean isImplicitCast() {
    return isImplicit();
  }

  @Override
  protected boolean localEquals(Expr that) {
    if (!super.localEquals(that)) return false;
    CastExpr other = (CastExpr) that;
    return isImplicit_ == other.isImplicit_
        && type_.equals(other.type_);
  }

  @Override
  public int hashCode() {
    if (isImplicit()) {
      return children_.get(0).hashCode();
    }
    return Objects.hash(super.localHash(), type_, children_);
  }

  // Pass through since cast's are cheap.
  @Override
  public boolean shouldConvertToCNF() {
    return getChild(0).shouldConvertToCNF();
  }

  @Override
  public Expr clone() { return new CastExpr(this); }
}
