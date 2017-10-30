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

import java.util.List;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.TreeNode;
import org.apache.impala.thrift.TAggregateExpr;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;
import org.apache.impala.thrift.TFunctionBinaryType;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class FunctionCallExpr extends Expr {
  private final FunctionName fnName_;
  private final FunctionParams params_;
  private boolean isAnalyticFnCall_ = false;
  private boolean isInternalFnCall_ = false;

  // Non-null iff this is an aggregation function that executes the Merge() step. This
  // is an analyzed clone of the FunctionCallExpr that executes the Update() function
  // feeding into this Merge(). This is stored so that we can access the types of the
  // original input argument exprs. Note that the nullness affects the behaviour of
  // resetAnalysisState(), which is used during expr substitution.
  private FunctionCallExpr mergeAggInputFn_;

  // Printed in toSqlImpl(), if set. Used for merge agg fns.
  private String label_;

  public FunctionCallExpr(String functionName, List<Expr> params) {
    this(new FunctionName(functionName), new FunctionParams(false, params));
  }

  public FunctionCallExpr(FunctionName fnName, List<Expr> params) {
    this(fnName, new FunctionParams(false, params));
  }

  public FunctionCallExpr(FunctionName fnName, FunctionParams params) {
    this(fnName, params, null);
  }

  private FunctionCallExpr(FunctionName fnName, FunctionParams params,
      FunctionCallExpr mergeAggInputFn) {
    super();
    fnName_ = fnName;
    params_ = params;
    mergeAggInputFn_ =
        mergeAggInputFn == null ? null : (FunctionCallExpr)mergeAggInputFn.clone();
    if (params.exprs() != null) children_ = Lists.newArrayList(params_.exprs());
  }

  /**
   * Returns an Expr that evaluates the function call <fnName>(<params>). The returned
   * Expr is not necessarily a FunctionCallExpr (example: DECODE())
   */
  public static Expr createExpr(FunctionName fnName, FunctionParams params) {
    FunctionCallExpr functionCallExpr = new FunctionCallExpr(fnName, params);
    if (functionNameEqualsBuiltin(fnName, "decode")) {
      return new CaseExpr(functionCallExpr);
    }
    if (functionNameEqualsBuiltin(fnName, "nvl2")) {
      List<Expr> plist = Lists.newArrayList(params.exprs());
      if (!plist.isEmpty()) {
        plist.set(0, new IsNullPredicate(plist.get(0), true));
      }
      return new FunctionCallExpr("if", plist);
    }
    // nullif(x, y) -> if(x DISTINCT FROM y, x, NULL)
    if (functionNameEqualsBuiltin(fnName, "nullif") && params.size() == 2) {
      return new FunctionCallExpr("if", Lists.newArrayList(
          new BinaryPredicate(BinaryPredicate.Operator.DISTINCT_FROM, params.exprs().get(0),
            params.exprs().get(1)), // x IS DISTINCT FROM y
          params.exprs().get(0), // x
          new NullLiteral() // NULL
      ));
    }
    return functionCallExpr;
  }

  /** Returns true if fnName is a built-in with given name. */
  private static boolean functionNameEqualsBuiltin(FunctionName fnName, String name) {
    return fnName.getFnNamePath().size() == 1
           && fnName.getFnNamePath().get(0).equalsIgnoreCase(name)
        || fnName.getFnNamePath().size() == 2
           && fnName.getFnNamePath().get(0).equals(Catalog.BUILTINS_DB)
           && fnName.getFnNamePath().get(1).equalsIgnoreCase(name);
  }

  /**
   * Returns a new function call expr on the given params for performing the merge()
   * step of the given aggregate function.
   */
  public static FunctionCallExpr createMergeAggCall(
      FunctionCallExpr agg, List<Expr> params) {
    Preconditions.checkState(agg.isAnalyzed());
    Preconditions.checkState(agg.isAggregateFunction());
    // If the input aggregate function is already a merge aggregate function (due to
    // 2-phase aggregation), its input types will be the intermediate value types. The
    // original input argument exprs are in 'agg.mergeAggInputFn_' so use it instead.
    FunctionCallExpr mergeAggInputFn = agg.isMergeAggFn() ? agg.mergeAggInputFn_ : agg;
    FunctionCallExpr result = new FunctionCallExpr(
        agg.fnName_, new FunctionParams(false, params), mergeAggInputFn);
    // Inherit the function object from 'agg'.
    result.fn_ = agg.fn_;
    result.type_ = agg.type_;
    // Set an explicit label based on the input agg.
    if (agg.isMergeAggFn()) {
      result.label_ = agg.label_;
    } else {
      // fn(input) becomes fn:merge(input).
      result.label_ = agg.toSql().replaceFirst(agg.fnName_.toString(),
          agg.fnName_.toString() + ":merge");
    }
    Preconditions.checkState(!result.type_.isWildcardDecimal());
    return result;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected FunctionCallExpr(FunctionCallExpr other) {
    super(other);
    fnName_ = other.fnName_;
    isAnalyticFnCall_ = other.isAnalyticFnCall_;
    isInternalFnCall_ = other.isInternalFnCall_;
    mergeAggInputFn_ = other.mergeAggInputFn_ == null ?
        null : (FunctionCallExpr)other.mergeAggInputFn_.clone();
    // Clone the params in a way that keeps the children_ and the params.exprs()
    // in sync. The children have already been cloned in the super c'tor.
    if (other.params_.isStar()) {
      Preconditions.checkState(children_.isEmpty());
      params_ = FunctionParams.createStarParam();
    } else {
      params_ = new FunctionParams(other.params_.isDistinct(),
          other.params_.isIgnoreNulls(), children_);
    }
    label_ = other.label_;
  }

  public boolean isMergeAggFn() { return mergeAggInputFn_ != null; }

  @Override
  public void resetAnalysisState() {
    super.resetAnalysisState();
    // Resolving merge agg functions after substitution may fail e.g., if the
    // intermediate agg type is not the same as the output type. Preserve the original
    // fn_ such that analyze() hits the special-case code for merge agg fns that
    // handles this case.
    if (!isMergeAggFn()) fn_ = null;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    FunctionCallExpr o = (FunctionCallExpr)obj;
    return fnName_.equals(o.fnName_) &&
           params_.isDistinct() == o.params_.isDistinct() &&
           params_.isIgnoreNulls() == o.params_.isIgnoreNulls() &&
           params_.isStar() == o.params_.isStar();
  }

  @Override
  public String toSqlImpl() {
    if (label_ != null) return label_;
    // Merge agg fns should have an explicit label.
    Preconditions.checkState(!isMergeAggFn());
    StringBuilder sb = new StringBuilder();
    sb.append(fnName_).append("(");
    if (params_.isStar()) sb.append("*");
    if (params_.isDistinct()) sb.append("DISTINCT ");
    sb.append(Joiner.on(", ").join(childrenToSql()));
    if (params_.isIgnoreNulls()) sb.append(" IGNORE NULLS");
    sb.append(")");
    return sb.toString();
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("name", fnName_)
        .add("isStar", params_.isStar())
        .add("isDistinct", params_.isDistinct())
        .add("isIgnoreNulls", params_.isIgnoreNulls())
        .addValue(super.debugString())
        .toString();
  }

  public FunctionParams getParams() { return params_; }
  public boolean isScalarFunction() {
    Preconditions.checkNotNull(fn_);
    return fn_ instanceof ScalarFunction ;
  }

  public Type getReturnType() {
    Preconditions.checkNotNull(fn_);
    return fn_.getReturnType();
  }

  /**
   * Returns true if this is a call to a non-analytic aggregate function.
   */
  public boolean isAggregateFunction() {
    Preconditions.checkNotNull(fn_);
    return fn_ instanceof AggregateFunction && !isAnalyticFnCall_;
  }

  /**
   * Returns true if this is a call to an aggregate function that returns
   * non-null on an empty input (e.g. count).
   */
  public boolean returnsNonNullOnEmpty() {
    Preconditions.checkNotNull(fn_);
    return fn_ instanceof AggregateFunction &&
        ((AggregateFunction)fn_).returnsNonNullOnEmpty();
  }

  public boolean isDistinct() {
    Preconditions.checkState(isAggregateFunction());
    return params_.isDistinct();
  }

  public boolean ignoresDistinct() {
    Preconditions.checkState(isAggregateFunction());
    return ((AggregateFunction)fn_).ignoresDistinct();
  }

  public FunctionName getFnName() { return fnName_; }
  public void setIsAnalyticFnCall(boolean v) { isAnalyticFnCall_ = v; }
  public void setIsInternalFnCall(boolean v) { isInternalFnCall_ = v; }

  static boolean isNondeterministicBuiltinFnName(String fnName) {
    if (fnName.equalsIgnoreCase("rand") || fnName.equalsIgnoreCase("random")
        || fnName.equalsIgnoreCase("uuid")) {
      return true;
    }
    return false;
  }

  /**
   * Returns true if function is a non-deterministic builtin function, i.e. for a fixed
   * input, it may not always produce the same output for every invocation.
   * Functions that use randomness or variable runtime state are non-deterministic.
   * This only applies to builtin functions, and does not provide any information
   * about user defined functions.
   */
  public boolean isNondeterministicBuiltinFn() {
    String fnName = fnName_.getFunction();
    return isNondeterministicBuiltinFnName(fnName);
  }

  @Override
  protected void toThrift(TExprNode msg) {
    if (isAggregateFunction() || isAnalyticFnCall_) {
      msg.node_type = TExprNodeType.AGGREGATE_EXPR;
      List<TColumnType> aggFnArgTypes = Lists.newArrayList();
      FunctionCallExpr inputAggFn = isMergeAggFn() ? mergeAggInputFn_ : this;
      for (Expr child: inputAggFn.children_) {
        aggFnArgTypes.add(child.getType().toThrift());
      }
      msg.setAgg_expr(new TAggregateExpr(isMergeAggFn(), aggFnArgTypes));
    } else {
      msg.node_type = TExprNodeType.FUNCTION_CALL;
    }
  }

  @Override
  protected boolean isConstantImpl() {
    // TODO: we can't correctly determine const-ness before analyzing 'fn_'. We should
    // rework logic so that we do not call this function on unanalyzed exprs.
    // Aggregate functions are never constant.
    if (fn_ instanceof AggregateFunction) return false;

    String fnName = fnName_.getFunction();
    if (fnName == null) {
      // This expr has not been analyzed yet, get the function name from the path.
      List<String> path = fnName_.getFnNamePath();
      fnName = path.get(path.size() - 1);
    }
    // Non-deterministic functions are never constant.
    if (isNondeterministicBuiltinFnName(fnName)) {
      return false;
    }
    // Sleep is a special function for testing.
    if (fnName.equalsIgnoreCase("sleep")) return false;
    return super.isConstantImpl();
  }

  // Provide better error message for some aggregate builtins. These can be
  // a bit more user friendly than a generic function not found.
  // TODO: should we bother to do this? We could also improve the general
  // error messages. For example, listing the alternatives.
  protected String getFunctionNotFoundError(Type[] argTypes) {
    if (fnName_.isBuiltin()) {
      // Some custom error message for builtins
      if (params_.isStar()) {
        return "'*' can only be used in conjunction with COUNT";
      }
      if (fnName_.getFunction().equalsIgnoreCase("count")) {
        if (!params_.isDistinct() && argTypes.length > 1) {
          return "COUNT must have DISTINCT for multiple arguments: " + toSql();
        }
      }
      if (fnName_.getFunction().equalsIgnoreCase("sum")) {
        return "SUM requires a numeric parameter: " + toSql();
      }
      if (fnName_.getFunction().equalsIgnoreCase("avg")) {
        return "AVG requires a numeric or timestamp parameter: " + toSql();
      }
    }

    String[] argTypesSql = new String[argTypes.length];
    for (int i = 0; i < argTypes.length; ++i) {
      argTypesSql[i] = argTypes[i].toSql();
    }
    return String.format(
        "No matching function with signature: %s(%s).",
        fnName_, params_.isStar() ? "*" : Joiner.on(", ").join(argTypesSql));
  }

  /**
   * Builtins that return decimals are specified as the wildcard decimal(decimal(*,*))
   * and the specific decimal can only be determined based on the inputs. We currently
   * don't have a mechanism to specify this with the UDF interface. Until we add
   * that (i.e. allowing UDFs to participate in the planning phase), we will
   * manually resolve the wildcard types for the few functions that need it.
   * This can only be called for functions that return wildcard decimals and the first
   * argument is a wildcard decimal.
   * TODO: this prevents UDFs from using wildcard decimals and is in general not scalable.
   * We should add a prepare_fn() to UDFs for doing this.
   */
  private Type resolveDecimalReturnType(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(type_.isWildcardDecimal());
    Preconditions.checkState(fn_.getBinaryType() == TFunctionBinaryType.BUILTIN);
    Preconditions.checkState(children_.size() > 0);

    // Find first decimal input (some functions, such as if(), begin with non-decimal
    // arguments).
    ScalarType childType = null;
    for (Expr child : children_) {
      if (child.type_.isDecimal()) {
        childType = (ScalarType) child.type_;
        break;
      }
    }
    Preconditions.checkState(childType != null && !childType.isWildcardDecimal());
    Type returnType = childType;

    if (fnName_.getFunction().equalsIgnoreCase("sum")) {
      return childType.getMaxResolutionType();
    }

    int digitsBefore = childType.decimalPrecision() - childType.decimalScale();
    int digitsAfter = childType.decimalScale();
    if (fnName_.getFunction().equalsIgnoreCase("avg") &&
        analyzer.getQueryOptions().isDecimal_v2()) {
      // AVG() always gets at least MIN_ADJUSTED_SCALE decimal places since it performs
      // an implicit divide. The output type isn't always the same as SUM()/COUNT().
      // Scale is set the same as MS SQL Server, which takes the max of the input scale
      // and MIN_ADJUST_SCALE. For precision, MS SQL always sets it to 38. We choose to
      // trim it down to the size that's needed because the absolute value of the result
      // is less than the absolute value of the largest input. Using a smaller precision
      // allows for better DECIMAL types to be chosen for the overall expression when
      // AVG() is a subexpression. For DECIMAL_V1, we set the output type to be the same
      // as the input type.
      int resultScale = Math.max(ScalarType.MIN_ADJUSTED_SCALE, digitsAfter);
      int resultPrecision = digitsBefore + resultScale;
      return ScalarType.createAdjustedDecimalType(resultPrecision, resultScale);
    } else if (fnName_.getFunction().equalsIgnoreCase("ceil") ||
               fnName_.getFunction().equalsIgnoreCase("ceiling") ||
               fnName_.getFunction().equals("floor") ||
               fnName_.getFunction().equals("dfloor")) {
      // These functions just return with scale 0 but can trigger rounding. We need
      // to increase the precision by 1 to handle that.
      ++digitsBefore;
      digitsAfter = 0;
    } else if (fnName_.getFunction().equalsIgnoreCase("truncate") ||
               fnName_.getFunction().equalsIgnoreCase("dtrunc") ||
               fnName_.getFunction().equalsIgnoreCase("trunc") ||
               fnName_.getFunction().equalsIgnoreCase("round") ||
               fnName_.getFunction().equalsIgnoreCase("dround")) {
      if (children_.size() > 1) {
        // The second argument to these functions is the desired scale, otherwise
        // the default is 0.
        Preconditions.checkState(children_.size() == 2);
        if (children_.get(1).isNullLiteral()) {
          throw new AnalysisException(fnName_.getFunction() +
              "() cannot be called with a NULL second argument.");
        }

        if (!children_.get(1).isConstant()) {
          // We don't allow calling truncate or round with a non-constant second
          // (desired scale) argument. e.g. select round(col1, col2). This would
          // mean we don't know the scale of the resulting type and would need some
          // kind of dynamic type handling which is not yet possible. This seems like
          // a reasonable restriction.
          throw new AnalysisException(fnName_.getFunction() +
              "() must be called with a constant second argument.");
        }
        NumericLiteral scaleLiteral = (NumericLiteral) LiteralExpr.create(
            children_.get(1), analyzer.getQueryCtx());
        digitsAfter = (int)scaleLiteral.getLongValue();
        if (Math.abs(digitsAfter) > ScalarType.MAX_SCALE) {
          throw new AnalysisException("Cannot round/truncate to scales greater than " +
              ScalarType.MAX_SCALE + ".");
        }
        // Round/Truncate to a negative scale means to round to the digit before
        // the decimal e.g. round(1234.56, -2) would be 1200.
        // The resulting scale is always 0.
        digitsAfter = Math.max(digitsAfter, 0);
      } else {
        // Round()/Truncate() with no second argument.
        digitsAfter = 0;
      }

      if ((fnName_.getFunction().equalsIgnoreCase("round") ||
           fnName_.getFunction().equalsIgnoreCase("dround")) &&
          digitsAfter < childType.decimalScale()) {
        // If we are rounding to fewer decimal places, it's possible we need another
        // digit before the decimal.
        ++digitsBefore;
      }
    }
    Preconditions.checkState(returnType.isDecimal() && !returnType.isWildcardDecimal());
    return ScalarType.createClippedDecimalType(digitsBefore + digitsAfter, digitsAfter);
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    fnName_.analyze(analyzer);

    if (isMergeAggFn()) {
      // This is the function call expr after splitting up to a merge aggregation.
      // The function has already been analyzed so just do the minimal sanity
      // check here.
      AggregateFunction aggFn = (AggregateFunction)fn_;
      Preconditions.checkNotNull(aggFn);
      Type intermediateType = aggFn.getIntermediateType();
      if (intermediateType == null) intermediateType = type_;
      Preconditions.checkState(!type_.isWildcardDecimal());
      return;
    }

    Type[] argTypes = collectChildReturnTypes();

    // User needs DB access.
    Db db = analyzer.getDb(fnName_.getDb(), Privilege.VIEW_METADATA, true);
    if (!db.containsFunction(fnName_.getFunction())) {
      throw new AnalysisException(fnName_ + "() unknown");
    }

    if (fnName_.getFunction().equals("count") && params_.isDistinct()) {
      // Treat COUNT(DISTINCT ...) special because of how we do the rewrite.
      // There is no version of COUNT() that takes more than 1 argument but after
      // the rewrite, we only need count(*).
      // TODO: fix how we rewrite count distinct.
      argTypes = new Type[0];
      Function searchDesc = new Function(fnName_, argTypes, Type.INVALID, false);
      fn_ = db.getFunction(searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
      type_ = fn_.getReturnType();
      // Make sure BE doesn't see any TYPE_NULL exprs
      for (int i = 0; i < children_.size(); ++i) {
        if (getChild(i).getType().isNull()) {
          uncheckedCastChild(ScalarType.BOOLEAN, i);
        }
      }
      return;
    }

    // TODO: We allow implicit cast from string->timestamp but only
    // support avg(timestamp). This means avg(string_col) would work
    // from our casting rules. This is not right.
    // We need to revisit where implicit casts are allowed for string
    // to timestamp
    if (fnName_.getFunction().equalsIgnoreCase("avg") &&
      children_.size() == 1 && children_.get(0).getType().isStringType()) {
      throw new AnalysisException(
          "AVG requires a numeric or timestamp parameter: " + toSql());
    }

    Function searchDesc = new Function(fnName_, argTypes, Type.INVALID, false);
    fn_ = db.getFunction(searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    if (fn_ == null || (!isInternalFnCall_ && !fn_.userVisible())) {
      throw new AnalysisException(getFunctionNotFoundError(argTypes));
    }

    if (isAggregateFunction()) {
      // subexprs must not contain aggregates
      if (TreeNode.contains(children_, Expr.isAggregatePredicate())) {
        throw new AnalysisException(
            "aggregate function must not contain aggregate parameters: " + this.toSql());
      }

      // .. or analytic exprs
      if (Expr.contains(children_, AnalyticExpr.class)) {
        throw new AnalysisException(
            "aggregate function must not contain analytic parameters: " + this.toSql());
      }

      // The catalog contains count() with no arguments to handle count(*) but don't
      // accept count().
      // TODO: can this be handled more cleanly. It does seem like a special case since
      // no other aggregate functions (currently) can accept '*'.
      if (fnName_.getFunction().equalsIgnoreCase("count") &&
          !params_.isStar() && children_.size() == 0) {
        throw new AnalysisException("count() is not allowed.");
      }

      // TODO: the distinct rewrite does not handle this but why?
      if (params_.isDistinct()) {
        // The second argument in group_concat(distinct) must be a constant expr that
        // returns a string.
        if (fnName_.getFunction().equalsIgnoreCase("group_concat")
            && getChildren().size() == 2
            && !getChild(1).isConstant()) {
            throw new AnalysisException("Second parameter in GROUP_CONCAT(DISTINCT)" +
                " must be a constant expression that returns a string.");
        }
        if (fn_.getBinaryType() != TFunctionBinaryType.BUILTIN) {
          throw new AnalysisException("User defined aggregates do not support DISTINCT.");
        }
      }

      AggregateFunction aggFn = (AggregateFunction)fn_;
      if (aggFn.ignoresDistinct()) params_.setIsDistinct(false);
    }

    if (params_.isIgnoreNulls() && !isAnalyticFnCall_) {
      throw new AnalysisException("Function " + fnName_.getFunction().toUpperCase()
          + " does not accept the keyword IGNORE NULLS.");
    }

    if (isScalarFunction()) validateScalarFnParams(params_);
    if (fn_ instanceof AggregateFunction
        && ((AggregateFunction) fn_).isAnalyticFn()
        && !((AggregateFunction) fn_).isAggregateFn()
        && !isAnalyticFnCall_) {
      throw new AnalysisException(
          "Analytic function requires an OVER clause: " + toSql());
    }

    castForFunctionCall(false);
    type_ = fn_.getReturnType();
    if (type_.isDecimal() && type_.isWildcardDecimal()) {
      type_ = resolveDecimalReturnType(analyzer);
    }

    // We do not allow any function to return a type CHAR or VARCHAR
    // TODO add support for CHAR(N) and VARCHAR(N) return values in post 2.0,
    // support for this was not added to the backend in 2.0
    if (type_.isWildcardChar() || type_.isWildcardVarchar()) {
      type_ = ScalarType.STRING;
    }
  }

  @Override
  protected float computeEvalCost() {
    // TODO(tmarshall): Differentiate based on the specific function.
    return hasChildCosts() ? getChildCosts() + FUNCTION_CALL_COST : UNKNOWN_COST;
  }

  public FunctionCallExpr getMergeAggInputFn() { return mergeAggInputFn_; }
  public void setMergeAggInputFn(FunctionCallExpr fn) { mergeAggInputFn_ = fn; }

  /**
   * Checks that no special aggregate params are included in 'params' that would be
   * invalid for a scalar function. Analysis of the param exprs is not done.
   */
  static void validateScalarFnParams(FunctionParams params)
      throws AnalysisException {
    if (params.isStar()) {
      throw new AnalysisException("Cannot pass '*' to scalar function.");
    }
    if (params.isDistinct()) {
      throw new AnalysisException("Cannot pass 'DISTINCT' to scalar function.");
    }
  }

  /**
   * Validate that the internal state, specifically types, is consistent between the
   * the Update() and Merge() aggregate functions.
   */
  void validateMergeAggFn(FunctionCallExpr inputAggFn) {
    Preconditions.checkState(isMergeAggFn());
    List<Expr> copiedInputExprs = mergeAggInputFn_.getChildren();
    List<Expr> inputExprs = inputAggFn.isMergeAggFn() ?
        inputAggFn.mergeAggInputFn_.getChildren() : inputAggFn.getChildren();
    Preconditions.checkState(copiedInputExprs.size() == inputExprs.size());
    for (int i = 0; i < inputExprs.size(); ++i) {
      Type copiedInputType = copiedInputExprs.get(i).getType();
      Type inputType = inputExprs.get(i).getType();
      Preconditions.checkState(copiedInputType.equals(inputType),
          String.format("Copied expr %s arg type %s differs from input expr type %s " +
            "in original expr %s", toSql(), copiedInputType.toSql(),
            inputType.toSql(), inputAggFn.toSql()));
    }
  }

  @Override
  public Expr clone() { return new FunctionCallExpr(this); }

  @Override
  protected Expr substituteImpl(ExprSubstitutionMap smap, Analyzer analyzer) {
    Expr e = super.substituteImpl(smap, analyzer);
    if (!(e instanceof FunctionCallExpr)) return e;
    FunctionCallExpr fn = (FunctionCallExpr) e;
    FunctionCallExpr mergeFn = fn.getMergeAggInputFn();
    if (mergeFn != null) {
      // The merge function needs to be substituted as well.
      Expr substitutedFn = mergeFn.substitute(smap, analyzer, true);
      Preconditions.checkState(substitutedFn instanceof FunctionCallExpr);
      fn.setMergeAggInputFn((FunctionCallExpr) substitutedFn);
    }
    return e;
  }

}
