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

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.AggregateFunction;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.ScalarFunction;
import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.TreeNode;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TFunctionBinaryType;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class FunctionCallExpr extends Expr {
  private final FunctionName fnName_;
  private final FunctionParams params_;

  // Indicates whether this is a merge aggregation function. This flag affects
  // resetAnalysisState() which is used during expr substitution.
  // TODO: Re-think how we generate the merge aggregation and remove this flag.
  private boolean isMergeAggFn_ = false;

  public FunctionCallExpr(String functionName, List<Expr> params) {
    this(new FunctionName(functionName), new FunctionParams(false, params));
  }

  public FunctionCallExpr(FunctionName fnName, List<Expr> params) {
    this(fnName, new FunctionParams(false, params));
  }

  public FunctionCallExpr(FunctionName fnName, FunctionParams params) {
    super();
    fnName_ = fnName;
    params_ = params;
    if (params.exprs() != null) children_.addAll(params.exprs());
  }

  // Constructs the same agg function with new params.
  public FunctionCallExpr(FunctionCallExpr e, FunctionParams params) {
    Preconditions.checkState(e.isAnalyzed_);
    Preconditions.checkState(e.isAggregateFunction());
    fnName_ = e.fnName_;
    params_ = params;
    // Just inherit the function object from 'e'.
    fn_ = e.fn_;
    type_ = e.type_;
    Preconditions.checkState(!type_.isWildcardDecimal());
    if (params.exprs() != null) children_.addAll(params.exprs());
  }

  // This is a total hack because of how we remap count/avg aggregate functions.
  // This needs to be removed when we stop doing the rewrites.
  public FunctionCallExpr(String name, FunctionParams params) {
    fnName_ = new FunctionName(Catalog.BUILTINS_DB, name);
    params_ = params;
    // Just inherit the function object from 'e'.
    if (params.exprs() != null) children_.addAll(params.exprs());
  }

  /**
   * Copy c'tor used in clone().
   */
  protected FunctionCallExpr(FunctionCallExpr other) {
    super(other);
    fnName_ = other.fnName_;
    isMergeAggFn_ = other.isMergeAggFn_;
    // No need to deep clone the params, its exprs are already in children_.
    params_ = other.params_;
  }

  @Override
  public void resetAnalysisState() {
    isAnalyzed_ = false;
    // Resolving merge agg functions after substitution may fail e.g., if the
    // intermediate agg type is not the same as the output type. Preserve the original
    // fn_ such that analyze() hits the special-case code for merge agg fns that
    // handles this case.
    if (!isMergeAggFn_) fn_ = null;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    FunctionCallExpr o = (FunctionCallExpr)obj;
    return fnName_.equals(o.fnName_) &&
           params_.isDistinct() == o.params_.isDistinct() &&
           params_.isStar() == o.params_.isStar();
  }

  @Override
  public String toSqlImpl() {
    StringBuilder sb = new StringBuilder();
    sb.append(fnName_).append("(");
    if (params_.isStar()) sb.append("*");
    if (params_.isDistinct()) sb.append("DISTINCT ");
    sb.append(Joiner.on(", ").join(childrenToSql())).append(")");
    return sb.toString();
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("name", fnName_)
        .add("isStar", params_.isStar())
        .add("isDistinct", params_.isDistinct())
        .addValue(super.debugString())
        .toString();
  }

  public FunctionParams getParams() { return params_; }
  public boolean isScalarFunction() {
    Preconditions.checkState(fn_ != null);
    return fn_ instanceof ScalarFunction ;
  }

  public boolean isAggregateFunction() {
    Preconditions.checkState(fn_ != null);
    return fn_ instanceof AggregateFunction;
  }

  public boolean isDistinct() {
    Preconditions.checkState(isAggregateFunction());
    return params_.isDistinct();
  }

  public void setIsMergeAggFn() { isMergeAggFn_ = true; }
  public FunctionName getFnName() { return fnName_; }

  @Override
  protected void toThrift(TExprNode msg) {
    if (Expr.isAggregatePredicate().apply(this)) {
      msg.node_type = TExprNodeType.AGGREGATE_EXPR;
    } else {
      ScalarFunction fn = (ScalarFunction)fn_;
      if (fn.getBinaryType() == TFunctionBinaryType.BUILTIN &&
          !fn.isUdfInterface()) {
        msg.node_type = TExprNodeType.COMPUTE_FUNCTION_CALL;
      } else {
        msg.node_type = TExprNodeType.FUNCTION_CALL;
      }

    }
  }

  // Provide better error message for some aggregate builtins. These can be
  // a bit more user friendly than a generic function not found.
  // TODO: should we bother to do this? We could also improve the general
  // error messages. For example, listing the alternatives.
  private String getFunctionNotFoundError(Type[] argTypes) {
    if (fnName_.isBuiltin_) {
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
    if (fnName_.getFunction().equalsIgnoreCase("ceil") ||
               fnName_.getFunction().equalsIgnoreCase("ceiling") ||
               fnName_.getFunction().equals("floor")) {
      // These functions just return with scale 0 but can trigger rounding. We need
      // to increase the precision by 1 to handle that.
      ++digitsBefore;
      digitsAfter = 0;
    } else if (fnName_.getFunction().equalsIgnoreCase("truncate") ||
               fnName_.getFunction().equalsIgnoreCase("round")) {
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

      if (fnName_.getFunction().equalsIgnoreCase("round") &&
          digitsAfter < childType.decimalScale()) {
        // If we are rounding to fewer decimal places, it's possible we need another
        // digit before the decimal.
        ++digitsBefore;
      }
    }
    Preconditions.checkState(returnType.isDecimal() && !returnType.isWildcardDecimal());
    return ScalarType.createDecimalTypeInternal(digitsBefore + digitsAfter, digitsAfter);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    fnName_.analyze(analyzer);

    if (fn_ != null && Expr.isAggregatePredicate().apply(this)) {
      // This is the function call expr after splitting up to a merge aggregation.
      // The function has already been analyzed so just do the minimal sanity
      // check here.
      // TODO: rethink how we generate the merge aggregation.
      AggregateFunction aggFn = (AggregateFunction)fn_;
      Type intermediateType = aggFn.getIntermediateType();
      if (intermediateType == null) intermediateType = type_;
      // TODO: this needs to change when the intermediate type != the return type
      Preconditions.checkArgument(intermediateType.equals(fn_.getReturnType()));
      Preconditions.checkState(!type_.isWildcardDecimal());
      return;
    }

    Type[] argTypes = collectChildReturnTypes();

    // User needs DB access.
    Db db = analyzer.getDb(fnName_.getDb(), Privilege.VIEW_METADATA);
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
      fn_ = db.getFunction(searchDesc, Function.CompareMode.IS_SUPERTYPE_OF);
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
    fn_ = db.getFunction(searchDesc, Function.CompareMode.IS_SUPERTYPE_OF);

    if (fn_ == null || !fn_.userVisible()) {
      throw new AnalysisException(getFunctionNotFoundError(argTypes));
    }

    if (isAggregateFunction()) {
      // subexprs must not contain aggregates
      if (TreeNode.contains(children_, Expr.isAggregatePredicate())) {
        throw new AnalysisException(
            "aggregate function cannot contain aggregate parameters: " + this.toSql());
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
        if (fnName_.getFunction().equalsIgnoreCase("group_concat")) {
          throw new AnalysisException("GROUP_CONCAT() does not support DISTINCT.");
        }
        if (fn_.getBinaryType() != TFunctionBinaryType.BUILTIN) {
          throw new AnalysisException("User defined aggregates do not support DISTINCT.");
        }
      }

      AggregateFunction aggFn = (AggregateFunction)fn_;
      if (aggFn.ignoresDistinct()) params_.setIsDistinct(false);
    } else {
      if (params_.isStar()) {
        throw new AnalysisException("Cannot pass '*' to scalar function.");
      }
      if (params_.isDistinct()) {
        throw new AnalysisException("Cannot pass 'DISTINCT' to scalar function.");
      }
    }

    castForFunctionCall(false);
    type_ = fn_.getReturnType();
    if (type_.isDecimal() && type_.isWildcardDecimal()) {
      type_ = resolveDecimalReturnType(analyzer);
    }
  }

  @Override
  public Expr clone() { return new FunctionCallExpr(this); }
}
