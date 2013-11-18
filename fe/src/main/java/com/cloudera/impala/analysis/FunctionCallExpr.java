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

import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Uda;
import com.cloudera.impala.catalog.Udf;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.opcode.FunctionOperator;
import com.cloudera.impala.thrift.TAggregateFunctionCall;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TFunctionCallExpr;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

// TODO: for aggregations, we need to unify the code paths for builtins and UDAs.
public class FunctionCallExpr extends Expr {
  private final FunctionName fnName_;
  private final BuiltinAggregateFunction.Operator agg_op_;
  private final FunctionParams params_;

  // The function to call. This can either be a builtin scalar, UDF or UDA.
  // Set in analyze() except if this expr is created from creating the
  // distributed plan for aggregations. In that case, fn_ is inherited from
  // the FunctionCallExpr expr object that was created during the non-distributed
  // planning phase.
  private Function fn_;

  public FunctionCallExpr(String functionName, List<Expr> params) {
    this(new FunctionName(functionName), new FunctionParams(false, params));
  }

  public FunctionCallExpr(FunctionName fnName, List<Expr> params) {
    this(fnName, new FunctionParams(false, params));
  }

  public FunctionCallExpr(FunctionName fnName, FunctionParams params) {
    super();
    this.fnName_ = fnName;
    params_ = params;
    agg_op_ = null;
    if (params.exprs() != null) children.addAll(params.exprs());
  }

  public FunctionCallExpr(BuiltinAggregateFunction.Operator op, FunctionParams params) {
    Preconditions.checkState(op != null);
    fnName_ = FunctionName.CreateBuiltinName(op.name());
    agg_op_ = op;
    params_ = params;
    if (params.exprs() != null) children.addAll(params.exprs());
  }

  // Constructs the same agg function with new params.
  public FunctionCallExpr(FunctionCallExpr e, FunctionParams params) {
    Preconditions.checkState(e.isAnalyzed);
    Preconditions.checkState(e.isAggregateFunction());
    fnName_ = e.fnName_;
    agg_op_ = e.agg_op_;
    params_ = params;
    // Just inherit the function object from 'e'.
    fn_ = e.fn_;
    if (params.exprs() != null) children.addAll(params.exprs());
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    FunctionCallExpr o = (FunctionCallExpr)obj;
    return opcode == o.opcode &&
           agg_op_ == o.agg_op_ &&
           fnName_.equals(o.fnName_) &&
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
        .add("op", agg_op_)
        .add("name", fnName_)
        .add("isStar", params_.isStar())
        .add("isDistinct", params_.isDistinct())
        .addValue(super.debugString())
        .toString();
  }

  public FunctionParams getParams() { return params_; }
  public boolean isScalarFunction() {
    Preconditions.checkState(fn_ != null);
    return fn_ instanceof Udf || fn_ instanceof OpcodeRegistry.BuiltinFunction;
  }

  public boolean isAggregateFunction() {
    Preconditions.checkState(fn_ != null);
    return fn_ instanceof Uda || fn_ instanceof BuiltinAggregateFunction;
  }

  public boolean isDistinct() {
    Preconditions.checkState(isAggregateFunction());
    return params_.isDistinct();
  }

  public BuiltinAggregateFunction.Operator getAggOp() { return agg_op_; }

  @Override
  protected void toThrift(TExprNode msg) {
    // TODO: we never serialize this to thrift if it's an aggregate function
    // except in test cases that do it explicitly.
    if (isAggregate()) return;

    if (fn_ instanceof Udf) {
      Udf udf = (Udf)fn_;
      msg.node_type = TExprNodeType.FUNCTION_CALL;
      TFunctionCallExpr fnCall = new TFunctionCallExpr();
      fnCall.setFn(udf.toThrift());
      if (udf.hasVarArgs()) {
        fnCall.setVararg_start_idx(udf.getNumArgs() - 1);
      }
      msg.setFn_call_expr(fnCall);
    } else {
      Preconditions.checkState(fn_ instanceof OpcodeRegistry.BuiltinFunction);
      OpcodeRegistry.BuiltinFunction builtin = (OpcodeRegistry.BuiltinFunction)fn_;
      if (builtin.udfInterface) {
        msg.node_type = TExprNodeType.FUNCTION_CALL;
        TFunctionCallExpr fnCall = new TFunctionCallExpr();
        fnCall.setFn(fn_.toThrift());
        if (fn_.hasVarArgs()) {
          fnCall.setVararg_start_idx(fn_.getNumArgs() - 1);
        }
        msg.setFn_call_expr(fnCall);
      } else {
        // TODO: remove. All builtins will go through UDF_CALL.
        msg.node_type = TExprNodeType.COMPUTE_FUNCTION_CALL;
      }
      msg.setOpcode(opcode);
    }
  }

  public TAggregateFunctionCall toTAggregateFunctionCall() {
    Preconditions.checkState(isAggregateFunction());

    List<TExpr> inputExprs = Lists.newArrayList();
    for (Expr e: children) {
      inputExprs.add(e.treeToThrift());
    }

    TAggregateFunctionCall f = new TAggregateFunctionCall();
    f.setFn(fn_.toThrift());
    f.setInput_exprs(inputExprs);
    return f;
  }

  private void analyzeBuiltinAggFunction(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    Preconditions.checkState(agg_op_ != null);

    if (params_.isStar() && agg_op_ != BuiltinAggregateFunction.Operator.COUNT) {
      throw new AnalysisException("'*' can only be used in conjunction with COUNT: "
          + this.toSql());
    }

    if (agg_op_ == BuiltinAggregateFunction.Operator.COUNT) {
      // for multiple exprs count must be qualified with distinct
      if (children.size() > 1 && !params_.isDistinct()) {
        throw new AnalysisException(
            "COUNT must have DISTINCT for multiple arguments: " + this.toSql());
      }
      ArrayList<PrimitiveType> childTypes = Lists.newArrayList();
      for (int i = 0; i < children.size(); ++i) {
        childTypes.add(getChild(i).type);
      }
      fn_ = new BuiltinAggregateFunction(agg_op_, childTypes,
          PrimitiveType.BIGINT, ColumnType.createType(PrimitiveType.BIGINT));
      return;
    }

    if (agg_op_ == BuiltinAggregateFunction.Operator.GROUP_CONCAT) {
      ArrayList<PrimitiveType> argTypes = Lists.newArrayList();
      if (children.size() > 2 || children.isEmpty()) {
        throw new AnalysisException(
            agg_op_.toString() + " requires one or two parameters: " + this.toSql());
      }

      if (params_.isDistinct()) {
        throw new AnalysisException(agg_op_.toString() + " does not support DISTINCT");
      }

      Expr arg0 = getChild(0);
      if (!arg0.type.isStringType() && !arg0.type.isNull()) {
        throw new AnalysisException(
            agg_op_.toString() + " requires first parameter to be of type STRING: "
            + this.toSql());
      }
      argTypes.add(PrimitiveType.STRING);

      if (children.size() == 2) {
        Expr arg1 = getChild(1);
        if (!arg1.type.isStringType() && !arg1.type.isNull()) {
          throw new AnalysisException(
              agg_op_.toString() + " requires second parameter to be of type STRING: "
              + this.toSql());
        }
        argTypes.add(PrimitiveType.STRING);
      } else {
        // Add the default string so the BE always see two arguments.
        Expr arg2 = new NullLiteral();
        arg2.analyze(analyzer);
        addChild(arg2);
        argTypes.add(PrimitiveType.NULL_TYPE);
      }

      // TODO: we really want the intermediate type to be CHAR(16)
      fn_ = new BuiltinAggregateFunction(agg_op_, argTypes,
          PrimitiveType.STRING, agg_op_.intermediateType());
      return;
    }

    // only COUNT and GROUP_CONCAT can contain multiple exprs
    if (children.size() != 1) {
      throw new AnalysisException(
          agg_op_.toString() + " requires exactly one parameter: " + this.toSql());
    }

    // determine type
    Expr arg = getChild(0);

    // SUM and AVG cannot be applied to non-numeric types
    if (agg_op_ == BuiltinAggregateFunction.Operator.SUM &&
        !arg.type.isNumericType() && !arg.type.isNull()) {
      throw new AnalysisException(
            "SUM requires a numeric parameter: " + this.toSql());
    }
    if (agg_op_ == BuiltinAggregateFunction.Operator.AVG && !arg.type.isNumericType()
        && arg.type != PrimitiveType.TIMESTAMP && !arg.type.isNull()) {
      throw new AnalysisException(
          "AVG requires a numeric or timestamp parameter: " + this.toSql());
    }

    ColumnType intermediateType = agg_op_.intermediateType();
    ArrayList<PrimitiveType> argTypes = Lists.newArrayList();
    if (agg_op_ == BuiltinAggregateFunction.Operator.AVG) {
      // division always results in a double value
      type = PrimitiveType.DOUBLE;
      intermediateType = ColumnType.createType(type);
    } else if (agg_op_ == BuiltinAggregateFunction.Operator.SUM) {
      // numeric types need to be accumulated at maximum precision
      type = arg.type.getMaxResolutionType();
      argTypes.add(type);
      intermediateType = ColumnType.createType(type);
    } else if (agg_op_ == BuiltinAggregateFunction.Operator.MIN ||
        agg_op_ == BuiltinAggregateFunction.Operator.MAX) {
      type = arg.type;
      params_.setIsDistinct(false);  // DISTINCT is meaningless here
      argTypes.add(type);
      intermediateType = ColumnType.createType(type);
    } else if (agg_op_ == BuiltinAggregateFunction.Operator.DISTINCT_PC ||
          agg_op_ == BuiltinAggregateFunction.Operator.DISTINCT_PCSA ||
          agg_op_ == BuiltinAggregateFunction.Operator.NDV) {
      type = PrimitiveType.STRING;
      params_.setIsDistinct(false);
      argTypes.add(arg.getType());
    }
    fn_ = new BuiltinAggregateFunction(agg_op_, argTypes, type, intermediateType);
  }

  // Sets fn_ to the proper function object.
  private void setFunction(Analyzer analyzer, PrimitiveType[] argTypes)
      throws AnalysisException, AuthorizationException {
    // First check if this is a builtin
    FunctionOperator op = OpcodeRegistry.instance().getFunctionOperator(
        fnName_.getFunction());
    if (op != FunctionOperator.INVALID_OPERATOR) {
      OpcodeRegistry.BuiltinFunction match =
          OpcodeRegistry.instance().getFunctionInfo(op, true, argTypes);
      if (match != null) {
        this.opcode = match.opcode;
        fn_ = match;
      }
    } else {
      // Next check if it is a UDF/UDA
      String dbName = analyzer.getTargetDbName(fnName_);
      fnName_.setDb(dbName);

      // User needs DB access.
      analyzer.getDb(dbName, Privilege.ANY);

      if (!analyzer.getCatalog().functionExists(fnName_)) {
        throw new AnalysisException(fnName_ + "() unknown");
      }

      Function searchDesc =
          new Function(fnName_, argTypes, PrimitiveType.INVALID_TYPE, false);

      fn_ = analyzer.getCatalog().getFunction(
          searchDesc, Function.CompareMode.IS_SUBTYPE);
    }

    if (fn_ == null) {
      throw new AnalysisException(String.format(
          "No matching function with signature: %s(%s).",
          fnName_, Joiner.on(", ").join(argTypes)));
    }
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (isAnalyzed) return;
    super.analyze(analyzer);

    if (fn_ != null && isAggregate()) {
      ColumnType intermediateType = null;
      if (fn_ instanceof Uda) {
        intermediateType = ((Uda)fn_).getIntermediateType();
      } else {
        Preconditions.checkState(fn_ instanceof BuiltinAggregateFunction);
        intermediateType = ((BuiltinAggregateFunction)fn_).getIntermediateType();
      }
      type = fn_.getReturnType();
      if (intermediateType == null) intermediateType = ColumnType.createType(type);
      // TODO: this needs to change when the intermediate type != the return type
      Preconditions.checkArgument(intermediateType.getType() == fn_.getReturnType());

      if (agg_op_ == BuiltinAggregateFunction.Operator.GROUP_CONCAT &&
          getChildren().size() == 1) {
        Expr arg2 = new NullLiteral();
        arg2.analyze(analyzer);
        addChild(arg2);
      }
      return;
    }

    PrimitiveType[] argTypes = new PrimitiveType[this.children.size()];
    for (int i = 0; i < this.children.size(); ++i) {
      this.children.get(i).analyze(analyzer);
      argTypes[i] = this.children.get(i).getType();
    }

    if (agg_op_ != null) {
      analyzeBuiltinAggFunction(analyzer);
    } else {
      setFunction(analyzer, argTypes);
    }
    Preconditions.checkState(fn_ != null);

    if (isAggregateFunction()) {
      // subexprs must not contain aggregates
      if (Expr.containsAggregate(children)) {
        throw new AnalysisException(
            "aggregate function cannot contain aggregate parameters: " + this.toSql());
      }
    } else {
      if (params_.isStar()) {
        throw new AnalysisException("Cannot pass '*' to scalar function.");
      }
      if (params_.isDistinct()) {
        throw new AnalysisException("Cannot pass 'DISTINCT' to scalar function.");
      }
    }

    PrimitiveType[] args = fn_.getArgs();
    if (args.length > 0) {
      // Implicitly cast all the children to match the function if necessary
      for (int i = 0; i < argTypes.length; ++i) {
        // For varargs, we must compare with the last type in callArgs.argTypes.
        int ix = Math.min(args.length - 1, i);
        if (argTypes[i] != args[ix]) castChild(args[ix], i);
      }
    }
    this.type = fn_.getReturnType();
  }
}
