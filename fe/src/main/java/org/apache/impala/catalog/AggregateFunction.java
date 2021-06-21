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

package org.apache.impala.catalog;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.HdfsUri;
import org.apache.impala.thrift.TAggregateFunction;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TSymbolLookupParams;
import org.apache.impala.thrift.TSymbolType;

import com.google.common.base.Preconditions;

/**
 * Internal representation of an aggregate function.
 * TODO: Create separate AnalyticFunction class
 */
public class AggregateFunction extends Function {
  // Set if different from retType_, null otherwise.
  private Type intermediateType_;

  // The symbol inside the binary at location_ that contains this particular.
  // They can be null if it is not required.
  private String updateFnSymbol_;
  private String initFnSymbol_;
  private String serializeFnSymbol_;
  private String mergeFnSymbol_;
  private String getValueFnSymbol_;
  private String removeFnSymbol_;
  private String finalizeFnSymbol_;

  // If true, this aggregate function should ignore distinct.
  // e.g. min(distinct col) == min(col).
  // TODO: currently it is not possible for user functions to specify this. We should
  // extend the create aggregate function stmt to allow additional metadata like this.
  private boolean ignoresDistinct_;

  // True if this function can appear within an analytic expr (fn() OVER(...)).
  // TODO: Instead of manually setting this flag for all builtin aggregate functions
  // we should identify this property from the function itself (e.g., based on which
  // functions of the UDA API are implemented).
  // Currently, there is no reliable way of doing that.
  private boolean isAnalyticFn_;

  // True if this function can be used for aggregation (without an OVER() clause).
  private boolean isAggregateFn_;

  // True if this function returns a non-null value on an empty input. It is used
  // primarily during the rewrite of scalar subqueries.
  // TODO: Instead of manually setting this flag, we should identify this
  // property from the function itself (e.g. evaluating the function on an
  // empty input in BE).
  private boolean returnsNonNullOnEmpty_;

  public AggregateFunction(FunctionName fnName, List<Type> argTypes, Type retType,
      boolean hasVarArgs) {
    super(fnName, argTypes, retType, hasVarArgs);
  }

  public AggregateFunction(FunctionName fnName, List<Type> argTypes,
      Type retType, Type intermediateType,
      HdfsUri location, String updateFnSymbol, String initFnSymbol,
      String serializeFnSymbol, String mergeFnSymbol, String getValueFnSymbol,
      String removeFnSymbol, String finalizeFnSymbol) {
    super(fnName, argTypes, retType, false);
    setLocation(location);
    intermediateType_ = (intermediateType.equals(retType)) ? null : intermediateType;
    updateFnSymbol_ = updateFnSymbol;
    initFnSymbol_ = initFnSymbol;
    serializeFnSymbol_ = serializeFnSymbol;
    mergeFnSymbol_ = mergeFnSymbol;
    getValueFnSymbol_ = getValueFnSymbol;
    removeFnSymbol_ = removeFnSymbol;
    finalizeFnSymbol_ = finalizeFnSymbol;
    ignoresDistinct_ = false;
    isAnalyticFn_ = false;
    isAggregateFn_ = true;
    returnsNonNullOnEmpty_ = false;
  }

  public static AggregateFunction createForTesting(FunctionName fnName,
      List<Type> argTypes, Type retType, Type intermediateType,
      HdfsUri location, String updateFnSymbol, String initFnSymbol,
      String serializeFnSymbol, String mergeFnSymbol, String getValueFnSymbol,
      String removeFnSymbol, String finalizeFnSymbol,
      TFunctionBinaryType fnType) {
    AggregateFunction fn = new AggregateFunction(fnName, argTypes, retType,
        intermediateType, location, updateFnSymbol, initFnSymbol,
        serializeFnSymbol, mergeFnSymbol, getValueFnSymbol, removeFnSymbol,
        finalizeFnSymbol);
    fn.setBinaryType(fnType);
    return fn;
  }

  public static AggregateFunction createBuiltin(Db db, String name,
      List<Type> argTypes, Type retType, Type intermediateType,
      String initFnSymbol, String updateFnSymbol, String mergeFnSymbol,
      String serializeFnSymbol, String finalizeFnSymbol, boolean ignoresDistinct,
      boolean isAnalyticFn, boolean returnsNonNullOnEmpty) {
    return createBuiltin(db, name, argTypes, retType, intermediateType, initFnSymbol,
        updateFnSymbol, mergeFnSymbol, serializeFnSymbol, null, null, finalizeFnSymbol,
        ignoresDistinct, isAnalyticFn, returnsNonNullOnEmpty);
  }

  public static AggregateFunction createBuiltin(Db db, String name,
      List<Type> argTypes, Type retType, Type intermediateType,
      String initFnSymbol, String updateFnSymbol, String mergeFnSymbol,
      String serializeFnSymbol, String getValueFnSymbol, String removeFnSymbol,
      String finalizeFnSymbol, boolean ignoresDistinct, boolean isAnalyticFn,
      boolean returnsNonNullOnEmpty) {
    Preconditions.checkState(initFnSymbol != null);
    Preconditions.checkState(updateFnSymbol != null);
    Preconditions.checkState(mergeFnSymbol != null);
    AggregateFunction fn = new AggregateFunction(new FunctionName(db.getName(), name),
        argTypes, retType, intermediateType, null, updateFnSymbol, initFnSymbol,
        serializeFnSymbol, mergeFnSymbol, getValueFnSymbol, removeFnSymbol,
        finalizeFnSymbol);
    fn.setBinaryType(TFunctionBinaryType.BUILTIN);
    fn.ignoresDistinct_ = ignoresDistinct;
    fn.isAnalyticFn_ = isAnalyticFn;
    fn.isAggregateFn_ = true;
    fn.returnsNonNullOnEmpty_ = returnsNonNullOnEmpty;
    fn.setIsPersistent(true);
    return fn;
  }

  public static AggregateFunction createUnsupportedBuiltin(Db db, String name,
      List<Type> argTypes, Type retType, Type intermediateType) {
    AggregateFunction fn = new AggregateFunction(new FunctionName(db.getName(), name),
        argTypes, retType, intermediateType, null, null, null, null, null, null, null,
        null);
    fn.setBinaryType(TFunctionBinaryType.BUILTIN);
    fn.isAggregateFn_ = true;
    fn.setUnsupported();
    return fn;
  }

  /**
   * Create a builtin with no concrete implementation that will be rewritten during
   * analysis, e.g. grouping() and grouping_id().
   */
  public static AggregateFunction createRewrittenBuiltin(Db db, String name,
      List<Type> argTypes, Type retType, boolean ignoresDistinct,
      boolean isAnalyticFn, boolean returnsNonNullOnEmpty) {
    AggregateFunction fn = new AggregateFunction(new FunctionName(db.getName(), name),
        argTypes, retType, false);
    fn.setBinaryType(TFunctionBinaryType.BUILTIN);
    fn.ignoresDistinct_ = ignoresDistinct;
    fn.isAnalyticFn_ = isAnalyticFn;
    fn.isAggregateFn_ = true;
    fn.returnsNonNullOnEmpty_ = returnsNonNullOnEmpty;
    fn.setIsPersistent(true);
    return fn;
  }

  public static AggregateFunction createAnalyticBuiltin(Db db, String name,
      List<Type> argTypes, Type retType, Type intermediateType) {
    return createAnalyticBuiltin(db, name, argTypes, retType, intermediateType, null,
        null, null, null, null, true);
  }

  public static AggregateFunction createAnalyticBuiltin(Db db, String name,
      List<Type> argTypes, Type retType, Type intermediateType,
      String initFnSymbol, String updateFnSymbol, String removeFnSymbol,
      String getValueFnSymbol, String finalizeFnSymbol) {
    return createAnalyticBuiltin(db, name, argTypes, retType, intermediateType,
        initFnSymbol, updateFnSymbol, removeFnSymbol, getValueFnSymbol, finalizeFnSymbol,
        true);
  }

  public static AggregateFunction createAnalyticBuiltin(Db db, String name,
      List<Type> argTypes, Type retType, Type intermediateType,
      String initFnSymbol, String updateFnSymbol, String removeFnSymbol,
      String getValueFnSymbol, String finalizeFnSymbol, boolean isUserVisible) {
    AggregateFunction fn = new AggregateFunction(new FunctionName(db.getName(), name),
        argTypes, retType, intermediateType, null, updateFnSymbol, initFnSymbol,
        null, null, getValueFnSymbol, removeFnSymbol, finalizeFnSymbol);
    fn.setBinaryType(TFunctionBinaryType.BUILTIN);
    fn.ignoresDistinct_ = false;
    fn.isAnalyticFn_ = true;
    fn.isAggregateFn_ = false;
    fn.returnsNonNullOnEmpty_ = false;
    fn.setUserVisible(isUserVisible);
    fn.setIsPersistent(true);
    return fn;
  }

  public String getUpdateFnSymbol() { return updateFnSymbol_; }
  public String getInitFnSymbol() { return initFnSymbol_; }
  public String getSerializeFnSymbol() { return serializeFnSymbol_; }
  public String getMergeFnSymbol() { return mergeFnSymbol_; }
  public String getFinalizeFnSymbol() { return finalizeFnSymbol_; }
  public String getRemoveFnSymbol() { return removeFnSymbol_; }
  public String getValueFnSymbol() { return getValueFnSymbol_; }
  public boolean ignoresDistinct() { return ignoresDistinct_; }
  public boolean isAnalyticFn() { return isAnalyticFn_; }
  public boolean isAggregateFn() { return isAggregateFn_; }
  public boolean returnsNonNullOnEmpty() { return returnsNonNullOnEmpty_; }

  /**
   * Returns the intermediate type of this aggregate function or null
   * if it is identical to the return type.
   */
  public Type getIntermediateType() { return intermediateType_; }
  public void setUpdateFnSymbol(String fn) { updateFnSymbol_ = fn; }
  public void setInitFnSymbol(String fn) { initFnSymbol_ = fn; }
  public void setSerializeFnSymbol(String fn) { serializeFnSymbol_ = fn; }
  public void setMergeFnSymbol(String fn) { mergeFnSymbol_ = fn; }
  public void setFinalizeFnSymbol(String fn) { finalizeFnSymbol_ = fn; }
  public void setIntermediateType(Type t) { intermediateType_ = t; }

  /**
   * Simplifies and returns the children of the given distinct aggregate expression such
   * that they are suitable for comparing/grouping using equals():
   * - removes top-level implicit casts
   * - removes the trailing constant separator for group_concat
   * TODO: Deal with constant exprs more generally, instead of special-casing
   * group_concat().
   */
  public static List<Expr> getCanonicalDistinctAggChildren(FunctionCallExpr aggFn) {
    Preconditions.checkState(aggFn.isDistinct());
    List<Expr> result = new ArrayList<>();
    if (aggFn.getFnName().getFunction().equalsIgnoreCase("group_concat")) {
      result.add(aggFn.getChild(0).ignoreImplicitCast());
    } else {
      for (Expr c : aggFn.getChildren()) result.add(c.ignoreImplicitCast());
    }
    return result;
  }

  @Override
  protected TSymbolLookupParams getLookupParams() {
    return buildLookupParams(getUpdateFnSymbol(), TSymbolType.UDF_EVALUATE,
        intermediateType_, hasVarArgs(), false, getArgs());
  }

  @Override
  public String toSql(boolean ifNotExists) {
    StringBuilder sb = new StringBuilder("CREATE AGGREGATE FUNCTION ");
    if (ifNotExists) sb.append("IF NOT EXISTS ");
    sb.append(dbName()).append(".").append(signatureString())
      .append("\n RETURNS ").append(getReturnType());
    if (intermediateType_ != null) {
      sb.append("\n INTERMEDIATE ").append(intermediateType_);
    }
    sb.append("\n LOCATION '").append(location_)
      .append("'\n UPDATE_FN='").append(updateFnSymbol_)
      .append("'\n INIT_FN='").append(initFnSymbol_)
      .append("'\n MERGE_FN='").append(mergeFnSymbol_).append("'");
    if (serializeFnSymbol_ != null) {
      sb.append("\n SERIALIZE_FN='").append(serializeFnSymbol_).append("'");
    }
    if (finalizeFnSymbol_ != null) {
      sb.append("\n FINALIZE_FN='").append(finalizeFnSymbol_).append("'");
    }
    return sb.toString();
  }

  @Override
  public TFunction toThrift() {
    TFunction fn = super.toThrift();
    TAggregateFunction agg_fn = new TAggregateFunction();
    agg_fn.setIs_analytic_only_fn(isAnalyticFn_ && !isAggregateFn_);
    agg_fn.setUpdate_fn_symbol(updateFnSymbol_);
    agg_fn.setInit_fn_symbol(initFnSymbol_);
    if (serializeFnSymbol_ != null) agg_fn.setSerialize_fn_symbol(serializeFnSymbol_);
    agg_fn.setMerge_fn_symbol(mergeFnSymbol_);
    if (getValueFnSymbol_  != null) agg_fn.setGet_value_fn_symbol(getValueFnSymbol_);
    if (removeFnSymbol_  != null) agg_fn.setRemove_fn_symbol(removeFnSymbol_);
    if (finalizeFnSymbol_  != null) agg_fn.setFinalize_fn_symbol(finalizeFnSymbol_);
    if (intermediateType_ != null) {
      agg_fn.setIntermediate_type(intermediateType_.toThrift());
    } else {
      agg_fn.setIntermediate_type(getReturnType().toThrift());
    }
    agg_fn.setIgnores_distinct(ignoresDistinct_);
    fn.setAggregate_fn(agg_fn);
    return fn;
  }
}
