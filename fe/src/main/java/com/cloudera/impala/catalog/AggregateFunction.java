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

package com.cloudera.impala.catalog;

import java.util.List;

import com.cloudera.impala.analysis.FunctionArgs;
import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.analysis.HdfsUri;
import com.cloudera.impala.thrift.TAggregateFunction;
import com.cloudera.impala.thrift.TFunction;
import com.cloudera.impala.thrift.TFunctionBinaryType;

/**
 * Internal representation of an aggregate function.
 */
public class AggregateFunction extends Function {
  private Type intermediateType_;

  // The symbol inside the binary at location_ that contains this particular.
  // They can be null if it is not required.
  private String updateFnSymbol_;
  private String initFnSymbol_;
  private String serializeFnSymbol_;
  private String mergeFnSymbol_;
  private String finalizeFnSymbol_;

  private static String BE_BUILTINS_CLASS = "AggregateFunctions";

  // If true, this aggregate function should ignore distinct.
  // e.g. min(distinct col) == min(col).
  // TODO: currently it is not possible for user functions to specify this. We should
  // extend the create aggregate function stmt to allow additional metadata like this.
  private boolean ignoresDistinct_;

  // true if this function can only appear within an analytic expr (fn() OVER(...))
  private boolean needsAnalyticExpr_;

  public AggregateFunction(FunctionName fnName, FunctionArgs args, Type retType) {
    super(fnName, args.argTypes, retType, args.hasVarArgs);
  }

  public AggregateFunction(FunctionName fnName, List<Type> argTypes,
      Type retType, Type intermediateType,
      HdfsUri location, String updateFnSymbol, String initFnSymbol,
      String serializeFnSymbol, String mergeFnSymbol, String finalizeFnSymbol) {
    super(fnName, argTypes, retType, false);
    setLocation(location);
    intermediateType_ = intermediateType;
    updateFnSymbol_ = updateFnSymbol;
    initFnSymbol_ = initFnSymbol;
    serializeFnSymbol_ = serializeFnSymbol;
    mergeFnSymbol_ = mergeFnSymbol;
    finalizeFnSymbol_ = finalizeFnSymbol;
    ignoresDistinct_ = false;
    needsAnalyticExpr_ = false;
  }

  public static AggregateFunction createBuiltin(Db db, String name,
      List<Type> argTypes, Type retType, Type intermediateType,
      String initFnSymbol, String updateFnSymbol, String mergeFnSymbol,
      String serializeFnSymbol, String finalizeFnSymbol,
      boolean ignoresDistinct) {
    AggregateFunction fn = new AggregateFunction(new FunctionName(db.getName(), name),
        argTypes, retType, intermediateType, null, updateFnSymbol, initFnSymbol,
        serializeFnSymbol, mergeFnSymbol, finalizeFnSymbol);
    fn.setBinaryType(TFunctionBinaryType.BUILTIN);
    fn.ignoresDistinct_ = ignoresDistinct;
    fn.needsAnalyticExpr_ = false;
    return fn;
  }

  public static AggregateFunction createAnalyticBuiltin(Db db, String name,
      List<Type> argTypes, Type retType, Type intermediateType) {
    AggregateFunction fn = new AggregateFunction(new FunctionName(db.getName(), name),
        argTypes, retType, intermediateType, null, null, null, null, null, null);
    fn.setBinaryType(TFunctionBinaryType.BUILTIN);
    fn.ignoresDistinct_ = false;
    fn.needsAnalyticExpr_ = true;
    return fn;
  }

  public String getUpdateFnSymbol() { return updateFnSymbol_; }
  public String getInitFnSymbol() { return initFnSymbol_; }
  public String getSerializeFnSymbol() { return serializeFnSymbol_; }
  public String getMergeFnSymbol() { return mergeFnSymbol_; }
  public String getFinalizeFnSymbol() { return finalizeFnSymbol_; }
  public Type getIntermediateType() { return intermediateType_; }
  public boolean ignoresDistinct() { return ignoresDistinct_; }
  public boolean needsAnalyticExpr() { return needsAnalyticExpr_; }

  public void setUpdateFnSymbol(String fn) { updateFnSymbol_ = fn; }
  public void setInitFnSymbol(String fn) { initFnSymbol_ = fn; }
  public void setSerializeFnSymbol(String fn) { serializeFnSymbol_ = fn; }
  public void setMergeFnSymbol(String fn) { mergeFnSymbol_ = fn; }
  public void setFinalizeFnSymbol(String fn) { finalizeFnSymbol_ = fn; }
  public void setIntermediateType(Type t) { intermediateType_ = t; }

  @Override
  public TFunction toThrift() {
    TFunction fn = super.toThrift();
    TAggregateFunction agg_fn = new TAggregateFunction();
    agg_fn.setUpdate_fn_symbol(updateFnSymbol_);
    agg_fn.setInit_fn_symbol(initFnSymbol_);
    if (serializeFnSymbol_ != null) agg_fn.setSerialize_fn_symbol(serializeFnSymbol_);
    agg_fn.setMerge_fn_symbol(mergeFnSymbol_);
    if (finalizeFnSymbol_  != null) agg_fn.setFinalize_fn_symbol(finalizeFnSymbol_);
    agg_fn.setIntermediate_type(intermediateType_.toThrift());
    agg_fn.setIgnores_distinct(ignoresDistinct_);
    fn.setAggregate_fn(agg_fn);
    return fn;
  }
}
