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

import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.HdfsUri;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.hive.executor.UdfExecutor.JavaUdfDataType;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TScalarFunction;
import org.apache.impala.thrift.TSymbolLookupParams;
import org.apache.impala.thrift.TSymbolType;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * Internal representation of a scalar function.
 */
public class ScalarFunction extends Function {
  // The name inside the binary at location_ that contains this particular
  // function. e.g. org.example.MyUdf.class.
  protected String symbolName_;
  protected String prepareFnSymbol_;
  protected String closeFnSymbol_;

  public ScalarFunction(FunctionName fnName, List<Type> argTypes, Type retType,
      boolean hasVarArgs) {
    super(fnName, argTypes, retType, hasVarArgs);
  }

  public ScalarFunction(FunctionName fnName, Type argTypes[], Type retType,
      boolean hasVarArgs) {
    super(fnName, argTypes, retType, hasVarArgs);
  }

  public ScalarFunction(FunctionName fnName, List<Type> argTypes,
      Type retType, HdfsUri location, String symbolName, String initFnSymbol,
      String closeFnSymbol) {
    super(fnName, argTypes, retType, false);
    setLocation(location);
    setSymbolName(symbolName);
    setPrepareFnSymbol(initFnSymbol);
    setCloseFnSymbol(closeFnSymbol);
  }

  /**
   * Creates a builtin scalar function. This is a helper that wraps a few steps
   * into one call.
   */
  public static ScalarFunction createBuiltin(String name, List<Type> argTypes,
      boolean hasVarArgs, Type retType, String symbol,
      String prepareFnSymbol, String closeFnSymbol, boolean userVisible) {
    Preconditions.checkNotNull(symbol);
    ScalarFunction fn = new ScalarFunction(
        new FunctionName(BuiltinsDb.NAME, name), argTypes, retType, hasVarArgs);
    fn.setBinaryType(TFunctionBinaryType.BUILTIN);
    fn.setUserVisible(userVisible);
    fn.setIsPersistent(true);
    try {
      fn.symbolName_ = fn.lookupSymbol(symbol, TSymbolType.UDF_EVALUATE, null,
          fn.hasVarArgs(), fn.getArgs());
    } catch (AnalysisException e) {
      // This should never happen
      throw new RuntimeException("Builtin symbol '" + symbol + "'" + argTypes
          + " not found!", e);
    }
    if (prepareFnSymbol != null) {
      try {
        fn.prepareFnSymbol_ = fn.lookupSymbol(prepareFnSymbol, TSymbolType.UDF_PREPARE);
      } catch (AnalysisException e) {
        // This should never happen
        throw new RuntimeException(
            "Builtin symbol '" + prepareFnSymbol + "' not found!", e);
      }
    }
    if (closeFnSymbol != null) {
      try {
        fn.closeFnSymbol_ = fn.lookupSymbol(closeFnSymbol, TSymbolType.UDF_CLOSE);
      } catch (AnalysisException e) {
        // This should never happen
        throw new RuntimeException(
            "Builtin symbol '" + closeFnSymbol + "' not found!", e);
      }
    }
    return fn;
  }

  /**
   * Creates a Function object based on following inputs.
   * @param dbName Name of fn's database
   * @param fnName Name of the function
   * @param fnClass Function symbol name
   * @param fnArgs List of Class objects corresponding to the args of evaluate method
   * @param fnRetType Class corresponding to the return type of the evaluate method
   * @param hdfsUri URI of the jar holding the udf class.
   * @return Function object corresponding to the hive udf if the parameters are
   *         compatible, null otherwise.
   */
  public static Function fromHiveFunction(String dbName, String fnName, String fnClass,
      Class<?>[] fnArgs, Class<?> fnRetType, String hdfsUri) {
    // Check if the return type and the method arguments are supported.
    // Currently we only support certain primitive types.
    JavaUdfDataType javaRetType = JavaUdfDataType.getType(fnRetType);
    if (javaRetType == JavaUdfDataType.INVALID_TYPE) return null;
    List<Type> fnArgsList = new ArrayList<>();
    for (Class<?> argClass: fnArgs) {
      JavaUdfDataType javaUdfType = JavaUdfDataType.getType(argClass);
      if (javaUdfType == JavaUdfDataType.INVALID_TYPE) return null;
      fnArgsList.add(new ScalarType(
          PrimitiveType.fromThrift(javaUdfType.getPrimitiveType())));
    }
    ScalarType retType = new ScalarType(
        PrimitiveType.fromThrift(javaRetType.getPrimitiveType()));
    ScalarFunction fn = new ScalarFunction(new FunctionName(dbName, fnName), fnArgsList,
        retType, new HdfsUri(hdfsUri), fnClass, null, null);
    // We do not support varargs for Java UDFs, and neither does Hive.
    fn.setHasVarArgs(false);
    fn.setBinaryType(TFunctionBinaryType.JAVA);
    fn.setIsPersistent(true);
    return fn;
  }

  /**
   * Creates a Hive function object from 'this'. Returns null if 'this' is not
   * a Java UDF.
   */
  public org.apache.hadoop.hive.metastore.api.Function toHiveFunction() {
    if (getBinaryType() != TFunctionBinaryType.JAVA) return null;
    List<ResourceUri> resources = Lists.newArrayList(new ResourceUri(ResourceType.JAR,
        getLocation().toString()));
    return new org.apache.hadoop.hive.metastore.api.Function(functionName(), dbName(),
        symbolName_, "", PrincipalType.USER, (int) (System.currentTimeMillis() / 1000),
        FunctionType.JAVA, resources);
  }

  /**
   * Creates a builtin scalar operator function. This is a helper that wraps a few steps
   * into one call.
   * TODO: this needs to be kept in sync with what generates the be operator
   * implementations. (gen_functions.py). Is there a better way to coordinate this.
   */
  public static ScalarFunction createBuiltinOperator(String name,
      List<Type> argTypes, Type retType) {
    // Operators have a well defined symbol based on the function name and type.
    // Convert Add(TINYINT, TINYINT) --> Add_TinyIntVal_TinyIntVal
    String beFn = Character.toUpperCase(name.charAt(0)) + name.substring(1);
    boolean usesDecimal = false;
    for (int i = 0; i < argTypes.size(); ++i) {
      switch (argTypes.get(i).getPrimitiveType()) {
        case BOOLEAN:
          beFn += "_BooleanVal";
          break;
        case TINYINT:
          beFn += "_TinyIntVal";
          break;
        case SMALLINT:
          beFn += "_SmallIntVal";
          break;
        case INT:
          beFn += "_IntVal";
          break;
        case BIGINT:
          beFn += "_BigIntVal";
          break;
        case FLOAT:
          beFn += "_FloatVal";
          break;
        case DOUBLE:
          beFn += "_DoubleVal";
          break;
        case STRING:
        case VARCHAR:
          beFn += "_StringVal";
          break;
        case CHAR:
          beFn += "_Char";
          break;
        case TIMESTAMP:
          beFn += "_TimestampVal";
          break;
        case DECIMAL:
          beFn += "_DecimalVal";
          usesDecimal = true;
          break;
        case DATE:
          beFn += "_DateVal";
          break;
        default:
          Preconditions.checkState(false,
              "Argument type not supported: " + argTypes.get(i).toSql());
      }
    }
    String beClass = usesDecimal ? "DecimalOperators" : "Operators";
    String symbol = "impala::" + beClass + "::" + beFn;
    return createBuiltinOperator(name, symbol, argTypes, retType);
  }

  public static ScalarFunction createBuiltinOperator(String name, String symbol,
      List<Type> argTypes, Type retType) {
    return createBuiltin(name, symbol, argTypes, false, retType, false);
  }

  public static ScalarFunction createBuiltin(String name, String symbol,
      List<Type> argTypes, boolean hasVarArgs, Type retType,
      boolean userVisible) {
    ScalarFunction fn = new ScalarFunction(
        new FunctionName(BuiltinsDb.NAME, name), argTypes, retType, hasVarArgs);
    fn.setBinaryType(TFunctionBinaryType.BUILTIN);
    fn.setUserVisible(userVisible);
    fn.setIsPersistent(true);
    try {
      fn.symbolName_ = fn.lookupSymbol(symbol, TSymbolType.UDF_EVALUATE, null,
          fn.hasVarArgs(), fn.getArgs());
    } catch (AnalysisException e) {
      // This should never happen
      Preconditions.checkState(false, "Builtin symbol '" + symbol + "'" +
          argTypes + " not found: " +
          Throwables.getStackTraceAsString(e));
      throw new RuntimeException("Builtin symbol not found!", e);
    }
    return fn;
  }

  /**
   * Static helper method to create a scalar function of given
   * TFunctionBinaryType.
   */
  public static ScalarFunction createForTesting(String db,
      String fnName, List<Type> args, Type retType, String uriPath,
      String symbolName, String initFnSymbol, String closeFnSymbol,
      TFunctionBinaryType type) {
    ScalarFunction fn = new ScalarFunction(new FunctionName(db, fnName), args,
        retType, new HdfsUri(uriPath), symbolName, initFnSymbol, closeFnSymbol);
    fn.setBinaryType(type);
    fn.setIsPersistent(true);
    return fn;
  }

  public void setSymbolName(String s) { symbolName_ = s; }
  public void setPrepareFnSymbol(String s) { prepareFnSymbol_ = s; }
  public void setCloseFnSymbol(String s) { closeFnSymbol_ = s; }

  public String getSymbolName() { return symbolName_; }

  @Override
  protected TSymbolLookupParams getLookupParams() {
    return buildLookupParams(
        getSymbolName(), TSymbolType.UDF_EVALUATE, null, hasVarArgs(), false, getArgs());
  }

  @Override
  public String toSql(boolean ifNotExists) {
    StringBuilder sb = new StringBuilder("CREATE FUNCTION ");
    if (ifNotExists) sb.append("IF NOT EXISTS ");
    sb.append(dbName()).append(".");
    if (binaryType_ == TFunctionBinaryType.JAVA) {
      sb.append(name_.getFunction());
    } else {
      sb.append(signatureString()).append("\n");
      sb.append(" RETURNS " + getReturnType());
    }
    if (getLocation() != null)
      sb.append("\n").append(" LOCATION '" + getLocation()).append("'");
    if (getSymbolName() != null)
      sb.append("\n").append(" SYMBOL='" + getSymbolName()).append("'");
    return sb.toString();
  }

  @Override
  public String toString() { return toSql(false); }

  @Override
  public TFunction toThrift() {
    TFunction fn = super.toThrift();
    fn.setScalar_fn(new TScalarFunction());
    fn.getScalar_fn().setSymbol(symbolName_);
    if (prepareFnSymbol_ != null) fn.getScalar_fn().setPrepare_fn_symbol(prepareFnSymbol_);
    if (closeFnSymbol_ != null) fn.getScalar_fn().setClose_fn_symbol(closeFnSymbol_);
    return fn;
  }
}
