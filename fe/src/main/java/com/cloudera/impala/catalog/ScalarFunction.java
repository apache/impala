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

import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.analysis.FunctionArgs;
import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.analysis.HdfsUri;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TFunction;
import com.cloudera.impala.thrift.TFunctionBinaryType;
import com.cloudera.impala.thrift.TScalarFunction;
import com.cloudera.impala.thrift.TSymbolType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Internal representation of a scalar function.
 */
public class ScalarFunction extends Function {
  // The name inside the binary at location_ that contains this particular
  // function. e.g. org.example.MyUdf.class.
  private String symbolName_;
  private String prepareFnSymbol_;
  private String closeFnSymbol_;

  // If true, this function is implemented with the Udf interface. Otherwise,
  // it is implemented using the old ComputeFn interface.
  // TODO: remove when ComputeFn interface is removed.
  private boolean udfInterface_;

  public ScalarFunction(FunctionName fnName, FunctionArgs args, Type retType) {
    super(fnName, args.argTypes, retType, args.hasVarArgs);
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
   * into one call. This defaults to not using a Prepare/Close function.
   */
  public static ScalarFunction createBuiltin(String name, ArrayList<Type> argTypes,
      boolean hasVarArgs, Type retType, String symbol, boolean udfInterface) {
    return createBuiltin(name, argTypes, hasVarArgs, retType,
                         symbol, null, null, udfInterface, false);
  }

  /**
   * Creates a builtin scalar function. This is a helper that wraps a few steps
   * into one call.
   */
  public static ScalarFunction createBuiltin(String name, ArrayList<Type> argTypes,
      boolean hasVarArgs, Type retType, String symbol,
      String prepareFnSymbol, String closeFnSymbol, boolean udfInterface,
      boolean isOperator) {
    Preconditions.checkNotNull(symbol);
    FunctionArgs fnArgs = new FunctionArgs(argTypes, hasVarArgs);
    ScalarFunction fn =
        new ScalarFunction(new FunctionName(Catalog.BUILTINS_DB, name), fnArgs, retType);
    fn.setBinaryType(TFunctionBinaryType.BUILTIN);
    fn.setUserVisible(!isOperator);
    fn.udfInterface_ = udfInterface;
    if (fn.udfInterface_) {
      try {
        fn.symbolName_ = fn.lookupSymbol(symbol, TSymbolType.UDF_EVALUATE, null,
            fn.hasVarArgs(), fn.getArgs());
      } catch (AnalysisException e) {
        // This should never happen
        Preconditions.checkState(false, "Builtin symbol '" + symbol + "'" + argTypes
            + " not found!" + e.getStackTrace());
        throw new RuntimeException("Builtin symbol not found!", e);
      }
    } else {
      fn.symbolName_ = symbol;
    }
    fn.prepareFnSymbol_ = prepareFnSymbol;
    fn.closeFnSymbol_ = closeFnSymbol;
    return fn;
  }

  /**
   * Creates a builtin scalar operator function. This is a helper that wraps a few steps
   * into one call.
   * TODO: this needs to be kept in sync with what generates the be operator
   * implementations. (gen_functions.py). Is there a better way to coordinate this.
   */
  public static ScalarFunction createBuiltinOperator(String name,
      ArrayList<Type> argTypes, Type retType) {
    // Operators have a well defined symbol based on the function name and type.
    // Convert Add(TINYINT, TINYINT) --> Add_char_char
    String beFn = Character.toUpperCase(name.charAt(0)) + name.substring(1);
    boolean usesDecimal = false;
    for (int i = 0; i < argTypes.size(); ++i) {
      switch (argTypes.get(i).getPrimitiveType()) {
        case BOOLEAN:
          beFn += "_bool";
          break;
        case TINYINT:
          beFn += "_char";
          break;
        case SMALLINT:
          beFn += "_short";
          break;
        case INT:
          beFn += "_int";
          break;
        case BIGINT:
          beFn += "_long";
          break;
        case FLOAT:
          beFn += "_float";
          break;
        case DOUBLE:
          beFn += "_double";
          break;
        case STRING:
          beFn += "_StringValue";
          break;
        case TIMESTAMP:
          beFn += "_TimestampValue";
          break;
        case DECIMAL:
          beFn += "_decimal";
          usesDecimal = true;
          break;
        default:
          Preconditions.checkState(false,
              "Argument type not supported: " + argTypes.get(i).toSql());
      }
    }
    String beClass = usesDecimal ? "DecimalOperators" : "ComputeFunctions";
    return createBuiltinOperator(name, beClass, beFn, argTypes, retType);
  }

  /**
   * Create a builtin function with the symbol beClass::beFn
   */
  public static ScalarFunction createBuiltinOperator(String name,
      String beClass, String beFn, ArrayList<Type> argTypes, Type retType) {
    FunctionArgs fnArgs = new FunctionArgs(argTypes, false);
    ScalarFunction fn =
        new ScalarFunction(new FunctionName(Catalog.BUILTINS_DB, name), fnArgs, retType);
    fn.setBinaryType(TFunctionBinaryType.BUILTIN);
    fn.setUserVisible(false);
    fn.udfInterface_ = false;
    fn.symbolName_ = GetComputeFnSymbol(beClass, beFn);
    return fn;
  }

  // Convert ComputeFunctions::Add_char_char
  // '_ZN6impala16ComputeFunctions13Add_char_charEPNS_4ExprEPNS_8TupleRowE'
  // TODO: this needs to be updated when we retire the ComputeFunction interface.
  private static String GetComputeFnSymbol(String beClass, String beFn) {
    String result = "_ZN6impala";
    result += beClass.length() + beClass;
    result += beFn.length() + beFn;
    result += "EPNS_4ExprEPNS_8TupleRowE";
    return result;
  }

  /**
   * Create a function that is used to search the catalog for a matching builtin. Only
   * the fields necessary for matching function prototypes are specified.
   */
  public static ScalarFunction createBuiltinSearchDesc(String name, Type[] argTypes,
      boolean hasVarArgs) {
    FunctionArgs fnArgs = new FunctionArgs(
        argTypes == null ? new ArrayList<Type>() : Lists.newArrayList(argTypes),
        hasVarArgs);
    ScalarFunction fn = new ScalarFunction(
        new FunctionName(Catalog.BUILTINS_DB, name), fnArgs, Type.INVALID);
    fn.setBinaryType(TFunctionBinaryType.BUILTIN);
    return fn;
  }

  public void setSymbolName(String s) { symbolName_ = s; }
  public void setPrepareFnSymbol(String s) { prepareFnSymbol_ = s; }
  public void setCloseFnSymbol(String s) { closeFnSymbol_ = s; }

  public String getSymbolName() { return symbolName_; }
  public String getPrepareFnSymbol() { return prepareFnSymbol_; }
  public String getCloseFnSymbol() { return closeFnSymbol_; }

  public boolean isUdfInterface() { return udfInterface_; }

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
