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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.opcode.FunctionOperator;
import com.cloudera.impala.opcode.FunctionRegistry;
import com.cloudera.impala.thrift.TExprOpcode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * The OpcodeRegistry provides a mapping between function signatures and opcodes. The
 * supported functions are code-gen'ed and added to the registry with an assigned opcode.
 * The opcode is shared with the backend.  The frontend can use the registry to look up
 * a function's opcode.
 *
 * The OpcodeRegistry also contains a mapping between function names (as strings) to
 * operators.
 *
 * The OpcodeRegistry is a singleton.
 *
 * TODO: The opcode registry should be versioned in the FE/BE.
 */
public class OpcodeRegistry {

  private final static Logger LOG = LoggerFactory.getLogger(OpcodeRegistry.class);
  private static OpcodeRegistry instance = new OpcodeRegistry();

  /**
   * This is a mapping of Operator,#args to builtins with a fixed number of arguments.
   * The builtin is defined by the operator enum and the arguments
   * and is a one to one mapping to opcodes.
   * The map is structured this way to more efficiently look for builtin matches.
   * Builtins that have the same number of arguments have a potential to be matches
   * by allowing types to be implicitly cast.
   * Functions with a variable number of arguments are put into the varArgOperations map.
   */
  private final Map<Pair<FunctionOperator, Integer>, List<BuiltinFunction>> operations;

  /**
   * This is a mapping of Operator,varArgType to builtins of vararg functions only.
   * varArgType must be a maximum-resolution type.
   * We use a separate map to be able to support multiple vararg builtins for the same
   * FunctionOperator.
   * Contains a special entry mapping from Operator,NULL_TYPE to builtins for each
   * Operator to correctly match varag functions when all args are NULL.
   * Limitations: Since we do not consider the number of arguments, each FunctionOperator
   * is limited to having one vararg builtin per maximum-resolution PrimitiveType.
   * For example, one can have two builtins func(float, int ...) and func(string ...),
   * but not func(float, int ...) and func (int ...).
   */
  private final Map<Pair<FunctionOperator, PrimitiveType>, List<BuiltinFunction>>
      varArgOperations;

  /**
   * This contains a mapping of function names to a FunctionOperator enum.  This is used
   * by FunctionCallExpr to go from the parser input to function opcodes.
   * This is a many to one mapping (i.e. substr and substring both map to the same
   * operation).
   * The mappings are filled in in FunctionRegistry.java which is auto-generated.
   */
  private final HashMap<String, FunctionOperator> functionNameMap;

  // Singleton interface
  public static OpcodeRegistry instance() {
    return instance;
  }

  /**
   * Static utility functions
   */
  public static boolean isBitwiseOperation(FunctionOperator operator) {
    return operator == FunctionOperator.BITAND || operator == FunctionOperator.BITNOT ||
           operator == FunctionOperator.BITOR || operator == FunctionOperator.BITXOR;
  }

  /**
   * Contains all the information about a builtin function.
   */
  public static class BuiltinFunction {
    public TExprOpcode opcode;
    public FunctionOperator operator;
    public Function desc;

    // Constructor for searching, specifying the op and arguments
    public BuiltinFunction(FunctionOperator operator, PrimitiveType[] args) {
      this.operator = operator;
      this.desc = new Function(operator.toString(),
          args, PrimitiveType.INVALID_TYPE, false);
    }

    private BuiltinFunction(TExprOpcode opcode, FunctionOperator operator,
        boolean varArgs, PrimitiveType ret, PrimitiveType[] args) {
      this.operator = operator;
      this.opcode = opcode;
      this.desc = new Function(opcode.toString(), args, ret, varArgs);
    }

    /**
     * Returns if the 'this' function has a compatible signature with the 'other'. The op
     * and number of arguments must match and it must be allowed to implicitly cast
     * each argument of this signature to the matching argument in 'other'
     */
    public boolean isCompatible(BuiltinFunction other) {
      return desc.isCompatible(other.getDesc());
    }

    @Override
    /**
     * Functions are equal with C++/Java function signature semantics.  They are
     * equal if the operation and all the arguments are the same.
     */
    public boolean equals(Object o) {
      if (o == null || !(o instanceof BuiltinFunction)) {
        return false;
      }
      BuiltinFunction s = (BuiltinFunction) o;
      return desc.equals(s.getDesc());
    }

    public Function getDesc() {
      return desc;
    }
  }

  /**
   * Returns the set of function names.
   * @return
   */
  public Set<String> getFunctionNames() {
    return functionNameMap.keySet();
  }

  /**
   * Returns the function operator enum.  The lookup is case insensitive.
   * (i.e. "Substring" --> TExprOperator.STRING_SUBSTR).
   * Returns INVALID_OP is that function name is unknown.
   */
  public FunctionOperator getFunctionOperator(String fnName) {
    String lookup = fnName.toLowerCase();
    if (functionNameMap.containsKey(lookup)) {
      return functionNameMap.get(lookup);
    }
    return FunctionOperator.INVALID_OPERATOR;
  }

  /**
   * Query for a function in the registry, specifying the operation, 'op', the arguments.
   * If there is no matching signature, null will be returned.
   * If there is a match, the matching signature will be returned.
   * If 'allowImplicitCasts' is true the matching signature does not have to match the
   * input identically, implicit type promotion is allowed.
   */
  public BuiltinFunction getFunctionInfo(FunctionOperator op, boolean allowImplicitCasts,
      PrimitiveType ... argTypes) {
    Pair<FunctionOperator, Integer> lookup = Pair.create(op, argTypes.length);
    // Take the last argument's type as the vararg type.
    Pair<FunctionOperator, PrimitiveType> varArgsLookup = null;
    if (argTypes.length > 0) {
      PrimitiveType varArgMatchType = getRightMostNonNullTypeOrNull(argTypes);
      varArgsLookup = Pair.create(op, varArgMatchType.getMaxResolutionType());
    }
    List<BuiltinFunction> functions = null;
    if (operations.containsKey(lookup)) {
      functions = operations.get(lookup);
    } else if(varArgsLookup != null && varArgOperations.containsKey(varArgsLookup)) {
      functions = varArgOperations.get(varArgsLookup);
    }
    if (functions == null) {
      return null;
    }
    BuiltinFunction compatibleMatch = null;
    BuiltinFunction search = new BuiltinFunction(op, argTypes);
    for (BuiltinFunction function : functions) {
      if (search.equals(function)) {
        return function;
      } else if (allowImplicitCasts && compatibleMatch == null
          && function.isCompatible(search)) {
        compatibleMatch = function;
      }
    }
    return compatibleMatch;
  }

  /**
   * Returns right-most argType that is not NULL_TYPE, otherwise NULL_TYPE.
   */
  private PrimitiveType getRightMostNonNullTypeOrNull(PrimitiveType[] argTypes) {
    for (int i = argTypes.length - 1; i >= 0; --i) {
      if (!argTypes[i].isNull()) {
        return argTypes[i];
      }
    }
    return PrimitiveType.NULL_TYPE;
  }

  /**
   * Add a function with the specified opcode/signature to the registry.
   */
  public boolean add(FunctionOperator op, TExprOpcode opcode, boolean varArgs,
      PrimitiveType retType, PrimitiveType ... args) {
    List<BuiltinFunction> functions;
    Pair<FunctionOperator, Integer> lookup = Pair.create(op, args.length);
    // Take the last argument's type as the vararg type.
    Pair<FunctionOperator, PrimitiveType> varArgsLookup = null;
    // Special signature for vararg functions to handle matching when all args are NULL.
    Pair<FunctionOperator, PrimitiveType> varArgsNullLookup = null;
    Preconditions.checkArgument((varArgs) ? args.length > 0 : true);
    if (varArgs && args.length > 0) {
      varArgsLookup = Pair.create(op, args[args.length - 1].getMaxResolutionType());
      varArgsNullLookup = Pair.create(op, PrimitiveType.NULL_TYPE);
    }
    if (operations.containsKey(lookup)) {
      functions = operations.get(lookup);
    } else if (varArgsLookup != null && varArgOperations.containsKey(varArgsLookup)) {
      functions = varArgOperations.get(varArgsLookup);
    } else {
      functions = new ArrayList<BuiltinFunction>();
      if (varArgs) {
        varArgOperations.put(varArgsLookup, functions);
        varArgOperations.put(varArgsNullLookup, functions);
      } else {
        operations.put(lookup, functions);
      }
    }

    BuiltinFunction function = new BuiltinFunction(opcode, op, varArgs, retType, args);
    if (functions.contains(function)) {
      LOG.error("OpcodeRegistry: Function already exists: " + opcode);
      return false;
    }
    functions.add(function);

    return true;
  }

  public boolean addFunctionMapping(String functionName, FunctionOperator op) {
    if (functionNameMap.containsKey(functionName)) {
      LOG.error("OpcodeRegistry: Function mapping already exists: " + functionName);
      return false;
    }
    functionNameMap.put(functionName, op);
    return true;
  }

  // Singleton interface, don't call the constructor
  private OpcodeRegistry() {
    operations = Maps.newHashMap();
    varArgOperations = Maps.newHashMap();
    functionNameMap = Maps.newHashMap();

    // Add all the function signatures to the registry and the function name(string)
    // to FunctionOperator mapping
    FunctionRegistry.InitFunctions(this);
  }
}
