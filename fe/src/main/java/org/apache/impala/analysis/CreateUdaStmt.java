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
import java.util.Map;

import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TSymbolType;
import com.google.common.base.Preconditions;

/**
 * Represents a CREATE AGGREGATE FUNCTION statement.
 */
public class CreateUdaStmt extends CreateFunctionStmtBase {
  private final TypeDef intermediateTypeDef_;

  /**
   * Builds a CREATE AGGREGATE FUNCTION statement
   * @param fnName - Name of the function
   * @param fnArgs - List of types for the arguments to this function
   * @param retType - The type this function returns.
   * @param intermediateType_- The type used for the intermediate data.
   * @param location - Path in HDFS containing the UDA.
   * @param ifNotExists - If true, no errors are thrown if the function already exists
   * @param additionalArgs - Key/Value pairs for additional arguments. The keys are
   *        validated in analyze()
   */
  public CreateUdaStmt(FunctionName fnSymbol, FunctionArgs args,
      TypeDef retTypeDef, TypeDef intermediateTypeDef,
      HdfsUri location, boolean ifNotExists,
      Map<CreateFunctionStmtBase.OptArg, String> optArgs) {
    super(fnSymbol, args, retTypeDef, location, ifNotExists, optArgs);
    intermediateTypeDef_ = intermediateTypeDef;
  }

  private void reportCouldNotInferSymbol(String function) throws AnalysisException {
    throw new AnalysisException("Could not infer symbol for "
        + function + "() function.");
  }

  // Gets the symbol for 'arg'. If the user set it from the dll, return that. Otherwise
  // try to infer the Symbol from the Update function. To infer the Symbol, the update
  // function must contain "update" or "Update" and we switch that out with 'defaultSymbol'.
  // Returns null if no symbol was found.
  private String getSymbolSymbol(OptArg arg, String defaultSymbol) {
    // First lookup if the user explicitly set it.
    if (optArgs_.get(arg) != null) return optArgs_.get(arg);
    // Try to match it from Update
    String updateFn = optArgs_.get(OptArg.UPDATE_FN);
    // Mangled strings start with _Z. We can't get substitute Symbols for mangled
    // strings.
    // TODO: this is doable in the BE with more symbol parsing.
    if (updateFn.startsWith("_Z")) return null;

    if (updateFn.contains("update")) return updateFn.replace("update", defaultSymbol);
    if (updateFn.contains("Update")) {
      char[] array = defaultSymbol.toCharArray();
      array[0] = Character.toUpperCase(array[0]);
      String s = new String(array);
      return updateFn.replace("Update", s);
    }
    return null;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    Preconditions.checkNotNull(fn_);
    Preconditions.checkState(fn_ instanceof AggregateFunction);
    AggregateFunction uda = (AggregateFunction) fn_;

    if (uda.getNumArgs() == 0) {
      throw new AnalysisException("UDAs must take at least one argument.");
    }

    if (uda.getBinaryType() == TFunctionBinaryType.JAVA) {
      throw new AnalysisException("Java UDAs are not supported.");
    }

    // TODO: these are temporarily restrictions since the BE cannot yet
    // execute them.
    if (uda.getBinaryType() == TFunctionBinaryType.IR) {
      throw new AnalysisException("IR UDAs are not yet supported.");
    }
    if (fn_.hasVarArgs()) {
      throw new AnalysisException("UDAs with varargs are not yet supported.");
    }
    if (fn_.getNumArgs() > 8) {
      throw new AnalysisException(
          "UDAs with more than 8 arguments are not yet supported.");
    }

    if (uda.getReturnType().getPrimitiveType() == PrimitiveType.CHAR) {
      throw new AnalysisException("UDAs with CHAR return type are not yet supported.");
    }
    if (uda.getReturnType().getPrimitiveType() == PrimitiveType.VARCHAR) {
      throw new AnalysisException("UDAs with VARCHAR return type are not yet supported.");
    }
    for (int i = 0; i < uda.getNumArgs(); ++i) {
      if (uda.getArgs()[i].getPrimitiveType() == PrimitiveType.CHAR) {
        throw new AnalysisException("UDAs with CHAR arguments are not yet supported.");
      }
      if (uda.getArgs()[i].getPrimitiveType() == PrimitiveType.VARCHAR) {
        throw new AnalysisException("UDAs with VARCHAR arguments are not yet supported.");
      }
    }

    Type intermediateType = null;
    if (intermediateTypeDef_ == null) {
      intermediateType = uda.getReturnType();
    } else {
      intermediateTypeDef_.analyze(analyzer);
      intermediateType = intermediateTypeDef_.getType();
    }
    uda.setIntermediateType(intermediateType);

    // Check arguments that are only valid in UDFs are not set.
    checkOptArgNotSet(OptArg.SYMBOL);
    checkOptArgNotSet(OptArg.PREPARE_FN);
    checkOptArgNotSet(OptArg.CLOSE_FN);

    // The user must provide the symbol for Update.
    uda.setUpdateFnSymbol(uda.lookupSymbol(
        checkAndGetOptArg(OptArg.UPDATE_FN), TSymbolType.UDF_EVALUATE, intermediateType,
        uda.hasVarArgs(), uda.getArgs()));

    // If the ddl did not specify the init/serialize/merge/finalize function
    // Symbols, guess them based on the update fn Symbol.
    Preconditions.checkNotNull(uda.getUpdateFnSymbol());
    uda.setInitFnSymbol(getSymbolSymbol(OptArg.INIT_FN, "init"));
    uda.setSerializeFnSymbol(getSymbolSymbol(OptArg.SERIALIZE_FN, "serialize"));
    uda.setMergeFnSymbol(getSymbolSymbol(OptArg.MERGE_FN, "merge"));
    uda.setFinalizeFnSymbol(getSymbolSymbol(OptArg.FINALIZE_FN, "finalize"));

    // Init and merge are required.
    if (uda.getInitFnSymbol() == null) reportCouldNotInferSymbol("init");
    if (uda.getMergeFnSymbol() == null) reportCouldNotInferSymbol("merge");

    // Validate that all set symbols exist.
    uda.setInitFnSymbol(uda.lookupSymbol(uda.getInitFnSymbol(),
        TSymbolType.UDF_EVALUATE, intermediateType, false));
    uda.setMergeFnSymbol(uda.lookupSymbol(uda.getMergeFnSymbol(),
        TSymbolType.UDF_EVALUATE, intermediateType, false, intermediateType));
    if (uda.getSerializeFnSymbol() != null) {
      try {
        uda.setSerializeFnSymbol(uda.lookupSymbol(uda.getSerializeFnSymbol(),
            TSymbolType.UDF_EVALUATE, null, false, intermediateType));
      } catch (AnalysisException e) {
        if (optArgs_.get(OptArg.SERIALIZE_FN) != null) {
          throw e;
        } else {
          // Ignore, these symbols are optional.
          uda.setSerializeFnSymbol(null);
        }
      }
    }
    if (uda.getFinalizeFnSymbol() != null) {
      try {
        uda.setFinalizeFnSymbol(uda.lookupSymbol(
            uda.getFinalizeFnSymbol(), TSymbolType.UDF_EVALUATE, null, false,
            intermediateType));
      } catch (AnalysisException e) {
        if (optArgs_.get(OptArg.FINALIZE_FN) != null) {
          throw e;
        } else {
          // Ignore, these symbols are optional.
          uda.setFinalizeFnSymbol(null);
        }
      }
    }

    // If the intermediate type is not the return type, then finalize is
    // required.
    if (!intermediateType.equals(fn_.getReturnType()) &&
        uda.getFinalizeFnSymbol() == null) {
      throw new AnalysisException("Finalize() is required for this UDA.");
    }

    sqlString_ = uda.toSql(ifNotExists_);
  }

  @Override
  protected Function createFunction(FunctionName fnName, List<Type> argTypes,
      Type retType, boolean hasVarArgs) {
    return new AggregateFunction(fnName_, args_.getArgTypes(), retTypeDef_.getType(),
        args_.hasVarArgs());
  }
}
