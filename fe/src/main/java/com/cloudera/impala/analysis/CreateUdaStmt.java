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

import java.util.HashMap;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.AggregateFunction;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAggregateFunction;
import com.cloudera.impala.thrift.TCreateFunctionParams;
import com.cloudera.impala.thrift.TFunctionBinaryType;
import com.google.common.base.Preconditions;

/**
 * Represents a CREATE AGGREGATE FUNCTION statement.
 */
public class CreateUdaStmt extends CreateFunctionStmtBase {
  // Same as super.fn_. Typed here for convenience.
  private final AggregateFunction uda_;
  private ColumnType intermediateType_;

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
      ColumnType retType, ColumnType intermediateType,
      HdfsUri location, boolean ifNotExists,
      HashMap<CreateFunctionStmtBase.OptArg, String> optArgs) {
    super(new AggregateFunction(fnSymbol, args, retType), location, ifNotExists, optArgs);
    uda_ = (AggregateFunction)super.fn_;
    intermediateType_ = intermediateType;
  }

  @Override
  public TCreateFunctionParams toThrift() {
    TCreateFunctionParams params = super.toThrift();
    TAggregateFunction udaFn = new TAggregateFunction();
    udaFn.setUpdate_fn_symbol(uda_.getUpdateFnSymbol());
    udaFn.setInit_fn_symbol(uda_.getInitFnSymbol());
    udaFn.setSerialize_fn_symbol(uda_.getSerializeFnSymbol());
    udaFn.setMerge_fn_symbol(uda_.getMergeFnSymbol());
    udaFn.setFinalize_fn_symbol(uda_.getFinalizeFnSymbol());
    udaFn.setIntermediate_type(uda_.getIntermediateType().toThrift());
    params.getFn().setAggregate_fn(udaFn);
    return params;
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
    Preconditions.checkState(uda_.getUpdateFnSymbol() != null);
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
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    super.analyze(analyzer);

    if (uda_.getNumArgs() == 0) {
      throw new AnalysisException("UDAs must take at least one argument.");
    }

    if (uda_.getBinaryType() == TFunctionBinaryType.HIVE) {
      throw new AnalysisException("Java UDAs are not supported.");
    }

    // TODO: these are temporarily restrictions since the BE cannot yet
    // execute them.
    if (uda_.getBinaryType() == TFunctionBinaryType.IR) {
      throw new AnalysisException("IR UDAs are not yet supported.");
    }
    if (fn_.hasVarArgs()) {
      throw new AnalysisException("UDAs with varargs are not yet supported.");
    }
    if (fn_.getNumArgs() > 8) {
      throw new AnalysisException(
          "UDAs with more than 8 arguments are not yet supported.");
    }

    if (intermediateType_ == null) {
      intermediateType_ = uda_.getReturnType();
    } else {
      intermediateType_.analyze();
    }
    uda_.setIntermediateType(intermediateType_);

    // TODO: this is a temporary restriction. Remove when we can support
    // different intermediate types.
    if (!intermediateType_.equals(fn_.getReturnType())) {
      StringBuilder error = new StringBuilder();
      error.append("UDAs with an intermediate type, ")
           .append(intermediateType_.toString())
           .append(", that is different from the return type, ")
           .append(fn_.getReturnType().toString())
           .append(", are currently not supported.");
      throw new AnalysisException(error.toString());
    }

    // Check arguments that are only valid in UDFs are not set.
    checkOptArgNotSet(OptArg.SYMBOL);

    // The user must provide the symbol for Update.
    uda_.setUpdateFnSymbol(lookupSymbol(
        checkAndGetOptArg(OptArg.UPDATE_FN), intermediateType_, fn_.hasVarArgs(),
        fn_.getArgs()));

    // If the ddl did not specify the init/serialize/merge/finalize function
    // Symbols, guess them based on the update fn Symbol.
    uda_.setInitFnSymbol(getSymbolSymbol(OptArg.INIT_FN, "init"));
    uda_.setSerializeFnSymbol(getSymbolSymbol(OptArg.SERIALIZE_FN, "serialize"));
    uda_.setMergeFnSymbol(getSymbolSymbol(OptArg.MERGE_FN, "merge"));
    uda_.setFinalizeFnSymbol(getSymbolSymbol(OptArg.FINALIZE_FN, "finalize"));

    // Init and merge are required.
    if (uda_.getInitFnSymbol() == null) reportCouldNotInferSymbol("init");
    if (uda_.getMergeFnSymbol() == null) reportCouldNotInferSymbol("merge");

    // Validate that all set symbols exist.
    uda_.setInitFnSymbol(lookupSymbol(uda_.getInitFnSymbol(), intermediateType_, false));
    uda_.setMergeFnSymbol(lookupSymbol(uda_.getMergeFnSymbol(), intermediateType_, false,
        intermediateType_));
    if (uda_.getSerializeFnSymbol() != null) {
      try {
        uda_.setSerializeFnSymbol(lookupSymbol(
            uda_.getSerializeFnSymbol(), null, false, intermediateType_));
      } catch (AnalysisException e) {
        if (optArgs_.get(OptArg.SERIALIZE_FN) != null) {
          throw e;
        } else {
          // Ignore, these symbols are optional.
          uda_.setSerializeFnSymbol(null);
        }
      }
    }
    if (uda_.getFinalizeFnSymbol() != null) {
      try {
        uda_.setFinalizeFnSymbol(lookupSymbol(
            uda_.getFinalizeFnSymbol(), null, false, intermediateType_));
      } catch (AnalysisException e) {
        if (optArgs_.get(OptArg.FINALIZE_FN) != null) {
          throw e;
        } else {
          // Ignore, these symbols are optional.
          uda_.setFinalizeFnSymbol(null);
        }
      }
    }

    // If the intermediate type is not the return type, then finalize is
    // required.
    if (!intermediateType_.equals(fn_.getReturnType()) &&
        uda_.getFinalizeFnSymbol() == null) {
      throw new AnalysisException("Finalize() is required for this UDA.");
    }

    StringBuilder sb = new StringBuilder("CREATE ");
    sb.append("AGGREGATE FUNCTION ");
    if (ifNotExists_) sb.append("IF NOT EXISTS ");
    sb.append(uda_.signatureString())
      .append(" RETURNS ").append(uda_.getReturnType())
      .append(" INTERMEDIATE ").append(uda_.getIntermediateType())
      .append(" LOCATION ").append(uda_.getLocation())
      .append(" UPDATE_FN=").append(uda_.getUpdateFnSymbol())
      .append(" INIT_FN=").append(uda_.getInitFnSymbol())
      .append(" MERGE_FN=").append(uda_.getMergeFnSymbol());
    if (uda_.getSerializeFnSymbol() != null) {
      sb.append(" SERIALIZE_FN=").append(uda_.getSerializeFnSymbol());
    }
    if (uda_.getFinalizeFnSymbol() != null) {
      sb.append(" FINALIZE_FN=").append(uda_.getFinalizeFnSymbol());
    }
    if (getComment() != null) sb.append(" COMMENT = '" + getComment() + "'");
    sqlString_ = sb.toString();
  }
}
