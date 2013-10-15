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
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Uda;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TCreateFunctionParams;
import com.cloudera.impala.thrift.TFunctionBinaryType;
import com.cloudera.impala.thrift.TUda;
import com.google.common.base.Preconditions;

/**
 * Represents a CREATE AGGREGATE FUNCTION statement.
 */
public class CreateUdaStmt extends CreateFunctionStmtBase {
  // Same as super.fn_. Typed here for convenience.
  private final Uda uda_;
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
  public CreateUdaStmt(FunctionName fnName, FunctionArgs args,
      PrimitiveType retType, ColumnType intermediateType,
      HdfsURI location, boolean ifNotExists,
      HashMap<CreateFunctionStmtBase.OptArg, String> optArgs) {
    super(new Uda(fnName, args, retType), location, ifNotExists, optArgs);
    uda_ = (Uda)super.fn_;
    intermediateType_ = intermediateType;
  }

  @Override
  public TCreateFunctionParams toThrift() {
    TCreateFunctionParams params = super.toThrift();
    TUda udaFn = new TUda();
    udaFn.setUpdate_fn_name(uda_.getUpdateFnName());
    udaFn.setInit_fn_name(uda_.getInitFnName());
    udaFn.setSerialize_fn_name(uda_.getSerializeFnName());
    udaFn.setMerge_fn_name(uda_.getMergeFnName());
    udaFn.setFinalize_fn_name(uda_.getFinalizeFnName());
    udaFn.setIntermediate_type(uda_.getIntermediateType().toThrift());
    params.getFn().setUda(udaFn);
    return params;
  }

  private void reportCouldNotInferSymbol(String function) throws AnalysisException {
    throw new AnalysisException("Could not infer symbol for "
        + function + "() function.");
  }

  // Gets the symbol for 'arg'. If the user set it from the dll, return that. Otherwise
  // try to infer the name from the Update function. To infer the name, the update
  // function must contain "update" or "Update" and we switch that out with 'defaultName'.
  // Returns null if no symbol was found.
  private String getSymbolName(OptArg arg, String defaultName) {
    Preconditions.checkState(uda_.getUpdateFnName() != null);
    // First lookup if the user explicitly set it.
    if (optArgs_.get(arg) != null) return optArgs_.get(arg);
    // Try to match it from Update
    String updateFn = optArgs_.get(OptArg.UPDATE_FN);
    // Mangled strings start with _Z. We can't get substitute names for mangled
    // strings.
    // TODO: this is doable in the BE with more symbol parsing.
    if (updateFn.startsWith("_Z")) return null;

    if (updateFn.contains("update")) return updateFn.replace("update", defaultName);
    if (updateFn.contains("Update")) {
      char[] array = defaultName.toCharArray();
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

    if (uda_.getBinaryType() == TFunctionBinaryType.HIVE) {
      throw new AnalysisException("Java UDAs are not supported.");
    }

    if (uda_.getNumArgs() == 0) {
      throw new AnalysisException("UDAs must take at least one argument.");
    }

    if (intermediateType_ == null) {
      intermediateType_ = ColumnType.createType(uda_.getReturnType());
    } else {
      intermediateType_.analyze();
    }
    uda_.setIntermediateType(intermediateType_);

    // Check arguments that are only valid in UDFs are not set.
    checkOptArgNotSet(OptArg.SYMBOL);

    // The user must provide the symbol for Update.
    uda_.setUpdateFnName(lookupSymbol(
        checkAndGetOptArg(OptArg.UPDATE_FN), intermediateType_, fn_.hasVarArgs(),
        ColumnType.toColumnType(fn_.getArgs())));

    // If the ddl did not specify the init/serialize/merge/finalize function
    // names, guess them based on the update fn name.
    uda_.setInitFnName(getSymbolName(OptArg.INIT_FN, "init"));
    uda_.setSerializeFnName(getSymbolName(OptArg.SERIALIZE_FN, "serialize"));
    uda_.setMergeFnName(getSymbolName(OptArg.MERGE_FN, "merge"));
    uda_.setFinalizeFnName(getSymbolName(OptArg.FINALIZE_FN, "finalize"));

    // Init and merge are required.
    if (uda_.getInitFnName() == null) reportCouldNotInferSymbol("init");
    if (uda_.getMergeFnName() == null) reportCouldNotInferSymbol("merge");

    // Validate that all set symbols exist.
    uda_.setInitFnName(lookupSymbol(uda_.getInitFnName(), intermediateType_, false));
    uda_.setMergeFnName(lookupSymbol(uda_.getMergeFnName(), intermediateType_, false,
        intermediateType_));
    if (uda_.getSerializeFnName() != null) {
      try {
        uda_.setSerializeFnName(lookupSymbol(
            uda_.getSerializeFnName(), null, false, intermediateType_));
      } catch (AnalysisException e) {
        if (optArgs_.get(OptArg.SERIALIZE_FN) != null) {
          throw e;
        } else {
          // Ignore, these symbols are optional.
          uda_.setSerializeFnName(null);
        }
      }
    }
    if (uda_.getFinalizeFnName() != null) {
      try {
        uda_.setFinalizeFnName(lookupSymbol(
            uda_.getFinalizeFnName(), null, false, intermediateType_));
      } catch (AnalysisException e) {
        if (optArgs_.get(OptArg.FINALIZE_FN) != null) {
          throw e;
        } else {
          // Ignore, these symbols are optional.
          uda_.setFinalizeFnName(null);
        }
      }
    }

    // If the intermediate type is not the return type, then finalize is
    // required.
    if (intermediateType_.getType() != fn_.getReturnType() &&
        uda_.getFinalizeFnName() == null) {
      throw new AnalysisException("Finalize() is required for this UDA.");
    }

    StringBuilder sb = new StringBuilder("CREATE ");
    sb.append("AGGREGATE FUNCTION ");
    if (ifNotExists_) sb.append("IF NOT EXISTS ");
    sb.append(uda_.signatureString())
      .append(" RETURNS ").append(uda_.getReturnType())
      .append(" INTERMEDIATE ").append(uda_.getIntermediateType())
      .append(" LOCATION ").append(uda_.getLocation())
      .append(" UPDATE_FN=").append(uda_.getUpdateFnName())
      .append(" INIT_FN=").append(uda_.getInitFnName())
      .append(" MERGE_FN=").append(uda_.getMergeFnName());
    if (uda_.getSerializeFnName() != null) {
      sb.append(" SERIALIZE_FN=").append(uda_.getSerializeFnName());
    }
    if (uda_.getFinalizeFnName() != null) {
      sb.append(" FINALIZE_FN=").append(uda_.getFinalizeFnName());
    }
    if (getComment() != null) sb.append(" COMMENT = '" + getComment() + "'");
    sqlString_ = sb.toString();
  }
}
