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

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Uda;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TCreateFunctionParams;
import com.cloudera.impala.thrift.TCreateUdaParams;
import com.cloudera.impala.thrift.TFunctionBinaryType;
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
  public CreateUdaStmt(FunctionName fnName, ArrayList<PrimitiveType> fnArgs,
      PrimitiveType retType, ColumnType intermediateType,
      HdfsURI location, boolean ifNotExists,
      HashMap<CreateFunctionStmtBase.OptArg, String> optArgs) {
    super(new Uda(fnName, fnArgs, retType), location, ifNotExists, optArgs);
    uda_ = (Uda)super.fn_;
    intermediateType_ = intermediateType;
  }

  @Override
  public TCreateFunctionParams toThrift() {
    TCreateFunctionParams params = super.toThrift();
    TCreateUdaParams udaParams = new TCreateUdaParams();
    udaParams.setUpdate_fn_name(uda_.getUpdateFnName());
    udaParams.setInit_fn_name(uda_.getInitFnName());
    udaParams.setSerialize_fn_name(uda_.getSerializeFnName());
    udaParams.setMerge_fn_name(uda_.getMergeFnName());
    udaParams.setFinalize_fn_name(uda_.getFinalizeFnName());
    udaParams.setIntermediate_type(uda_.getIntermediateType().toThrift());
    params.setUda_params(udaParams);
    return params;
  }

  // Matches *update* to *target* or *Update* to *Target*. After guessing the symbol
  // name, verifies that symbol exists in the binary.
  // returns null if no matching function was found.
  private String inferName(String updateFn, String target) {
    if (updateFn.contains("update")) {
      String s = updateFn.replace("update", target);
      if (symbolExists(s)) return s;
    }
    if (updateFn.contains("Update")) {
      char[] array = target.toCharArray();
      array[0] = Character.toUpperCase(array[0]);
      String s = new String(array);
      s = updateFn.replace("Update", s);
      if (symbolExists(s)) return s;
    }
    return null;
  }

  private void reportCouldNotInferSymbol(String function) throws AnalysisException {
    throw new AnalysisException("Could not infer symbol for "
        + function + "() function.");
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    super.analyze(analyzer);

    if (uda_.getBinaryType() == TFunctionBinaryType.HIVE) {
      throw new AnalysisException("Java UDAs are not supported.");
    }

    if (uda_.getNumArgs() == 0) {
      throw new AnalysisException("UDA must take at least 1 argument.");
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
    uda_.setUpdateFnName(checkAndGetOptArg(OptArg.UPDATE_FN));
    if (!symbolExists(uda_.getUpdateFnName())) {
      reportSymbolNotFound(uda_.getUpdateFnName());
    }

    uda_.setInitFnName(optArgs_.get(OptArg.INIT_FN));
    uda_.setSerializeFnName(optArgs_.get(OptArg.SERIALIZE_FN));
    uda_.setMergeFnName(optArgs_.get(OptArg.MERGE_FN));
    uda_.setFinalizeFnName(optArgs_.get(OptArg.FINALIZE_FN));

    // If the ddl did not specify the init/serialize/merge/finalize function
    // names, guess them based on the update fn name. Otherwise, validate what
    // the ddl specified is a valid symbol.
    if (uda_.getInitFnName() == null) {
      uda_.setInitFnName(inferName(uda_.getUpdateFnName(), "init"));
      if (uda_.getInitFnName() == null) reportCouldNotInferSymbol("init");
    } else if (!symbolExists(uda_.getInitFnName())) {
      reportSymbolNotFound(uda_.getInitFnName());
    }

    if (uda_.getSerializeFnName() == null) {
      uda_.setSerializeFnName(inferName(uda_.getUpdateFnName(), "serialize"));
      // Serialize is optional.
    } else if (!symbolExists(uda_.getSerializeFnName())) {
      reportSymbolNotFound(uda_.getSerializeFnName());
    }

    if (uda_.getMergeFnName() == null) {
      uda_.setMergeFnName(inferName(uda_.getUpdateFnName(), "merge"));
      if (uda_.getMergeFnName() == null) reportCouldNotInferSymbol("merge");
    } else if (!symbolExists(uda_.getMergeFnName())) {
      reportSymbolNotFound(uda_.getMergeFnName());
    }

    if (uda_.getFinalizeFnName() == null) {
      uda_.setFinalizeFnName(inferName(uda_.getUpdateFnName(), "finalize"));
      if (uda_.getFinalizeFnName() == null) reportCouldNotInferSymbol("finalize");
    } else if (!symbolExists(uda_.getFinalizeFnName())) {
      reportSymbolNotFound(uda_.getFinalizeFnName());
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
    sb.append(" FINALIZE_FN=").append(uda_.getFinalizeFnName());

    if (getComment() != null) sb.append(" COMMENT = '" + getComment() + "'");
    sqlString_ = sb.toString();
  }
}
