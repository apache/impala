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
import com.cloudera.impala.catalog.ColumnType;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.ScalarFunction;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TFunctionBinaryType;
import com.cloudera.impala.thrift.TSymbolType;

/**
 * Represents a CREATE FUNCTION statement.
 */
public class CreateUdfStmt extends CreateFunctionStmtBase {
  // Same as super.fn_. Typed here for convenience.
  private final ScalarFunction udf_;

  /**
   * Builds a CREATE FUNCTION statement
   * @param fnName - Name of the function
   * @param fnArgs - List of types for the arguments to this function
   * @param retType - The type this function returns.
   * @param location - Path in HDFS containing the UDA.
   * @param ifNotExists - If true, no errors are thrown if the function already exists
   * @param additionalArgs - Key/Value pairs for additional arguments. The keys are
   *        validated in analyze()
   */
  public CreateUdfStmt(FunctionName fnName, FunctionArgs args,
      ColumnType retType, HdfsUri location, boolean ifNotExists,
      HashMap<CreateFunctionStmtBase.OptArg, String> optArgs) {
    super(new ScalarFunction(fnName, args, retType), location, ifNotExists, optArgs);
    udf_ = (ScalarFunction)fn_;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    super.analyze(analyzer);

    if (udf_.getBinaryType() == TFunctionBinaryType.HIVE) {
      if (udf_.getReturnType().getPrimitiveType() == PrimitiveType.TIMESTAMP) {
        throw new AnalysisException(
            "Hive UDFs that use timestamp are not yet supported.");
      }
      for (int i = 0; i < udf_.getNumArgs(); ++i) {
        if (udf_.getArgs()[i].getPrimitiveType() == PrimitiveType.TIMESTAMP) {
          throw new AnalysisException(
              "Hive UDFs that use timestamp are not yet supported.");
        }
      }
    }

    // Check the user provided symbol exists
    udf_.setSymbolName(udf_.lookupSymbol(
        checkAndGetOptArg(OptArg.SYMBOL), TSymbolType.UDF_EVALUATE, null,
        udf_.hasVarArgs(), udf_.getArgs()));

    // Set optional Prepare/Close functions
    String prepareFn = optArgs_.get(OptArg.PREPARE_FN);
    if (prepareFn != null) {
      udf_.setPrepareFnSymbol(udf_.lookupSymbol(prepareFn, TSymbolType.UDF_PREPARE));
    }
    String closeFn = optArgs_.get(OptArg.CLOSE_FN);
    if (closeFn != null) {
      udf_.setCloseFnSymbol(udf_.lookupSymbol(closeFn, TSymbolType.UDF_CLOSE));
    }

    // Udfs should not set any of these
    checkOptArgNotSet(OptArg.UPDATE_FN);
    checkOptArgNotSet(OptArg.INIT_FN);
    checkOptArgNotSet(OptArg.SERIALIZE_FN);
    checkOptArgNotSet(OptArg.MERGE_FN);
    checkOptArgNotSet(OptArg.FINALIZE_FN);

    StringBuilder sb = new StringBuilder("CREATE ");
    sb.append("FUNCTION ");
    if (ifNotExists_) sb.append("IF NOT EXISTS ");
    sb.append(udf_.signatureString())
      .append(" RETURNS ").append(udf_.getReturnType())
      .append(" LOCATION ").append(udf_.getLocation())
      .append(" SYMBOL=").append(udf_.getSymbolName());
    if (getComment() != null) sb.append(" COMMENT = '" + getComment() + "'");
    sqlString_ = sb.toString();
  }
}
