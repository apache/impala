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
import com.cloudera.impala.catalog.Udf;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TCreateFunctionParams;
import com.cloudera.impala.thrift.TUdf;

/**
 * Represents a CREATE FUNCTION statement.
 */
public class CreateUdfStmt extends CreateFunctionStmtBase {
  // Same as super.fn_. Typed here for convenience.
  private final Udf udf_;

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
      PrimitiveType retType, HdfsURI location, boolean ifNotExists,
      HashMap<CreateFunctionStmtBase.OptArg, String> optArgs) {
    super(new Udf(fnName, args, retType), location, ifNotExists, optArgs);
    udf_ = (Udf)fn_;
  }

  @Override
  public TCreateFunctionParams toThrift() {
    TCreateFunctionParams params = super.toThrift();
    params.getFn().setUdf(new TUdf(udf_.getSymbolName()));
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    super.analyze(analyzer);

    // Check the user provided symbol exists
    udf_.setSymbolName(lookupSymbol(
        checkAndGetOptArg(OptArg.SYMBOL), null, fn_.hasVarArgs(),
        ColumnType.toColumnType(fn_.getArgs())));

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
