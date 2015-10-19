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

import jline.internal.Preconditions;

import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.ScalarFunction;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TFunctionBinaryType;
import com.cloudera.impala.thrift.TSymbolType;

/**
 * Represents a CREATE FUNCTION statement.
 */
public class CreateUdfStmt extends CreateFunctionStmtBase {
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
      TypeDef retTypeDef, HdfsUri location, boolean ifNotExists,
      HashMap<CreateFunctionStmtBase.OptArg, String> optArgs) {
    super(fnName, args, retTypeDef, location, ifNotExists, optArgs);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    Preconditions.checkNotNull(fn_);
    Preconditions.checkNotNull(fn_ instanceof ScalarFunction);
    ScalarFunction udf = (ScalarFunction) fn_;

    if (udf.getBinaryType() == TFunctionBinaryType.HIVE) {
      if (!udf.getReturnType().isScalarType()) {
        throw new AnalysisException("Non-scalar return types not supported: "
            + udf.getReturnType().toSql());
      }
      if (udf.getReturnType().isTimestamp()) {
        throw new AnalysisException(
            "Hive UDFs that use TIMESTAMP are not yet supported.");
      }
      if (udf.getReturnType().isDecimal()) {
        throw new AnalysisException(
            "Hive UDFs that use DECIMAL are not yet supported.");
      }
      for (int i = 0; i < udf.getNumArgs(); ++i) {
        if (!udf.getArgs()[i].isScalarType()) {
          throw new AnalysisException("Non-scalar argument types not supported: "
              + udf.getArgs()[i].toSql());
        }
        if (udf.getArgs()[i].isTimestamp()) {
          throw new AnalysisException(
              "Hive UDFs that use TIMESTAMP are not yet supported.");
        }
        if (udf.getArgs()[i].isDecimal()) {
          throw new AnalysisException(
              "Hive UDFs that use DECIMAL are not yet supported.");
        }
      }
    }

    if (udf.getReturnType().getPrimitiveType() == PrimitiveType.CHAR) {
      throw new AnalysisException("UDFs that use CHAR are not yet supported.");
    }
    if (udf.getReturnType().getPrimitiveType() == PrimitiveType.VARCHAR) {
      throw new AnalysisException("UDFs that use VARCHAR are not yet supported.");
    }
    for (int i = 0; i < udf.getNumArgs(); ++i) {
      if (udf.getArgs()[i].getPrimitiveType() == PrimitiveType.CHAR) {
        throw new AnalysisException("UDFs that use CHAR are not yet supported.");
      }
      if (udf.getArgs()[i].getPrimitiveType() == PrimitiveType.VARCHAR) {
        throw new AnalysisException("UDFs that use VARCHAR are not yet supported.");
      }
    }

    // Check the user provided symbol exists
    udf.setSymbolName(udf.lookupSymbol(
        checkAndGetOptArg(OptArg.SYMBOL), TSymbolType.UDF_EVALUATE, null,
        udf.hasVarArgs(), udf.getArgs()));

    // Set optional Prepare/Close functions
    String prepareFn = optArgs_.get(OptArg.PREPARE_FN);
    if (prepareFn != null) {
      udf.setPrepareFnSymbol(udf.lookupSymbol(prepareFn, TSymbolType.UDF_PREPARE));
    }
    String closeFn = optArgs_.get(OptArg.CLOSE_FN);
    if (closeFn != null) {
      udf.setCloseFnSymbol(udf.lookupSymbol(closeFn, TSymbolType.UDF_CLOSE));
    }

    // Udfs should not set any of these
    checkOptArgNotSet(OptArg.UPDATE_FN);
    checkOptArgNotSet(OptArg.INIT_FN);
    checkOptArgNotSet(OptArg.SERIALIZE_FN);
    checkOptArgNotSet(OptArg.MERGE_FN);
    checkOptArgNotSet(OptArg.FINALIZE_FN);

    sqlString_ = udf.toSql(ifNotExists_);
  }

  @Override
  protected Function createFunction(FunctionName fnName, ArrayList<Type> argTypes, Type retType,
      boolean hasVarArgs) {
    return new ScalarFunction(fnName, argTypes, retType, hasVarArgs);
  }
}
