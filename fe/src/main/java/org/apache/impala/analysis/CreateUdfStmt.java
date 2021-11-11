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

import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.hive.executor.JavaUdfDataType;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TSymbolType;

import com.google.common.base.Preconditions;

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
      Map<CreateFunctionStmtBase.OptArg, String> optArgs) {
    super(fnName, args, retTypeDef, location, ifNotExists, optArgs);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    Preconditions.checkNotNull(fn_);
    Preconditions.checkState(fn_ instanceof ScalarFunction);
    ScalarFunction udf = (ScalarFunction) fn_;

    if (hasSignature()) {
      if (udf.getBinaryType() == TFunctionBinaryType.JAVA) {
        if (!JavaUdfDataType.isSupported(udf.getReturnType())) {
          throw new AnalysisException(
              "Type " + udf.getReturnType().toSql() + " is not supported for Java UDFs.");
        }
        for (int i = 0; i < udf.getNumArgs(); ++i) {
          if (!JavaUdfDataType.isSupported(udf.getArgs()[i])) {
            throw new AnalysisException(
                "Type " + udf.getArgs()[i].toSql() + " is not supported for Java UDFs.");
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

    // Check that there is no function with the same name and isPersistent field not
    // the same as udf.isPersistent_. For example we don't allow two JAVA udfs with
    // same name and opposite persistence values set. This only applies for JAVA udfs
    // as all the native udfs are persisted. Additionally we don't throw exceptions
    // if "IF NOT EXISTS" is specified in the query.
    if (udf.getBinaryType() != TFunctionBinaryType.JAVA || ifNotExists_) return;

    Preconditions.checkNotNull(db_);
    for (Function fn: db_.getFunctions(udf.functionName())) {
      if (!hasSignature() || (hasSignature() && fn.isPersistent())) {
        throw new AnalysisException(
            String.format(Analyzer.FN_ALREADY_EXISTS_ERROR_MSG +
                fn.signatureString()));
      }
    }
  }

  @Override
  protected Function createFunction(FunctionName fnName, List<Type> argTypes,
      Type retType, boolean hasVarArgs) {
    return new ScalarFunction(fnName, argTypes, retType, hasVarArgs);
  }
}
