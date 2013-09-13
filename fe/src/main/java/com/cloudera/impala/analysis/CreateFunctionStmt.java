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
import java.util.List;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Udf;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.opcode.FunctionOperator;
import com.cloudera.impala.thrift.TCreateFunctionParams;
import com.cloudera.impala.thrift.TFunctionName;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.cloudera.impala.thrift.TUdfType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents a CREATE FUNCTION statement.
 */
public class CreateFunctionStmt extends StatementBase {
  private final Udf function_;
  private final boolean ifNotExists_;
  private final String comment_;

  // Set in analyze()
  private String sqlString_;

  /**
   * Builds a CREATE FUNCTION statement
   * @param fnNamePath - Scoped name of the new function
   * @param fnArgs - List of types for the arguments to this function
   * @param retType - The type this function returns.
   * @param location - The HDFS location of where the binary for the function is stored.
   * @param comment - Comment to attach to the function
   * @param binaryName - Name of the function within the binary for this function.
             This is the classpath for Java functions and the symbol for native functions.
   * @param ifNotExists - If true, no errors are thrown if the function already exists
   */
  public CreateFunctionStmt(FunctionName fnName, ArrayList<PrimitiveType> fnArgs,
      PrimitiveType retType, HdfsURI location, String binaryName,
      boolean ifNotExists, String comment) {
    Preconditions.checkNotNull(fnArgs);
    Preconditions.checkNotNull(location);
    Preconditions.checkNotNull(binaryName);

    this.function_ = new Udf(fnName, fnArgs, retType, location, binaryName);
    this.comment_ = comment;
    this.ifNotExists_ = ifNotExists;
  }

  public String getComment() { return comment_; }
  public boolean getIfNotExists() { return ifNotExists_; }

  @Override
  public String toSql() { return sqlString_; }

  public TCreateFunctionParams toThrift() {
    TCreateFunctionParams params = new TCreateFunctionParams();
    params.setFn_name(new TFunctionName(function_.dbName(), function_.functionName()));
    params.setLocation(function_.getLocation().toString());
    params.setBinary_name(function_.getBinaryName());
    params.setUdf_type(function_.getUdfType());
    List<TPrimitiveType> types = Lists.newArrayList();
    if (function_.getNumArgs() > 0) {
      for (PrimitiveType t: function_.getArgs()) {
        types.add(t.toThrift());
      }
    }
    params.setArg_types(types);
    params.setRet_type(function_.getReturnType().toThrift());
    params.setComment(getComment());
    params.setIf_not_exists(getIfNotExists());
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    // Validate function name is legal
    function_.getName().analyze(analyzer);

    // If the function name is not fully qualified, it can't be the same as a builtin
    if (!function_.getName().isFullyQualified() &&
        OpcodeRegistry.instance().getFunctionOperator(
            function_.functionName()) != FunctionOperator.INVALID_OPERATOR) {
      throw new AnalysisException("Function cannot have the same name as a builtin: " +
          function_.functionName());
    }

    String dbName = analyzer.getTargetDbName(function_.getName());
    function_.getName().setDb(dbName);

    if (analyzer.getCatalog().getDb(
        dbName, analyzer.getUser(), Privilege.CREATE) == null) {
      throw new AnalysisException(Analyzer.DB_DOES_NOT_EXIST_ERROR_MSG + dbName);
    }
    if (analyzer.getCatalog().getUdf(function_,true) != null &&
        !ifNotExists_) {
      throw new AnalysisException(Analyzer.FN_ALREADY_EXISTS_ERROR_MSG +
          function_.signatureString());
    }

    // TODO: check binaryName_ exists
    function_.getLocation().analyze(analyzer, Privilege.CREATE);

    // Check the file type from the binary type to infer the type of the UDF
    // TODO: we could consider adding this as part of the create function ddl
    // but there seems to be little reason to deviate from the standard extensions
    // and just adds more boilerplate to the create ddl.
    TUdfType udfType = null;
    String udfBinaryPath = function_.getLocation().getLocation();
    int udfSuffixIndex = udfBinaryPath.lastIndexOf(".");
    if (udfSuffixIndex != -1) {
      String suffix = udfBinaryPath.substring(udfSuffixIndex + 1);
      if (suffix.equalsIgnoreCase("jar")) {
        udfType = TUdfType.HIVE;
      } else if (suffix.equalsIgnoreCase("so")) {
        udfType = TUdfType.NATIVE;
      } else if (suffix.equalsIgnoreCase("ll")) {
        udfType = TUdfType.IR;
      }
    }

    if (udfType == null) {
      throw new AnalysisException("Unknown udf binary type: '" + udfBinaryPath +
          "'. Binary must end in .jar, .so or .ll");
    }

    function_.setUdfType(udfType);

    StringBuilder sb = new StringBuilder("CREATE ");
    sb.append("FUNCTION ");
    if (ifNotExists_) sb.append("IF NOT EXISTS ");
    sb.append(function_.signatureString());
    sb.append(" RETURNS ");
    sb.append(function_.getReturnType());
    sb.append(" LOCATION ");
    sb.append(function_.getLocation());
    sb.append(" ");
    sb.append(function_.getBinaryName());
    if (comment_ != null) sb.append(" COMMENT = '" + comment_ + "'");
    sqlString_ = sb.toString();
  }
}
