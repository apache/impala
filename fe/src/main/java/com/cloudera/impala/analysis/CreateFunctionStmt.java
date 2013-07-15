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
import com.cloudera.impala.thrift.TPrimitiveType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents a CREATE FUNCTION statement.
 */
public class CreateFunctionStmt extends StatementBase {
  private final FunctionName fnName_;
  private final Udf function_;
  private final boolean ifNotExists_;
  private final String comment_;

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
  public CreateFunctionStmt(ArrayList<String> fnNamePath, ArrayList<PrimitiveType> fnArgs,
      PrimitiveType retType, HdfsURI location, String binaryName,
      boolean ifNotExists, String comment) {
    Preconditions.checkNotNull(fnArgs);
    Preconditions.checkNotNull(location);
    Preconditions.checkNotNull(binaryName);

    this.fnName_ = new FunctionName(fnNamePath);
    this.function_ = new Udf("", fnArgs, retType, location, binaryName);
    this.comment_ = comment;
    this.ifNotExists_ = ifNotExists;
  }

  public String getComment() { return comment_; }
  public boolean getIfNotExists() { return ifNotExists_; }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder("CREATE ");
    sb.append("FUNCTION ");
    if (ifNotExists_) sb.append("IF NOT EXISTS ");
    sb.append(function_.getDesc().signatureString());
    sb.append(" RETURNS ");
    sb.append(function_.getDesc().getReturnType());
    sb.append(" LOCATION ");
    sb.append(function_.getLocation());
    sb.append(" ");
    sb.append(function_.getBinaryName());
    if (comment_ != null) sb.append(" COMMENT = '" + comment_ + "'");
    return sb.toString();
  }

  public TCreateFunctionParams toThrift() {
    TCreateFunctionParams params = new TCreateFunctionParams();
    params.setFn_name(function_.getDesc().getName());
    params.setLocation(function_.getLocation().toString());
    params.setBinary_name(function_.getBinaryName());
    List<TPrimitiveType> types = Lists.newArrayList();
    if (function_.getDesc().getNumArgs() > 0) {
      for (PrimitiveType t: function_.getDesc().getArgs()) {
        types.add(t.toThrift());
      }
    }
    params.setArg_types(types);
    params.setRet_type(function_.getDesc().getReturnType().toThrift());
    params.setComment(getComment());
    params.setIf_not_exists(getIfNotExists());
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    // Validate function name is legal
    fnName_.analyze(analyzer);
    if (OpcodeRegistry.instance().getFunctionOperator(fnName_.getName()) !=
        FunctionOperator.INVALID_OPERATOR) {
      // TODO: should we allow overloaded version of the builtins?
      throw new AnalysisException("Function cannot have the same name as a builtin: " +
          fnName_.getName());
    }

    function_.getDesc().setName(fnName_.getName());

    if (analyzer.getCatalog().getUdf(function_.getDesc(),
        analyzer.getUser(), Privilege.CREATE, true) != null && !ifNotExists_) {
      throw new AnalysisException(Analyzer.FN_ALREADY_EXISTS_ERROR_MSG +
          function_.getDesc().signatureString());
    }

    function_.getLocation().analyze(analyzer, Privilege.CREATE);

    // TODO: check binaryName_ exists
  }
}
