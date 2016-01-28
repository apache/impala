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

import com.cloudera.impala.authorization.AuthorizeableFn;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.PrivilegeRequest;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TDropFunctionParams;
import com.cloudera.impala.thrift.TFunctionCategory;

import java.util.ArrayList;

/**
 * Represents a DROP [IF EXISTS] FUNCTION statement
 * TODO: try to consolidate this with the other Drop*Stmt class, perhaps
 * by adding a DropStatementBase class.
 */
public class DropFunctionStmt extends StatementBase {
  private final FunctionName fnName_;
  private final FunctionArgs fnArgs_;
  private final boolean ifExists_;

  // Set in analyze().
  private Function desc_;

  /**
   * Constructor for building the drop statement. If ifExists is true, an error will not
   * be thrown if the function does not exist.
   */
  public DropFunctionStmt(FunctionName fnName, FunctionArgs fnArgs, boolean ifExists) {
    fnName_ = fnName;
    fnArgs_ = fnArgs;
    ifExists_ = ifExists;
  }

  public FunctionName getFunction() { return desc_.getFunctionName(); }
  public boolean getIfExists() { return ifExists_; }
  private boolean hasSignature() { return fnArgs_ != null; }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder("DROP FUNCTION");
    if (ifExists_) sb.append(" IF EXISTS ");
    sb.append(desc_.signatureString());
    sb.append(")");
    return sb.toString();
  }

  public TDropFunctionParams toThrift() {
    TDropFunctionParams params = new TDropFunctionParams();
    params.setFn_name(desc_.getFunctionName().toThrift());
    params.setArg_types(Type.toThrift(desc_.getArgs()));
    params.setIf_exists(getIfExists());
    if (hasSignature()) params.setSignature(desc_.signatureString());
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    fnName_.analyze(analyzer);

    if (hasSignature()) {
      fnArgs_.analyze(analyzer);
      desc_ = new Function(fnName_, fnArgs_.getArgTypes(), Type.INVALID,
          fnArgs_.hasVarArgs());
    } else {
      desc_ = new Function(fnName_, new ArrayList<Type>(), Type.INVALID,
          false);
    }

    // For now, if authorization is enabled, the user needs ALL on the server
    // to drop functions.
    // TODO: this is not the right granularity but acceptable for now.
    analyzer.registerPrivReq(new PrivilegeRequest(
        new AuthorizeableFn(desc_.signatureString()), Privilege.ALL));

    Db db =  analyzer.getDb(desc_.dbName(), Privilege.DROP, false);
    if (db == null && !ifExists_) {
      throw new AnalysisException(Analyzer.DB_DOES_NOT_EXIST_ERROR_MSG + desc_.dbName());
    }

    if (!hasSignature() && db != null && db.getFunctions(
        desc_.functionName()).isEmpty() && !ifExists_) {
      throw new AnalysisException(
          Analyzer.FN_DOES_NOT_EXIST_ERROR_MSG + desc_.functionName());
    }

    if (hasSignature() && analyzer.getCatalog().getFunction(
        desc_, Function.CompareMode.IS_IDENTICAL) == null && !ifExists_) {
      throw new AnalysisException(
          Analyzer.FN_DOES_NOT_EXIST_ERROR_MSG + desc_.signatureString());
    }
  }
}
