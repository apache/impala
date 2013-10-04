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

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TDropFunctionParams;
import com.cloudera.impala.thrift.TFunctionName;

/**
 * Represents a DROP [IF EXISTS] FUNCTION statement
 * TODO: try to consolidate this with the other Drop*Stmt class, perhaps
 * by adding a DropStatementBase class.
 */
public class DropFunctionStmt extends StatementBase {
  private final Function desc_;
  private final boolean ifExists_;

  /**
   * Constructor for building the drop statement. If ifExists is true, an error will not
   * be thrown if the function does not exist.
   */
  public DropFunctionStmt(FunctionName fnName, FunctionArgs fnArgs, boolean ifExists) {
    desc_ = new Function(
        fnName, fnArgs.argTypes, PrimitiveType.INVALID_TYPE, fnArgs.hasVarArgs);
    ifExists_ = ifExists;
  }

  public FunctionName getFunction() { return desc_.getFunctionName(); }
  public boolean getIfExists() { return ifExists_; }

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
    params.setFn_name(new TFunctionName(desc_.getFunctionName().getDb(),
        desc_.getFunctionName().getFunction()));
    params.setArg_types(PrimitiveType.toThrift(desc_.getArgs()));
    params.setIf_exists(getIfExists());
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    desc_.getFunctionName().analyze(analyzer);
    String dbName = analyzer.getTargetDbName(desc_.getFunctionName());
    desc_.getFunctionName().setDb(dbName);
    if (analyzer.getCatalog().getDb(dbName, analyzer.getUser(), Privilege.DROP) == null
        && !ifExists_) {
      throw new AnalysisException(Analyzer.DB_DOES_NOT_EXIST_ERROR_MSG + dbName);
    }

    if (analyzer.getCatalog().getFunction(
        desc_, Function.CompareMode.IS_IDENTICAL) == null && !ifExists_) {
      throw new AnalysisException(
          Analyzer.FN_DOES_NOT_EXIST_ERROR_MSG + desc_.signatureString());
    }
  }
}
