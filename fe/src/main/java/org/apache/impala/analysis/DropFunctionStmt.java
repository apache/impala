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

import java.util.ArrayList;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TDropFunctionParams;

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

  public boolean getIfExists() { return ifExists_; }
  private boolean hasSignature() { return fnArgs_ != null; }

  @Override
  public String toSql(ToSqlOptions options) {
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
    fnName_.analyze(analyzer, false);

    if (hasSignature()) {
      fnArgs_.analyze(analyzer);
      desc_ = new Function(fnName_, fnArgs_.getArgTypes(), Type.INVALID,
          fnArgs_.hasVarArgs());
    } else {
      desc_ = new Function(fnName_, new ArrayList<>(), Type.INVALID,
          false);
    }

    // Start with ANY privilege in case of IF EXISTS, and register DROP privilege later
    // only if the function exists. See IMPALA-8851 for more explanation.
    registerFnPriv(analyzer, ifExists_ ? Privilege.ANY : Privilege.DROP);

    FeDb db =  analyzer.getDb(desc_.dbName(), false);
    if (db == null) {
      if (ifExists_) return;
      // db does not exist and if exists clause is not provided
      throw new AnalysisException(Analyzer.DB_DOES_NOT_EXIST_ERROR_MSG + desc_.dbName());
    }
    if (!hasSignature() && db.getFunctions(desc_.functionName()).isEmpty()) {
      if (ifExists_) return;
      throw new AnalysisException(
          Analyzer.FN_DOES_NOT_EXIST_ERROR_MSG + desc_.functionName());
    }
    if (hasSignature() && analyzer.getCatalog().getFunction(
        desc_, Function.CompareMode.IS_IDENTICAL) == null) {
      if (ifExists_) return;
      throw new AnalysisException(
          Analyzer.FN_DOES_NOT_EXIST_ERROR_MSG + desc_.signatureString());
    }

    // Register the "stronger" DROP privilege if only ANY was registered due to
    // IF EXISTS.
    if (ifExists_) registerFnPriv(analyzer, Privilege.DROP);
  }

  private void registerFnPriv(Analyzer analyzer, Privilege priv) {
    analyzer.registerPrivReq(builder ->
          builder.onFunction(desc_.dbName(), desc_.signatureString())
              .allOf(priv)
              .build());
  }
}
