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
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TDropFunctionParams;
import com.cloudera.impala.thrift.TFunctionName;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.google.common.collect.Lists;

/**
 * Represents a DROP [IF EXISTS] FUNCTION statement
 * TODO: try to consolidate this with the other Drop*Stmt class, perhaps
 * by adding a DropStatementBase class.
 */
public class DropFunctionStmt extends StatementBase {
  private final Function desc_;
  private final boolean ifExists_;

  // If true, drop UDA, otherwise, drop UDF
  private final boolean isAggregate_;

  /**
   * Constructor for building the drop statement. If ifExists is true, an error will not
   * be thrown if the function does not exist.
   */
  public DropFunctionStmt(FunctionName fnName, ArrayList<PrimitiveType> fnArgs,
      boolean ifExists, boolean isAggregate) {
    desc_ = new Function(fnName, fnArgs, PrimitiveType.INVALID_TYPE, false);
    ifExists_ = ifExists;
    isAggregate_ = isAggregate;
  }

  public FunctionName getFunction() { return desc_.getName(); }
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
    params.setFn_name(
        new TFunctionName(desc_.getName().getDb(), desc_.getName().getFunction()));
    List<TPrimitiveType> types = Lists.newArrayList();
    if (desc_.getNumArgs() > 0) {
      for (PrimitiveType t: desc_.getArgs()) {
        types.add(t.toThrift());
      }
    }
    params.setArg_types(types);
    params.setIf_exists(getIfExists());
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    desc_.getName().analyze(analyzer);
    String dbName = analyzer.getTargetDbName(desc_.getName());
    desc_.getName().setDb(dbName);
    if (analyzer.getCatalog().getDb(dbName, analyzer.getUser(), Privilege.DROP) == null
        && !ifExists_) {
      throw new AnalysisException(Analyzer.DB_DOES_NOT_EXIST_ERROR_MSG + dbName);
    }

    if (analyzer.getCatalog().getFunction(desc_, true) == null && !ifExists_) {
      throw new AnalysisException(
          Analyzer.FN_DOES_NOT_EXIST_ERROR_MSG + desc_.signatureString());
    }
  }
}
