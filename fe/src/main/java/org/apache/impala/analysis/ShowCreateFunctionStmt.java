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

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TFunctionCategory;
import org.apache.impala.thrift.TGetFunctionsParams;

import com.google.common.base.Preconditions;

/**
 * Representation of a SHOW CREATE FUNCTION statement which returns the "CREATE FUNCTION ..."
 * string that re-creates the function.
 *
 * Syntax: SHOW CREATE [AGGREGATE] FUNCTION [<db_name>.]<function_name>
 */
public class ShowCreateFunctionStmt extends StatementBase {
  private final FunctionName functionName_;
  private final TFunctionCategory category_;

  public ShowCreateFunctionStmt(FunctionName functionName, TFunctionCategory category) {
    Preconditions.checkNotNull(functionName);
    Preconditions.checkArgument(category == TFunctionCategory.SCALAR ||
        category == TFunctionCategory.AGGREGATE);
    functionName_ = functionName;
    category_ = category;
  }

  @Override
  public String toSql() {
    return "SHOW CREATE " +
        (category_ == TFunctionCategory.AGGREGATE ? "AGGREGATE " : "") +
        "FUNCTION " + functionName_;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    functionName_.analyze(analyzer);
    Db db = analyzer.getDb(functionName_.getDb(), Privilege.VIEW_METADATA);
    List<Function> functions = db.getFunctions(category_, functionName_.getFunction());
    if (functions.isEmpty()) {
      throw new AnalysisException("Function " + functionName_.getFunction() + "() " +
          "does not exist in database " + functionName_.getDb());
    }
  }

  public TGetFunctionsParams toThrift() {
    TGetFunctionsParams params = new TGetFunctionsParams();
    params.setCategory(category_);
    params.setDb(functionName_.getDb());
    params.setPattern(functionName_.getFunction());
    return params;
  }
}
