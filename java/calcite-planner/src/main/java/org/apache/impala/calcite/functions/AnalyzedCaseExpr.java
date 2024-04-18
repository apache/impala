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

package org.apache.impala.calcite.functions;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.CaseExpr;
import org.apache.impala.analysis.CaseWhenClause;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

import java.util.List;

/**
 * A CaseExpr that is always in analyzed state
 */
public class AnalyzedCaseExpr extends CaseExpr {

  private final Function savedFunction_;
  private final Type savedType_;

  public AnalyzedCaseExpr(Function fn, List<CaseWhenClause> whenClauses,
      Expr elseExpr, Type retType) {
    // Calcite never has the first parameter 'case' expression filled in.
    super(null, whenClauses, elseExpr);
    this.savedFunction_ = fn;
    this.savedType_ = retType;
  }

  public AnalyzedCaseExpr(Function fn, Type retType, FunctionCallExpr decodeExpr) {
    super(decodeExpr);
    this.savedFunction_ = fn;
    this.savedType_ = retType;
  }

  public AnalyzedCaseExpr(AnalyzedCaseExpr other) {
    super(other);
    this.savedFunction_ = other.savedFunction_;
    this.savedType_ = other.savedType_;
  }

  @Override
  public Expr clone() {
    return new AnalyzedCaseExpr(this);
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    this.fn_ = this.savedFunction_;
    this.type_  = this.savedType_;
  }
}
