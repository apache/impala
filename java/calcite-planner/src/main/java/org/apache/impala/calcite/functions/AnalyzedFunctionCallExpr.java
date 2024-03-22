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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.FunctionParams;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;

import java.util.List;

/**
 * A FunctionCallExpr that is always in an analyzed state
 *
 * The analysis for Calcite expressions can be done in the constructor
 * rather than issuing a separate call to "analyze" after the object
 * is constructed.
 *
 * For this class, we also want to override the "analyzeImpl" call since
 * the "fn_" member is passed in rather than deduced at analysis time.
 *
 */
public class AnalyzedFunctionCallExpr extends FunctionCallExpr {

  // Need to save the function because it is known at constructor time. The
  // resetAnalyzeState() method can be called at various points which could
  // set the fn_ member to null. So we save the function in the savedFunction_
  // variable so it can be properly set in analyzeImpl()
  private final Function savedFunction_;

  private final Analyzer analyzer_;

  // c'tor that takes a list of Exprs that eventually get converted to FunctionParams
  public AnalyzedFunctionCallExpr(Function fn, List<Expr> params,
      RexCall rexCall, Type retType, Analyzer analyzer) throws ImpalaException {
    super(fn.getFunctionName(), params);
    this.savedFunction_ = fn;
    this.type_ = retType;
    this.analyze(analyzer);
    this.analyzer_ = analyzer;
  }

  // c'tor which does not depend on Calcite's RexCall but is used when Impala's
  // FunctionParams are created or there is some modifications to it
  public AnalyzedFunctionCallExpr(Function fn, FunctionParams funcParams,
      Type retType, Analyzer analyzer) throws ImpalaException {
    super(fn.getFunctionName(), funcParams);
    this.savedFunction_ = fn;
    this.type_ = retType;
    this.analyze(analyzer);
    this.analyzer_ = analyzer;
  }

  public AnalyzedFunctionCallExpr(AnalyzedFunctionCallExpr other) {
    super(other);
    this.savedFunction_ = other.savedFunction_;
    this.type_ = other.type_;
    this.analyzer_ = other.analyzer_;
    try {
      this.analyze(this.analyzer_);
    } catch (ImpalaException e) {
      //TODO: IMPALA-13097: Don't throw runtime exception
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    // Functions have already gone through the analysis phase in Calcite so the
    // analyzeImpl method is overridden.  However, the FunctionName object
    // still needs to be analyzed.  This allows Expr.toSql() to display the names
    // correctly in the explain plan.
    getFnName().analyze(analyzer);
    this.fn_ = savedFunction_;
  }

  @Override
  protected float computeEvalCost() {
    // TODO: IMPALA-13098: need to implement
    return UNKNOWN_COST;
  }

  @Override
  public Expr clone() { return new AnalyzedFunctionCallExpr(this); }

  @Override
  public FunctionCallExpr cloneWithNewParams(FunctionParams params) {
    try {
      return new AnalyzedFunctionCallExpr(this.getFn(), params,
          this.type_, analyzer_);
    } catch (ImpalaException e) {
      throw new IllegalStateException(e);
    }
  }

}
