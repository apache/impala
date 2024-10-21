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

import com.google.common.base.Preconditions;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.FunctionParams;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;

import java.util.Arrays;
import java.util.List;

/**
 * A FunctionCallExpr specialized for Calcite.
 *
 * The analysis for Calcite expressions is done through Calcite and
 * does not need the analysis provided through the Impala expression.
 * The analyzeImpl is overridden for FunctionCallExpr and only does
 * the minimal analysis needed.
 *
 */
public class AnalyzedFunctionCallExpr extends FunctionCallExpr {

  // Need to save the function because it is known at constructor time. The
  // resetAnalyzeState() method can be called at various points which could
  // set the fn_ member to null. So we save the function in the savedFunction_
  // variable so it can be properly set in analyzeImpl()
  private Function savedFunction_;

  // c'tor that takes a list of Exprs that eventually get converted to FunctionParams
  public AnalyzedFunctionCallExpr(Function fn, List<Expr> params, Type retType) {
    super(fn.getFunctionName(), params);
    this.savedFunction_ = fn;
    this.type_ = retType;
  }

  // c'tor when FunctionParams are known
  public AnalyzedFunctionCallExpr(Function fn, FunctionParams funcParams,
      Type retType) {
    super(fn.getFunctionName(), funcParams);
    this.savedFunction_ = fn;
    this.type_ = retType;
  }

  public AnalyzedFunctionCallExpr(AnalyzedFunctionCallExpr other) {
    super(other);
    this.savedFunction_ = other.savedFunction_;
    this.type_ = other.type_;
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
    return 1;
  }

  @Override
  public Expr clone() { return new AnalyzedFunctionCallExpr(this); }

  @Override
  public FunctionCallExpr cloneWithNewParams(FunctionParams params) {
    return new AnalyzedFunctionCallExpr(this.getFn(), params,
        this.type_);
  }

  /**
   * Resets lag and lead function after it has been standardized by the
   * AnalyzedAnalyticExpr wrapper.  Because of the current architecture,
   * this fix is a bit hacky as we are mutating the object. Also, this
   * is a bit specific to analytic expressions and does not belong within
   * the general FunctionCallExpr, but this is the way analytic expressions
   * currently work.
   */
  public void resetAnalyticOffsetFn() throws ImpalaException {
    if (fn_ instanceof AggregateFunction) {
      // since the function lookup is based on exact match of data types (unlike Impala's
      // builtin db which does allow matches based on implicit cast), we need to adjust
      // one or more operands based on the function type
      List<Type> operandTypes = Arrays.asList(collectChildReturnTypes());
      String funcName = getFnName().getFunction().toLowerCase();
      switch (funcName) {
        case "lag":
        case "lead":
          // at this point the function should have been standardized into
          // a 3 operand function and the second operand is the 'offset' which
          // should be an integer (tinyint/smallint/int/bigint) type
          Preconditions.checkArgument(operandTypes.size() == 3 &&
              operandTypes.get(1).isIntegerType());
          // upcast the second argument (offset) since it must always be BIGINT
          if (operandTypes.get(1) != Type.BIGINT) {
            operandTypes.set(1, Type.BIGINT);
            uncheckedCastChild(Type.BIGINT, 1);
          }
          // Last argument could be NULL with TYPE_NULL but since Impala BE expects
          // a concrete type, we cast it to the type of the first argument
          if (operandTypes.get(2) == Type.NULL) {
            Preconditions.checkArgument(operandTypes.get(0) != Type.NULL);
            operandTypes.set(2, operandTypes.get(0));
            uncheckedCastChild(operandTypes.get(0), 2);
          }
          fn_ = FunctionResolver.getExactFunction(getFnName().getFunction(),
              ImpalaTypeConverter.createRelDataTypes(operandTypes));
          this.savedFunction_ = fn_;
          break;
      default:
        throw new AnalysisException("Unsupported aggregate function.");
      }
    }
  }
}
