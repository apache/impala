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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.CaseWhenClause;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Static Helper class that returns Exprs for RexCall nodes.
 */
public class RexCallConverter {
  protected static final Logger LOG =
      LoggerFactory.getLogger(RexCallConverter.class.getName());

  public static Map<SqlKind, BinaryPredicate.Operator> BINARY_OP_MAP =
      ImmutableMap.<SqlKind, BinaryPredicate.Operator> builder()
      .put(SqlKind.EQUALS, BinaryPredicate.Operator.EQ)
      .put(SqlKind.NOT_EQUALS, BinaryPredicate.Operator.NE)
      .put(SqlKind.GREATER_THAN, BinaryPredicate.Operator.GT)
      .put(SqlKind.GREATER_THAN_OR_EQUAL, BinaryPredicate.Operator.GE)
      .put(SqlKind.LESS_THAN, BinaryPredicate.Operator.LT)
      .put(SqlKind.LESS_THAN_OR_EQUAL, BinaryPredicate.Operator.LE)
      .put(SqlKind.IS_DISTINCT_FROM, BinaryPredicate.Operator.DISTINCT_FROM)
      .put(SqlKind.IS_NOT_DISTINCT_FROM, BinaryPredicate.Operator.NOT_DISTINCT)
      .build();

  /*
   * Returns the Impala Expr object for RexCallConverter.
   */
  public static Expr getExpr(RexCall rexCall, List<Expr> params, RexBuilder rexBuilder,
      Analyzer analyzer) throws ImpalaException {

    // Some functions are known just based on their RexCall signature.
    switch (rexCall.getOperator().getKind()) {
      case OR:
      case AND:
        return createCompoundExpr(rexCall, params);
      case CAST:
        return createCastExpr(rexCall, params, analyzer);
    }

    String funcName = rexCall.getOperator().getName().toLowerCase();

    Function fn = getFunction(rexCall);

    if (fn == null) {
      List<RelDataType> argTypes =
          Lists.transform(rexCall.getOperands(), RexNode::getType);
      Preconditions.checkState(false, "Could not find function \"" + funcName +
        "\" in Impala " + "with args " + argTypes + " and return type " +
        rexCall.getType());
      return null;
    }

    Type impalaRetType = ImpalaTypeConverter.createImpalaType(fn.getReturnType(),
        rexCall.getType().getPrecision(), rexCall.getType().getScale());

    if (rexCall.isA(SqlKind.BINARY_COMPARISON)) {
      return createBinaryCompExpr(fn, params, rexCall.getOperator().getKind(),
          impalaRetType);
    }

    switch (rexCall.getOperator().getKind()) {
      case CASE:
        return createCaseExpr(fn, params, impalaRetType);
      default:
        return new AnalyzedFunctionCallExpr(fn, params, impalaRetType);
    }
  }

  private static Function getFunction(RexCall call) {
    List<RelDataType> argTypes = Lists.transform(call.getOperands(), RexNode::getType);
    String name = call.getOperator().getName();
    return FunctionResolver.getExactFunction(name, call.getKind(), argTypes);
  }

  /**
   * Create a Compound Expr
   */
  private static Expr createCompoundExpr(RexCall rexCall, List<Expr> params) {
    switch (rexCall.getOperator().getKind()) {
      case OR:
        return CompoundPredicate.createDisjunctivePredicate(params);
      case AND:
        return CompoundPredicate.createConjunctivePredicate(params);
    }
    Preconditions.checkState(false, "Unknown type: " + rexCall.getOperator().getKind());
    return null;
  }

  private static Expr createCastExpr(RexCall call, List<Expr> params, Analyzer analyzer)
      throws ImpalaException {
    Type impalaRetType = ImpalaTypeConverter.createImpalaType(call.getType());
    if (params.get(0).getType() == Type.NULL) {
      return new AnalyzedNullLiteral(impalaRetType);
    }

    // no need for redundant cast.
    if (params.get(0).getType().equals(impalaRetType)) {
      return params.get(0);
    }

    // Small hack: Most cast expressions have "isImplicit" set to true. If this
    // is the case, then it blocks "analyze" from working through the cast. We
    // need to analyze the expression before creating the cast around it.
    params.get(0).analyze(analyzer);
    return new AnalyzedCastExpr(impalaRetType, params.get(0));
  }

  private static Expr createCaseExpr(Function fn, List<Expr> params, Type retType) {
    List<CaseWhenClause> caseWhenClauses = new ArrayList<>();
    Expr whenParam = null;
    // params alternate between "when" and the action expr
    for (Expr param : params) {
      if (whenParam == null) {
        whenParam = param;
      } else {
        caseWhenClauses.add(new CaseWhenClause(whenParam, param));
        whenParam = null;
      }
    }
    // Leftover 'when' param is the 'else' param, null if there is no leftover
    return new AnalyzedCaseExpr(fn, caseWhenClauses, whenParam, retType);
  }


  private static Expr createBinaryCompExpr(Function fn, List<Expr> params,
      SqlKind sqlKind, Type retType) {
    Preconditions.checkArgument(params.size() == 2);
    BinaryPredicate.Operator op = BINARY_OP_MAP.get(sqlKind);
    Preconditions.checkNotNull(op, "Unknown Calcite op: " + sqlKind);
    return new AnalyzedBinaryCompExpr(fn, op, params.get(0), params.get(1));
  }
}
