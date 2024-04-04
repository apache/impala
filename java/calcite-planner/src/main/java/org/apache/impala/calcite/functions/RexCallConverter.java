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
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  public static Expr getExpr(RexCall rexCall, List<Expr> params, RexBuilder rexBuilder) {

    String funcName = rexCall.getOperator().getName().toLowerCase();

    Function fn = getFunction(rexCall);

    Type impalaRetType = ImpalaTypeConverter.createImpalaType(fn.getReturnType(),
        rexCall.getType().getPrecision(), rexCall.getType().getScale());

    if (rexCall.isA(SqlKind.BINARY_COMPARISON)) {
      return createBinaryCompExpr(fn, params, rexCall.getOperator().getKind(),
          impalaRetType);
    }

    return new AnalyzedFunctionCallExpr(fn, params, impalaRetType);
  }

  private static Function getFunction(RexCall call) {
    List<RelDataType> argTypes = Lists.transform(call.getOperands(), RexNode::getType);
    String name = call.getOperator().getName();
    Function fn = FunctionResolver.getFunction(name, call.getKind(), argTypes);
    Preconditions.checkNotNull(fn, "Could not find function \"" + name + "\" in Impala "
          + "with args " + argTypes + " and return type " + call.getType());
    return fn;
  }

  private static Expr createBinaryCompExpr(Function fn, List<Expr> params,
      SqlKind sqlKind, Type retType) {
    Preconditions.checkArgument(params.size() == 2);
    BinaryPredicate.Operator op = BINARY_OP_MAP.get(sqlKind);
    Preconditions.checkNotNull(op, "Unknown Calcite op: " + sqlKind);
    return new AnalyzedBinaryCompExpr(fn, op, params.get(0), params.get(1));
  }
}
