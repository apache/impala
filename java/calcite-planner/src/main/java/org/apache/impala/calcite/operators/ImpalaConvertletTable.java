/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.operators;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.ReflectiveConvertletTable;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.impala.calcite.operators.ImpalaCustomOperatorTable;

import java.util.List;

/**
 * ImpalaConvertletTable adds the ability to override any converlets in the
 * StandardConvertlet table provided by Calcite. The convertlets are executed in
 * the step where Calcite converts the SqlNode tree into a RelNode tree and creating
 * RexNodes from SqlNodes.
 */
public class ImpalaConvertletTable extends ReflectiveConvertletTable {
  public static final ImpalaConvertletTable INSTANCE =
      new ImpalaConvertletTable();

  public ImpalaConvertletTable() {
    addAlias(ImpalaCustomOperatorTable.PERCENT_REMAINDER, SqlStdOperatorTable.MOD);
    registerOp(ImpalaCastFunction.INSTANCE, this::convertExplicitCast);
    registerOp(SqlStdOperatorTable.IS_DISTINCT_FROM, this::convertIsDistinctFrom);
    registerOp(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, this::convertIsNotDistinctFrom);
  }

  @Override
  public SqlRexConvertlet get(SqlCall call) {
    // If we were using Calcite's PERCENT_REMAINDER, the "addAlias" is defined
    // in StandardConvertletTable. But since the PERCENT_REMAINDER is overridden
    // with an Impala version (which derives the Impala return type), we need
    // to handle the alias in our own convertlet.
    if (call.getOperator().equals(ImpalaCustomOperatorTable.PERCENT_REMAINDER)) {
      return super.get(call);
    }

    if (call.getOperator().getKind().equals(SqlKind.IS_DISTINCT_FROM) ||
        call.getOperator().getKind().equals(SqlKind.IS_NOT_DISTINCT_FROM)) {
      return super.get(call);
    }

    // EXPLICIT_CAST convertlet has to be handled by our convertlet. Operation
    // was registered in the constructor and it will call convertExplicitCast
    if (call.getOperator().getName().equals("EXPLICIT_CAST")) {
      return super.get(call);
    }

    return StandardConvertletTable.INSTANCE.get(call);
  }

  protected RexNode convertIsDistinctFrom(
      SqlRexContext cx, SqlCall call) {
    final SqlNode expr1 = call.operand(0);
    final SqlNode expr2 = call.operand(1);
    final RexBuilder rexBuilder = cx.getRexBuilder();
    RelDataType returnType =
        cx.getValidator().getValidatedNodeTypeIfKnown(call);
    List<RexNode> operands = Lists.newArrayList(cx.convertExpression(expr1),
        cx.convertExpression(expr2));
    return rexBuilder.makeCall(returnType, SqlStdOperatorTable.IS_DISTINCT_FROM,
        operands);
  }

  protected RexNode convertIsNotDistinctFrom(
      SqlRexContext cx, SqlCall call) {
    final SqlNode expr1 = call.operand(0);
    final SqlNode expr2 = call.operand(1);
    final RexBuilder rexBuilder = cx.getRexBuilder();
    RelDataType returnType =
        cx.getValidator().getValidatedNodeTypeIfKnown(call);
    List<RexNode> operands = Lists.newArrayList(cx.convertExpression(expr1),
        cx.convertExpression(expr2));
    return rexBuilder.makeCall(returnType, SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
        operands);
  }

  protected RexNode convertExplicitCast(
      SqlRexContext cx, SqlCall call) {
    final SqlNode expr = call.operand(0);
    final RexBuilder rexBuilder = cx.getRexBuilder();
    RelDataType returnType =
        cx.getValidator().getValidatedNodeTypeIfKnown(call);
    List<RexNode> operands = Lists.newArrayList(cx.convertExpression(expr));
    return rexBuilder.makeCall(returnType, ImpalaCastFunction.INSTANCE, operands);
  }
}
