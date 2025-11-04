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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.RangeSets;
import org.apache.calcite.util.Sarg;

import java.util.ArrayList;
import java.util.List;
import static java.util.Objects.requireNonNull;

/**
 * ImpalaRexUtil: class for RexUtil type functions.
 *
 * This class contains the copied "expandSearch" method from Calcite's RexUtil method
 * and all the code that is called from it. As of Calcite 1.40, there is no native
 * IN operator (CALCITE-7232). Instead, Calcite will expandSearch into an inefficient
 * string of ORs. The code inside here expands the search into a custom IN operator
 * when needed.
 */
public class ImpalaRexUtil {

  private static RexNode deref(RexProgram program, RexNode node) {
    while (node instanceof RexLocalRef) {
      node = requireNonNull(program, "program")
          .getExprList().get(((RexLocalRef) node).getIndex());
    }
    return node;
  }

  private static <C extends Comparable<C>> RexNode sargRef(RexBuilder rexBuilder,
      RexNode ref, Sarg<C> sarg, RelDataType type, RexUnknownAs unknownAs) {
    if (sarg.isAll() || sarg.isNone()) {
      return RexUtil.simpleSarg(rexBuilder, ref, sarg, unknownAs);
    }
    final List<RexNode> orList = new ArrayList<>();
    if (sarg.nullAs == RexUnknownAs.TRUE
        && unknownAs == RexUnknownAs.UNKNOWN) {
      orList.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, ref));
    }
    if (sarg.isPoints()) {
      List<RexNode> operands = new ArrayList<>();
      operands.add(ref);
      ///////////////////////
      // IMPALA change: Generate IN operator
      ///////////////////////
      sarg.rangeSet.asRanges().forEach(range ->
          operands.add(rexBuilder.makeLiteral(range.lowerEndpoint(), type, true, true)));
      orList.add(rexBuilder.makeCall(ImpalaInOperator.OP, operands));
    } else if (sarg.isComplementedPoints()) {
      List<RexNode> operands = new ArrayList<>();
      operands.add(ref);
      ///////////////////////
      // IMPALA change: Generate NOT_IN operator
      ///////////////////////
      sarg.rangeSet.complement().asRanges().forEach(range ->
          operands.add(rexBuilder.makeLiteral(range.lowerEndpoint(), type, true, true)));
      orList.add(rexBuilder.makeCall(SqlStdOperatorTable.NOT_IN, operands));
    } else {
      final RangeSets.Consumer<C> consumer =
          new RangeToRex<>(ref, orList, rexBuilder, type);
      RangeSets.forEach(sarg.rangeSet, consumer);
    }
    RexNode node = RexUtil.composeDisjunction(rexBuilder, orList);
    if (sarg.nullAs == RexUnknownAs.FALSE
        && unknownAs == RexUnknownAs.UNKNOWN) {
      node =
          rexBuilder.makeCall(SqlStdOperatorTable.AND,
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, ref),
              node);
    }
    return node;
  }
  /** Shuttle that expands calls to
   * {@link org.apache.calcite.sql.fun.SqlStdOperatorTable#SEARCH}.
   *
   * <p>Calls whose complexity is greater than {@link #maxComplexity}
   * are retained (not expanded). */
  private static class SearchExpandingShuttle extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final RexProgram program;
    private final int maxComplexity;

    SearchExpandingShuttle(RexProgram program, RexBuilder rexBuilder,
        int maxComplexity) {
      this.program = program;
      this.rexBuilder = rexBuilder;
      this.maxComplexity = maxComplexity;
    }

    @Override public RexNode visitCall(RexCall call) {
      final boolean[] update = {false};
      final List<RexNode> clonedOperands;
      switch (call.getKind()) {
        case SEARCH:
          final RexNode ref = call.operands.get(0);
          final RexLiteral literal =
              (RexLiteral) deref(program, call.operands.get(1));
          final Sarg sarg = requireNonNull(literal.getValueAs(Sarg.class), "Sarg");
          if (maxComplexity < 0 || sarg.complexity() < maxComplexity) {
            return sargRef(rexBuilder, ref, sarg, literal.getType(),
                RexUnknownAs.UNKNOWN);
          }
          // Sarg is complex (therefore useful); fall through
        default:
          return super.visitCall(call);
      }
    }
  }

  /** Converts a {@link Range} to a {@link RexNode} expression.
   *
   * @param <C> Value type */
  private static class RangeToRex<C extends Comparable<C>>
      implements RangeSets.Consumer<C> {
    private final List<RexNode> list;
    private final RexBuilder rexBuilder;
    private final RelDataType type;
    private final RexNode ref;

    RangeToRex(RexNode ref, List<RexNode> list, RexBuilder rexBuilder,
        RelDataType type) {
      this.ref = requireNonNull(ref, "ref");
      this.list = requireNonNull(list, "list");
      this.rexBuilder = requireNonNull(rexBuilder, "rexBuilder");
      this.type = requireNonNull(type, "type");
    }

    private void addAnd(RexNode... nodes) {
      list.add(rexBuilder.makeCall(SqlStdOperatorTable.AND, nodes));
    }

    private RexNode op(SqlOperator op, C value) {
      return rexBuilder.makeCall(op, ref,
          rexBuilder.makeLiteral(value, type, true, true));
    }

    @Override public void all() {
      list.add(rexBuilder.makeLiteral(true));
    }

    @Override public void atLeast(C lower) {
      list.add(op(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, lower));
    }

    @Override public void atMost(C upper) {
      list.add(op(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, upper));
    }

    @Override public void greaterThan(C lower) {
      list.add(op(SqlStdOperatorTable.GREATER_THAN, lower));
    }

    @Override public void lessThan(C upper) {
      list.add(op(SqlStdOperatorTable.LESS_THAN, upper));
    }

    @Override public void singleton(C value) {
      list.add(op(SqlStdOperatorTable.EQUALS, value));
    }

    @Override public void closed(C lower, C upper) {
      addAnd(op(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, lower),
          op(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, upper));
    }

    @Override public void closedOpen(C lower, C upper) {
      addAnd(op(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, lower),
          op(SqlStdOperatorTable.LESS_THAN, upper));
    }

    @Override public void openClosed(C lower, C upper) {
      addAnd(op(SqlStdOperatorTable.GREATER_THAN, lower),
          op(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, upper));
    }

    @Override public void open(C lower, C upper) {
      addAnd(op(SqlStdOperatorTable.GREATER_THAN, lower),
          op(SqlStdOperatorTable.LESS_THAN, upper));
    }
  }

  /** Expands calls to {@link SqlStdOperatorTable#SEARCH}
   * whose complexity is greater than {@code maxComplexity} in an expression. */
  public static RexNode expandSearch(RexBuilder rexBuilder, RexNode node) {
    RexShuttle shuttle = new SearchExpandingShuttle(null, rexBuilder, -1);
    return node.accept(shuttle);
  }

  // TODO: IMPALA-14432: remove this function when we upgrade to Calcite 1.40
  // since it is copied from RexBuilder.java
  public static RexNode makeNullable(RexBuilder rexBuilder, RexNode exp,
      boolean nullability) {
    final RelDataType type = exp.getType();
    if (type.isNullable() == nullability) {
      return exp;
    }
    final RelDataType type2 =
        rexBuilder.getTypeFactory().createTypeWithNullability(type, nullability);
    return rexBuilder.makeAbstractCast(type2, exp, false);
  }
}
