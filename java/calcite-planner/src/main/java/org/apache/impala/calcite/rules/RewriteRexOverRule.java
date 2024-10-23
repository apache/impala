/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.rules;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.impala.calcite.operators.ImpalaAggOperator;
import org.apache.impala.calcite.operators.ImpalaCustomOperatorTable;
import org.apache.impala.calcite.operators.ImpalaOperatorTable;
import org.apache.impala.calcite.operators.ImpalaOperator;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * Rule to replace unsupported RexOver with supported syntax. For instance,
 * the percent_rank function isn't supported directly, but it can be calculated
 * through the rank() and count() functions which are supported.
 */
public class RewriteRexOverRule extends RelOptRule {
  private final ProjectFactory projectFactory;

  public static final RewriteRexOverRule INSTANCE =
      new RewriteRexOverRule();

  private RewriteRexOverRule() {
    super(operand(Project.class, any()));
    this.projectFactory = RelFactories.DEFAULT_PROJECT_FACTORY;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final RelNode input = project.getInput();
    final RexBuilder rexBuilder = project.getCluster().getRexBuilder();

    RexOverReplacer replacer = new RexOverReplacer(rexBuilder);

    List<RexNode> exprs = new ArrayList<>();
    for (RexNode r : project.getProjects()) {
      exprs.add(replacer.apply(r));
    }

    if (!replacer.replacedValue) {
      return;
    }

    RelNode newProject = projectFactory.createProject(
        input, Collections.emptyList(), exprs, project.getRowType().getFieldNames());

    call.transformTo(newProject);
  }

  private static class RexOverReplacer extends RexShuttle {
    public boolean replacedValue;
    private final RexBuilder rexBuilder;

    public RexOverReplacer(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitOver(RexOver over) {
      if (over.getOperator().getName().toLowerCase().equals("percent_rank")) {
        replacedValue = true;
        return replacePercentRank(over);
      }
      if (over.getOperator().getName().toLowerCase().equals("cume_dist")) {
        replacedValue = true;
        return replaceCumeDist(over);
      }
      if (over.getOperator().getName().toLowerCase().equals("ntile")) {
        replacedValue = true;
        return replaceNTile(over);
      }
      return super.visitOver(over);
    }

    /**
     * Rewrite cume_dist() to the following:
     *
     * cume_dist() over([partition by clause] order by clause)
     *    = ((Count - Rank) + 1)/Count
     * where,
     *  Rank = rank() over([partition by clause] order by clause DESC)
     *  Count = count() over([partition by clause])
     */
    public RexNode replaceCumeDist(RexOver over) {
      ImmutableList<RexFieldCollation> reversedCollation =
          reverseCollation(over.getWindow().orderKeys);
      RelDataType bigintType =
          rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

      // create RexNode for  rank() over([partition by clause] order by clause DESC)
      RexNode rankNode = rexBuilder.makeOver(bigintType, SqlStdOperatorTable.RANK,
          over.getOperands(), over.getWindow().partitionKeys, reversedCollation,
          over.getWindow().getLowerBound(), over.getWindow().getUpperBound(),
          over.getWindow().isRows(), true, false, over.isDistinct(), over.ignoreNulls());

      // create RexNode for count() over([partition by clause])
      RexNode countNode = rexBuilder.makeOver(bigintType,
          ImpalaCustomOperatorTable.COUNT, over.getOperands(),
          over.getWindow().partitionKeys, ImmutableList.of(),
          over.getWindow().getLowerBound(), over.getWindow().getUpperBound(),
          over.getWindow().isRows(), true, false, over.isDistinct(), over.ignoreNulls());

      RexNode one = rexBuilder.makeCast(bigintType, rexBuilder.makeExactLiteral(
          BigDecimal.valueOf(1)));
      RexNode countMinusRank = rexBuilder.makeCall(ImpalaCustomOperatorTable.MINUS,
          countNode, rankNode);

      // create RexNode for((Count - Rank) + 1)
      RexNode countMinusRankPlusOne = rexBuilder.makeCall(ImpalaCustomOperatorTable.PLUS,
          countMinusRank, one);
      RexNode numeratorDouble = rexBuilder.makeCast(over.type, countMinusRankPlusOne);

      RexNode denominatorDouble = rexBuilder.makeCast(over.type, countNode);
      return rexBuilder.makeCall(ImpalaCustomOperatorTable.DIVIDE, numeratorDouble,
          denominatorDouble);
    }

    /**
     * Rewrite percent_rank() to the following:
     *
     * percent_rank() over([partition by clause] order by clause)
     *    = case
     *        when Count = 1 then 0
     *        else (Rank - 1)/(Count - 1)
     * where,
     *  Rank = rank() over([partition by clause] order by clause)
     *  Count = count() over([partition by clause])
     */
    public RexNode replacePercentRank(RexOver over) {
      RelDataType bigintType =
          rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

      // create RexNode for rank() over ([partition by clause] order by clause)
      RexNode rankNode = rexBuilder.makeOver(bigintType, SqlStdOperatorTable.RANK,
          over.getOperands(), over.getWindow().partitionKeys, over.getWindow().orderKeys,
          over.getWindow().getLowerBound(), over.getWindow().getUpperBound(),
          over.getWindow().isRows(), true, false, over.isDistinct(), over.ignoreNulls());

      // create RexNode for literal "1"
      RexNode one = rexBuilder.makeCast(bigintType, rexBuilder.makeExactLiteral(
          BigDecimal.valueOf(1)));

      // create RexNode for (Rank - 1)
      RexNode rankNodeMinusOne = rexBuilder.makeCall(ImpalaCustomOperatorTable.MINUS,
          rankNode, one);
      RexNode rankDouble = rexBuilder.makeCast(over.type, rankNodeMinusOne);

      // create RexNode for count() over([partition by clause])
      RexNode countNode = rexBuilder.makeOver(bigintType, ImpalaCustomOperatorTable.COUNT,
          over.getOperands(), over.getWindow().partitionKeys, ImmutableList.of(),
          over.getWindow().getLowerBound(), over.getWindow().getUpperBound(),
          over.getWindow().isRows(), true, false, over.isDistinct(), over.ignoreNulls());

      // create RexNode for (Count - 1)
      RexNode countNodeMinusOne = rexBuilder.makeCall(ImpalaCustomOperatorTable.MINUS,
          countNode, one);
      RexNode countDouble = rexBuilder.makeCast(over.type, countNodeMinusOne);

      // create else RexNode for (Rank - 1)/(Count - 1)
      RexNode rankDivCount = rexBuilder.makeCall(ImpalaCustomOperatorTable.DIVIDE,
          rankDouble, countDouble);

      // create when clause for case statement: Count = 1
      RexNode caseCountOne = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, countNode,
          one);
      // create then clause for case statement: 0
      RexNode zeroDouble = rexBuilder.makeCast(over.type, rexBuilder.makeExactLiteral(
          BigDecimal.valueOf(0)));
      return rexBuilder.makeCall(SqlStdOperatorTable.CASE, caseCountOne, zeroDouble,
          rankDivCount);
    }

    /**
     * Rewrite ntile() to the following:
     *
     * ntile(B) over([partition by clause] order by clause)
     *    = floor(min(Count, B) * (RowNumber - 1)/Count) + 1
     * where,
     *  RowNumber = row_number() over([partition by clause] order by clause)
     *  Count = count() over([partition by clause])
     */
    public RexNode replaceNTile(RexOver over) {
      RelDataType bigintType =
          rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

      // create RowNumber: row_number() over([partition by clause] order by clause)
      RexNode rowNumberNode = rexBuilder.makeOver(bigintType,
          SqlStdOperatorTable.ROW_NUMBER,
          new ArrayList<>(), over.getWindow().partitionKeys, over.getWindow().orderKeys,
          over.getWindow().getLowerBound(), over.getWindow().getUpperBound(),
          over.getWindow().isRows(), true, false, over.isDistinct(), over.ignoreNulls());

      ImmutableList<RexFieldCollation> orderKeys = ImmutableList.of();

      // create Count: count() over([partition by clause])
      RexNode countNode = rexBuilder.makeOver(bigintType, ImpalaCustomOperatorTable.COUNT,
          new ArrayList<>(), over.getWindow().partitionKeys, orderKeys,
          over.getWindow().getLowerBound(), over.getWindow().getUpperBound(),
          over.getWindow().isRows(), true, false, over.isDistinct(), over.ignoreNulls());

      RexNode one = rexBuilder.makeCast(bigintType, rexBuilder.makeExactLiteral(
          BigDecimal.valueOf(1)));
      RexNode zero = rexBuilder.makeCast(bigintType, rexBuilder.makeExactLiteral(
          BigDecimal.valueOf(0)));

      // create min: min(Count, B)
      RexNode minCountNTile = rexBuilder.makeCall(new ImpalaAggOperator("least"),
          countNode, over.getOperands().get(0));

      // create RowNumber - 1
      RexNode rowNumberMinusOne = rexBuilder.makeCall(ImpalaCustomOperatorTable.MINUS,
          rowNumberNode, one);

      // create min(Count, B) * (RowNumber - 1)
      RexNode numeratorInt = rexBuilder.makeCall(ImpalaCustomOperatorTable.MULTIPLY,
          minCountNTile, rowNumberMinusOne);

      // create floor(min(Count, B) * (RowNumber - 1)/Count)
      RexNode NTileMinusOne  = rexBuilder.makeCall(new ImpalaOperator("int_divide"),
          numeratorInt, countNode);

      // create floor(min(Count, B) * (RowNumber - 1)/Count) + 1
      return rexBuilder.makeCall(over.getType(), ImpalaCustomOperatorTable.PLUS,
          ImmutableList.of(NTileMinusOne, one));
    }

    private static ImmutableList<RexFieldCollation> reverseCollation(
        List<RexFieldCollation> collationList) {
      ImmutableList.Builder<RexFieldCollation> builder = ImmutableList.builder();
      for (RexFieldCollation collation : collationList) {
        ImmutableSet.Builder<SqlKind> directionBuilder = ImmutableSet.builder();
        switch (collation.getDirection()) {
          case ASCENDING:
          case STRICTLY_ASCENDING:
            directionBuilder.add(SqlKind.DESCENDING);
            break;
        }
        if (collation.getNullDirection() == RelFieldCollation.NullDirection.FIRST) {
          directionBuilder.add(SqlKind.NULLS_LAST);
        }
        builder.add(new RexFieldCollation(collation.getKey(), directionBuilder.build()));
      }
      return builder.build();
    }
  }
}
