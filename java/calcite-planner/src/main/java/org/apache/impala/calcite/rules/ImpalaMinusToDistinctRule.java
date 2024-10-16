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

package org.apache.impala.calcite.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.impala.calcite.operators.ImpalaOperator;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.math.BigDecimal;

/**
 * ImpalaMinusToDistinctRule is the same as Calcite's MinustToDistinctRule with some
 * changes. The Calcite code was grabbed from here:
 * https://github.com/apache/calcite/blob/calcite-1.36.0/core/src/main/java/org/apache/...
 * /calcite/rel/rules/MinusToDistinctRule.java
 *
 * The Calcite code generates a "count() filter" operation which is not supported by
 * Impala. A couple of small changes were made in the creation of the RelNodes which
 * can be found in this file by searching for 'IMPALA CHANGE' in multiple places.
 */

/**
 * Planner rule that translates a distinct
 * {@link org.apache.calcite.rel.core.Minus}
 * (<code>all</code> = <code>false</code>)
 * into a group of operators composed of
 * {@link org.apache.calcite.rel.core.Union},
 * {@link org.apache.calcite.rel.core.Aggregate},
 * {@link org.apache.calcite.rel.core.Filter},etc.
 *
 * <p>For example, the query plan

 * <blockquote><pre>{@code
 *  LogicalMinus(all=[false])
 *    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])
 *      LogicalFilter(condition=[=($7, 10)])
 *        LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 *    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])
 *      LogicalFilter(condition=[=($7, 20)])
 *        LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 * }</pre></blockquote>
 *
 * <p> will convert to
 *
 * <blockquote><pre>{@code
 *  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])
 *    LogicalFilter(condition=[AND(>($3, 0), =($4, 0))])
 *      LogicalAggregate(group=[{0, 1, 2}], agg#0=[COUNT() FILTER $3],
 *                                                      agg#1=[COUNT() FILTER $4])
 *        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], $f3=[=($3, 0)], $f4=[=($3, 1)])
 *          LogicalUnion(all=[true])
 *            LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], $f3=[0])
 *              LogicalFilter(condition=[=($7, 10)])
 *                LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 *            LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], $f3=[1])
 *              LogicalFilter(condition=[=($7, 20)])
 *                LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 * }</pre></blockquote>
 *
 * @see CoreRules#MINUS_TO_DISTINCT
 */
@Value.Enclosing
public class ImpalaMinusToDistinctRule
    extends RelRule<ImpalaMinusToDistinctRule.Config>
    implements TransformationRule {

  protected ImpalaMinusToDistinctRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Minus minus = call.rel(0);

    if (minus.all) {
      // Nothing we can do
      return;
    }

    final RelOptCluster cluster = minus.getCluster();
    final RelBuilder relBuilder = call.builder();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final int branchCount = minus.getInputs().size();

    // For each child branch in minus, add a column which indicates the branch index
    //
    // e.g., select EMPNO from emp -> select EMPNO, 0 from emp
    // 0 indicates that it comes from the first child branch (operand) of the minus
    // operator
    for (int i = 0; i < branchCount; i++) {
      relBuilder.push(minus.getInput(i));
      relBuilder.projectPlus(relBuilder.literal(new BigDecimal(i)));
    }

    // create a union above all the branches
    relBuilder.union(true, branchCount);

    final RelNode union = relBuilder.peek();
    final int originalFieldCnt = union.getRowType().getFieldCount() - 1;

    ImmutableList.Builder<RexNode> projects = ImmutableList.builder();
    // skip the branch index column
    projects.addAll(Util.first(relBuilder.fields(), originalFieldCnt));

    // IMPALA CHANGE 1, commented out below code because Impala does not support
    // COUNT FILTER operation
    // On top of the Union, add a Project and add one boolean column per branch counter,
    // where the i-th boolean column is true iff the tuple comes from the i-th branch
    //
    // e.g., LogicalProject(EMPNO=[$0], $f1=[=($1, 0)], $f2=[=($1, 1)], $f3=[=($1, 2)])
    // $f1,$f2,$f3 are the boolean indicate whether it comes from the corresponding branch
    //for (int i = 0; i < branchCount; i++) {
    //  projects.add(
    //      relBuilder.equals(relBuilder.field(originalFieldCnt),
    //          relBuilder.literal(new BigDecimal(i))));
    //}
    // IMPALA CHANGE 2, instead, added this code:
    // The count filter operation is not supported. Using the above example, the code
    // we create for Impala is:
    // e.g., LogicalProject(EMPNO=[$0], $f1=[if(=($1, 0),1,null)],
    //                                  $f2=[if(=($1, 1),1,null)],
    //                                  $f3=[if(=($1, 2),1,null)])
    // A '1' is created for counting purposes only if it comes from the branch, and a null
    // otherwise.  Since the count() operation will not count null values for the field,
    // we get the count for each branch for each new column
    for (int i = 0; i < branchCount; i++) {
      RexNode equalsNode = relBuilder.equals(
          relBuilder.field(originalFieldCnt), relBuilder.literal(new BigDecimal(i)));
      RexNode one = relBuilder.literal(new BigDecimal(1));
      RexNode ifOp = rexBuilder.makeCall(new ImpalaOperator("if"),
          ImmutableList.of(equalsNode, one, rexBuilder.makeNullLiteral(one.getType())));
      projects.add(ifOp);
    }

    relBuilder.project(projects.build());

    // IMPALA CHANGE 3: commented out below code because Impala does not support
    // COUNT FILTER operation
    // Add the count(*) filter $f1(..) for each branch
    //ImmutableList.Builder<RelBuilder.AggCall> aggCalls = ImmutableList.builder();
    //for (int i = 0; i < branchCount; i++) {
    //  aggCalls.add(relBuilder.countStar(null).filter(
    //      relBuilder.field(originalFieldCnt + i)));
    //}
    // IMPALA CHANGE 4: Instead, added just a plain count. This combined with the above
    // logic should provide the same count as the count filter operation.
    ImmutableList.Builder<RelBuilder.AggCall> aggCalls = ImmutableList.builder();
    for (int i = 0; i < branchCount; i++) {
      aggCalls.add(relBuilder.count(relBuilder.field(originalFieldCnt + i)));
    }

    final ImmutableBitSet groupSet = ImmutableBitSet.range(originalFieldCnt);
    relBuilder.aggregate(relBuilder.groupKey(groupSet), aggCalls.build());

    ImmutableList.Builder<RexNode> filters = ImmutableList.builder();
    for (int i = 0; i < branchCount; i++) {
      SqlOperator operator =
          i == 0 ? SqlStdOperatorTable.GREATER_THAN
              : SqlStdOperatorTable.EQUALS;
      filters.add(
          rexBuilder.makeCall(operator, relBuilder.field(originalFieldCnt + i),
          relBuilder.literal(new BigDecimal(0))));
    }

    relBuilder.filter(filters.build());
    relBuilder.project(Util.first(relBuilder.fields(), originalFieldCnt));
    call.transformTo(relBuilder.build());
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableImpalaMinusToDistinctRule.Config.builder()
        .operandSupplier(b -> b.operand(LogicalMinus.class).anyInputs()).build();

    @Override default ImpalaMinusToDistinctRule toRule() {
      return new ImpalaMinusToDistinctRule(this);
    }
  }
}
