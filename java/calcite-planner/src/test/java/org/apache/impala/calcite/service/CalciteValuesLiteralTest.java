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

package org.apache.impala.calcite.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.impala.analysis.Expr;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.calcite.rel.node.NodeWithExprs;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.UnionNode;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.NoOpEventSequence;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * The literals a VALUES row carries, from Calcite's tuple to Impala's plan node.
 *
 * <p>A TIMESTAMP is the case that used to fail. ImpalaValuesRel converts each literal
 * twice: RexLiteralConverter folds a cast from the literal's text and gets a
 * TimestampLiteral, and then the result was re-created from its own text through
 * LiteralExpr.createFromStr to give it the column's declared type. That entry point
 * refuses TIMESTAMP, so a correctly built value was thrown away and the query failed
 * with "Literal unsupported: TIMESTAMP".
 *
 * <p>The re-creation is there to give the literal the column's declared type, and it
 * stays in place for the case where the two differ. Skipping it when they already
 * agree cannot change the result: both paths return a literal of the declared type,
 * and the text a literal is re-created from is the text it was created with.
 */
public class CalciteValuesLiteralTest extends FrontendTestBase {

  @Test
  public void aTimestampLiteralInAValuesRowPlans() throws ImpalaException {
    // Three fractional digits: nothing here depends on how wide the value is.
    assertEquals("[TIMESTAMP '1970-01-01 00:02:03.456000000']",
        constantRow("values (timestamp '1970-01-01 00:02:03.456')"));
  }

  /** Impala keeps nanoseconds, and a literal that carries them arrives intact. */
  @Test
  public void aNanosecondTimestampLiteralKeepsItsDigits() throws ImpalaException {
    assertEquals("[TIMESTAMP '1970-01-01 00:00:01.234567891']",
        constantRow("values (timestamp '1970-01-01 00:00:01.234567891')"));
  }

  /**
   * Rows of different width keep their own literals.
   *
   * <p>Measured rather than assumed: these two rows do not merge into one Values, so
   * each keeps the type its own literal was parsed at and the union node above them is
   * what spans the two. Nothing is widened here, so the case is pinned by what it
   * produces rather than by a type both rows were expected to share.
   */
  @Test
  public void rowsOfDifferentWidthKeepTheirOwnLiterals() throws ImpalaException {
    List<Expr> literals =
        constantExprs(planNodeFor("values (1), (cast(40000 as int))"));
    // Sorted: which operand of the union comes first is not what this pins.
    assertEquals("[1, 40000]", sorted(sqlOf(literals)));
    assertEquals("[INT, TINYINT]", sorted(typesOf(literals)));
  }

  /** The control on the shape: a cast stays above the row and never becomes a tuple. */
  @Test
  public void aCastStaysAboveTheRow() throws ImpalaException {
    RelNode plan = optimize("values (cast('1970-01-01 00:02:03.456' as timestamp))");
    assertTrue("expected the cast to stay in a projection, got " + plan.getRowType(),
        !(plan instanceof Values));
  }

  /** The control on the type: DATE has always had an entry point and still works. */
  @Test
  public void aDateLiteralInAValuesRowPlans() throws ImpalaException {
    assertEquals("[DATE '1970-01-01']", constantRow("values (date '1970-01-01')"));
  }

  /** The literals the physical plan holds for a single-row VALUES, as SQL. */
  private String constantRow(String sql) throws ImpalaException {
    return sqlOf(constantExprs(planNodeFor(sql)));
  }

  private static String sqlOf(List<Expr> literals) {
    ImmutableList.Builder<String> text = ImmutableList.builder();
    for (Expr literal : literals) text.add(literal.toSql());
    return text.build().toString();
  }

  private static String sorted(String bracketedList) {
    String[] parts = bracketedList.substring(1, bracketedList.length() - 1).split(", ");
    Arrays.sort(parts);
    return "[" + String.join(", ", parts) + "]";
  }

  private static String typesOf(List<Expr> literals) {
    ImmutableList.Builder<String> text = ImmutableList.builder();
    for (Expr literal : literals) text.add(literal.getType().toSql());
    return text.build().toString();
  }

  /** The optimized Calcite plan, before it becomes plan nodes. */
  private RelNode optimize(String sql) throws ImpalaException {
    CalciteAnalysisResult analysisResult = analyze(sql);
    CalciteRelNodeConverter relNodeConverter =
        new CalciteRelNodeConverter(analysisResult);
    return new CalciteOptimizer(analysisResult, NoOpEventSequence.INSTANCE,
        new TQueryOptions())
        .optimize(relNodeConverter.convert(analysisResult.getValidatedNode()));
  }

  /** The single-node physical plan for one query. */
  private NodeWithExprs planNodeFor(String sql) throws ImpalaException {
    CalciteAnalysisResult analysisResult = analyze(sql);
    CalciteRelNodeConverter relNodeConverter =
        new CalciteRelNodeConverter(analysisResult);
    ImpalaPlanRel optimized = new CalciteOptimizer(analysisResult,
        NoOpEventSequence.INSTANCE, new TQueryOptions())
        .optimize(relNodeConverter.convert(analysisResult.getValidatedNode()));
    PlannerContext plannerContext = new PlannerContext(analysisResult.getAnalyzer(),
        analysisResult.getAnalyzer().getQueryCtx(), NoOpEventSequence.INSTANCE);
    return new CalcitePhysPlanCreator(analysisResult.getAnalyzer(), plannerContext)
        .create(optimized);
  }

  private CalciteAnalysisResult analyze(String sql) throws ImpalaException {
    return (CalciteAnalysisResult) parseAndAnalyze(sql,
        feFixture_.createAnalysisCtx(), new CalciteCompilerFactory());
  }

  /** Every constant row a plan's union nodes carry, flattened. */
  private static List<Expr> constantExprs(NodeWithExprs plan) {
    ImmutableList.Builder<Expr> literals = ImmutableList.builder();
    collectConstants(plan.planNode_, literals);
    return literals.build();
  }

  private static void collectConstants(PlanNode node,
      ImmutableList.Builder<Expr> literals) {
    if (node instanceof UnionNode) {
      for (List<Expr> row : ((UnionNode) node).getConstExprLists()) {
        literals.addAll(row);
      }
    }
    for (PlanNode child : node.getChildren()) collectConstants(child, literals);
  }
}
