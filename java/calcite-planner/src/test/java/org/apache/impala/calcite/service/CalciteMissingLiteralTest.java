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

import com.google.common.collect.ImmutableList;

import org.apache.impala.analysis.Expr;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.calcite.rel.node.NodeWithExprs;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.NoOpEventSequence;
import org.junit.Test;


/**
 * The literals RexLiteralConverter had no case for, planned by the Calcite planner.
 *
 * <p>Impala's parser has no {@code X'...'} literal, so this form reaches only the
 * Calcite planner. Its type is mapped in both directions by
 * {@code ImpalaTypeConverter}, and {@code LiteralExpr} has held binary values since
 * BINARY was added; what was missing was the case in {@code RexLiteralConverter} that
 * turns the Calcite literal into an Impala one, so a plan carrying one died at
 * "Unsupported RexLiteral: BINARY" after the schema had travelled intact.
 *
 * <p>The control is the same value written as a cast: it plans either way, because the
 * cast is a call over a string literal rather than a binary literal of its own.
 */
public class CalciteMissingLiteralTest extends FrontendTestBase {

  @Test
  public void aBinaryLiteralPlans() throws ImpalaException {
    assertEquals("[BINARY]", outputTypes("select x'616263' as b"));
  }

  @Test
  public void aBinaryLiteralInAValuesRowPlans() throws ImpalaException {
    assertEquals("[BINARY]", outputTypes("values (x'616263')"));
  }

  /** Bytes that are not valid UTF-8 are held as bytes, not as a String. */
  @Test
  public void bytesThatAreNotTextPlan() throws ImpalaException {
    assertEquals("[BINARY]", outputTypes("select x'ff00fe' as b"));
  }

  /**
   * A REAL literal is Impala's DOUBLE, and the original planner accepts the SQL that
   * produces one, so the Calcite planner refusing it was a difference between the two.
   */
  @Test
  public void aRealLiteralPlans() throws ImpalaException {
    assertEquals("[DOUBLE]", outputTypes("select cast(1.5 as real) as r"));
    assertEquals("[DOUBLE]", outputTypes("values (cast(1.5 as real))"));
  }

  /** The control: the same value as a cast has always planned. */
  @Test
  public void theSameValueAsACastPlans() throws ImpalaException {
    assertEquals("[BINARY]", outputTypes("select cast('abc' as binary) as b"));
  }

  /** The Impala types of the physical plan's output expressions. */
  private String outputTypes(String sql) throws ImpalaException {
    CalciteAnalysisResult analysisResult = (CalciteAnalysisResult) parseAndAnalyze(
        sql, feFixture_.createAnalysisCtx(), new CalciteCompilerFactory());
    CalciteRelNodeConverter relNodeConverter =
        new CalciteRelNodeConverter(analysisResult);
    ImpalaPlanRel optimized = new CalciteOptimizer(analysisResult,
        NoOpEventSequence.INSTANCE, new TQueryOptions())
        .optimize(relNodeConverter.convert(analysisResult.getValidatedNode()));
    PlannerContext plannerContext = new PlannerContext(analysisResult.getAnalyzer(),
        analysisResult.getAnalyzer().getQueryCtx(), NoOpEventSequence.INSTANCE);
    NodeWithExprs plan = new CalcitePhysPlanCreator(analysisResult.getAnalyzer(),
        plannerContext).create(optimized);
    ImmutableList.Builder<String> types = ImmutableList.builder();
    for (Expr output : plan.outputExprs_) types.add(output.getType().toSql());
    return types.build().toString();
  }
}
