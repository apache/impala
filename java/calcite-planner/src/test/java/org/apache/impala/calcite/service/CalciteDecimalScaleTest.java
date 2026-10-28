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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.SingleNodePlannerIntf;
import org.apache.impala.planner.UnionNode;
import org.apache.impala.service.CompilerFactory;
import org.apache.impala.service.CompilerFactoryImpl;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.util.EventSequence;
import org.junit.Test;

/** Numeric scale functions must preserve the native decimal result type rules. */
public class CalciteDecimalScaleTest extends FrontendTestBase {
  private static final String[] FUNCTIONS =
      {"trunc", "truncate", "dtrunc", "round", "dround"};

  private TResultSetMetadata metadata(String sql, boolean calcite) throws Exception {
    CompilerFactory factory = calcite
        ? new CalciteCompilerFactory() : new CompilerFactoryImpl();
    AnalysisContext ctx = feFixture_.createAnalysisCtx();
    ctx.getQueryCtx().client_request.setStmt(sql);
    AnalysisResult result = parseAndAnalyze(sql, ctx, factory);
    SingleNodePlannerIntf planner = factory.createSingleNodePlanner(new PlannerContext(
        result, ctx.getQueryCtx(), new EventSequence("Decimal scale parity")));
    planner.createSingleNodePlan();
    return planner.getTResultSetMetadata(result.getParsedStmt());
  }

  private void assertParity(String expression) throws Exception {
    String sql = "select " + expression + " as value";
    assertEquals(sql, metadata(sql, false), metadata(sql, true));
  }

  private void assertRoundResult(String expression, int precision, String expectedValue,
      boolean decimalV2) throws Exception {
    String sql = "select " + expression + " as value";
    for (boolean calcite : new boolean[] {false, true}) {
      // Calcite supports only DECIMAL V2; exercise V1 through the Original planner.
      if (calcite && !decimalV2) continue;
      CompilerFactory factory = calcite
          ? new CalciteCompilerFactory() : new CompilerFactoryImpl();
      AnalysisContext ctx = feFixture_.createAnalysisCtx();
      ctx.getQueryCtx().client_request.query_options.setDecimal_v2(decimalV2);
      ctx.getQueryCtx().client_request.query_options.setEnable_expr_rewrites(false);
      ctx.getQueryCtx().client_request.setStmt(sql);
      AnalysisResult result = parseAndAnalyze(sql, ctx, factory);
      SingleNodePlannerIntf planner = factory.createSingleNodePlanner(new PlannerContext(
          result, ctx.getQueryCtx(), new EventSequence("Decimal rounding carry")));
      PlanNode root = planner.createSingleNodePlan();
      assertEquals(sql, metadata("select cast(0 as decimal(" + precision
          + ",0)) as value", false),
          planner.getTResultSetMetadata(result.getParsedStmt()));
      List<Expr> expressions = new ArrayList<>();
      collectConstants(root, expressions);
      assertEquals(sql, 1, expressions.size());
      LiteralExpr value = LiteralExpr.create(expressions.get(0), ctx.getQueryCtx());
      assertTrue(sql, value instanceof NumericLiteral);
      assertEquals(sql, 0, new BigDecimal(expectedValue).compareTo(
          ((NumericLiteral) value).getValue()));
    }
  }

  private static void collectConstants(PlanNode node, List<Expr> expressions) {
    if (node instanceof UnionNode) {
      for (List<Expr> row : ((UnionNode) node).getConstExprLists()) {
        expressions.addAll(row);
      }
    }
    for (PlanNode child : node.getChildren()) collectConstants(child, expressions);
  }

  @Test
  public void testIntegerRoundingCarry() throws Exception {
    for (boolean decimalV2 : new boolean[] {false, true}) {
      for (String name : new String[] {"round", "dround"}) {
        for (int precision : new int[] {4, 9, 18, 37}) {
          String input = "9".repeat(precision - 1) + "5";
          String expected = "1" + "0".repeat(precision);
          for (String sign : new String[] {"", "-"}) {
            assertRoundResult(name + "(cast(" + sign + input + " as decimal("
                + precision + ",0)),-1)", precision + 1, sign + expected, decimalV2);
          }
        }
        // The carry digit must not produce the unsupported DECIMAL(39,0).
        assertRoundResult(name + "(cast(12345 as decimal(38,0)),-1)",
            38, "12350", decimalV2);
        // Scale 0 and the default scale leave integer decimals unchanged.
        assertRoundResult(name + "(cast(9995 as decimal(4,0)),0)",
            4, "9995", decimalV2);
        assertRoundResult(name + "(cast(9995 as decimal(4,0)))",
            4, "9995", decimalV2);
      }
    }
  }

  @Test
  public void testReducedScale() throws Exception {
    for (String name : FUNCTIONS) {
      for (int scale : new int[] {1, 2}) {
        assertParity(name + "(cast(1.234 as decimal(15,3))," + scale + ")");
      }
    }
  }

  @Test
  public void testEqualAndHigherScale() throws Exception {
    for (String name : FUNCTIONS) {
      for (int scale : new int[] {3, 5, 38, 39}) {
        assertParity(name + "(cast(1.234 as decimal(15,3))," + scale + ")");
      }
    }
  }

  @Test
  public void testDefaultAndNegativeScale() throws Exception {
    for (String name : FUNCTIONS) {
      assertParity(name + "(cast(1.234 as decimal(15,3)))");
      for (int scale : new int[] {0, -1, -10, -38, -39}) {
        assertParity(name + "(cast(1.234 as decimal(15,3))," + scale + ")");
      }
    }
  }

  @Test
  public void testPrecisionBoundaries() throws Exception {
    for (String name : FUNCTIONS) {
      for (String type : new String[] {
          "decimal(1,0)", "decimal(3,2)", "decimal(38,0)",
          "decimal(38,3)"}) {
        for (int scale : new int[] {0, 1, 2, 3, 37, 38, 39, -1}) {
          assertParity(name + "(cast(0.999 as " + type + ")," + scale + ")");
        }
      }
    }
  }

  @Test
  public void testFractionalOnlyInput() throws Exception {
    for (String name : FUNCTIONS) {
      for (int scale : new int[] {1, 2, 37, 38, 39}) {
        assertParity(name + "(cast(0.999 as decimal(38,38))," + scale + ")");
      }
      for (int scale : new int[] {0, -1}) {
        String sql = "select " + name
            + "(cast(0.999 as decimal(38,38))," + scale + ") as value";
        // Native truncation reports DECIMAL(0,0), which Calcite cannot represent.
        // Removing all fractional digits requires the valid DECIMAL(1,0) type.
        assertEquals(sql, metadata("select cast(0 as decimal(1,0)) as value", false),
            metadata(sql, true));
      }
    }
  }

  @Test
  public void testNullDecimals() throws Exception {
    for (String name : FUNCTIONS) {
      assertParity(name + "(cast(null as decimal(15,3)),2)");
      assertParity(name + "(cast(null as decimal(15,3)),-1)");
    }
  }

  @Test
  public void testColumnsAndNestedExpressions() throws Exception {
    addTestDb("decimal_scale_test", "Synthetic decimal scale metadata");
    addTestTable("create table decimal_scale_test.t (d decimal(15,3)) "
        + "stored as parquet location '/decimal-scale-test/t'");
    for (String name : FUNCTIONS) {
      String sql = "select " + name + "(d,2) as value from decimal_scale_test.t";
      assertEquals(sql, metadata(sql, false), metadata(sql, true));
      assertParity(name + "(cast(1.234 as decimal(15,3)),2) + "
          + "cast(1.00 as decimal(5,2))");
      assertParity(name + "(" + name + "(cast(1.234 as decimal(15,3)),2),1)");
    }
  }

  @Test
  public void testFloatingPointControls() throws Exception {
    for (String name : FUNCTIONS) {
      for (String type : new String[] {"float", "double"}) {
        assertParity(name + "(cast(1.234 as " + type + "))");
        if (name.equals("round") || name.equals("dround")) {
          assertParity(name + "(cast(1.234 as " + type + "),2)");
        }
      }
    }
  }
}
