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

package org.apache.impala.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FrontendTestBase;
import org.junit.Test;

/**
 * Test literal expressions from the Java side. Note that a comprehensive
 * set of tests exist on the C++ site in expr-test.cc. Those tests call
 * into Java, then verify the results. Very complete, but hard to debug
 * from the Java side.
 */
public class LiteralExprTest extends FrontendTestBase {
  // Test creation of LiteralExprs from Strings, e.g., for partitioning keys.
  @Test
  public void TestLiteralExpr() {
    testLiteralExprPositive("false", Type.BOOLEAN);
    testLiteralExprPositive("1", Type.TINYINT);
    testLiteralExprPositive("1", Type.SMALLINT);
    testLiteralExprPositive("1", Type.INT);
    testLiteralExprPositive("1", Type.BIGINT);
    testLiteralExprPositive("1.0", Type.FLOAT);
    testLiteralExprPositive("1.0", Type.DOUBLE);
    testLiteralExprPositive("ABC", Type.STRING);
    testLiteralExprPositive("ABC", Type.BINARY);
    testLiteralExprPositive("1.1", ScalarType.createDecimalType(2, 1));
    testLiteralExprPositive("2001-02-28", Type.DATE);

    // INVALID_TYPE should always fail
    testLiteralExprNegative("ABC", Type.INVALID);

    // Invalid casts
    testLiteralExprNegative("ABC", Type.BOOLEAN);
    testLiteralExprNegative("ABC", Type.TINYINT);
    testLiteralExprNegative("ABC", Type.SMALLINT);
    testLiteralExprNegative("ABC", Type.INT);
    testLiteralExprNegative("ABC", Type.BIGINT);
    testLiteralExprNegative("ABC", Type.FLOAT);
    testLiteralExprNegative("ABC", Type.DOUBLE);
    testLiteralExprNegative("ABC", Type.TIMESTAMP);
    testLiteralExprNegative("ABC", ScalarType.createDecimalType());
    testLiteralExprNegative("ABC", Type.DATE);
    // Invalid date test
    testLiteralExprNegative("2001-02-31", Type.DATE);

    // DATETIME/TIMESTAMP types not implemented
    testLiteralExprNegative("2010-01-01", Type.DATETIME);
    testLiteralExprNegative("2010-01-01", Type.TIMESTAMP);
  }

  private void testLiteralExprPositive(String value, Type type) {
    LiteralExpr expr = null;
    try {
      expr = LiteralExpr.createFromUnescapedStr(value, type);
    } catch (Exception e) {
      fail("\nFailed to create LiteralExpr of type: " + type.toString() +
          " from: " + value + " due to " + e.getMessage() + "\n");
    }
    if (expr == null) {
      fail("\nFailed to create LiteralExpr\n");
    }
  }

  private void testLiteralExprNegative(String value, Type type) {
    boolean failure = false;
    LiteralExpr expr = null;
    try {
      expr = LiteralExpr.createFromUnescapedStr(value, type);
    } catch (Exception e) {
      failure = true;
    }
    if (expr == null) {
      failure = true;
    }
    if (!failure) {
      fail("\nUnexpectedly succeeded to create LiteralExpr of type: "
          + type.toString() + " from: " + value + "\n");
    }
  }

  private Expr analyze(String query, boolean useDecimalV2, boolean enableRewrite) {
    AnalysisContext ctx = createAnalysisCtx();
    ctx.getQueryOptions().setDecimal_v2(useDecimalV2);
    ctx.getQueryOptions().setEnable_expr_rewrites(enableRewrite);
    return ((SelectStmt) AnalyzesOk(query, ctx)).getSelectList()
        .getItems().get(0).getExpr();
  }

  /**
   * Test extreme literal cases to ensure the value passes
   * through the analyzer correctly.
   */
  @Test
  public void testLiteralCast() {
    for (int i = 0; i < 3; i++) {
      boolean useDecimalV2 = i > 1;
      boolean enableRewrite = (i % 2) == 1;
      {
        // Boundary case in which the positive value is a DECIMAL,
        // becomes BIGINT when negated by the parser.
        String query = "select getBit(" + Long.MIN_VALUE + ", 63)";
        Expr expr = analyze(query, useDecimalV2, enableRewrite);
        assertEquals(Type.TINYINT, expr.getType());
      }
      {
        // Would eval to NaN, so keep original expr.
        String query = "select cast(10 as double) / cast(0 as double)";
        Expr expr = analyze(query, useDecimalV2, enableRewrite);
        assertEquals(Type.DOUBLE, expr.getType());
        assertTrue(expr instanceof ArithmeticExpr);
      }
      {
        // Extreme double value. Ensure double-->BigDecimal noise
        // does not cause overflows
        String query = "select cast(" + Double.toString(Double.MAX_VALUE) +
            " as double)";
        Expr expr = analyze(query, useDecimalV2, enableRewrite);
        assertEquals(Type.DOUBLE, expr.getType());
        if (enableRewrite) {
          assertTrue(expr instanceof NumericLiteral);
        } else {
          assertTrue(expr instanceof CastExpr);
        }
      }
      {
        // As above, but for extreme minimum (smallest) values
        String query = "select cast(" + Double.toString(Double.MIN_VALUE) +
            " as double)";
        Expr expr = analyze(query, useDecimalV2, enableRewrite);
        assertEquals(Type.DOUBLE, expr.getType());
        if (enableRewrite) {
          assertTrue(expr instanceof NumericLiteral);
        } else {
          assertTrue(expr instanceof CastExpr);
        }
      }
      {
        // Math would cause overflow, don't rewrite
        String query = "select cast(1.7976931348623157e+308 as double)" +
            " / cast(2.2250738585072014e-308 as double)";
        Expr expr = analyze(query, useDecimalV2, enableRewrite);
        assertEquals(Type.DOUBLE, expr.getType());
        assertTrue(expr instanceof ArithmeticExpr);
      }
      {
        // Math would cause overflow, don't rewrite
        String query = "select cast(cast(1.7976931348623157e+308 as double) as float)";
        Expr expr = analyze(query, useDecimalV2, enableRewrite);
        assertEquals(Type.FLOAT, expr.getType());
        assertTrue(expr instanceof CastExpr);
      }
    }
  }
}
