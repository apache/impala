package com.cloudera.impala.analysis;

import static org.junit.Assert.fail;

import org.junit.Test;

import com.cloudera.impala.catalog.PrimitiveType;

public class ExprTest {
  // Test creation of LiteralExprs from Strings, e.g., for partitioning keys.
  @Test
  public void TestLiteralExpr() {
    testLiteralExprPositive("false", PrimitiveType.BOOLEAN);
    testLiteralExprPositive("1", PrimitiveType.TINYINT);
    testLiteralExprPositive("1", PrimitiveType.SMALLINT);
    testLiteralExprPositive("1", PrimitiveType.INT);
    testLiteralExprPositive("1", PrimitiveType.BIGINT);
    testLiteralExprPositive("1.0", PrimitiveType.FLOAT);
    testLiteralExprPositive("1.0", PrimitiveType.DOUBLE);
    testLiteralExprPositive("ABC", PrimitiveType.STRING);

    // INVALID_TYPE should always fail
    testLiteralExprNegative("ABC", PrimitiveType.INVALID_TYPE);

    // Invalid casts
    testLiteralExprNegative("ABC", PrimitiveType.BOOLEAN);
    testLiteralExprNegative("ABC", PrimitiveType.TINYINT);
    testLiteralExprNegative("ABC", PrimitiveType.SMALLINT);
    testLiteralExprNegative("ABC", PrimitiveType.INT);
    testLiteralExprNegative("ABC", PrimitiveType.BIGINT);
    testLiteralExprNegative("ABC", PrimitiveType.FLOAT);
    testLiteralExprNegative("ABC", PrimitiveType.DOUBLE);
    testLiteralExprNegative("ABC", PrimitiveType.TIMESTAMP);

    // Date types not implemented
    testLiteralExprNegative("2010-01-01", PrimitiveType.DATE);
    testLiteralExprNegative("2010-01-01", PrimitiveType.DATETIME);
    testLiteralExprNegative("2010-01-01", PrimitiveType.TIMESTAMP);
  }

  private void testLiteralExprPositive(String value, PrimitiveType type) {
    LiteralExpr expr = null;
    try {
      expr = LiteralExpr.create(value, type);
    } catch (Exception e) {
      fail("\nFailed to create LiteralExpr of type: " + type.toString() + " from: " + value
          + " due to " + e.getMessage() + "\n");
    }
    if (expr == null) {
      fail("\nFailed to create LiteralExpr\n");
    }
  }

  private void testLiteralExprNegative(String value, PrimitiveType type) {
    boolean failure = false;
    LiteralExpr expr = null;
    try {
      expr = LiteralExpr.create(value, type);
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
}
