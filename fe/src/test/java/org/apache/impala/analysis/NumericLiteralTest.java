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

import static org.junit.Assert.*;

import java.math.BigDecimal;

import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.TypeCompatibility;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InvalidValueException;
import org.apache.impala.common.SqlCastException;
import org.junit.Test;

/**
 * Tests the numeric literal which is complex because of its ability to hold
 * values across many types. The value and type must be compatible at all times.
 *
 * Note that a comprehensive set of tests exist on the C++ site in expr-test.cc.
 * Those tests call into Java, then verify the results. Very complete, but hard
 * to debug from the Java side.
 */
public class NumericLiteralTest {

  // Approximate maximum DECIMAL scale for a BIGINT
  // 9,223,372,036,854,775,807
  private static final int MAX_BIGINT_PRECISION = 19;

  // "One above" and "one below" values for testing type ranges
  private static final BigDecimal ABOVE_TINYINT =
      NumericLiteral.MAX_TINYINT.add(BigDecimal.ONE);
  private static final BigDecimal BELOW_TINYINT =
      NumericLiteral.MIN_TINYINT.subtract(BigDecimal.ONE);
  private static final BigDecimal ABOVE_SMALLINT =
      NumericLiteral.MAX_SMALLINT.add(BigDecimal.ONE);
  private static final BigDecimal BELOW_SMALLINT =
      NumericLiteral.MIN_SMALLINT.subtract(BigDecimal.ONE);
  private static final BigDecimal ABOVE_INT =
      NumericLiteral.MAX_INT.add(BigDecimal.ONE);
  private static final BigDecimal BELOW_INT =
      NumericLiteral.MIN_INT.subtract(BigDecimal.ONE);
  private static final BigDecimal ABOVE_BIGINT =
      NumericLiteral.MAX_BIGINT.add(BigDecimal.ONE);
  private static final BigDecimal BELOW_BIGINT =
      NumericLiteral.MIN_BIGINT.subtract(BigDecimal.ONE);

  private static String repeat(String str, int n) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < n; i++) buf.append(str);
    return buf.toString();
  }

  private static String genDecimal(int precision, int scale) {
    if (scale == 0) {
      return repeat("9", precision);
    } else {
      return repeat("9", precision - scale) + "." +
             repeat("4", scale);
    }
  }

  @Test
  public void testBasics() throws AnalysisException {
    NumericLiteral n = NumericLiteral.create(0);
    // Starts with the smallest possible type
    assertEquals(Type.TINYINT, n.getType());
    // Literals start analyzed
    assertTrue(n.isAnalyzed());
    // With their costs set
    assertEquals(LiteralExpr.LITERAL_COST, n.getCost(), 0.01);
    assertEquals(-1, n.getSelectivity(), 0.01);
    // Sanity check of the string representation
    assertEquals("0", n.getStringValue());
    assertEquals("0:TINYINT", n.toString());
    // Sanity check of casting
    n.castTo(Type.SMALLINT);
    assertEquals(Type.SMALLINT, n.getType());
    assertEquals("0:SMALLINT", n.toString());
  }

  /**
   * Sanity test: we rely on the constants to be accurate the other tests.
   * This will, hopefully, catch any accidental changes to them.
   */
  @Test
  public void testConstants() throws InvalidValueException {
    assertEquals(BigDecimal.valueOf(Byte.MIN_VALUE), NumericLiteral.MIN_TINYINT);
    assertEquals(BigDecimal.valueOf(Byte.MAX_VALUE), NumericLiteral.MAX_TINYINT);
    assertEquals(BigDecimal.valueOf(Short.MIN_VALUE), NumericLiteral.MIN_SMALLINT);
    assertEquals(BigDecimal.valueOf(Short.MAX_VALUE), NumericLiteral.MAX_SMALLINT);
    assertEquals(BigDecimal.valueOf(Integer.MIN_VALUE), NumericLiteral.MIN_INT);
    assertEquals(BigDecimal.valueOf(Integer.MAX_VALUE), NumericLiteral.MAX_INT);
    assertEquals(BigDecimal.valueOf(Long.MIN_VALUE), NumericLiteral.MIN_BIGINT);
    assertEquals(BigDecimal.valueOf(Long.MAX_VALUE), NumericLiteral.MAX_BIGINT);
    assertEquals(BigDecimal.valueOf(-Float.MAX_VALUE), NumericLiteral.MIN_FLOAT);
    assertEquals(BigDecimal.valueOf(Float.MAX_VALUE), NumericLiteral.MAX_FLOAT);
    assertEquals(BigDecimal.valueOf(-Double.MAX_VALUE), NumericLiteral.MIN_DOUBLE);
    assertEquals(BigDecimal.valueOf(Double.MAX_VALUE), NumericLiteral.MAX_DOUBLE);
  }

  /**
   * Detailed test of the mechanism to infer the "natural type" (smallest
   * type) for a value.
   */
  @Test
  public void testInferType() throws SqlCastException {
    assertEquals(Type.TINYINT, NumericLiteral.inferType(BigDecimal.ZERO));
    assertEquals(Type.TINYINT, NumericLiteral.inferType(NumericLiteral.MIN_TINYINT));
    assertEquals(Type.TINYINT, NumericLiteral.inferType(NumericLiteral.MAX_TINYINT));

    assertEquals(Type.SMALLINT, NumericLiteral.inferType(ABOVE_TINYINT));
    assertEquals(Type.SMALLINT, NumericLiteral.inferType(BELOW_TINYINT));
    assertEquals(Type.SMALLINT, NumericLiteral.inferType(NumericLiteral.MIN_SMALLINT));
    assertEquals(Type.SMALLINT, NumericLiteral.inferType(NumericLiteral.MAX_SMALLINT));

    assertEquals(Type.INT, NumericLiteral.inferType(ABOVE_SMALLINT));
    assertEquals(Type.INT, NumericLiteral.inferType(BELOW_SMALLINT));
    assertEquals(Type.INT, NumericLiteral.inferType(NumericLiteral.MIN_INT));
    assertEquals(Type.INT, NumericLiteral.inferType(NumericLiteral.MAX_INT));

    assertEquals(Type.BIGINT, NumericLiteral.inferType(ABOVE_INT));
    assertEquals(Type.BIGINT, NumericLiteral.inferType(BELOW_INT));
    assertEquals(Type.BIGINT, NumericLiteral.inferType(NumericLiteral.MIN_BIGINT));
    assertEquals(Type.BIGINT, NumericLiteral.inferType(NumericLiteral.MAX_BIGINT));

    // 9.9. Is DECIMAL
    assertEquals(ScalarType.createDecimalType(2, 1),
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(2, 1))));
    assertEquals(ScalarType.createDecimalType(2, 1),
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(2, 1)).negate()));

    // One bigger or smaller than BIGINT
    assertEquals(ScalarType.createDecimalType(MAX_BIGINT_PRECISION, 0),
        NumericLiteral.inferType(ABOVE_BIGINT));
    assertEquals(ScalarType.createDecimalType(MAX_BIGINT_PRECISION, 0),
        NumericLiteral.inferType(BELOW_BIGINT));

    // All 9s, just bigger than BIGINT
    assertEquals(ScalarType.createDecimalType(MAX_BIGINT_PRECISION, 0),
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(MAX_BIGINT_PRECISION, 0))));
    assertEquals(ScalarType.createDecimalType(MAX_BIGINT_PRECISION, 0),
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(MAX_BIGINT_PRECISION, 0)).negate()));

    // All 9s, at limits of DECIMAL precision
    assertEquals(ScalarType.createDecimalType(ScalarType.MAX_SCALE, 0),
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(ScalarType.MAX_SCALE, 0))));
    assertEquals(ScalarType.createDecimalType(ScalarType.MAX_SCALE, 0),
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(ScalarType.MAX_SCALE, 0)).negate()));

    // Too large for DECIMAL, flips to DOUBLE
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(ScalarType.MAX_SCALE + 1, 0))));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(ScalarType.MAX_SCALE + 1, 0)).negate()));

    // Too large for Decimal, small enough for FLOAT
    // DECIMAL range is e38 as is FLOAT. So, there is a small range
    // in which we could flip from DECIMAL to FLOAT, but we choose
    // to use DOUBLE even in this range.
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(NumericLiteral.MIN_FLOAT));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(NumericLiteral.MAX_FLOAT));
    // Float.MIN_VALUE means smallest positive value, confusingly
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(Float.MIN_VALUE)));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(-Float.MIN_VALUE)));

    // Too large for Decimal, exponent too large for FLOAT
    String value = "12345" + repeat("0", 40);
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value)));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value).negate()));

    value = repeat("9", 10) + repeat("0", 40);
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value)));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value).negate()));

    // Too many digits for DOUBLE, but exponent fits
    value = repeat("9", 30) + repeat("0", 50);
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value)));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value).negate()));
    value = genDecimal(100, 10);
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value)));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value).negate()));

    // Limit of DOUBLE range
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(NumericLiteral.MIN_DOUBLE));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(NumericLiteral.MAX_DOUBLE));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(Double.MIN_VALUE)));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(-Double.MIN_VALUE)));

    // Too big for DOUBLE or DECIMAL
    try {
      NumericLiteral.inferType(new BigDecimal(genDecimal(309, 0)));
      fail();
    } catch (SqlCastException e) {
      // Expected
    }

    // Overflows DOUBLE, but low digits are truncated, so is DOUBLE
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(
            NumericLiteral.MAX_DOUBLE.add(BigDecimal.ONE)));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(
            NumericLiteral.MAX_DOUBLE.add(BigDecimal.ONE).negate()));

    // Another power of 10, actual overflow
    try {
      NumericLiteral.inferType(
          NumericLiteral.MAX_DOUBLE.multiply(BigDecimal.TEN));
      fail();
    } catch (SqlCastException e) {
      // Expected
    }
    try {
      NumericLiteral.inferType(
          NumericLiteral.MAX_DOUBLE.multiply(BigDecimal.TEN).negate());
      fail();
    } catch (SqlCastException e) {
      // Expected
    }

    // Too small for DOUBLE, too many digits for DECIMAL
    // BUG: Should round to zero per SQL standard.
    value = "." + repeat("0", 325) + "1";
    try {
      NumericLiteral.inferType(new BigDecimal(value));
      fail();
    } catch (SqlCastException e) {
      // Expected
    }
  }

  @Test
  public void testIsOverflow() throws InvalidValueException {
    assertFalse(NumericLiteral.isOverflow(BigDecimal.ZERO, Type.TINYINT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MIN_TINYINT, Type.TINYINT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MAX_TINYINT, Type.TINYINT));
    assertTrue(NumericLiteral.isOverflow(ABOVE_TINYINT, Type.TINYINT));
    assertTrue(NumericLiteral.isOverflow(BELOW_TINYINT, Type.TINYINT));

    assertFalse(NumericLiteral.isOverflow(BigDecimal.ZERO, Type.SMALLINT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MIN_SMALLINT, Type.SMALLINT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MAX_SMALLINT, Type.SMALLINT));
    assertTrue(NumericLiteral.isOverflow(ABOVE_SMALLINT, Type.SMALLINT));
    assertTrue(NumericLiteral.isOverflow(BELOW_SMALLINT, Type.SMALLINT));

    assertFalse(NumericLiteral.isOverflow(BigDecimal.ZERO, Type.INT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MIN_INT, Type.INT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MAX_INT, Type.INT));
    assertTrue(NumericLiteral.isOverflow(ABOVE_INT, Type.INT));
    assertTrue(NumericLiteral.isOverflow(BELOW_INT, Type.INT));

    assertFalse(NumericLiteral.isOverflow(BigDecimal.ZERO, Type.BIGINT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MIN_BIGINT, Type.BIGINT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MAX_BIGINT, Type.BIGINT));
    assertTrue(NumericLiteral.isOverflow(ABOVE_BIGINT, Type.BIGINT));
    assertTrue(NumericLiteral.isOverflow(BELOW_BIGINT, Type.BIGINT));

    assertFalse(NumericLiteral.isOverflow(BigDecimal.ZERO, Type.FLOAT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MIN_FLOAT, Type.FLOAT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MAX_FLOAT, Type.FLOAT));
    assertFalse(NumericLiteral.isOverflow(
        BigDecimal.valueOf(Float.MIN_VALUE), Type.FLOAT));
    assertTrue(NumericLiteral.isOverflow(
        NumericLiteral.MAX_FLOAT.add(BigDecimal.ONE), Type.FLOAT));
    assertTrue(NumericLiteral.isOverflow(
        NumericLiteral.MAX_FLOAT.multiply(BigDecimal.TEN), Type.FLOAT));
    // Underflow is not overflow
    assertFalse(NumericLiteral.isOverflow(BigDecimal.valueOf(
        Float.MIN_VALUE).divide(BigDecimal.TEN), Type.FLOAT));

    assertFalse(NumericLiteral.isOverflow(BigDecimal.ZERO, Type.DOUBLE));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MIN_DOUBLE, Type.DOUBLE));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MAX_DOUBLE, Type.DOUBLE));
    assertFalse(NumericLiteral.isOverflow(
        BigDecimal.valueOf(Double.MIN_VALUE), Type.DOUBLE));
    // Have to add quite a bit (enough to hit a significant digit in the
    // double), else BigDecimal truncates the bits when converting to double.
    assertTrue(NumericLiteral.isOverflow(
        NumericLiteral.MAX_DOUBLE.add(new BigDecimal("1e300")), Type.DOUBLE));
    assertTrue(NumericLiteral.isOverflow(
        NumericLiteral.MAX_DOUBLE.multiply(BigDecimal.TEN), Type.DOUBLE));
    // Underflow is not overflow
    assertFalse(NumericLiteral.isOverflow(BigDecimal.valueOf(
        Double.MIN_VALUE).divide(BigDecimal.TEN), Type.DOUBLE));

    assertFalse(NumericLiteral.isOverflow(BigDecimal.ZERO, Type.DECIMAL));
    assertFalse(NumericLiteral.isOverflow(
        new BigDecimal(genDecimal(10,5)), Type.DECIMAL));
    assertFalse(NumericLiteral.isOverflow(
        new BigDecimal(genDecimal(10,5)), Type.DECIMAL));
    assertFalse(NumericLiteral.isOverflow(
        new BigDecimal(genDecimal(ScalarType.MAX_PRECISION, 0)), Type.DECIMAL));
    assertFalse(NumericLiteral.isOverflow(
        new BigDecimal(genDecimal(0, ScalarType.MAX_PRECISION)), Type.DECIMAL));
    assertTrue(NumericLiteral.isOverflow(
        new BigDecimal(genDecimal(ScalarType.MAX_PRECISION + 1, 0)), Type.DECIMAL));
    assertTrue(NumericLiteral.isOverflow(
        new BigDecimal(genDecimal(ScalarType.MAX_PRECISION + 1, 1)), Type.DECIMAL));
    assertTrue(NumericLiteral.isOverflow(
        new BigDecimal(genDecimal(0, ScalarType.MAX_PRECISION + 1)), Type.DECIMAL));
  }

  /**
   * Test the constructor that takes a BigDecimal argument and sets the
   * literal type to the "natural" type (the smallest type that can hold
   * the value.)
   */
  @Test
  public void testSimpleCtor() throws SqlCastException {
    // Spot check. Assumes uses inferType() tested above.
    NumericLiteral n = new NumericLiteral(BigDecimal.ZERO);
    assertEquals(0, n.getLongValue());
    assertEquals(Type.TINYINT, n.getType());

    n = new NumericLiteral(NumericLiteral.MAX_TINYINT);
    assertEquals(Byte.MAX_VALUE, n.getLongValue());
    assertEquals(Type.TINYINT, n.getType());

    n = new NumericLiteral(NumericLiteral.MAX_BIGINT);
    assertEquals(Type.BIGINT, n.getType());
    assertEquals(Long.MAX_VALUE, n.getLongValue());

    n = new NumericLiteral(NumericLiteral.MAX_DOUBLE);
    assertEquals(Double.MAX_VALUE, n.getDoubleValue(), 1.0);
    assertEquals(Type.DOUBLE, n.getType());

    n = new NumericLiteral(new BigDecimal(genDecimal(35, 0)));
    assertEquals(ScalarType.createDecimalType(35, 0), n.getType());

    try {
      new NumericLiteral(NumericLiteral.MAX_DOUBLE.multiply(BigDecimal.TEN));
      fail();
    } catch(SqlCastException e) {
      // Expected
    }
  }

  @Test
  public void testTypeCtor() throws InvalidValueException, SqlCastException {
    NumericLiteral n = new NumericLiteral(BigDecimal.ZERO);
    assertEquals(0, n.getLongValue());
    assertEquals(Type.TINYINT, n.getType());

    n = new NumericLiteral(NumericLiteral.MAX_TINYINT);
    assertEquals(Byte.MAX_VALUE, n.getLongValue());
    assertEquals(Type.TINYINT, n.getType());

    n = new NumericLiteral(NumericLiteral.MAX_BIGINT);
    assertEquals(Type.BIGINT, n.getType());
    assertEquals(Long.MAX_VALUE, n.getLongValue());

    n = new NumericLiteral(NumericLiteral.MAX_DOUBLE);
    assertEquals(Double.MAX_VALUE, n.getDoubleValue(), 1.0);
    assertEquals(Type.DOUBLE, n.getType());

    n = new NumericLiteral(new BigDecimal(genDecimal(35, 0)));
    assertEquals(ScalarType.createDecimalType(35, 0), n.getType());

    try {
      new NumericLiteral(NumericLiteral.MAX_DOUBLE.multiply(BigDecimal.TEN));
      fail();
    } catch(SqlCastException e) {
      // Expected
    }
    try {
      new NumericLiteral(new BigDecimal("123.45"),
        ScalarType.createDecimalType(3, 1));
      fail();
    } catch(SqlCastException e) {
      // Expected
    }
    try {
      new NumericLiteral(new BigDecimal(Integer.MAX_VALUE),
          Type.TINYINT);
      fail();
    } catch(SqlCastException e) {
      // Expected
    }

    n = new NumericLiteral(new BigDecimal("1.567"), ScalarType.createDecimalType(2, 1));
    assertEquals(ScalarType.createDecimalType(2, 1), n.getType());
    assertEquals("1.6", n.getValue().toString());
  }

  @Test
  public void testExtremes() throws InvalidValueException, SqlCastException {
    NumericLiteral n = new NumericLiteral(NumericLiteral.MAX_DOUBLE);
    assertEquals(Double.MAX_VALUE, n.getDoubleValue(), 1);
    n = new NumericLiteral(NumericLiteral.MIN_DOUBLE);
    assertEquals(-Double.MAX_VALUE, n.getDoubleValue(), 1);
  }

  @Test
  public void testCastTo() throws AnalysisException {
    {
      // Integral types
      NumericLiteral n = new NumericLiteral(BigDecimal.ZERO);
      Expr result = n.uncheckedCastTo(Type.BIGINT, TypeCompatibility.DEFAULT);
      assertSame(n, result);
      assertEquals(Type.BIGINT, n.getType());
      result = n.uncheckedCastTo(Type.TINYINT, TypeCompatibility.DEFAULT);
      assertSame(n, result);
      assertEquals(Type.TINYINT, n.getType());
      result = n.uncheckedCastTo(
          ScalarType.createDecimalType(5, 0), TypeCompatibility.DEFAULT);
      assertSame(n, result);
      assertEquals(ScalarType.createDecimalType(5, 0), n.getType());
    }
    {
      // Integral types, with overflow
      NumericLiteral n = new NumericLiteral(ABOVE_SMALLINT);
      Expr result = n.uncheckedCastTo(Type.BIGINT, TypeCompatibility.DEFAULT);
      assertSame(n, result);
      assertEquals(Type.BIGINT, n.getType());
      Expr result2 = n.uncheckedCastTo(Type.SMALLINT, TypeCompatibility.DEFAULT);
      assertTrue(result2 instanceof CastExpr);
      assertEquals(Type.SMALLINT, result2.getType());
    }
    {
      // Decimal types, with overflow
      // Note: not safe to reuse above value after exception
      NumericLiteral n = new NumericLiteral(ABOVE_SMALLINT);
      Expr result = n.uncheckedCastTo(Type.BIGINT, TypeCompatibility.DEFAULT);
      assertSame(n, result);
      assertEquals(Type.BIGINT, n.getType());
      Expr result2 = n.uncheckedCastTo(
          ScalarType.createDecimalType(2, 0), TypeCompatibility.DEFAULT);
      assertTrue(result2 instanceof CastExpr);
      assertEquals(ScalarType.createDecimalType(2, 0), result2.getType());
    }
    {
      // Decimal types
      NumericLiteral n = new NumericLiteral(new BigDecimal("123.45"));
      assertEquals(ScalarType.createDecimalType(5, 2), n.getType());
      Expr result = n.uncheckedCastTo(
          ScalarType.createDecimalType(6, 3), TypeCompatibility.DEFAULT);
      assertSame(n, result);
      assertEquals(ScalarType.createDecimalType(6, 3), n.getType());
      result = n.uncheckedCastTo(
          ScalarType.createDecimalType(5, 2), TypeCompatibility.DEFAULT);
      assertSame(n, result);
      assertEquals(ScalarType.createDecimalType(5, 2), n.getType());
      result = n.uncheckedCastTo(
          ScalarType.createDecimalType(4, 1), TypeCompatibility.DEFAULT);
      assertNotSame(n, result);
      assertEquals(ScalarType.createDecimalType(4, 1), result.getType());
      assertEquals("123.5", ((NumericLiteral) result).toSql());
    }
  }

  /**
   * Test the swap() sign method used by the parser which recognizes
   * numbers as:
   *
   * 12345 --> number
   * - number --> swap sign
   *
   * Note that swap sign can be applied multiple times:
   *
   * - -1234 --> number, swap sign, swap sign
   *
   * Swapping sign is not as simple as it might seem. For integers,
   * the positive range is smaller than the negative range, so:
   *
   * 256 --> SMALLINT
   * -256 --> TINYINT
   *
   * This means that 256 starts as a SMALLINT, but drops one
   * size to TINYINT when it becomes negative. And, if negated again, it jumps
   * up one size to again become SMALLINT.
   */
  @Test
  public void testSwapSign() {
    {
      // TINYINT size promotion
      int absValue = -(int) Byte.MIN_VALUE;
      NumericLiteral n = NumericLiteral.create(absValue);
      assertEquals(Type.SMALLINT, n.getType());
      assertEquals(Type.SMALLINT, n.getExplicitType());
      assertEquals(absValue, n.getIntValue());
      n.swapSign();
      assertEquals(Type.TINYINT, n.getType());
      assertEquals(Type.TINYINT, n.getExplicitType());
      assertEquals(-absValue, n.getIntValue());
      n.swapSign();
      assertEquals(Type.SMALLINT, n.getType());
      assertEquals(Type.SMALLINT, n.getExplicitType());
      assertEquals(absValue, n.getIntValue());
    }
    {
      // Max BIGINT promotion: can't become another, larger
      // integer, so must become a DECIMAL.
      BigDecimal absValue = NumericLiteral.MIN_BIGINT.negate();
      NumericLiteral n = NumericLiteral.create(absValue);
      Type posType = ScalarType.createDecimalType(19);
      assertEquals(posType, n.getType());
      assertEquals(posType, n.getExplicitType());
      assertTrue(absValue.compareTo(n.getValue()) == 0);
      n.swapSign();
      assertEquals(Type.BIGINT, n.getType());
      assertEquals(Type.BIGINT, n.getExplicitType());
      assertEquals(Long.MIN_VALUE, n.getLongValue());
      n.swapSign();
      assertEquals(posType, n.getType());
      assertEquals(posType, n.getExplicitType());
      assertTrue(absValue.compareTo(n.getValue()) == 0);
    }
  }

  /**
   * Test of the major cases for convertValue(). Details of overflow
   * detection are tested above.
   */
  @Test
  public void testConvertValue() throws SqlCastException {
    BigDecimal result = NumericLiteral.convertValue(BigDecimal.ZERO, Type.TINYINT);
    assertSame(result, BigDecimal.ZERO);
    result = NumericLiteral.convertValue(BigDecimal.ZERO, Type.DOUBLE);
    assertSame(result, BigDecimal.ZERO);
    result = NumericLiteral.convertValue(BigDecimal.ZERO,
        ScalarType.createDecimalType(2, 2));
    assertSame(result, BigDecimal.ZERO);

    // Overflow case
    try {
      NumericLiteral.convertValue(ABOVE_TINYINT, Type.TINYINT);
      fail();
    } catch(SqlCastException e) {
      // Expected
    }

    // Round to integer
    result = NumericLiteral.convertValue(
        new BigDecimal("1234.56"), Type.INT);
    assertEquals("1235", result.toString());

    // Round to decimal precision
    BigDecimal input = new BigDecimal("1234.56789");
    result = NumericLiteral.convertValue(
        input, ScalarType.createDecimalType(7, 3));
    assertEquals("1234.568", result.toString());
    result = NumericLiteral.convertValue(
        input, ScalarType.createDecimalType(4, 0));
    assertEquals("1235", result.toString());

    // Decimal overflow
    try {
      NumericLiteral.convertValue(
          new BigDecimal("1234.56789"), ScalarType.createDecimalType(3, 2));
      fail();
    } catch(SqlCastException e) {
      // Expected
    }

    // Reuse value as decimal
    input = new BigDecimal("1235.56");
    result = NumericLiteral.convertValue(input,
        ScalarType.createDecimalType(6, 2));
    assertSame(input, result);
    input = new BigDecimal("0.01");
    result = NumericLiteral.convertValue(input,
        ScalarType.createDecimalType(2, 2));
    assertSame(input, result);
  }

  @Test
  public void testCast() throws SqlCastException {
    NumericLiteral n = NumericLiteral.create(1000);
    assertEquals(Type.SMALLINT, n.getType());
    Expr result = n.uncheckedCastTo(Type.TINYINT, TypeCompatibility.DEFAULT);
    assertTrue(result instanceof CastExpr);
    assertEquals(Type.TINYINT, result.getType());

    result = n.uncheckedCastTo(Type.INT, TypeCompatibility.DEFAULT);
    assertSame(n, result);
    assertEquals(Type.INT, n.getType());

    n = new NumericLiteral(new BigDecimal("123.45"));
    assertEquals(ScalarType.createDecimalType(5, 2), n.getType());
    assertSame(n,
        n.uncheckedCastTo(ScalarType.createDecimalType(6, 3), TypeCompatibility.DEFAULT));
    assertEquals(ScalarType.createDecimalType(6, 3), n.getType());

    n = new NumericLiteral(new BigDecimal("123.45"));
    Type newType = ScalarType.createDecimalType(4, 1);
    result = n.uncheckedCastTo(newType, TypeCompatibility.DEFAULT);
    assertNotSame(result, n);
    assertTrue(result instanceof NumericLiteral);
    assertEquals(newType, result.getType());
    NumericLiteral n2 = (NumericLiteral) result;
    assertEquals("123.5", n2.getValue().toString());

    Expr result2 = n2.uncheckedCastTo(Type.SMALLINT, TypeCompatibility.DEFAULT);
    assertNotSame(result2, result);
    assertEquals(Type.SMALLINT, result2.getType());
    assertEquals("124", ((NumericLiteral)result2).getValue().toString());
  }

  @Test
  public void testEquality() throws SqlCastException {
    NumericLiteral n1 = NumericLiteral.create(10);
    NumericLiteral n2 = NumericLiteral.create(10);
    NumericLiteral n3 = NumericLiteral.create(10, Type.INT);
    NumericLiteral n4 = NumericLiteral.create(11);
    NumericLiteral n5 = NumericLiteral.create(
        new BigDecimal("10.000"), Type.TINYINT);

    // Same object
    assertTrue(n1.localEquals(n1));
    // Types and values match
    assertTrue(n1.localEquals(n2));
    // Types differ, values same
    assertFalse(n1.localEquals(n3));
    // Types same, values differ
    assertFalse(n1.localEquals(n4));
    // Types same, values are considered the same
    // (Though BigDecimal.equals() considers the values different
    // due to different precisions.)
    assertTrue(n1.localEquals(n5));
  }
}
