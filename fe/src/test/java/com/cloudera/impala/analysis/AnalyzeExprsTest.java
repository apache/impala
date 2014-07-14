// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.cloudera.impala.analysis.TimestampArithmeticExpr.TimeUnit;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.ScalarFunction;
import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.TestSchemaUtils;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class AnalyzeExprsTest extends AnalyzerTest {

  @Test
  public void TestNumericLiteralMinMaxValues() {
    testNumericLiteral(Byte.toString(Byte.MIN_VALUE), Type.TINYINT);
    testNumericLiteral(Byte.toString(Byte.MAX_VALUE), Type.TINYINT);
    testNumericLiteral("- " + Byte.toString(Byte.MIN_VALUE), Type.SMALLINT);
    testNumericLiteral("- " + Byte.toString(Byte.MAX_VALUE), Type.TINYINT);

    testNumericLiteral(Short.toString(Short.MIN_VALUE), Type.SMALLINT);
    testNumericLiteral(Short.toString(Short.MAX_VALUE), Type.SMALLINT);
    testNumericLiteral("- " + Short.toString(Short.MIN_VALUE), Type.INT);
    testNumericLiteral("- " + Short.toString(Short.MAX_VALUE), Type.SMALLINT);

    testNumericLiteral(Integer.toString(Integer.MIN_VALUE), Type.INT);
    testNumericLiteral(Integer.toString(Integer.MAX_VALUE), Type.INT);
    testNumericLiteral("- " + Integer.toString(Integer.MIN_VALUE), Type.BIGINT);
    testNumericLiteral("- " + Integer.toString(Integer.MAX_VALUE), Type.INT);

    testNumericLiteral(Long.toString(Long.MIN_VALUE), Type.BIGINT);
    testNumericLiteral(Long.toString(Long.MAX_VALUE), Type.BIGINT);
    testNumericLiteral(Long.toString(Long.MIN_VALUE), Type.BIGINT);
    testNumericLiteral("- " + Long.toString(Long.MAX_VALUE), Type.BIGINT);

    // Result type is a decimal because Long can't hold the value.
    testNumericLiteral(Long.toString(Long.MIN_VALUE) + "1",
        ScalarType.createDecimalType(20, 0));
    testNumericLiteral(Long.toString(Long.MIN_VALUE) + "1",
        ScalarType.createDecimalType(20, 0));
    // Test min int64-1.
    BigInteger minMinusOne = BigInteger.valueOf(Long.MIN_VALUE);
    minMinusOne = minMinusOne.subtract(BigInteger.ONE);
    testNumericLiteral(minMinusOne.toString(), ScalarType.createDecimalType(19, 0));
    // Test max int64+1.
    BigInteger maxPlusOne = BigInteger.valueOf(Long.MAX_VALUE);
    maxPlusOne = maxPlusOne.add(BigInteger.ONE);
    testNumericLiteral(maxPlusOne.toString(), ScalarType.createDecimalType(19, 0));

    // Test floating-point types.
    testNumericLiteral(Float.toString(Float.MIN_VALUE), Type.DOUBLE);
    testNumericLiteral(Float.toString(Float.MAX_VALUE), Type.DOUBLE);
    testNumericLiteral("-" + Float.toString(Float.MIN_VALUE), Type.DOUBLE);
    testNumericLiteral("-" + Float.toString(Float.MAX_VALUE), Type.DOUBLE);
    testNumericLiteral(Double.toString(Double.MIN_VALUE), Type.DOUBLE);
    testNumericLiteral(Double.toString(Double.MAX_VALUE), Type.DOUBLE);
    testNumericLiteral("-" + Double.toString(Double.MIN_VALUE), Type.DOUBLE);
    testNumericLiteral("-" + Double.toString(Double.MAX_VALUE), Type.DOUBLE);

    AnalysisError(String.format("select %s1", Double.toString(Double.MAX_VALUE)),
      "Numeric literal '1.7976931348623157E+3081' exceeds maximum range of doubles.");
    AnalysisError(String.format("select %s1", Double.toString(Double.MIN_VALUE)),
      "Numeric literal '4.9E-3241' underflows minimum resolution of doubles.");

    testNumericLiteral("0.99999999999999999999999999999999999999",
        ScalarType.createDecimalType(38,38));
    testNumericLiteral("99999999999999999999999999999999999999.",
        ScalarType.createDecimalType(38,0));
    testNumericLiteral("-0.99999999999999999999999999999999999999",
        ScalarType.createDecimalType(38,38));
    testNumericLiteral("-99999999999999999999999999999999999999.",
        ScalarType.createDecimalType(38,0));
    testNumericLiteral("999999999999999999999.99999999999999999",
        ScalarType.createDecimalType(38,17));
    testNumericLiteral("-999999999999999999.99999999999999999999",
        ScalarType.createDecimalType(38,20));

  }

  /**
   * Asserts that "select literal" analyzes ok and that the expectedType
   * matches the actual type.
   */
  private void testNumericLiteral(String literal, Type expectedType) {
    SelectStmt selectStmt = (SelectStmt) AnalyzesOk("select " + literal);
    Type actualType = selectStmt.resultExprs_.get(0).getType();
    Assert.assertTrue("Expected Type: " + expectedType + " Actual type: " + actualType,
        expectedType.equals(actualType));
  }

  @Test
  public void TestTimestampValueExprs() throws AnalysisException {
    AnalyzesOk("select cast (0 as timestamp)");
    AnalyzesOk("select cast (0.1 as timestamp)");
    AnalyzesOk("select cast ('1970-10-10 10:00:00.123' as timestamp)");
  }

  @Test
  public void TestBooleanValueExprs() throws AnalysisException {
    // Test predicates in where clause.
    AnalyzesOk("select * from functional.AllTypes where true");
    AnalyzesOk("select * from functional.AllTypes where false");
    AnalyzesOk("select * from functional.AllTypes where NULL");
    AnalyzesOk("select * from functional.AllTypes where bool_col = true");
    AnalyzesOk("select * from functional.AllTypes where bool_col = false");
    AnalyzesOk("select * from functional.AllTypes where bool_col = NULL");
    AnalyzesOk("select * from functional.AllTypes where NULL = NULL");
    AnalyzesOk("select * from functional.AllTypes where NULL and NULL or NULL");
    AnalyzesOk("select * from functional.AllTypes where true or false");
    AnalyzesOk("select * from functional.AllTypes where true and false");
    AnalyzesOk("select * from functional.AllTypes " +
        "where true or false and bool_col = false");
    AnalyzesOk("select * from functional.AllTypes " +
        "where true and false or bool_col = false");
    // In select list.
    AnalyzesOk("select bool_col = true from functional.AllTypes");
    AnalyzesOk("select bool_col = false from functional.AllTypes");
    AnalyzesOk("select bool_col = NULL from functional.AllTypes");
    AnalyzesOk("select true or false and bool_col = false from functional.AllTypes");
    AnalyzesOk("select true and false or bool_col = false from functional.AllTypes");
    AnalyzesOk("select NULL or NULL and NULL from functional.AllTypes");
  }

  @Test
  public void TestBinaryPredicates() throws AnalysisException {
    AnalyzesOk("select * from functional.alltypes where bool_col != true");
    AnalyzesOk("select * from functional.alltypes where tinyint_col <> 1");
    AnalyzesOk("select * from functional.alltypes where smallint_col <= 23");
    AnalyzesOk("select * from functional.alltypes where int_col > 15");
    AnalyzesOk("select * from functional.alltypes where bigint_col >= 17");
    AnalyzesOk("select * from functional.alltypes where float_col < 15.0");
    AnalyzesOk("select * from functional.alltypes where double_col > 7.7");
    // automatic type cast if compatible
    AnalyzesOk("select * from functional.alltypes where 1 = 0");
    AnalyzesOk("select * from functional.alltypes where int_col = smallint_col");
    AnalyzesOk("select * from functional.alltypes where bigint_col = float_col");
    AnalyzesOk("select * from functional.alltypes where bool_col = 0");
    AnalyzesOk("select * from functional.alltypes where int_col = cast('0' as int)");
    AnalyzesOk("select * from functional.alltypes where cast(string_col as int) = 15");
    // tests with NULL
    AnalyzesOk("select * from functional.alltypes where bool_col != NULL");
    AnalyzesOk("select * from functional.alltypes where tinyint_col <> NULL");
    AnalyzesOk("select * from functional.alltypes where smallint_col <= NULL");
    AnalyzesOk("select * from functional.alltypes where int_col > NULL");
    AnalyzesOk("select * from functional.alltypes where bigint_col >= NULL");
    AnalyzesOk("select * from functional.alltypes where float_col < NULL");
    AnalyzesOk("select * from functional.alltypes where double_col > NULL");
    AnalyzesOk("select * from functional.alltypes where string_col = NULL");
    AnalyzesOk("select * from functional.alltypes where timestamp_col = NULL");
    // invalid casts
    AnalysisError("select * from functional.alltypes where bool_col = '15'",
        "operands of type BOOLEAN and STRING are not comparable: bool_col = '15'");
    // AnalysisError("select * from functional.alltypes where date_col = 15",
    // "operands are not comparable: date_col = 15");
    // AnalysisError("select * from functional.alltypes where datetime_col = 1.0",
    // "operands are not comparable: datetime_col = 1.0");
  }


  @Test
  public void TestDecimalCasts() throws AnalysisException {
    AnalyzesOk("select cast(1.1 as boolean)");
    AnalyzesOk("select cast(1.1 as timestamp)");

    AnalysisError("select cast(true as decimal)",
        "Invalid type cast of TRUE from BOOLEAN to DECIMAL(9,0)");
    AnalysisError("select cast(cast(1 as timestamp) as decimal)",
        "Invalid type cast of CAST(1 AS TIMESTAMP) from TIMESTAMP to DECIMAL(9,0)");

    for (Type type: Type.getSupportedTypes()) {
      if (type.isNull() || type.isDecimal() || type.isBoolean() || type.isDateType()) {
        continue;
      }
      AnalyzesOk("select cast(1.1 as " + type + ")");
      AnalyzesOk("select cast(cast(1 as " + type + ") as decimal)");
    }

    // Casts to all other decimals are supported.
    for (int precision = 1; precision <= ScalarType.MAX_PRECISION; ++precision) {
      for (int scale = 0; scale < precision; ++scale) {
        Type t = ScalarType.createDecimalType(precision, scale);
        AnalyzesOk("select cast(1.1 as " + t + ")");
        AnalyzesOk("select cast(cast(1 as " + t + ") as decimal)");
      }
    }

    AnalysisError("select cast(1 as decimal(0, 1))",
        "Decimal precision must be greater than 0.");
  }

  @Test
  public void TestStringCasts() throws AnalysisException {
    // No implicit cast from STRING to numeric and boolean
    AnalysisError("select * from functional.alltypes where tinyint_col = '1'",
        "operands of type TINYINT and STRING are not comparable: tinyint_col = '1'");
    AnalysisError("select * from functional.alltypes where bool_col = '0'",
        "operands of type BOOLEAN and STRING are not comparable: bool_col = '0'");
    // No explicit cast from STRING to boolean.
    AnalysisError("select cast('false' as boolean) from functional.alltypes",
        "Invalid type cast of 'false' from STRING to BOOLEAN");

    AnalyzesOk("select * from functional.alltypes where " +
        "tinyint_col = cast('0.5' as float)");
    AnalyzesOk("select * from functional.alltypes where " +
        "smallint_col = cast('0.5' as float)");
    AnalyzesOk("select * from functional.alltypes where int_col = cast('0.5' as float)");
    AnalyzesOk("select * from functional.alltypes where " +
        "bigint_col = cast('0.5' as float)");
    AnalyzesOk("select 1.0 = cast('" + Double.toString(Double.MIN_VALUE) +
        "' as double)");
    AnalyzesOk("select 1.0 = cast('-" + Double.toString(Double.MIN_VALUE) +
        "' as double)");
    AnalyzesOk("select 1.0 = cast('" + Double.toString(Double.MAX_VALUE) +
        "' as double)");
    AnalyzesOk("select 1.0 = cast('-" + Double.toString(Double.MAX_VALUE) +
        "' as double)");
    // Test chains of minus. Note that "--" is the a comment symbol.
    AnalyzesOk("select * from functional.alltypes where " +
        "tinyint_col = cast('-1' as tinyint)");
    AnalyzesOk("select * from functional.alltypes where " +
        "tinyint_col = cast('- -1' as tinyint)");
    AnalyzesOk("select * from functional.alltypes where " +
        "tinyint_col = cast('- - -1' as tinyint)");
    AnalyzesOk("select * from functional.alltypes where " +
        "tinyint_col = cast('- - - -1' as tinyint)");
    // Test correct casting to compatible type on bitwise ops.
    AnalyzesOk("select 1 | cast('" + Byte.toString(Byte.MIN_VALUE) + "' as int)");
    AnalyzesOk("select 1 | cast('" + Byte.toString(Byte.MAX_VALUE) + "' as int)");
    AnalyzesOk("select 1 | cast('" + Short.toString(Short.MIN_VALUE) + "' as int)");
    AnalyzesOk("select 1 | cast('" + Short.toString(Short.MAX_VALUE) + "' as int)");
    AnalyzesOk("select 1 | cast('" + Integer.toString(Integer.MIN_VALUE) + "' as int)");
    AnalyzesOk("select 1 | cast('" + Integer.toString(Integer.MAX_VALUE) + "' as int)");
    // We need to add 1 to MIN_VALUE because there are no negative integer literals.
    // The reason is that whether a minus belongs to an
    // arithmetic expr or a literal must be decided by the parser, not the lexer.
    AnalyzesOk("select 1 | cast('" + Long.toString(Long.MIN_VALUE + 1) + "' as bigint)");
    AnalyzesOk("select 1 | cast('" + Long.toString(Long.MAX_VALUE) + "' as bigint)");
    // Cast to numeric never overflow
    AnalyzesOk("select * from functional.alltypes where tinyint_col = " +
        "cast('" + Long.toString(Long.MIN_VALUE) + "1' as tinyint)");
    AnalyzesOk("select * from functional.alltypes where tinyint_col = " +
        "cast('" + Long.toString(Long.MAX_VALUE) + "1' as tinyint)");
    AnalyzesOk("select * from functional.alltypes where tinyint_col = " +
        "cast('" + Double.toString(Double.MAX_VALUE) + "1' as tinyint)");
    // Java converts a float underflow to 0.0.
    // Since there is no easy, reliable way to detect underflow,
    // we don't consider it an error.
    AnalyzesOk("select * from functional.alltypes where tinyint_col = " +
        "cast('" + Double.toString(Double.MIN_VALUE) + "1' as tinyint)");
    // Cast never raise analysis exception
    AnalyzesOk("select * from functional.alltypes where " +
        "tinyint_col = cast('--1' as tinyint)");

    // Cast string literal to string
    AnalyzesOk("select cast('abc' as string)");

    // Cast decimal to string
    AnalyzesOk("select cast(cast('1.234' as decimal) as string)");
  }

  /**
   * Tests that cast(null to type) returns type for all types.
   */
  @Test
  public void TestNullCasts() throws AnalysisException {
    for (Type type: Type.getSupportedTypes()) {
      // TODO: Implement CHAR
      if (type.getPrimitiveType() == PrimitiveType.CHAR) continue;
       // Cannot cast to NULL_TYPE
      if (type.isNull()) continue;
      if (type.isDecimal()) type = Type.DEFAULT_DECIMAL;
      checkExprType("select cast(null as " + type + ")", type);
    }
  }

  // Analyzes query and asserts that the first result expr returns the given type.
  // Requires query to parse to a SelectStmt.
  private void checkExprType(String query, Type type) {
    SelectStmt select = (SelectStmt) AnalyzesOk(query);
    assertEquals(select.getResultExprs().get(0).getType(), type);
  }

  @Test
  public void TestLikePredicates() throws AnalysisException {
    AnalyzesOk("select * from functional.alltypes where string_col like  'test%'");
    AnalyzesOk("select * from functional.alltypes where string_col like string_col");
    AnalyzesOk("select * from functional.alltypes where 'test' like string_col");
    AnalyzesOk("select * from functional.alltypes where string_col rlike 'test%'");
    AnalyzesOk("select * from functional.alltypes where string_col regexp 'test.*'");
    AnalysisError("select * from functional.alltypes where string_col like 5",
        "right operand of LIKE must be of type STRING");
    AnalysisError("select * from functional.alltypes where 'test' like 5",
        "right operand of LIKE must be of type STRING");
    AnalysisError("select * from functional.alltypes where int_col like 'test%'",
        "left operand of LIKE must be of type STRING");
    AnalysisError("select * from functional.alltypes where string_col regexp 'test]['",
        "invalid regular expression in 'string_col REGEXP 'test][''");
    // Test NULLs.
    String[] likePreds = new String[] {"LIKE", "RLIKE", "REGEXP"};
    for (String likePred: likePreds) {
      AnalyzesOk(String.format("select * from functional.alltypes " +
          "where string_col %s NULL", likePred));
      AnalyzesOk(String.format("select * from functional.alltypes " +
          "where NULL %s string_col", likePred));
      AnalyzesOk(String.format("select * from functional.alltypes " +
          "where NULL %s NULL", likePred));
    }
  }

  @Test
  public void TestCompoundPredicates() throws AnalysisException {
    AnalyzesOk("select * from functional.alltypes where " +
        "string_col = '5' and int_col = 5");
    AnalyzesOk("select * from functional.alltypes where " +
        "string_col = '5' or int_col = 5");
    AnalyzesOk("select * from functional.alltypes where (string_col = '5' " +
        "or int_col = 5) and string_col > '1'");
    AnalyzesOk("select * from functional.alltypes where not string_col = '5'");
    AnalyzesOk("select * from functional.alltypes where int_col = cast('5' as int)");

    // Test all combinations of truth values and bool_col with all boolean operators.
    String[] operands = new String[]{ "true", "false", "NULL", "bool_col" };
    for (String lop: operands) {
      for (String rop: operands) {
        for (CompoundPredicate.Operator op: CompoundPredicate.Operator.values()) {
          // Unary operator tested elsewhere (below).
          if (op == CompoundPredicate.Operator.NOT) continue;
          String expr = String.format("%s %s %s", lop, op, rop);
          AnalyzesOk(String.format("select %s from functional.alltypes where %s",
              expr, expr));
        }
      }
      String notExpr = String.format("%s %s", CompoundPredicate.Operator.NOT, lop);
      AnalyzesOk(String.format("select %s from functional.alltypes where %s",
          notExpr, notExpr));
    }

    // arbitrary exprs as operands should fail to analyze
    AnalysisError("select * from functional.alltypes where 1 + 2 and false",
        "Operand '1 + 2' part of predicate '1 + 2 AND FALSE' should return " +
            "type 'BOOLEAN' but returns type 'SMALLINT'.");
    AnalysisError("select * from functional.alltypes where 1 + 2 or true",
        "Operand '1 + 2' part of predicate '1 + 2 OR TRUE' should return " +
            "type 'BOOLEAN' but returns type 'SMALLINT'.");
    AnalysisError("select * from functional.alltypes where not 1 + 2",
        "Operand '1 + 2' part of predicate 'NOT 1 + 2' should return " +
            "type 'BOOLEAN' but returns type 'SMALLINT'.");
    AnalysisError("select * from functional.alltypes where 1 + 2 and true",
        "Operand '1 + 2' part of predicate '1 + 2 AND TRUE' should return " +
            "type 'BOOLEAN' but returns type 'SMALLINT'.");
    AnalysisError("select * from functional.alltypes where false and trim('abc')",
        "Operand 'trim('abc')' part of predicate 'FALSE AND trim('abc')' should " +
            "return type 'BOOLEAN' but returns type 'STRING'.");
    AnalysisError("select * from functional.alltypes where bool_col or double_col",
        "Operand 'double_col' part of predicate 'bool_col OR double_col' should " +
            "return type 'BOOLEAN' but returns type 'DOUBLE'.");
  }

  @Test
  public void TestIsNullPredicates() throws AnalysisException {
    AnalyzesOk("select * from functional.alltypes where int_col is null");
    AnalyzesOk("select * from functional.alltypes where string_col is not null");
    AnalyzesOk("select * from functional.alltypes where null is not null");
  }

  @Test
  public void TestBetweenPredicates() throws AnalysisException {
    AnalyzesOk("select * from functional.alltypes " +
        "where tinyint_col between smallint_col and int_col");
    AnalyzesOk("select * from functional.alltypes " +
        "where tinyint_col not between smallint_col and int_col");
    AnalyzesOk("select * from functional.alltypes " +
        "where 'abc' between string_col and date_string_col");
    AnalyzesOk("select * from functional.alltypes " +
        "where 'abc' not between string_col and date_string_col");
    // Additional predicates before and/or after between predicate.
    AnalyzesOk("select * from functional.alltypes " +
        "where string_col = 'abc' and tinyint_col between 10 and 20");
    AnalyzesOk("select * from functional.alltypes " +
        "where tinyint_col between 10 and 20 and string_col = 'abc'");
    AnalyzesOk("select * from functional.alltypes " +
        "where bool_col and tinyint_col between 10 and 20 and string_col = 'abc'");
    // Chaining/nesting of between predicates.
    AnalyzesOk("select * from functional.alltypes " +
        "where true between false and true and 'b' between 'a' and 'c'");
    // true between ('b' between 'a' and 'b') and ('bb' between 'aa' and 'cc)
    AnalyzesOk("select * from functional.alltypes " +
        "where true between 'b' between 'a' and 'c' and 'bb' between 'aa' and 'cc'");
    // Test proper precedence with exprs before between.
    AnalyzesOk("select 5 + 1 between 4 and 10");
    AnalyzesOk("select 'abc' like '%a' between true and false");
    AnalyzesOk("select false between (true and true) and (false and true)");
    // Lower and upper bounds require implicit casts.
    AnalyzesOk("select * from functional.alltypes " +
        "where double_col between smallint_col and int_col");
    // Comparison expr requires implicit cast.
    AnalyzesOk("select * from functional.alltypes " +
        "where smallint_col between float_col and double_col");
    // Test NULLs.
    AnalyzesOk("select * from functional.alltypes " +
        "where NULL between float_col and double_col");
    AnalyzesOk("select * from functional.alltypes " +
        "where smallint_col between NULL and double_col");
    AnalyzesOk("select * from functional.alltypes " +
        "where smallint_col between float_col and NULL");
    AnalyzesOk("select * from functional.alltypes " +
        "where NULL between NULL and NULL");
    // Incompatible types.
    AnalysisError("select * from functional.alltypes " +
        "where string_col between bool_col and double_col",
        "Incompatible return types 'STRING' and 'BOOLEAN' " +
        "of exprs 'string_col' and 'bool_col'.");
    AnalysisError("select * from functional.alltypes " +
        "where timestamp_col between int_col and double_col",
        "Incompatible return types 'TIMESTAMP' and 'INT' " +
        "of exprs 'timestamp_col' and 'int_col'.");
  }

  @Test
  public void TestInPredicates() throws AnalysisException {
    AnalyzesOk("select * from functional.alltypes where int_col in (1, 2, 3, 4)");
    AnalyzesOk("select * from functional.alltypes where int_col not in (1, 2, 3, 4)");
    AnalyzesOk("select * from functional.alltypes where " +
        "string_col in ('a', 'b', 'c', 'd')");
    AnalyzesOk("select * from functional.alltypes where " +
        "string_col not in ('a', 'b', 'c', 'd')");
    // Test booleans.
    AnalyzesOk("select * from functional.alltypes where " +
        "true in (bool_col, true and false)");
    AnalyzesOk("select * from functional.alltypes where " +
        "true not in (bool_col, true and false)");
    // In list requires implicit casts.
    AnalyzesOk("select * from functional.alltypes where " +
        "double_col in (int_col, bigint_col)");
    // Comparison expr requires implicit cast.
    AnalyzesOk("select * from functional.alltypes where " +
        "int_col in (double_col, bigint_col)");
    // Test predicates.
    AnalyzesOk("select * from functional.alltypes where " +
        "!true in (false or true, true and false)");
    // Test NULLs.
    AnalyzesOk("select * from functional.alltypes where " +
        "NULL in (NULL, NULL)");
    // Incompatible types.
    AnalysisError("select * from functional.alltypes where " +
        "string_col in (bool_col, double_col)",
        "Incompatible return types 'STRING' and 'BOOLEAN' " +
        "of exprs 'string_col' and 'bool_col'.");
    AnalysisError("select * from functional.alltypes where " +
        "timestamp_col in (int_col, double_col)",
        "Incompatible return types 'TIMESTAMP' and 'INT' " +
        "of exprs 'timestamp_col' and 'int_col'.");
    AnalysisError("select * from functional.alltypes where " +
        "timestamp_col in (NULL, int_col)",
        "Incompatible return types 'TIMESTAMP' and 'INT' " +
        "of exprs 'timestamp_col' and 'int_col'.");
  }

  /**
   * Test of all arithmetic type casts.
   */
  @Test
  public void TestArithmeticTypeCasts() throws AnalysisException {
    // Test all non-decimal numeric types and the null type.
    // Decimal has custom type promotion rules which are tested elsewhere.
    Type[] numericTypes = new Type[] { Type.TINYINT, Type.SMALLINT, Type.INT,
        Type.BIGINT, Type.FLOAT, Type.DOUBLE , Type.NULL };
    for (Type type1 : numericTypes) {
      for (Type type2 : numericTypes) {
        Type t = Type.getAssignmentCompatibleType(type1, type2);
        assertTrue(t.isScalarType());
        ScalarType compatibleType = (ScalarType) t;
        Type promotedType = compatibleType.getNextResolutionType();
        boolean inputsNull = false;
        if (type1.isNull() && type2.isNull()) {
          inputsNull = true;
          promotedType = Type.DOUBLE;
          compatibleType = Type.INT;
        }

        // +, -, *, %
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.ADD, null,
            promotedType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.ADD, null,
            promotedType);
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.SUBTRACT, null,
            promotedType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.SUBTRACT, null,
            promotedType);
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.MULTIPLY, null,
            promotedType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.MULTIPLY, null,
            promotedType);
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.MOD, null,
            inputsNull ? Type.DOUBLE : compatibleType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.MOD, null,
            inputsNull ? Type.DOUBLE : compatibleType);

        // /
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.DIVIDE, null,
            Type.DOUBLE);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.DIVIDE, null,
            Type.DOUBLE);

        // div, &, |, ^ only for fixed-point types
        if ((!type1.isFixedPointType() && !type1.isNull())
            || (!type2.isFixedPointType() && !type2.isNull())) {
          continue;
        }
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.INT_DIVIDE, null,
            compatibleType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.INT_DIVIDE, null,
            compatibleType);
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.BITAND, null,
            compatibleType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.BITAND, null,
            compatibleType);
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.BITOR, null,
            compatibleType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.BITOR, null,
            compatibleType);
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.BITXOR, null,
            compatibleType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.BITXOR, null,
            compatibleType);
      }
    }

    List<Type> fixedPointTypes = new ArrayList<Type>(
        Type.getIntegerTypes());
    fixedPointTypes.add(Type.NULL);
    for (Type type: fixedPointTypes) {
      typeCastTest(null, type, false, ArithmeticExpr.Operator.BITNOT, null,
          type.isNull() ? Type.INT : type);
    }
  }

  /**
   * Test of all type casts in comparisons following mysql's casting policy.
   */
  @Test
  public void TestComparisonTypeCasts() throws AnalysisException {
    // Test all non-decimal numeric types and the null type.
    // Decimal has custom type promotion rules which are tested elsewhere.
    Type[] types = new Type[] { Type.TINYINT, Type.SMALLINT, Type.INT,
        Type.BIGINT, Type.FLOAT, Type.DOUBLE , Type.NULL };

    // test on all comparison ops
    for (BinaryPredicate.Operator cmpOp : BinaryPredicate.Operator.values()) {
      for (Type type1 : types) {
        for (Type type2 : types) {
          Type compatibleType =
              Type.getAssignmentCompatibleType(type1, type2);
          typeCastTest(type1, type2, false, null, cmpOp, compatibleType);
          typeCastTest(type1, type2, true, null, cmpOp, compatibleType);
        }
      }
    }
  }

  /**
   * Generate an expr of the form "<type1> <arithmeticOp | cmpOp> <type2>"
   * and make sure that the expr has the correct type (opType for arithmetic
   * ops or bool for comparisons) and that both operands are of type 'opType'.
   * @throws AnalysisException
   */
  private void typeCastTest(Type type1, Type type2,
      boolean op1IsLiteral, ArithmeticExpr.Operator arithmeticOp,
      BinaryPredicate.Operator cmpOp, Type opType) throws AnalysisException {
    Preconditions.checkState((arithmeticOp == null) != (cmpOp == null));
    boolean arithmeticMode = arithmeticOp != null;
    String op1 = "";
    if (type1 != null) {
      if (op1IsLiteral) {
        op1 = typeToLiteralValue_.get(type1);
      } else {
        op1 = TestSchemaUtils.getAllTypesColumn(type1);
      }
    }
    String op2 = TestSchemaUtils.getAllTypesColumn(type2);
    String queryStr = null;
    if (arithmeticMode) {
      queryStr = "select " + op1 + " " + arithmeticOp.toString() + " " + op2 +
          " AS a from functional.alltypes";
    } else {
      queryStr = "select int_col from functional.alltypes " +
          "where " + op1 + " " + cmpOp.toString() + " " + op2;
    }
    SelectStmt select = (SelectStmt) AnalyzesOk(queryStr);
    Expr expr = null;
    if (arithmeticMode) {
      ArrayList<Expr> selectListExprs = select.getResultExprs();
      assertNotNull(selectListExprs);
      assertEquals(selectListExprs.size(), 1);
      // check the first expr in select list
      expr = selectListExprs.get(0);
      assert(opType.equals(expr.getType()));
    } else {
      // check the where clause
      expr = select.getWhereClause();
      if (!expr.getType().isNull()) {
        assertEquals(PrimitiveType.BOOLEAN, expr.getType().getPrimitiveType());
      }
    }
    checkCasts(expr);
    // The children's types must be NULL or equal to the requested opType.
    Type child1Type = expr.getChild(0).getType();
    Type child2Type = type1 == null ? null : expr.getChild(1).getType();
    Assert.assertTrue("opType= " + opType + " child1Type=" + child1Type,
        opType.equals(child1Type) || opType.isNull() || child1Type.isNull());
    if (type1 != null) {
      Assert.assertTrue("opType= " + opType + " child2Type=" + child2Type,
          opType.equals(child2Type) || opType.isNull() || child2Type.isNull());
    }
  }

  private void checkReturnType(String stmt, Type resultType) {
    SelectStmt select = (SelectStmt) AnalyzesOk(stmt);
    ArrayList<Expr> selectListExprs = select.getResultExprs();
    assertNotNull(selectListExprs);
    assertEquals(selectListExprs.size(), 1);
    // check the first expr in select list
    Expr expr = selectListExprs.get(0);
    assertEquals("Expected: " + resultType + " != " + expr.getType(),
        resultType, expr.getType());
  }

  @Test
  public void TestNumericLiteralTypeResolution() throws AnalysisException {
    checkReturnType("select 1", Type.TINYINT);
    checkReturnType("select 1.1", ScalarType.createDecimalType(2,1));
    checkReturnType("select 01.1", ScalarType.createDecimalType(2,1));
    checkReturnType("select 1 + 1.1", Type.DOUBLE);
    checkReturnType("select 0.23 + 1.1", ScalarType.createDecimalType(4,2));

    checkReturnType("select float_col + float_col from functional.alltypestiny",
        Type.DOUBLE);
    checkReturnType("select int_col + int_col from functional.alltypestiny",
        Type.BIGINT);

    // floating point + numeric literal = floating point
    checkReturnType("select float_col + 1.1 from functional.alltypestiny",
        Type.DOUBLE);
    // decimal + numeric literal = decimal
    checkReturnType("select d1 + 1.1 from functional.decimal_tbl",
        ScalarType.createDecimalType(11,1));
    // int + numeric literal = floating point
    checkReturnType("select int_col + 1.1 from functional.alltypestiny",
        Type.DOUBLE);

    // Explicitly casting the literal to a decimal will override the behavior
    checkReturnType("select int_col + cast(1.1 as decimal(2,1)) from "
        + " functional.alltypestiny", ScalarType.createDecimalType(12,1));
    checkReturnType("select float_col + cast(1.1 as decimal(2,1)) from "
        + " functional.alltypestiny", ScalarType.createDecimalType(38,9));
    checkReturnType("select float_col + cast(1.1*1.2+1.3 as decimal(2,1)) from "
        + " functional.alltypestiny", ScalarType.createDecimalType(38,9));

    // The location and complexity of the expr should not matter.
    checkReturnType("select 1.0 + float_col + 1.1 from functional.alltypestiny",
        Type.DOUBLE);
    checkReturnType("select 1.0 + 2.0 + float_col from functional.alltypestiny",
        Type.DOUBLE);
    checkReturnType("select 1.0 + 2.0 + pi() * float_col from functional.alltypestiny",
        Type.DOUBLE);
    checkReturnType("select 1.0 + d1 + 1.1 from functional.decimal_tbl",
        ScalarType.createDecimalType(12,1));
    checkReturnType("select 1.0 + 2.0 + d1 from functional.decimal_tbl",
        ScalarType.createDecimalType(11,1));
    checkReturnType("select 1.0 + 2.0 + pi()*d1 from functional.decimal_tbl",
        ScalarType.createDecimalType(38,17));

    // Test with multiple cols
    checkReturnType("select double_col + 1.23 + float_col + 1.0 " +
        " from functional.alltypestiny", Type.DOUBLE);
    checkReturnType("select double_col + 1.23 + float_col + 1.0 + int_col " +
        " + bigint_col from functional.alltypestiny", Type.DOUBLE);
    checkReturnType("select d1 + 1.23 + d2 + 1.0 " +
        " from functional.decimal_tbl", ScalarType.createDecimalType(14,2));

    // Test with slot of both decimal and non-decimal
    checkReturnType("select t1.int_col + t2.c1 from functional.alltypestiny t1 " +
        " cross join functional.decimal_tiny t2", ScalarType.createDecimalType(15,4));
    checkReturnType("select 1.1 + t1.int_col + t2.c1 from functional.alltypestiny t1 " +
        " cross join functional.decimal_tiny t2", ScalarType.createDecimalType(38,17));
  }

  /**
   * Check that:
   * - we don't implicitly cast literals (we should have simply converted the literal
   *   to the target type)
   * - we don't do redundant casts (ie, we don't cast a bigint expr to a bigint)
   */
  private void checkCasts(Expr expr) {
    if (expr instanceof CastExpr) {
      CastExpr cast = (CastExpr)expr;
      if (cast.isImplicit()) {
        Assert.assertFalse(expr.getType() + " == " + expr.getChild(0).getType(),
            expr.getType().equals(expr.getChild(0).getType()));
        Assert.assertFalse(expr.debugString(), expr.getChild(0) instanceof LiteralExpr);
      }
    }
    for (Expr child: expr.getChildren()) {
      checkCasts(child);
    }
  }

  // TODO: re-enable tests as soon as we have date-related types
  // @Test
  public void DoNotTestStringLiteralToDateCasts() throws AnalysisException {
    // positive tests are included in TestComparisonTypeCasts
    AnalysisError("select int_col from functional.alltypes where date_col = 'ABCD'",
        "Unable to parse string 'ABCD' to date");
    AnalysisError("select int_col from functional.alltypes " +
        "where date_col = 'ABCD-EF-GH'",
        "Unable to parse string 'ABCD-EF-GH' to date");
    AnalysisError("select int_col from functional.alltypes where date_col = '2006'",
        "Unable to parse string '2006' to date");
    AnalysisError("select int_col from functional.alltypes where date_col = '0.5'",
        "Unable to parse string '0.5' to date");
    AnalysisError("select int_col from functional.alltypes where " +
        "date_col = '2006-10-10 ABCD'",
        "Unable to parse string '2006-10-10 ABCD' to date");
    AnalysisError("select int_col from functional.alltypes where " +
        "date_col = '2006-10-10 12:11:05.ABC'",
        "Unable to parse string '2006-10-10 12:11:05.ABC' to date");
  }

  // TODO: generate all possible error combinations of types and operands
  @Test
  public void TestFixedPointArithmeticOps() throws AnalysisException {
    // negative tests, no floating point types allowed
    AnalysisError("select ~float_col from functional.alltypes",
        "Bitwise operations only allowed on integer types");
    AnalysisError("select float_col ^ int_col from functional.alltypes",
        "Invalid non-integer argument to operation '^'");
    AnalysisError("select float_col & int_col from functional.alltypes",
        "Invalid non-integer argument to operation '&'");
    AnalysisError("select double_col | bigint_col from functional.alltypes",
        "Invalid non-integer argument to operation '|'");
    AnalysisError("select int_col from functional.alltypes where " +
        "float_col & bool_col > 5",
        "Arithmetic operation requires numeric operands");
  }

  /**
   * We have three variants of timestamp arithmetic exprs, as in MySQL:
   * http://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html
   * (section #function_date-add)
   * 1. Non-function-call like version, e.g., 'a + interval b timeunit'
   * 2. Beginning with an interval (only for '+'), e.g., 'interval b timeunit + a'
   * 3. Function-call like version, e.g., date_add(a, interval b timeunit)
   */
  @Test
  public void TestTimestampArithmeticExpressions() {
    String[] valueTypeCols =
        new String[] {"tinyint_col", "smallint_col", "int_col", "bigint_col", "NULL"};

    // Tests all time units.
    for (TimeUnit timeUnit : TimeUnit.values()) {
      // Tests on all valid time value types (fixed points).
      for (String col : valueTypeCols) {
        // Non-function call like version.
        AnalyzesOk("select timestamp_col + interval " + col + " " + timeUnit.toString() +
            " from functional.alltypes");
        AnalyzesOk("select timestamp_col - interval " + col + " " + timeUnit.toString() +
            " from functional.alltypes");
        AnalyzesOk("select NULL - interval " + col + " " + timeUnit.toString() +
            " from functional.alltypes");
        // Reversed interval and timestamp using addition.
        AnalyzesOk("select interval " + col + " " + timeUnit.toString() +
            " + timestamp_col from functional.alltypes");
        // Function-call like version.
        AnalyzesOk("select date_add(timestamp_col, interval " + col + " " +
            timeUnit.toString() + ") from functional.alltypes");
        AnalyzesOk("select date_sub(timestamp_col, interval " + col + " " +
            timeUnit.toString() + ") from functional.alltypes");
        AnalyzesOk("select date_add(NULL, interval " + col + " " +
            timeUnit.toString() + ") from functional.alltypes");
        AnalyzesOk("select date_sub(NULL, interval " + col + " " +
            timeUnit.toString() + ") from functional.alltypes");
      }
    }

    // First operand does not return a timestamp. Non-function-call like version.
    AnalysisError("select float_col + interval 10 years from functional.alltypes",
        "Operand 'float_col' of timestamp arithmetic expression " +
        "'float_col + INTERVAL 10 years' returns type 'FLOAT'. " +
        "Expected type 'TIMESTAMP'.");
    AnalysisError("select string_col + interval 10 years from functional.alltypes",
        "Operand 'string_col' of timestamp arithmetic expression " +
        "'string_col + INTERVAL 10 years' returns type 'STRING'. " +
        "Expected type 'TIMESTAMP'.");
    // Reversed interval and timestamp using addition.
    AnalysisError("select interval 10 years + float_col from functional.alltypes",
        "Operand 'float_col' of timestamp arithmetic expression " +
        "'INTERVAL 10 years + float_col' returns type 'FLOAT'. " +
        "Expected type 'TIMESTAMP'");
    AnalysisError("select interval 10 years + string_col from functional.alltypes",
        "Operand 'string_col' of timestamp arithmetic expression " +
        "'INTERVAL 10 years + string_col' returns type 'STRING'. " +
        "Expected type 'TIMESTAMP'");
    // First operand does not return a timestamp. Function-call like version.
    AnalysisError("select date_add(float_col, interval 10 years) " +
        "from functional.alltypes",
        "Operand 'float_col' of timestamp arithmetic expression " +
        "'DATE_ADD(float_col, INTERVAL 10 years)' returns type 'FLOAT'. " +
        "Expected type 'TIMESTAMP'.");
    AnalysisError("select date_add(string_col, interval 10 years) " +
        "from functional.alltypes",
        "Operand 'string_col' of timestamp arithmetic expression " +
        "'DATE_ADD(string_col, INTERVAL 10 years)' returns type 'STRING'. " +
        "Expected type 'TIMESTAMP'.");

    // Second operand is not compatible with a fixed-point type.
    // Non-function-call like version.
    AnalysisError("select timestamp_col + interval 5.2 years from functional.alltypes",
        "Operand '5.2' of timestamp arithmetic expression " +
        "'timestamp_col + INTERVAL 5.2 years' returns type 'DECIMAL(2,1)'. " +
        "Expected an integer type.");

    // No implicit cast from STRING to integer types.
    AnalysisError("select timestamp_col + interval '10' years from functional.alltypes",
                  "Operand ''10'' of timestamp arithmetic expression 'timestamp_col + " +
                  "INTERVAL '10' years' returns type 'STRING'. " +
                  "Expected an integer type.");
    AnalysisError("select date_add(timestamp_col, interval '10' years) " +
                  "from functional.alltypes", "Operand ''10'' of timestamp arithmetic " +
                  "expression 'DATE_ADD(timestamp_col, INTERVAL '10' years)' returns " +
                  "type 'STRING'. Expected an integer type.");

    // Cast from STRING to INT.
    AnalyzesOk("select timestamp_col + interval cast('10' as int) years " +
        "from functional.alltypes");
    // Reversed interval and timestamp using addition.
    AnalysisError("select interval 5.2 years + timestamp_col from functional.alltypes",
        "Operand '5.2' of timestamp arithmetic expression " +
        "'INTERVAL 5.2 years + timestamp_col' returns type 'DECIMAL(2,1)'. " +
        "Expected an integer type.");
    // Cast from STRING to INT.
    AnalyzesOk("select interval cast('10' as int) years + timestamp_col " +
        "from functional.alltypes");
    // Second operand is not compatible with type INT. Function-call like version.
    AnalysisError("select date_add(timestamp_col, interval 5.2 years) " +
        "from functional.alltypes",
        "Operand '5.2' of timestamp arithmetic expression " +
        "'DATE_ADD(timestamp_col, INTERVAL 5.2 years)' returns type 'DECIMAL(2,1)'. " +
        "Expected an integer type.");
    // Cast from STRING to INT.
    AnalyzesOk("select date_add(timestamp_col, interval cast('10' as int) years) " +
        " from functional.alltypes");

    // Invalid time unit. Non-function-call like version.
    AnalysisError("select timestamp_col + interval 10 error from functional.alltypes",
        "Invalid time unit 'error' in timestamp arithmetic expression " +
         "'timestamp_col + INTERVAL 10 error'.");
    AnalysisError("select timestamp_col - interval 10 error from functional.alltypes",
        "Invalid time unit 'error' in timestamp arithmetic expression " +
         "'timestamp_col - INTERVAL 10 error'.");
    // Reversed interval and timestamp using addition.
    AnalysisError("select interval 10 error + timestamp_col from functional.alltypes",
        "Invalid time unit 'error' in timestamp arithmetic expression " +
        "'INTERVAL 10 error + timestamp_col'.");
    // Invalid time unit. Function-call like version.
    AnalysisError("select date_add(timestamp_col, interval 10 error) " +
        "from functional.alltypes",
        "Invalid time unit 'error' in timestamp arithmetic expression " +
        "'DATE_ADD(timestamp_col, INTERVAL 10 error)'.");
    AnalysisError("select date_sub(timestamp_col, interval 10 error) " +
        "from functional.alltypes",
        "Invalid time unit 'error' in timestamp arithmetic expression " +
        "'DATE_SUB(timestamp_col, INTERVAL 10 error)'.");
  }

  @Test
  public void TestFunctions() throws AnalysisException {
    AnalyzesOk("select pi()");
    AnalyzesOk("select sin(pi())");
    AnalyzesOk("select sin(cos(pi()))");
    AnalyzesOk("select sin(cos(tan(e())))");
    AnalysisError("select pi(*)", "Cannot pass '*' to scalar function.");
    AnalysisError("select sin(DISTINCT 1)",
        "Cannot pass 'DISTINCT' to scalar function.");
    AnalysisError("select * from functional.alltypes where pi(*) = 5",
        "Cannot pass '*' to scalar function.");

    // Call function that only accepts decimal
    AnalyzesOk("select precision(1)");
    AnalyzesOk("select precision(cast('1.1' as decimal))");
    AnalyzesOk("select scale(1.1)");
    AnalysisError("select scale('1.1')",
        "No matching function with signature: scale(STRING).");

    AnalyzesOk("select round(cast('1.1' as decimal), cast(1 as int))");
    // 1 is a tinyint, so the function is not a perfect match
    AnalyzesOk("select round(cast('1.1' as decimal), 1)");
  }

  @Test
  public void TestVarArgFunctions() throws AnalysisException {
    AnalyzesOk("select concat('a')");
    AnalyzesOk("select concat('a', 'b')");
    AnalyzesOk("select concat('a', 'b', 'c')");
    AnalyzesOk("select concat('a', 'b', 'c', 'd')");
    AnalyzesOk("select concat('a', 'b', 'c', 'd', 'e')");
    // Test different vararg type signatures for same function name.
    AnalyzesOk("select coalesce(true)");
    AnalyzesOk("select coalesce(true, false, true)");
    AnalyzesOk("select coalesce(5)");
    AnalyzesOk("select coalesce(5, 6, 7)");
    AnalyzesOk("select coalesce('a')");
    AnalyzesOk("select coalesce('a', 'b', 'c')");
    // Need at least one argument.
    AnalysisError("select concat()",
                  "No matching function with signature: concat().");
    AnalysisError("select coalesce()",
                  "No matching function with signature: coalesce().");
  }

  /**
   * Tests that functions with NULL arguments get resolved properly,
   * and that proper errors are reported when the non-null arguments
   * cannot be cast to match a signature.
   */
  @Test
  public void TestNullFunctionArguments() {
    // Test fixed arg functions using 'substring' as representative.
    AnalyzesOk("select substring(NULL, 1, 2)");
    AnalyzesOk("select substring('a', NULL, 2)");
    AnalyzesOk("select substring('a', 1, NULL)");
    AnalyzesOk("select substring(NULL, NULL, NULL)");
    // Cannot cast non-null args to match a signature.
    AnalysisError("select substring(1, NULL, NULL)",
        "No matching function with signature: " +
            "substring(TINYINT, NULL_TYPE, NULL_TYPE).");
    AnalysisError("select substring(NULL, 'a', NULL)",
        "No matching function with signature: " +
            "substring(NULL_TYPE, STRING, NULL_TYPE).");

    // Test vararg functions with 'concat' as representative.
    AnalyzesOk("select concat(NULL, 'a', 'b')");
    AnalyzesOk("select concat('a', NULL, 'b')");
    AnalyzesOk("select concat('a', 'b', NULL)");
    AnalyzesOk("select concat(NULL, NULL, NULL)");
    // Cannot cast non-null args to match a signature.
    AnalysisError("select concat(NULL, 1, 'b')",
        "No matching function with signature: concat(NULL_TYPE, TINYINT, STRING).");
    AnalysisError("select concat('a', NULL, 1)",
        "No matching function with signature: concat(STRING, NULL_TYPE, TINYINT).");
    AnalysisError("select concat(1, 'b', NULL)",
        "No matching function with signature: concat(TINYINT, STRING, NULL_TYPE).");
  }

  @Test
  public void TestCaseExpr() throws AnalysisException {
    // No case expr.
    AnalyzesOk("select case when 20 > 10 then 20 else 15 end");
    // No else.
    AnalyzesOk("select case when 20 > 10 then 20 end");
    // First when condition is a boolean slotref.
    AnalyzesOk("select case when bool_col then 20 else 15 end from functional.alltypes");
    // Requires casting then exprs.
    AnalyzesOk("select case when 20 > 10 then 20 when 1 > 2 then 1.0 else 15 end");
    // Requires casting then exprs.
    AnalyzesOk("select case when 20 > 10 then 20 when 1 > 2 then 1.0 " +
        "when 4 < 5 then 2 else 15 end");
    // First when expr doesn't return boolean.
    AnalysisError("select case when 20 then 20 when 1 > 2 then timestamp_col " +
        "when 4 < 5 then 2 else 15 end from functional.alltypes",
        "When expr '20' is not of type boolean and not castable to type boolean.");
    // Then exprs return incompatible types.
    AnalysisError("select case when 20 > 10 then 20 when 1 > 2 then timestamp_col " +
        "when 4 < 5 then 2 else 15 end from functional.alltypes",
        "Incompatible return types 'TINYINT' and 'TIMESTAMP' " +
         "of exprs '20' and 'timestamp_col'.");

    // With case expr.
    AnalyzesOk("select case int_col when 20 then 30 else 15 end " +
        "from functional.alltypes");
    // No else.
    AnalyzesOk("select case int_col when 20 then 30 end " +
        "from functional.alltypes");
    // Requires casting case expr.
    AnalyzesOk("select case int_col when bigint_col then 30 else 15 end " +
        "from functional.alltypes");
    // Requires casting when expr.
    AnalyzesOk("select case bigint_col when int_col then 30 else 15 end " +
        "from functional.alltypes");
    // Requires multiple casts.
    AnalyzesOk("select case bigint_col when int_col then 30 " +
        "when double_col then 1.0 else 15 end from functional.alltypes");
    // Type of case expr is incompatible with first when expr.
    AnalysisError("select case bigint_col when timestamp_col then 30 " +
        "when double_col then 1.0 else 15 end from functional.alltypes",
        "Incompatible return types 'BIGINT' and 'TIMESTAMP' " +
        "of exprs 'bigint_col' and 'timestamp_col'.");
    // Then exprs return incompatible types.
    AnalysisError("select case bigint_col when int_col then 30 " +
        "when double_col then timestamp_col else 15 end from functional.alltypes",
        "Incompatible return types 'TINYINT' and 'TIMESTAMP' " +
         "of exprs '30' and 'timestamp_col'.");

    // Test different type classes (all types are tested in BE tests).
    AnalyzesOk("select case when true then 1 end");
    AnalyzesOk("select case when true then 1.0 end");
    AnalyzesOk("select case when true then 'abc' end");
    AnalyzesOk("select case when true then cast('2011-01-01 09:01:01' " +
        "as timestamp) end");
    // Test NULLs.
    AnalyzesOk("select case NULL when 1 then 2 else 3 end");
    AnalyzesOk("select case 1 when NULL then 2 else 3 end");
    AnalyzesOk("select case 1 when 2 then NULL else 3 end");
    AnalyzesOk("select case 1 when 2 then 3 else NULL end");
    AnalyzesOk("select case NULL when NULL then NULL else NULL end");
  }

  @Test
  public void TestConditionalExprs() {
    // Test IF conditional expr.
    AnalyzesOk("select if(true, false, false)");
    AnalyzesOk("select if(1 != 2, false, false)");
    AnalyzesOk("select if(bool_col, false, true) from functional.alltypes");
    AnalyzesOk("select if(bool_col, int_col, double_col) from functional.alltypes");
    // Test NULLs.
    AnalyzesOk("select if(NULL, false, true) from functional.alltypes");
    AnalyzesOk("select if(bool_col, NULL, true) from functional.alltypes");
    AnalyzesOk("select if(bool_col, false, NULL) from functional.alltypes");
    AnalyzesOk("select if(NULL, NULL, NULL) from functional.alltypes");

    // if() only accepts three arguments
    AnalysisError("select if(true, false, true, true)",
        "No matching function with signature: if(BOOLEAN, BOOLEAN, BOOLEAN, " +
        "BOOLEAN).");
    AnalysisError("select if(true, false)",
        "No matching function with signature: if(BOOLEAN, BOOLEAN).");
    AnalysisError("select if(false)",
        "No matching function with signature: if(BOOLEAN).");

    // Test IsNull() conditional function.
    for (PrimitiveType t: PrimitiveType.values()) {
      String literal = typeToLiteralValue_.get(t);
      AnalyzesOk(String.format("select isnull(%s, %s)", literal, literal));
      AnalyzesOk(String.format("select isnull(%s, NULL)", literal));
      AnalyzesOk(String.format("select isnull(NULL, %s)", literal));
    }
    // IsNull() requires two arguments.
    AnalysisError("select isnull(1)",
        "No matching function with signature: isnull(TINYINT).");
    AnalysisError("select isnull(1, 2, 3)",
        "No matching function with signature: isnull(TINYINT, TINYINT, TINYINT).");
    // Incompatible types.
    AnalysisError("select isnull('a', true)",
        "No matching function with signature: isnull(STRING, BOOLEAN).");
  }

  @Test
  public void TestUdfs() {
    HdfsUri dummyUri = new HdfsUri("");

    AnalysisError("select udf()", "default.udf() unknown");
    AnalysisError("select functional.udf()", "functional.udf() unknown");
    AnalysisError("select udf(1)", "default.udf() unknown");

    // Add a udf default.udf(), default.udf(int), default.udf(string...),
    // default.udf(int, string...) and functional.udf(double)
    catalog_.addFunction(new ScalarFunction(new FunctionName("default", "udf"),
        new ArrayList<Type>(), Type.INT, dummyUri, null, null, null));
    catalog_.addFunction(new ScalarFunction(new FunctionName("default", "udf"),
        Lists.<Type>newArrayList(Type.INT),
        Type.INT, dummyUri, null, null, null));
    ScalarFunction varArgsUdf1 = new ScalarFunction(new FunctionName("default", "udf"),
        Lists.<Type>newArrayList(Type.STRING),
        Type.INT, dummyUri, null, null, null);
    varArgsUdf1.setHasVarArgs(true);
    catalog_.addFunction(varArgsUdf1);
    ScalarFunction varArgsUdf2 = new ScalarFunction(new FunctionName("default", "udf"),
        Lists.<Type>newArrayList(
            Type.INT, Type.STRING),
        Type.INT, dummyUri, null, null, null);
    varArgsUdf2.setHasVarArgs(true);
    catalog_.addFunction(varArgsUdf2);
    ScalarFunction udf = new ScalarFunction(new FunctionName("functional", "udf"),
        Lists.<Type>newArrayList(Type.DOUBLE),
        Type.INT, dummyUri, null, null, null);
    catalog_.addFunction(udf);

    AnalyzesOk("select udf()");
    AnalyzesOk("select default.udf()");
    AnalyzesOk("select udf(1)");
    AnalyzesOk("select udf(cast (1.1 as INT))");
    AnalyzesOk("select udf(cast(1.1 as TINYINT))");

    // Var args
    AnalyzesOk("select udf('a')");
    AnalyzesOk("select udf('a', 'b')");
    AnalyzesOk("select udf('a', 'b', 'c')");
    AnalysisError("select udf(1, 1)",
        "No matching function with signature: default.udf(TINYINT, TINYINT).");
    AnalyzesOk("select udf(1, 'a')");
    AnalyzesOk("select udf(1, 'a', 'b')");
    AnalyzesOk("select udf(1, 'a', 'b', 'c')");
    AnalysisError("select udf(1, 'a', 2)",
        "No matching function with signature: default.udf(TINYINT, STRING, TINYINT).");

    AnalysisError("select udf(1.1)",
        "No matching function with signature: default.udf(DECIMAL(2,1))");

    AnalyzesOk("select functional.udf(1.1)");
    AnalysisError("select functional.udf('Hello')",
        "No matching function with signature: functional.udf(STRING).");

    AnalysisError("select udf(1, 2)",
         "No matching function with signature: default.udf(TINYINT, TINYINT).");
    catalog_.removeFunction(udf);
  }

  @Test
  public void TestExprChildLimit() {
    // Test IN predicate.
    StringBuilder inPredStr = new StringBuilder("select 1 IN(");
    for (int i = 0; i < Expr.EXPR_CHILDREN_LIMIT - 1; ++i) {
      inPredStr.append(i);
      if (i + 1 != Expr.EXPR_CHILDREN_LIMIT - 1) inPredStr.append(", ");
    }
    AnalyzesOk(inPredStr.toString() + ")");
    inPredStr.append(", " + 1234);
    AnalysisError(inPredStr.toString() + ")",
        String.format("Exceeded the maximum number of child expressions (%s).\n" +
        "Expression has %s children",  Expr.EXPR_CHILDREN_LIMIT,
        Expr.EXPR_CHILDREN_LIMIT + 1));

    // Test CASE expr.
    StringBuilder caseExprStr = new StringBuilder("select case");
    for (int i = 0; i < Expr.EXPR_CHILDREN_LIMIT/2; ++i) {
      caseExprStr.append(" when true then 1");
    }
    AnalyzesOk(caseExprStr.toString() + " end");
    caseExprStr.append(" when true then 1");
    AnalysisError(caseExprStr.toString() + " end",
        String.format("Exceeded the maximum number of child expressions (%s).\n" +
        "Expression has %s children", Expr.EXPR_CHILDREN_LIMIT,
        Expr.EXPR_CHILDREN_LIMIT + 2));
  }

  @Test
  public void TestExprDepthLimit() {
    // Compound predicates.
    testInfixExprDepthLimit("select true", " and false");
    testInfixExprDepthLimit("select true", " or false");

    // Arithmetic expr. Use a bigint value to avoid casts that make reasoning about the
    // expr depth more difficult.
    testInfixExprDepthLimit("select " + String.valueOf(Long.MAX_VALUE),
        " + " + String.valueOf(Long.MAX_VALUE));

    // Function-call expr.
    testFuncExprDepthLimit("lower(", "'abc'", ")");

    // UDF.
    ScalarFunction udf = new ScalarFunction(new FunctionName("default", "udf"),
        Lists.<Type>newArrayList(Type.INT),
        Type.INT, new HdfsUri(""), null, null, null);
    catalog_.addFunction(udf);
    try {
      testFuncExprDepthLimit("udf(", "1", ")");
    } finally {
      catalog_.removeFunction(udf);
    }

    // Timestamp arithmetic expr.
    testFuncExprDepthLimit("date_add(", "now()", ", interval 1 day)");

    // Casts.
    testFuncExprDepthLimit("cast(", "1", " as int)");
  }

  // Verifies the resulting expr decimal type is exptectedType
  private void testDecimalExpr(String expr, Type expectedType) {
    SelectStmt selectStmt = (SelectStmt) AnalyzesOk("select " + expr);
    Expr root = selectStmt.resultExprs_.get(0);
    Type actualType = root.getType();
    Assert.assertTrue(
        "Expr: " + expr + " Expected: " + expectedType + " Actual: " + actualType,
        expectedType.equals(actualType));
  }

  @Test
  public void TestDecimalArithmetic() {
    String decimal_10_0 = "cast(1 as decimal(10,0))";
    String decimal_5_5 = "cast(1 as decimal(5, 5))";
    String decimal_38_34 = "cast(1 as decimal(38, 34))";

    testDecimalExpr(decimal_10_0, ScalarType.createDecimalType(10, 0));
    testDecimalExpr(decimal_5_5, ScalarType.createDecimalType(5, 5));
    testDecimalExpr(decimal_38_34, ScalarType.createDecimalType(38, 34));

    // Test arithmetic operations.
    testDecimalExpr(decimal_10_0 + " + " + decimal_10_0,
        ScalarType.createDecimalType(11, 0));
    testDecimalExpr(decimal_10_0 + " - " + decimal_10_0,
        ScalarType.createDecimalType(11, 0));
    testDecimalExpr(decimal_10_0 + " * " + decimal_10_0,
        ScalarType.createDecimalType(20, 0));
    testDecimalExpr(decimal_10_0 + " / " + decimal_10_0,
        ScalarType.createDecimalType(21, 11));
    testDecimalExpr(decimal_10_0 + " % " + decimal_10_0,
        ScalarType.createDecimalType(10, 0));

    testDecimalExpr(decimal_10_0 + " + " + decimal_5_5,
        ScalarType.createDecimalType(16, 5));
    testDecimalExpr(decimal_10_0 + " - " + decimal_5_5,
        ScalarType.createDecimalType(16, 5));
    testDecimalExpr(decimal_10_0 + " * " + decimal_5_5,
        ScalarType.createDecimalType(15, 5));
    testDecimalExpr(decimal_10_0 + " / " + decimal_5_5,
        ScalarType.createDecimalType(21, 6));
    testDecimalExpr(decimal_10_0 + " % " + decimal_5_5,
            ScalarType.createDecimalType(5, 5));

    testDecimalExpr(decimal_5_5 + " + " + decimal_10_0,
        ScalarType.createDecimalType(16, 5));
    testDecimalExpr(decimal_5_5 + " - " + decimal_10_0,
        ScalarType.createDecimalType(16, 5));
    testDecimalExpr(decimal_5_5 + " * " + decimal_10_0,
        ScalarType.createDecimalType(15, 5));
    testDecimalExpr(decimal_5_5 + " / " + decimal_10_0,
        ScalarType.createDecimalType(16, 16));
    testDecimalExpr(decimal_5_5 + " % " + decimal_10_0,
        ScalarType.createDecimalType(5, 5));

    // Test some overflow cases.
    testDecimalExpr(decimal_10_0 + " + " + decimal_38_34,
        ScalarType.createDecimalType(38, 34));
    testDecimalExpr(decimal_10_0 + " - " + decimal_38_34,
        ScalarType.createDecimalType(38, 34));
    testDecimalExpr(decimal_10_0 + " * " + decimal_38_34,
        ScalarType.createDecimalType(38, 34));
    testDecimalExpr(decimal_10_0 + " / " + decimal_38_34,
        ScalarType.createDecimalType(38, 34));
    testDecimalExpr(decimal_10_0 + " % " + decimal_38_34,
        ScalarType.createDecimalType(38, 34));

    testDecimalExpr(decimal_38_34 + " + " + decimal_5_5,
        ScalarType.createDecimalType(38, 34));
    testDecimalExpr(decimal_38_34 + " - " + decimal_5_5,
        ScalarType.createDecimalType(38, 34));
    testDecimalExpr(decimal_38_34 + " * " + decimal_5_5,
        ScalarType.createDecimalType(38, 38));
    testDecimalExpr(decimal_38_34 + " / " + decimal_5_5,
        ScalarType.createDecimalType(38, 34));
    testDecimalExpr(decimal_38_34 + " % " + decimal_5_5,
        ScalarType.createDecimalType(34, 34));

    testDecimalExpr(decimal_10_0 + " + " + decimal_10_0 + " + " + decimal_10_0,
        ScalarType.createDecimalType(12, 0));
    testDecimalExpr(decimal_10_0 + " - " + decimal_10_0 + " * " + decimal_10_0,
        ScalarType.createDecimalType(21, 0));
    testDecimalExpr(decimal_10_0 + " / " + decimal_10_0 + " / " + decimal_10_0,
        ScalarType.createDecimalType(32, 22));
    testDecimalExpr(decimal_10_0 + " % " + decimal_10_0 + " + " + decimal_10_0,
        ScalarType.createDecimalType(11, 0));

    // Operators between decimal and numeric types should be supported. The int
    // should be cast to the appropriate decimal (e.g. tinyint -> decimal(3,0)).
    testDecimalExpr(decimal_10_0 + " + cast(1 as tinyint)",
        ScalarType.createDecimalType(11, 0));
    testDecimalExpr(decimal_10_0 + " + cast(1 as smallint)",
        ScalarType.createDecimalType(11, 0));
    testDecimalExpr(decimal_10_0 + " + cast(1 as int)",
        ScalarType.createDecimalType(11, 0));
    testDecimalExpr(decimal_10_0 + " + cast(1 as bigint)",
        ScalarType.createDecimalType(20, 0));
    testDecimalExpr(decimal_10_0 + " + cast(1 as float)",
        ScalarType.createDecimalType(38, 9));
    testDecimalExpr(decimal_10_0 + " + cast(1 as double)",
        ScalarType.createDecimalType(38, 17));

    testDecimalExpr(decimal_5_5 + " + cast(1 as tinyint)",
        ScalarType.createDecimalType(9, 5));
    testDecimalExpr(decimal_5_5 + " - cast(1 as smallint)",
        ScalarType.createDecimalType(11, 5));
    testDecimalExpr(decimal_5_5 + " * cast(1 as int)",
        ScalarType.createDecimalType(15, 5));
    testDecimalExpr(decimal_5_5 + " % cast(1 as bigint)",
        ScalarType.createDecimalType(5, 5));
    testDecimalExpr(decimal_5_5 + " / cast(1 as float)",
        ScalarType.createDecimalType(38, 9));
    testDecimalExpr(decimal_5_5 + " + cast(1 as double)",
        ScalarType.createDecimalType(38, 17));

    AnalyzesOk("select " + decimal_5_5 + " = cast(1 as tinyint)");
    AnalyzesOk("select " + decimal_5_5 + " != cast(1 as smallint)");
    AnalyzesOk("select " + decimal_5_5 + " > cast(1 as int)");
    AnalyzesOk("select " + decimal_5_5 + " < cast(1 as bigint)");
    AnalyzesOk("select " + decimal_5_5 + " >= cast(1 as float)");
    AnalyzesOk("select " + decimal_5_5 + " <= cast(1 as double)");

    AnalysisError("select " + decimal_5_5 + " + 'abcd'",
        "Arithmetic operation requires numeric operands: "
        + "CAST(1 AS DECIMAL(5,5)) + 'abcd'");
    AnalysisError("select " + decimal_5_5 + " + 'cast(1 as timestamp)'",
        "Arithmetic operation requires numeric operands: "
        + "CAST(1 AS DECIMAL(5,5)) + 'cast(1 as timestamp)'");

    AnalysisError("select " + decimal_5_5 + " = 'abcd'",
        "operands of type DECIMAL(5,5) and STRING are not comparable: " +
        "CAST(1 AS DECIMAL(5,5)) = 'abcd'");
    AnalysisError("select " + decimal_5_5 + " > 'cast(1 as timestamp)'",
        "operands of type DECIMAL(5,5) and STRING are not comparable: "
        + "CAST(1 AS DECIMAL(5,5)) > 'cast(1 as timestamp)'");
  }

  @Test
  public void TestDecimalOperators() throws AnalysisException {
    AnalyzesOk("select d2 % d5 from functional.decimal_tbl");

    AnalyzesOk("select d1 from functional.decimal_tbl");
    AnalyzesOk("select cast(d2 as decimal(1)) from functional.decimal_tbl");
    AnalyzesOk("select d3 + d4 from functional.decimal_tbl");
    AnalyzesOk("select d5 - d1 from functional.decimal_tbl");
    AnalyzesOk("select d2 * d2 from functional.decimal_tbl");
    AnalyzesOk("select d4 / d1 from functional.decimal_tbl");
    AnalyzesOk("select d2 % d5 from functional.decimal_tbl");

    AnalysisError("select d1 & d1 from functional.decimal_tbl",
        "Invalid non-integer argument to operation '&': d1 & d1");
    AnalysisError("select d1 | d1 from functional.decimal_tbl",
        "Invalid non-integer argument to operation '|': d1 | d1");
    AnalysisError("select d1 ^ d1 from functional.decimal_tbl",
        "Invalid non-integer argument to operation '^': d1 ^ d1");
    AnalysisError("select ~d1 from functional.decimal_tbl",
        "Bitwise operations only allowed on integer types: ~d1");

    AnalyzesOk("select d3 = d4 from functional.decimal_tbl");
    AnalyzesOk("select d5 != d1 from functional.decimal_tbl");
    AnalyzesOk("select d2 > d2 from functional.decimal_tbl");
    AnalyzesOk("select d4 >= d1 from functional.decimal_tbl");
    AnalyzesOk("select d2 < d5 from functional.decimal_tbl");
    AnalyzesOk("select d2 <= d5 from functional.decimal_tbl");
  }

  @Test
  public void TestDecimalCast() throws AnalysisException {
    AnalyzesOk("select cast(1 as decimal)");
    AnalyzesOk("select cast(1 as decimal(1))");
    AnalyzesOk("select cast(1 as decimal(38))");
    AnalyzesOk("select cast(1 as decimal(1, 0))");
    AnalyzesOk("select cast(1 as decimal(10, 5))");
    AnalyzesOk("select cast(1 as decimal(38, 0))");
    AnalyzesOk("select cast(1 as decimal(38, 38))");

    AnalysisError("select cast(1 as decimal(0))",
        "Decimal precision must be greater than 0.");
    AnalysisError("select cast(1 as decimal(39))",
        "Decimal precision must be <= 38.");
    AnalysisError("select cast(1 as decimal(1, 2))",
        "Decimal scale (2) must be <= precision (1).");
  }

  @Test
  public void TestDecimalFunctions() throws AnalysisException {
    AnalyzesOk("select abs(cast(1 as decimal))");
    AnalyzesOk("select abs(cast(-1.1 as decimal(10,3)))");

    AnalyzesOk("select floor(cast(-1.1 as decimal(10,3)))");
    AnalyzesOk("select ceil(cast(1.123 as decimal(10,3)))");

    AnalyzesOk("select round(cast(1.123 as decimal(10,3)))");
    AnalyzesOk("select round(cast(1.123 as decimal(10,3)), 0)");
    AnalyzesOk("select round(cast(1.123 as decimal(10,3)), 2)");
    AnalyzesOk("select round(cast(1.123 as decimal(10,3)), 5)");
    AnalyzesOk("select round(cast(1.123 as decimal(10,3)), -2)");

    AnalyzesOk("select truncate(cast(1.123 as decimal(10,3)))");
    AnalyzesOk("select truncate(cast(1.123 as decimal(10,3)), 0)");
    AnalyzesOk("select truncate(cast(1.123 as decimal(10,3)), 2)");
    AnalyzesOk("select truncate(cast(1.123 as decimal(10,3)), 5)");
    AnalyzesOk("select truncate(cast(1.123 as decimal(10,3)), -1)");

    AnalysisError("select round(cast(1.123 as decimal(10,3)), 5.1)",
        "No matching function with signature: round(DECIMAL(10,3), DECIMAL(2,1))");
    AnalysisError("select round(cast(1.123 as decimal(30,20)), 40)",
        "Cannot round/truncate to scales greater than 38.");
    AnalysisError("select truncate(cast(1.123 as decimal(10,3)), 40)",
        "Cannot round/truncate to scales greater than 38.");
    AnalysisError("select round(cast(1.123 as decimal(10,3)), NULL)",
        "round() cannot be called with a NULL second argument.");

    // This has 39 digits and can only be represented as a DOUBLE.
    AnalysisError("select precision(999999999999999999999999999999999999999.)",
        "No matching function with signature: precision(DOUBLE).");

    AnalysisError("select precision(cast(1 as float))",
        "No matching function with signature: precision(FLOAT)");

    AnalysisError("select precision(NULL)",
        "Cannot resolve DECIMAL precision and scale from NULL type.");
    AnalysisError("select scale(NULL)",
        "Cannot resolve DECIMAL precision and scale from NULL type.");

    testDecimalExpr("round(1.23)", ScalarType.createDecimalType(2, 0));
    testDecimalExpr("round(1.23, 1)", ScalarType.createDecimalType(3, 1));
    testDecimalExpr("round(1.23, 0)", ScalarType.createDecimalType(2, 0));
    testDecimalExpr("round(1.23, 3)", ScalarType.createDecimalType(4, 3));
    testDecimalExpr("round(1.23, -1)", ScalarType.createDecimalType(2, 0));
    testDecimalExpr("round(1.23, -2)", ScalarType.createDecimalType(2, 0));
    testDecimalExpr("round(cast(1.23 as decimal(3,2)), -2)",
        ScalarType.createDecimalType(2, 0));

    testDecimalExpr("ceil(123.45)", ScalarType.createDecimalType(4, 0));
    testDecimalExpr("floor(12.345)", ScalarType.createDecimalType(3, 0));

    testDecimalExpr("truncate(1.23)", ScalarType.createDecimalType(1, 0));
    testDecimalExpr("truncate(1.23, 1)", ScalarType.createDecimalType(2, 1));
    testDecimalExpr("truncate(1.23, 0)", ScalarType.createDecimalType(1, 0));
    testDecimalExpr("truncate(1.23, 3)", ScalarType.createDecimalType(4, 3));
    testDecimalExpr("truncate(1.23, -1)", ScalarType.createDecimalType(1, 0));
    testDecimalExpr("truncate(1.23, -2)", ScalarType.createDecimalType(1, 0));
  }

  /**
   * Test expr depth limit of operators in infix notation, e.g., 1 + 1.
   * Generates test exprs using the pattern: prefix + repeatSuffix*
   */
  private void testInfixExprDepthLimit(String prefix, String repeatSuffix) {
    StringBuilder exprStr = new StringBuilder(prefix);
    for (int i = 0; i < Expr.EXPR_DEPTH_LIMIT - 1; ++i) {
      exprStr.append(repeatSuffix);
    }
    AnalyzesOk(exprStr.toString());
    exprStr.append(repeatSuffix);
    AnalysisError(exprStr.toString(),
        String.format("Exceeded the maximum depth of an expression tree (%s).",
        Expr.EXPR_DEPTH_LIMIT));

    // Test 10x the safe depth (already at 1x, append 9x).
    for (int i = 0; i < Expr.EXPR_DEPTH_LIMIT * 9; ++i) {
      exprStr.append(repeatSuffix);
    }
    AnalysisError(exprStr.toString(),
        String.format("Exceeded the maximum depth of an expression tree (%s).",
        Expr.EXPR_DEPTH_LIMIT));
  }

  /**
   * Test expr depth limit of function-like operations, e.g., f(a).
   * Generates test exprs using the pattern: openFunc* baseArg closeFunc*
   */
  private void testFuncExprDepthLimit(String openFunc, String baseArg,
      String closeFunc) {
    AnalyzesOk("select " + getNestedFuncExpr(openFunc, baseArg, closeFunc,
        Expr.EXPR_DEPTH_LIMIT - 1));
    AnalysisError("select " + getNestedFuncExpr(openFunc, baseArg, closeFunc,
        Expr.EXPR_DEPTH_LIMIT),
        String.format("Exceeded the maximum depth of an expression tree (%s).",
        Expr.EXPR_DEPTH_LIMIT));
    // Test 10x the safe depth.
    AnalysisError("select " + getNestedFuncExpr(openFunc, baseArg, closeFunc,
        Expr.EXPR_DEPTH_LIMIT * 10),
        String.format("Exceeded the maximum depth of an expression tree (%s).",
        Expr.EXPR_DEPTH_LIMIT));
  }

  /**
   * Generates a string: openFunc* baseArg closeFunc*,
   * where * repetition of exactly numFuncs times.
   */
  private String getNestedFuncExpr(String openFunc, String baseArg,
      String closeFunc, int numFuncs) {
    StringBuilder exprStr = new StringBuilder();
    for (int i = 0; i < numFuncs; ++i) {
      exprStr.append(openFunc);
    }
    exprStr.append(baseArg);
    for (int i = 0; i < numFuncs; ++i) {
      exprStr.append(closeFunc);
    }
    return exprStr.toString();
  }
}
