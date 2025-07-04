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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.impala.analysis.TimestampArithmeticExpr.TimeUnit;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TestSchemaUtils;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.TypeCompatibility;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Joiner;
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
      "Numeric literal '1.7976931348623157E+3081' exceeds maximum range of DOUBLE.");
    AnalysisError(String.format("select %s1", Double.toString(Double.MIN_VALUE)),
      "Numeric literal '4.9E-3241' underflows minimum resolution of DOUBLE.");

    // Test edge cases near the upper limits of decimal precision - 38 digits.
    // Integer literal.
    checkDecimalReturnType("select 12345678901234567890123456789012345678",
        ScalarType.createDecimalType(38, 0));
    // Decimal point at front.
    testNumericLiteral("0.99999999999999999999999999999999999999",
        ScalarType.createDecimalType(38,38));
    // Decimal point at back.
    testNumericLiteral("99999999999999999999999999999999999999.",
        ScalarType.createDecimalType(38,0));
    // Negative values.
    testNumericLiteral("-0.99999999999999999999999999999999999999",
        ScalarType.createDecimalType(38,38));
    testNumericLiteral("-99999999999999999999999999999999999999.",
        ScalarType.createDecimalType(38,0));
    // Decimal point in middle.
    testNumericLiteral("999999999999999999999.99999999999999999",
        ScalarType.createDecimalType(38,17));
    testNumericLiteral("-999999999999999999.99999999999999999999",
        ScalarType.createDecimalType(38,20));

    // Test literals that do not fit into a decimal and are treated as DOUBLE.
    // Edge cases that have 39 digits.
    testNumericLiteral("123456789012345678901234567890123456789", Type.DOUBLE);
    testNumericLiteral("123456789012345678901234567890123456789.", Type.DOUBLE);
    testNumericLiteral("1234567890123.45678901234567890123456789", Type.DOUBLE);
    testNumericLiteral(".123456789012345678901234567890123456789", Type.DOUBLE);

    // Really huge literals are always interpreted as DOUBLE.
    testNumericLiteral("12345678901234567890123456789012345678123455555555555555555555"
        + "55559999999999999999999999999999999999999999999999999999999", Type.DOUBLE);
    testNumericLiteral("1234567890123456789012345678901234567812.345555555555555555555"
        + "555559999999999999999999999999999999999999999999999999999999", Type.DOUBLE);
    testNumericLiteral(".1234567890123456789012345678901234567812345555555555555555555"
        + "555559999999999999999999999999999999999999999999999999999999", Type.DOUBLE);
  }

  /**
   * Test scientific notation typing. Values in scientific notation are expanded to
   * decimal or integer literals and the same typing rules then apply.
   */
  @Test
  public void TestScientificNumericLiterals() {
    checkDecimalReturnType("select 1e9", Type.INT);
    checkDecimalReturnType("select 1.123456789e9", Type.INT);
    checkDecimalReturnType("select 1.1234567891e9",
        ScalarType.createDecimalType(11, 1));
    checkDecimalReturnType("select 1.1234567891e6",
        ScalarType.createDecimalType(11, 4));
    checkDecimalReturnType("select 9e9", Type.BIGINT);
    checkDecimalReturnType("select 1e-2", ScalarType.createDecimalType(2, 2));
    checkDecimalReturnType("select 1.123e-2", ScalarType.createDecimalType(5, 5));

    // Scientific notation - edge cases between decimal and double.
    checkDecimalReturnType("select 1e37", ScalarType.createDecimalType(38, 0));
    checkDecimalReturnType("select 1.23456e37", ScalarType.createDecimalType(38, 0));
    checkDecimalReturnType("select 1e38", Type.DOUBLE);
    checkDecimalReturnType("select 1.23456e38", Type.DOUBLE);
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
    AnalyzesOk("select cast (cast ('1970-10-10' as date) as timestamp)");
    AnalyzesOk("select cast (date '1970-10-10' as timestamp)");
  }

  @Test
  public void testDateValueExprs() throws AnalysisException {
    AnalyzesOk("select DATE '1970-10-10'");
    AnalyzesOk("select cast ('1970-10-10' as date)");
    AnalyzesOk("select cast (cast ('1970-10-10 10:00:00.123' as timestamp) as date)");
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
    for (String operator: new String[]{"<=>", "IS DISTINCT FROM",
        "IS NOT DISTINCT FROM", "<", ">", ">=", "<=", "!=", "=", "<>"}) {
      // Operator can compare numeric values (literals, casts, and columns), even ones of
      // different types.
      List<String> numericValues =
          new ArrayList<String>(Arrays.asList("0", "1", "1.1", "-7", "-7.7", "1.2e99",
              "false", "1234567890123456789012345678901234567890", "tinyint_col",
              "smallint_col", "int_col", "bigint_col", "float_col", "double_col"));
      String numericTypes[] = new String[] {
          "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL"};
      for (String numericType : numericTypes) {
        numericValues.add("cast(NULL as " + numericType + ")");
      }
      for (String lhs : numericValues) {
        for (String rhs : numericValues) {
          AnalyzesOk("select * from functional.alltypes where " + lhs + " "
              + operator + " " + rhs);
        }
      }

      // Operator can compare identical non-numeric types
      for (String operand :
          new String[] {"bool_col", "string_col", "timestamp_col", "NULL"}) {
        AnalyzesOk("select * from functional.alltypes where " + operand + " "
            + operator + " " + operand);
        AnalyzesOk("select * from functional.alltypes where " + operand + " "
            + operator + " NULL");
        AnalyzesOk(
            "select * from functional.alltypes where NULL " + operator + " " + operand);
      }

      // Operator can compare date columns
      AnalyzesOk("select * from functional.date_tbl where date_col " + operator
          + " date_col");
      AnalyzesOk("select * from functional.date_tbl where date_col " + operator
          + " NULL");
      AnalyzesOk("select * from functional.date_tbl where NULL " + operator
          + " date_col");
      // Operator can compare date column and timestamp column
      AnalyzesOk("select * from functional.date_tbl, functional.alltypes where date_col "
          + operator + " timestamp_col");
      AnalyzesOk("select * from functional.date_tbl, functional.alltypes where "
          + "timestamp_col " + operator + " date_col");
      // Operator can compare date column and string literals. This works beacuse the
      // string literal is implicitly converted to date.
      AnalyzesOk("select * from functional.date_tbl where date_col " + operator
          + " '1993-01-21'");

      // Operator can compare string column and literals
      AnalyzesOk(
          "select * from functional.alltypes where string_col " + operator + " 'hi'");
      // Operator can compare timestamp column and literals
      AnalyzesOk("select * from functional.alltypes where timestamp_col "
          + operator + " '1993-01-21 02:00:00'");
      // Operator can compare bool column and literals
      AnalyzesOk("select * from functional.alltypes where bool_col " + operator
          + " true");
      // Operator can compare binary columns and literals.
      AnalyzesOk(
          "select * from functional.binary_tbl where binary_col " + operator +
           " cast('hi' as binary)");

      // Decimal types of different precisions and scales are comparable
      String decimalColumns[] = new String[]{"d1", "d2", "d3", "d4", "d5", "NULL"};
      for (String operand1 : decimalColumns) {
        for (String operand2 : decimalColumns) {
          AnalyzesOk("select * from functional.decimal_tbl where " + operand1 + " "
              + operator + " " + operand2);
        }
      }

      // IMPALA-7260: Check that when a numerical type gets implicitly cast to decimal,
      // we do not get an error when the type of the decimal operand is not able to fit
      // all of the digits of the non-deicmal operand.
      AnalyzesOk("select cast(1 as decimal(38,37)) " + operator + " cast(2 as bigint)");
      AnalyzesOk("select cast(1 as bigint) " + operator + " cast(2 as decimal(38,37))");
      AnalyzesOk("select cast(1 as decimal(38,1)) " + operator + " cast(2 as bigint)");
      AnalyzesOk("select cast(1 as decimal(38,37)) " + operator + " cast(2 as tinyint)");
      AnalyzesOk("select cast(1 as decimal(38,37)) " + operator + " cast(2 as double)");

      // Chars of different length are comparable
      for (int i = 1; i < 16; ++i) {
        AnalyzesOk("select cast('hi' as char(" + i + ")) " + operator +
            " 'hi'");
        AnalyzesOk("select cast('hi' as char(" + i + ")) " + operator +
            " NULL");
        for (int j = 1; j < 16; ++j) {
          AnalyzesOk("select cast('hi' as char(" + i + ")) " + operator +
              " cast('hi' as char(" + j + "))");
        }
      }

      // Binary operators do not operate on expression with incompatible types
      for (String numeric_type: new String[]{"BOOLEAN", "TINYINT", "SMALLINT", "INT",
          "BIGINT", "FLOAT", "DOUBLE", "DECIMAL(9,0)"}) {
        for (String string_type: new String[]{"STRING", "BINARY", "TIMESTAMP", "DATE"}) {
          AnalysisError("select cast(NULL as " + numeric_type + ") "
              + operator + " cast(NULL as " + string_type + ")",
              "operands of type " + numeric_type + " and " + string_type +
              " are not comparable:");
          AnalysisError("select cast(NULL as " + string_type + ") "
              + operator + " cast(NULL as " + numeric_type + ")",
              "operands of type " + string_type + " and " + numeric_type +
              " are not comparable:");
        }
      }
    }

    // invalid casts
    AnalysisError("select * from functional.alltypes where bool_col = '15'",
        "operands of type BOOLEAN and STRING are not comparable: bool_col = '15'");
    // Complex types are not comparable.
    AnalysisError("select 1 from functional.allcomplextypes where int_array_col = 1",
        "operands of type ARRAY<INT> and TINYINT are not comparable: int_array_col = 1");
    AnalysisError("select 1 from functional.allcomplextypes where int_map_col = 1",
        "operands of type MAP<STRING,INT> and TINYINT are not comparable: " +
        "int_map_col = 1");
    AnalysisError("select 1 from functional_orc_def.complextypes_structs where " +
        "tiny_struct = true",
        "operands of type STRUCT<b:BOOLEAN> and BOOLEAN are not comparable: " +
        "tiny_struct = TRUE");
    // Complex types are not comparable even if identical.
    // TODO: Reconsider this behavior. Such a comparison should ideally work,
    // but may require complex-typed SlotRefs and BE functions able to accept
    // complex-typed parameters.
    AnalysisError("select 1 from functional.allcomplextypes " +
        "where int_map_col = int_map_col",
        "operands of type MAP<STRING,INT> and MAP<STRING,INT> are not comparable: " +
        "int_map_col = int_map_col");
    AnalysisError("select * from functional.date_tbl where date_col = 15",
        "operands of type DATE and TINYINT are not comparable: date_col = 15");
    // TODO: Enable once datetime is implemented.
    // AnalysisError("select * from functional.alltypes where datetime_col = 1.0",
    // "operands are not comparable: datetime_col = 1.0");
  }


  @Test
  public void TestDecimalCasts() throws AnalysisException {
    AnalyzesOk("select cast(1.1 as boolean)");
    AnalyzesOk("select cast(1.1 as timestamp)");

    AnalysisError("select cast(1.1 as date)",
        "Invalid type cast of 1.1 from DECIMAL(2,1) to DATE");
    AnalysisError("select cast(true as decimal)",
        "Invalid type cast of TRUE from BOOLEAN to DECIMAL(9,0)");
    AnalysisError("select cast(cast(1 as timestamp) as decimal)",
        "Invalid type cast of CAST(1 AS TIMESTAMP) from TIMESTAMP to DECIMAL(9,0)");
    AnalysisError("select cast(date '1970-01-01' as decimal)",
        "Invalid type cast of DATE '1970-01-01' from DATE to DECIMAL(9,0)");
    AnalysisError("select cast(cast(\"1.1\" as binary) as decimal)",
        "Invalid type cast of CAST('1.1' AS BINARY) from BINARY to DECIMAL(9,0)");

    for (Type type: Type.getSupportedTypes()) {
      if (type.isNull() || type.isDecimal() || type.isBoolean() || type.isDateOrTimeType()
          || type.getPrimitiveType() == PrimitiveType.VARCHAR
          || type.getPrimitiveType() == PrimitiveType.CHAR
          || type.getPrimitiveType() == PrimitiveType.BINARY) {
        continue;
      }
      AnalyzesOk("select cast(1.1 as " + type + ")");
      AnalyzesOk("select cast(cast(1 as " + type + ") as decimal)");
    }

    // Casts to all other decimals are supported.
    for (int precision = 1; precision <= ScalarType.MAX_PRECISION; ++precision) {
      for (int scale = 0; scale < precision; ++scale) {
        Type t = ScalarType.createDecimalType(precision, scale);
        AnalyzesOk("select cast(1.1 as " + t.toSql() + ")");
        AnalyzesOk("select cast(cast(1 as " + t.toSql() + ") as decimal)");
      }
    }

    AnalysisError("select cast(1 as decimal(0, 1))",
        "Decimal precision must be > 0: 0");

    // IMPALA-2264: decimal is implicitly cast to lower-precision integer in edge cases.
    checkDecimalReturnType(
        "select CAST(999 AS DECIMAL(3,0))", ScalarType.createDecimalType(3,0));
    AnalysisError("insert into functional.alltypesinsert (tinyint_col, year, month) " +
        "values(CAST(999 AS DECIMAL(3,0)), 1, 1)",
        "Possible loss of precision for target table 'functional.alltypesinsert'.\n" +
        "Expression 'CAST(999 AS DECIMAL(3,0))' (type: DECIMAL(3,0)) would need to be " +
        "cast to TINYINT for column 'tinyint_col'");
  }

  /**
   * Asserts that an expression can be cast as the specified type
   */
  private void testExprCast(String literal, Type expectedType) {
    SelectStmt selectStmt = (SelectStmt) AnalyzesOk("select cast(" + literal +
        " as " + expectedType.toSql() + ")");
    Type actualType = selectStmt.resultExprs_.get(0).getType();
    Assert.assertTrue("Expected Type: " + expectedType + " Actual type: " + actualType,
        expectedType.equals(actualType));
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

    // Cast to / from VARCHAR
    AnalyzesOk("select cast('helloworld' as VARCHAR(3))");
    AnalyzesOk("select cast(cast('helloworld' as VARCHAR(3)) as string)");
    AnalyzesOk("select cast(cast('3.0' as VARCHAR(5)) as float)");
    AnalyzesOk("select NULL = cast('123' as CHAR(3))");
    AnalyzesOk("select * from functional.chars_tiny where cs = vc");
    AnalyzesOk("select * from functional.chars_tiny where vc = cs");
    AnalyzesOk("insert into functional.chars_tiny(vc) VALUES " +
        "(cast('aaabbb' as varchar(6))), (cast('cccddd' as char(6)))");
    AnalyzesOk("insert into functional.chars_tiny(vc) VALUES " +
        "(cast('aaabbb' as varchar(32))), (cast('cccddd' as char(32)))");
    AnalysisError("select now() = cast('hi' as CHAR(3))",
        "operands of type TIMESTAMP and CHAR(3) are not comparable: " +
        "now() = CAST('hi' AS CHAR(3))");
    AnalysisError("select cast(now() as DATE) = cast('hi' as CHAR(3))",
        "operands of type DATE and CHAR(3) are not comparable: " +
        "CAST(now() AS DATE) = CAST('hi' AS CHAR(3))");
    testExprCast("cast('Hello' as VARCHAR(5))", ScalarType.createVarcharType(7));
    testExprCast("cast('Hello' as VARCHAR(5))", ScalarType.createVarcharType(3));

    AnalysisError("select cast('foo' as varchar(0))",
        "Varchar size must be > 0: 0");
    AnalysisError("select cast('foo' as varchar(65536))",
        "Varchar size must be <= 65535: 65536");
    AnalysisError("select cast('foo' as char(0))",
        "Char size must be > 0: 0");
    AnalysisError("select cast('foo' as char(256))",
        "Char size must be <= 255: 256");

    testExprCast("'Hello'", ScalarType.createCharType(5));
    testExprCast("cast('Hello' as CHAR(5))", ScalarType.STRING);
    testExprCast("cast('Hello' as CHAR(5))", ScalarType.createVarcharType(7));
    testExprCast("cast('Hello' as VARCHAR(5))", ScalarType.createCharType(7));
    testExprCast("cast('Hello' as CHAR(7))", ScalarType.createVarcharType(5));
    testExprCast("cast('Hello' as VARCHAR(7))", ScalarType.createCharType(5));
    testExprCast("cast('Hello' as CHAR(5))", ScalarType.createVarcharType(5));
    testExprCast("cast('Hello' as VARCHAR(5))", ScalarType.createCharType(5));
    testExprCast("1", ScalarType.createCharType(5));

    testExprCast("cast('abcde' as char(10)) IN " +
        "(cast('abcde' as CHAR(20)), cast('abcde' as VARCHAR(10)), 'abcde')",
        ScalarType.createCharType(10));
    testExprCast("'abcde' IN " +
        "(cast('abcde' as CHAR(20)), cast('abcde' as VARCHAR(10)), 'abcde')",
        ScalarType.STRING);
    testExprCast("cast('abcde' as varchar(10)) IN " +
        "(cast('abcde' as CHAR(20)), cast('abcde' as VARCHAR(10)), 'abcde')",
        ScalarType.createVarcharType(10));

  }

  /**
   * Tests that cast(null to type) returns type for all types.
   */
  @Test
  public void TestNullCasts() throws AnalysisException {
    for (Type type: Type.getSupportedTypes()) {
       // Cannot cast to NULL_TYPE
      if (type.isNull()) continue;
      if (type.isDecimal()) type = Type.DEFAULT_DECIMAL;
      if (type.getPrimitiveType() == PrimitiveType.VARCHAR) {
        type = ScalarType.createVarcharType(1);
      }
      if (type.getPrimitiveType() == PrimitiveType.CHAR) {
        type = ScalarType.createCharType(1);
      }
      checkExprType("select cast(null as " + type + ")", type);
    }
  }

  /**
   * Tests that casts to complex types fail with an appropriate error message.
   */
  @Test
  public void TestComplexTypeCasts() throws AnalysisException {
    AnalysisError("select cast(1 as array<int>)",
        "Unsupported cast to complex type: ARRAY<INT>");
    AnalysisError("select cast(1 as map<int, int>)",
        "Unsupported cast to complex type: MAP<INT,INT>");
    AnalysisError("select cast(1 as struct<a:int,b:char(20)>)",
        "Unsupported cast to complex type: STRUCT<a:INT,b:CHAR(20)>");
  }

  @Test
  public void TestLikePredicates() throws AnalysisException {
    AnalyzesOk("select * from functional.alltypes where string_col like  'test%'");
    AnalyzesOk("select * from functional.alltypes where string_col like string_col");
    AnalyzesOk("select * from functional.alltypes where string_col ilike  'test%'");
    AnalyzesOk("select * from functional.alltypes where string_col ilike string_col");
    AnalyzesOk("select * from functional.alltypes where 'test' like string_col");
    AnalyzesOk("select * from functional.alltypes where string_col rlike 'test%'");
    AnalyzesOk("select * from functional.alltypes where string_col regexp 'test.*'");
    AnalyzesOk("select * from functional.alltypes where string_col iregexp 'test.*'");
    AnalysisError("select * from functional.alltypes where string_col like 5",
        "right operand of LIKE must be of type STRING");
    AnalysisError("select * from functional.alltypes where string_col ilike 5",
        "right operand of ILIKE must be of type STRING");
    AnalysisError("select * from functional.alltypes where 'test' like 5",
        "right operand of LIKE must be of type STRING");
    AnalysisError("select * from functional.alltypes where 'test' ilike 5",
        "right operand of ILIKE must be of type STRING");
    AnalysisError("select * from functional.alltypes where string_col like " +
                  "cast('test%' as binary)",
        "right operand of LIKE must be of type STRING");
    AnalysisError("select * from functional.alltypes where int_col like 'test%'",
        "left operand of LIKE must be of type STRING");
    AnalysisError("select * from functional.alltypes where int_col ilike 'test%'",
        "left operand of ILIKE must be of type STRING");
    AnalysisError("select * from functional.binary_tbl where binary_col like 'test%'",
        "left operand of LIKE must be of type STRING");
    AnalysisError("select * from functional.alltypes where string_col regexp 'test]['",
        "invalid regular expression in 'string_col REGEXP 'test][''");
    AnalysisError("select * from functional.alltypes where string_col iregexp 'test]['",
        "invalid regular expression in 'string_col IREGEXP 'test][''");
    // Test NULLs.
    String[] likePreds = new String[] {"LIKE", "RLIKE", "REGEXP", "ILIKE", "IREGEXP"};
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
    AnalysisError("select int_array_col or true from functional.allcomplextypes",
        "Operand 'int_array_col' part of predicate 'int_array_col OR TRUE' should " +
            "return type 'BOOLEAN' but returns type 'ARRAY<INT>'");
    AnalysisError("select false and tiny_struct from " +
        "functional_orc_def.complextypes_structs",
        "Operand 'tiny_struct' part of predicate 'FALSE AND tiny_struct' should " +
            "return type 'BOOLEAN' but returns type 'STRUCT<b:BOOLEAN>'.");
    AnalysisError("select not int_map_col from functional.allcomplextypes",
        "Operand 'int_map_col' part of predicate 'NOT int_map_col' should return " +
            "type 'BOOLEAN' but returns type 'MAP<STRING,INT>'.");
  }

  @Test
  public void TestIsNullPredicates() throws AnalysisException {
    AnalyzesOk("select * from functional.alltypes where int_col is null");
    AnalyzesOk("select * from functional.alltypes where string_col is not null");
    AnalyzesOk("select * from functional.binary_tbl where binary_col is null");
    AnalyzesOk("select * from functional.alltypes where null is not null");

    AnalysisError("select 1 from functional.allcomplextypes where int_map_col is null",
        "IS NULL predicate does not support complex types: int_map_col IS NULL");
    AnalysisError("select * from functional_orc_def.complextypes_structs where " +
        "tiny_struct is null",
        "IS NULL predicate does not support complex types: tiny_struct IS NULL");
    AnalysisError("select * from functional_orc_def.complextypes_structs where " +
        "tiny_struct is not null",
        "IS NOT NULL predicate does not support complex types: tiny_struct " +
            "IS NOT NULL");
  }

  @Test
  public void TestBoolTestExpression() throws AnalysisException {
    String[] rhsOptions = new String[] {"true", "false", "unknown"};
    String[] lhsOptions = new String[] {
        "bool_col",      // column reference
        "1>1",           // boolean expression
        "istrue(false)", // function
        "(1>1 is true)"  // nested expression
        };

    // Bool test in both the select and where clauses. Includes negated IS clauses.
    for (String rhsVal : rhsOptions) {
      String template = "select (%s is %s) from functional.alltypes where (%s is %s)";
      String lhsVal = rhsVal == "unknown" ? "null" : rhsVal;
      String negatedRhsVal = "not " + rhsVal;

      // Tests for lhs being one of true, false or null.
      AnalyzesOk(String.format(template, lhsVal, rhsVal, lhsVal, rhsVal));
      AnalyzesOk(String.format(template, lhsVal, negatedRhsVal, lhsVal, negatedRhsVal));

      // Tests for lhs being one lhsOptions expressions.
      for (String lhs : lhsOptions) {
        AnalyzesOk(String.format(template, lhs, rhsVal, lhs, rhsVal));
        AnalyzesOk(String.format(template, lhs, negatedRhsVal, lhs, negatedRhsVal));
      }
    }

    AnalysisError("select ('foo' is true)",
        "No matching function with signature: istrue(STRING).");
    AnalysisError("select (10 is true)",
        "No matching function with signature: istrue(TINYINT).");
    AnalysisError("select (10 is not true)",
        "No matching function with signature: isnottrue(TINYINT).");
    AnalysisError("select (10 is false)",
        "No matching function with signature: isfalse(TINYINT).");
    AnalysisError("select (10 is not false)",
        "No matching function with signature: isnotfalse(TINYINT).");
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
    AnalyzesOk("select * from functional.binary_tbl " +
        "where cast('abc' as binary) not between cast(string_col as binary) " +
        "and binary_col");
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
    AnalyzesOk("select * from functional.alltypes, functional.date_tbl " +
        "where timestamp_col between date_part and date_col");
    // Comparison expr requires implicit cast.
    AnalyzesOk("select * from functional.alltypes " +
        "where smallint_col between float_col and double_col");
    AnalyzesOk("select * from functional.date_tbl, functional.alltypes " +
        "where date_col between timestamp_col and timestamp_col + interval 1 day");
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
    AnalysisError("select * from functional.date_tbl, functional.alltypes " +
        "where date_col between int_col and double_col",
        "Incompatible return types 'DATE' and 'INT' " +
        "of exprs 'date_col' and 'int_col'.");
    AnalysisError("select 1 from functional_orc_def.complextypes_structs " +
        "where tiny_struct between 10 and 20",
        "Incompatible return types 'STRUCT<b:BOOLEAN>' and 'TINYINT' " +
        "of exprs 'tiny_struct' and '10'.");
    AnalysisError("select * from functional.binary_tbl " +
        "where string_col between binary_col and 'a'",
        "Incompatible return types 'STRING' and 'BINARY' " +
        "of exprs 'string_col' and 'binary_col'.");

    // IMPALA-7211: Do not cast decimal types to other decimal types
    AnalyzesOk("select cast(1 as decimal(38,2)) between " +
        "0.9 * cast(1 as decimal(38,3)) and 3");
    AnalyzesOk("select cast(2 as decimal(38,37)) between " +
        "cast(1 as decimal(38,1)) and cast(3 as decimal(38,15))");
    AnalyzesOk("select cast(2 as decimal(38,37)) between " +
        "cast(1 as double) and cast(3 as decimal(38,1))");
    AnalyzesOk("select cast(2 as decimal(38,37)) between " +
        "cast(1 as decimal(38,1)) and cast(3 as double)");
    AnalyzesOk("select cast(2 as decimal(38,37)) between " +
        "cast(1 as decimal(38,1)) and cast(3 as bigint)");
    AnalyzesOk("select cast(2 as decimal(38,37)) between " +
        "cast(1 as bigint) and cast(3 as bigint)");
    AnalyzesOk("select cast(2 as decimal(38,37)) between " +
        "cast(1 as bigint) and cast(3 as decimal(38,1))");
    AnalyzesOk("select cast(2 as decimal(38,37)) between " +
        "cast(1 as decimal(38,1)) and cast(3 as int)");
    AnalyzesOk("select cast(2 as decimal(38,37)) between " +
        "cast(1 as int) and cast(3 as decimal(38,1))");
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
    AnalyzesOk("select * from functional.alltypes, functional.date_tbl where " +
        "timestamp_col in (date_col, date_part)");
    // Comparison expr requires implicit cast.
    AnalyzesOk("select * from functional.alltypes where " +
        "int_col in (double_col, bigint_col)");
    AnalyzesOk("select * from functional.alltypes, functional.date_tbl where " +
        "date_col in (timestamp_col, timestamp_col + interval 1 day)");
    // Test predicates.
    AnalyzesOk("select * from functional.alltypes where " +
        "!true in (false or true, true and false)");
    // Test NULLs.
    AnalyzesOk("select * from functional.alltypes where " +
        "NULL in (NULL, NULL)");
    // Test IN in binary predicates
    AnalyzesOk("select bool_col = (int_col in (1,2)), " +
        "case when tinyint_col in (10, NULL) then tinyint_col else NULL end " +
        "from functional.alltypestiny where int_col > (bool_col in (false)) " +
        "and (int_col in (1,2)) = (select min(bool_col) from functional.alltypes) " +
        "and (int_col in (3,4)) = (tinyint_col in (4,5))");
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
    AnalysisError("select * from functional.date_tbl, functional.alltypes where " +
        "date_col in (int_col, double_col)",
        "Incompatible return types 'DATE' and 'INT' " +
        "of exprs 'date_col' and 'int_col'.");
    AnalysisError("select * from functional.date_tbl, functional.alltypes where " +
        "date_col in (NULL, int_col)",
        "Incompatible return types 'DATE' and 'INT' " +
        "of exprs 'date_col' and 'int_col'.");
    AnalysisError("select 1 from functional.allcomplextypes where " +
        "int_array_col in (id, NULL)",
        "Incompatible return types 'ARRAY<INT>' and 'INT' " +
        "of exprs 'int_array_col' and 'id'.");
  }

  @Test
  public void TestAnalyticExprs() throws AnalysisException {
    AnalyzesOk("select int_col from functional.alltypessmall order by count(*) over () "
        + "limit 10");
    AnalyzesOk("select sum(int_col) over () from functional.alltypes");
    AnalyzesOk("select avg(bigint_col) over (partition by id) from functional.alltypes");
    AnalyzesOk("select count(smallint_col) over (partition by id order by tinyint_col) "
        + "from functional.alltypes");
    AnalyzesOk("select min(int_col) over (partition by id order by tinyint_col "
        + "rows between unbounded preceding and current row) from functional.alltypes");
    AnalyzesOk("select sum(int_col) over (partition by id order by tinyint_col "
        + "rows 2 preceding) from functional.alltypes");
    AnalyzesOk("select sum(int_col) over (partition by id order by tinyint_col "
        + "rows between 2 preceding and unbounded following) from functional.alltypes");
    AnalyzesOk("select min(int_col) over (partition by id order by tinyint_col "
        + "rows between unbounded preceding and 2 preceding) from functional.alltypes");
    AnalyzesOk("select sum(int_col) over (partition by id order by tinyint_col "
        + "rows between 2 following and unbounded following) from functional.alltypes");
    // TODO: Enable after RANGE windows with offset boundaries are supported
    //AnalyzesOk("select sum(int_col) over (partition by id order by tinyint_col "
    //    + "range between 2 preceding and unbounded following) from functional.alltypes");
    //AnalyzesOk("select sum(int_col) over (partition by id order by tinyint_col "
    //    + "range between 2 preceding and unbounded following) from functional.alltypes");
    AnalyzesOk("select sum(int_col) over (partition by id order by tinyint_col "
        + "rows between 2 preceding and 6 following) from functional.alltypes");
        // TODO: substitute constants in-line so that 2*3 becomes implicitly castable
        // to tinyint
        //+ "range between 2 preceding and 2 * 3 following) from functional.alltypes");
    AnalyzesOk("select sum(int_col) over (partition by id order by tinyint_col "
        + "rows between 2 preceding and unbounded following) from functional.alltypes");
    AnalyzesOk("select lead(int_col, 1, null) over "
        + "(partition by id order by tinyint_col) from functional.alltypes");
    AnalyzesOk("select rank() over "
        + "(partition by id order by tinyint_col) from functional.alltypes");
    AnalyzesOk(
        "select id from functional.alltypes order by rank() over (order by tinyint_col)");

    // last_value/first_value
    AnalyzesOk(
        "select first_value(tinyint_col) over (order by id) from functional.alltypesagg");
    AnalyzesOk(
        "select last_value(tinyint_col) over (order by id) from functional.alltypesagg");
    AnalyzesOk("select first_value(tinyint_col ignore nulls) over (order by id) from "
        + "functional.alltypesagg");
    AnalyzesOk("select last_value(tinyint_col ignore nulls) over (order by id) from "
        + "functional.alltypesagg");
    // IMPALA-4301: Test IGNORE NULLS with subqueries.
    AnalyzesOk("select first_value(tinyint_col ignore nulls) over (order by id)," +
               "last_value(tinyint_col ignore nulls) over (order by id)" +
               "from functional.alltypesagg a " +
               "where exists (select 1 from functional.alltypes b where a.id = b.id)");

    // last_value/first_value without using order by
    AnalyzesOk("select first_value(tinyint_col) over () from functional.alltypesagg");
    AnalyzesOk("select last_value(tinyint_col) over () from functional.alltypesagg");
    AnalyzesOk("select first_value(tinyint_col ignore nulls) over () from "
        + "functional.alltypesagg");
    AnalyzesOk("select last_value(tinyint_col ignore nulls) over () from "
        + "functional.alltypesagg");
    AnalyzesOk("select first_value(tinyint_col ignore nulls) over ()," +
        "last_value(tinyint_col ignore nulls) over () from functional.alltypesagg a " +
        "where exists (select 1 from functional.alltypes b where a.id = b.id)");

    // legal combinations of analytic and agg functions
    AnalyzesOk("select sum(count(id)) over (partition by min(int_col) "
        + "order by max(bigint_col)) from functional.alltypes group by id, tinyint_col "
        + "order by rank() over (order by max(bool_col), tinyint_col)");
    AnalyzesOk("select lead(count(id)) over (order by tinyint_col) "
        + "from functional.alltypes group by id, tinyint_col "
        + "order by rank() over (order by tinyint_col)");
    AnalyzesOk("select min(count(id)) over (order by tinyint_col) "
        + "from functional.alltypes group by id, tinyint_col "
        + "order by rank() over (order by tinyint_col)");
    // IMPALA-1231: COUNT(t1.int_col) shows up in two different ways: as agg output
    // and as an analytic fn call
    AnalyzesOk("select t1.int_col, COUNT(t1.int_col) OVER (ORDER BY t1.int_col) "
        + "FROM functional.alltypes t1 GROUP BY t1.int_col HAVING COUNT(t1.int_col) > 1");

    // legal windows
    AnalyzesOk(
        "select sum(int_col) over (partition by id order by tinyint_col, int_col "
          + "rows between 2 following and 4 following) from functional.alltypes");
    AnalyzesOk(
        "select max(int_col) over (partition by id order by tinyint_col, int_col "
          + "range between unbounded preceding and current row) "
          + "from functional.alltypes");
    AnalyzesOk(
        "select sum(int_col) over (partition by id order by tinyint_col, int_col "
          + "range between current row and unbounded following) "
          + "from functional.alltypes");
    AnalyzesOk(
        "select max(int_col) over (partition by id order by tinyint_col, int_col "
          + "range between unbounded preceding and unbounded following) "
          + "from functional.alltypes");
    AnalyzesOk("select sum(int_col) over (partition by id order by tinyint_col "
        + "rows between 4 preceding and 2 preceding) from functional.alltypes");
    AnalyzesOk("select sum(int_col) over (partition by id order by tinyint_col "
        + "rows between 2 following and 4 following) from functional.alltypes");
    AnalyzesOk( "select "
        + "2 * min(tinyint_col) over (partition by id order by tinyint_col "
        + "  rows between unbounded preceding and current row), "
        + "concat(max(string_col) over (partition by timestamp_col, int_col "
        + "  order by tinyint_col, smallint_col "
        + "  rows between unbounded preceding and current row), "
        + "min(string_col) over (partition by timestamp_col, int_col "
        + "  order by tinyint_col, smallint_col "
        + "  rows between unbounded preceding and current row)) "
        + "from functional.alltypes");
    AnalyzesOk(
        "select sum(int_col) over (partition by id order by tinyint_col, int_col "
          + "rows between current row and current row) from functional.alltypes");
    AnalysisError(
        "select sum(int_col) over (partition by id order by tinyint_col, int_col "
          + "range between current row and current row) from functional.alltypes",
        "RANGE is only supported with both the lower and upper bounds UNBOUNDED or one "
          + "UNBOUNDED and the other CURRENT ROW.");
    AnalysisError("select sum(int_col) over (partition by id order by tinyint_col "
          + "range between 2 following and 2 following) from functional.alltypes",
        "RANGE is only supported with both the lower and upper bounds UNBOUNDED or one "
            + "UNBOUNDED and the other CURRENT ROW.");

    // Min/max do not support start bounds with offsets
    AnalysisError("select max(int_col) over (partition by id order by tinyint_col "
        + "rows 2 preceding) from functional.alltypes",
        "'max(int_col)' is only supported with an UNBOUNDED PRECEDING start bound.");
    // If the query can be re-written so that the start is unbounded, it should
    // be supported (IMPALA-1433).
    AnalyzesOk("select max(id) over (order by id rows between current row and "
        + "unbounded following) from functional.alltypes");
    AnalyzesOk("select min(int_col) over (partition by id order by tinyint_col "
        + "rows between 2 preceding and unbounded following) from functional.alltypes");
    // TODO: Enable after RANGE windows with offset boundaries are supported
    //AnalysisError("select max(int_col) over (partition by id order by tinyint_col "
    //    + "range 2 preceding) from functional.alltypes",
    //    "'max(int_col)' is only supported with an UNBOUNDED PRECEDING start bound.");

    // missing grouping expr
    AnalysisError(
        "select lead(count(bigint_col)) over (order by tinyint_col) "
          + "from functional.alltypes group by id, tinyint_col "
          + "order by rank() over (order by smallint_col)",
        "ORDER BY expression not produced by aggregation output (missing from GROUP "
          + "BY clause?): rank() OVER (ORDER BY smallint_col ASC)");
    // missing Over clause
    AnalysisError("select 1, lag(int_col) from functional.alltypes",
        "Analytic function requires an OVER clause: lag(int_col)");
    // no FROM clause
    AnalysisError("select 1, count(*) over()",
        "Analytic expressions require FROM clause");
    // can't mix DISTINCT and analytics
    AnalysisError(
        "select distinct int_col, sum(double_col) over () from functional.alltypes",
        "cannot combine SELECT DISTINCT with analytic functions");
    // analytic expr in Group By
    AnalysisError(
        "select id, count(*) from functional.alltypes "
          + "group by 1, rank() over(order by int_col)",
        "GROUP BY expression must not contain analytic expressions: rank() OVER "
          + "(ORDER BY int_col ASC)");
    AnalysisError(
        "select id, rank() over(order by int_col), count(*) "
          + "from functional.alltypes group by 1, 2",
        "GROUP BY expression must not contain analytic expressions: rank() OVER "
          + "(ORDER BY int_col ASC)");
    // analytic expr in Having
    AnalysisError(
        "select id, count(*) from functional.alltypes group by 1 "
          + "having rank() over(order by int_col) > 1",
        "HAVING clause must not contain analytic expressions: rank() OVER "
          + "(ORDER BY int_col ASC)");
    // analytic expr in Where
    AnalysisError(
        "select id, tinyint_col from functional.alltypes "
          + "where row_number() over(order by id) > 1",
        "WHERE clause must not contain analytic expressions: row_number() OVER "
          + "(ORDER BY id ASC)");
    // analytic expr with Distinct
    AnalysisError(
        "select id, tinyint_col, sum(distinct tinyint_col) over(order by id) "
          + "from functional.alltypes",
        "DISTINCT not allowed in analytic function");
    // IGNORE NULLS may only be used with first_value/last_value
    AnalysisError(
        "select sum(id ignore nulls) over (order by id) from functional.alltypes",
        "Function SUM does not accept the keyword IGNORE NULLS.");
    // IMPALA-1256: AnalyticExpr.resetAnalysisState() didn't sync up w/ orderByElements_
    AnalyzesOk("with t as ("
        + "select * from (select sum(t1.year) over ("
        + "  order by max(t1.id), t1.year "
        + "  rows between unbounded preceding and 5 preceding) "
        + "from functional.alltypes t1 group by t1.year) t1) select * from t");
    AnalyzesOk("with t as ("
        + "select sum(t1.smallint_col) over () from functional.alltypes t1) "
        + "select * from t");
    // IMPALA-1234
    AnalyzesOk("with t as (select 1 as int_col_1 from functional.alltypesagg t1) "
        + "select count(t1.int_col_1) as int_col_1 from t t1 where t1.int_col_1 is null "
        + "group by t1.int_col_1 union all "
        + "select min(t1.day) over () from functional.alltypesagg t1");
    // IMPALA-2532: Resolve wildcard decimals returned from first_value().
    AnalyzesOk("select 1 in " +
        "(first_value(cast(int_col AS DECIMAL)) " +
        " over (order by int_col rows between 2 preceding and 1 preceding)) " +
        "from functional.alltypestiny");
    // IMPALA-6323: Allow constant expressions in analytic window exprs.
    AnalyzesOk("select rank() over (order by 1) from functional.alltypestiny");
    AnalyzesOk("select count() over (partition by 2) from functional.alltypestiny");
    AnalyzesOk(
        "select rank() over (partition by 2 order by 1) from functional.alltypestiny");

    // nested analytic exprs
    AnalysisError(
        "select sum(int_col) over (partition by id, rank() over (order by int_col) "
          + "order by tinyint_col, int_col "
          + "rows between 2 following and 4 following) from functional.alltypes",
        "Nesting of analytic expressions is not allowed");
    AnalysisError(
        "select lead(rank() over (order by int_col)) over (partition by id "
          + "order by tinyint_col, int_col) from functional.alltypes",
        "Nesting of analytic expressions is not allowed");
    AnalysisError(
        "select max(int_col) over (partition by id "
          + "order by rank() over (order by tinyint_col), int_col) "
          + "from functional.alltypes",
        "Nesting of analytic expressions is not allowed");
    // lead/lag variants
    AnalyzesOk(
        "select lag(int_col, 10, 5 + 1) over (partition by id, bool_col "
          + "order by tinyint_col, int_col) from functional.alltypes");
    AnalyzesOk(
        "select lead(string_col, 1, 'default') over ("
          + "order by tinyint_col, int_col) from functional.alltypes");
    AnalyzesOk(
        "select lag(bool_col, 3) over ("
          + "order by id, int_col) from functional.alltypes");
    AnalyzesOk(
        "select lead(float_col, 2) over (partition by string_col, timestamp_col "
          + "order by tinyint_col, int_col) from functional.alltypes");
    AnalyzesOk(
        "select lag(double_col) over ("
          + "order by string_col, int_col) from functional.alltypes");
    AnalyzesOk(
        "select lead(timestamp_col) over (partition by id "
          + "order by tinyint_col, int_col) from functional.alltypes");
    // missing offset w/ default
    AnalysisError(
        "select lag(string_col, 'x') over (partition by id "
          + "order by tinyint_col, int_col) from functional.alltypes",
        "No matching function with signature: lag(STRING, STRING)");
    // mismatched default type
    AnalysisError(
        "select lead(int_col, 1, 'x') over ("
          + "order by tinyint_col, int_col) from functional.alltypes",
        "No matching function with signature: lead(INT, TINYINT, STRING)");
    // missing params
    AnalysisError(
        "select lag() over (partition by id "
          + "order by tinyint_col, int_col) from functional.alltypes",
        "No matching function with signature: lag()");
    // bad offsets
    AnalysisError(
        "select lead(int_col, -1) over ("
          + "order by tinyint_col, int_col) from functional.alltypes",
        "The offset parameter of LEAD/LAG must be a constant positive integer");
    AnalysisError(
        "select lag(int_col, tinyint_col * 2, 5) over ("
          + "order by tinyint_col, int_col) from functional.alltypes",
        "The offset parameter of LEAD/LAG must be a constant positive integer");
    AnalysisError(
        "select lag(int_col, 1, int_col) over ("
          + "order by tinyint_col, int_col) from functional.alltypes",
        "The default parameter (parameter 3) of LEAD/LAG must be a constant");

    // wrong type of function
    AnalysisError("select abs(float_col) over (partition by id order by tinyint_col "
        + "rows between unbounded preceding and current row) from functional.alltypes",
        "OVER clause requires aggregate or analytic function: abs(float_col)");
    AnalysisError("select group_concat(string_col) over (order by tinyint_col "
        + "rows between unbounded preceding and current row) from functional.alltypes",
        "Aggregate function 'group_concat(string_col)' not supported with OVER clause.");
    // Order By missing
    AnalysisError("select sum(int_col) over (partition by id "
        + "rows between unbounded preceding and current row) from functional.alltypes",
        "Windowing clause requires ORDER BY clause");
    // Order By missing for ranking fn
    AnalysisError("select dense_rank() over (partition by id) from functional.alltypes",
        "'dense_rank()' requires an ORDER BY clause");
    // Order By missing for offset fn
    AnalysisError("select lag(tinyint_col, 1, null) over (partition by id) "
        + "from functional.alltypes",
        "'lag(tinyint_col, 1, NULL)' requires an ORDER BY clause");
    // Window for ranking fn
    AnalysisError("select row_number() over (partition by id order by tinyint_col "
        + "rows between unbounded preceding and current row) from functional.alltypes",
        "Windowing clause not allowed with 'row_number()'");
    // Window for offset fn
    AnalysisError("select lead(tinyint_col, 1, null) over (partition by id "
        + "order by tinyint_col rows between unbounded preceding and current row) "
        + "from functional.alltypes",
        "Windowing clause not allowed with 'lead(tinyint_col, 1, NULL)'");

    // windows
    AnalysisError("select sum(tinyint_col) over (partition by id "
        + "order by tinyint_col rows between unbounded following and current row) "
        + "from functional.alltypes",
        "UNBOUNDED FOLLOWING is only allowed for upper bound of BETWEEN");
    AnalysisError("select sum(tinyint_col) over (partition by id "
        + "order by tinyint_col rows unbounded following) from functional.alltypes",
        "UNBOUNDED FOLLOWING is only allowed for upper bound of BETWEEN");
    AnalysisError("select sum(tinyint_col) over (partition by id "
        + "order by tinyint_col rows between current row and unbounded preceding) "
        + "from functional.alltypes",
        "UNBOUNDED PRECEDING is only allowed for lower bound of BETWEEN");
    AnalysisError("select sum(tinyint_col) over (partition by id "
        + "order by tinyint_col rows 2 following) from functional.alltypes",
        "FOLLOWING requires a BETWEEN clause");
    AnalysisError(
        "select sum(tinyint_col) over (partition by id "
          + "order by tinyint_col rows between 2 following and current row) "
          + "from functional.alltypes",
        "A lower window bound of FOLLOWING requires that the upper bound also be "
          + "FOLLOWING");
    AnalysisError(
        "select sum(tinyint_col) over (partition by id "
          + "order by tinyint_col rows between current row and 2 preceding) "
          + "from functional.alltypes",
        "An upper window bound of PRECEDING requires that the lower bound also be "
          + "PRECEDING");

    // offset boundaries
    AnalysisError(
        "select min(int_col) over (partition by id order by tinyint_col "
          + "rows between tinyint_col preceding and current row) "
          + "from functional.alltypes",
        "For ROWS window, the value of a PRECEDING/FOLLOWING offset must be a "
          + "constant positive integer: tinyint_col PRECEDING");
    AnalysisError(
        "select min(int_col) over (partition by id order by tinyint_col "
          + "rows between current row and '2' following) from functional.alltypes",
        "For ROWS window, the value of a PRECEDING/FOLLOWING offset must be a "
          + "constant positive integer: '2' FOLLOWING");
    AnalysisError(
        "select min(int_col) over (partition by id order by tinyint_col "
          + "rows between -2 preceding and current row) from functional.alltypes",
        "For ROWS window, the value of a PRECEDING/FOLLOWING offset must be a "
          + "constant positive integer: -2 PRECEDING");
    AnalysisError(
        "select min(int_col) over (partition by id order by tinyint_col "
          + "rows between 2 preceding and 3 preceding) from functional.alltypes",
        "Offset boundaries are in the wrong order: ROWS BETWEEN 2 PRECEDING AND "
          + "3 PRECEDING");
    AnalysisError(
        "select min(int_col) over (partition by id order by tinyint_col "
          + "rows between count(*) preceding and current row) from functional.alltypes",
        "For ROWS window, the value of a PRECEDING/FOLLOWING offset must be a "
          + "constant positive integer: count(*) PRECEDING");

    // TODO: Enable after RANGE windows with offset boundaries are supported.
    //AnalysisError(
    //    "select min(int_col) over (partition by id order by float_col "
    //      + "range between -2.1 preceding and current row) from functional.alltypes",
    //    "For RANGE window, the value of a PRECEDING/FOLLOWING offset must be a "
    //      + "constant positive number: -2.1 PRECEDING");
    //AnalysisError(
    //    "select min(int_col) over (partition by id order by int_col "
    //      + "range between current row and 2.1 following) from functional.alltypes",
    //    "The value expression of a PRECEDING/FOLLOWING clause of a RANGE window must "
    //      + "be implicitly convertable to the ORDER BY expression's type: 2.1 cannot "
    //      + "be implicitly converted to INT");
    //AnalysisError(
    //    "select min(int_col) over (partition by id order by int_col "
    //      + "range between 2 * tinyint_col preceding and current row) "
    //      + "from functional.alltypes",
    //    "For RANGE window, the value of a PRECEDING/FOLLOWING offset must be a "
    //      + "constant positive number");
    //AnalysisError(
    //    "select min(int_col) over (partition by id order by int_col "
    //      + "range between 3.1 following and 2.0 following) "
    //      + "from functional.alltypes",
    //    "Offset boundaries are in the wrong order");
    //// multiple ordering exprs w/ range offset
    //AnalysisError(
    //    "select min(int_col) over (partition by id order by int_col, tinyint_col "
    //      + "range between 2 preceding and current row) "
    //      + "from functional.alltypes",
    //    "Only one ORDER BY expression allowed if used with a RANGE window with "
    //      + "PRECEDING/FOLLOWING");

    // percent_rank(), cume_dist() and ntile() tests
    AnalyzesOk("select percent_rank() over(order by id) from functional.alltypes");
    AnalyzesOk("select cume_dist() over(order by id) from functional.alltypes");
    AnalyzesOk("select ntile(3) over(order by id) from functional.alltypes");
    AnalyzesOk("select ntile(3000) over(order by id) from functional.alltypes");
    AnalyzesOk("select ntile(3000000000) over(order by id) from functional.alltypes");
    AnalyzesOk("select percent_rank() over(partition by tinyint_col, bool_col "
        + "order by id), ntile(3) over(partition by int_col, bool_col "
        + "order by smallint_col, id), cume_dist() over(partition by int_col, bool_col "
        + "order by month) from functional.alltypes");

    AnalysisError("select ntile(-1) over(order by int_col) from functional.alltypestiny",
        "NTILE() requires a positive argument: -1");
    AnalysisError("select percent_rank() over(partition by int_col) "
        + "from functional.alltypestiny",
        "'percent_rank()' requires an ORDER BY clause");
    AnalysisError("select cume_dist() over(partition by int_col) "
        + "from functional.alltypestiny",
      "'cume_dist()' requires an ORDER BY clause");
    AnalysisError("select ntile(2) over(partition by int_col) "
        + "from functional.alltypestiny",
      "'ntile(2)' requires an ORDER BY clause");
    // TODO: Remove this test once we allow for non-constant arguments in ntile()
    AnalysisError(
      "select ntile(int_col) over(order by tinyint_col) "
        + "from functional.alltypestiny",
      "NTILE() requires a constant argument");

    // Cannot order or partition by complex-typed expression.
    AnalysisError("select id, row_number() over (order by int_array_col) " +
        "from functional_parquet.allcomplextypes", "ORDER BY expression " +
        "'int_array_col' with complex type 'ARRAY<INT>' is not supported.");
    AnalysisError("select id, count() over (partition by tiny_struct) from " +
        "functional_orc_def.complextypes_structs",
        "PARTITION BY expression 'tiny_struct' with complex type " +
        "'STRUCT<b:BOOLEAN>' is not supported.");
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
    for (Type type1: numericTypes) {
      for (Type type2: numericTypes) {
        Type t =
            Type.getAssignmentCompatibleType(type1, type2, TypeCompatibility.DEFAULT);
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

    List<Type> fixedPointTypes = new ArrayList<Type>(Type.getIntegerTypes());
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
    for (BinaryPredicate.Operator cmpOp: BinaryPredicate.Operator.values()) {
      for (Type type1: types) {
        for (Type type2: types) {
          // Prefer strict matching.
          Type compatibleType = Type.getAssignmentCompatibleType(
              type1, type2, TypeCompatibility.ALL_STRICT);
          if (compatibleType.isInvalid()) {
            compatibleType =
                Type.getAssignmentCompatibleType(type1, type2, TypeCompatibility.DEFAULT);
          }
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
      List<Expr> selectListExprs = select.getResultExprs();
      assertNotNull(selectListExprs);
      assertEquals(selectListExprs.size(), 1);
      // check the first expr in select list
      expr = selectListExprs.get(0);
      Assert.assertEquals("opType= " + opType + " exprType=" + expr.getType(),
          opType, expr.getType());
    } else {
      // check the where clause
      expr = select.getWhereClause();
      if (!expr.getType().isNull()) {
        assertEquals(Type.BOOLEAN, expr.getType());
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

  /**
   * Get the result type of a select statement with a single select list element.
   */
  Type getReturnType(String stmt, AnalysisContext ctx) {
    SelectStmt select = (SelectStmt) AnalyzesOk(stmt, ctx);
    List<Expr> selectListExprs = select.getResultExprs();
    assertNotNull(selectListExprs);
    assertEquals(selectListExprs.size(), 1);
    // check the first expr in select list
    Expr expr = selectListExprs.get(0);
    return expr.getType();
  }

  private void checkReturnType(String stmt, Type resultType, AnalysisContext ctx) {
    Type exprType = getReturnType(stmt, ctx);
    assertEquals("Expected: " + resultType + " != " + exprType, resultType, exprType);
  }

  private void checkReturnType(String stmt, Type resultType) {
    checkReturnType(stmt, resultType, createAnalysisCtx(Catalog.DEFAULT_DB));
  }

  /**
   * Test expressions involving decimal types that return different numeric types
   * depending on the DECIMAL_V2 setting.
   */
  private void checkDecimalReturnType(String stmt, Type decimalV1ResultType,
      Type decimalV2ResultType) {
    AnalysisContext ctx = createAnalysisCtx(Catalog.DEFAULT_DB);
    ctx.getQueryOptions().setDecimal_v2(false);
    checkReturnType(stmt, decimalV1ResultType, ctx);
    ctx = createAnalysisCtx(Catalog.DEFAULT_DB);
    ctx.getQueryOptions().setDecimal_v2(true);
    checkReturnType(stmt, decimalV2ResultType, ctx);
  }

  /**
   * Test expressions that return the same type with either DECIMAL_V2 setting.
   */
  private void checkDecimalReturnType(String stmt, Type resultType) {
    checkDecimalReturnType(stmt, resultType, resultType);
  }

  @Test
  public void TestNumericLiteralTypeResolution() throws AnalysisException {
    checkReturnType("select 1", Type.TINYINT);
    checkDecimalReturnType("select 1.1", ScalarType.createDecimalType(2,1));
    checkDecimalReturnType("select 01.1", ScalarType.createDecimalType(2,1));
    checkDecimalReturnType("select 1 + 1.1",
        Type.DOUBLE, ScalarType.createDecimalType(5, 1));
    checkDecimalReturnType("select 0.23 + 1.1", ScalarType.createDecimalType(4,2));

    checkReturnType("select float_col + float_col from functional.alltypestiny",
        Type.DOUBLE);
    checkReturnType("select int_col + int_col from functional.alltypestiny",
        Type.BIGINT);

    // All arithmetic operators except multiplication involving floating point follow the
    // same rules for deciding whether the output is a decimal or floating point.
    //
    // DECIMAL_V1: floating point + any numeric literal = floating point
    // DECIMAL_V2: floating point + any expr of type decimal = decimal
    checkDecimalReturnType("select float_col + 1.1 from functional.alltypestiny",
        Type.DOUBLE, ScalarType.createDecimalType(38, 8));
    checkDecimalReturnType("select float_col - 1.1 from functional.alltypestiny",
        Type.DOUBLE, ScalarType.createDecimalType(38, 8));
    checkDecimalReturnType("select float_col / 1.1 from functional.alltypestiny",
        Type.DOUBLE, ScalarType.createDecimalType(38, 8));
    checkDecimalReturnType("select float_col % 1.1 from functional.alltypestiny",
        Type.FLOAT, ScalarType.createDecimalType(10, 9));
    // BOTH: decimal + decimal literal = decimal
    checkDecimalReturnType("select d1 + 1.1 from functional.decimal_tbl",
        ScalarType.createDecimalType(11, 1));
    checkDecimalReturnType("select d1 - 1.1 from functional.decimal_tbl",
        ScalarType.createDecimalType(11, 1));
    checkDecimalReturnType("select d1 * 1.1 from functional.decimal_tbl",
        ScalarType.createDecimalType(11, 1), ScalarType.createDecimalType(12, 1));
    checkDecimalReturnType("select d1 / 1.1 from functional.decimal_tbl",
        ScalarType.createDecimalType(14, 4), ScalarType.createDecimalType(16, 6));
    checkDecimalReturnType("select d1 % 1.1 from functional.decimal_tbl",
        ScalarType.createDecimalType(2, 1));
    // BOTH: decimal + int literal = decimal
    checkDecimalReturnType("select d1 + 2 from functional.decimal_tbl",
        ScalarType.createDecimalType(10, 0));
    checkDecimalReturnType("select d1 - 2 from functional.decimal_tbl",
        ScalarType.createDecimalType(10, 0));
    checkDecimalReturnType("select d1 * 2 from functional.decimal_tbl",
        ScalarType.createDecimalType(12, 0), ScalarType.createDecimalType(13, 0));
    checkDecimalReturnType("select d1 / 2 from functional.decimal_tbl",
        ScalarType.createDecimalType(13, 4), ScalarType.createDecimalType(15, 6));
    checkDecimalReturnType("select d1 % 2 from functional.decimal_tbl",
        ScalarType.createDecimalType(3, 0));
    // DECIMAL_V1: int + numeric literal = floating point
    // DECIMAL_V2: int + decimal expr = decimal
    checkDecimalReturnType("select int_col + 1.1 from functional.alltypestiny",
        Type.DOUBLE, ScalarType.createDecimalType(12, 1));
    checkDecimalReturnType("select int_col - 1.1 from functional.alltypestiny",
        Type.DOUBLE, ScalarType.createDecimalType(12, 1));
    checkDecimalReturnType("select int_col * 1.1 from functional.alltypestiny",
        Type.DOUBLE, ScalarType.createDecimalType(13, 1));
    checkDecimalReturnType("select int_col / 1.1 from functional.alltypestiny",
        Type.DOUBLE, ScalarType.createDecimalType(17, 6));
    checkDecimalReturnType("select int_col % 1.1 from functional.alltypestiny",
        Type.DOUBLE, ScalarType.createDecimalType(2, 1));

    // Multiplying a floating point type with any other type always results in a double.
    checkDecimalReturnType("select float_col * d1 from functional.alltypestiny " +
         "join functional.decimal_tbl", Type.DOUBLE);
    checkDecimalReturnType("select d1 * float_col from functional.alltypestiny " +
         "join functional.decimal_tbl", Type.DOUBLE);
    checkDecimalReturnType("select double_col * d1 from functional.alltypestiny " +
         "join functional.decimal_tbl", Type.DOUBLE);
    checkDecimalReturnType("select float_col * 10 from functional.alltypestiny",
        Type.DOUBLE);
    checkDecimalReturnType("select 10 * float_col from functional.alltypestiny",
        Type.DOUBLE);
    checkDecimalReturnType("select double_col * 10 from functional.alltypestiny",
        Type.DOUBLE);
    checkDecimalReturnType("select float_col * 10.0 from functional.alltypestiny",
        Type.DOUBLE);
    checkDecimalReturnType("select 10.0 * float_col from functional.alltypestiny",
        Type.DOUBLE);
    checkDecimalReturnType("select double_col * 10.0 from functional.alltypestiny",
        Type.DOUBLE);

    // IMPALA-3439: constant expression involving a function with mixed decimal and
    // integer inputs. Regression tests to make sure that only the decimal literals
    // in the constant decimal expr are cast to double. The second argument of round()
    // must be an integer.
    checkDecimalReturnType("select round(1.2345, 2) * pow(10, 10)", Type.DOUBLE);
    checkDecimalReturnType("select round(1.2345, 2) + pow(10, 10)",
        Type.DOUBLE, ScalarType.createDecimalType(38, 16));

    // Explicitly casting the literal to a decimal or float changes the type of the
    // literal. This is independent of the DECIMAL_V2 setting.
    checkDecimalReturnType("select int_col + cast(1.1 as decimal(2,1)) from "
        + " functional.alltypestiny", ScalarType.createDecimalType(12, 1));
    checkDecimalReturnType("select float_col + cast(1.1 as decimal(2,1)) from "
        + " functional.alltypestiny",
        ScalarType.createDecimalType(38, 9), ScalarType.createDecimalType(38, 8));
    checkDecimalReturnType("select float_col + cast(1.1*1.2+1.3 as decimal(2,1)) from "
        + " functional.alltypestiny",
        ScalarType.createDecimalType(38, 9), ScalarType.createDecimalType(38, 8));
    checkDecimalReturnType("select float_col + cast(1.1 as float) from "
        + " functional.alltypestiny", Type.DOUBLE);
    checkDecimalReturnType("select float_col + cast(1.1 as float) from "
        + " functional.alltypestiny", Type.DOUBLE);
    checkDecimalReturnType("select float_col + cast(1.1 as double) from "
        + " functional.alltypestiny", Type.DOUBLE);

    // Test behavior of compound expressions with a single slot ref and many literals.
    checkDecimalReturnType("select 1.0 + float_col + 1.1 from functional.alltypestiny",
        Type.DOUBLE, ScalarType.createDecimalType(38, 7));
    checkDecimalReturnType("select 1.0 + 2.0 + float_col from functional.alltypestiny",
        Type.DOUBLE, ScalarType.createDecimalType(38, 8));
    checkDecimalReturnType("select 1.0 + 2.0 + pi() * float_col from functional.alltypestiny",
        Type.DOUBLE, ScalarType.createDecimalType(38, 16));
    checkDecimalReturnType("select 1.0 + d1 + 1.1 from functional.decimal_tbl",
        ScalarType.createDecimalType(12, 1));
    checkDecimalReturnType("select 1.0 + 2.0 + d1 from functional.decimal_tbl",
        ScalarType.createDecimalType(11, 1));
    checkDecimalReturnType("select 1.0 + 2.0 + pi() * d1 from functional.decimal_tbl",
        Type.DOUBLE, ScalarType.createDecimalType(38, 16));

    // Test behavior of compound expressions with multiple slot refs and literals.
    checkDecimalReturnType("select double_col + 1.23 + float_col + 1.0 " +
        " from functional.alltypestiny", Type.DOUBLE, ScalarType.createDecimalType(38, 7));
    checkDecimalReturnType("select double_col + 1.23 + float_col + 1.0 + int_col " +
        " + bigint_col from functional.alltypestiny", Type.DOUBLE,
        ScalarType.createDecimalType(38, 6));
    checkDecimalReturnType("select d1 + 1.23 + d2 + 1.0 " +
        " from functional.decimal_tbl", ScalarType.createDecimalType(14, 2));

    // Test with slot refs of both decimal and non-decimal
    checkDecimalReturnType("select t1.int_col + t2.c1 from functional.alltypestiny t1 " +
        " cross join functional.decimal_tiny t2", ScalarType.createDecimalType(15, 4));
    checkDecimalReturnType("select 1.1 + t1.int_col + t2.c1 from functional.alltypestiny t1 " +
        " cross join functional.decimal_tiny t2", ScalarType.createDecimalType(38, 17),
        ScalarType.createDecimalType(16, 4));
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
        "'~' operation only allowed on integer types");
    AnalysisError("select float_col! from functional.alltypes",
        "'!' operation only allowed on integer types");
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
   * We have three variants of timestamp/date arithmetic exprs, as in MySQL:
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
    for (TimeUnit timeUnit: TimeUnit.values()) {
      // Tests on all valid time value types (fixed points).
      for (String col: valueTypeCols) {
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
        "Operand 'float_col' of timestamp/date arithmetic expression " +
        "'float_col + INTERVAL 10 years' returns type 'FLOAT'. " +
        "Expected type 'TIMESTAMP' or 'DATE'.");
    AnalysisError("select string_col + interval 10 years from functional.alltypes",
        "Operand 'string_col' of timestamp/date arithmetic expression " +
        "'string_col + INTERVAL 10 years' returns type 'STRING'. " +
        "Expected type 'TIMESTAMP' or 'DATE'.");
    AnalysisError(
        "select tiny_struct + interval 10 years from " +
            "functional_orc_def.complextypes_structs",
        "Operand 'tiny_struct' of timestamp/date arithmetic expression " +
        "'tiny_struct + INTERVAL 10 years' returns type 'STRUCT<b:BOOLEAN>'. " +
        "Expected type 'TIMESTAMP' or 'DATE'.");
    // Reversed interval and timestamp using addition.
    AnalysisError("select interval 10 years + float_col from functional.alltypes",
        "Operand 'float_col' of timestamp/date arithmetic expression " +
        "'INTERVAL 10 years + float_col' returns type 'FLOAT'. " +
        "Expected type 'TIMESTAMP' or 'DATE'");
    AnalysisError("select interval 10 years + string_col from functional.alltypes",
        "Operand 'string_col' of timestamp/date arithmetic expression " +
        "'INTERVAL 10 years + string_col' returns type 'STRING'. " +
        "Expected type 'TIMESTAMP' or 'DATE'");
    AnalysisError(
        "select interval 10 years + int_array_col from functional.allcomplextypes",
        "Operand 'int_array_col' of timestamp/date arithmetic expression " +
        "'INTERVAL 10 years + int_array_col' returns type 'ARRAY<INT>'. " +
        "Expected type 'TIMESTAMP' or 'DATE'.");
    // First operand does not return a timestamp. Function-call like version.
    AnalysisError("select date_add(float_col, interval 10 years) " +
        "from functional.alltypes",
        "Operand 'float_col' of timestamp/date arithmetic expression " +
        "'DATE_ADD(float_col, INTERVAL 10 years)' returns type 'FLOAT'. " +
        "Expected type 'TIMESTAMP' or 'DATE'.");
    AnalysisError("select date_add(string_col, interval 10 years) " +
        "from functional.alltypes",
        "Operand 'string_col' of timestamp/date arithmetic expression " +
        "'DATE_ADD(string_col, INTERVAL 10 years)' returns type 'STRING'. " +
        "Expected type 'TIMESTAMP' or 'DATE'.");
    AnalysisError("select date_add(int_map_col, interval 10 years) " +
        "from functional.allcomplextypes",
        "Operand 'int_map_col' of timestamp/date arithmetic expression " +
        "'DATE_ADD(int_map_col, INTERVAL 10 years)' returns type 'MAP<STRING,INT>'. " +
        "Expected type 'TIMESTAMP' or 'DATE'.");

    // Second operand is not compatible with a fixed-point type.
    // Non-function-call like version.
    AnalysisError("select timestamp_col + interval 5.2 years from functional.alltypes",
        "Operand '5.2' of timestamp/date arithmetic expression " +
        "'timestamp_col + INTERVAL 5.2 years' returns type 'DECIMAL(2,1)'. " +
        "Expected an integer type.");
    AnalysisError("select cast(0 as timestamp) + interval int_array_col years " +
        "from functional.allcomplextypes",
        "Operand 'int_array_col' of timestamp/date arithmetic expression " +
        "'CAST(0 AS TIMESTAMP) + INTERVAL int_array_col years' " +
        "returns type 'ARRAY<INT>'. Expected an integer type.");

    // No implicit cast from STRING to integer types.
    AnalysisError("select timestamp_col + interval '10' years from functional.alltypes",
                  "Operand ''10'' of timestamp/date arithmetic expression " +
                  "'timestamp_col + INTERVAL '10' years' returns type 'STRING'. " +
                  "Expected an integer type.");
    AnalysisError("select date_add(timestamp_col, interval '10' years) " +
                  "from functional.alltypes",
                  "Operand ''10'' of timestamp/date " +
                  "arithmetic expression " +
                  "'DATE_ADD(timestamp_col, INTERVAL '10' years)' returns type " +
                  "'STRING'. Expected an integer type.");

    // Cast from STRING to INT.
    AnalyzesOk("select timestamp_col + interval cast('10' as int) years " +
        "from functional.alltypes");
    // Reversed interval and timestamp using addition.
    AnalysisError("select interval 5.2 years + timestamp_col from functional.alltypes",
        "Operand '5.2' of timestamp/date arithmetic expression " +
        "'INTERVAL 5.2 years + timestamp_col' returns type 'DECIMAL(2,1)'. " +
        "Expected an integer type.");
    // Cast from STRING to INT.
    AnalyzesOk("select interval cast('10' as int) years + timestamp_col " +
        "from functional.alltypes");
    // Second operand is not compatible with type INT. Function-call like version.
    AnalysisError("select date_add(timestamp_col, interval 5.2 years) " +
        "from functional.alltypes",
        "Operand '5.2' of timestamp/date arithmetic expression " +
        "'DATE_ADD(timestamp_col, INTERVAL 5.2 years)' returns type 'DECIMAL(2,1)'. " +
        "Expected an integer type.");
    // Cast from STRING to INT.
    AnalyzesOk("select date_add(timestamp_col, interval cast('10' as int) years) " +
        " from functional.alltypes");

    // Invalid time unit. Non-function-call like version.
    AnalysisError("select timestamp_col + interval 10 error from functional.alltypes",
        "Invalid time unit 'error' in timestamp/date arithmetic expression " +
         "'timestamp_col + INTERVAL 10 error'.");
    AnalysisError("select timestamp_col - interval 10 error from functional.alltypes",
        "Invalid time unit 'error' in timestamp/date arithmetic expression " +
         "'timestamp_col - INTERVAL 10 error'.");
    // Reversed interval and timestamp using addition.
    AnalysisError("select interval 10 error + timestamp_col from functional.alltypes",
        "Invalid time unit 'error' in timestamp/date arithmetic expression " +
        "'INTERVAL 10 error + timestamp_col'.");
    // Invalid time unit. Function-call like version.
    AnalysisError("select date_add(timestamp_col, interval 10 error) " +
        "from functional.alltypes",
        "Invalid time unit 'error' in timestamp/date arithmetic expression " +
        "'DATE_ADD(timestamp_col, INTERVAL 10 error)'.");
    AnalysisError("select date_sub(timestamp_col, interval 10 error) " +
        "from functional.alltypes",
        "Invalid time unit 'error' in timestamp/date arithmetic expression " +
        "'DATE_SUB(timestamp_col, INTERVAL 10 error)'.");
  }

  @Test
  public void TestFunctionCallExpr() throws AnalysisException {
    AnalyzesOk("select pi()");
    AnalyzesOk("select _impala_builtins.pi()");
    AnalyzesOk("select _impala_builtins.decode(1, 2, 3)");
    AnalyzesOk("select _impala_builtins.DECODE(1, 2, 3)");
    AnalyzesOk("select sin(pi())");
    AnalyzesOk("select sin(cos(pi()))");
    AnalyzesOk("select sin(cos(tan(e())))");
    AnalysisError("select pi(*)", "Cannot pass '*' to scalar function.");
    AnalysisError("select sin(DISTINCT 1)",
        "Cannot pass 'DISTINCT' to scalar function.");
    AnalysisError("select * from functional.alltypes where pi(*) = 5",
        "Cannot pass '*' to scalar function.");
    // Invalid function name.
    AnalysisError("select a.b.sin()",
        "Invalid function name: 'a.b.sin'. Expected [dbname].funcname");

    // Call function that only accepts decimal
    AnalyzesOk("select precision(1)");
    AnalyzesOk("select precision(cast('1.1' as decimal))");
    AnalyzesOk("select scale(1.1)");
    AnalysisError("select scale('1.1')",
        "No matching function with signature: scale(STRING).");

    AnalyzesOk("select round(cast('1.1' as decimal), cast(1 as int))");
    // 1 is a tinyint, so the function is not a perfect match
    AnalyzesOk("select round(cast('1.1' as decimal), 1)");

    // No matching signature for complex type.
    AnalysisError("select lower(tiny_struct) from " +
        "functional_orc_def.complextypes_structs",
        "No matching function with signature: lower(STRUCT<b:BOOLEAN>).");

    // Special cases for FROM in EXTRACT function call
    AnalyzesOk("select extract(year from now())");
    AnalyzesOk("select extract(year from cast(now() as date))");
    AnalyzesOk("select extract(year from date_col) from functional.date_tbl");
    AnalyzesOk("select extract(hour from now())");
    AnalysisError("select extract(hour from cast(now() as date))",
        "Time unit 'hour' in expression 'EXTRACT(hour FROM CAST(now() AS DATE))' is " +
        "invalid. Expected one of YEAR, QUARTER, MONTH, DAY.");
    AnalysisError("select extract(foo from now())",
        "Time unit 'foo' in expression 'EXTRACT(foo FROM now())' is invalid. Expected " +
        "one of YEAR, QUARTER, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, EPOCH.");
    AnalysisError("select extract(foo from date_col) from functional.date_tbl",
        "Time unit 'foo' in expression 'EXTRACT(foo FROM date_col)' is " +
        "invalid. Expected one of YEAR, QUARTER, MONTH, DAY.");
    AnalysisError("select extract(year from 0)",
        "Expression '0' in 'EXTRACT(year FROM 0)' has a return type of TINYINT but a " +
        "TIMESTAMP or DATE is required.");
    AnalysisError("select functional.extract(year from now())",
        "Function functional.extract conflicts with the EXTRACT builtin");
    AnalysisError("select date_part(year from now())",
        "Function DATE_PART does not accept the keyword FROM");

    // Special cases for FROM in TRIM in function call
    // TRIM(where FROM string): trim space by default
    AnalyzesOk("select trim(trailing FROM '     &@&127+  &@   ')");
    AnalyzesOk("select tRIm(trailing FROM '     &@&127+  &@   ')");
    AnalyzesOk("select utf8_trim(trailing FROM '     &@&127+  &@   ')");
    AnalysisError("select trim(foo from '     &@&127+  &@   ')",
        "Trim option 'foo' in expression 'trim(foo FROM '     &@&127+  &@   ')' is " +
        "invalid. Expected one of: LEADING, TRAILING, BOTH.");
    AnalysisError("select utf8_trim(foo from '     &@&127+  &@   ')",
        "Trim option 'foo' in expression 'utf8_trim(foo FROM '     &@&127+  &@   ')' " +
        "is invalid. Expected one of: LEADING, TRAILING, BOTH.");
    AnalysisError("select trim(leading from 0)",
        "Expression '0' has a return type of TINYINT but a STRING is required.");
    AnalysisError("select date_part(both from ' xyz ')",
        "Function DATE_PART does not accept the keyword FROM.");

    // TRIM(string FROM string): trim from both sides by default
    AnalyzesOk("select trim('+-*' from '&$^$)*(^*^++--*')");
    AnalyzesOk("select trim(NULL from '&$^$)*(^*^++--*')");
    AnalyzesOk("select trim('' from '&$^$)*(^*^++--*')");
    AnalysisError("select trim('cosmos' from 10)",
        "Expression '10' has a return type of TINYINT but a STRING is required.");
    AnalysisError("select trim(100 from '  universe  ');",
        "Expression '100' has a return type of TINYINT but a STRING is required.");
    AnalysisError("select trim(3.1415926 from 2.718281828);",
        "Expression '3.1415926' has a return type of DECIMAL(8,7) but a STRING is " +
        "required. Expression '2.718281828' has a return type of DECIMAL(10,9) but a " +
        "STRING is required.");
    AnalysisError("select trim(int_col from 'abc') from functional.alltypes",
        "Expression 'int_col' has a return type of INT but a STRING is required.");

    // TRIM(where string FROM string): regular test cases
    AnalyzesOk("select trim(trailing 'a0-' from 'c2aa0a+&$%-a00000-a')");
    AnalyzesOk("select trim(leAdiNG 'rt' From 'rrrrssssstttttt')");
    AnalyzesOk("select trim(tRAIlinG 'rt' FROM 'rrrrssssstttttt')");
    AnalysisError("select trim(xyz ' ' from now())",
        "Syntax error in line 1:\n" +
        "select trim(xyz ' ' from now())\n" +
        "                ^\n" +
        "Encountered: STRING LITERAL\n" +
        "Expected: AND, BETWEEN, DIV, FROM, IGNORE, ILIKE, IN, IREGEXP, IS, LIKE, ||, " +
        "NOT, OR, REGEXP, RLIKE, COMMA\n\n");
    AnalysisError("select trim(both ' ' from now())",
        "Expression 'now()' has a return type of TIMESTAMP but a STRING is required.");
    AnalysisError("select trim(xyz ' ' from ' Quantum ')",
        "Syntax error in line 1:\n" +
        "select trim(xyz ' ' from ' Quantum ')\n" +
        "                ^\n" +
        "Encountered: STRING LITERAL\n" +
        "Expected: AND, BETWEEN, DIV, FROM, IGNORE, ILIKE, IN, IREGEXP, IS, LIKE, ||, " +
        "NOT, OR, REGEXP, RLIKE, COMMA\n\n");
    AnalysisError("select trim(both 6.022140857E23 from '  Avogadro constant ')",
        "Expression '6.022140857E+23' has a return type of DECIMAL(24,0) but a STRING " +
        "is required.");
    AnalysisError("select trim(both 1.6E-19 from 6.62606896E-34)",
        "Expression '1.6E-19' has a return type of DECIMAL(20,20) but a STRING is " +
        "required. Expression '6.62606896E-34' has a return type of DOUBLE but a " +
        "STRING is required.");

    // IGNORE NULLS may only be used with first_value/last_value
    AnalysisError("select lower('FOO' ignore nulls)",
        "Function LOWER does not accept the keyword IGNORE NULLS.");

    // NVL2() is converted to IF() before analysis.
    AnalyzesOk("select nvl2(1, 'not null', 'null')");
    AnalyzesOk("select nvl2(null, 'not null', 'null')");
    AnalyzesOk("select nvl2('null', 'not null', 'null')");
    AnalyzesOk("select nvl2(int_col, 'not null', 'null') from functional.alltypesagg");
    AnalyzesOk("select nvl2(int_col, extract (year from now()), extract (month from " +
        "now())) from functional.alltypesagg");
    AnalyzesOk("select nvl2(nvl2(null, 1, 2), 'not null', 'null')");
    AnalyzesOk("select nvl2(nvl2(null, null, null), nvl2(null, 'not null', 'null'), " +
        "nvl2(2, 'not null', 'null'))");
    AnalyzesOk("select int_col from functional.alltypesagg where nvl2(int_col, true, " +
        "false)");
    AnalysisError("select nvl2('null', true, '4')", "No matching function with " +
        "signature: if(BOOLEAN, BOOLEAN, STRING).");
    AnalysisError("select nvl2(now(), true)", "No matching function with signature: " +
        "if(BOOLEAN, BOOLEAN).");
    AnalysisError("select nvl2()", "No matching function with signature: if().");

    // IFNULL() is converted to IF() before analysis.
    AnalyzesOk("select nullif(1, 1)");
    AnalyzesOk("select nullif(NULL, 'not null')");
    AnalyzesOk("select nullif('not null', null)");
    AnalyzesOk("select nullif(int_col, int_col) from functional.alltypesagg");
    // Because of conversion, nullif() isn't found rather than having no
    // matching function with the signature.
    AnalysisError("select nullif(1,2,3)", "default.nullif() unknown");
    AnalysisError("select nullif('x', 1)",
        "operands of type STRING and TINYINT are not comparable: 'x' IS DISTINCT FROM 1");

    // Check limited function support for BINARY.
    AnalyzesOk("select length(cast('a' as binary))");
    AnalysisError("select lower(cast('a' as binary))",
        "No matching function with signature: lower(BINARY).");
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
    AnalyzesOk("select case when 20 > 10 then date '2001-01-01' else " +
        "cast('2001-01-01' as timestamp) end");
    // First when expr doesn't return boolean.
    AnalysisError("select case when 20 then 20 when 1 > 2 then timestamp_col " +
        "when 4 < 5 then 2 else 15 end from functional.alltypes",
        "When expr '20' is not of type boolean and not castable to type boolean.");
    AnalysisError("select case when int_array_col then 20 when 1 > 2 then id end " +
        "from functional.allcomplextypes",
        "When expr 'int_array_col' is not of type boolean and not castable to " +
        "type boolean.");
    // Then exprs return incompatible types.
    AnalysisError("select case when 20 > 10 then 20 when 1 > 2 then timestamp_col " +
        "when 4 < 5 then 2 else 15 end from functional.alltypes",
        "Incompatible return types 'TINYINT' and 'TIMESTAMP' " +
         "of exprs '20' and 'timestamp_col'.");
    AnalysisError("select case when 20 > 10 then 20 when 1 > 2 then date_col " +
        "when 4 < 5 then 2 else 15 end from functional.date_tbl",
        "Incompatible return types 'TINYINT' and 'DATE' " +
         "of exprs '20' and 'date_col'.");
    AnalysisError("select case when 20 > 10 then 20 when 1 > 2 then int_map_col " +
        "else 15 end from functional.allcomplextypes",
        "Incompatible return types 'TINYINT' and 'MAP<STRING,INT>' of exprs " +
        "'20' and 'int_map_col'.");

    // With case expr.
    AnalyzesOk("select case int_col when 20 then 30 else 15 end " +
        "from functional.alltypes");
    // No else.
    AnalyzesOk("select case int_col when 20 then 30 end " +
        "from functional.alltypes");
    // Requires casting case expr.
    AnalyzesOk("select case int_col when bigint_col then 30 else 15 end " +
        "from functional.alltypes");
    AnalyzesOk("select case date_col when timestamp_col then 30 else " +
        "15 end from functional.date_tbl, functional.alltypes");
    // Requires casting when expr.
    AnalyzesOk("select case bigint_col when int_col then 30 else 15 end " +
        "from functional.alltypes");
    AnalyzesOk("select case timestamp_col when date_col then 30 else 15 end " +
        "from functional.alltypes, functional.date_tbl");
    // Requires multiple casts.
    AnalyzesOk("select case bigint_col when int_col then 30 " +
        "when double_col then 1.0 else 15 end from functional.alltypes");
    // Type of case expr is incompatible with first when expr.
    AnalysisError("select case bigint_col when timestamp_col then 30 " +
        "when double_col then 1.0 else 15 end from functional.alltypes",
        "Incompatible return types 'BIGINT' and 'TIMESTAMP' " +
        "of exprs 'bigint_col' and 'timestamp_col'.");
    AnalysisError("select case bigint_col when date_col then 30 " +
        "when double_col then 1.0 else 15 end from functional.alltypes, " +
        "functional.date_tbl",
        "Incompatible return types 'BIGINT' and 'DATE' " +
        "of exprs 'bigint_col' and 'date_col'.");
    // Then exprs return incompatible types.
    AnalysisError("select case bigint_col when int_col then 30 " +
        "when double_col then timestamp_col else 15 end from functional.alltypes",
        "Incompatible return types 'TINYINT' and 'TIMESTAMP' " +
         "of exprs '30' and 'timestamp_col'.");
    AnalysisError("select case bigint_col when int_col then 30 " +
        "when double_col then date_col else 15 end from functional.alltypes, " +
        "functional.date_tbl",
        "Incompatible return types 'TINYINT' and 'DATE' " +
         "of exprs '30' and 'date_col'.");

    // Test different type classes (all types are tested in BE tests).
    AnalyzesOk("select case when true then 1 end");
    AnalyzesOk("select case when true then 1.0 end");
    AnalyzesOk("select case when true then 'abc' end");
    AnalyzesOk("select case when true then cast('2011-01-01 09:01:01' " +
        "as timestamp) end");
    AnalyzesOk("select case when true then date '2011-01-01' end");
    // Test NULLs.
    AnalyzesOk("select case NULL when 1 then 2 else 3 end");
    AnalyzesOk("select case 1 when NULL then 2 else 3 end");
    AnalyzesOk("select case 1 when 2 then NULL else 3 end");
    AnalyzesOk("select case 1 when 2 then 3 else NULL end");
    AnalyzesOk("select case NULL when NULL then NULL else NULL end");

    // IMPALA-3155: Verify that a CHAR type is returned when all THEN
    // exprs are of type CHAR.
    checkReturnType("select case when 5 < 3 then cast('L'  as char(1)) else " +
        "cast('M'  as char(1)) end", ScalarType.createCharType(1));
    // IMPALA-3155: Verify that a STRING type is returned when at least one THEN
    // expr is of type STRING.
    checkReturnType("select case when 5 < 3 then cast('L'  as string) else " +
        "cast('M'  as char(1)) end", ScalarType.STRING);
  }

  @Test
  public void TestDecodeExpr() throws AnalysisException {
    AnalyzesOk("select decode(1, 1, 1)");
    AnalyzesOk("select decode(1, 1, 'foo')");
    AnalyzesOk("select decode(1, 2, true, false)");
    AnalyzesOk("select decode(null, null, null, null, null, null)");
    assertCaseEquivalence(
        "CASE WHEN 1 = 2 THEN NULL ELSE 'foo' END",
        "decode(1, 2, NULL, 'foo')");
    assertCaseEquivalence(
        "CASE WHEN 1 = 2 THEN NULL ELSE 4 END",
        "decode(1, 2, NULL, 4)");
    assertCaseEquivalence(
        "CASE WHEN string_col = 'a' THEN 1 WHEN string_col = 'b' THEN 2 ELSE 3 END",
        "decode(string_col, 'a', 1, 'b', 2, 3)");
    assertCaseEquivalence(
        "CASE WHEN int_col IS NULL AND bigint_col IS NULL "
            + "OR int_col = bigint_col THEN tinyint_col ELSE smallint_col END",
        "decode(int_col, bigint_col, tinyint_col, smallint_col)");
    assertCaseEquivalence(
        "CASE WHEN int_col = 1 THEN 1 WHEN int_col IS NULL AND bigint_col IS NULL OR "
            + "int_col = bigint_col THEN 2 WHEN int_col IS NULL THEN 3 ELSE 4 END",
        "decode(int_col, 1, 1, bigint_col, 2, NULL, 3, 4)");
    assertCaseEquivalence(
        "CASE WHEN NULL IS NULL THEN NULL ELSE NULL END",
        "decode(null, null, null, null)");

    AnalysisError("select decode()",
        "DECODE in 'decode()' requires at least 3 arguments");
    AnalysisError("select decode(1)",
        "DECODE in 'decode(1)' requires at least 3 arguments");
    AnalysisError("select decode(1, 2)",
        "DECODE in 'decode(1, 2)' requires at least 3 arguments");
    AnalysisError("select decode(*)", "Cannot pass '*'");
    AnalysisError("select decode(distinct 1, 2, 3)", "Cannot pass 'DISTINCT'");
    AnalysisError("select decode(true, 'foo', 1)",
        "operands of type BOOLEAN and STRING are not comparable: TRUE = 'foo'");
    AnalysisError("select functional.decode(1, 1, 1)", "functional.decode() unknown");
  }

  /**
   * Assert that the caseSql and decodeSql have the same underlying child expr
   * and thrift representation.
   */
  void assertCaseEquivalence(String caseSql, String decodeSql)
      throws AnalysisException {
    String sqlTemplate = "select %s from functional.alltypes";
    SelectStmt stmt = (SelectStmt)AnalyzesOk(String.format(sqlTemplate, caseSql));
    CaseExpr caseExpr =
        (CaseExpr)stmt.getSelectList().getItems().get(0).getExpr();
    makeExprExecutable(caseExpr, stmt.getAnalyzer());
    TExpr caseThrift = caseExpr.treeToThrift();
    stmt = (SelectStmt)AnalyzesOk(String.format(sqlTemplate, decodeSql));
    CaseExpr decodeExpr =
        (CaseExpr)stmt.getSelectList().getItems().get(0).getExpr();
    Assert.assertEquals(caseSql, decodeExpr.toCaseSql());
    makeExprExecutable(caseExpr, stmt.getAnalyzer());
    Assert.assertEquals(caseThrift, decodeExpr.treeToThrift());
  }

  /**
   * Marks all slots referenced by 'e' as materialized. Also marks all referenced tuples
   * as materialized and computes their mem layout.
   */
  private void makeExprExecutable(Expr e, Analyzer analyzer) {
    List<TupleId> tids = new ArrayList<>();
    List<SlotId> sids = new ArrayList<>();
    e.getIds(tids, sids);
    for (SlotId sid: sids) {
      SlotDescriptor slotDesc = analyzer.getDescTbl().getSlotDesc(sid);
      slotDesc.setIsMaterialized(true);
    }
    for (TupleId tid: tids) {
      TupleDescriptor tupleDesc = analyzer.getTupleDesc(tid);
      tupleDesc.setIsMaterialized(true);
      tupleDesc.computeMemLayout();
    }
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
    // No matching signature.
    AnalysisError("select if(true, tiny_struct, tiny_struct) " +
        "from functional_orc_def.complextypes_structs",
        "No matching function with signature: " +
        "if(BOOLEAN, STRUCT<b:BOOLEAN>, STRUCT<b:BOOLEAN>).");

    // if() only accepts three arguments
    AnalysisError("select if(true, false, true, true)",
        "No matching function with signature: if(BOOLEAN, BOOLEAN, BOOLEAN, " +
        "BOOLEAN).");
    AnalysisError("select if(true, false)",
        "No matching function with signature: if(BOOLEAN, BOOLEAN).");
    AnalysisError("select if(false)",
        "No matching function with signature: if(BOOLEAN).");

    // Test IsNull() conditional function.
    for (String literal : typeToLiteralValue_.values()) {
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
    // No matching signature.
    AnalysisError("select isnull(1, int_array_col) from functional.allcomplextypes",
        "No matching function with signature: isnull(TINYINT, ARRAY<INT>).");
  }

  @Test
  public void TestUdfs() {
    AnalysisError("select udf()", "default.udf() unknown");
    AnalysisError("select functional.udf()", "functional.udf() unknown");
    AnalysisError("select udf(1)", "default.udf() unknown");

    // Add a udf default.udf(), default.udf(int), default.udf(string...),
    // default.udf(int, string...) and functional.udf(double)
    catalog_.addFunction(ScalarFunction.createForTesting("default", "udf",
        new ArrayList<Type>(), Type.INT, "/dummy", "dummy.class", null, null,
        TFunctionBinaryType.NATIVE));
    catalog_.addFunction(ScalarFunction.createForTesting("default", "udf",
        Lists.<Type>newArrayList(Type.INT), Type.INT, "/dummy", "dummy.class",
        null, null, TFunctionBinaryType.NATIVE));
    ScalarFunction varArgsUdf1 = ScalarFunction.createForTesting("default",
        "udf", Lists.<Type>newArrayList(Type.STRING), Type.INT, "/dummy",
        "dummy.class", null, null, TFunctionBinaryType.NATIVE);
    varArgsUdf1.setHasVarArgs(true);
    catalog_.addFunction(varArgsUdf1);
    ScalarFunction varArgsUdf2 = ScalarFunction.createForTesting("default",
        "udf", Lists.<Type>newArrayList(Type.INT, Type.STRING), Type.INT,
        "/dummy", "dummy.class", null, null, TFunctionBinaryType.NATIVE);
    varArgsUdf2.setHasVarArgs(true);
    catalog_.addFunction(varArgsUdf2);
    ScalarFunction udf = ScalarFunction.createForTesting("functional", "udf",
        Lists.<Type>newArrayList(Type.DOUBLE), Type.INT, "/dummy",
        "dummy.class", null, null, TFunctionBinaryType.NATIVE);
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

  // Serializes to thrift and checks the mtime setting.
  private void checkSerializedMTime(Expr expr, boolean expectMTime) {
    Preconditions.checkNotNull(expr.getFn());
    TExpr thriftExpr = expr.treeToThrift();
    Preconditions.checkState(thriftExpr.getNodesSize() > 0);
    Preconditions.checkState(thriftExpr.getNodes().get(0).isSetFn());
    TFunction thriftFn = thriftExpr.getNodes().get(0).getFn();
    if (expectMTime) {
      assertTrue(thriftFn.getLast_modified_time() >= 0);
    } else {
      assertEquals(thriftFn.getLast_modified_time(), -1);
    }
  }

  // Checks that 'expr', when serialized to thrift, has mtime
  // set as expected. 'expr' must be a single function call.
  private void testMTime(String expr, boolean expectMTime) {
    SelectStmt stmt =
        (SelectStmt) AnalyzesOk("select " + expr + " from functional.alltypes");
    Preconditions.checkState(stmt.getSelectList().getItems().size() == 1);
    TupleDescriptor tblRefDesc = stmt.fromClause_.get(0).getDesc();
    tblRefDesc.materializeSlots();
    tblRefDesc.computeMemLayout();
    if (stmt.hasMultiAggInfo()) {
      Preconditions.checkState(stmt.getMultiAggInfo().getAggClasses().size() == 1);
      AggregateInfo aggInfo = stmt.getMultiAggInfo().getAggClass(0);
      TupleDescriptor intDesc = aggInfo.intermediateTupleDesc_;
      intDesc.materializeSlots();
      intDesc.computeMemLayout();
      checkSerializedMTime(aggInfo.getAggregateExprs().get(0), expectMTime);
      checkSerializedMTime(
          aggInfo.getMergeAggInfo().getAggregateExprs().get(0), expectMTime);
    } else {
      checkSerializedMTime(stmt.getSelectList().getItems().get(0).getExpr(), expectMTime);
    }
  }

  @Test
  public void TestFunctionMTime() {
    // Expect unset mtime for builtin.
    testMTime("sleep(1)", false);
    testMTime("concat('a', 'b')", false);
    testMTime("3 is null", false);
    testMTime("1 < 3", false);
    testMTime("1 + 1", false);
    testMTime("avg(int_col)", false);

    final String nativeSymbol =
        "'_Z8IdentityPN10impala_udf15FunctionContextERKNS_10BooleanValE'";
    final String nativeUdfPath = "/test-warehouse/libTestUdfs.so";
    final String javaSymbol = "SYMBOL='org.apache.impala.TestUdf";
    final String javaPath = "/test-warehouse/impala-hive-udfs.jar";
    final String nativeUdaPath = "hdfs://localhost:20500/test-warehouse/libTestUdas.so";

    // Spec for a bogus builtin that specifies a bogus path.
    catalog_.addFunction(ScalarFunction.createForTesting("default", "udf_builtin_bug",
        new ArrayList<Type>(), Type.INT, "/dummy", "dummy.class", null, null,
        TFunctionBinaryType.BUILTIN));
    // Valid specs for native and java udf/uda's.
    catalog_.addFunction(ScalarFunction.createForTesting("default", "udf_jar",
        Lists.<Type>newArrayList(Type.INT), Type.INT, javaPath, javaSymbol, null, null,
        TFunctionBinaryType.JAVA));
    catalog_.addFunction(ScalarFunction.createForTesting("default", "udf_native",
        new ArrayList<Type>(), Type.INT, nativeUdfPath, nativeSymbol, null, null,
        TFunctionBinaryType.NATIVE));
    catalog_.addFunction(AggregateFunction.createForTesting(
        new FunctionName("default", "uda"), Lists.<Type>newArrayList(Type.INT), Type.INT,
        Type.INT, new HdfsUri(nativeUdaPath), "init_fn_symbol", "update_fn_symbol", null,
        null, null, null, null, TFunctionBinaryType.NATIVE));

    // Expect these to have mtime set.
    testMTime("udf_builtin_bug()", false);
    testMTime("udf_jar(3)", true);
    testMTime("udf_native()", true);
    testMTime("uda(int_col)", true);
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
        String.format("Exceeded the maximum number of child expressions (%d).\n" +
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
        String.format("Exceeded the maximum number of child expressions (%d).\n" +
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
    ScalarFunction udf = ScalarFunction.createForTesting("default", "udf",
        Lists.<Type>newArrayList(Type.INT), Type.INT, "/dummy",
        "dummy.class", null, null, TFunctionBinaryType.NATIVE);
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

  /** Generates a specific number of expressions by repeating a column reference.
   * It generates a string containing 'num_copies' expressions. If 'use_alias' is true,
   * each column reference has a distinct alias. This allows the repeated columns to be
   * used in places that require distinct names (e.g. the definition of a view).
   */
  String getRepeatedColumnReference(String col_name, int num_copies, boolean use_alias) {
    StringBuilder repColRef = new StringBuilder();

    for (int i = 0; i < num_copies; ++i) {
      if (i != 0) repColRef.append(", ");
      repColRef.append(col_name);
      if (use_alias) repColRef.append(String.format(" alias%d", i));
    }
    return repColRef.toString();
  }

  /** Get the error message for a statement with 'actual_num_expressions' that exceeds
   * the statment_expression_limit 'limit'.
   */
  String getExpressionLimitErrorStr(int actual_num_expressions, int limit) {
    return String.format("Exceeded the statement expression limit (%d)\n" +
        "Statement has %d expressions", limit, actual_num_expressions);
  }

  @Test
  public void TestStatementExprLimit() {
    // To keep it simple and fast, we use a low value for statement_expression_limit.
    // Since the statement_expression_limit is evaluated before rewrites, turn off
    // expression rewrites.
    TQueryOptions queryOpts = new TQueryOptions();
    queryOpts.setStatement_expression_limit(20);
    queryOpts.setEnable_expr_rewrites(false);
    AnalysisContext ctx = createAnalysisCtx(queryOpts);

    // Known SQL patterns (repeated reference to column) that generate 20 and 21
    // expressions, respectively.
    String repCols20 = getRepeatedColumnReference("int_col", 20, true);
    String repCols21 = getRepeatedColumnReference("int_col", 21, true);

    // Select from table
    AnalyzesOk(String.format("select %s from functional.alltypes", repCols20), ctx);
    assertEquals(ctx.getAnalyzer().getNumStmtExprs(), 20);
    AnalysisError(String.format("select %s from functional.alltypes", repCols21),
        ctx, getExpressionLimitErrorStr(21, 20));

    // WHERE clause
    // This statement has 20 expressions without the WHERE clause, as tested above.
    // So, this will only be an error if the bool_col in the WHERE clause counts.
    AnalysisError(String.format("select %s from functional.alltypes where bool_col",
        repCols20), ctx, getExpressionLimitErrorStr(21, 20));

    // Create table as select
    AnalyzesOk(String.format("create table exprlimit1 as " +
        "select %s from functional.alltypes", repCols20), ctx);
    assertEquals(ctx.getAnalyzer().getNumStmtExprs(), 20);
    AnalysisError(String.format("create table exprlimit1 as " +
        "select %s from functional.alltypes", repCols21), ctx,
        getExpressionLimitErrorStr(21, 20));

    // Create view
    AnalyzesOk(String.format("create view exprlimit1 as " +
        "select %s from functional.alltypes", repCols20), ctx);
    assertEquals(ctx.getAnalyzer().getNumStmtExprs(), 20);
    AnalysisError(String.format("create view exprlimit1 as " +
        "select %s from functional.alltypes", repCols21), ctx,
        getExpressionLimitErrorStr(21, 20));

    // Subquery
    AnalyzesOk(String.format("select * from (select %s from functional.alltypes) x",
        repCols20), ctx);
    assertEquals(ctx.getAnalyzer().getNumStmtExprs(), 20);
    AnalysisError(String.format("select * from (select %s from functional.alltypes) x",
        repCols21), ctx, getExpressionLimitErrorStr(21, 20));

    // WITH clause
    AnalyzesOk(String.format("with v as (select %s from functional.alltypes) " +
        "select * from v", repCols20), ctx);
    assertEquals(ctx.getAnalyzer().getNumStmtExprs(), 20);
    AnalysisError(String.format("with v as (select %s from functional.alltypes) " +
        "select * from v", repCols21), ctx, getExpressionLimitErrorStr(21, 20));

    // View is counted (functional.alltypes_parens has a few expressions)
    AnalysisError(String.format("select %s from functional.alltypes_parens", repCols20),
        ctx, getExpressionLimitErrorStr(32, 20));

    // If a view is referenced multiple times, it counts multiple times.
    AnalysisError(String.format("with v as (select %s from functional.alltypes) " +
        "select * from v v1,v v2", repCols20), ctx, getExpressionLimitErrorStr(40, 20));

    // If a view is not referenced, it doesn't count.
    AnalyzesOk(String.format("with v as (select %s from functional.alltypes) select 1",
        repCols21), ctx);
    // This is zero because literals do not count.
    assertEquals(ctx.getAnalyzer().getNumStmtExprs(), 0);

    // EXPLAIN is not exempted from the limit
    AnalysisError(String.format("explain select %s from functional.alltypes", repCols21),
        ctx, getExpressionLimitErrorStr(21, 20));

    // Wide table doesn't impact *
    AnalyzesOk("select * from functional.widetable_1000_cols", ctx);

    // Literal expressions don't count towards the limit, so build a list of literals
    // to use to test various cases.
    StringBuilder literalList = new StringBuilder();
    for (int i = 0; i < 200; ++i) {
      if (i != 0) literalList.append(", ");
      literalList.append(i);
    }

    // Literals in the select list do not count
    AnalyzesOk(String.format("select %s", literalList.toString()), ctx);
    assertEquals(ctx.getAnalyzer().getNumStmtExprs(), 0);

    // Literals in an IN list do not count
    AnalyzesOk(String.format("select int_col IN (%s) from functional.alltypes",
        literalList.toString()), ctx);

    // Literals in a VALUES clause do not count
    StringBuilder valuesList = new StringBuilder();
    for (int i = 0; i < 200; ++i) {
      if (i != 0) valuesList.append(", ");
      valuesList.append("(" + i + ")");
    }
    AnalyzesOk("insert into functional.insert_overwrite_nopart (col1) VALUES " +
        valuesList.toString(), ctx);
    assertEquals(ctx.getAnalyzer().getNumStmtExprs(), 0);

    // Expressions that are operators applied to constants still count
    StringBuilder constantExpr = new StringBuilder();
    for (int i = 0; i < 21; ++i) {
      if (i != 0) constantExpr.append(" * ");
      constantExpr.append(i);
    }
    // 21 literals with 20 multiplications requires 20 ArithmeticExprs
    AnalyzesOk(String.format("select %s", constantExpr.toString()), ctx);
    assertEquals(ctx.getAnalyzer().getNumStmtExprs(), 20);
    constantExpr.append(" * 100");
    // 22 literals with 21 multiplications requires 21 ArithmeticExprs
    AnalysisError(String.format("select %s", constantExpr.toString()),
        ctx, getExpressionLimitErrorStr(21, 20));

    // Put a non-literal in an IN list to verify it still counts appropriately.
    StringBuilder inListExpr = new StringBuilder();
    for (int i = 0; i < 19; i++) {
      if (i != 0) inListExpr.append(" * ");
      inListExpr.append(i);
    }
    // 19 literals with 18 multiplications = 18 expressions. int_col and IN are another
    // two expressions. This has 20 expressions total.
    AnalyzesOk(String.format("select int_col IN (%s) from functional.alltypes",
        inListExpr.toString()), ctx);
    assertEquals(ctx.getAnalyzer().getNumStmtExprs(), 20);
    // Adding one more expression pushes us over the limit.
    inListExpr.append(" * 100");
    AnalysisError(String.format("select int_col IN (%s) from functional.alltypes",
        inListExpr.toString()), ctx, getExpressionLimitErrorStr(21, 20));
  }

  // Verifies the resulting expr decimal type is expectedType under decimal v1 or
  // decimal v2.
  private void testDecimalExpr(String expr, Type decimalExpectedType, boolean isV2) {
    TQueryOptions queryOpts = new TQueryOptions();

    queryOpts.setDecimal_v2(isV2);
    SelectStmt selectStmt =
        (SelectStmt) AnalyzesOk("select " + expr, createAnalysisCtx(queryOpts));
    Expr root = selectStmt.resultExprs_.get(0);
    Type actualType = root.getType();
    Assert.assertTrue(
        "Expr: " + expr + " Decimal Version: " + (isV2? "2" : "1") +
        " Expected: " + decimalExpectedType + " Actual: " + actualType,
        decimalExpectedType.equals(actualType));
  }

  // Verifies the resulting expr decimal type is expectedType under decimal v1.
  private void testDecimalExprV1(String expr, Type decimalExpectedType) {
    testDecimalExpr(expr, decimalExpectedType, false);
  }

  // Verifies the resulting expr decimal type is expectedType under decimal v2.
  private void testDecimalExprV2(String expr, Type decimalExpectedType) {
    testDecimalExpr(expr, decimalExpectedType, true);
  }

  // Verifies the resulting expr decimal type is expectedType under decimal v1 and
  // decimal v2.
  private void testDecimalExpr(String expr,
      Type decimalV1ExpectedType, Type decimalV2ExpectedType) {
    testDecimalExprV1(expr, decimalV1ExpectedType);
    testDecimalExprV2(expr, decimalV2ExpectedType);
  }

  // Verifies the resulting expr decimal type is exptectedType
  private void testDecimalExpr(String expr, Type expectedType) {
    testDecimalExpr(expr, expectedType, expectedType);
  }

  // Verify that mod and % returns the same type when it's DECIMAL V2 mode.
  // See IMPALA-6202.
  @Test
  public void TestModReturnType() {
    testDecimalExprV2("mod(9.8, 3)", ScalarType.createDecimalType(2,1));
    testDecimalExprV2("9.7 % 3", ScalarType.createDecimalType(2,1));

    testDecimalExprV2("mod(109.8, 3)", ScalarType.createDecimalType(4,1));
    testDecimalExprV2("109.7 % 3", ScalarType.createDecimalType(4,1));

    testDecimalExprV2("mod(109.8, 3.45)", ScalarType.createDecimalType(3,2));
    testDecimalExprV2("109.7 % 3.45", ScalarType.createDecimalType(3,2));

    testDecimalExprV1("mod(9.6, 3)", ScalarType.createDecimalType(4,1));
    testDecimalExprV1("9.5 % 3", Type.DOUBLE);
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
        ScalarType.createDecimalType(20, 0), ScalarType.createDecimalType(21, 0));
    testDecimalExpr(decimal_10_0 + " / " + decimal_10_0,
        ScalarType.createDecimalType(21, 11));
    testDecimalExpr(decimal_10_0 + " % " + decimal_10_0,
        ScalarType.createDecimalType(10, 0));

    testDecimalExpr(decimal_10_0 + " + " + decimal_5_5,
        ScalarType.createDecimalType(16, 5));
    testDecimalExpr(decimal_10_0 + " - " + decimal_5_5,
        ScalarType.createDecimalType(16, 5));
    testDecimalExpr(decimal_10_0 + " * " + decimal_5_5,
        ScalarType.createDecimalType(15, 5), ScalarType.createDecimalType(16, 5));
    testDecimalExpr(decimal_10_0 + " / " + decimal_5_5,
        ScalarType.createDecimalType(21, 6));
    testDecimalExpr(decimal_10_0 + " % " + decimal_5_5,
        ScalarType.createDecimalType(5, 5));

    testDecimalExpr(decimal_5_5 + " + " + decimal_10_0,
        ScalarType.createDecimalType(16, 5));
    testDecimalExpr(decimal_5_5 + " - " + decimal_10_0,
        ScalarType.createDecimalType(16, 5));
    testDecimalExpr(decimal_5_5 + " * " + decimal_10_0,
        ScalarType.createDecimalType(15, 5), ScalarType.createDecimalType(16, 5));
    testDecimalExpr(decimal_5_5 + " / " + decimal_10_0,
        ScalarType.createDecimalType(16, 16));
    testDecimalExpr(decimal_5_5 + " % " + decimal_10_0,
        ScalarType.createDecimalType(5, 5));

    // Test some overflow cases.
    testDecimalExpr(decimal_10_0 + " + " + decimal_38_34,
        ScalarType.createDecimalType(38, 34), ScalarType.createDecimalType(38, 27));
    testDecimalExpr(decimal_10_0 + " - " + decimal_38_34,
        ScalarType.createDecimalType(38, 34), ScalarType.createDecimalType(38, 27));
    testDecimalExpr(decimal_10_0 + " * " + decimal_38_34,
        ScalarType.createDecimalType(38, 34), ScalarType.createDecimalType(38, 23));
    testDecimalExpr(decimal_10_0 + " / " + decimal_38_34,
        ScalarType.createDecimalType(38, 34), ScalarType.createDecimalType(38, 6));
    testDecimalExpr(decimal_10_0 + " % " + decimal_38_34,
        ScalarType.createDecimalType(38, 34));

    testDecimalExpr(decimal_38_34 + " + " + decimal_5_5,
        ScalarType.createDecimalType(38, 34), ScalarType.createDecimalType(38, 33));
    testDecimalExpr(decimal_38_34 + " - " + decimal_5_5,
        ScalarType.createDecimalType(38, 34), ScalarType.createDecimalType(38, 33));
    testDecimalExpr(decimal_38_34 + " * " + decimal_5_5,
        ScalarType.createDecimalType(38, 38), ScalarType.createDecimalType(38, 33));
    testDecimalExpr(decimal_38_34 + " / " + decimal_5_5,
        ScalarType.createDecimalType(38, 34), ScalarType.createDecimalType(38, 29));
    testDecimalExpr(decimal_38_34 + " % " + decimal_5_5,
        ScalarType.createDecimalType(34, 34));

    testDecimalExpr(decimal_10_0 + " + " + decimal_10_0 + " + " + decimal_10_0,
        ScalarType.createDecimalType(12, 0));
    testDecimalExpr(decimal_10_0 + " - " + decimal_10_0 + " * " + decimal_10_0,
        ScalarType.createDecimalType(21, 0), ScalarType.createDecimalType(22, 0));
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
        ScalarType.createDecimalType(38, 9), ScalarType.createDecimalType(38, 8));
    testDecimalExpr(decimal_10_0 + " + cast(1 as double)",
        ScalarType.createDecimalType(38, 17), ScalarType.createDecimalType(38, 16));

    testDecimalExpr(decimal_5_5 + " + cast(1 as tinyint)",
        ScalarType.createDecimalType(9, 5));
    testDecimalExpr(decimal_5_5 + " - cast(1 as smallint)",
        ScalarType.createDecimalType(11, 5));
    testDecimalExpr(decimal_5_5 + " * cast(1 as int)",
        ScalarType.createDecimalType(15, 5), ScalarType.createDecimalType(16, 5));
    testDecimalExpr(decimal_5_5 + " % cast(1 as bigint)",
        ScalarType.createDecimalType(5, 5));
    testDecimalExpr(decimal_5_5 + " / cast(1 as float)",
        ScalarType.createDecimalType(38, 9), ScalarType.createDecimalType(38, 29));
    testDecimalExpr(decimal_5_5 + " + cast(1 as double)",
        ScalarType.createDecimalType(38, 17), ScalarType.createDecimalType(38, 16));

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
    AnalysisError("select " + decimal_5_5 + " + \"DATE '1970-01-01'\"",
        "Arithmetic operation requires numeric operands: "
        + "CAST(1 AS DECIMAL(5,5)) + 'DATE \\\'1970-01-01\\\''");

    AnalysisError("select " + decimal_5_5 + " = 'abcd'",
        "operands of type DECIMAL(5,5) and STRING are not comparable: " +
        "CAST(1 AS DECIMAL(5,5)) = 'abcd'");
    AnalysisError("select " + decimal_5_5 + " > 'cast(1 as timestamp)'",
        "operands of type DECIMAL(5,5) and STRING are not comparable: "
        + "CAST(1 AS DECIMAL(5,5)) > 'cast(1 as timestamp)'");
    AnalysisError("select " + decimal_5_5 + " > date '1899-12-23'",
        "operands of type DECIMAL(5,5) and DATE are not comparable: "
        + "CAST(1 AS DECIMAL(5,5)) > DATE '1899-12-23'");

    // IMPALA-7254: Inconsistent decimal behavior when finding a compatible type.
    // for casting.
    // In predicate that does not result in a new precision.
    AnalyzesOk("select cast(2 as double) in " +
        "(cast(2 as decimal(38, 37)), cast(3 as decimal(38, 37)))");
    AnalyzesOk("select cast(2 as decimal(38, 37)) in " +
        "(cast(2 as double), cast(3 as decimal(38, 37)))");
    AnalyzesOk("select cast(2 as decimal(38, 37)) in " +
        "(cast(2 as decimal(38, 37)), cast(3 as double))");
    // In predicate that results in a precision.
    AnalyzesOk("select cast(2 as double) in " +
        "(cast(2 as decimal(5, 3)), cast(3 as decimal(10, 5)))");
    AnalyzesOk("select cast(2 as decimal(5, 3)) in " +
        "(cast(2 as double), cast(3 as decimal(10, 5)))");
    AnalyzesOk("select cast(2 as decimal(5, 3)) in " +
        "(cast(2 as decimal(10, 5)), cast(3 as double))");
    // In predicate that results in a loss of precision.
    AnalysisError("select cast(2 as decimal(38, 37)) in (cast(2 as decimal(38, 1)))",
        "Incompatible return types 'DECIMAL(38,37)' and 'DECIMAL(38,1)' of exprs " +
        "'CAST(2 AS DECIMAL(38,37))' and 'CAST(2 AS DECIMAL(38,1))'");
    AnalyzesOk("select cast(2 as double) in " +
        "(cast(2 as decimal(38, 37)), cast(3 as decimal(38, 1)))");
    AnalyzesOk("select cast(2 as decimal(38, 37)) in " +
        "(cast(2 as double), cast(3 as decimal(38, 1)))");
    AnalyzesOk("select cast(2 as decimal(38, 37)) in " +
        "(cast(2 as decimal(38, 1)), cast(3 as double))");
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
        "'~' operation only allowed on integer types: ~d1");
    AnalysisError("select d1! from functional.decimal_tbl",
        "'!' operation only allowed on integer types: d1!");
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
        "Decimal precision must be > 0: 0");
    AnalysisError("select cast(1 as decimal(39))",
        "Decimal precision must be <= 38: 39");
    AnalysisError("select cast(1 as decimal(1, 2))",
        "Decimal scale (2) must be <= precision (1)");
  }

  @Test
  public void TestDecimalFunctions() throws AnalysisException {
    final String [] aliasesOfTruncate = new String[]{"truncate", "dtrunc", "trunc"};
    AnalyzesOk("select abs(cast(1 as decimal))");
    AnalyzesOk("select abs(cast(-1.1 as decimal(10,3)))");

    AnalyzesOk("select floor(cast(-1.1 as decimal(10,3)))");
    AnalyzesOk("select ceil(cast(1.123 as decimal(10,3)))");

    AnalyzesOk("select round(cast(1.123 as decimal(10,3)))");
    AnalyzesOk("select round(cast(1.123 as decimal(10,3)), 0)");
    AnalyzesOk("select round(cast(1.123 as decimal(10,3)), 2)");
    AnalyzesOk("select round(cast(1.123 as decimal(10,3)), 5)");
    AnalyzesOk("select round(cast(1.123 as decimal(10,3)), -2)");

    for (final String alias : aliasesOfTruncate) {
        AnalyzesOk(String.format("select %s(cast(1.123 as decimal(10,3)))", alias));
        AnalyzesOk(String.format("select %s(cast(1.123 as decimal(10,3)), 0)", alias));
        AnalyzesOk(String.format("select %s(cast(1.123 as decimal(10,3)), 2)", alias));
        AnalyzesOk(String.format("select %s(cast(1.123 as decimal(10,3)), 5)", alias));
        AnalyzesOk(String.format("select %s(cast(1.123 as decimal(10,3)), -1)", alias));
    }

    AnalysisError("select round(cast(1.123 as decimal(10,3)), 5.1)",
        "No matching function with signature: round(DECIMAL(10,3), DECIMAL(2,1))");
    AnalyzesOk("select round(cast(1.123 as decimal(30,20)), 40)");
    for (final String alias : aliasesOfTruncate) {
        AnalyzesOk(String.format("select %s(cast(1.123 as decimal(10,3)), 40)", alias));
        AnalyzesOk(String.format("select %s(NULL)", alias));
        AnalysisError(String.format("select %s(NULL, 1)", alias),
            String.format("Cannot resolve DECIMAL precision and scale from NULL type " +
                "in %s function.", alias));
    }
    AnalysisError("select round(cast(1.123 as decimal(10,3)), NULL)",
        "round() cannot be called with a NULL second argument.");

    // This has 39 digits and can only be represented as a DOUBLE.
    AnalysisError("select precision(999999999999999999999999999999999999999.)",
        "No matching function with signature: precision(DOUBLE).");

    AnalysisError("select precision(cast(1 as float))",
        "No matching function with signature: precision(FLOAT)");

    AnalysisError("select precision(NULL)",
        "Cannot resolve DECIMAL precision and scale from NULL type in " +
            "precision function");
    AnalysisError("select scale(NULL)",
        "Cannot resolve DECIMAL precision and scale from NULL type in " +
            "scale function.");

    testDecimalExpr("round(1.23)", ScalarType.createDecimalType(2, 0));
    testDecimalExpr("round(1.23, 1)", ScalarType.createDecimalType(3, 1));
    testDecimalExpr("round(1.23, 0)", ScalarType.createDecimalType(2, 0));
    testDecimalExpr("round(1.23, 3)", ScalarType.createDecimalType(3, 2));
    testDecimalExpr("round(1.23, -1)", ScalarType.createDecimalType(2, 0));
    testDecimalExpr("round(1.23, -2)", ScalarType.createDecimalType(2, 0));
    testDecimalExpr("round(cast(1.23 as decimal(3,2)), -2)",
        ScalarType.createDecimalType(2, 0));
    testDecimalExpr("round(cast(1.23 as decimal(30, 20)), 40)",
        ScalarType.createDecimalType(30, 20));

    testDecimalExpr("ceil(123.45)", ScalarType.createDecimalType(4, 0));
    testDecimalExpr("floor(12.345)", ScalarType.createDecimalType(3, 0));

    for (final String alias : aliasesOfTruncate) {
        testDecimalExpr(String.format("%s(1.23)", alias),
            ScalarType.createDecimalType(1, 0));
        testDecimalExpr(String.format("%s(1.23, 1)", alias),
            ScalarType.createDecimalType(2, 1));
        testDecimalExpr(String.format("%s(1.23, 0)", alias),
            ScalarType.createDecimalType(1, 0));
        testDecimalExpr(String.format("%s(1.23, 3)", alias),
            ScalarType.createDecimalType(3, 2));
        testDecimalExpr(String.format("%s(1.23, -1)", alias),
            ScalarType.createDecimalType(1, 0));
        testDecimalExpr(String.format("%s(1.23, -2)", alias),
            ScalarType.createDecimalType(1, 0));
        testDecimalExpr(String.format("%s(cast(1.23 as decimal(30, 20)), 40)", alias),
            ScalarType.createDecimalType(30, 20));
    }

    AnalysisContext decimalV1Ctx = createAnalysisCtx();
    decimalV1Ctx.getQueryOptions().setDecimal_v2(false);
    AnalysisContext decimalV2Ctx = createAnalysisCtx();
    decimalV2Ctx.getQueryOptions().setDecimal_v2(true);

    testDecimalExpr("coalesce(cast(0.789 as decimal(19, 19)), " +
        "cast(123 as decimal(19, 0)))", ScalarType.createDecimalType(38, 19));
    AnalyzesOk("select coalesce(cast(0.789 as decimal(20, 20)), " +
            "cast(123 as decimal(19, 0)))", decimalV1Ctx);
    AnalysisError("select coalesce(cast(0.789 as decimal(20, 20)), " +
        "cast(123 as decimal(19, 0)))", decimalV2Ctx,
        "Cannot resolve DECIMAL types of the coalesce(DECIMAL(20,20), " +
        "DECIMAL(19,0)) function arguments. You need to wrap the arguments in a CAST.");

    testDecimalExpr("if(true, cast(0.789 as decimal(19, 19)), " +
        "cast(123 as decimal(19, 0)))", ScalarType.createDecimalType(38, 19));
    AnalyzesOk("select if(true, cast(0.789 as decimal(20, 20)), " +
            "cast(123 as decimal(19, 0)))", decimalV1Ctx);
    AnalysisError("select if(true, cast(0.789 as decimal(20, 20)), " +
        "cast(123 as decimal(19, 0)))", decimalV2Ctx,
        "Cannot resolve DECIMAL types of the if(BOOLEAN, " +
        "DECIMAL(20,20), DECIMAL(19,0)) function arguments. " +
        "You need to wrap the arguments in a CAST.");

    testDecimalExpr("isnull(cast(0.789 as decimal(19, 19)), " +
        "cast(123 as decimal(19, 0)))", ScalarType.createDecimalType(38, 19));
    AnalyzesOk("select isnull(cast(0.789 as decimal(20, 20)), " +
        "cast(123 as decimal(19, 0)))", decimalV1Ctx);
    AnalysisError("select isnull(cast(0.789 as decimal(20, 20)), " +
        "cast(123 as decimal(19, 0)))", decimalV2Ctx,
        "Cannot resolve DECIMAL types of the isnull(DECIMAL(20,20), " +
        "DECIMAL(19,0)) function arguments. You need to wrap the arguments in a CAST.");

    testDecimalExpr("case 1 when 0 then cast(0.789 as decimal(19, 19)) " +
        "else cast(123 as decimal(19, 0)) end", ScalarType.createDecimalType(38, 19));
    AnalyzesOk("select case 1 when 0 then cast(0.789 as decimal(19, 19)) " +
            "else cast(123 as decimal(20, 0)) end", decimalV1Ctx);
    AnalysisError("select case 1 when 0 then cast(0.789 as decimal(19, 19)) " +
        "else cast(123 as decimal(20, 0)) end", decimalV2Ctx,
        "Incompatible return types 'DECIMAL(19,19)' and 'DECIMAL(20,0)' " +
        "of exprs 'CAST(0.789 AS DECIMAL(19,19))' and 'CAST(123 AS DECIMAL(20,0))'.");
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
    exprStr.append(repeatSuffix);
    AnalysisError(exprStr.toString(),
        String.format("Exceeded the maximum depth of an expression tree (%s).",
        Expr.EXPR_DEPTH_LIMIT));

    // Test 1x the safe depth (already at 1x, append 1x).
    for (int i = 0; i < Expr.EXPR_DEPTH_LIMIT; ++i) {
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
        Expr.EXPR_DEPTH_LIMIT + 1),
        String.format("Exceeded the maximum depth of an expression tree (%s).",
        Expr.EXPR_DEPTH_LIMIT));
    // Test 2x the safe depth.
    AnalysisError("select " + getNestedFuncExpr(openFunc, baseArg, closeFunc,
        Expr.EXPR_DEPTH_LIMIT * 2),
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

  @Test
  public void TestAppxCountDistinctOption() throws AnalysisException {
    TQueryOptions queryOptions = new TQueryOptions();
    queryOptions.setAppx_count_distinct(true);

    // Accumulates count(distinct) for all columns of alltypesTbl or decimalTbl.
    List<String> countDistinctFns = new ArrayList<>();
    // Accumulates count(distinct) for all columns of both alltypesTbl and decimalTbl.
    List<String> allCountDistinctFns = new ArrayList<>();

    Table alltypesTbl = catalog_.getOrLoadTable("functional", "alltypes");
    for (Column col: alltypesTbl.getColumns()) {
      String colName = col.getName();
      // Test a single count(distinct) with some other aggs.
      String countDistinctFn = String.format("count(distinct %s)", colName);
      SelectStmt stmt = (SelectStmt) AnalyzesOk(String.format(
          "select %s, sum(distinct smallint_col), " +
          "avg(float_col), min(%s) " +
          "from functional.alltypes",
          countDistinctFn, colName), createAnalysisCtx(queryOptions));
      validateSingleColAppxCountDistinctRewrite(stmt, colName, 4, 1, 1);
      countDistinctFns.add(countDistinctFn);
    }
    // Test a single query with a count(distinct) on all columns of alltypesTbl.
    SelectStmt alltypesStmt = (SelectStmt) AnalyzesOk(String.format(
        "select %s from functional.alltypes",
        Joiner.on(",").join(countDistinctFns)), createAnalysisCtx(queryOptions));
    assertAllNdvAggExprs(alltypesStmt, alltypesTbl.getColumns().size());

    allCountDistinctFns.addAll(countDistinctFns);
    countDistinctFns.clear();
    Table decimalTbl = catalog_.getOrLoadTable("functional", "decimal_tbl");
    for (Column col: decimalTbl.getColumns()) {
      String colName = col.getName();
      // Test a single count(distinct) with some other aggs.
      SelectStmt stmt = (SelectStmt) AnalyzesOk(String.format(
          "select count(distinct %s), sum(distinct d1), " +
          "avg(d2), min(%s) " +
          "from functional.decimal_tbl",
          colName, colName), createAnalysisCtx(queryOptions));
      countDistinctFns.add(String.format("count(distinct %s)", colName));
      validateSingleColAppxCountDistinctRewrite(stmt, colName, 4, 1, 1);
    }
    // Test a single query with a count(distinct) on all columns of decimalTbl.
    SelectStmt decimalTblStmt = (SelectStmt) AnalyzesOk(String.format(
        "select %s from functional.decimal_tbl",
        Joiner.on(",").join(countDistinctFns)), createAnalysisCtx(queryOptions));
    assertAllNdvAggExprs(decimalTblStmt, decimalTbl.getColumns().size());

    allCountDistinctFns.addAll(countDistinctFns);
    countDistinctFns.clear();
    Table dateTbl = catalog_.getOrLoadTable("functional", "date_tbl");
    for (Column col: dateTbl.getColumns()) {
      String colName = col.getName();
      // Test a single count(distinct) with some other aggs.
      String countDistinctFn = String.format("count(distinct %s)", colName);
      SelectStmt stmt = (SelectStmt) AnalyzesOk(String.format(
          "select %s, avg(%s), min(%s) from functional.date_tbl",
          countDistinctFn, colName, colName), createAnalysisCtx(queryOptions));
      validateSingleColAppxCountDistinctRewrite(stmt, colName, 3, 0, 1);
      countDistinctFns.add(countDistinctFn);
    }
    // Test a single query with a count(distinct) on all columns of dateTbl.
    SelectStmt dateStmt = (SelectStmt) AnalyzesOk(String.format(
        "select %s from functional.date_tbl",
        Joiner.on(",").join(countDistinctFns)), createAnalysisCtx(queryOptions));
    assertAllNdvAggExprs(dateStmt, dateTbl.getColumns().size());

    allCountDistinctFns.addAll(countDistinctFns);

    // Test a single query with a count(distinct) on all columns of alltypes, decimalTbl
    // and dateTbl.
    SelectStmt comboStmt = (SelectStmt) AnalyzesOk(String.format(
        "select %s from functional.alltypes cross join functional.decimal_tbl " +
        "cross join functional.date_tbl",
        Joiner.on(",").join(allCountDistinctFns)), createAnalysisCtx(queryOptions));
    assertAllNdvAggExprs(comboStmt, alltypesTbl.getColumns().size() +
        decimalTbl.getColumns().size() + dateTbl.getColumns().size());

    // The rewrite does not work for multiple count() arguments.
    SelectStmt noRewriteStmt = (SelectStmt) AnalyzesOk(
        "select count(distinct int_col, bigint_col), " +
        "count(distinct string_col, float_col) from functional.alltypes");
    assertNoNdvAggExprs(noRewriteStmt, 2);

    // The rewrite only applies to the count() function.
    noRewriteStmt = (SelectStmt) AnalyzesOk(
        "select avg(distinct int_col), sum(distinct float_col) from functional.alltypes",
        createAnalysisCtx(queryOptions));
    assertNoNdvAggExprs(noRewriteStmt, 2);
  }

  // Checks that 'stmt' has two aggregate exprs - DISTINCT 'sum' and non-DISTINCT 'ndv'.
  private void validateSingleColAppxCountDistinctRewrite(
      SelectStmt stmt, String rewrittenColName, int expNumAggExprs,
      int expNumDistinctExprs, int expNumNdvExprs) {
    MultiAggregateInfo multiAggInfo = stmt.getMultiAggInfo();
    List<FunctionCallExpr> aggExprs = multiAggInfo.getAggExprs();
    assertEquals(expNumAggExprs, aggExprs.size());
    int numDistinctExprs = 0;
    int numNdvExprs = 0;
    for (FunctionCallExpr aggExpr : aggExprs) {
      if (aggExpr.isDistinct()) {
        assertEquals("sum", aggExpr.getFnName().toString());
        ++numDistinctExprs;
      }
      if (aggExpr.getFnName().toString().equals("ndv")) {
        assertEquals(ToSqlUtils.getIdentSql(rewrittenColName),
            aggExpr.getChild(0).toSql());
        ++numNdvExprs;
      }
    }
    assertEquals(expNumDistinctExprs, numDistinctExprs);
    assertEquals(expNumNdvExprs, numNdvExprs);
  }

  // Checks that all aggregate exprs in 'stmt' are non-DISTINCT 'ndv'.
  private void assertAllNdvAggExprs(SelectStmt stmt, int expectedNumAggExprs) {
    MultiAggregateInfo multiAggInfo = stmt.getMultiAggInfo();
    List<FunctionCallExpr> aggExprs = multiAggInfo.getAggExprs();
    assertEquals(expectedNumAggExprs, aggExprs.size());
    for (FunctionCallExpr aggExpr : aggExprs) {
      assertFalse(aggExpr.isDistinct());
      assertEquals("ndv", aggExpr.getFnName().toString());
    }
  }

  // Checks that all aggregate exprs in 'stmt' are DISTINCT and not 'ndv'.
  private void assertNoNdvAggExprs(SelectStmt stmt, int expectedNumAggExprs) {
    MultiAggregateInfo multiAggInfo = stmt.getMultiAggInfo();
    List<FunctionCallExpr> aggExprs = multiAggInfo.getAggExprs();
    assertEquals(expectedNumAggExprs, aggExprs.size());
    for (FunctionCallExpr aggExpr : aggExprs) {
      assertTrue(aggExpr.isDistinct());
      assertNotEquals("ndv", aggExpr.getFnName().toString());
    }
  }

  @Test
  // IMPALA-2233: Regression test for loss of precision through implicit casts.
  public void TestImplicitArgumentCasts() throws AnalysisException {
    FunctionName fnName = new FunctionName(BuiltinsDb.NAME, "greatest");
    Function tinyIntFn = new Function(fnName, new Type[] {ScalarType.DOUBLE},
        Type.DOUBLE, true);
    Function decimalFn = new Function(fnName,
        new Type[] {ScalarType.TINYINT, ScalarType.createDecimalType(30, 10)},
        Type.INVALID, false);
    Assert.assertFalse(tinyIntFn.compare(decimalFn, CompareMode.IS_SUPERTYPE_OF));
    Assert.assertTrue(tinyIntFn.compare(decimalFn,
        CompareMode.IS_NONSTRICT_SUPERTYPE_OF));
    // Check that this resolves to the decimal version of the function.
    Db db = catalog_.getDb(BuiltinsDb.NAME);
    Function foundFn = db.getFunction(decimalFn, CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    assertNotNull(foundFn);
    Assert.assertTrue(foundFn.getArgs()[0].isDecimal());

    // The double version of the function is a non-strict supertype if the arguments are
    // (double, decimal).
    Function doubleDecimalFn = new Function(fnName,
        new Type[] {ScalarType.DOUBLE, ScalarType.createDecimalType(30, 10)},
        Type.INVALID, false);
    foundFn = db.getFunction(doubleDecimalFn, CompareMode.IS_SUPERTYPE_OF);
    Assert.assertNull(foundFn);
    foundFn = db.getFunction(doubleDecimalFn, CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    assertNotNull(foundFn);
    Assert.assertEquals(Type.DOUBLE, foundFn.getArgs()[0]);

    // Float and any precision decimal can be matched to FLOAT non-strictly.
    Function floatDecimalFn = new Function(fnName,
        new Type[] {ScalarType.FLOAT, ScalarType.createDecimalType(10, 7)},
        Type.INVALID, false);
    foundFn = db.getFunction(floatDecimalFn, CompareMode.IS_SUPERTYPE_OF);
    Assert.assertNull(foundFn);
    foundFn = db.getFunction(floatDecimalFn, CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    assertNotNull(foundFn);
    Assert.assertEquals(Type.FLOAT, foundFn.getArgs()[0]);

    // Float and bigint should be matched to double.
    Function floatBigIntFn = new Function(fnName,
        new Type[] {ScalarType.FLOAT, ScalarType.BIGINT}, Type.INVALID, false);
    foundFn = db.getFunction(floatBigIntFn, CompareMode.IS_SUPERTYPE_OF);
    Assert.assertNotNull(foundFn);
    Assert.assertEquals(Type.DOUBLE, foundFn.getArgs()[0]);

    FunctionName lagFnName = new FunctionName(BuiltinsDb.NAME, "lag");
    // String should not be converted to timestamp or date if string overload available.
    Function lagStringFn = new Function(lagFnName,
        new Type[] {ScalarType.STRING, Type.TINYINT}, Type.INVALID, false);
    foundFn = db.getFunction(lagStringFn, CompareMode.IS_SUPERTYPE_OF);
    Assert.assertNotNull(foundFn);
    Assert.assertEquals(Type.STRING, foundFn.getArgs()[0]);

    // Date should not be converted to timestamp if date overload is available.
    Function lagDateFn = new Function(lagFnName,
        new Type[] {ScalarType.DATE, Type.TINYINT}, Type.INVALID, false);
    foundFn = db.getFunction(lagDateFn, CompareMode.IS_INDISTINGUISHABLE);
    Assert.assertNull(foundFn);
    foundFn = db.getFunction(lagDateFn, CompareMode.IS_SUPERTYPE_OF);
    Assert.assertNotNull(foundFn);
    Assert.assertEquals(Type.DATE, foundFn.getArgs()[0]);

    // String is matched non-strictly to date, instead of converting both string and date
    // arguments to timestamp.
    FunctionName ifFnName = new FunctionName(BuiltinsDb.NAME, "if");
    Function ifStringDateFn = new Function(ifFnName,
        new Type[] {ScalarType.BOOLEAN, ScalarType.STRING, ScalarType.DATE}, Type.INVALID,
            false);
    foundFn = db.getFunction(ifStringDateFn, CompareMode.IS_SUPERTYPE_OF);
    Assert.assertNull(foundFn);
    foundFn = db.getFunction(ifStringDateFn, CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    Assert.assertNotNull(foundFn);
    Assert.assertEquals(Type.DATE, foundFn.getArgs()[1]);
    Assert.assertEquals(Type.DATE, foundFn.getArgs()[2]);

    // Date is matched non-strictly to timestamp
    ifStringDateFn = new Function(ifFnName,
        new Type[] {ScalarType.BOOLEAN, ScalarType.TIMESTAMP, ScalarType.DATE},
            Type.INVALID, false);
    foundFn = db.getFunction(ifStringDateFn, CompareMode.IS_SUPERTYPE_OF);
    Assert.assertNull(foundFn);
    foundFn = db.getFunction(ifStringDateFn, CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    Assert.assertNotNull(foundFn);
    Assert.assertEquals(Type.TIMESTAMP, foundFn.getArgs()[1]);
    Assert.assertEquals(Type.TIMESTAMP, foundFn.getArgs()[2]);

    // String is matched non-strictly to timestamp if there's no string overload but both
    // timestamp and date overloads are available.
    FunctionName yearFnName = new FunctionName(BuiltinsDb.NAME, "year");
    Function yearStringFn = new Function(yearFnName,
        new Type[] {ScalarType.STRING}, Type.INT, false);
    foundFn = db.getFunction(yearStringFn, CompareMode.IS_SUPERTYPE_OF);
    Assert.assertNull(foundFn);
    foundFn = db.getFunction(yearStringFn, CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    Assert.assertNotNull(foundFn);
    Assert.assertEquals(Type.TIMESTAMP, foundFn.getArgs()[0]);
  }

  @Test
  public void TestCastFormatClauseFromString() throws AnalysisException {
    AnalysisError("select cast('05-01-2017' AS DATETIME FORMAT 'MM-dd-yyyy')",
        "Unsupported data type: DATETIME");
    AnalysisError("select cast('05-01-2017' AS INT FORMAT 'MM-dd-yyyy')",
        "FORMAT clause is not applicable from STRING to INT");
    AnalysisError("select cast('05-01-2017' AS STRING FORMAT 'MM-dd-yyyy')",
        "FORMAT clause is not applicable from STRING to STRING");
    AnalysisError("select cast('05-01-2017' AS BOOLEAN FORMAT 'MM-dd-yyyy')",
        "FORMAT clause is not applicable from STRING to BOOLEAN");
    AnalysisError("select cast('05-01-2017' AS DOUBLE FORMAT 'MM-dd-yyyy')",
        "FORMAT clause is not applicable from STRING to DOUBLE");
    AnalysisError("select cast('05-01-2017' AS TIMESTAMP FORMAT '')",
        "FORMAT clause can't be empty");
    AnalyzesOk("select cast('05-01-2017' AS TIMESTAMP FORMAT 'MM-dd-yyyy')");
    AnalyzesOk("select cast('05-01-2017' AS DATE FORMAT 'MM-dd-yyyy')");
  }

  @Test
  public void TestCastFormatClauseFromDatetime() throws AnalysisException {
    RunCastFormatTestOnType("TIMESTAMP");
    RunCastFormatTestOnType("DATE");
  }

  @Test
  public void TestMatchingCasts() throws AnalysisException {
    // IMPALA-12800: ensure identical cast-to-decimal expressions match hashCodes.
    String clause =
        "sum(CASE WHEN id IS NOT NULL THEN cast(0.4 as decimal(20,10)) ELSE 0 END)";
    AnalyzesOk("SELECT CASE WHEN (" + clause + ") > 0 " +
      "THEN (" + clause + ") ELSE null END q FROM functional.alltypes");
  }

  private void RunCastFormatTestOnType(String type) {
    String to_timestamp_cast = "cast('05-01-2017' as " + type + ")";
    AnalysisError(
        "select cast(" + to_timestamp_cast + " as DATETIME FORMAT 'MM-dd-yyyy')",
        "Unsupported data type: DATETIME");
    if (!type.equals("TIMESTAMP")) {
      AnalysisError(
          "select cast(" + to_timestamp_cast + " as TIMESTAMP FORMAT 'MM-dd-yyyy')",
          "FORMAT clause is not applicable from " + type + " to TIMESTAMP");
    }
    if (!type.equals("DATE")) {
      AnalysisError("select cast(" + to_timestamp_cast + " as DATE FORMAT 'MM-dd-yyyy')",
          "FORMAT clause is not applicable from " + type + " to DATE");
    }
    AnalysisError("select cast(" + to_timestamp_cast + " AS INT FORMAT 'MM-dd-yyyy')",
        "FORMAT clause is not applicable from " + type +" to INT");
    AnalysisError("select cast(" + to_timestamp_cast + " AS BOOLEAN FORMAT 'MM-dd-yyyy')",
        "FORMAT clause is not applicable from " + type + " to BOOLEAN");
    AnalysisError("select cast(" + to_timestamp_cast + " AS DOUBLE FORMAT 'MM-dd-yyyy')",
        "FORMAT clause is not applicable from " + type + " to DOUBLE");
    AnalysisError("select cast(" + to_timestamp_cast + " AS STRING FORMAT '')",
        "FORMAT clause can't be empty");
    AnalyzesOk("select cast(" + to_timestamp_cast + " AS STRING FORMAT 'MM-dd-yyyy')");
    AnalyzesOk("select cast(" + to_timestamp_cast + " AS VARCHAR FORMAT 'MM-dd-yyyy')");
    AnalyzesOk("select cast(" + to_timestamp_cast + " AS CHAR(10) FORMAT 'MM-dd-yyyy')");
  }

  @Test
  public void TestToStringOnCastFormatClause() throws AnalysisException {
    String cast_str = "CAST('05-01-2017' AS TIMESTAMP FORMAT 'MM-dd-yyyy')";
    SelectStmt select = (SelectStmt) AnalyzesOk("select " + cast_str);
    Assert.assertEquals(cast_str, select.getResultExprs().get(0).toSqlImpl());

    cast_str = "CAST('05-01-2017' AS DATE FORMAT 'MM-dd-yyyy')";
    select = (SelectStmt) AnalyzesOk("select " + cast_str);
    Assert.assertEquals(cast_str, select.getResultExprs().get(0).toSqlImpl());

    // Check that escaped single quotes in the format remain escaped in the printout.
    cast_str = "CAST('2019-01-01te\\\'xt' AS DATE FORMAT " +
        "'YYYY\\\'MM\\\'DD\"te\\\'xt\"')";
    select = (SelectStmt) AnalyzesOk("select " + cast_str);
    Assert.assertEquals(cast_str, select.getResultExprs().get(0).toSqlImpl());

    // Check that non-escaped single quotes in the format are escaped in the printout.
    // Also check that the surrounding double quotes around the format string are
    // replaced with single quotes in the output.
    cast_str = "select CAST(\"2019-01-02te'xt\" AS DATE FORMAT " +
        "\"YYYY'MM'DD\\\"te'xt\\\"\")";
    select = (SelectStmt) AnalyzesOk(cast_str);
    String expected_str = "CAST('2019-01-02te\\\'xt' AS DATE FORMAT " +
        "'YYYY\\\'MM\\\'DD\\\"te\\\'xt\\\"')";
    Assert.assertEquals(expected_str, select.getResultExprs().get(0).toSqlImpl());

    // Check that a single quote in a free text token is escaped in the output even if it
    // is after an escaped backslash.
    cast_str = "select CAST(\"2019-01-02te\\'xt\" AS DATE FORMAT " +
        "\"YYYY-MM-DD\\\"te\\\\'xt\\\"\")";
    select = (SelectStmt) AnalyzesOk(cast_str);
    expected_str = "CAST('2019-01-02te\\\'xt' AS DATE FORMAT " +
        "'YYYY-MM-DD\\\"te\\\\\\'xt\\\"')";
    Assert.assertEquals(expected_str, select.getResultExprs().get(0).toSqlImpl());
  }

  @Test
  public void testUnsafeCasts() {
    AnalysisContext unsafeCtx = createAnalysisCtx();
    unsafeCtx.getQueryOptions().setAllow_unsafe_casts(true);

    String[] numericTypes = {"tinyint", "smallint", "int", "bigint", "float", "double"};
    Pair<String, String>[] stringTypes =
        new Pair[] {Pair.create("string", "alltypesnopart"),
            Pair.create("varchar", "chars_medium"), Pair.create("char", "chars_medium")};

    for (String numericType : numericTypes) {
      for (Pair<String, String> stringType : stringTypes) {
        String numericToStringStatement =
            String.format("insert into functional.%s(%s_col) values(cast(100 as %s))",
                stringType.second, stringType.first, numericType);
        String stringToNumericStatement = String.format(
            "insert into functional.alltypesnopart(%s_col) values(\"100\")", numericType);
        String nonConstStatement = String.format(
            "insert into functional.alltypesnopart(%s_col) select string_col "
                + "from functional.alltypes",
            numericType);

        // Constant values are allowed
        AnalyzesOk(numericToStringStatement, unsafeCtx);
        AnalyzesOk(stringToNumericStatement, unsafeCtx);
        // Non-constant values are not allowed
        AnalysisError(nonConstStatement, unsafeCtx,
            "Unsafe implicit cast is prohibited for non-const expression: string_col");
      }
    }

    // Decimal is not allowed
    AnalysisError(
        "insert into functional.alltypesnopart(string_col) values (cast(100 as decimal))",
        unsafeCtx,
        "Target table 'functional.alltypesnopart' is incompatible with "
            + "source expressions.\nExpression 'cast(100 as decimal(9,0))' "
            + "(type: DECIMAL(9,0)) is not compatible with column "
            + "'string_col' (type: STRING)");
  }
}
