// Copyright 2013 Cloudera Inc.
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

import static org.junit.Assert.fail;

import org.junit.Test;

import com.cloudera.impala.catalog.ColumnType;


public class ExprTest {
  // Test creation of LiteralExprs from Strings, e.g., for partitioning keys.
  @Test
  public void TestLiteralExpr() {
    testLiteralExprPositive("false", ColumnType.BOOLEAN);
    testLiteralExprPositive("1", ColumnType.TINYINT);
    testLiteralExprPositive("1", ColumnType.SMALLINT);
    testLiteralExprPositive("1", ColumnType.INT);
    testLiteralExprPositive("1", ColumnType.BIGINT);
    testLiteralExprPositive("1.0", ColumnType.FLOAT);
    testLiteralExprPositive("1.0", ColumnType.DOUBLE);
    testLiteralExprPositive("ABC", ColumnType.STRING);
    testLiteralExprPositive("1.1", ColumnType.createDecimalType(2, 1));

    // INVALID_TYPE should always fail
    testLiteralExprNegative("ABC", ColumnType.INVALID);

    // Invalid casts
    testLiteralExprNegative("ABC", ColumnType.BOOLEAN);
    testLiteralExprNegative("ABC", ColumnType.TINYINT);
    testLiteralExprNegative("ABC", ColumnType.SMALLINT);
    testLiteralExprNegative("ABC", ColumnType.INT);
    testLiteralExprNegative("ABC", ColumnType.BIGINT);
    testLiteralExprNegative("ABC", ColumnType.FLOAT);
    testLiteralExprNegative("ABC", ColumnType.DOUBLE);
    testLiteralExprNegative("ABC", ColumnType.TIMESTAMP);
    testLiteralExprNegative("ABC", ColumnType.createDecimalType());

    // Date types not implemented
    testLiteralExprNegative("2010-01-01", ColumnType.DATE);
    testLiteralExprNegative("2010-01-01", ColumnType.DATETIME);
    testLiteralExprNegative("2010-01-01", ColumnType.TIMESTAMP);
  }

  private void testLiteralExprPositive(String value, ColumnType type) {
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

  private void testLiteralExprNegative(String value, ColumnType type) {
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
