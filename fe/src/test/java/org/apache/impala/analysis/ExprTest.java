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

import static org.junit.Assert.fail;

import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.junit.Test;


public class ExprTest {
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
    testLiteralExprPositive("1.1", ScalarType.createDecimalType(2, 1));

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

    // Date types not implemented
    testLiteralExprNegative("2010-01-01", Type.DATE);
    testLiteralExprNegative("2010-01-01", Type.DATETIME);
    testLiteralExprNegative("2010-01-01", Type.TIMESTAMP);
  }

  private void testLiteralExprPositive(String value, Type type) {
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

  private void testLiteralExprNegative(String value, Type type) {
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
