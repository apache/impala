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

import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.Type;

// TODO: move other types related tests into this class to break up the large
// AnalyzerTest files.
public class TypesUtilTest extends AnalyzerTest {

  private void verifyDecimalType(Type t1, Type t2) {
    assertTrue(t1.getPrimitiveType() == PrimitiveType.DECIMAL);
    assertTrue(t2.getPrimitiveType() == PrimitiveType.DECIMAL);
    assertTrue(t1.equals(t2));
  }

  @Test
  // Tests to verify that we can compute the correct type for assignment.
  public void TestDecimalAssignementType() {
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            Type.DEFAULT_DECIMAL, Type.DEFAULT_DECIMAL),
        Type.DEFAULT_DECIMAL);
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ScalarType.createDecimalType(10, 2), ScalarType.createDecimalType(12, 2)),
        ScalarType.createDecimalType(12, 2));
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ScalarType.createDecimalType(10, 5), ScalarType.createDecimalType(12, 3)),
        ScalarType.createDecimalType(14, 5));
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ScalarType.createDecimalType(12, 2), ScalarType.createDecimalType(10, 2)),
        ScalarType.createDecimalType(12, 2));
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ScalarType.createDecimalType(12, 3), ScalarType.createDecimalType(10, 5)),
        ScalarType.createDecimalType(14, 5));
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(ScalarType.createDecimalType(10, 0),
            ScalarType.createDecimalType(16, 5)),
        ScalarType.createDecimalType(16, 5));

    // Decimal(10, 0) && Decimal(10, 0) --> Decimal(10, 0)
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ScalarType.createDecimalType(), ScalarType.createDecimalType()),
        ScalarType.createDecimalType());

    // decimal(10, 2) && decimal(12, 2) -> decimal(12, 2)
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ScalarType.createDecimalType(10, 2), ScalarType.createDecimalType(12, 2)),
        ScalarType.createDecimalType(12, 2));


    // decimal (38, 38) && decimal(3, 0) -> decimal(38 , 38)
    // In this case, since we only support 38 digits, there is no type (we'd
    // need 41 digits). Return the best we can do.
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ScalarType.createDecimalType(38, 38), ScalarType.createDecimalType(3)),
        ScalarType.createDecimalType(38, 38));

    // Decimal(5,0) with Decimal(*,*) should be Decimal(5,0)
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ScalarType.createDecimalType(5, 0), Type.DECIMAL),
        ScalarType.createDecimalType(5, 0));
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            Type.DECIMAL, ScalarType.createDecimalType(5, 0)),
        ScalarType.createDecimalType(5, 0));
  }

  @Test
  // Test for implicit casts between numeric types, with many boundary cases.
  public void TestNumericImplicitCast() {
    // Decimals can be cast to integers if there is no loss of precision.
    Assert.assertTrue(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(2, 0), Type.TINYINT));
    Assert.assertTrue(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(4, 0), Type.SMALLINT));
    Assert.assertTrue(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(9, 0), Type.INT));
    Assert.assertTrue(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(18, 0), Type.BIGINT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(3, 0), Type.TINYINT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(5, 0), Type.SMALLINT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(10, 0), Type.INT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(19, 0), Type.BIGINT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(2, 1), Type.TINYINT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(4, 1), Type.SMALLINT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(2, 1), Type.INT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(18, 5), Type.BIGINT));

    // Integers are only converted to decimal when all values of the source type can be
    // represented in the destination type.
    Assert.assertFalse(Type.isImplicitlyCastable(
        Type.TINYINT, ScalarType.createDecimalType(2, 0)));
    Assert.assertFalse(Type.isImplicitlyCastable(
        Type.SMALLINT, ScalarType.createDecimalType(4, 0)));
    Assert.assertFalse(Type.isImplicitlyCastable(
        Type.INT, ScalarType.createDecimalType(9, 0)));
    Assert.assertFalse(Type.isImplicitlyCastable(
        Type.BIGINT, ScalarType.createDecimalType(18, 0)));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.TINYINT, ScalarType.createDecimalType(3, 0)));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.SMALLINT, ScalarType.createDecimalType(5, 0)));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.INT, ScalarType.createDecimalType(10, 0)));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.BIGINT, ScalarType.createDecimalType(19, 0)));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.TINYINT, ScalarType.createDecimalType(4, 1)));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.SMALLINT, ScalarType.createDecimalType(6, 1)));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.INT, ScalarType.createDecimalType(11, 1)));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.BIGINT, ScalarType.createDecimalType(20, 1)));
  }
}
