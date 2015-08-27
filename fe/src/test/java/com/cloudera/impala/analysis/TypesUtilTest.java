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

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.impala.catalog.ArrayType;
import com.cloudera.impala.catalog.MapType;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.StructField;
import com.cloudera.impala.catalog.StructType;
import com.cloudera.impala.catalog.Type;
import com.google.common.collect.Lists;

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
        ScalarType.createDecimalType(2, 0), Type.TINYINT, false));
    Assert.assertTrue(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(4, 0), Type.SMALLINT, false));
    Assert.assertTrue(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(9, 0), Type.INT, false));
    Assert.assertTrue(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(18, 0), Type.BIGINT, false));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(3, 0), Type.TINYINT, false));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(5, 0), Type.SMALLINT, false));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(10, 0), Type.INT, false));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(19, 0), Type.BIGINT, false));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(2, 1), Type.TINYINT, false));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(4, 1), Type.SMALLINT, false));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(2, 1), Type.INT, false));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(18, 5), Type.BIGINT, false));

    // Integers are only converted to decimal when all values of the source type can be
    // represented in the destination type.
    Assert.assertFalse(Type.isImplicitlyCastable(
        Type.TINYINT, ScalarType.createDecimalType(2, 0), false));
    Assert.assertFalse(Type.isImplicitlyCastable(
        Type.SMALLINT, ScalarType.createDecimalType(4, 0), false));
    Assert.assertFalse(Type.isImplicitlyCastable(
        Type.INT, ScalarType.createDecimalType(9, 0), false));
    Assert.assertFalse(Type.isImplicitlyCastable(
        Type.BIGINT, ScalarType.createDecimalType(18, 0), false));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.TINYINT, ScalarType.createDecimalType(3, 0), false));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.SMALLINT, ScalarType.createDecimalType(5, 0), false));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.INT, ScalarType.createDecimalType(10, 0), false));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.BIGINT, ScalarType.createDecimalType(19, 0), false));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.TINYINT, ScalarType.createDecimalType(4, 1), false));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.SMALLINT, ScalarType.createDecimalType(6, 1), false));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.INT, ScalarType.createDecimalType(11, 1), false));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.BIGINT, ScalarType.createDecimalType(20, 1), false));

    // Only promotions are allowed for integer types.
    List<Type> intTypes = Arrays.<Type>asList(Type.TINYINT, Type.SMALLINT, Type.INT,
        Type.BIGINT);
    for (Type t1: intTypes) {
      for (Type t2: intTypes) {
        if (t1.getSlotSize() == t2.getSlotSize()) {
          Assert.assertTrue(Type.isImplicitlyCastable(t1, t2, true));
          Assert.assertTrue(Type.isImplicitlyCastable(t1, t2, false));
        } else if (t1.getSlotSize() < t2.getSlotSize()) {
          Assert.assertTrue(Type.isImplicitlyCastable(t1, t2, true));
          Assert.assertTrue(Type.isImplicitlyCastable(t1, t2, false));
          Assert.assertFalse(Type.isImplicitlyCastable(t2, t1, true));
          Assert.assertFalse(Type.isImplicitlyCastable(t2, t1, false));
        } else {
          Assert.assertFalse(Type.isImplicitlyCastable(t1, t2, true));
          Assert.assertFalse(Type.isImplicitlyCastable(t1, t2, false));
          Assert.assertTrue(Type.isImplicitlyCastable(t2, t1, true));
          Assert.assertTrue(Type.isImplicitlyCastable(t2, t1, false));
        }
      }
    }
    // Only promotions are allowed for floating point types.
    Assert.assertTrue(Type.isImplicitlyCastable(Type.FLOAT, Type.FLOAT, true));
    Assert.assertFalse(Type.isImplicitlyCastable(Type.DOUBLE, Type.FLOAT, false));
    Assert.assertTrue(Type.isImplicitlyCastable(Type.FLOAT, Type.DOUBLE, false));
    Assert.assertTrue(Type.isImplicitlyCastable(Type.FLOAT, Type.DOUBLE, true));

    // Decimal is convertible to a floating point types only in non-strict mode.
    List<ScalarType> dts = Arrays.asList(ScalarType.createDecimalType(30, 10),
        ScalarType.createDecimalType(2, 0));
    for (Type dt: dts) {
      Assert.assertFalse(Type.isImplicitlyCastable(dt, Type.FLOAT, true));
      Assert.assertTrue(Type.isImplicitlyCastable(dt, Type.FLOAT, false));
      Assert.assertFalse(Type.isImplicitlyCastable(dt, Type.DOUBLE, true));
      Assert.assertTrue(Type.isImplicitlyCastable(dt, Type.DOUBLE, false));
    }
  }

  @Test
  // Test that we don't allow casting to/from complex types.
  public void TestComplexImplicitCast() {
    ArrayType arrayType = new ArrayType(Type.INT);
    Assert.assertFalse(Type.isImplicitlyCastable(Type.INT, arrayType, false));
    Assert.assertFalse(Type.isImplicitlyCastable(arrayType, Type.INT, false));
    MapType mapType = new MapType(Type.STRING, Type.INT);
    Assert.assertFalse(Type.isImplicitlyCastable(Type.INT, mapType, false));
    Assert.assertFalse(Type.isImplicitlyCastable(mapType, Type.INT, false));
    Assert.assertFalse(Type.isImplicitlyCastable(mapType, arrayType, false));
    StructType structType = new StructType(Lists.newArrayList(
        new StructField("foo", Type.FLOAT, ""), new StructField("bar", Type.FLOAT, "")));
    Assert.assertFalse(Type.isImplicitlyCastable(structType, Type.INT, false));
    Assert.assertFalse(Type.isImplicitlyCastable(Type.INT, structType, false));
    Assert.assertFalse(Type.isImplicitlyCastable(arrayType, structType, false));
  }
}
