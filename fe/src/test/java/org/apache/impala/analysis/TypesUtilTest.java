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

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.impala.catalog.TypeCompatibility;
import org.junit.Assert;
import org.junit.Test;

import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import com.google.common.collect.Lists;

// TODO: move other types related tests into this class to break up the large
// AnalyzerTest files.
public class TypesUtilTest extends AnalyzerTest {

  private void verifyDecimalType(Type t1, Type t2) {
    assertTrue(t1.getPrimitiveType() == PrimitiveType.DECIMAL);
    assertTrue(t2.getPrimitiveType() == PrimitiveType.DECIMAL);
    assertTrue(t1.equals(t2));
  }

  private void verifyInvalid(Type t) {
    assertTrue(t.equals(Type.INVALID));
  }

  @Test
  // Tests to verify that we can compute the correct type for assignment.
  public void TestDecimalAssignementType() {
    for (final boolean decimalV2 : new boolean[] { false, true} ) {
      verifyDecimalType(
          TypesUtil.getDecimalAssignmentCompatibleType(
              Type.DEFAULT_DECIMAL,
              Type.DEFAULT_DECIMAL, decimalV2),
          Type.DEFAULT_DECIMAL);
      verifyDecimalType(
          TypesUtil.getDecimalAssignmentCompatibleType(
              ScalarType.createDecimalType(10, 2),
              ScalarType.createDecimalType(12, 2), decimalV2),
          ScalarType.createDecimalType(12, 2));
      verifyDecimalType(
          TypesUtil.getDecimalAssignmentCompatibleType(
              ScalarType.createDecimalType(10, 5),
              ScalarType.createDecimalType(12, 3), decimalV2),
          ScalarType.createDecimalType(14, 5));
      verifyDecimalType(
          TypesUtil.getDecimalAssignmentCompatibleType(
              ScalarType.createDecimalType(12, 2),
              ScalarType.createDecimalType(10, 2), decimalV2),
          ScalarType.createDecimalType(12, 2));
      verifyDecimalType(
          TypesUtil.getDecimalAssignmentCompatibleType(
              ScalarType.createDecimalType(12, 3),
              ScalarType.createDecimalType(10, 5), decimalV2),
          ScalarType.createDecimalType(14, 5));
      verifyDecimalType(
          TypesUtil.getDecimalAssignmentCompatibleType(
              ScalarType.createDecimalType(10, 0),
              ScalarType.createDecimalType(16, 5), decimalV2),
          ScalarType.createDecimalType(16, 5));

      // Decimal(10, 0) && Decimal(10, 0) --> Decimal(10, 0)
      verifyDecimalType(
          TypesUtil.getDecimalAssignmentCompatibleType(
              ScalarType.createDecimalType(),
              ScalarType.createDecimalType(), decimalV2),
          ScalarType.createDecimalType());

      // decimal(10, 2) && decimal(12, 2) -> decimal(12, 2)
      verifyDecimalType(
          TypesUtil.getDecimalAssignmentCompatibleType(
              ScalarType.createDecimalType(10, 2),
              ScalarType.createDecimalType(12, 2), decimalV2),
          ScalarType.createDecimalType(12, 2));

      // Decimal(5,0) with Decimal(*,*) should be Decimal(5,0)
      verifyDecimalType(
          TypesUtil.getDecimalAssignmentCompatibleType(
              ScalarType.createDecimalType(5, 0),
              Type.DECIMAL, decimalV2),
          ScalarType.createDecimalType(5, 0));
      verifyDecimalType(
          TypesUtil.getDecimalAssignmentCompatibleType(
              Type.DECIMAL,
              ScalarType.createDecimalType(5, 0), decimalV2),
          ScalarType.createDecimalType(5, 0));
    }

    // decimal (38, 38) && decimal(3, 0) -> decimal(38 , 38)
    // In this case, since we only support 38 digits, there is no type (we'd
    // need 41 digits). Return a clipped decimal if decimal_v2 is disabled.
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ScalarType.createDecimalType(38, 38),
            ScalarType.createDecimalType(3), false),
        ScalarType.createDecimalType(38, 38));
    verifyInvalid(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ScalarType.createDecimalType(38, 38),
            ScalarType.createDecimalType(3), true));
  }

  @Test
  // Test for implicit casts between numeric types, with many boundary cases.
  public void TestNumericImplicitCast() {
    // Decimals can be cast to integers if there is no loss of precision.
    Assert.assertTrue(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(2, 0), Type.TINYINT, TypeCompatibility.DEFAULT));
    Assert.assertTrue(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(4, 0), Type.SMALLINT, TypeCompatibility.DEFAULT));
    Assert.assertTrue(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(9, 0), Type.INT, TypeCompatibility.DEFAULT));
    Assert.assertTrue(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(18, 0), Type.BIGINT, TypeCompatibility.DEFAULT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(3, 0), Type.TINYINT, TypeCompatibility.DEFAULT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(5, 0), Type.SMALLINT, TypeCompatibility.DEFAULT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(10, 0), Type.INT, TypeCompatibility.DEFAULT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(19, 0), Type.BIGINT, TypeCompatibility.DEFAULT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(2, 1), Type.TINYINT, TypeCompatibility.DEFAULT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(4, 1), Type.SMALLINT, TypeCompatibility.DEFAULT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(2, 1), Type.INT, TypeCompatibility.DEFAULT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        ScalarType.createDecimalType(18, 5), Type.BIGINT, TypeCompatibility.DEFAULT));

    // Integers are only converted to decimal when all values of the source type can be
    // represented in the destination type.
    Assert.assertFalse(Type.isImplicitlyCastable(
        Type.TINYINT, ScalarType.createDecimalType(2, 0), TypeCompatibility.DEFAULT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        Type.SMALLINT, ScalarType.createDecimalType(4, 0), TypeCompatibility.DEFAULT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        Type.INT, ScalarType.createDecimalType(9, 0), TypeCompatibility.DEFAULT));
    Assert.assertFalse(Type.isImplicitlyCastable(
        Type.BIGINT, ScalarType.createDecimalType(18, 0), TypeCompatibility.DEFAULT));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.TINYINT, ScalarType.createDecimalType(3, 0), TypeCompatibility.DEFAULT));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.SMALLINT, ScalarType.createDecimalType(5, 0), TypeCompatibility.DEFAULT));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.INT, ScalarType.createDecimalType(10, 0), TypeCompatibility.DEFAULT));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.BIGINT, ScalarType.createDecimalType(19, 0), TypeCompatibility.DEFAULT));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.TINYINT, ScalarType.createDecimalType(4, 1), TypeCompatibility.DEFAULT));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.SMALLINT, ScalarType.createDecimalType(6, 1), TypeCompatibility.DEFAULT));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.INT, ScalarType.createDecimalType(11, 1), TypeCompatibility.DEFAULT));
    Assert.assertTrue(Type.isImplicitlyCastable(
        Type.BIGINT, ScalarType.createDecimalType(20, 1), TypeCompatibility.DEFAULT));

    // Only promotions are allowed for integer types.
    List<Type> intTypes = Arrays.<Type>asList(Type.TINYINT, Type.SMALLINT, Type.INT,
        Type.BIGINT);
    for (Type t1: intTypes) {
      for (Type t2: intTypes) {
        if (t1.getSlotSize() == t2.getSlotSize()) {
          Assert.assertTrue(Type.isImplicitlyCastable(t1, t2, TypeCompatibility.STRICT));
          Assert.assertTrue(Type.isImplicitlyCastable(t1, t2, TypeCompatibility.DEFAULT));
        } else if (t1.getSlotSize() < t2.getSlotSize()) {
          Assert.assertTrue(Type.isImplicitlyCastable(t1, t2, TypeCompatibility.STRICT));
          Assert.assertTrue(Type.isImplicitlyCastable(t1, t2, TypeCompatibility.DEFAULT));
          Assert.assertFalse(Type.isImplicitlyCastable(t2, t1, TypeCompatibility.STRICT));
          Assert.assertFalse(
              Type.isImplicitlyCastable(t2, t1, TypeCompatibility.DEFAULT));
        } else {
          Assert.assertFalse(Type.isImplicitlyCastable(t1, t2, TypeCompatibility.STRICT));
          Assert.assertFalse(
              Type.isImplicitlyCastable(t1, t2, TypeCompatibility.DEFAULT));
          Assert.assertTrue(Type.isImplicitlyCastable(t2, t1, TypeCompatibility.STRICT));
          Assert.assertTrue(Type.isImplicitlyCastable(t2, t1, TypeCompatibility.DEFAULT));
        }
      }
    }
    // Only promotions are allowed for floating point types.
    Assert.assertTrue(
        Type.isImplicitlyCastable(Type.FLOAT, Type.FLOAT, TypeCompatibility.STRICT));
    Assert.assertFalse(
        Type.isImplicitlyCastable(Type.DOUBLE, Type.FLOAT, TypeCompatibility.DEFAULT));
    Assert.assertTrue(
        Type.isImplicitlyCastable(Type.FLOAT, Type.DOUBLE, TypeCompatibility.DEFAULT));
    Assert.assertTrue(
        Type.isImplicitlyCastable(Type.FLOAT, Type.DOUBLE, TypeCompatibility.STRICT));

    // Decimal is convertible to a floating point types only in non-strict mode.
    List<ScalarType> dts = Arrays.asList(ScalarType.createDecimalType(30, 10),
        ScalarType.createDecimalType(2, 0));
    for (Type dt: dts) {
      Assert.assertFalse(
          Type.isImplicitlyCastable(dt, Type.FLOAT, TypeCompatibility.STRICT));
      Assert.assertTrue(
          Type.isImplicitlyCastable(dt, Type.FLOAT, TypeCompatibility.DEFAULT));
      Assert.assertFalse(
          Type.isImplicitlyCastable(dt, Type.DOUBLE, TypeCompatibility.STRICT));
      Assert.assertTrue(
          Type.isImplicitlyCastable(dt, Type.DOUBLE, TypeCompatibility.DEFAULT));
    }
  }

  @Test
  // Test that we don't allow casting to/from complex types.
  public void TestComplexImplicitCast() {
    ArrayType arrayType = new ArrayType(Type.INT);
    Assert.assertFalse(
        Type.isImplicitlyCastable(Type.INT, arrayType, TypeCompatibility.DEFAULT));
    Assert.assertFalse(
        Type.isImplicitlyCastable(arrayType, Type.INT, TypeCompatibility.DEFAULT));
    MapType mapType = new MapType(Type.STRING, Type.INT);
    Assert.assertFalse(
        Type.isImplicitlyCastable(Type.INT, mapType, TypeCompatibility.DEFAULT));
    Assert.assertFalse(
        Type.isImplicitlyCastable(mapType, Type.INT, TypeCompatibility.DEFAULT));
    Assert.assertFalse(
        Type.isImplicitlyCastable(mapType, arrayType, TypeCompatibility.DEFAULT));
    StructType structType = new StructType(Lists.newArrayList(
        new StructField("foo", Type.FLOAT, ""), new StructField("bar", Type.FLOAT, "")));
    Assert.assertFalse(
        Type.isImplicitlyCastable(structType, Type.INT, TypeCompatibility.DEFAULT));
    Assert.assertFalse(
        Type.isImplicitlyCastable(Type.INT, structType, TypeCompatibility.DEFAULT));
    Assert.assertFalse(
        Type.isImplicitlyCastable(arrayType, structType, TypeCompatibility.DEFAULT));
  }
}
