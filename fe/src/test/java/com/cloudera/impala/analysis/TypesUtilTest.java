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

import org.junit.Test;

import com.cloudera.impala.catalog.ColumnType;
import com.cloudera.impala.catalog.PrimitiveType;

// TODO: move other types related tests into this class to break up the large
// AnalyzerTest files.
public class TypesUtilTest extends AnalyzerTest {

  private void verifyDecimalType(ColumnType t1, ColumnType t2) {
    assertTrue(t1.getPrimitiveType() == PrimitiveType.DECIMAL);
    assertTrue(t2.getPrimitiveType() == PrimitiveType.DECIMAL);
    assertTrue(t1.equals(t2));
  }

  @Test
  // Tests to verify that we can compute the correct type for assignment.
  public void TestDecimalAssignementType() {
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ColumnType.DEFAULT_DECIMAL, ColumnType.DEFAULT_DECIMAL),
        ColumnType.DEFAULT_DECIMAL);
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ColumnType.createDecimalType(10, 2), ColumnType.createDecimalType(12, 2)),
        ColumnType.createDecimalType(12, 2));
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ColumnType.createDecimalType(10, 5), ColumnType.createDecimalType(12, 3)),
        ColumnType.createDecimalType(14, 5));
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ColumnType.createDecimalType(12, 2), ColumnType.createDecimalType(10, 2)),
        ColumnType.createDecimalType(12, 2));
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ColumnType.createDecimalType(12, 3), ColumnType.createDecimalType(10, 5)),
        ColumnType.createDecimalType(14, 5));
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(ColumnType.createDecimalType(10, 0),
            ColumnType.createDecimalType(16, 5)),
        ColumnType.createDecimalType(16, 5));

    // Decimal(10, 0) && Decimal(10, 0) --> Decimal(10, 0)
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ColumnType.createDecimalType(), ColumnType.createDecimalType()),
        ColumnType.createDecimalType());

    // decimal(10, 2) && decimal(12, 2) -> decimal(12, 2)
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ColumnType.createDecimalType(10, 2), ColumnType.createDecimalType(12, 2)),
        ColumnType.createDecimalType(12, 2));


    // decimal (38, 38) && decimal(3, 0) -> decimal(38 , 38)
    // In this case, since we only support 38 digits, there is no type (we'd
    // need 41 digits). Return the best we can do.
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ColumnType.createDecimalType(38, 38), ColumnType.createDecimalType(3)),
        ColumnType.createDecimalType(38, 38));

    // Decimal(5,0) with Decimal(*,*) should be Decimal(5,0)
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ColumnType.createDecimalType(5, 0), ColumnType.DECIMAL),
        ColumnType.createDecimalType(5, 0));
    verifyDecimalType(
        TypesUtil.getDecimalAssignmentCompatibleType(
            ColumnType.DECIMAL, ColumnType.createDecimalType(5, 0)),
        ColumnType.createDecimalType(5, 0));
  }
}
