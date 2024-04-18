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

package org.apache.impala.calcite.service;

import static org.junit.Assert.assertTrue;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.impala.calcite.functions.ImplicitTypeChecker;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.TypeCompatibility;

import org.junit.Test;

public class CompatibilityTest {
  @Test
  public void testCompatibility() {
    for (PrimitiveType fromPType : PrimitiveType.values()) {
      for (PrimitiveType toPType : PrimitiveType.values()) {
        // skip the invalid type since that will never map to/from anything
        if (fromPType == PrimitiveType.INVALID_TYPE ||
            toPType == PrimitiveType.INVALID_TYPE) {
          continue;
        }
        //TODO: support datetime
        if (fromPType == PrimitiveType.DATETIME||
            toPType == PrimitiveType.DATETIME) {
          continue;
        }
        // intermediate type not used for Calcite mappings
        if (fromPType == PrimitiveType.FIXED_UDA_INTERMEDIATE ||
            toPType == PrimitiveType.FIXED_UDA_INTERMEDIATE) {
          continue;
        }
        if (fromPType == PrimitiveType.FLOAT && toPType == PrimitiveType.DECIMAL) {
          continue;
        }
        if (fromPType == PrimitiveType.DOUBLE && toPType == PrimitiveType.DECIMAL) {
          continue;
        }
        ScalarType fromType = Type.getDefaultScalarType(fromPType);
        ScalarType toType = Type.getDefaultScalarType(toPType);
        RelDataType fromRelDataType = ImpalaTypeConverter.getRelDataType(fromType);
        RelDataType toRelDataType = ImpalaTypeConverter.getRelDataType(toType);
        if (fromRelDataType == null) {
          throw new RuntimeException("Couldn't translate " + fromType);
        }
        if (toRelDataType == null) {
          throw new RuntimeException("Couldn't translate " + toType);
        }
        boolean isSupportedCalcite = ImplicitTypeChecker.supportsImplicitCasting(
                fromRelDataType, toRelDataType);

        boolean isSupportedImpala =
            Type.isImplicitlyCastable(fromType, toType, TypeCompatibility.DEFAULT);

        // special case float and double to decimal. Calcite allows this because of
        // arithmetic operations. A float plus a decimal returns a decimal type.
        if ((fromPType == PrimitiveType.FLOAT || fromPType == PrimitiveType.DOUBLE) &&
            toPType == PrimitiveType.DECIMAL) {
          assertTrue("Support mismatch for " + fromPType + " to  " + toPType + ". " +
              "Impala does" + (isSupportedImpala ? "" : " not") +
              " support this conversion." +
              "SqlTypeName from is " + fromRelDataType.getSqlTypeName() +
              ", SqlTypeName to is " + toRelDataType.getSqlTypeName() + ".",
             !isSupportedImpala && isSupportedCalcite);
        } else {
          assertTrue("Support mismatch for " + fromPType + " to  " + toPType + ". " +
              "Impala does" + (isSupportedImpala ? "" : " not") +
              " support this conversion." +
              "SqlTypeName from is " + fromRelDataType.getSqlTypeName() +
              ", SqlTypeName to is " + toRelDataType.getSqlTypeName() + ".",
             isSupportedImpala == isSupportedCalcite);
        }
      }
    }
  }
}
