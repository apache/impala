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

package org.apache.impala.catalog.paimon;

import static org.junit.Assert.assertEquals;

import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;
import org.junit.Test;

import java.util.Arrays;

public class ImpalaTypeUtilsTest {
  private interface ConvFunc { public void convertFun(); }

  private static void assertThrowUnexpected(ConvFunc convFunc) {
    try {
      convFunc.convertFun();
    } catch (UnsupportedOperationException ex) {
      // do nothing
    }
  }

  @Test
  public void testToImpalaType() {
    // Test primitive types
    assertEquals(Type.BOOLEAN, PaimonImpalaTypeUtils.toImpalaType(new BooleanType()));
    assertEquals(Type.TINYINT, PaimonImpalaTypeUtils.toImpalaType(new TinyIntType()));
    assertEquals(Type.SMALLINT, PaimonImpalaTypeUtils.toImpalaType(new SmallIntType()));
    assertEquals(Type.INT, PaimonImpalaTypeUtils.toImpalaType(new IntType()));
    assertEquals(Type.BIGINT, PaimonImpalaTypeUtils.toImpalaType(new BigIntType()));
    assertEquals(Type.FLOAT, PaimonImpalaTypeUtils.toImpalaType(new FloatType()));
    assertEquals(Type.DOUBLE, PaimonImpalaTypeUtils.toImpalaType(new DoubleType()));
    assertEquals(Type.DATE, PaimonImpalaTypeUtils.toImpalaType(DataTypes.DATE()));
    assertEquals(Type.TIMESTAMP, PaimonImpalaTypeUtils
        .toImpalaType(new TimestampType()));

    // Test decimal type
    DecimalType decimalType = new DecimalType(10, 2);
    assertEquals(
        ScalarType.createDecimalType(10, 2),
        PaimonImpalaTypeUtils.toImpalaType(decimalType));

    // Test char and varchar types
    assertEquals(
        ScalarType.createCharType(10),
        PaimonImpalaTypeUtils.toImpalaType(new CharType(10)));
    assertEquals(ScalarType.createVarcharType(10),
        PaimonImpalaTypeUtils.toImpalaType(new VarCharType(10)));
    assertEquals(Type.STRING, PaimonImpalaTypeUtils
        .toImpalaType(new CharType(256)));
    assertEquals(Type.STRING, PaimonImpalaTypeUtils
        .toImpalaType(new VarCharType(65536)));
    // Test array type
    ArrayType arrayType = new ArrayType(new IntType());
    assertEquals(new org.apache.impala.catalog.ArrayType(Type.INT),
        PaimonImpalaTypeUtils.toImpalaType(arrayType));

    // Test map type
    MapType mapType = new MapType(new IntType(), DataTypes.STRING());
    assertEquals(new org.apache.impala.catalog.MapType(Type.INT, Type.STRING),
        PaimonImpalaTypeUtils.toImpalaType(mapType));

    // Test row type
    RowType rowType = new RowType(Arrays.asList(new DataField(0, "id", new IntType()),
        new DataField(1, "name", DataTypes.STRING())));
    StructType expectedStructType = new StructType(
        Arrays.asList(new PaimonStructField("id", Type.INT,
                          rowType.getField(0).description(), rowType.getField(0).id()),
            new PaimonStructField("name", Type.STRING, rowType.getField(1).description(),
                rowType.getField(1).id())));
    assertEquals(expectedStructType, PaimonImpalaTypeUtils.toImpalaType(rowType));

    // doesn't support time
    assertThrowUnexpected(() -> PaimonImpalaTypeUtils.toImpalaType(DataTypes.TIME()));
    // doesn't support multiset
    assertThrowUnexpected(
        () -> PaimonImpalaTypeUtils
            .toImpalaType(DataTypes.MULTISET(DataTypes.INT())));
    // doesn't support timestamp with local timezone
    assertThrowUnexpected(
        () -> PaimonImpalaTypeUtils
            .toImpalaType(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()));
  }

  @Test
  public void testFromImpalaType() {
    // Test primitive types
    assertEquals(DataTypes.BOOLEAN(), PaimonImpalaTypeUtils
        .fromImpalaType(Type.BOOLEAN));
    assertEquals(DataTypes.TINYINT(), PaimonImpalaTypeUtils
        .fromImpalaType(Type.TINYINT));
    assertEquals(DataTypes.SMALLINT(), PaimonImpalaTypeUtils
        .fromImpalaType(Type.SMALLINT));
    assertEquals(DataTypes.INT(), PaimonImpalaTypeUtils.fromImpalaType(Type.INT));
    assertEquals(DataTypes.BIGINT(), PaimonImpalaTypeUtils.fromImpalaType(Type.BIGINT));
    assertEquals(DataTypes.FLOAT(), PaimonImpalaTypeUtils.fromImpalaType(Type.FLOAT));
    assertEquals(DataTypes.DOUBLE(), PaimonImpalaTypeUtils.fromImpalaType(Type.DOUBLE));
    assertEquals(DataTypes.DATE(), PaimonImpalaTypeUtils.fromImpalaType(Type.DATE));
    assertEquals(DataTypes.TIMESTAMP(), PaimonImpalaTypeUtils
        .fromImpalaType(Type.TIMESTAMP));

    // Test decimal type
    ScalarType decimalType = ScalarType.createDecimalType(10, 2);
    assertEquals(DataTypes.DECIMAL(10, 2),
        PaimonImpalaTypeUtils.fromImpalaType(decimalType));

    // Test char and varchar types
    assertEquals(DataTypes.CHAR(10),
        PaimonImpalaTypeUtils.fromImpalaType(ScalarType.createCharType(10)));
    assertEquals(DataTypes.VARCHAR(255),
        PaimonImpalaTypeUtils.fromImpalaType(ScalarType.createVarcharType(255)));
    // Test array type
    org.apache.impala.catalog.ArrayType arrayType =
        new org.apache.impala.catalog.ArrayType(Type.INT);
    assertEquals(
        DataTypes.ARRAY(DataTypes.INT()), PaimonImpalaTypeUtils
            .fromImpalaType(arrayType));

    // Test map type
    org.apache.impala.catalog.MapType mapType =
        new org.apache.impala.catalog.MapType(Type.INT, Type.STRING);
    assertEquals(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
        PaimonImpalaTypeUtils.fromImpalaType(mapType));

    // Test struct type
    StructType structType = new StructType(Arrays.asList(
        new StructField("id", Type.INT), new StructField("name", Type.STRING)));
    RowType expectedRowType = RowType.of(new DataField(0, "id", DataTypes.INT()),
        new DataField(1, "name", DataTypes.STRING()));
    assertEquals(expectedRowType, PaimonImpalaTypeUtils.fromImpalaType(structType));

    // doesn't support datetime
    assertThrowUnexpected(() -> PaimonImpalaTypeUtils.fromImpalaType(Type.DATETIME));
    // doesn't support NULL TYPE
    assertThrowUnexpected(() -> PaimonImpalaTypeUtils.fromImpalaType(Type.NULL));
    // doesn't support INVALID
    assertThrowUnexpected(() -> PaimonImpalaTypeUtils.fromImpalaType(Type.INVALID));
    // doesn't support FIXED_UDA_INTERMEDIATE
    assertThrowUnexpected(
        () -> PaimonImpalaTypeUtils.fromImpalaType(Type.FIXED_UDA_INTERMEDIATE));
  }
}