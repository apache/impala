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
import org.junit.Assert;
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
    assertEquals(Type.BOOLEAN, ImpalaTypeUtils.toImpalaType(new BooleanType()));
    assertEquals(Type.TINYINT, ImpalaTypeUtils.toImpalaType(new TinyIntType()));
    assertEquals(Type.SMALLINT, ImpalaTypeUtils.toImpalaType(new SmallIntType()));
    assertEquals(Type.INT, ImpalaTypeUtils.toImpalaType(new IntType()));
    assertEquals(Type.BIGINT, ImpalaTypeUtils.toImpalaType(new BigIntType()));
    assertEquals(Type.FLOAT, ImpalaTypeUtils.toImpalaType(new FloatType()));
    assertEquals(Type.DOUBLE, ImpalaTypeUtils.toImpalaType(new DoubleType()));
    assertEquals(Type.DATE, ImpalaTypeUtils.toImpalaType(DataTypes.DATE()));
    assertEquals(Type.TIMESTAMP, ImpalaTypeUtils.toImpalaType(new TimestampType()));

    // Test decimal type
    DecimalType decimalType = new DecimalType(10, 2);
    assertEquals(
        ScalarType.createDecimalType(10, 2), ImpalaTypeUtils.toImpalaType(decimalType));

    // Test char and varchar types
    assertEquals(
        ScalarType.createCharType(10), ImpalaTypeUtils.toImpalaType(new CharType(10)));
    assertEquals(ScalarType.createVarcharType(10),
        ImpalaTypeUtils.toImpalaType(new VarCharType(10)));
    assertEquals(Type.STRING, ImpalaTypeUtils.toImpalaType(new CharType(256)));
    assertEquals(Type.STRING, ImpalaTypeUtils.toImpalaType(new VarCharType(65536)));
    // Test array type
    ArrayType arrayType = new ArrayType(new IntType());
    assertEquals(new org.apache.impala.catalog.ArrayType(Type.INT),
        ImpalaTypeUtils.toImpalaType(arrayType));

    // Test map type
    MapType mapType = new MapType(new IntType(), DataTypes.STRING());
    assertEquals(new org.apache.impala.catalog.MapType(Type.INT, Type.STRING),
        ImpalaTypeUtils.toImpalaType(mapType));

    // Test row type
    RowType rowType = new RowType(Arrays.asList(new DataField(0, "id", new IntType()),
        new DataField(1, "name", DataTypes.STRING())));
    StructType expectedStructType = new StructType(Arrays.asList(
        new StructField("id", Type.INT), new StructField("name", Type.STRING)));
    assertEquals(expectedStructType, ImpalaTypeUtils.toImpalaType(rowType));

    // doesn't support time
    assertThrowUnexpected(() -> ImpalaTypeUtils.toImpalaType(DataTypes.TIME()));
    // doesn't support multiset
    assertThrowUnexpected(
        () -> ImpalaTypeUtils.toImpalaType(DataTypes.MULTISET(DataTypes.INT())));
    // doesn't support timestamp with local timezone
    assertThrowUnexpected(
        () -> ImpalaTypeUtils.toImpalaType(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()));
  }

  @Test
  public void testFromImpalaType() {
    // Test primitive types
    assertEquals(DataTypes.BOOLEAN(), ImpalaTypeUtils.fromImpalaType(Type.BOOLEAN));
    assertEquals(DataTypes.TINYINT(), ImpalaTypeUtils.fromImpalaType(Type.TINYINT));
    assertEquals(DataTypes.SMALLINT(), ImpalaTypeUtils.fromImpalaType(Type.SMALLINT));
    assertEquals(DataTypes.INT(), ImpalaTypeUtils.fromImpalaType(Type.INT));
    assertEquals(DataTypes.BIGINT(), ImpalaTypeUtils.fromImpalaType(Type.BIGINT));
    assertEquals(DataTypes.FLOAT(), ImpalaTypeUtils.fromImpalaType(Type.FLOAT));
    assertEquals(DataTypes.DOUBLE(), ImpalaTypeUtils.fromImpalaType(Type.DOUBLE));
    assertEquals(DataTypes.DATE(), ImpalaTypeUtils.fromImpalaType(Type.DATE));
    assertEquals(DataTypes.TIMESTAMP(), ImpalaTypeUtils.fromImpalaType(Type.TIMESTAMP));

    // Test decimal type
    ScalarType decimalType = ScalarType.createDecimalType(10, 2);
    assertEquals(DataTypes.DECIMAL(10, 2), ImpalaTypeUtils.fromImpalaType(decimalType));

    // Test char and varchar types
    assertEquals(DataTypes.CHAR(10),
        ImpalaTypeUtils.fromImpalaType(ScalarType.createCharType(10)));
    assertEquals(DataTypes.VARCHAR(255),
        ImpalaTypeUtils.fromImpalaType(ScalarType.createVarcharType(255)));
    // Test array type
    org.apache.impala.catalog.ArrayType arrayType =
        new org.apache.impala.catalog.ArrayType(Type.INT);
    assertEquals(
        DataTypes.ARRAY(DataTypes.INT()), ImpalaTypeUtils.fromImpalaType(arrayType));

    // Test map type
    org.apache.impala.catalog.MapType mapType =
        new org.apache.impala.catalog.MapType(Type.INT, Type.STRING);
    assertEquals(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
        ImpalaTypeUtils.fromImpalaType(mapType));

    // Test struct type
    StructType structType = new StructType(Arrays.asList(
        new StructField("id", Type.INT), new StructField("name", Type.STRING)));
    RowType expectedRowType = RowType.of(new DataField(0, "id", DataTypes.INT()),
        new DataField(1, "name", DataTypes.STRING()));
    assertEquals(expectedRowType, ImpalaTypeUtils.fromImpalaType(structType));

    // doesn't support datetime
    assertThrowUnexpected(() -> ImpalaTypeUtils.fromImpalaType(Type.DATETIME));
    // doesn't support NULL TYPE
    assertThrowUnexpected(() -> ImpalaTypeUtils.fromImpalaType(Type.NULL));
    // doesn't support INVALID
    assertThrowUnexpected(() -> ImpalaTypeUtils.fromImpalaType(Type.INVALID));
    // doesn't support FIXED_UDA_INTERMEDIATE
    assertThrowUnexpected(
        () -> ImpalaTypeUtils.fromImpalaType(Type.FIXED_UDA_INTERMEDIATE));
  }
}