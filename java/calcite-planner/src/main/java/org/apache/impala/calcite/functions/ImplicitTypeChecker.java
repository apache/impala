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

package org.apache.impala.calcite.functions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that checks if an implicit cast is supported through the
 * supportsImplicitCasting static method.
 */
public class ImplicitTypeChecker {
  protected static final Logger LOG =
      LoggerFactory.getLogger(ImplicitTypeChecker.class.getName());

  // Set containing a pair of casts that are supported. The first type
  // is the argument and the second type is the return type.
  private static Set<Pair<SqlTypeName, SqlTypeName>> SUPPORTED_IMPLICIT_CASTS =
      ImmutableSet.<Pair<SqlTypeName, SqlTypeName>> builder()
      .add(Pair.of(SqlTypeName.NULL, SqlTypeName.NULL))
      .add(Pair.of(SqlTypeName.NULL, SqlTypeName.BOOLEAN))
      .add(Pair.of(SqlTypeName.NULL, SqlTypeName.TINYINT))
      .add(Pair.of(SqlTypeName.NULL, SqlTypeName.SMALLINT))
      .add(Pair.of(SqlTypeName.NULL, SqlTypeName.INTEGER))
      .add(Pair.of(SqlTypeName.NULL, SqlTypeName.BIGINT))
      .add(Pair.of(SqlTypeName.NULL, SqlTypeName.FLOAT))
      .add(Pair.of(SqlTypeName.NULL, SqlTypeName.DOUBLE))
      .add(Pair.of(SqlTypeName.NULL, SqlTypeName.DECIMAL))
      .add(Pair.of(SqlTypeName.NULL, SqlTypeName.TIMESTAMP))
      .add(Pair.of(SqlTypeName.NULL, SqlTypeName.DATE))
      .add(Pair.of(SqlTypeName.NULL, SqlTypeName.CHAR))
      .add(Pair.of(SqlTypeName.NULL, SqlTypeName.VARCHAR))
      .add(Pair.of(SqlTypeName.NULL, SqlTypeName.BINARY))
      .add(Pair.of(SqlTypeName.BOOLEAN, SqlTypeName.BOOLEAN))
      .add(Pair.of(SqlTypeName.BOOLEAN, SqlTypeName.TINYINT))
      .add(Pair.of(SqlTypeName.BOOLEAN, SqlTypeName.SMALLINT))
      .add(Pair.of(SqlTypeName.BOOLEAN, SqlTypeName.INTEGER))
      .add(Pair.of(SqlTypeName.BOOLEAN, SqlTypeName.BIGINT))
      .add(Pair.of(SqlTypeName.BOOLEAN, SqlTypeName.FLOAT))
      .add(Pair.of(SqlTypeName.BOOLEAN, SqlTypeName.DOUBLE))
      .add(Pair.of(SqlTypeName.TINYINT, SqlTypeName.TINYINT))
      .add(Pair.of(SqlTypeName.TINYINT, SqlTypeName.SMALLINT))
      .add(Pair.of(SqlTypeName.TINYINT, SqlTypeName.INTEGER))
      .add(Pair.of(SqlTypeName.TINYINT, SqlTypeName.BIGINT))
      .add(Pair.of(SqlTypeName.TINYINT, SqlTypeName.FLOAT))
      .add(Pair.of(SqlTypeName.TINYINT, SqlTypeName.DOUBLE))
      .add(Pair.of(SqlTypeName.TINYINT, SqlTypeName.DECIMAL))
      .add(Pair.of(SqlTypeName.SMALLINT, SqlTypeName.SMALLINT))
      .add(Pair.of(SqlTypeName.SMALLINT, SqlTypeName.INTEGER))
      .add(Pair.of(SqlTypeName.SMALLINT, SqlTypeName.BIGINT))
      .add(Pair.of(SqlTypeName.SMALLINT, SqlTypeName.FLOAT))
      .add(Pair.of(SqlTypeName.SMALLINT, SqlTypeName.DOUBLE))
      .add(Pair.of(SqlTypeName.SMALLINT, SqlTypeName.DECIMAL))
      .add(Pair.of(SqlTypeName.INTEGER, SqlTypeName.INTEGER))
      .add(Pair.of(SqlTypeName.INTEGER, SqlTypeName.BIGINT))
      .add(Pair.of(SqlTypeName.INTEGER, SqlTypeName.FLOAT))
      .add(Pair.of(SqlTypeName.INTEGER, SqlTypeName.DOUBLE))
      .add(Pair.of(SqlTypeName.INTEGER, SqlTypeName.DECIMAL))
      .add(Pair.of(SqlTypeName.BIGINT, SqlTypeName.BIGINT))
      .add(Pair.of(SqlTypeName.BIGINT, SqlTypeName.FLOAT))
      .add(Pair.of(SqlTypeName.BIGINT, SqlTypeName.DOUBLE))
      .add(Pair.of(SqlTypeName.BIGINT, SqlTypeName.DECIMAL))
      .add(Pair.of(SqlTypeName.FLOAT, SqlTypeName.FLOAT))
      .add(Pair.of(SqlTypeName.FLOAT, SqlTypeName.DOUBLE))
      .add(Pair.of(SqlTypeName.DOUBLE, SqlTypeName.DOUBLE))
      .add(Pair.of(SqlTypeName.TIMESTAMP, SqlTypeName.TIMESTAMP))
      .add(Pair.of(SqlTypeName.CHAR, SqlTypeName.CHAR))
      .add(Pair.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR))
      .add(Pair.of(SqlTypeName.DATE, SqlTypeName.DATE))
      .add(Pair.of(SqlTypeName.DECIMAL, SqlTypeName.DECIMAL))
      .add(Pair.of(SqlTypeName.DECIMAL, SqlTypeName.FLOAT))
      .add(Pair.of(SqlTypeName.DECIMAL, SqlTypeName.DOUBLE))
      .add(Pair.of(SqlTypeName.DATE, SqlTypeName.TIMESTAMP))
      .add(Pair.of(SqlTypeName.BINARY, SqlTypeName.BINARY))
      // These casts aren't compatible within Impala, but they are used in
      // arithmetic operations like "float_col + decimal_col", so Calcite does
      // need these supported.
      .add(Pair.of(SqlTypeName.FLOAT, SqlTypeName.DECIMAL))
      .add(Pair.of(SqlTypeName.DOUBLE, SqlTypeName.DECIMAL))
      .build();

  // Check to see if the "from" type can be cast up to the "to" type. In the case
  // where both are decimals, this will return true.
  public static boolean supportsImplicitCasting(RelDataType castFrom,
      RelDataType castTo) {

    if (SUPPORTED_IMPLICIT_CASTS.contains(
        Pair.of(castFrom.getSqlTypeName(), castTo.getSqlTypeName()))) {
      return true;
    }

    if (areCompatibleDataTypes(castFrom, castTo)) {
      return true;
    }

    return false;
  }

  private static boolean areCompatibleDataTypes(RelDataType fromType,
      RelDataType toType) {

    // special cases for string. The varchar type is overloaded for both varchar
    // and string types.
    if (fromType.getSqlTypeName().equals(SqlTypeName.VARCHAR) &&
        fromType.getPrecision() == Integer.MAX_VALUE) {
      // string to string is ok. string to varchar is not.
      if (toType.getPrecision() == Integer.MAX_VALUE) {
        return true;
      }
      switch (toType.getSqlTypeName()) {
        case DATE:
        case TIMESTAMP:
          return true;
        default:
          return false;
      }
    }

    // Calcite has interval types where Impala needs some kind of numeric type, so
    // these are compatible.
    if ((fromType.getSqlTypeName().getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME) ||
        (fromType.getSqlTypeName().getFamily() == SqlTypeFamily.INTERVAL_YEAR_MONTH) ||
        (toType.getSqlTypeName().getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME) ||
        (toType.getSqlTypeName().getFamily() == SqlTypeFamily.INTERVAL_YEAR_MONTH)) {
      if ((fromType.getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC) ||
          (toType.getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC)) {
        return true;
      }
    }

    return fromType.getSqlTypeName() == toType.getSqlTypeName();
  }

}
