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

package org.apache.impala.calcite.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.impala.analysis.ArithmeticExpr;
import org.apache.impala.analysis.TypesUtil;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ImpalaTypeSystemImpl contains constants that are specific
 * to Impala datatypes that are used by Calcite.
 * Many of these constants were copied from the Hive repository
 * in the HiveTypeSystemImpl file. These are not fully tested
 * at this point, but since Hive datatypes are similar to Impala
 * datatypes, these definitions probably make sense. This may
 * change later as more code gets added if it turns out some of these
 * definitions do not make sense.
 */
public class ImpalaTypeSystemImpl extends RelDataTypeSystemImpl {
  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaTypeSystemImpl.class.getName());
  private static final int MAX_BINARY_PRECISION      = Integer.MAX_VALUE;
  // TIMESTAMP precision can go up to nanos
  private static final int MAX_TIMESTAMP_PRECISION   = 15;
  private static final int MAX_TIMESTAMP_WITH_LOCAL_TIME_ZONE_PRECISION = 15; // nanos
  // The precisions here match the number of digits that can be held by the type.
  // For example, the maximum value of TINYINT is 127, which is 3 digits.
  // The float and double precisions also match the number of total digits in the number.
  // Note: The FLOAT precision here is different from the precision used in the
  // Calcite RelDataTypeSystem file.  Calcite treats its floats the same as doubles.
  // Also note that the precision sizes match the values existing in
  // HiveDataTypeSystemImpl in the Hive github code base.
  private static final int DEFAULT_TINYINT_PRECISION  = 3;
  private static final int DEFAULT_SMALLINT_PRECISION = 5;
  private static final int DEFAULT_INTEGER_PRECISION  = 10;
  private static final int DEFAULT_BIGINT_PRECISION   = 19;
  private static final int DEFAULT_FLOAT_PRECISION    = 7;
  private static final int DEFAULT_DOUBLE_PRECISION   = 15;


  @Override
  public int getMaxScale(SqlTypeName typeName) {
    switch (typeName) {
    case DECIMAL:
      return getMaxNumericScale();
    case INTERVAL_YEAR:
    case INTERVAL_MONTH:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return SqlTypeName.MAX_INTERVAL_FRACTIONAL_SECOND_PRECISION;
    default:
      return -1;
    }
  }

  @Override
  public int getDefaultPrecision(SqlTypeName typeName) {
    switch (typeName) {
    // Binary doesn't need any sizes; Decimal has the default of 10.
    case BINARY:
    case VARBINARY:
      return RelDataType.PRECISION_NOT_SPECIFIED;
    case CHAR:
      return RelDataType.PRECISION_NOT_SPECIFIED;
    case VARCHAR:
      return RelDataType.PRECISION_NOT_SPECIFIED;
    case DECIMAL:
      return RelDataType.PRECISION_NOT_SPECIFIED;
    case INTERVAL_YEAR:
    case INTERVAL_MONTH:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return SqlTypeName.DEFAULT_INTERVAL_START_PRECISION;
    default:
      return getMaxPrecision(typeName);
    }
  }

  @Override
  public int getMaxPrecision(SqlTypeName typeName) {
    switch (typeName) {
    case BINARY:
    case VARBINARY:
      return MAX_BINARY_PRECISION;
    case TIME:
    case TIMESTAMP:
      return MAX_TIMESTAMP_PRECISION;
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return MAX_TIMESTAMP_WITH_LOCAL_TIME_ZONE_PRECISION;
    case CHAR:
      return ScalarType.MAX_CHAR_LENGTH;
    case VARCHAR:
      return Integer.MAX_VALUE;
    case DECIMAL:
      return getMaxNumericPrecision();
    case INTERVAL_YEAR:
    case INTERVAL_MONTH:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return SqlTypeName.MAX_INTERVAL_START_PRECISION;
    case TINYINT:
      return DEFAULT_TINYINT_PRECISION;
    case SMALLINT:
      return DEFAULT_SMALLINT_PRECISION;
    case INTEGER:
      return DEFAULT_INTEGER_PRECISION;
    case BIGINT:
      return DEFAULT_BIGINT_PRECISION;
    case FLOAT:
      return DEFAULT_FLOAT_PRECISION;
    case DOUBLE:
      return DEFAULT_DOUBLE_PRECISION;
    default:
      return -1;
    }
  }

  @Override
  public int getMaxNumericScale() {
    return ScalarType.MAX_SCALE;
  }

  @Override
  public int getMaxNumericPrecision() {
    return ScalarType.MAX_PRECISION;
  }

  @Override
  public boolean isSchemaCaseSensitive() {
    return false;
  }

  @Override
  public RelDataType deriveSumType(RelDataTypeFactory typeFactory,
      RelDataType argumentType) {
    // When adding two decimals, Impala sets the precision to 38
    RelDataType rdt = null;
    switch (argumentType.getSqlTypeName()) {
      case DECIMAL:
        if (argumentType.getPrecision() == ScalarType.MAX_PRECISION) {
          return argumentType;
        }
        rdt = typeFactory.createSqlType(
            SqlTypeName.DECIMAL,
            ScalarType.MAX_PRECISION,
            argumentType.getScale());
        break;
      case FLOAT:
      case DOUBLE:
        rdt = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        break;
      default:
        rdt = typeFactory.createSqlType(SqlTypeName.BIGINT);
        break;
    }
    return typeFactory.createTypeWithNullability(rdt, true);
  }

  public static RelDataType deriveArithmeticType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2, ArithmeticExpr.Operator op) {
    try {
      Type t1 = ImpalaTypeConverter.createImpalaType(type1);
      Type t2 = ImpalaTypeConverter.createImpalaType(type2);
      // Call out to Impala code to get the correct derived precision on
      // arithmetic operations.
      if (t1.equals(Type.TIMESTAMP) || t2.equals(Type.TIMESTAMP)) {
        return ImpalaTypeConverter.getRelDataType(Type.TIMESTAMP);
      }
      if (SqlTypeName.INTERVAL_TYPES.contains(type1.getSqlTypeName())) {
        return type1;
      }
      if (SqlTypeName.INTERVAL_TYPES.contains(type2.getSqlTypeName())) {
        return type2;
      }

      Type retType = TypesUtil.getArithmeticResultType(t1, t2, op, true);
      SqlTypeName sqlTypeName =
          ImpalaTypeConverter.getRelDataType(retType).getSqlTypeName();
      RelDataType preNullableType =
          (sqlTypeName == SqlTypeName.DECIMAL)
              ? typeFactory.createSqlType(sqlTypeName,
                  retType.getPrecision(), retType.getDecimalDigits())
              : typeFactory.createSqlType(sqlTypeName);
      boolean nullable = type1.isNullable() && type2.isNullable();
      return typeFactory.createTypeWithNullability(preNullableType, nullable);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RelDataType deriveDecimalPlusType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    return deriveArithmeticType(typeFactory, type1, type2,  ArithmeticExpr.Operator.ADD);
  }

  @Override
  public RelDataType deriveDecimalMultiplyType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    return deriveArithmeticType(typeFactory, type1, type2,
        ArithmeticExpr.Operator.MULTIPLY);
  }

  @Override
  public RelDataType deriveDecimalDivideType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    return deriveArithmeticType(typeFactory, type1, type2,
        ArithmeticExpr.Operator.DIVIDE);
  }

  @Override
  public RelDataType deriveDecimalModType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    return deriveArithmeticType(typeFactory, type1, type2, ArithmeticExpr.Operator.MOD);
  }

  @Override
  public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory,
      RelDataType argumentType) {
    try {
      switch (argumentType.getSqlTypeName()) {
        case DECIMAL: {
          // This code is similar to code in Impala's FunctionCallExpr
          ScalarType t1 = (ScalarType) ImpalaTypeConverter.createImpalaType(argumentType);
          int digitsBefore = t1.decimalPrecision() - t1.decimalScale();
          int digitsAfter = t1.decimalScale();
          int resultScale = Math.max(ScalarType.MIN_ADJUSTED_SCALE, digitsAfter);
          int resultPrecision = digitsBefore + resultScale;
          return typeFactory.createSqlType(SqlTypeName.DECIMAL, resultPrecision,
              resultScale);
        }
        case TIMESTAMP: {
          return argumentType;
        }
        default: {
          return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
