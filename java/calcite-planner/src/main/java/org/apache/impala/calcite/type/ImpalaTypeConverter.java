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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TPrimitiveType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A utility class that holds methods that convert impala types to Calcite
 * types and vice versa.
 *
 * One important distinction is the different classes used here. On the Impala side,
 * there are:
 * - Normalized Type names (e.g. Type.BOOLEAN).  These are pre-created types. This becomes
 * important for types with precisions like decimal, char, and varchar.  The Type.DECIMAL
 * (and char and varchar) do not have any precision (or scale) associated with them.  In
 * the function signatures, all precisions are allowed, so this type is used when
 * describing them.
 * - types with precisions.  These also use Types (also of type ScalarType), but the
 * precision (and scale) is included with the datatype.
 *
 * On the Calcite side, there are:
 * - Normalized RelDataTypes.  While theoretically we should have been able to use
 * SqlTypeName to have the same purpose as the Impala default dataypes, there is no
 * SqlTypeName.STRING. Therefore, we needed to resort to RelDataTypes for this purpose.
 * - types with precisions. The normal RelDataType is used.
 */
public class ImpalaTypeConverter {

  // Maps Impala default types to Calcite default types.
  private static Map<Type, RelDataType> impalaToCalciteMap;

  static {
    RexBuilder rexBuilder =
        new RexBuilder(new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl()));
    RelDataTypeFactory factory = rexBuilder.getTypeFactory();
    Map<Type, RelDataType> map = new HashMap<>();
    map.put(Type.BOOLEAN, factory.createSqlType(SqlTypeName.BOOLEAN));
    map.put(Type.TINYINT, factory.createSqlType(SqlTypeName.TINYINT));
    map.put(Type.SMALLINT, factory.createSqlType(SqlTypeName.SMALLINT));
    map.put(Type.INT, factory.createSqlType(SqlTypeName.INTEGER));
    map.put(Type.BIGINT, factory.createSqlType(SqlTypeName.BIGINT));
    map.put(Type.FLOAT, factory.createSqlType(SqlTypeName.FLOAT));
    map.put(Type.DOUBLE, factory.createSqlType(SqlTypeName.DOUBLE));
    map.put(Type.TIMESTAMP, factory.createSqlType(SqlTypeName.TIMESTAMP));
    map.put(Type.DATE, factory.createSqlType(SqlTypeName.DATE));
    map.put(Type.DECIMAL, factory.createSqlType(SqlTypeName.DECIMAL));
    map.put(Type.BINARY, factory.createSqlType(SqlTypeName.BINARY));
    map.put(Type.CHAR, factory.createSqlType(SqlTypeName.CHAR, 1));
    map.put(Type.VARCHAR, factory.createSqlType(SqlTypeName.VARCHAR, 1));
    map.put(Type.STRING, factory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE));
    map.put(Type.NULL, factory.createSqlType(SqlTypeName.NULL));

    ImmutableMap.Builder<Type, RelDataType> builder = ImmutableMap.builder();
    for (Type t : map.keySet()) {
      RelDataType r = map.get(t);
      builder.put(t, factory.createTypeWithNullability(r, true));
    }
    impalaToCalciteMap = builder.build();
  }

  /**
   * Create a new RelDataType given the Impala type.
   */
  public static RelDataType createRelDataType(RelDataTypeFactory factory,
      Type impalaType) {
    if (impalaType == null) {
      return null;
    }
    TPrimitiveType primitiveType = impalaType.getPrimitiveType().toThrift();
    ScalarType scalarType = (ScalarType) impalaType;
    switch (primitiveType)  {
      case DECIMAL:
        RelDataType decimalDefinedRetType = factory.createSqlType(SqlTypeName.DECIMAL,
            scalarType.decimalPrecision(), scalarType.decimalScale());
        return factory.createTypeWithNullability(decimalDefinedRetType, true);
      case VARCHAR:
        RelDataType varcharType = factory.createSqlType(SqlTypeName.VARCHAR,
            scalarType.getLength());
        return factory.createTypeWithNullability(varcharType, true);
      case CHAR:
        RelDataType charType = factory.createSqlType(SqlTypeName.CHAR,
            scalarType.getLength());
        return factory.createTypeWithNullability(charType, true);
      default:
        Type normalizedImpalaType = getImpalaType(primitiveType);
        return impalaToCalciteMap.get(normalizedImpalaType);
    }
  }

  /**
   * Get the normalized RelDataType given an impala type.
   */
  public static RelDataType getRelDataType(Type impalaType) {
    if (impalaType == null) {
      return null;
    }
    TPrimitiveType primitiveType = impalaType.getPrimitiveType().toThrift();
    Type normalizedImpalaType = getImpalaType(primitiveType);
    return impalaToCalciteMap.get(normalizedImpalaType);
  }

  /**
   * Create Impala types given primitive types.
   * Primitive types should not be exposed outside of this class.
   */
  public static Type getImpalaType(TPrimitiveType argType) {
    // Char and decimal contain precisions and need to be treated separately from
    // the rest. The precisions for this case are unknown though, as we are only given
    // a "primitivetype'.
    switch (argType) {
      case CHAR:
        return Type.CHAR;
      case VARCHAR:
        return Type.VARCHAR;
      case DECIMAL:
        return Type.DECIMAL;
      case BOOLEAN:
        return Type.BOOLEAN;
      case TINYINT:
        return Type.TINYINT;
      case SMALLINT:
        return Type.SMALLINT;
      case INT:
        return Type.INT;
      case BIGINT:
        return Type.BIGINT;
      case FLOAT:
        return Type.FLOAT;
      case DOUBLE:
        return Type.DOUBLE;
      case TIMESTAMP:
        return Type.TIMESTAMP;
      case DATE:
        return Type.DATE;
      case STRING:
        return Type.STRING;
      case FIXED_UDA_INTERMEDIATE:
        return Type.FIXED_UDA_INTERMEDIATE;
      case NULL_TYPE:
        return Type.NULL;
      case BINARY:
        return Type.BINARY;
      default:
        throw new RuntimeException("Unknown type " + argType);
    }
  }

  /**
   * Create a new impala type given a relDataType
   */
  public static Type createImpalaType(RelDataType relDataType) {
    // First retrieve the normalized impala type
    if (relDataType.getSqlTypeName() == SqlTypeName.VARCHAR &&
        ((relDataType.getPrecision() == Integer.MAX_VALUE) ||
        (relDataType.getPrecision() == -1))) {
      return Type.STRING;
    }
    Type impalaType = getType(relDataType.getSqlTypeName());
    // create the impala type given the normalized type, precision, and scale.
    return createImpalaType(impalaType, relDataType.getPrecision(),
        relDataType.getScale());
  }

  public static Type createImpalaType(Type impalaType, int precision, int scale) {
    TPrimitiveType primitiveType = impalaType.getPrimitiveType().toThrift();
    // Char, varchar, decimal, and fixed_uda_intermediate contain precisions and need to
    // be treated separately.
    switch (primitiveType) {
      case CHAR:
        return ScalarType.createCharType(precision);
      case VARCHAR:
        return (precision == Integer.MAX_VALUE || precision == -1)
           ? Type.STRING
           : ScalarType.createVarcharType(precision);
      case DECIMAL:
        if (precision == -1) {
          return Type.DECIMAL;
        }
        return ScalarType.createDecimalType(precision, scale);
      case FIXED_UDA_INTERMEDIATE:
        return ScalarType.createFixedUdaIntermediateType(precision);
      default:
        return impalaType;
    }
  }

  public static Type getType(SqlTypeName calciteTypeName) {
    switch (calciteTypeName) {
      case TINYINT:
        return Type.TINYINT;
      case SMALLINT:
        return Type.SMALLINT;
      case INTEGER:
        return Type.INT;
      case INTERVAL_YEAR:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_HOUR:
      case INTERVAL_MINUTE:
      case INTERVAL_SECOND:
      case BIGINT:
        return Type.BIGINT;
      case VARCHAR:
        return Type.VARCHAR;
      case BOOLEAN:
        return Type.BOOLEAN;
      case REAL:
      case FLOAT:
        return Type.FLOAT;
      case DOUBLE:
        return Type.DOUBLE;
      case DECIMAL:
        return Type.DECIMAL;
      case CHAR:
        return Type.CHAR;
      case TIMESTAMP:
        return Type.TIMESTAMP;
      case DATE:
        return Type.DATE;
      case NULL:
        return Type.NULL;
      case BINARY:
        return Type.BINARY;
      default:
        throw new RuntimeException("Type " + calciteTypeName + "  not supported yet.");
    }
  }

  // helper function to handle translation of lists.
  public static List<Type> getNormalizedImpalaTypes(List<RelDataType> relDataTypes) {
    return Lists.transform(relDataTypes, ImpalaTypeConverter::getNormalizedImpalaType);
  }

  /**
   * Return the default impala type given a reldatatype that potentially has precision.
   */
  public static ScalarType getNormalizedImpalaType(RelDataType relDataType) {
    SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
    if (SqlTypeName.INTERVAL_TYPES.contains(sqlTypeName)) {
      return Type.BIGINT;
    }
    switch (sqlTypeName) {
      case VARCHAR:
        return relDataType.getPrecision() == Integer.MAX_VALUE
            ? Type.STRING : Type.VARCHAR;
      case CHAR:
        return Type.CHAR;
      case DECIMAL:
        return Type.DECIMAL;
      case BOOLEAN:
        return Type.BOOLEAN;
      case TINYINT:
        return Type.TINYINT;
      case SMALLINT:
        return Type.SMALLINT;
      case INTEGER:
        return Type.INT;
      case BIGINT:
        return Type.BIGINT;
      case FLOAT:
      case REAL:
        return Type.FLOAT;
      case DOUBLE:
        return Type.DOUBLE;
      case TIMESTAMP:
        return Type.TIMESTAMP;
      case DATE:
        return Type.DATE;
      case BINARY:
        return Type.BINARY;
      case SYMBOL:
        return null;
      case NULL:
        return Type.NULL;
      default:
        throw new RuntimeException("Unknown SqlTypeName " + sqlTypeName +
            " to convert to Impala.");
    }
  }

  public static List<RelDataType> createRelDataTypes(List<Type> impalaTypes) {
    List<RelDataType> result = Lists.newArrayList();
    for (Type t : impalaTypes) {
      result.add(createRelDataType(t));
    }
    return result;
  }

  /**
   * Create a new RelDataType given the Impala type.
   */
  public static RelDataType createRelDataType(Type impalaType) {
    if (impalaType == null) {
      return null;
    }
    TPrimitiveType primitiveType = impalaType.getPrimitiveType().toThrift();
    if (primitiveType == TPrimitiveType.DECIMAL) {
      ScalarType scalarType = (ScalarType) impalaType;
      RexBuilder rexBuilder =
          new RexBuilder(new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl()));
      RelDataTypeFactory factory = rexBuilder.getTypeFactory();
      RelDataType decimalDefinedRetType = factory.createSqlType(SqlTypeName.DECIMAL,
          scalarType.decimalPrecision(), scalarType.decimalScale());
      return factory.createTypeWithNullability(decimalDefinedRetType, true);
    }
    // for all other arguments besides decimal, we just normalize the datatype and return
    // the previously created RelDataType.
    Type normalizedImpalaType = getImpalaType(primitiveType);
    return impalaToCalciteMap.get(normalizedImpalaType);
  }
}
