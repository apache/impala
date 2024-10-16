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

package org.apache.impala.calcite.operators;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.calcite.type.ImpalaTypeConverter;

/**
 * The ImpalaAdjustScaleFunction operator contains the special Impala rules
 * needed for inferring the return type from its parameters.
 */
public class ImpalaAdjustScaleFunction extends SqlFunction {

  public ImpalaAdjustScaleFunction(String name) {
    super(name, null, SqlKind.OTHER_FUNCTION,
        null, null, OperandTypes.NUMERIC_OPTIONAL_INTEGER,
        SqlFunctionCategory.NUMERIC);
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    if (opBinding.getOperandType(0).getSqlTypeName().equals(SqlTypeName.DOUBLE) ||
        opBinding.getOperandType(0).getSqlTypeName().equals(SqlTypeName.FLOAT)) {
      return ImpalaTypeConverter.getRelDataType(Type.DOUBLE);
    }

    ScalarType impalaType =
        (ScalarType) ImpalaTypeConverter.createImpalaType(opBinding.getOperandType(0));
    ScalarType decimalType = impalaType.getMinResolutionDecimal();

    int precision = getName().equals("ROUND") || getName().equals("DROUND")
        ? Math.min(decimalType.decimalPrecision() + 1,
            ScalarType.MAX_PRECISION)
        : decimalType.decimalPrecision();

    Integer scale = opBinding.getOperandCount() > 1
        ? opBinding.getOperandLiteralValue(1, Integer.class)
        : 0;

    Type newDecimalType = ScalarType.createDecimalType(precision, scale);

    return ImpalaTypeConverter.createRelDataType(newDecimalType);
  }
}
