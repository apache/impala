/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.operators;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlStaticAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.Type;

/**
 * Special implementation of Calcite's SqlMinMaxAggFunction which allows a TIMESTAMP
 * as a parameter.
 */
public class ImpalaMinMaxAggFunction extends SqlMinMaxAggFunction {

  public ImpalaMinMaxAggFunction(SqlKind kind) {
    super(kind);
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    final RelDataType operandType = CommonOperatorFunctions.getOperandType(opBinding, 0);
    switch (operandType.getSqlTypeName()) {
      case NULL:
        return ImpalaTypeConverter.getRelDataType(Type.BOOLEAN);
      case CHAR:
      case VARCHAR:
        return ImpalaTypeConverter.getRelDataType(Type.STRING);
      case TIMESTAMP:
        return ImpalaTypeConverter.getRelDataType(Type.TIMESTAMP);
      default:
        return super.inferReturnType(opBinding);
    }
  }

  @Override
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    return callBinding.getOperandType(0).getSqlTypeName().equals(SqlTypeName.TIMESTAMP)
        ? true
        : super.checkOperandTypes(callBinding, throwOnFailure);
  }
}
