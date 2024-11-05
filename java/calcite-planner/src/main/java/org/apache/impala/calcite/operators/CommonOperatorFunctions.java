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


import com.google.common.base.Preconditions;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.core.Aggregate.AggCallBinding;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.impala.calcite.functions.FunctionResolver;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * ImpalaOperatorCommon contains common functionality across Agg and nonAgg
 * operators.
 */
public class CommonOperatorFunctions {

  // Allow any count because this is used for all functions. Validation for specific
  // number of parameters will be done when Impala function resolving is done.
  public static SqlOperandCountRange ANY_COUNT_RANGE = SqlOperandCountRanges.any();

  public static RelDataType inferReturnType(SqlOperatorBinding opBinding,
      String name) {
    final List<RelDataType> operandTypes = getOperandTypes(opBinding);

    RexBuilder rexBuilder =
        new RexBuilder(new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl()));
    RelDataTypeFactory factory = rexBuilder.getTypeFactory();

    // Resolve Impala function through Impala method.
    Function fn = FunctionResolver.getSupertypeFunction(name, operandTypes);

    if (fn == null) {
      throw new IllegalArgumentException("Cannot infer return type for "
          + name + "; operand types: " + operandTypes);
    }

    RelDataType returnType =
        ImpalaTypeConverter.getRelDataType(fn.getReturnType());
    return isNullable(operandTypes)
        ? returnType
        : factory.createTypeWithNullability(returnType, true);
  }

  public static SqlOperandCountRange getOperandCountRange() {
    // Validation for operand count ranges are done when checking for signature in
    // the inferReturnType method.
    return ANY_COUNT_RANGE;
  }

  /**
   * getAllowedSignatures used for error messages.
   */
  public static String getAllowedSignatures(String opNameToUse) {
    // TODO: IMPALA-13099 leave blank for now since this might be hard to derive because
    // of implicit type support.
    return "";
  }

  public static List<RelDataType> getOperandTypes(SqlOperatorBinding opBinding) {
    List<RelDataType> operandTypes = new ArrayList<>(opBinding.getOperandCount());
    for (int i = 0; i < opBinding.getOperandCount(); ++i) {
      operandTypes.add(getOperandType(opBinding, i));
    }
    return operandTypes;
  }

  public static RelDataType getOperandType(SqlOperatorBinding opBinding, int operand) {
    if (opBinding instanceof AggCallBinding) {
      return opBinding.getOperandType(operand);
    }

    if (opBinding.isOperandNull(operand, false)) {
      return ImpalaTypeConverter.getRelDataType(Type.NULL);
    }

    RelDataType opType = opBinding.getOperandType(operand);
    if (opType.getSqlTypeName().equals(SqlTypeName.INTEGER) &&
        opBinding.isOperandLiteral(operand, true)) {
      // For literal types, currently Calcite treats all of them as INTEGERs. Impala
      // requires the smallest possible type (e.g. 2 should be a TINYINT), and needs
      // this to infer the proper return type. Note though: this method is only used
      // for inferring the return type and not coercing the operand, which will stay
      // an INTEGER for now. The operand will be coerced later in the compilation,
      // under the coercenodes modules.
      BigDecimal bd0 = opBinding.getOperandLiteralValue(operand, BigDecimal.class);
      return ImpalaTypeConverter.getLiteralDataType(bd0, opType);
    }
    return opType;
  }

  // return true if any operand type is nullable.
  public static boolean isNullable(List<RelDataType> operandTypes) {
    return operandTypes.stream().anyMatch(rdt -> rdt.isNullable());
  }
}
