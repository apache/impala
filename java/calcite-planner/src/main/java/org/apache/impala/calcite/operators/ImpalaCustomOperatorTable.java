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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.ExplicitOperatorBinding;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.impala.analysis.ArithmeticExpr;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.impala.catalog.Type;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImpalaCustomOperatorTable extends ReflectiveSqlOperatorTable {

  protected static final Logger LOG = LoggerFactory.getLogger(
      ImpalaCustomOperatorTable.class.getName());
  //~ Static fields/initializers ---------------------------------------------

  private static final RelDataType inferReturnTypeForArithmeticOps(
      SqlOperatorBinding opBinding, ArithmeticExpr.Operator op) {

    List<RelDataType> operandTypes = opBinding.collectOperandTypes();

    if (SqlTypeUtil.isDate(operandTypes.get(0)) ||
        SqlTypeUtil.isDate(operandTypes.get(1))) {
      return ImpalaTypeConverter.getRelDataType(Type.DATE);
    }

    if (SqlTypeUtil.isDatetime(operandTypes.get(0)) ||
        SqlTypeUtil.isDatetime(operandTypes.get(1))) {
      return ImpalaTypeConverter.getRelDataType(Type.TIMESTAMP);
    }

    RelDataType type0 =
        getOperandForArithmeticOpSqlOperatorBinding(opBinding, operandTypes, 0);
    RelDataType type1 =
        getOperandForArithmeticOpSqlOperatorBinding(opBinding, operandTypes, 1);

    RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    ImpalaTypeSystemImpl typeSystemImpl =
        (ImpalaTypeSystemImpl) typeFactory.getTypeSystem();
    return typeSystemImpl.deriveArithmeticType(typeFactory, type0, type1, op);
  };

  private static RelDataType getOperandForArithmeticOpSqlOperatorBinding(
      SqlOperatorBinding opBinding, List<RelDataType> operandTypes, int index) {
    if (opBinding instanceof ExplicitOperatorBinding) {
      return operandTypes.get(index);
    } else if (opBinding.isOperandNull(index, true)) {
      // for nulltypes, just assume smallest possible number type
      return ImpalaTypeConverter.getRelDataType(Type.TINYINT);
    } else if (operandTypes.get(index).getSqlTypeName().equals(SqlTypeName.INTEGER) &&
        opBinding.isOperandLiteral(index, true)) {
      // For literal types, currently Calcite treats all of them as INTEGERs. Impala
      // requires the smallest possible type (e.g. 2 should be a TINYINT), and needs
      // this to infer the proper return type. Note though: this method is only used
      // for inferring the return type and not coercing the operand, which will stay
      // an INTEGER for now. The operand will be coerced later in the compilation,
      // under the coercenodes modules.
      BigDecimal bd0 = opBinding.getOperandLiteralValue(index, BigDecimal.class);
      RelDataType rdt =
          ImpalaTypeConverter.getLiteralDataType(bd0, opBinding.getOperandType(index));
      return rdt;
    }

    return opBinding.getOperandType(index);
  }

  private static final Supplier<ImpalaCustomOperatorTable> INSTANCE =
      Suppliers.memoize(() ->
          (ImpalaCustomOperatorTable) new ImpalaCustomOperatorTable().init());

  public static final SqlReturnTypeInference ADD_ADJUSTED_RETURN_TYPE = opBinding -> {
    return inferReturnTypeForArithmeticOps(opBinding, ArithmeticExpr.Operator.ADD);
  };

  public static final SqlReturnTypeInference ADD_ADJUSTED_RETURN_TYPE_NULLABLE =
      ADD_ADJUSTED_RETURN_TYPE.andThen(SqlTypeTransforms.TO_NULLABLE);

  public static final SqlReturnTypeInference MINUS_ADJUSTED_RETURN_TYPE = opBinding -> {
    return inferReturnTypeForArithmeticOps(opBinding, ArithmeticExpr.Operator.SUBTRACT);
  };

  public static final SqlReturnTypeInference MINUS_ADJUSTED_RETURN_TYPE_NULLABLE =
      MINUS_ADJUSTED_RETURN_TYPE.andThen(SqlTypeTransforms.TO_NULLABLE);

  public static final SqlReturnTypeInference MULT_ADJUSTED_RETURN_TYPE = opBinding -> {
    return inferReturnTypeForArithmeticOps(opBinding, ArithmeticExpr.Operator.MULTIPLY);
  };

  public static final SqlReturnTypeInference MULT_ADJUSTED_RETURN_TYPE_NULLABLE =
      MULT_ADJUSTED_RETURN_TYPE.andThen(SqlTypeTransforms.TO_NULLABLE);

  public static final SqlReturnTypeInference DIVIDE_ADJUSTED_RETURN_TYPE = opBinding -> {
    return inferReturnTypeForArithmeticOps(opBinding, ArithmeticExpr.Operator.DIVIDE);
  };

  public static final SqlReturnTypeInference DIVIDE_ADJUSTED_RETURN_TYPE_NULLABLE =
      DIVIDE_ADJUSTED_RETURN_TYPE.andThen(SqlTypeTransforms.TO_NULLABLE);

  public static final SqlReturnTypeInference MOD_ADJUSTED_RETURN_TYPE = opBinding -> {
    return inferReturnTypeForArithmeticOps(opBinding, ArithmeticExpr.Operator.MOD);
  };

  public static final SqlReturnTypeInference MOD_ADJUSTED_RETURN_TYPE_NULLABLE =
      MOD_ADJUSTED_RETURN_TYPE.andThen(SqlTypeTransforms.TO_NULLABLE);

  public static final SqlReturnTypeInference STRING_TYPE = opBinding -> {
    return ImpalaTypeConverter.getRelDataType(Type.STRING);
  };

  public static final SqlBinaryOperator PLUS =
      new SqlMonotonicBinaryOperator(
          "+",
          SqlKind.PLUS,
          40,
          true,
          ADD_ADJUSTED_RETURN_TYPE_NULLABLE,
          InferTypes.FIRST_KNOWN,
          OperandTypes.PLUS_OPERATOR);

  public static final SqlBinaryOperator MINUS =
      new SqlMonotonicBinaryOperator(
          "-",
          SqlKind.MINUS,
          40,
          true,
          MINUS_ADJUSTED_RETURN_TYPE_NULLABLE,
          InferTypes.FIRST_KNOWN,
          OperandTypes.MINUS_OPERATOR);

  public static final SqlBinaryOperator MULTIPLY =
      new SqlMonotonicBinaryOperator(
          "*",
          SqlKind.TIMES,
          60,
          true,
          MULT_ADJUSTED_RETURN_TYPE_NULLABLE,
          InferTypes.FIRST_KNOWN,
          OperandTypes.MULTIPLY_OPERATOR);

  public static final SqlBinaryOperator DIVIDE =
      new SqlBinaryOperator(
          "/",
          SqlKind.DIVIDE,
          60,
          true,
          DIVIDE_ADJUSTED_RETURN_TYPE_NULLABLE,
          InferTypes.FIRST_KNOWN,
          OperandTypes.DIVISION_OPERATOR);

  public static final SqlBinaryOperator PERCENT_REMAINDER =
      new SqlBinaryOperator(
          "%",
          SqlKind.MOD,
          60,
          true,
          MOD_ADJUSTED_RETURN_TYPE_NULLABLE,
          null,
          OperandTypes.NUMERIC_NUMERIC);

  public static final SqlAggFunction COUNT =
      new SqlCountAggFunction("COUNT", OperandTypes.VARIADIC);

  public static ImpalaCustomOperatorTable instance() {
    return INSTANCE.get();
  }
}
