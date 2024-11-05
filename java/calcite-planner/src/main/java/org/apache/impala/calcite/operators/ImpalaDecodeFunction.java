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
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;

import java.util.List;

/**
 * Special operator for Decode.
 *
 * The decode operator has to infer its return type off of a compatible
 * parameter from all the choices of return parameters. These choices
 * are the third parameter, the fifth parameter, the seventh, etc...
 * If there are an even number of parameters, the last parameter is part
 * of an else clause, so that is also a return parameter.
 *
 * This class also ensures that the search parameters are all compatible
 * as well.  These are all the parameters not mentioned; The first, second,
 * fourth, sixth, and every other parameter except the last one.
 *
 * An example of a decode function (in the documentation) is:
 * DECODE(day_of_week, 1, "Monday", 2, "Tuesday", 3, "Wednesday",
 *     4, "Thursday", 5, "Friday", 6, "Saturday", 7, "Sunday", "Unknown day")
 * The return values here are of type string, and the search parameters are of
 * some numeric type for day_of_week and the choices for day of the week.
 *
 */
public class ImpalaDecodeFunction extends ImpalaOperator {

  public static ImpalaDecodeFunction INSTANCE = new ImpalaDecodeFunction();

  private ImpalaDecodeFunction() {
    super("DECODE");
  }


  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    List<RelDataType> operandTypes = CommonOperatorFunctions.getOperandTypes(opBinding);

    RexBuilder rexBuilder =
        new RexBuilder(new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl()));
    RelDataTypeFactory factory = rexBuilder.getTypeFactory();

    // No need to capture the return value, but an exception will be thrown
    // if the parameters are not compatible.
    getCompatibleSearchOperand(operandTypes, factory);

    return getCompatibleReturnType(operandTypes, factory);
  }

  public static RelDataType getCompatibleSearchOperand(
      List<RelDataType> operandTypes, RelDataTypeFactory factory)  {
    // check search operands are all compatible, grab the first.
    RelDataType commonSearchOperand = operandTypes.get(0);
    int searchOperandsToCheck = operandTypes.size()/2;
    // Check that the second, fourth, sixth, etc... parameter is compatible
    // but don't check the last one.
    for (int i = 1; i < operandTypes.size() - 1; i += 2) {
      commonSearchOperand = ImpalaTypeConverter.getCompatibleType(commonSearchOperand,
          operandTypes.get(i), factory);
      if (commonSearchOperand == null) {
        throw new IllegalArgumentException("Decode function has incompatible " +
            "types with search argument and argument number " + i);
      }
    }
    return commonSearchOperand;
  }

  public static RelDataType getCompatibleReturnType(
      List<RelDataType> operandTypes, RelDataTypeFactory factory)  {
    // Initialize with the third parameter
    RelDataType returnType = operandTypes.get(2);
    returnType = factory.createTypeWithNullability(returnType, true);
    // Skip over every other parameter starting with the fifth one.
    for (int i = 4; i < operandTypes.size(); i += 2) {
      returnType = ImpalaTypeConverter.getCompatibleType(returnType,
          operandTypes.get(i), factory);
      if (returnType == null) {
        throw new IllegalArgumentException("Decode function has incompatible " +
            "return type (argument number) " + i);
      }
    }
    // If there are an even number of parameters, the last one is the else
    // clause, so check that one too.
    if ((operandTypes.size() % 2) == 0) {
      returnType = ImpalaTypeConverter.getCompatibleType(returnType,
          operandTypes.get(operandTypes.size() - 1), factory);
    }
    return returnType;
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.from(3);
  }
}
