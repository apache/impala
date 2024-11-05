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

import com.google.common.base.Preconditions;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;
import org.apache.impala.calcite.functions.FunctionResolver;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.impala.catalog.Function;

import java.util.List;

/**
 * ImpalaCastFunction is the operator that handles explicit casts within the
 * parsed query. It is created in order to avoid Calcite optimizing out the
 * operator where we can potentially lose the originally designated type.
 */
public class ImpalaCastFunction extends ImpalaOperator {

  public static ImpalaCastFunction INSTANCE = new ImpalaCastFunction();

  public ImpalaCastFunction() {
    super("EXPLICIT_CAST");
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {

    final List<RelDataType> operandTypes =
        CommonOperatorFunctions.getOperandTypes(opBinding);

    String castFunctionName = "castto" + getCastToName(operandTypes.get(1));
    RexBuilder rexBuilder =
        new RexBuilder(new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl()));
    RelDataTypeFactory factory = rexBuilder.getTypeFactory();

    List<RelDataType> castFromList = operandTypes.subList(0, 1);

    return CommonOperatorFunctions.isNullable(castFromList)
        ? factory.createTypeWithNullability(operandTypes.get(1), true)
        : operandTypes.get(1);
  }

  private String getCastToName(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case TINYINT:
        return "tinyint";
      case SMALLINT:
        return "smallint";
      case INTEGER:
        return "int";
      case BIGINT:
        return "bigint";
      case VARCHAR:
        return "string";
      case BOOLEAN:
        return "boolean";
      case FLOAT:
        return "float";
      case REAL:
      case DOUBLE:
        return "double";
      case DECIMAL:
        return "decimal";
      case CHAR:
        return "char";
      case TIMESTAMP:
        return "timestamp";
      case DATE:
        return "date";
      case BINARY:
        return "binary";
      default:
        throw new RuntimeException("Type " + type + "  not supported for casting.");
    }
  }
}

