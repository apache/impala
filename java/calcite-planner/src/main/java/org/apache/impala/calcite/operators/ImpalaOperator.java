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
import org.apache.commons.lang.StringUtils;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
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
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.calcite.functions.FunctionResolver;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ImpalaOperator is a custom Calcite operator that handles all generic functions
 * that are not defined by Calcite. It is preferable to use a Calcite operator
 * if possible because Calcite has optimizations that are based on the operator
 * class.
 */
public class ImpalaOperator extends SqlFunction {
  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaOperator.class.getName());

  // Allow any count because this is used for all functions. Validation for specific
  // number of parameters will be done when Impala function resolving is done.
  public static SqlOperandCountRange ANY_COUNT_RANGE = SqlOperandCountRanges.any();

  public ImpalaOperator(String name) {
    super(name.toUpperCase(), SqlKind.OTHER, null, null, null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    final List<RelDataType> operandTypes = getOperandTypes(opBinding);

    RexBuilder rexBuilder =
        new RexBuilder(new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl()));
    RelDataTypeFactory factory = rexBuilder.getTypeFactory();

    // Resolve Impala function through Impala method.
    // TODO: IMPALA-13022: Right now, CompareMode is INDISTINGUISHABLE because this
    // commit only deals with exact matches.  This will change in a future commit.
    Function fn = FunctionResolver.getFunction(getName(), operandTypes);

    if (fn == null) {
      throw new IllegalArgumentException("Cannot infer return type for "
          + getName() + "; operand types: " + operandTypes);
    }

    RelDataType returnType =
        ImpalaTypeConverter.getRelDataType(fn.getReturnType());
    return isNullable(operandTypes)
        ? returnType
        : factory.createTypeWithNullability(returnType, true);
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
    // Validation for operand types are done when checking for signature in
    // the inferReturnType method.
    return true;
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    // Validation for operand count ranges are done when checking for signature in
    // the inferReturnType method.
    return ANY_COUNT_RANGE;
  }

  /**
   * getAllowedSignatures used for error messages.
   */
  @Override
  public String getAllowedSignatures(String opNameToUse) {
    // TODO: IMPALA-13099 leave blank for now since this might be hard to derive because
    // of implicit type support.
    return "";
  }

  @Override
  public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION;
  }

  private List<RelDataType> getOperandTypes(SqlOperatorBinding opBinding) {
    Preconditions.checkState(opBinding instanceof SqlCallBinding);
    SqlCallBinding callBinding = (SqlCallBinding) opBinding;

    List<RelDataType> operandTypes = new ArrayList<>();
    for (int i = 0; i < callBinding.getOperandCount(); ++i) {
      if (callBinding.isOperandNull(i, false)) {
        operandTypes.add(ImpalaTypeConverter.getRelDataType(Type.NULL));
      } else {
        operandTypes.add(callBinding.getOperandType(i));
      }
    }
    return operandTypes;
  }

  // return false if all operand types are not nullable. Else return true.
  private boolean isNullable(List<RelDataType> operandTypes) {
    for (RelDataType rdt : operandTypes) {
      if (rdt.isNullable()) {
        return true;
      }
    }
    return false;
  }
}
