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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;

/**
 * ImpalaAggOperator is a custom Calcite operator that handles all generic functions
 * that are not defined by Calcite. It is preferable to use a Calcite operator
 * if possible because Calcite has optimizations that are based on the operator
 * class or SqlKind.
 */
public class ImpalaAggOperator extends SqlAggFunction {

  public ImpalaAggOperator(String name) {
    super(name.toUpperCase(), SqlKind.OTHER, null, null, null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    return CommonOperatorFunctions.inferReturnType(opBinding, getName());
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
    // Validation for operand types are done when checking for signature in
    // the inferReturnType method.
    return true;
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return CommonOperatorFunctions.getOperandCountRange();
  }

  /**
   * getAllowedSignatures used for error messages.
   */
  @Override
  public String getAllowedSignatures(String opNameToUse) {
    return CommonOperatorFunctions.getAllowedSignatures(opNameToUse);
  }

  @Override
  public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION;
  }
}
