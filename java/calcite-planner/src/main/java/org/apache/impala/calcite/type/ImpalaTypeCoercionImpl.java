/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.Type;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ImpalaTypeCoercionImpl extends TypeCoercionImpl {

  public ImpalaTypeCoercionImpl(RelDataTypeFactory typeFactory, SqlValidator validator) {
    super(typeFactory, validator);
  }

  @Override
  public RelDataType getWiderTypeFor(List<RelDataType> typeList,
      boolean stringPromotion) {
    // a little hack. At type coercion (validation) time, we can't tell if the type
    // is a char column or a char literal.  The problem is that Calcite, as is, treats
    // char literals as char type instead of STRING. Let's treat all char types as
    // STRING right now. If it does turn out to be a real CHAR column, this will
    // be caught when resolving functions.
    List<RelDataType> newTypeList = new ArrayList<>();
    for (RelDataType type : typeList) {
      if (type.getSqlTypeName().equals(SqlTypeName.CHAR)) {
        newTypeList.add(ImpalaTypeConverter.getRelDataType(Type.STRING));
      } else {
        newTypeList.add(type);
      }
    }

    return ImpalaTypeConverter.getCompatibleType(newTypeList, factory);
  }

  // Do type coercion for In Clause. Calcite allows numerics
  // of different types to match (e.g. smallint to decimal). Most
  // of these issues are taken care of in Coerce*Rules, but the problem
  // with "In" is that Calcite converts the "in" clause to a bunch of
  // "or" clauses in its RelNodeConverter step. At this point, some
  // valuable information is lost.  For instance, in the clause:
  // "10.2 IN (int_col)", the clause is converted to
  // =(10.2, cast(int_col as DECIMAL(3,1)".  Of course, an int_col will
  // never be equal to 10.2, so this would kinda be ok...but this causes
  // an overflow error on the Impala backend. So we will set the right
  // casting at validation time for the "In" clause.
  @Override
  public boolean inOperationCoercion(SqlCallBinding binding) {
    // Only handle IN (or NOT IN) clause with a list as its second param
    if (!(binding.operand(1) instanceof SqlNodeList)) {
      return false;
    }

    SqlCall call = binding.getCall();
    SqlValidatorScope scope = binding.getScope();

    SqlNode leftOperand = call.operand(0);
    RelDataType leftOperandType = deriveType(validator, scope, leftOperand);

    SqlNodeList inList = (SqlNodeList) call.operand(1);
    List<RelDataType> rightOperandTypes = new ArrayList<>();
    Set<RelDataType> uniqueRightOperandTypes = new HashSet<>();
    for (SqlNode node : inList) {
      RelDataType derivedType = deriveType(validator, scope, node);
      rightOperandTypes.add(derivedType);
      uniqueRightOperandTypes.add(derivedType);
    }

    // commonType will contain a compatible type for both the left side of the
    // IN operator and all the types within the IN clause.
    RelDataType commonType =
        ImpalaTypeConverter.getCompatibleType(uniqueRightOperandTypes, factory);
    commonType =
        ImpalaTypeConverter.getCompatibleType(commonType, leftOperandType, factory);

    // This will mutate the binding if changed.  The "coerced" parameter is set
    // to true to let the caller know that something mutated.
    boolean coerced = coerceInOperand(scope, call, 0, leftOperandType, commonType);

    coerced |= coerceInList(scope, inList, uniqueRightOperandTypes, rightOperandTypes,
        commonType);

    return coerced;
  }

  private boolean coerceInOperand(SqlValidatorScope scope, SqlCall call,
      int index, RelDataType fromType, RelDataType toType) {
    if (!needsCasting(fromType, toType)) {
      return false;
    }
    SqlNode castNode = castTo(call.operand(index), toType);
    call.setOperand(index, castNode);
    updateInferredType(castNode, toType);
    return true;
  }

  private boolean coerceInList(SqlValidatorScope scope, SqlNodeList inList,
      Set<RelDataType> uniqueFromTypes, List<RelDataType> fromTypes, RelDataType toType) {

    boolean coerced = uniqueFromTypes.stream().anyMatch(ft -> needsCasting(ft, toType));

    if (coerced) {
      for (int i = 0; i < inList.size(); ++i) {
        if (needsCasting(fromTypes.get(i), toType)) {
          SqlNode castNode = castTo(inList.get(i), toType);
          inList.set(i, castNode);
          updateInferredType(castNode, toType);
        }
      }
    }
    return coerced;
  }

  private boolean needsCasting(RelDataType fromType, RelDataType toType) {
    if (fromType.getSqlTypeName().equals(SqlTypeName.NULL)) {
      return false;
    }
    if (toType.getSqlTypeName().equals(fromType.getSqlTypeName())) {
      return false;
    }
    return true;
  }

  private RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope,
      SqlNode node) {
    if (node instanceof SqlCharStringLiteral) {
      return ImpalaTypeConverter.getRelDataType(Type.STRING);
    }
    return validator.deriveType(scope, node);
  }

  private static SqlNode castTo(SqlNode node, RelDataType type) {
    return SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, node,
        SqlTypeUtil.convertTypeToSpec(type).withNullable(type.isNullable()));
  }
}
