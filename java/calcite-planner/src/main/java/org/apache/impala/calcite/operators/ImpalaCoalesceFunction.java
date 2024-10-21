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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;

import java.util.List;

/**
 * Implementation of coalesce which generates a return type
 * that is common to all parameters.
 */
public class ImpalaCoalesceFunction extends ImpalaOperator {

  public ImpalaCoalesceFunction() {
    super("COALESCE");
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    RexBuilder rexBuilder =
        new RexBuilder(new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl()));
    RelDataTypeFactory factory = rexBuilder.getTypeFactory();

    List<RelDataType> operands = CommonOperatorFunctions.getOperandTypes(opBinding);
    return ImpalaTypeConverter.getCompatibleType(operands, factory);
  }
}
