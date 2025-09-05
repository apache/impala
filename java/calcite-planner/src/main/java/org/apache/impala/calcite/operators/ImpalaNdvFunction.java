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

import org.apache.calcite.sql.SqlCallBinding;

import java.math.BigDecimal;

/**
 * Implementation of NDV that handles special validation logic specific
 * to Impala.
 */
public class ImpalaNdvFunction extends ImpalaAggOperator {
  public ImpalaNdvFunction() {
    super("NDV");
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
    int operandCount = callBinding.getOperandCount();
    if (operandCount < 1 || operandCount > 2) {
      throw new IllegalArgumentException("Error in NDV function, " +
          "must contain 1 or 2 parameters.");
    }

    if (operandCount == 1) {
      return true;
    }

    BigDecimal bd = callBinding.getOperandLiteralValue(1, BigDecimal.class);
    if (bd == null) {
      throw new IllegalArgumentException("Error in NDV function, " +
          "second parameter needs to be an integer.");
    }

    if (bd.intValue() < 1 || bd.intValue() > 10) {
      throw new IllegalArgumentException("Error in NDV function, " +
          "second parameter needs to be between 1 and 10.");
    }

    return true;
  }
}
