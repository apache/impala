// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import java.math.BigInteger;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TIntLiteral;
import com.google.common.base.Preconditions;

public class IntLiteral extends LiteralExpr {
  // We use a higher-resolution type to address edge cases such as Long.MIN_VALUE,
  // which the lexer/parser cannot properly recognize by itself.
  // We detect overflow and set the type in analysis.
  private BigInteger value;

  public IntLiteral(BigInteger value) {
    this.value = value;
  }

  public IntLiteral(String value) throws AnalysisException,
      AuthorizationException {
    Long longValue = null;
    try {
      longValue = new Long(value);
    } catch (NumberFormatException e) {
      throw new AnalysisException("invalid integer literal: " + value, e);
    }
    this.value = BigInteger.valueOf(longValue.longValue());
    analyze(null);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    super.analyze(analyzer);
    if (value.compareTo(BigInteger.valueOf(Byte.MAX_VALUE)) <= 0 &&
        value.compareTo(BigInteger.valueOf(Byte.MIN_VALUE)) >= 0) {
      type = PrimitiveType.TINYINT;
    } else if (value.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) <= 0 &&
        value.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) >= 0) {
      type = PrimitiveType.SMALLINT;
    } else if (value.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) <= 0 &&
        value.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) >= 0) {
      type = PrimitiveType.INT;
    } else {
      if (value.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0 ||
          value.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
        throw new AnalysisException(
            String.format("Literal '%s' exceeds maximum range of integers.", toSql()));
      }
      type = PrimitiveType.BIGINT;
    }
  }

  public long getValue() {
    return value.longValue();
  }

  @Override
  public String getStringValue() {
    return value.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return value.equals(((IntLiteral) obj).value);
  }

  @Override
  public String toSqlImpl() {
    return getStringValue();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.INT_LITERAL;
    msg.int_literal = new TIntLiteral(value.longValue());
  }

  @Override
  protected Expr uncheckedCastTo(PrimitiveType targetType) throws AnalysisException {
    Preconditions.checkState(targetType.isNumericType());
    if (targetType.isFixedPointType()) {
      this.type = targetType;
      return this;
    } else if (targetType.isFloatingPointType()) {
      return new FloatLiteral(new Double(value.longValue()), targetType);
    }
    return this;
  }

  @Override
  public void swapSign() throws NotImplementedException {
    value = value.negate();
  }
}
