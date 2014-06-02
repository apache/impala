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
import com.cloudera.impala.catalog.ColumnType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TIntLiteral;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class IntLiteral extends LiteralExpr {
  // We use a higher-resolution type to address edge cases such as Long.MIN_VALUE,
  // which the lexer/parser cannot properly recognize by itself.
  // We detect overflow and set the type in analysis.
  private BigInteger value_;

  public IntLiteral(BigInteger value) {
    this.value_ = value;
    isAnalyzed_ = false;
  }

  public IntLiteral(String value) throws AnalysisException,
      AuthorizationException {
    Long longValue = null;
    try {
      longValue = new Long(value);
    } catch (NumberFormatException e) {
      throw new AnalysisException("invalid integer literal: " + value, e);
    }
    this.value_ = BigInteger.valueOf(longValue.longValue());
    isAnalyzed_ = false;
    analyze(null);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    if (value_.compareTo(BigInteger.valueOf(Byte.MAX_VALUE)) <= 0 &&
        value_.compareTo(BigInteger.valueOf(Byte.MIN_VALUE)) >= 0) {
      type_ = ColumnType.TINYINT;
    } else if (value_.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) <= 0 &&
        value_.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) >= 0) {
      type_ = ColumnType.SMALLINT;
    } else if (value_.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) <= 0 &&
        value_.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) >= 0) {
      type_ = ColumnType.INT;
    } else {
      if (value_.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0 ||
          value_.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
        throw new AnalysisException(
            String.format("Literal '%s' exceeds maximum range of integers.", toSql()));
      }
      type_ = ColumnType.BIGINT;
    }
    isAnalyzed_ = true;
  }

  public long getValue() { return value_.longValue(); }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("value", value_)
        .toString();
  }

  @Override
  public String getStringValue() {
    return value_.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return value_.equals(((IntLiteral) obj).value_);
  }

  @Override
  public String toSqlImpl() {
    return getStringValue();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.INT_LITERAL;
    msg.int_literal = new TIntLiteral(value_.longValue());
  }

  @Override
  protected Expr uncheckedCastTo(ColumnType targetType) throws AnalysisException {
    Preconditions.checkState(targetType.isNumericType());
    if (targetType.isIntegerType()) {
      this.type_ = targetType;
      return this;
    } else if (targetType.isFloatingPointType() || targetType.isDecimal()) {
      return new DecimalLiteral(value_, targetType);
    }
    Preconditions.checkState(false, "Unhandled case");
    return this;
  }

  @Override
  public void swapSign() throws NotImplementedException {
    value_ = value_.negate();
  }

  @Override
  public int compareTo(LiteralExpr o) {
    if (!(o instanceof IntLiteral)) return -1;
    IntLiteral other = (IntLiteral) o;
    if (getValue() > other.getValue()) return 1;
    if (getValue() < other.getValue()) return -1;
    return 0;
  }
}
