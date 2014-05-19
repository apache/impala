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

import java.math.BigDecimal;
import java.math.BigInteger;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.ColumnType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.thrift.TDecimalLiteral;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TFloatLiteral;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Literal for all decimal values. Things that contain a decimal point are
 * parsed to this class. (e.g. "1.0")
 */
public class DecimalLiteral extends LiteralExpr {
  private BigDecimal value_;

  // If true, this literal has been expicitly cast to a type and should not
  // be analyzed (which infers the type from value_).
  private boolean expliciltyCast_;

  public DecimalLiteral(BigDecimal value) {
    init(value);
  }

  public DecimalLiteral(BigInteger value) {
    init(new BigDecimal(value));
  }

  public DecimalLiteral(String value, ColumnType t)
      throws AnalysisException, AuthorizationException {
    BigDecimal val = null;
    try {
      val = new BigDecimal(value);
    } catch (NumberFormatException e) {
      throw new AnalysisException("invalid decimal literal: " + value, e);
    }
    init(val);
    this.analyze(null);
    if (t.isFloatingPointType()) explicitlyCastToFloat(t);
  }

  /**
   * The versions of the ctor that take types assume the type is correct
   * and the DecimalLiteral is created as analyzed with that type.
   */
  public DecimalLiteral(BigInteger value, ColumnType type) {
    isAnalyzed_ = true;
    value_ = new BigDecimal(value);
    type_ = type;
  }

  public DecimalLiteral(BigDecimal value, ColumnType type) {
    isAnalyzed_ = true;
    value_ = value;
    type_ = type;
  }


  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("value", value_)
        .add("type", type_)
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    return ((DecimalLiteral) obj).value_.equals(value_);
  }

  @Override
  public String toSqlImpl() { return getStringValue(); }
  @Override
  public String getStringValue() { return value_.toString(); }
  public double getDoubleValue() { return value_.doubleValue(); }
  public long getLongValue() { return value_.longValue(); }

  @Override
  protected void toThrift(TExprNode msg) {
    switch (type_.getPrimitiveType()) {
      case FLOAT:
      case DOUBLE:
        msg.node_type = TExprNodeType.FLOAT_LITERAL;
        msg.float_literal = new TFloatLiteral(value_.doubleValue());
        break;
      case DECIMAL:
        msg.node_type = TExprNodeType.DECIMAL_LITERAL;
        TDecimalLiteral literal = new TDecimalLiteral();
        literal.setValue(value_.unscaledValue().toByteArray());
        msg.decimal_literal = literal;
        break;
      default:
        Preconditions.checkState(false);
    }
  }

  public BigDecimal getValue() { return value_; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    if (!expliciltyCast_) {
      // Compute the precision and scale from the BigDecimal.
      type_ = TypesUtil.computeDecimalType(value_);
      if (type_ == null) {
        Double d = new Double(value_.doubleValue());
        if (d.isInfinite()) {
          throw new AnalysisException("Decimal literal '" + toSql() +
              "' exceeds maximum range of doubles.");
        } else if (d.doubleValue() == 0 && value_ != BigDecimal.ZERO) {
          throw new AnalysisException("Decimal literal '" + toSql() +
              "' underflows minimum resolution of doubles.");
        }

        // Literal could not be stored in any of the supported decimal precisions and
        // scale. Store it as a float/double instead.
        float fvalue;
        fvalue = value_.floatValue();
        if (fvalue == value_.doubleValue()) {
          type_ = ColumnType.FLOAT;
        } else {
          type_ = ColumnType.DOUBLE;
        }
      }
    }
    type_.analyze();
    isAnalyzed_ = true;
  }

  /**
   * Explicitly cast this literal to 'targetType'. The targetType must be a
   * float point type.
   */
  protected void explicitlyCastToFloat(ColumnType targetType) {
    Preconditions.checkState(targetType.isFloatingPointType());
    type_ = targetType;
    expliciltyCast_ = true;
  }

  @Override
  protected Expr uncheckedCastTo(ColumnType targetType) throws AnalysisException {
    Preconditions.checkState(targetType.isNumericType());
    if (targetType.isFloatingPointType() || targetType.isDecimal()) {
      type_ = targetType;
    } else if (targetType.isIntegerType()) {
      Preconditions.checkState(type_.isDecimal());
      Preconditions.checkState(type_.decimalScale() == 0);
      type_ = targetType;
    } else {
      return new CastExpr(targetType, this, true);
    }
    return this;
  }

  @Override
  public void swapSign() throws NotImplementedException {
    // swapping sign does not change the type
    value_ = value_.negate();
  }

  @Override
  public int compareTo(LiteralExpr o) {
    if (!(o instanceof DecimalLiteral)) return -1;
    DecimalLiteral other = (DecimalLiteral) o;
    return value_.compareTo(other.value_);
  }

  private void init(BigDecimal value) {
    isAnalyzed_ = false;
    value_ = value;
  }
}
