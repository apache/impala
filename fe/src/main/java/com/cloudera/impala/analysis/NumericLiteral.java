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

import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.thrift.TDecimalLiteral;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TFloatLiteral;
import com.cloudera.impala.thrift.TIntLiteral;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Literal for all numeric values, including integer, floating-point and decimal types.
 * Analysis of this expr determines the smallest type that can hold this value.
 */
public class NumericLiteral extends LiteralExpr {
  // Use the java BigDecimal (arbitrary scale/precision) to represent the value.
  // This object has notions of precision and scale but they do *not* match what
  // we need. BigDecimal's precision is similar to significant figures and scale
  // is the exponent.
  // ".1" could be represented with an unscaled value = 1 and scale = 1 or
  // unscaled value = 100 and scale = 3. Manipulating the value_ (e.g. multiplying
  // it by 10) does not unnecessarily change the unscaled value. Special care
  // needs to be taken when converting between the big decimals unscaled value
  // and ours. (See getUnscaledValue()).
  private BigDecimal value_;

  // If true, this literal has been explicitly cast to a type and should not
  // be analyzed (which infers the type from value_).
  private boolean explicitlyCast_;

  public NumericLiteral(BigDecimal value) {
    init(value);
  }

  public NumericLiteral(String value, Type t) throws AnalysisException {
    BigDecimal val = null;
    try {
      val = new BigDecimal(value);
    } catch (NumberFormatException e) {
      throw new AnalysisException("invalid numeric literal: " + value, e);
    }
    init(val);
    this.analyze(null);
    if (type_.isDecimal() && t.isDecimal()) {
      // Verify that the input decimal value is consistent with the specified
      // column type.
      ScalarType scalarType = (ScalarType) t;
      if (!scalarType.isSupertypeOf((ScalarType) type_)) {
        StringBuilder errMsg = new StringBuilder();
        errMsg.append("invalid ").append(t);
        errMsg.append(" value: " + value);
        throw new AnalysisException(errMsg.toString());
      }
    }
    if (t.isFloatingPointType()) explicitlyCastToFloat(t);
  }

  /**
   * The versions of the ctor that take types assume the type is correct
   * and the NumericLiteral is created as analyzed with that type. The specified
   * type is preserved across substitutions and re-analysis.
   */
  public NumericLiteral(BigInteger value, Type type) {
    isAnalyzed_ = true;
    value_ = new BigDecimal(value);
    type_ = type;
    explicitlyCast_ = true;
  }

  public NumericLiteral(BigDecimal value, Type type) {
    isAnalyzed_ = true;
    value_ = value;
    type_ = type;
    explicitlyCast_ = true;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected NumericLiteral(NumericLiteral other) {
    super(other);
    value_ = other.value_;
    explicitlyCast_ = other.explicitlyCast_;
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
    return ((NumericLiteral) obj).value_.equals(value_);
  }

  @Override
  public int hashCode() { return value_.hashCode(); }

  @Override
  public String toSqlImpl() { return getStringValue(); }
  @Override
  public String getStringValue() { return value_.toString(); }
  public double getDoubleValue() { return value_.doubleValue(); }
  public long getLongValue() { return value_.longValue(); }
  public long getIntValue() { return value_.intValue(); }

  @Override
  protected void toThrift(TExprNode msg) {
    switch (type_.getPrimitiveType()) {
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
        msg.node_type = TExprNodeType.INT_LITERAL;
        msg.int_literal = new TIntLiteral(value_.longValue());
        break;
      case FLOAT:
      case DOUBLE:
        msg.node_type = TExprNodeType.FLOAT_LITERAL;
        msg.float_literal = new TFloatLiteral(value_.doubleValue());
        break;
      case DECIMAL:
        msg.node_type = TExprNodeType.DECIMAL_LITERAL;
        TDecimalLiteral literal = new TDecimalLiteral();
        literal.setValue(getUnscaledValue().toByteArray());
        msg.decimal_literal = literal;
        break;
      default:
        Preconditions.checkState(false);
    }
  }

  public BigDecimal getValue() { return value_; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    if (!explicitlyCast_) {
      // Compute the precision and scale from the BigDecimal.
      type_ = TypesUtil.computeDecimalType(value_);
      if (type_ == null) {
        Double d = new Double(value_.doubleValue());
        if (d.isInfinite()) {
          throw new AnalysisException("Numeric literal '" + toSql() +
              "' exceeds maximum range of doubles.");
        } else if (d.doubleValue() == 0 && value_ != BigDecimal.ZERO) {
          throw new AnalysisException("Numeric literal '" + toSql() +
              "' underflows minimum resolution of doubles.");
        }

        // Literal could not be stored in any of the supported decimal precisions and
        // scale. Store it as a float/double instead.
        float fvalue;
        fvalue = value_.floatValue();
        if (fvalue == value_.doubleValue()) {
          type_ = Type.FLOAT;
        } else {
          type_ = Type.DOUBLE;
        }
      } else {
        // Check for integer types.
        Preconditions.checkState(type_.isScalarType());
        ScalarType scalarType = (ScalarType) type_;
        if (scalarType.decimalScale() == 0) {
          if (value_.compareTo(BigDecimal.valueOf(Byte.MAX_VALUE)) <= 0 &&
              value_.compareTo(BigDecimal.valueOf(Byte.MIN_VALUE)) >= 0) {
            type_ = Type.TINYINT;
          } else if (value_.compareTo(BigDecimal.valueOf(Short.MAX_VALUE)) <= 0 &&
              value_.compareTo(BigDecimal.valueOf(Short.MIN_VALUE)) >= 0) {
            type_ = Type.SMALLINT;
          } else if (value_.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) <= 0 &&
              value_.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) >= 0) {
            type_ = Type.INT;
          } else if (value_.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0 &&
              value_.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) >= 0) {
            type_ = Type.BIGINT;
          }
        }
      }
    }
    isAnalyzed_ = true;
  }

  /**
   * Explicitly cast this literal to 'targetType'. The targetType must be a
   * float point type.
   */
  protected void explicitlyCastToFloat(Type targetType) {
    Preconditions.checkState(targetType.isFloatingPointType());
    type_ = targetType;
    explicitlyCast_ = true;
  }

  @Override
  protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
    Preconditions.checkState(targetType.isNumericType());
    // Implicit casting to decimals allows truncating digits from the left of the
    // decimal point (see TypesUtil). A literal that is implicitly cast to a decimal
    // with truncation is wrapped into a CastExpr so the BE can evaluate it and report
    // a warning. This behavior is consistent with casting/overflow of non-constant
    // exprs that return decimal.
    // IMPALA-1837: Without the CastExpr wrapping, such literals can exceed the max
    // expected byte size sent to the BE in toThrift().
    if (targetType.isDecimal()) {
      ScalarType decimalType = (ScalarType) targetType;
      // analyze() ensures that value_ never exceeds the maximum scale and precision.
      Preconditions.checkState(isAnalyzed_);
      // Sanity check that our implicit casting does not allow a reduced precision or
      // truncating values from the right of the decimal point.
      Preconditions.checkState(value_.precision() <= decimalType.decimalPrecision());
      Preconditions.checkState(value_.scale() <= decimalType.decimalScale());
      int valLeftDigits = value_.precision() - value_.scale();
      int typeLeftDigits = decimalType.decimalPrecision() - decimalType.decimalScale();
      if (typeLeftDigits < valLeftDigits) return new CastExpr(targetType, this);
    }
    type_ = targetType;
    return this;
  }

  @Override
  public void swapSign() throws NotImplementedException {
    // swapping sign does not change the type
    value_ = value_.negate();
  }

  @Override
  public int compareTo(LiteralExpr o) {
    int ret = super.compareTo(o);
    if (ret != 0) return ret;
    NumericLiteral other = (NumericLiteral) o;
    return value_.compareTo(other.value_);
  }

  private void init(BigDecimal value) {
    isAnalyzed_ = false;
    value_ = value;
  }

  // Returns the unscaled value of this literal. BigDecimal doesn't treat scale
  // the way we do. We need to pad it out with zeros or truncate as necessary.
  private BigInteger getUnscaledValue() {
    Preconditions.checkState(type_.isDecimal());
    BigInteger result = value_.unscaledValue();
    int valueScale = value_.scale();
    // If valueScale is less than 0, it indicates the power of 10 to multiply the
    // unscaled value. This path also handles this case by padding with zeros.
    // e.g. unscaled value = 123, value scale = -2 means 12300.
    ScalarType decimalType = (ScalarType) type_;
    return result.multiply(BigInteger.TEN.pow(decimalType.decimalScale() - valueScale));
  }

  @Override
  public Expr clone() { return new NumericLiteral(this); }

  /**
   * Check overflow.
   */
  public static boolean isOverflow(BigDecimal value, Type type)
      throws AnalysisException {
    switch (type.getPrimitiveType()) {
      case TINYINT:
        return (value.compareTo(BigDecimal.valueOf(Byte.MAX_VALUE)) > 0 ||
            value.compareTo(BigDecimal.valueOf(Byte.MIN_VALUE)) < 0);
      case SMALLINT:
        return (value.compareTo(BigDecimal.valueOf(Short.MAX_VALUE)) > 0 ||
            value.compareTo(BigDecimal.valueOf(Short.MIN_VALUE)) < 0);
      case INT:
        return (value.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0 ||
            value.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0);
      case BIGINT:
        return (value.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0 ||
            value.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) < 0);
      case FLOAT:
        return (value.compareTo(BigDecimal.valueOf(Float.MAX_VALUE)) > 0 ||
            value.compareTo(BigDecimal.valueOf(Float.MIN_VALUE)) < 0);
      case DOUBLE:
        return (value.compareTo(BigDecimal.valueOf(Double.MAX_VALUE)) > 0 ||
            value.compareTo(BigDecimal.valueOf(Double.MIN_VALUE)) < 0);
      case DECIMAL:
        return (TypesUtil.computeDecimalType(value) == null);
      default:
        throw new AnalysisException("Overflow check on " + type + " isn't supported.");
    }
  }
}
