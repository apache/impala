// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TStringLiteral;
import com.google.common.base.Preconditions;

public class StringLiteral extends LiteralExpr {
  private final String value;

  public StringLiteral(String value) {
    this.value = value;
    type = PrimitiveType.STRING;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((StringLiteral) obj).value.equals(value);
  }

  @Override
  public String toSql() {
    return "'" + value + "'";
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.STRING_LITERAL;
    msg.string_literal = new TStringLiteral(value);
  }

  public String getValue() {
    return value;
  }

  @Override
  protected Expr uncheckedCastTo(PrimitiveType targetType) throws AnalysisException {
    Preconditions.checkState(targetType.isNumericType() || targetType.isDateType());
    LiteralExpr newLiteral = this;
    if (targetType.isNumericType()) {
      newLiteral = convertToNumber(targetType);
    } else if (targetType.isDateType()) {
      newLiteral = convertToDate(targetType);
    }
    return newLiteral;
  }

  /**
   * Convert a string literal to numeric literal
   *
   * @param targetType
   *          is the desired type
   * @return new converted literal (not null)
   * @throws AnalysisException
   *           if NumberFormatException occurs,
   *           or if floating point value is NaN or infinite
   */
  private LiteralExpr convertToNumber(PrimitiveType targetType)
      throws AnalysisException {
    if (targetType.isFixedPointType()) {
      Long newValue = null;
      try {
        newValue = new Long(value);
      } catch (NumberFormatException e) {
        throw new AnalysisException("Cannot convert " + value + " to a fixed-point type", e);
      }
      return (LiteralExpr) new IntLiteral(newValue).castTo(targetType);
    } else {
      // floating point type
      Preconditions.checkArgument(targetType.isFloatingPointType());
      Double newValue = null;
      try {
        newValue = new Double(value);
      } catch (NumberFormatException e) {
        throw new AnalysisException("Cannot convert " + value + " to a floating point type", e);
      }
      // conversion succeeded but literal is infinity or not a number
      if (newValue.isInfinite() || newValue.isNaN()) {
        throw new AnalysisException("Conversion from " +
            type.toString() + " to " +
            targetType.toString() +
            " resulted in infinity or NaN.");
      }
      return (LiteralExpr) new FloatLiteral(newValue).castTo(targetType);
    }
  }

  /**
   * Convert a string literal to a date literal
   *
   * @param targetType
   *          is the desired type
   * @return new converted literal (not null)
   * @throws AnalysisException
   *           when entire given string cannot be transformed into a date
   */
  private LiteralExpr convertToDate(PrimitiveType targetType)
      throws AnalysisException {
    LiteralExpr newLiteral = null;
    newLiteral = new DateLiteral(value, targetType);
    return newLiteral;
  }
}
