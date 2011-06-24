// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

public class FloatLiteral extends LiteralExpr {
  private double value;

  private void init(Double value) {
    this.value = value.doubleValue();
    if ((this.value <= Float.MAX_VALUE && this.value >= Float.MIN_VALUE) || this.value == 0.0f) {
      type = PrimitiveType.FLOAT;
    } else {
      Preconditions.checkState((this.value <= Double.MAX_VALUE
          && this.value >= Double.MIN_VALUE) || this.value == 0.0);
      type = PrimitiveType.DOUBLE;
    }
  }

  public FloatLiteral(Double value) {
    init(value);
  }

  /**
   * C'tor forcing type, e.g., due to implicit cast
   */
  public FloatLiteral(Double value, PrimitiveType type) {
    this.value = value.doubleValue();
    this.type = type;
  }

  public FloatLiteral(String value) throws AnalysisException {
    Double floatValue = null;
    try {
      floatValue = new Double(value);
    } catch (NumberFormatException e) {
      throw new AnalysisException("invalid floating-point literal: " + value);
    }
    init(floatValue);
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((FloatLiteral) obj).value == value;
  }

  @Override
  public String toSql() {
    return Double.toString(value);
  }

  public double getValue() {
    return value;
  }

  @Override
  protected Expr uncheckedCastTo(PrimitiveType targetType) throws AnalysisException {
    Preconditions.checkState(targetType.isFloatingPointType());
    type = targetType;
    return this;
  }
}
