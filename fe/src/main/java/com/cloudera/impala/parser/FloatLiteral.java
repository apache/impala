// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

public class FloatLiteral extends LiteralExpr {
  private final double value;

  public FloatLiteral(Double value) {
    this.value = value.doubleValue();
    if ((this.value <= Float.MAX_VALUE && this.value >= Float.MIN_VALUE) || this.value == 0.0f) {
      type = PrimitiveType.FLOAT;
    } else {
      Preconditions.checkState((this.value <= Double.MAX_VALUE
          && this.value >= Double.MIN_VALUE) || this.value == 0.0);
      type = PrimitiveType.DOUBLE;
    }
  }

  /**
   * C'tor forcing type, e.g., due to implicit cast
   */
  public FloatLiteral(Double value, PrimitiveType type) {
    this.value = value.doubleValue();
    this.type = type;
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
