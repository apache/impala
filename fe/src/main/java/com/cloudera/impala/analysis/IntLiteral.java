// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

public class IntLiteral extends LiteralExpr {
  private long value;

  private void init(Long value) {
    this.value = value.longValue();
    if (this.value <= Byte.MAX_VALUE && this.value >= Byte.MIN_VALUE) {
      type = PrimitiveType.TINYINT;
    } else if (this.value <= Short.MAX_VALUE && this.value >= Short.MIN_VALUE) {
      type = PrimitiveType.SMALLINT;
    } else if (this.value <= Integer.MAX_VALUE && this.value >= Integer.MIN_VALUE) {
      type = PrimitiveType.INT;
    } else {
      Preconditions.checkState(this.value <= Long.MAX_VALUE
          && this.value >= Long.MIN_VALUE);
      type = PrimitiveType.BIGINT;
    }
  }

  public IntLiteral(Long value) {
    init(value);
  }

  /** C'tor forcing type, e.g., due to implicit cast */
  public IntLiteral(Long value, PrimitiveType type) {
    this.value = value.longValue();
    this.type = type;
  }

  public IntLiteral(String value) throws AnalysisException {
    Long intValue = null;
    try {
      intValue = new Long(value);
    } catch (NumberFormatException e) {
      throw new AnalysisException("invalid integer literal: " + value);
    }
    init(intValue);
  }

  public long getValue() {
    return value;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return value == ((IntLiteral) obj).value;
  }

  @Override
  public String toSql() {
    return Long.toString(value);
  }

  @Override
  protected Expr uncheckedCastTo(PrimitiveType targetType) throws AnalysisException {
    Preconditions.checkState(targetType.isNumericType());
    if (targetType.isFixedPointType()) {
      this.type = targetType;
      return this;
    } else if (targetType.isFloatingPointType()) {
      return new FloatLiteral(new Double(value), targetType);
    }
    return this;
  }
}
