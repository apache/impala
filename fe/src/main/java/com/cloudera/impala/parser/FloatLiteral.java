// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.cloudera.impala.catalog.PrimitiveType;
import com.google.common.base.Preconditions;

class FloatLiteral extends LiteralExpr {
  private final double value;

  public FloatLiteral(Double value) {
    this.value = value.doubleValue();
    if (this.value <= Float.MAX_VALUE && this.value >= Float.MIN_VALUE) {
      type = PrimitiveType.FLOAT;
    } else {
      Preconditions.checkState(this.value <= Double.MAX_VALUE
          && this.value >= Double.MIN_VALUE);
      type = PrimitiveType.BIGINT;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((FloatLiteral) obj).value == value;
  }

  public String toSql() {
    return Double.toString(value);
  }
}
