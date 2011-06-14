// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.cloudera.impala.catalog.PrimitiveType;
import com.google.common.base.Preconditions;

public class IntLiteral extends LiteralExpr {
  private final long value;

  public IntLiteral(Long value) {
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
}
