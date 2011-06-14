// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.cloudera.impala.catalog.PrimitiveType;

public class BoolLiteral extends LiteralExpr {
  private final boolean value;

  public BoolLiteral(Boolean value) {
    this.value = value.booleanValue();
    type = PrimitiveType.BOOLEAN;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((BoolLiteral) obj).value == value;
  }

  public boolean getValue() {
    return value;
  }
}
