// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.cloudera.impala.catalog.PrimitiveType;

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
}
