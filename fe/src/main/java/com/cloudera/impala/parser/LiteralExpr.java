// (c) Copyright 2011 Cloudera, Inc.

package com.cloudera.impala.parser;

import java.lang.String;

class LiteralExpr extends Expr {
  static public LiteralExpr createNumericLiteral(Number n) {
    return new LiteralExpr();
  }

  static public LiteralExpr createStringLiteral(String s) {
    return new LiteralExpr();
  }

  static public LiteralExpr createBoolLiteral(Boolean b) {
    return new LiteralExpr();
  }

  private LiteralExpr() {
    super();
  }
}
