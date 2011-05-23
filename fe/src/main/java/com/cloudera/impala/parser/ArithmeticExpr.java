// (c) Copyright 2011 Cloudera, Inc.

package com.cloudera.impala.parser;

import java.lang.String;

class ArithmeticExpr extends Expr {
  enum Operator {
    MULTIPLY,
    DIVIDE,
    MOD,
    INT_DIVIDE,
    PLUS,
    MINUS,
    BITAND,
    BITOR,
    BITXOR,
    BITNOT
  }

  public ArithmeticExpr(Operator op, Expr e1, Expr e2) {
    super();
  }
}
