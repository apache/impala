// (c) Copyright 2011 Cloudera, Inc.

package com.cloudera.impala.parser;

import java.lang.String;

class BinaryPredicate extends Predicate {
  enum Operator {
    EQ, // =
    NE, // !=, <>
    LE, // <=
    GE, // >=
    LT, // <
    GT, // >
  };

  public BinaryPredicate(Operator op, Expr e1, Expr e2) {
    super();
  }
}
