// (c) Copyright 2011 Cloudera, Inc.

package com.cloudera.impala.parser;

import java.lang.String;

class LikePredicate extends Predicate {
  enum Operator {
    LIKE,
    RLIKE,
    REGEXP
  }

  public LikePredicate(Operator op, Expr e1, Expr e2) {
    super();
  }
}
