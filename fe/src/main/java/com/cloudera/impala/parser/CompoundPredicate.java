// (c) Copyright 2011 Cloudera, Inc.

package com.cloudera.impala.parser;

import java.lang.String;

class CompoundPredicate extends Predicate {
  enum Operator {
    AND,
    OR,
    NOT
  }

  public CompoundPredicate(Operator op, Predicate p1, Predicate p2) {
    super();
  }
}
