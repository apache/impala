// (c) Copyright 2011 Cloudera, Inc.

package com.cloudera.impala.parser;

class LiteralPredicate extends Predicate {
  private boolean value;

  static public LiteralPredicate True() {
    return new LiteralPredicate(true);
  }

  static public LiteralPredicate False() {
    return new LiteralPredicate(false);
  }

  private LiteralPredicate(boolean val) {
    super();
    value = val;
  }
}
