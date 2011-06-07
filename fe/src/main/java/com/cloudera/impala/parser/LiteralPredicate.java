// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

class LiteralPredicate extends Predicate {
  private final boolean value;

  static public LiteralPredicate True() {
    return new LiteralPredicate(true);
  }

  static public LiteralPredicate False() {
    return new LiteralPredicate(false);
  }

  private LiteralPredicate(boolean val) {
    super();
    this.value = val;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((LiteralPredicate) obj).value == value;
  }
}
