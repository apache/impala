// (c) Copyright 2011 Cloudera, Inc.

package com.cloudera.impala.parser;

import java.lang.String;
import java.util.ArrayList;

class AggregateExpr extends Expr {
  enum Operator {
    COUNT,
    MIN,
    MAX,
    SUM,
    AVG
  }

  public AggregateExpr(Operator op, boolean isStar,
                       boolean isDistinct, ArrayList<Expr> exprs) {
    super();
  }
}
