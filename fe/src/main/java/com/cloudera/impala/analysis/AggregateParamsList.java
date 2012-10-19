// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.List;

/**
 * Return value of the grammar production that parses aggregate function
 * parameters.
 *
 */
class AggregateParamsList {
  private final boolean isStar;
  private final boolean isDistinct;
  private List<Expr> exprs;

  // c'tor for non-star params
  public AggregateParamsList(boolean isDistinct, List<Expr> exprs) {
    super();
    isStar = false;
    this.isDistinct = isDistinct;
    this.exprs = exprs;
  }

  static public AggregateParamsList createStarParam() {
    return new AggregateParamsList();
  }

  public boolean isStar() { return isStar; }
  public boolean isDistinct() { return isDistinct; }
  public List<Expr> exprs() { return exprs; }

  // c'tor for <agg>(*)
  private AggregateParamsList() {
    super();
    isStar = true;
    isDistinct = false;
  }
}
