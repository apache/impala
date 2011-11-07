// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.List;

/**
 * Encapsulates all the information needed to compute ORDER BY
 * This doesn't contain aliases or positional exprs.
 */
public class SortInfo {
  private final List<Expr> orderingExprs;
  private final List<Boolean> isAscOrder;

  public SortInfo(List<Expr> orderingExprs, List<Boolean> isAscOrder) {
    this.orderingExprs = orderingExprs;
    this.isAscOrder = isAscOrder;
  }

  public List<Expr> getOrderingExprs() {
    return orderingExprs;
  }

  public List<Boolean> getIsAscOrder() {
    return isAscOrder;
  }
}

