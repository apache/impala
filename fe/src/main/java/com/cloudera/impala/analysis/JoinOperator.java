// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.thrift.TJoinOp;

public enum JoinOperator {
  INNER_JOIN("INNER JOIN", TJoinOp.INNER_JOIN),
  LEFT_OUTER_JOIN("LEFT OUTER JOIN", TJoinOp.LEFT_OUTER_JOIN),
  LEFT_SEMI_JOIN("LEFT SEMI JOIN", TJoinOp.LEFT_SEMI_JOIN),
  RIGHT_OUTER_JOIN("RIGHT OUTER JOIN", TJoinOp.RIGHT_OUTER_JOIN),
  FULL_OUTER_JOIN("FULL OUTER JOIN", TJoinOp.FULL_OUTER_JOIN);

  private final String description;
  private final TJoinOp thriftJoinOp;

  private JoinOperator(String description, TJoinOp thriftJoinOp) {
    this.description = description;
    this.thriftJoinOp = thriftJoinOp;
  }

  @Override
  public String toString() {
    return description;
  }

  public TJoinOp toThrift() {
    return thriftJoinOp;
  }

  public boolean isOuterJoin() {
    return this == LEFT_OUTER_JOIN
        || this == RIGHT_OUTER_JOIN
        || this == FULL_OUTER_JOIN;
  }
}


