// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

public enum JoinOperator {
  INNER_JOIN("INNER JOIN"),
  LEFT_OUTER_JOIN("LEFT OUTER JOIN"),
  LEFT_SEMI_JOIN("LEFT SEMI JOIN"),
  RIGHT_OUTER_JOIN("RIGHT OUTER JOIN"),
  FULL_OUTER_JOIN("FULL OUTER JOIN");

  private final String description;
  private JoinOperator(String description) {
    this.description = description;
  }

  public String toString() {
    return description;
  }
}


