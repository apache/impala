// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.analysis;

import org.apache.impala.thrift.TJoinOp;

public enum JoinOperator {
  INNER_JOIN("INNER JOIN", TJoinOp.INNER_JOIN),
  LEFT_OUTER_JOIN("LEFT OUTER JOIN", TJoinOp.LEFT_OUTER_JOIN),
  LEFT_SEMI_JOIN("LEFT SEMI JOIN", TJoinOp.LEFT_SEMI_JOIN),
  LEFT_ANTI_JOIN("LEFT ANTI JOIN", TJoinOp.LEFT_ANTI_JOIN),
  RIGHT_OUTER_JOIN("RIGHT OUTER JOIN", TJoinOp.RIGHT_OUTER_JOIN),
  RIGHT_SEMI_JOIN("RIGHT SEMI JOIN", TJoinOp.RIGHT_SEMI_JOIN),
  RIGHT_ANTI_JOIN("RIGHT ANTI JOIN", TJoinOp.RIGHT_ANTI_JOIN),
  FULL_OUTER_JOIN("FULL OUTER JOIN", TJoinOp.FULL_OUTER_JOIN),
  CROSS_JOIN("CROSS JOIN", TJoinOp.CROSS_JOIN),
  // Variant of the LEFT ANTI JOIN that is used for the rewrite of
  // NOT IN subqueries. It can have a single equality join conjunct
  // that returns TRUE when the rhs is NULL.
  NULL_AWARE_LEFT_ANTI_JOIN(
      "NULL AWARE LEFT ANTI JOIN", TJoinOp.NULL_AWARE_LEFT_ANTI_JOIN),
  ICEBERG_DELETE_JOIN("ICEBERG DELETE JOIN", TJoinOp.ICEBERG_DELETE_JOIN);

  private final String description_;
  private final TJoinOp thriftJoinOp_;

  private JoinOperator(String description, TJoinOp thriftJoinOp) {
    this.description_ = description;
    this.thriftJoinOp_ = thriftJoinOp;
  }

  @Override
  public String toString() { return description_; }
  public TJoinOp toThrift() { return thriftJoinOp_; }

  public boolean isInnerJoin() { return this == INNER_JOIN; }
  public boolean isLeftOuterJoin() { return this == LEFT_OUTER_JOIN; }
  public boolean isRightOuterJoin() { return this == RIGHT_OUTER_JOIN; }

  public boolean isOuterJoin() {
    return this == LEFT_OUTER_JOIN
        || this == RIGHT_OUTER_JOIN
        || this == FULL_OUTER_JOIN;
  }

  public boolean isSemiJoin() {
    return this == JoinOperator.LEFT_SEMI_JOIN || this == JoinOperator.LEFT_ANTI_JOIN
        || this == JoinOperator.RIGHT_SEMI_JOIN || this == JoinOperator.RIGHT_ANTI_JOIN
        || this == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN
        || this == JoinOperator.ICEBERG_DELETE_JOIN;
  }

  public boolean isLeftSemiJoin() {
    return this == JoinOperator.LEFT_SEMI_JOIN || this == JoinOperator.LEFT_ANTI_JOIN
        || this == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN
        || this == JoinOperator.ICEBERG_DELETE_JOIN;
  }

  public boolean isRightSemiJoin() {
    return this == JoinOperator.RIGHT_SEMI_JOIN || this == JoinOperator.RIGHT_ANTI_JOIN;
  }

  public boolean isCrossJoin() {
    return this == JoinOperator.CROSS_JOIN;
  }

  public boolean isFullOuterJoin() {
    return this == JoinOperator.FULL_OUTER_JOIN;
  }

  public boolean isNullAwareLeftAntiJoin() {
    return this == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
  }

  public boolean isAntiJoin() {
    return this == JoinOperator.LEFT_ANTI_JOIN || this == JoinOperator.RIGHT_ANTI_JOIN
        || this == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN
        || this == JoinOperator.ICEBERG_DELETE_JOIN;
  }

  public boolean isIcebergDeleteJoin() {
    return this == JoinOperator.ICEBERG_DELETE_JOIN;
  }

  public JoinOperator invert() {
    switch (this) {
      case LEFT_OUTER_JOIN: return RIGHT_OUTER_JOIN;
      case RIGHT_OUTER_JOIN: return LEFT_OUTER_JOIN;
      case LEFT_SEMI_JOIN: return RIGHT_SEMI_JOIN;
      case RIGHT_SEMI_JOIN: return LEFT_SEMI_JOIN;
      case LEFT_ANTI_JOIN: return RIGHT_ANTI_JOIN;
      case RIGHT_ANTI_JOIN: return LEFT_ANTI_JOIN;
      case NULL_AWARE_LEFT_ANTI_JOIN:
      case ICEBERG_DELETE_JOIN: throw new IllegalStateException("Not implemented");
      default: return this;
    }
  }
}
