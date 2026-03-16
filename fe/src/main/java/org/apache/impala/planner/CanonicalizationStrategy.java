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

package org.apache.impala.planner;

import org.apache.impala.thrift.TCanonicalizationStrategy;

/**
 * Defines canonicalization strategies for History-Based Optimization (HBO).
 * Each strategy represents a different level of predicate normalization,
 * with higher error levels providing more aggressive canonicalization that
 * increases cache hit rates at the cost of less accurate statistics matching.
 *
 * Strategies are ordered by increasing "error level" (aggressiveness):
 * - EXPR_REWRITE: Most accurate, only normalizes syntax
 * - IGNORE_PARTITION_CONSTANTS: Moderate, ignores constants in partition predicates
 */
public enum CanonicalizationStrategy {
  /**
   * Basic expression rewriting and normalization.
   * - Sorts conjuncts for deterministic ordering
   * - Sorts IN predicate values
   * - Relies on ExprRewriter having already normalized binary predicates and OR chains
   * - Keeps all constants intact
   *
   * Example: WHERE a IN (2, 1) → WHERE a IN (1, 2)
   */
  EXPR_REWRITE(0),

  /**
   * Removes constants from equality predicates on partition columns.
   * Assumes all partitions have similar characteristics.
   * Only point predicates (equality and IN) are canonicalized.
   *
   * Example: WHERE ds = '2020-01-01' → WHERE ds = <CONST>
   * Non-example: WHERE ds > '2020-01-01' (range predicate, keeps constant)
   */
  IGNORE_PARTITION_CONSTANTS(1);

  // Lower values indicate higher accuracy, higher values indicate more aggressive
  // matching.
  private final int errorLevel_;

  CanonicalizationStrategy(int errorLevel) {
    this.errorLevel_ = errorLevel;
  }

  public int getErrorLevel() {
    return errorLevel_;
  }

  public TCanonicalizationStrategy toThrift() {
    switch (this) {
      case EXPR_REWRITE:
        return TCanonicalizationStrategy.EXPR_REWRITE;
      case IGNORE_PARTITION_CONSTANTS:
        return TCanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS;
      default:
        throw new IllegalStateException("Unknown strategy: " + this);
    }
  }
}
