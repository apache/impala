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

import java.util.List;

/**
 * Represents a parsed range partition bound value with its comparison operator.
 * The comparator is always normalized by the grammar to the form 'bound <cmp> VALUES',
 * so it indicates both which side of the range the bound defines (LESS_THAN/LESS_EQUAL
 * for a lower bound, GREATER_THAN/GREATER_EQUAL for an upper bound) and whether the
 * bound is inclusive. This allows both the canonical forms (e.g. 'X <= VALUES') and the
 * reversed forms accepted by IMPALA-7618 (e.g. 'VALUES >= X') to be represented
 * uniformly, regardless of the syntactic position in which they appear.
 */
public class RangeBound {

  public enum Comparator {
    LESS_THAN,
    LESS_EQUAL,
    GREATER_THAN,
    GREATER_EQUAL;

    public boolean isInclusive() {
      return this == LESS_EQUAL || this == GREATER_EQUAL;
    }
  }

  public enum BoundType {
    LOWER_BOUND,
    UPPER_BOUND;
  }

  private final List<Expr> values_;
  private final Comparator comparator_;

  public RangeBound(List<Expr> values, Comparator comparator) {
    values_ = values;
    comparator_ = comparator;
  }

  public List<Expr> getValues() { return values_; }
  public Comparator getComparator() { return comparator_; }
  public boolean isInclusive() { return comparator_.isInclusive(); }

  /**
   * Returns whether this bound is the lower or the upper bound of the range. Since the
   * comparator is normalized to 'bound <cmp> VALUES', a '<' / '<=' comparator denotes a
   * lower bound and a '>' / '>=' comparator denotes an upper bound.
   */
  public BoundType boundType() {
    if (comparator_ == Comparator.LESS_THAN || comparator_ == Comparator.LESS_EQUAL) {
      return BoundType.LOWER_BOUND;
    }
    return BoundType.UPPER_BOUND;
  }
}
