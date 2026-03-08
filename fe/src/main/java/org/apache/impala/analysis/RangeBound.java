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
 * Represents a parsed range partition bound value with its inclusiveness and
 * whether it was specified in reversed operator form (e.g., VALUES >= X instead
 * of X <= VALUES). The 'reversed' flag indicates that the bound was specified in
 * a position opposite to its semantic meaning and needs to be swapped during
 * range partition construction.
 */
public class RangeBound {
  private final List<Expr> values_;
  private final boolean inclusive_;
  // True if this bound was specified with a reversed comparator (> or >=)
  // and needs to be moved to the opposite bound position.
  private final boolean reversed_;

  public RangeBound(List<Expr> values, boolean inclusive, boolean reversed) {
    values_ = values;
    inclusive_ = inclusive;
    reversed_ = reversed;
  }

  public RangeBound(List<Expr> values, boolean inclusive) {
    this(values, inclusive, false);
  }

  public List<Expr> getValues() { return values_; }
  public boolean isInclusive() { return inclusive_; }
  public boolean isReversed() { return reversed_; }
}
