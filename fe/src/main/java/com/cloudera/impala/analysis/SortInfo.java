// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Encapsulates all the information needed to compute ORDER BY
 * This doesn't contain aliases or positional exprs.
 */
public class SortInfo {
  private final List<Expr> orderingExprs_;
  private final List<Boolean> isAscOrder_;
  // True if "NULLS FIRST", false if "NULLS LAST", null if not specified.
  private final List<Boolean> nullsFirstParams_;

  public SortInfo(List<Expr> orderingExprs,
      List<Boolean> isAscOrder,
      List<Boolean> nullsFirstParams) {
    Preconditions.checkArgument(orderingExprs.size() == isAscOrder.size());
    Preconditions.checkArgument(orderingExprs.size() == nullsFirstParams.size());
    this.orderingExprs_ = orderingExprs;
    this.isAscOrder_ = isAscOrder;
    this.nullsFirstParams_ = nullsFirstParams;
  }

  public List<Expr> getOrderingExprs() { return orderingExprs_; }
  public List<Boolean> getIsAscOrder() { return isAscOrder_; }
  public List<Boolean> getNullsFirstParams() { return nullsFirstParams_; }

  /**
   * Gets the list of booleans indicating whether nulls come first or last, independent
   * of asc/desc.
   */
  public List<Boolean> getNullsFirst() {
    List<Boolean> nullsFirst = Lists.newArrayList();
    for (int i = 0; i < orderingExprs_.size(); ++i) {
      nullsFirst.add(OrderByElement.nullsFirst(nullsFirstParams_.get(i),
          isAscOrder_.get(i)));
    }
    return nullsFirst;
  }

  /**
   * Substitute all the ordering expression according to the substitution map.
   * @param sMap
   */
  public void substitute(Expr.SubstitutionMap sMap) {
    Expr.substituteList(orderingExprs_, sMap);
  }
}

