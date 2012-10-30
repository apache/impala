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

  /**
   * Substitute all the ordering expression according to the substitution map.
   * @param sMap
   */
  public void substitute(Expr.SubstitutionMap sMap) {
    Expr.substituteList(orderingExprs, sMap);
  }
}

