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


/**
 * Combination of expr and ASC/DESC.
 *
 */
class OrderByElement {
  private final Expr expr;
  private final boolean isAsc;

  public OrderByElement(Expr expr, boolean isAsc) {
    super();
    this.expr = expr;
    this.isAsc = isAsc;
  }

  public Expr getExpr() {
    return expr;
  }

  public boolean getIsAsc() {
    return isAsc;
  }
}
