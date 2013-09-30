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
 * Return value of the grammar production that parses function
 * parameters. These parameters can be for scalar or aggregate functions.
 */
class FunctionParams {
  private final boolean isStar;
  private boolean isDistinct;
  private final List<Expr> exprs;

  // c'tor for non-star params
  public FunctionParams(boolean isDistinct, List<Expr> exprs) {
    isStar = false;
    this.isDistinct = isDistinct;
    this.exprs = exprs;
  }

  // c'tor for non-star, non-distinct params
  public FunctionParams(List<Expr> exprs) {
    this(false, exprs);
  }

  static public FunctionParams createStarParam() {
    return new FunctionParams();
  }

  public boolean isStar() { return isStar; }
  public boolean isDistinct() { return isDistinct; }
  public List<Expr> exprs() { return exprs; }

  public void setIsDistinct(boolean v) { isDistinct = v; }

  // c'tor for <agg>(*)
  private FunctionParams() {
    exprs = null;
    isStar = true;
    isDistinct = false;
  }
}
