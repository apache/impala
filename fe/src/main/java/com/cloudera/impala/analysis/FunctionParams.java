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
class FunctionParams implements Cloneable {
  private final boolean isStar_;
  private boolean isDistinct_;
  private final List<Expr> exprs_;

  // c'tor for non-star params
  public FunctionParams(boolean isDistinct, List<Expr> exprs) {
    isStar_ = false;
    this.isDistinct_ = isDistinct;
    this.exprs_ = exprs;
  }

  // c'tor for non-star, non-distinct params
  public FunctionParams(List<Expr> exprs) {
    this(false, exprs);
  }

  static public FunctionParams createStarParam() {
    return new FunctionParams();
  }

  public boolean isStar() { return isStar_; }
  public boolean isDistinct() { return isDistinct_; }
  public List<Expr> exprs() { return exprs_; }
  public void setIsDistinct(boolean v) { isDistinct_ = v; }
  public int size() { return exprs_ == null ? 0 : exprs_.size(); }

  // c'tor for <agg>(*)
  private FunctionParams() {
    exprs_ = null;
    isStar_ = true;
    isDistinct_ = false;
  }
}
