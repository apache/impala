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
 * Return value of the grammar production that parses function
 * parameters. These parameters can be for scalar or aggregate functions.
 */
public class FunctionParams implements Cloneable {
  private final boolean isStar_;
  private boolean isDistinct_;
  private boolean isIgnoreNulls_;
  private final List<Expr> exprs_;

  // c'tor for non-star params
  public FunctionParams(boolean isDistinct, boolean isIgnoreNulls, List<Expr> exprs) {
    this.isStar_ = false;
    this.isDistinct_ = isDistinct;
    this.isIgnoreNulls_ = isIgnoreNulls;
    this.exprs_ = exprs;
  }

  // c'tor for non-star, non-ignore-nulls params
  public FunctionParams(boolean isDistinct, List<Expr> exprs) {
    this(isDistinct, false, exprs);
  }

  // c'tor for non-star, non-distinct, non-ignore-nulls params
  public FunctionParams(List<Expr> exprs) {
    this(false, false, exprs);
  }

  static public FunctionParams createStarParam() {
    return new FunctionParams();
  }

  public boolean isStar() { return isStar_; }
  public boolean isDistinct() { return isDistinct_; }
  public void setIsDistinct(boolean v) { isDistinct_ = v; }
  public boolean isIgnoreNulls() { return isIgnoreNulls_; }
  public void setIsIgnoreNulls(boolean b) { isIgnoreNulls_ = b; }
  public List<Expr> exprs() { return exprs_; }
  public int size() { return exprs_ == null ? 0 : exprs_.size(); }

  // c'tor for <agg>(*)
  private FunctionParams() {
    exprs_ = null;
    isStar_ = true;
    isDistinct_ = false;
    isIgnoreNulls_ = false;
  }
}
