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

import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TExprNode;

/**
 * Class describing a BETWEEN predicate. This predicate needs to be rewritten into a
 * CompoundPredicate for it to be executable, i.e., it is illegal to call toThrift()
 * on this predicate because there is no BE implementation.
 */
public class BetweenPredicate extends Predicate {

  private final boolean isNotBetween_;

  // First child is the comparison expr which should be in [lowerBound, upperBound].
  public BetweenPredicate(Expr compareExpr, Expr lowerBound, Expr upperBound,
      boolean isNotBetween) {
    children_.add(compareExpr);
    children_.add(lowerBound);
    children_.add(upperBound);
    isNotBetween_ = isNotBetween;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected BetweenPredicate(BetweenPredicate other) {
    super(other);
    isNotBetween_ = other.isNotBetween_;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    super.analyzeImpl(analyzer);
    if (children_.get(0) instanceof Subquery &&
        (children_.get(1) instanceof Subquery || children_.get(2) instanceof Subquery)) {
      throw new AnalysisException("Comparison between subqueries is not " +
          "supported in a BETWEEN predicate: " + toSqlImpl());
    }
    analyzer.castAllToCompatibleType(children_);
  }

  public boolean isNotBetween() { return isNotBetween_; }

  @Override
  protected void toThrift(TExprNode msg) {
    throw new IllegalStateException(
        "BetweenPredicate needs to be rewritten into a CompoundPredicate.");
  }

  @Override
  public String toSqlImpl() {
    String notStr = (isNotBetween_) ? "NOT " : "";
    return children_.get(0).toSql() + " " + notStr + "BETWEEN " +
        children_.get(1).toSql() + " AND " + children_.get(2).toSql();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    return isNotBetween_ == ((BetweenPredicate)obj).isNotBetween_;
  }

  @Override
  public Expr clone() { return new BetweenPredicate(this); }
}
