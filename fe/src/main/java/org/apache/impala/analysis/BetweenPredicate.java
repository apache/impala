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

import java.util.Objects;

import org.apache.impala.catalog.ScalarType;
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

  /* Returns true if all the children should be cast to decimal. */
  private boolean checkDecimalCast() {
    boolean allScalar = true;
    // If there is at least one float, then all the children need to be cast to
    // float (instead of decimal).
    boolean noFloats = true;
    boolean atLeastOneDecimal = false;
    for(int i = 0; i < children_.size(); ++i) {
      if (!children_.get(i).getType().isScalarType()) allScalar = false;
      // If at least one child is a float, then we want to cast all the children to float.
      if (children_.get(i).getType().isFloatingPointType()) noFloats = false;
      if (children_.get(i).getType().isDecimal()) atLeastOneDecimal = true;
    }

    return allScalar && noFloats && atLeastOneDecimal;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    super.analyzeImpl(analyzer);
    if (children_.get(0) instanceof Subquery &&
        (children_.get(1) instanceof Subquery || children_.get(2) instanceof Subquery)) {
      throw new AnalysisException("Comparison between subqueries is not " +
          "supported in a BETWEEN predicate: " + toSqlImpl());
    }

    if (checkDecimalCast()) {
      for(int i = 0; i < children_.size(); ++i) {
        ScalarType t = (ScalarType) children_.get(i).getType();
        // The backend function can handle decimals of different precision and scale, so
        // it is ok if the children don't have the same decimal type.
        children_.get(i).castTo(t.getMinResolutionDecimal());
      }
    } else {
      analyzer.castAllToCompatibleType(children_);
    }
  }

  @Override
  protected float computeEvalCost() { return UNKNOWN_COST; }

  public boolean isNotBetween() { return isNotBetween_; }

  @Override
  protected void toThrift(TExprNode msg) {
    throw new IllegalStateException(
        "BetweenPredicate needs to be rewritten into a CompoundPredicate.");
  }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    String notStr = (isNotBetween_) ? "NOT " : "";
    return children_.get(0).toSql(options) + " " + notStr + "BETWEEN "
        + children_.get(1).toSql(options) + " AND " + children_.get(2).toSql(options);
  }

  @Override
  protected boolean localEquals(Expr that) {
    return super.localEquals(that) &&
        isNotBetween_ == ((BetweenPredicate)that).isNotBetween_;
  }

  @Override
  protected int localHash() {
    return Objects.hash(super.localHash(), isNotBetween_);
  }

  @Override
  public Expr clone() { return new BetweenPredicate(this); }
}
