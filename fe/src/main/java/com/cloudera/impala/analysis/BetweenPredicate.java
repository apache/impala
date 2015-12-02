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

import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Class describing between predicates. After successful analysis, we rewrite
 * the between predicate to a conjunctive/disjunctive compound predicate
 * to be handed to the backend.
 */
public class BetweenPredicate extends Predicate {

  private final boolean isNotBetween_;

  // After successful analysis, we rewrite this between predicate
  // into a conjunctive/disjunctive compound predicate.
  private CompoundPredicate rewrittenPredicate_;

  // Children of the BetweenPredicate, since this.children should hold the children
  // of the rewritten predicate to make sure toThrift() picks up the right ones.
  private ArrayList<Expr> originalChildren_ = Lists.newArrayList();

  // First child is the comparison expr which should be in [lowerBound, upperBound].
  public BetweenPredicate(Expr compareExpr, Expr lowerBound, Expr upperBound,
      boolean isNotBetween) {
    originalChildren_.add(compareExpr);
    originalChildren_.add(lowerBound);
    originalChildren_.add(upperBound);
    this.isNotBetween_ = isNotBetween;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected BetweenPredicate(BetweenPredicate other) {
    super(other);
    isNotBetween_ = other.isNotBetween_;
    originalChildren_ = Expr.cloneList(other.originalChildren_);
    if (other.rewrittenPredicate_ != null) {
      rewrittenPredicate_ = (CompoundPredicate) other.rewrittenPredicate_.clone();
    }
  }

  public CompoundPredicate getRewrittenPredicate() {
    Preconditions.checkState(isAnalyzed_);
    return rewrittenPredicate_;
  }
  public ArrayList<Expr> getOriginalChildren() { return originalChildren_; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    if (originalChildren_.get(0) instanceof Subquery &&
        (originalChildren_.get(1) instanceof Subquery ||
         originalChildren_.get(2) instanceof Subquery)) {
      throw new AnalysisException("Comparison between subqueries is not " +
          "supported in a between predicate: " + toSqlImpl());
    }
    analyzer.castAllToCompatibleType(originalChildren_);

    // Rewrite between predicate into a conjunctive/disjunctive compound predicate.
    if (isNotBetween_) {
      // Rewrite into disjunction.
      Predicate lower = new BinaryPredicate(BinaryPredicate.Operator.LT,
          originalChildren_.get(0), originalChildren_.get(1));
      Predicate upper = new BinaryPredicate(BinaryPredicate.Operator.GT,
          originalChildren_.get(0), originalChildren_.get(2));
      rewrittenPredicate_ =
          new CompoundPredicate(CompoundPredicate.Operator.OR, lower, upper);
    } else {
      // Rewrite into conjunction.
      Predicate lower = new BinaryPredicate(BinaryPredicate.Operator.GE,
          originalChildren_.get(0), originalChildren_.get(1));
      Predicate upper = new BinaryPredicate(BinaryPredicate.Operator.LE,
          originalChildren_.get(0), originalChildren_.get(2));
      rewrittenPredicate_ =
          new CompoundPredicate(CompoundPredicate.Operator.AND, lower, upper);
    }

    try {
      rewrittenPredicate_.analyze(analyzer);
      fn_ = rewrittenPredicate_.fn_;
    } catch (AnalysisException e) {
      // We should have already guaranteed that analysis will succeed.
      Preconditions.checkState(false, "Analysis failed in rewritten between predicate");
    }

    // Make sure toThrift() picks up the children of the rewritten predicate.
    children_ = rewrittenPredicate_.getChildren();
    isAnalyzed_ = true;
  }

  @Override
  public List<Expr> getConjuncts() {
    return rewrittenPredicate_.getConjuncts();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    rewrittenPredicate_.toThrift(msg);
  }

  @Override
  public String toSqlImpl() {
    String notStr = (isNotBetween_) ? "NOT " : "";
    return originalChildren_.get(0).toSql() + " " + notStr + "BETWEEN " +
        originalChildren_.get(1).toSql() + " AND " + originalChildren_.get(2).toSql();
  }

  /**
   * Also substitute the exprs in originalChildren when cloning.
   */
  @Override
  protected Expr substituteImpl(ExprSubstitutionMap smap, Analyzer analyzer)
      throws AnalysisException {
    BetweenPredicate clone = (BetweenPredicate) super.substituteImpl(smap, analyzer);
    Preconditions.checkNotNull(clone);
    clone.originalChildren_ =
        Expr.substituteList(originalChildren_, smap, analyzer, false);
    return clone;
  }

  @Override
  public Expr clone() { return new BetweenPredicate(this); }

  @Override
  public Expr reset() {
    super.reset();
    originalChildren_ = Expr.resetList(originalChildren_);
    return this;
  }
}
