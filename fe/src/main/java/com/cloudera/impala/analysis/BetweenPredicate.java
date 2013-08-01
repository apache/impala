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

  private final boolean isNotBetween;

  // After successful analysis, we rewrite this between predicate
  // into a conjunctive/disjunctive compound predicate.
  private CompoundPredicate rewrittenPredicate;

  // Children of the BetweenPredicate, since this.children should hold the children
  // of the rewritten predicate to make sure toThrift() picks up the right ones.
  private List<Expr> originalChildren = Lists.newArrayList();

  // First child is the comparison expr which should be in [lowerBound, upperBound].
  public BetweenPredicate(Expr compareExpr, Expr lowerBound, Expr upperBound,
      boolean isNotBetween) {
    originalChildren.add(compareExpr);
    originalChildren.add(lowerBound);
    originalChildren.add(upperBound);
    this.isNotBetween = isNotBetween;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    analyzer.castAllToCompatibleType(originalChildren);

    // TODO: improve with histograms
    selectivity = Expr.defaultSelectivity;

    // Rewrite between predicate into a conjunctive/disjunctive compound predicate.
    if (isNotBetween) {
      // Rewrite into disjunction.
      Predicate lower = new BinaryPredicate(BinaryPredicate.Operator.LT,
          originalChildren.get(0), originalChildren.get(1));
      Predicate upper = new BinaryPredicate(BinaryPredicate.Operator.GT,
          originalChildren.get(0), originalChildren.get(2));
      rewrittenPredicate =
          new CompoundPredicate(CompoundPredicate.Operator.OR, lower, upper);
    } else {
      // Rewrite into conjunction.
      Predicate lower = new BinaryPredicate(BinaryPredicate.Operator.GE,
          originalChildren.get(0), originalChildren.get(1));
      Predicate upper = new BinaryPredicate(BinaryPredicate.Operator.LE,
          originalChildren.get(0), originalChildren.get(2));
      rewrittenPredicate =
          new CompoundPredicate(CompoundPredicate.Operator.AND, lower, upper);
    }

    try {
      rewrittenPredicate.analyze(analyzer);
    } catch (AnalysisException e) {
      // We should have already guaranteed that analysis will succeed.
      Preconditions.checkState(false, "Analysis failed in rewritten between predicate");
    }

    // Make sure toThrift() picks up the children of the rewritten predicate.
    children = rewrittenPredicate.getChildren();
  }

  @Override
  public List<Expr> getConjuncts() {
    return rewrittenPredicate.getConjuncts();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    rewrittenPredicate.toThrift(msg);
  }

  @Override
  public String toSqlImpl() {
    String notStr = (isNotBetween) ? "NOT " : "";
    return originalChildren.get(0).toSql() + " " + notStr + "BETWEEN " +
        originalChildren.get(1).toSql() + " AND " + originalChildren.get(2).toSql();
  }

  /**
   * Also substitute the exprs in originalChildren when cloning.
   */
  @Override
  public Expr clone(SubstitutionMap sMap) {
    BetweenPredicate clone = (BetweenPredicate) super.clone(sMap);
    clone.originalChildren = Expr.cloneList(originalChildren, sMap);
    return clone;
  }
}
