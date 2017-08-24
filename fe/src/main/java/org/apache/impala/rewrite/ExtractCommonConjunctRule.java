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

package org.apache.impala.rewrite;

import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * This rule extracts common conjuncts from multiple disjunctions when it is applied
 * recursively bottom-up to a tree of CompoundPredicates.
 * It can be applied to pre-analysis expr trees and therefore does not reanalyze
 * the transformation output itself.
 *
 * Examples:
 * (a AND b AND c) OR (b AND d) ==> b AND ((a AND c) OR (d))
 * (a AND b) OR (a AND b) ==> a AND b
 * (a AND b AND c) OR (c) ==> c
 */
public class ExtractCommonConjunctRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new ExtractCommonConjunctRule();

  // Arbitrary limit the number of Expr.equals() comparisons in the O(N^2) loop below.
  // Used to avoid pathologically expensive invocations of this rule.
  // TODO: Implement Expr.hashCode() and move to a hash-based solution for the core
  // Expr.equals() comparison loop below.
  private static final int MAX_EQUALS_COMPARISONS = 30 * 30;

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) {
    if (!Expr.IS_OR_PREDICATE.apply(expr)) return expr;

    // Get childrens' conjuncts and check
    List<Expr> child0Conjuncts = expr.getChild(0).getConjuncts();
    List<Expr> child1Conjuncts = expr.getChild(1).getConjuncts();
    Preconditions.checkState(!child0Conjuncts.isEmpty() && !child1Conjuncts.isEmpty());
    // Impose cost bound.
    if (child0Conjuncts.size() * child1Conjuncts.size() > MAX_EQUALS_COMPARISONS) {
      return expr;
    }

    // Find common conjuncts.
    List<Expr> commonConjuncts = Lists.newArrayList();
    for (Expr conjunct: child0Conjuncts) {
      if (child1Conjuncts.contains(conjunct)) {
        // The conjunct may have parenthesis but there's no need to preserve them.
        // Removing them makes the toSql() easier to read.
        conjunct.setPrintSqlInParens(false);
        commonConjuncts.add(conjunct);
      }
    }
    if (commonConjuncts.isEmpty()) return expr;

    // Remove common conjuncts.
    child0Conjuncts.removeAll(commonConjuncts);
    child1Conjuncts.removeAll(commonConjuncts);

    // Check special case where one child contains all conjuncts of the other.
    // (a AND b) OR (a AND b) ==> a AND b
    // (a AND b AND c) OR (c) ==> c
    if (child0Conjuncts.isEmpty() || child1Conjuncts.isEmpty()) {
      Preconditions.checkState(!commonConjuncts.isEmpty());
      Expr result = CompoundPredicate.createConjunctivePredicate(commonConjuncts);
      return result;
    }

    // Re-assemble disjunctive predicate.
    Expr child0Disjunct = CompoundPredicate.createConjunctivePredicate(child0Conjuncts);
    child0Disjunct.setPrintSqlInParens(expr.getChild(0).getPrintSqlInParens());
    Expr child1Disjunct = CompoundPredicate.createConjunctivePredicate(child1Conjuncts);
    child1Disjunct.setPrintSqlInParens(expr.getChild(1).getPrintSqlInParens());
    List<Expr> newDisjuncts = Lists.newArrayList(child0Disjunct, child1Disjunct);
    Expr newDisjunction = CompoundPredicate.createDisjunctivePredicate(newDisjuncts);
    newDisjunction.setPrintSqlInParens(true);
    Expr result = CompoundPredicate.createConjunction(newDisjunction,
        CompoundPredicate.createConjunctivePredicate(commonConjuncts));
    return result;
  }

  private ExtractCommonConjunctRule() {}
}
