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

import com.google.common.collect.Lists;

import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.InPredicate;
import org.apache.impala.analysis.Subquery;

/**
 * Coalesces disjunctive equality predicates to an IN predicate, and merges compatible
 * equality or IN predicates into an existing IN predicate.
 * Examples:
 * (C=1) OR (C=2) OR (C=3) OR (C=4) -> C IN(1, 2, 3, 4)
 * (X+Y = 5) OR (X+Y = 6) -> X+Y IN (5, 6)
 * (A = 1) OR (A IN (2, 3)) -> A IN (1, 2, 3)
 * (B IN (1, 2)) OR (B IN (3, 4)) -> B IN (1, 2, 3, 4)
 */
public class EqualityDisjunctsToInRule implements ExprRewriteRule {

  public static ExprRewriteRule INSTANCE = new EqualityDisjunctsToInRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) {
    if (!Expr.IS_OR_PREDICATE.apply(expr)) return expr;

    Expr inAndOtherExpr = rewriteInAndOtherExpr(expr.getChild(0), expr.getChild(1));
    if (inAndOtherExpr != null) return inAndOtherExpr;

    Expr orChildExpr = rewriteEqEqPredicate(expr.getChild(0), expr.getChild(1));
    if (orChildExpr != null) return orChildExpr;

    return expr;
  }

  /**
   * Takes the children of an OR predicate and attempts to combine them into a single IN
   * predicate. The transformation is applied if one of the children is an IN predicate
   * and the other child is a compatible IN predicate or equality predicate. Returns the
   * transformed expr or null if no transformation was possible.
   */
  private Expr rewriteInAndOtherExpr(Expr child0, Expr child1) {
    InPredicate inPred = null;
    Expr otherPred = null;
    if (child0 instanceof InPredicate) {
      inPred = (InPredicate) child0;
      otherPred = child1;
    } else if (child1 instanceof InPredicate) {
      inPred = (InPredicate) child1;
      otherPred = child0;
    }
    if (inPred == null || inPred.isNotIn() || inPred.contains(Subquery.class) ||
        !inPred.getChild(0).equals(otherPred.getChild(0))) {
      return null;
    }

    // other predicate can be OR predicate or IN predicate
    List<Expr> newInList = Lists.newArrayList(
        inPred.getChildren().subList(1, inPred.getChildren().size()));
    if (Expr.IS_EXPR_EQ_LITERAL_PREDICATE.apply(otherPred)) {
      if (newInList.size() + 1 == Expr.EXPR_CHILDREN_LIMIT) return null;
      newInList.add(otherPred.getChild(1));
    } else if (otherPred instanceof InPredicate && !((InPredicate) otherPred).isNotIn()
        && !otherPred.contains(Subquery.class)) {
      if (newInList.size() + otherPred.getChildren().size() > Expr.EXPR_CHILDREN_LIMIT) {
        return null;
      }
      newInList.addAll(
          otherPred.getChildren().subList(1, otherPred.getChildren().size()));
    } else {
      return null;
    }

    return new InPredicate(inPred.getChild(0), newInList, false);
  }

  /**
   * Takes the children of an OR predicate and attempts to combine them into a single IN predicate.
   * The transformation is applied if both children are equality predicates with a literal on the
   * right hand side.
   * Returns the transformed expr or null if no transformation was possible.
   */
  private Expr rewriteEqEqPredicate(Expr child0, Expr child1) {
    if (!Expr.IS_EXPR_EQ_LITERAL_PREDICATE.apply(child0)) return null;
    if (!Expr.IS_EXPR_EQ_LITERAL_PREDICATE.apply(child1)) return null;

    if (!child0.getChild(0).equals(child1.getChild(0))) return null;
    Expr newExpr = new InPredicate(child0.getChild(0),
        Lists.newArrayList(child0.getChild(1), child1.getChild(1)), false);
    return newExpr;
  }

}
