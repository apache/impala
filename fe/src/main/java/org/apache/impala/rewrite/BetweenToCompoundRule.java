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

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BetweenPredicate;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.Predicate;

/**
 * Rewrites BetweenPredicates into an equivalent conjunctive/disjunctive
 * CompoundPredicate.
 * It can be applied to pre-analysis expr trees and therefore does not reanalyze
 * the transformation output itself.
 * Examples:
 * A BETWEEN X AND Y ==> A >= X AND A <= Y
 * A NOT BETWEEN X AND Y ==> A < X OR A > Y
 */
public class BetweenToCompoundRule implements ExprRewriteRule {
  public static final ExprRewriteRule INSTANCE = new BetweenToCompoundRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) {
    if (!(expr instanceof BetweenPredicate)) return expr;
    BetweenPredicate bp = (BetweenPredicate) expr;
    Expr value = bp.getChild(0);
    // Using a cloned value for the 'upper' binary predicate to avoid being affected by
    // changes in the 'lower' binary predicate.
    Expr clonedValue = value.clone();
    Expr lowerBound = bp.getChild(1);
    Expr upperBound = bp.getChild(2);

    BinaryPredicate.Operator lowerOperator;
    BinaryPredicate.Operator upperOperator;
    CompoundPredicate.Operator compoundOperator;

    if (bp.isNotBetween()) {
      // Rewrite into disjunction.
      lowerOperator = BinaryPredicate.Operator.LT;
      upperOperator = BinaryPredicate.Operator.GT;
      compoundOperator = CompoundPredicate.Operator.OR;
    } else {
      // Rewrite into conjunction.
      lowerOperator = BinaryPredicate.Operator.GE;
      upperOperator = BinaryPredicate.Operator.LE;
      compoundOperator = CompoundPredicate.Operator.AND;
    }
    Predicate lower = new BinaryPredicate(lowerOperator, value, lowerBound);
    Predicate upper = new BinaryPredicate(upperOperator, clonedValue, upperBound);
    return new CompoundPredicate(compoundOperator, lower, upper);
  }

  private BetweenToCompoundRule() {}
}
