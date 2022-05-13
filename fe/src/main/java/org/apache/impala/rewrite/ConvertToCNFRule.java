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
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.InlineViewRef;
import org.apache.impala.analysis.Predicate;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.common.AnalysisException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This rule converts a predicate to conjunctive normal form (CNF).
 * Converting to CNF enables multi-table predicates that were only
 * evaluated by a Join operator to be converted into either single-table
 * conjuncts that are eligible for predicate pushdown to the scan
 * operator or other multi-table conjuncts that are eligible to be pushed
 * to a Join below.
 *
 * By default, we do this conversion if the expressions in the predicate
 * reference 2 or more tables but depending on the forMultiTablesOnly flag,
 * it can work for single table predicates also.
 *
 * Since converting to CNF expands the number of exprs, we place a limit on the
 * maximum number of CNF exprs (each AND is counted as 1 CNF expr) that are considered;
 * once this limit is exceeded, whatever expression was supplied is returned without
 * further transformation. Note that in some systems (e.g Hive) the original OR
 * predicate is returned when such a limit is reached. However, Impala's rewrite
 * rule is applied bottom-up in ExprRewriter, so we may not have access to the
 * top level OR predicate when this limit is reached.
 *
 * The rule does one expansion at a time, so for complex predicates it relies on
 * the ExprRewriter's iterative invocations for expanding in successive steps as shown
 * in example 2 below.
 *
 * Currently, this rule handles the following common pattern:
 *  1.
 *  original: (a AND b) OR c
 *  rewritten: (a OR c) AND (b OR c)
 *  2.
 *  if 'c' is another compound predicate, a subsequent application of this
 *  rule would again convert to CNF:
 *  original: (a AND b) OR (c AND d)
 *  first rewrite: (a OR (c AND d)) AND (b OR (c AND d))
 *  subsequent rewrite: (a OR c) AND (a OR d) AND (b OR c) AND (b OR d)
 *  3.
 *  original: NOT(a OR b)
 *  rewritten: NOT(a) AND NOT(b)  (by De Morgan's theorem)
 *
 *  Following predicates are already in CNF, so no conversion is done:
 *   a OR b where 'a' and 'b' are not CompoundPredicates
 *   a AND b
 */
public class ConvertToCNFRule implements ExprRewriteRule {
  private final static Logger LOG = LoggerFactory.getLogger(ConvertToCNFRule.class);

  // maximum number of CNF exprs (each AND is counted as 1) allowed
  private final int maxCnfExprs_;
  // current number of CNF exprs
  private int numCnfExprs_ = 0;

  // flag that convert a disjunct to CNF only if the predicate involves
  // 2 or more tables. By default, we do this only for multi table case
  // but for unit testing it is useful to disable this
  private final boolean forMultiTablesOnly_;

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
    return convertToCNF(expr, analyzer);
  }

  private Expr convertToCNF(Expr pred, Analyzer analyzer) throws AnalysisException {
    if (!(pred instanceof CompoundPredicate)) {
      return pred;
    }

    if (!((CompoundPredicate)pred).shouldConvertToCNF()) {
      LOG.debug("It is not feasible to rewrite predicate " + pred.toSql() + " to CNF.");
      return pred;
    }

    if (maxCnfExprs_ > 0 && numCnfExprs_ >= maxCnfExprs_) {
      // max allowed CNF exprs has been reached .. in this case we
      // return the supplied predicate (also see related comments
      // in the class level comments above)
      return pred;
    }

    CompoundPredicate cpred = (CompoundPredicate) pred;
    if (cpred.getOp() == CompoundPredicate.Operator.AND) {
      // this is already a conjunct
      return cpred;
    } else if (cpred.getOp() == CompoundPredicate.Operator.OR
            || cpred.getOp() == CompoundPredicate.Operator.NOT) {
      if (forMultiTablesOnly_) {
        // check if this predicate references one or more tuples. If only 1 tuple,
        // we can skip the rewrite since the disjunct can be pushed down as-is
        List<TupleId> tids = new ArrayList<>();
        if (!cpred.isAnalyzed()) {
          // clone before analyzing to avoid side effects of analysis
          cpred = (CompoundPredicate) (cpred.clone());
          cpred.analyzeNoThrow(analyzer);
        }
        cpred.getIds(tids, null);
        if (tids.size() == 1) {
          TableRef tbl = analyzer.getTableRef(tids.get(0));
          if (tbl instanceof InlineViewRef) {
            Expr basePred = cpred.trySubstitute(((InlineViewRef)tbl).getBaseTblSmap(),
                analyzer, false);
            if (!basePred.equals(cpred)) {
              tids.clear();
              basePred.getIds(tids, null);
            }
          }
        }
        if (tids.size() <= 1) {
          // if no transform is done, return the original predicate,
          // not the one that that may have been analyzed above
          return pred;
        }
      }
      if (cpred.getOp() == CompoundPredicate.Operator.OR) {
        Expr lhs = cpred.getChild(0);
        Expr rhs = cpred.getChild(1);
        if (lhs instanceof CompoundPredicate &&
            ((CompoundPredicate)lhs).getOp() == CompoundPredicate.Operator.AND) {
          // predicate: (a AND b) OR c
          // convert to (a OR c) AND (b OR c)
          return createPredAndIncrementCount(lhs.getChild(0), rhs,
                  lhs.getChild(1), rhs, analyzer);
        } else if (rhs instanceof CompoundPredicate &&
            ((CompoundPredicate)rhs).getOp() == CompoundPredicate.Operator.AND) {
          // predicate: a OR (b AND c)
          // convert to (a OR b) AND (a or c)
          return createPredAndIncrementCount(lhs, rhs.getChild(0),
                  lhs, rhs.getChild(1), analyzer);
        }
      } else if (cpred.getOp() == CompoundPredicate.Operator.NOT) {
        Expr child = cpred.getChild(0);
        if (child instanceof CompoundPredicate &&
            ((CompoundPredicate) child).getOp() == CompoundPredicate.Operator.OR) {
          // predicate: NOT (a OR b)
          // convert to: NOT(a) AND NOT(b)
          Expr lhs = ((CompoundPredicate) child).getChild(0);
          Expr rhs = ((CompoundPredicate) child).getChild(1);
          Expr lhs1 = new CompoundPredicate(CompoundPredicate.Operator.NOT, lhs, null);
          Expr rhs1 = new CompoundPredicate(CompoundPredicate.Operator.NOT, rhs, null);
          Predicate newPredicate =
              (CompoundPredicate) CompoundPredicate.createConjunction(lhs1, rhs1);
          newPredicate.analyze(analyzer);
          numCnfExprs_++;
          return newPredicate;
        }
      }
    }
    return pred;
  }

  /**
   * Compose 2 disjunctive predicates using supplied exprs and combine
   * the disjuncts into a top level conjunct. Increment the CNF exprs count.
   */
  private Predicate createPredAndIncrementCount(Expr first_lhs, Expr second_lhs,
      Expr first_rhs, Expr second_rhs,
      Analyzer analyzer) throws AnalysisException {
    List<Expr> disjuncts = Arrays.asList(first_lhs, second_lhs);
    Expr lhs1 = (CompoundPredicate)
        CompoundPredicate.createDisjunctivePredicate(disjuncts);
    disjuncts = Arrays.asList(first_rhs, second_rhs);
    Expr rhs1 = (CompoundPredicate)
        CompoundPredicate.createDisjunctivePredicate(disjuncts);
    Predicate newPredicate = (CompoundPredicate)
        CompoundPredicate.createConjunction(lhs1, rhs1);
    newPredicate.analyze(analyzer);
    numCnfExprs_++;
    return newPredicate;
  }

  public ConvertToCNFRule(int maxCnfExprs, boolean forMultiTablesOnly) {
    maxCnfExprs_ = maxCnfExprs;
    forMultiTablesOnly_ = forMultiTablesOnly;
  }
}
