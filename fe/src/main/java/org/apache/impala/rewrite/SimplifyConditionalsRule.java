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

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.CaseExpr;
import org.apache.impala.analysis.CaseWhenClause;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/***
 * This rule simplifies conditional functions with constant conditions. It relies on
 * FoldConstantsRule to replace the constant conditions with a BoolLiteral or NullLiteral
 * first, and on NormalizeExprsRule to normalize CompoundPredicates.
 *
 * Examples:
 * if (true, 0, 1) -> 0
 * id = 0 OR false -> id = 0
 * false AND id = 1 -> false
 * case when false then 0 when true then 1 end -> 1
 * coalesce(1, 0) -> 1
 *
 * Unary functions like isfalse, isnotfalse, istrue, isnottrue, nullvalue,
 * and nonnullvalue don't need special handling as the fold constants rule
 * will handle them.  nullif and nvl2 are converted to an if in FunctionCallExpr,
 * and therefore don't need handling here.
 */
public class SimplifyConditionalsRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new SimplifyConditionalsRule();

  private static List<String> IFNULL_ALIASES = ImmutableList.of(
      "ifnull", "isnull", "nvl");

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
    if (!expr.isAnalyzed()) return expr;

    Expr simplified;
    if (expr instanceof FunctionCallExpr) {
      simplified = simplifyFunctionCallExpr((FunctionCallExpr) expr);
    } else if (expr instanceof CompoundPredicate) {
      simplified = simplifyCompoundPredicate((CompoundPredicate) expr);
    } else if (expr instanceof CaseExpr) {
      simplified = simplifyCaseExpr((CaseExpr) expr, analyzer);
    } else {
      return expr;
    }

    // IMPALA-5125: We can't eliminate aggregates as this may change the meaning of the
    // query, for example:
    // 'select if (true, 0, sum(id)) from alltypes' != 'select 0 from alltypes'
    if (expr != simplified) {
      simplified.analyze(analyzer);
      if (expr.contains(Expr.IS_AGGREGATE)
          && !simplified.contains(Expr.IS_AGGREGATE)) {
        return expr;
      }
    }
    return simplified;
  }

  /**
   * Simplifies IF by returning the corresponding child if the condition has a constant
   * TRUE, FALSE, or NULL (equivalent to FALSE) value.
   */
  private Expr simplifyIfFunctionCallExpr(FunctionCallExpr expr) {
    Preconditions.checkState(expr.getChildren().size() == 3);
    Expr head = expr.getChild(0);
    if (Expr.IS_TRUE_LITERAL.apply(head)) {
      // IF(TRUE)
      return expr.getChild(1);
    } else if (Expr.IS_FALSE_LITERAL.apply(head)) {
      // IF(FALSE)
      return expr.getChild(2);
    } else if (Expr.IS_NULL_LITERAL.apply(head)) {
      // IF(NULL)
      return expr.getChild(2);
    }
    return expr;
  }

  /**
   * Simplifies IFNULL if the condition is a literal, using the
   * following transformations:
   *   IFNULL(NULL, x) -> x
   *   IFNULL(a, x) -> a, if a is a non-null literal
   */
  private Expr simplifyIfNullFunctionCallExpr(FunctionCallExpr expr) {
    Preconditions.checkState(expr.getChildren().size() == 2);
    Expr child0 = expr.getChild(0);
    if (Expr.IS_NULL_LITERAL.apply(child0)) return expr.getChild(1);
    if (Expr.IS_LITERAL.apply(child0)) return child0;
    return expr;
  }

  /**
   * Simplify COALESCE by skipping leading nulls and applying the following transformations:
   * COALESCE(null, a, b) -> COALESCE(a, b);
   * COALESCE(<literal>, a, b) -> <literal>, when literal is not NullLiteral;
   */
  private Expr simplifyCoalesceFunctionCallExpr(FunctionCallExpr expr) {
    int numChildren = expr.getChildren().size();
    Expr result = NullLiteral.create(expr.getType());
    for (int i = 0; i < numChildren; ++i) {
      Expr childExpr = expr.getChildren().get(i);
      // Skip leading nulls.
      if (Expr.IS_NULL_VALUE.apply(childExpr)) continue;
      if ((i == numChildren - 1) || Expr.IS_LITERAL.apply(childExpr)) {
        result = childExpr;
      } else if (i == 0) {
        result = expr;
      } else {
        List<Expr> newChildren = Lists.newArrayList(expr.getChildren().subList(i, numChildren));
        result = new FunctionCallExpr(expr.getFnName(), newChildren);
      }
      break;
    }
    return result;
  }

  private Expr simplifyFunctionCallExpr(FunctionCallExpr expr) {
    String fnName = expr.getFnName().getFunction();

    if (fnName.equals("if")) {
      return simplifyIfFunctionCallExpr(expr);
    } else if (fnName.equals("coalesce")) {
      return simplifyCoalesceFunctionCallExpr(expr);
    } else if (IFNULL_ALIASES.contains(fnName)) {
      return simplifyIfNullFunctionCallExpr(expr);
    }
    return expr;
  }

  /**
   * Simplifies compound predicates with at least one BoolLiteral child, which
   * NormalizeExprsRule ensures will be the left child,  according to the following rules:
   * true AND 'expr' -> 'expr'
   * false AND 'expr' -> false
   * true OR 'expr' -> true
   * false OR 'expr' -> 'expr'
   *
   * Unlike other rules here such as IF, we cannot in general simplify CompoundPredicates
   * with a NullLiteral child (unless the other child is a BoolLiteral), eg. null and
   * 'expr' is false if 'expr' is false but null if 'expr' is true.
   *
   * NOT is covered by FoldConstantRule.
   */
  private Expr simplifyCompoundPredicate(CompoundPredicate expr) {
    Expr leftChild = expr.getChild(0);
    if (!(leftChild instanceof BoolLiteral)) return expr;

    if (expr.getOp() == CompoundPredicate.Operator.AND) {
      if (Expr.IS_TRUE_LITERAL.apply(leftChild)) {
        // TRUE AND 'expr', so return 'expr'.
        return expr.getChild(1);
      } else {
        // FALSE AND 'expr', so return FALSE.
        return leftChild;
      }
    } else if (expr.getOp() == CompoundPredicate.Operator.OR) {
      if (Expr.IS_TRUE_LITERAL.apply(leftChild)) {
        // TRUE OR 'expr', so return TRUE.
        return leftChild;
      } else {
        // FALSE OR 'expr', so return 'expr'.
        return expr.getChild(1);
      }
    }
    return expr;
  }

  /**
   * Simplifies CASE and DECODE. If any of the 'when's have constant FALSE/NULL values,
   * they are removed. If all of the 'when's are removed, just the ELSE is returned. If
   * any of the 'when's have constant TRUE values, the leftmost one becomes the ELSE
   * clause and all following cases are removed.
   *
   * Note that FunctionalCallExpr.createExpr() converts "nvl2" into "if",
   * "decode" into "case", and "nullif" into "if".
   */
  private Expr simplifyCaseExpr(CaseExpr expr, Analyzer analyzer)
      throws AnalysisException {
    Expr caseExpr = expr.hasCaseExpr() ? expr.getChild(0) : null;
    if (expr.hasCaseExpr() &&
        (!Expr.IS_LITERAL.apply(caseExpr) || Expr.IS_NULL_LITERAL.apply(caseExpr))) {
      return expr;
    }

    int numChildren = expr.getChildren().size();
    int loopStart = expr.hasCaseExpr() ? 1 : 0;
    // Check and return early if there's nothing that can be simplified.
    boolean canSimplify = false;
    for (int i = loopStart; i < numChildren - 1; i += 2) {
      if (Expr.IS_LITERAL.apply(expr.getChild(i))) {
        canSimplify = true;
        break;
      }
    }
    if (!canSimplify) return expr;

    // Contains all 'when' clauses with non-constant conditions, used to construct the new
    // CASE expr while removing any FALSE or NULL cases.
    List<CaseWhenClause> newWhenClauses = new ArrayList<CaseWhenClause>();
    // Set to THEN of first constant TRUE clause, if any.
    Expr elseExpr = null;
    for (int i = loopStart; i < numChildren - 1; i += 2) {
      Expr child = expr.getChild(i);
      if (Expr.IS_NULL_LITERAL.apply(child)) continue;

      Expr whenExpr;
      if (expr.hasCaseExpr()) {
        if (Expr.IS_LITERAL.apply(child)) {
          BinaryPredicate pred = new BinaryPredicate(
              BinaryPredicate.Operator.EQ, caseExpr, expr.getChild(i));
          pred.analyze(analyzer);
          whenExpr = analyzer.getConstantFolder().rewrite(pred, analyzer);
        } else {
          whenExpr = null;
        }
      } else {
        whenExpr = child;
      }

      if (whenExpr instanceof BoolLiteral) {
        if (((BoolLiteral) whenExpr).getValue()) {
          if (newWhenClauses.size() == 0) {
            // This WHEN is always TRUE, and any cases preceding it are constant
            // FALSE/NULL, so just return its THEN.
            return expr.getChild(i + 1);
          } else {
            // This WHEN is always TRUE, so the cases after it can never be reached.
            elseExpr = expr.getChild(i + 1);
            break;
          }
        } else {
          // This WHEN is always FALSE, so it can be removed.
        }
      } else {
        newWhenClauses.add(new CaseWhenClause(child, expr.getChild(i + 1)));
      }
    }

    if (expr.hasElseExpr() && elseExpr == null) elseExpr = expr.getChild(numChildren - 1);
    if (newWhenClauses.size() == 0) {
      // All of the WHEN clauses were FALSE, return the ELSE.
      if (elseExpr == null) return NullLiteral.create(expr.getType());
      return elseExpr;
    }
    return new CaseExpr(caseExpr, newWhenClauses, elseExpr);
  }
}
