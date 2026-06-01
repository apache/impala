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

package org.apache.impala.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.InPredicate;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.ToSqlOptions;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.catalog.FeTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for canonicalizing expressions for History-Based Optimization (HBO).
 *
 * This class handles expression normalization at different strategy levels:
 * - EXPR_REWRITE: Sorts conjuncts and IN values for deterministic hashing
 * - IGNORE_PARTITION_CONSTANTS: Additionally removes constants from partition column
 *   predicates
 *
 * Note: Most expression normalization (e.g., `1 = a` -> `a = 1`,
 * `(a=1 OR a=2)` -> `a IN (1,2)`) is already performed by ExprRewriter during analysis,
 * so we don't need to redo it here.
 */
public class ExprCanonicalizer {
  private final static Logger LOG = LoggerFactory.getLogger(ExprCanonicalizer.class);

  // Placeholder string used when removing constants from predicates
  private static final String CONST = "<CONST>";

  public static String canonicalizeConstExpr(Expr e) {
    if (e instanceof LiteralExpr) {
      // Use "INT" for all integer types to avoid implicit cast by the analyzer
      String type = e.getType().isIntegerType() ? "INT" : e.getType().toString();
      return type + ":" + e.toSql();
    }
    // TODO: consider evaluating the constant exprs, e.g. "1+1", if the performance
    // impact is negligible.
    return e.toSql(ToSqlOptions.FOR_HBO);
  }

  /**
   * Canonicalizes a list of expressions according to the specified strategy.
   *
   * @param exprs List of expressions to canonicalize
   * @param table The table being scanned (used to identify partition columns)
   * @param strategy The canonicalization strategy to apply
   * @return A new list of canonicalized expression strings, sorted deterministically
   */
  public static List<String> canonicalizeScanConjuncts(List<Expr> exprs, FeTable table,
      CanonicalizationStrategy strategy) {
    if (exprs == null || exprs.isEmpty()) return Collections.emptyList();
    List<String> result = new ArrayList<>();
    for (Expr expr : exprs) {
      String canonicalizedStr = canonicalizeExpr(expr, table, strategy);
      result.add(canonicalizedStr);
    }
    // Sort for deterministic ordering
    Collections.sort(result);
    return result;
  }

  /**
   * Canonicalizes a list of expressions for HBO keys on non-scan plan nodes, qualifying
   * each column with its canonical operand index (see {@link #qualifyForHbo}) when
   * 'operandIdx' is provided. Used for the single-operand WHERE conjuncts of a join
   * node so that columns of different operands are distinguishable.
   */
  public static List<String> canonicalizeExprs(List<Expr> exprs,
      CanonicalizationStrategy strategy, Map<TupleId, String> operandIdx) {
    return canonicalizeScanConjuncts(qualifyForHbo(exprs, operandIdx), null, strategy);
  }

  /**
   * Canonicalizes join predicates for HBO keys, qualifying each column with its
   * canonical operand index (see {@link #qualifyForHbo}) when 'operandIdx' is provided.
   * Qualification disambiguates predicates that would otherwise collapse to the same
   * string, e.g. "a.id = b.int_col" vs "b.id = a.int_col" both rendering as
   * "id = int_col".
   */
  public static List<String> canonicalizeJoinConjuncts(List<Expr> exprs,
      CanonicalizationStrategy strategy, Map<TupleId, String> operandIdx) {
    if (exprs == null || exprs.isEmpty()) return Collections.emptyList();
    List<String> result = new ArrayList<>();
    for (Expr expr : qualifyForHbo(exprs, operandIdx)) {
      result.add(canonicalizeJoinConjunct(expr, strategy));
    }
    Collections.sort(result);
    return result;
  }

  /**
   * Returns a list of deep clones of 'exprs' whose SlotRefs are tagged with their
   * canonical operand qualifier. The originals are never mutated.
   */
  private static List<Expr> qualifyForHbo(List<Expr> exprs,
      Map<TupleId, String> operandIdx) {
    if (operandIdx == null || operandIdx.isEmpty()) return exprs;
    List<Expr> result = new ArrayList<>(exprs.size());
    for (Expr expr : exprs) {
      Expr newExpr = qualifyForHbo(expr.clone(), operandIdx);
      result.add(newExpr);
    }
    return result;
  }

  /**
   * Returns a deep clone of 'expr' whose SlotRefs are tagged with a canonical operand
   * qualifier ("op<path>") so that FOR_HBO rendering records which operand each column
   * belongs to. Returns the original Expr if nothing needs qualification.
   */
  private static Expr qualifyForHbo(Expr node, Map<TupleId, String> operandIdx) {
    if (node instanceof SlotRef slot) {
      String path = resolveOperandIndex(slot, operandIdx);
      // For a SlotRef that resolves to a single operand, sets its qualifier.
      if (path != null) {
        slot.setHboQualifier("op" + path);
        return slot;
      }
      // If the slot doesn't resolve to a single operand, recurse into its source expr.
      // E.g. for a materialized slot whose single source is a compound expr, descend into
      // a clone of that source and qualify its nested SlotRefs individually.
      if (slot.hasDesc()) {
        SlotDescriptor desc = slot.getDesc();
        List<Expr> srcExprs = desc.getSourceExprs();
        if (!desc.isScanSlot() && srcExprs.size() == 1) {
          return qualifyForHbo(srcExprs.get(0).clone(), operandIdx);
        }
      }
      // Don't need qualifier.
      return slot;
    }
    // Recursively tags 'node' (a mutable clone) so FOR_HBO rendering records each
    // column's operand.
    for (int i = 0; i < node.getChildCount(); ++i) {
      node.setChild(i, qualifyForHbo(node.getChild(i), operandIdx));
    }
    return node;
  }

  /**
   * Returns the canonical operand path of 'slot', i.e. the path mapped to the first of
   * its enclosing tuples present in 'operandIdx'. For a substituted slot whose enclosing
   * tuple is not an operand tuple (e.g. a merge aggregation's output slot), the search
   * follows the slot's single source expr down to the base operand tuple.
   * Returns null if none is found, which means the slot doesn't need to be qualified.
   */
  private static String resolveOperandIndex(SlotRef slot,
      Map<TupleId, String> operandIdx) {
    SlotDescriptor desc = slot.getDesc();
    if (desc == null) return null;
    for (TupleDescriptor tupleDesc : desc.getEnclosingTupleDescs()) {
      String path = operandIdx.get(tupleDesc.getId());
      if (path != null) return path;
    }
    // Substituted slot: trace through the single source column reference to its operand.
    List<Expr> sourceExprs = desc.getSourceExprs();
    // Slots with more than one source expr, or a non-SlotRef source, are left
    // unqualified. In particular a UNION output slot has one source expr per branch and
    // cannot be attributed to a single operand.
    if (sourceExprs.size() != 1 || !(sourceExprs.get(0) instanceof SlotRef)) return null;
    return resolveOperandIndex(((SlotRef) sourceExprs.get(0)), operandIdx);
  }

  private static String canonicalizeJoinConjunct(Expr expr,
      CanonicalizationStrategy strategy) {
    // For symmetric equality predicates (EQ, NOT_DISTINCT), the two operands are sorted
    // so that join key order is independent of left/right child ordering.
    if (expr instanceof BinaryPredicate) {
      BinaryPredicate binPred = (BinaryPredicate) expr;
      if (binPred.getOp() == BinaryPredicate.Operator.EQ
          || binPred.getOp() == BinaryPredicate.Operator.NOT_DISTINCT) {
        String lhs = binPred.getChild(0).toSql(ToSqlOptions.FOR_HBO);
        String rhs = binPred.getChild(1).toSql(ToSqlOptions.FOR_HBO);
        if (lhs.compareTo(rhs) > 0) {
          String tmp = lhs;
          lhs = rhs;
          rhs = tmp;
        }
        return lhs + binPred.getOp().toString() + rhs;
      }
    }
    return canonicalizeExpr(expr, null, strategy);
  }

  /**
   * Resolves the table an expression's columns belong to by inspecting its SlotRefs.
   * Returns null if the expression references columns from multiple or no tables.
   */
  private static FeTable resolveSingleTable(Expr expr) {
    List<SlotRef> slotRefs = new ArrayList<>();
    expr.collect(SlotRef.class, slotRefs);
    FeTable table = null;
    for (SlotRef ref : slotRefs) {
      SlotDescriptor desc = ref.getDesc();
      if (desc == null || desc.getParent() == null) continue;
      FeTable t = desc.getParent().getTable();
      if (table == null) {
        table = t;
      } else if (table != t) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Expr {} references columns from different tables: {} and {}",
              expr.toSql(), table.getFullName(), t.getFullName());
        }
        return null;
      }
    }
    return table;
  }

  /**
   * Canonicalizes a single expression according to the strategy.
   */
  private static String canonicalizeExpr(Expr expr, FeTable table,
      CanonicalizationStrategy strategy) {
    // Non-scan callers (e.g. a join node's retained WHERE conjuncts) pass no table.
    // Resolve it from the expression's own slots.
    if (table == null) table = resolveSingleTable(expr);
    // Remove constants only from partition column equality predicates when the strategy
    // is not EXPR_REWRITE (i.e. more aggressive than EXPR_REWRITE).
    boolean shouldRemoveConstants = false;
    if (table != null && strategy != CanonicalizationStrategy.EXPR_REWRITE
        && Expr.IS_EQUALITY_PREDICATE.apply(expr)
        && table.referencesPartitionColumn(expr)) {
      shouldRemoveConstants = true;
    }
    if (expr instanceof InPredicate) {
      return canonicalizeInPredicate((InPredicate) expr, shouldRemoveConstants);
    }
    if (shouldRemoveConstants && expr instanceof BinaryPredicate) {
      return canonicalizeBinaryPredicate((BinaryPredicate) expr);
    }
    return expr.toSql(ToSqlOptions.FOR_HBO);
  }

  /**
   * Converts IN predicate to SQL with sorted IN values.
   * When shouldRemoveConstants is true, replaces constants with placeholders.
   * TODO: Consider moving this to InPredicate.java
   */
  private static String canonicalizeInPredicate(InPredicate inPred,
      boolean shouldRemoveConstants) {
    List<String> values = new ArrayList<>();
    for (int i = 1; i < inPred.getChildren().size(); i++) {
      Expr child = inPred.getChild(i);
      if (shouldRemoveConstants && child.isConstant()) {
        values.add(CONST);
      } else {
        values.add(child.toSql(ToSqlOptions.FOR_HBO));
      }
    }
    Collections.sort(values);

    StringBuilder sb = new StringBuilder();
    sb.append(inPred.getChild(0).toSql(ToSqlOptions.FOR_HBO));
    sb.append(inPred.isNotIn() ? " NOT IN (" : " IN (");
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append(values.get(i));
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * Converts binary predicate to SQL with constant replaced with placeholder.
   * So far we only deal with equality predicates.
   * TODO: Consider moving this to BinaryPredicate.java
   */
  private static String canonicalizeBinaryPredicate(BinaryPredicate binPred) {
    Expr lhs = binPred.getChild(0);
    Expr rhs = binPred.getChild(1);
    // NormalizeBinaryPredicatesRule guarantees that constant is on the right side.
    // We just deal with predicates like "part_col = const" here.
    // TODO: Support more complex predicates like substr(p, 1, 4) = '2025'.
    if ((binPred.getOp() == BinaryPredicate.Operator.EQ ||
         binPred.getOp() == BinaryPredicate.Operator.NOT_DISTINCT)
        && lhs instanceof SlotRef && rhs.isConstant()) {
      return lhs.toSql(ToSqlOptions.FOR_HBO) +
          binPred.getOp().toString() + CONST;
    }
    return binPred.toSql(ToSqlOptions.FOR_HBO);
  }
}
