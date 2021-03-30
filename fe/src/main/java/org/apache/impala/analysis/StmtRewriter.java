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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.UnionStmt.UnionOperand;
import org.apache.impala.common.AnalysisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;
import static org.apache.impala.analysis.ToSqlOptions.REWRITTEN;

/**
 * Class representing a statement rewriter. The base class traverses the stmt tree and
 * the specific rewrite rules are implemented in the subclasses and are called by the
 * hooks in the base class.
 * TODO: Now that we have a nested-loop join supporting all join modes we could
 * allow more rewrites, although it is not clear we would always want to.
 */
public class StmtRewriter {
  private final static Logger LOG = LoggerFactory.getLogger(StmtRewriter.class);

  /**
   * Rewrite the statement of an analysis result in-place. Assumes that BetweenPredicates
   * have already been rewritten.
   */
  public void rewrite(AnalysisResult analysisResult) throws AnalysisException {
    // Analyzed stmt that contains a query statement with subqueries to be rewritten.
    StatementBase stmt = analysisResult.getStmt();
    Preconditions.checkState(stmt.isAnalyzed());
    // Analyzed query statement to be rewritten.
    QueryStmt queryStmt;
    if (stmt instanceof QueryStmt) {
      queryStmt = (QueryStmt) analysisResult.getStmt();
    } else if (stmt instanceof InsertStmt) {
      queryStmt = ((InsertStmt) analysisResult.getStmt()).getQueryStmt();
    } else if (stmt instanceof CreateTableAsSelectStmt) {
      queryStmt = ((CreateTableAsSelectStmt) analysisResult.getStmt()).getQueryStmt();
    } else if (analysisResult.isUpdateStmt()) {
      queryStmt = ((UpdateStmt) analysisResult.getStmt()).getQueryStmt();
    } else if (analysisResult.isDeleteStmt()) {
      queryStmt = ((DeleteStmt) analysisResult.getStmt()).getQueryStmt();
    } else if (analysisResult.isTestCaseStmt()) {
      queryStmt = ((CopyTestCaseStmt) analysisResult.getStmt()).getQueryStmt();
    } else {
      throw new AnalysisException("Unsupported statement: " + stmt.toSql());
    }
    rewriteQueryStatement(queryStmt, queryStmt.getAnalyzer());
  }

  /**
   * Calls the appropriate rewrite method based on the specific type of query stmt. See
   * rewriteSelectStatement() and rewriteUnionStatement() documentation.
   */
  protected void rewriteQueryStatement(QueryStmt stmt, Analyzer analyzer)
      throws AnalysisException {
    Preconditions.checkNotNull(stmt);
    Preconditions.checkState(stmt.isAnalyzed());
    if (stmt instanceof SelectStmt) {
      rewriteSelectStatement((SelectStmt) stmt, analyzer);
    } else if (stmt instanceof UnionStmt) {
      rewriteUnionStatement((UnionStmt) stmt);
    } else {
      throw new AnalysisException(
          "Subqueries not supported for " + stmt.getClass().getSimpleName() +
              " statements");
    }
  }

  protected void rewriteSelectStatement(SelectStmt stmt, Analyzer analyzer)
      throws AnalysisException {
    for (TableRef tblRef : stmt.fromClause_) {
      if (!(tblRef instanceof InlineViewRef)) continue;
      InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
      rewriteQueryStatement(inlineViewRef.getViewStmt(), inlineViewRef.getAnalyzer());
    }
    // Currently only SubqueryRewriter touches the where clause. Recurse into the where
    // clause when the need arises.
    rewriteSelectStmtHook(stmt, analyzer);
    if (LOG.isTraceEnabled()) LOG.trace("Rewritten SQL: " + stmt.toSql(REWRITTEN));
  }

  /**
   * Rewrite all operands in a UNION. The conditions that apply to SelectStmt rewriting
   * also apply here.
   */
  private void rewriteUnionStatement(UnionStmt stmt) throws AnalysisException {
    for (UnionOperand operand : stmt.getOperands()) {
      Preconditions.checkState(operand.getQueryStmt() instanceof SelectStmt);
      rewriteSelectStatement((SelectStmt) operand.getQueryStmt(), operand.getAnalyzer());
    }
  }

  protected void rewriteSelectStmtHook(SelectStmt stmt, Analyzer analyzer)
      throws AnalysisException {}

  static class SubqueryRewriter extends StmtRewriter {
    /**
     * Returns true if the Expr tree rooted at 'expr' has at least one subquery
     * that participates in a disjunction.
     */
    private static boolean hasSubqueryInDisjunction(Expr expr) {
      if (!(expr instanceof CompoundPredicate)) return false;
      if (Expr.IS_OR_PREDICATE.apply(expr)) {
        return expr.contains(Subquery.class);
      }
      for (Expr child : expr.getChildren()) {
        if (hasSubqueryInDisjunction(child)) return true;
      }
      return false;
    }

    /**
     * Replace an ExistsPredicate that contains a subquery with a BoolLiteral if we
     * can determine its result without evaluating it. Return null if the result of the
     * ExistsPredicate can only be determined at run-time.
     */
    private static BoolLiteral replaceExistsPredicate(ExistsPredicate predicate) {
      Subquery subquery = predicate.getSubquery();
      Preconditions.checkNotNull(subquery);
      SelectStmt subqueryStmt = (SelectStmt) subquery.getStatement();
      BoolLiteral boolLiteral = null;
      if (subqueryStmt.getAnalyzer().hasEmptyResultSet()) {
        boolLiteral = new BoolLiteral(predicate.isNotExists());
      } else if (subqueryStmt.hasMultiAggInfo()
          && subqueryStmt.getMultiAggInfo().hasAggregateExprs()
          && !subqueryStmt.hasAnalyticInfo() && subqueryStmt.getHavingPred() == null) {
        boolLiteral = new BoolLiteral(!predicate.isNotExists());
      }
      return boolLiteral;
    }


    /**
     * Rewrites [NOT] IN predicate when the LHS is a constant and RHS is a subquery.
     * If 'inPred' is not rewritten, null is returned. If 'inPred' is rewritten, the
     * resulting expression is not analyzed (caller must analyze). 'outerBlock' is the
     * parent block of 'inPred'.
     *
     * Example: SELECT * FROM t WHERE 1 IN (SELECT id FROM s)
     *
     * The rewrite transforms 'inPred' using the following cases. C refers to the LHS
     * constant and RHS is the subquery. All cases apply to both correlated and
     * uncorrelated subqueries.
     *
     * 1) Predicate is IN: No rewrite since it can be evaluated using the existing
     *                     NestedLoop based Left Semijoin.
     *
     * 2) Predicate is NOT IN and RHS returns a single row.
     *
     *    Example: 10 NOT IN (SELECT 1)
     *    Example: 10 NOT IN (SELECT MAX(b) FROM t)
     *    Example: 10 NOT IN (SELECT x FROM t LIMIT 1)
     *
     *    REWRITE: C NOT IN RHS: => C != (RHS)
     *
     * 3) Predicate is NOT IN and RHS returns multiple rows.
     *
     *    Example: SELECT * FROM t WHERE 1 NOT IN (SELECT id FROM s)
     *
     *    Assume RHS is of the form SELECT expr FROM T WHERE ...
     *
     *    REWRITE:
     *     C NOT IN (RHS)
     *       Rewrites to:
     *     NOT EXISTS (SELECT x FROM (SELECT x FROM RHS) tmp
     *                 WHERE C IS NULL OR tmp.x IS NULL OR tmp.x = C)
     *
     *    Example:
     *     ... 10 NOT IN (SELECT x FROM t WHERE t.y > 3)
     *       Rewrites to:
     *     ... NOT EXISTS (SELECT x (SELECT x FROM t WHERE t.y > 3) tmp
     *                     WHERE 10 IS NULL OR tmp.x IS NULL OR tmp.x = 10)
     *
     *    The rewrite wraps the RHS subquery in an inline view and filters it with a
     *    condition using the LHS constant. The inline view ensures that the filter is
     *    logically evaluated over the RHS result. Alternatively, injecting the filter
     *    into the RHS is generally incorrect so requires push-down analysis to preserve
     *    correctness (consider cases such as limit, aggregation, and analytic functions).
     *    Such special cases are avoided here by using the inline view.
     *    TODO: Correlated NOT IN subqueries require that column resolution be extended to
     *    handle references to an outer block that is more than one nesting level away.
     *
     *    The filter constructed from the LHS constant is subtle, so warrants further
     *    explanation. Consider the cases where the LHS is NULL vs. NOT NULL and the RHS
     *    is empty vs. not-empty. When RHS subquery evaluates to the empty result set, the
     *    NOT EXISTS passes for all LHS values. When the RHS subquery is not-empty, it is
     *    useful to think of C NOT IN (RHS) as the boolean expansion:
     *          C != x_1 & C != x_2 & C != x_3 & ... where each x_i is bound to a result
     *          from the RHS subquery.
     *
     *    So, if C is equal to any x_i, the expression is false. Similarly, if any
     *    x_i is null or if C is null, then the overall expression also is false.
     */
    private static Expr rewriteInConstant(SelectStmt outerBlock, InPredicate inPred) {
      Expr lhs = inPred.getChild(0);
      Preconditions.checkArgument(lhs.isConstant());

      Expr rhs = inPred.getChild(1);
      QueryStmt subquery = inPred.getSubquery().getStatement();
      Preconditions.checkState(subquery instanceof SelectStmt);
      SelectStmt rhsQuery = (SelectStmt) subquery;

      // CASE 1, IN:
      if (!inPred.isNotIn()) return null;

      // CASE 2, NOT IN and RHS returns a single row:
      if (rhsQuery.returnsSingleRow()) {
        return new BinaryPredicate(BinaryPredicate.Operator.NE, lhs, rhs);
      }

      // CASE 3, NOT IN, RHS returns multiple rows.
      Preconditions.checkState(rhsQuery.getResultExprs().size() == 1);
      // Do not rewrite NOT IN when the RHS is correlated.
      if (isCorrelated(rhsQuery)) return null;

      // Wrap RHS in an inline view: (select wrapperColumnAlias from RHS) wrapperTableAlias.
      // Use outerBlock (parent block of subquery) to generate aliases. Doing so guarantees
      // that the wrapper view does not produce the same alias if further rewritten.
      String wrapperTableAlias = outerBlock.getTableAliasGenerator().getNextAlias();
      String wrapperColumnAlias = outerBlock.getColumnAliasGenerator().getNextAlias();
      InlineViewRef wrapperView = new InlineViewRef(wrapperTableAlias, rhsQuery,
          Lists.newArrayList(wrapperColumnAlias));
      SlotRef wrapperResult =
          new SlotRef(Lists.newArrayList(wrapperTableAlias, wrapperColumnAlias));

      // Build: lhs IS NULL OR rhsResultExpr IS NULL OR lhs = rhs
      Expr rewritePredicate = new CompoundPredicate(CompoundPredicate.Operator.OR,
          new IsNullPredicate(lhs, false),
          new CompoundPredicate(CompoundPredicate.Operator.OR,
              new IsNullPredicate(wrapperResult, false),
              new BinaryPredicate(BinaryPredicate.Operator.EQ, wrapperResult, lhs)));

      List<TableRef> fromList = new ArrayList<>();
      fromList.add(wrapperView);
      SelectStmt rewriteQuery = new SelectStmt(
          new SelectList(Lists.newArrayList(new SelectListItem(wrapperResult, null))),
          new FromClause(fromList), rewritePredicate, null, null, null, null);
      Subquery newSubquery = new Subquery(rewriteQuery);
      rhsQuery.reset();

      // Build: NOT EXISTS(newSubquery)
      return new ExistsPredicate(newSubquery, true);
    }

    /**
     * Tests if a subquery is correlated to its outer block.
     */
    private static boolean isCorrelated(SelectStmt subqueryStmt) {
      if (!subqueryStmt.hasWhereClause()) return false;
      return containsCorrelatedPredicate(subqueryStmt.getWhereClause(),
          subqueryStmt.getTableRefIds());
    }

    /**
     * Merge an expr containing a subquery with a SelectStmt 'stmt' by
     * converting the subquery stmt of the former into an inline view and
     * creating a join between the new inline view and the right-most table
     * from 'stmt'. Return true if the rewrite introduced a new visible tuple
     * due to a CROSS JOIN or a LEFT OUTER JOIN.
     * <p>
     * This process works as follows:
     * 1. Create a new inline view with the subquery as the view's stmt. Changes
     * made to the subquery's stmt will affect the inline view.
     * 2. Extract all correlated predicates from the subquery's WHERE
     * clause; the subquery's select list may be extended with new items and a
     * GROUP BY clause may be added.
     * 3. Add the inline view to stmt's tableRefs and create a
     * join (left semi join, anti-join, left outer join for agg functions
     * that return a non-NULL value for an empty input, or cross-join) with
     * stmt's right-most table.
     * 4. Initialize the ON clause of the new join from the original subquery
     * predicate and the new inline view.
     * 5. Apply expr substitutions such that the extracted correlated predicates
     * refer to columns of the new inline view.
     * 6. Add all extracted correlated predicates to the ON clause.
     */
    private static boolean mergeExpr(SelectStmt stmt, Expr expr, Analyzer analyzer)
        throws AnalysisException {
      Preconditions.checkNotNull(expr);
      Preconditions.checkNotNull(analyzer);
      boolean updateSelectList = false;
      SelectStmt subqueryStmt = (SelectStmt) expr.getSubquery().getStatement();
      boolean isScalarSubquery = expr.getSubquery().isScalarSubquery();
      boolean isScalarColumn = expr.getSubquery().returnsScalarColumn();
      boolean isRuntimeScalar = subqueryStmt.isRuntimeScalar();
      // Create a new inline view from the subquery stmt. The inline view will be added
      // to the stmt's table refs later. Explicitly set the inline view's column labels
      // to eliminate any chance that column aliases from the parent query could reference
      // select items from the inline view after the rewrite.
      List<String> colLabels = new ArrayList<>();
      for (int i = 0; i < subqueryStmt.getColLabels().size(); ++i) {
        colLabels.add(subqueryStmt.getColumnAliasGenerator().getNextAlias());
      }
      InlineViewRef inlineView =
          new InlineViewRef(stmt.getTableAliasGenerator().getNextAlias(), subqueryStmt,
              colLabels);

      // Extract all correlated predicates from the subquery.
      List<Expr> onClauseConjuncts = extractCorrelatedPredicates(subqueryStmt);
      if (!onClauseConjuncts.isEmpty()) {
        validateCorrelatedSubqueryStmt(expr);
        // For correlated subqueries, a LIMIT clause has no effect on the results, so we can
        // safely remove it.
        subqueryStmt.limitElement_ = new LimitElement(null, null);
      }
      // If runtime scalar, we need to prevent the propagation of predicates into the
      // inline view by setting a limit on the statement.
      if (isRuntimeScalar) subqueryStmt.setLimit(2);

      // Update the subquery's select list and/or its GROUP BY clause by adding
      // exprs from the extracted correlated predicates.
      boolean updateGroupBy = isScalarSubquery
          || (expr instanceof ExistsPredicate
                 && !subqueryStmt.getSelectList().isDistinct()
                 && subqueryStmt.hasMultiAggInfo());
      List<Expr> lhsExprs = new ArrayList<>();
      List<Expr> rhsExprs = new ArrayList<>();
      for (Expr conjunct : onClauseConjuncts) {
        updateInlineView(inlineView, conjunct, stmt.getTableRefIds(), lhsExprs, rhsExprs,
            updateGroupBy);
      }

      // Analyzing the inline view triggers reanalysis of the subquery's select statement.
      // However the statement is already analyzed and since statement analysis is not
      // idempotent, the analysis needs to be reset.
      inlineView.reset();
      inlineView.analyze(analyzer);
      inlineView.setLeftTblRef(stmt.fromClause_.get(stmt.fromClause_.size() - 1));
      stmt.fromClause_.add(inlineView);
      JoinOperator joinOp = JoinOperator.LEFT_SEMI_JOIN;

      // Create a join conjunct from the expr that contains a subquery.
      Expr joinConjunct =
          createJoinConjunct(expr, inlineView, analyzer, !onClauseConjuncts.isEmpty());
      if (joinConjunct != null) {
        SelectListItem firstItem =
            ((SelectStmt) inlineView.getViewStmt()).getSelectList().getItems().get(0);
        if (!onClauseConjuncts.isEmpty() && firstItem.getExpr() != null &&
            firstItem.getExpr().contains(Expr.NON_NULL_EMPTY_AGG)) {
          // Correlated subqueries with an aggregate function that returns non-null on
          // an empty input are rewritten using a LEFT OUTER JOIN because we
          // need to ensure that there is one agg value for every tuple of 'stmt'
          // (parent select block), even for those tuples of 'stmt' that get rejected
          // by the subquery due to some predicate. The new join conjunct is added to
          // stmt's WHERE clause because it needs to be applied to the result of the
          // LEFT OUTER JOIN (both matched and unmatched tuples).
          //
          // TODO Handle other aggregate functions and UDAs that return a non-NULL value
          // on an empty set.
          // TODO Handle count aggregate functions in an expression in subqueries
          // select list.
          stmt.whereClause_ =
              CompoundPredicate.createConjunction(joinConjunct, stmt.whereClause_);
          joinConjunct = null;
          joinOp = JoinOperator.LEFT_OUTER_JOIN;
          updateSelectList = true;
        }

        if (joinConjunct != null) onClauseConjuncts.add(joinConjunct);
      }

      // Ensure that all the extracted correlated predicates can be added to the ON-clause
      // of the generated join.
      if (!onClauseConjuncts.isEmpty()) {
        validateCorrelatedPredicates(expr, inlineView, onClauseConjuncts);
      }

      // Create the ON clause from the extracted correlated predicates.
      Expr onClausePredicate =
          CompoundPredicate.createConjunctivePredicate(onClauseConjuncts);

      if (onClausePredicate == null) {
        Preconditions.checkState(expr instanceof ExistsPredicate);
        ExistsPredicate existsPred = (ExistsPredicate) expr;
        // TODO This is very expensive if uncorrelated. Remove it when we implement
        // independent subquery evaluation.
        if (existsPred.isNotExists()) {
          inlineView.setJoinOp(JoinOperator.LEFT_ANTI_JOIN);
        } else {
          inlineView.setJoinOp(JoinOperator.LEFT_SEMI_JOIN);
        }
        // Note that the concept of a 'correlated inline view' is similar but not the same
        // as a 'correlated subquery', i.e., a subquery with a correlated predicate.
        if (!inlineView.isCorrelated()) {
          // For uncorrelated subqueries, we limit the number of rows returned by the
          // subquery.
          subqueryStmt.setLimit(1);
          inlineView.setOnClause(new BoolLiteral(true));
        }
        return false;
      }

      // Create an smap from the original select-list exprs of the select list to
      // the corresponding inline-view columns.
      ExprSubstitutionMap smap = new ExprSubstitutionMap();
      Preconditions.checkState(lhsExprs.size() == rhsExprs.size());
      for (int i = 0; i < lhsExprs.size(); ++i) {
        Expr lhsExpr = lhsExprs.get(i);
        Expr rhsExpr = rhsExprs.get(i);
        rhsExpr.analyze(analyzer);
        smap.put(lhsExpr, rhsExpr);
      }
      onClausePredicate = onClausePredicate.substitute(smap, analyzer, false);

      // Check for references to ancestor query blocks (cycles in the dependency
      // graph of query blocks are not supported).
      if (!onClausePredicate.isBoundByTupleIds(stmt.getTableRefIds())) {
        throw new AnalysisException(
            "Unsupported correlated subquery: " + subqueryStmt.toSql());
      }

      // Check if we have a valid ON clause for an equi-join.
      boolean hasEqJoinPred = false;
      for (Expr conjunct : onClausePredicate.getConjuncts()) {
        if (!(conjunct instanceof BinaryPredicate)) continue;
        BinaryPredicate.Operator operator = ((BinaryPredicate) conjunct).getOp();
        if (!operator.isEquivalence()) continue;
        List<TupleId> lhsTupleIds = new ArrayList<>();
        conjunct.getChild(0).getIds(lhsTupleIds, null);
        // Allows for constants to be a join predicate.
        if (lhsTupleIds.isEmpty() && !conjunct.getChild(0).isConstant()) continue;
        List<TupleId> rhsTupleIds = new ArrayList<>();
        conjunct.getChild(1).getIds(rhsTupleIds, null);
        if (rhsTupleIds.isEmpty()) continue;
        // Check if columns from the outer query block (stmt) appear in both sides
        // of the binary predicate.
        if ((lhsTupleIds.contains(inlineView.getDesc().getId()) &&
            lhsTupleIds.size() > 1) ||
            (rhsTupleIds.contains(inlineView.getDesc().getId()) &&
                rhsTupleIds.size() > 1)) {
          continue;
        }
        hasEqJoinPred = true;
        break;
      }

      if (!hasEqJoinPred && !inlineView.isCorrelated()) {
        // TODO: Requires support for non-equi joins.
        // TODO: Remove this when independent subquery evaluation is implemented.
        // TODO: IMPALA-5100 to cover all cases, we do let through runtime scalars with
        // group by clauses to allow for subqueries where we haven't implemented plan time
        // expression evaluation to ensure only a single row is returned. This may expose
        // runtime errors in the presence of multiple runtime scalar subqueries until we
        // implement independent evaluation.
        boolean hasGroupBy = ((SelectStmt) inlineView.getViewStmt()).hasGroupByClause();
        if ((!isScalarSubquery && !isRuntimeScalar)
            || (hasGroupBy && !stmt.selectList_.isDistinct() && !isScalarColumn
                   && !isRuntimeScalar)) {
          throw new AnalysisException(
              "Unsupported predicate with subquery: " + expr.toSql());
        }

        // TODO: Requires support for null-aware anti-join mode in nested-loop joins
        if (isScalarSubquery && expr instanceof InPredicate &&
            ((InPredicate) expr).isNotIn()) {
          throw new AnalysisException(
              "Unsupported NOT IN predicate with subquery: " + expr.toSql());
        }

        // We can rewrite the aggregate subquery using a cross join. All conjuncts
        // that were extracted from the subquery are added to stmt's WHERE clause.
        stmt.whereClause_ =
            CompoundPredicate.createConjunction(onClausePredicate, stmt.whereClause_);
        inlineView.setJoinOp(JoinOperator.CROSS_JOIN);
        // Indicate that the CROSS JOIN may add a new visible tuple to stmt's
        // select list (if the latter contains an unqualified star item '*')
        return true;
      }

      // We have a valid equi-join conjunct or the inline view is correlated.
      if (expr instanceof InPredicate && ((InPredicate) expr).isNotIn() ||
          expr instanceof ExistsPredicate && ((ExistsPredicate) expr).isNotExists()) {
        // For the case of a NOT IN with an eq join conjunct, replace the join
        // conjunct with a conjunct that uses the null-matching eq operator.
        if (expr instanceof InPredicate) {
          joinOp = JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
          List<TupleId> tIds = new ArrayList<>();
          joinConjunct.getIds(tIds, null);

          if (tIds.size() <= 1 || !tIds.contains(inlineView.getDesc().getId())) {
            throw new AnalysisException(
                "Unsupported NOT IN predicate with subquery: " + expr.toSql());
          }
          // Replace the EQ operator in the generated join conjunct with a
          // null-matching EQ operator.
          for (Expr conjunct : onClausePredicate.getConjuncts()) {
            if (conjunct.equals(joinConjunct)) {
              Preconditions.checkState(conjunct instanceof BinaryPredicate);
              BinaryPredicate binaryPredicate = (BinaryPredicate) conjunct;
              Preconditions.checkState(binaryPredicate.getOp().isEquivalence());
              binaryPredicate.setOp(BinaryPredicate.Operator.NULL_MATCHING_EQ);
              break;
            }
          }
        } else {
          joinOp = JoinOperator.LEFT_ANTI_JOIN;
        }
      }
      inlineView.setJoinOp(joinOp);
      inlineView.setOnClause(onClausePredicate);
      return updateSelectList;
    }

    /**
     * Replace all unqualified star exprs ('*') from stmt's select list with qualified
     * ones, i.e. tbl_1.*,...,tbl_n.*, where tbl_1,...,tbl_n are the visible tablerefs
     * in stmt. 'tableIdx' indicates the maximum tableRef ordinal to consider when
     * replacing an unqualified star item.
     */
    private static void replaceUnqualifiedStarItems(SelectStmt stmt, int tableIdx) {
      Preconditions.checkState(tableIdx < stmt.fromClause_.size());
      List<SelectListItem> newItems = new ArrayList<>();
      for (int i = 0; i < stmt.selectList_.getItems().size(); ++i) {
        SelectListItem item = stmt.selectList_.getItems().get(i);
        if (!item.isStar() || item.getRawPath() != null) {
          newItems.add(item);
          continue;
        }
        // '*' needs to be replaced by tbl1.*,...,tbln.*, where
        // tbl1,...,tbln are the visible tableRefs in stmt.
        for (int j = 0; j < tableIdx; ++j) {
          TableRef tableRef = stmt.fromClause_.get(j);
          if (tableRef.getJoinOp() == JoinOperator.LEFT_SEMI_JOIN ||
              tableRef.getJoinOp() == JoinOperator.LEFT_ANTI_JOIN) {
            continue;
          }
          newItems.add(SelectListItem
              .createStarItem(Lists.newArrayList(tableRef.getUniqueAlias())));
        }
      }
      Preconditions.checkState(!newItems.isEmpty());
      boolean isDistinct = stmt.selectList_.isDistinct();
      stmt.selectList_ =
          new SelectList(newItems, isDistinct, stmt.selectList_.getPlanHints());
    }

    /**
     * Return true if the Expr tree rooted at 'expr' can be safely
     * eliminated, i.e. it only consists of conjunctions of true BoolLiterals.
     */
    private static boolean canEliminate(Expr expr) {
      for (Expr conjunct : expr.getConjuncts()) {
        if (!Expr.IS_TRUE_LITERAL.apply(conjunct)) return false;
      }
      return true;
    }

    /**
     * Extract all correlated predicates of a subquery.
     * <p>
     * TODO Handle correlated predicates in a HAVING clause.
     */
    private static List<Expr> extractCorrelatedPredicates(SelectStmt subqueryStmt)
        throws AnalysisException {
      List<TupleId> subqueryTupleIds = subqueryStmt.getTableRefIds();
      List<Expr> correlatedPredicates = new ArrayList<>();

      if (subqueryStmt.hasWhereClause()) {
        if (!canExtractCorrelatedPredicates(subqueryStmt.getWhereClause(),
            subqueryTupleIds)) {
          throw new AnalysisException(
              "Disjunctions with correlated predicates " + "are not supported: " +
                  subqueryStmt.getWhereClause().toSql());
        }
        // Extract the correlated predicates from the subquery's WHERE clause and
        // replace them with true BoolLiterals.
        Expr newWhereClause =
            extractCorrelatedPredicates(subqueryStmt.getWhereClause(), subqueryTupleIds,
                correlatedPredicates);
        if (canEliminate(newWhereClause)) newWhereClause = null;
        subqueryStmt.setWhereClause(newWhereClause);
      }

      // Process all correlated predicates from subquery's ON clauses.
      for (TableRef tableRef : subqueryStmt.getTableRefs()) {
        if (tableRef.getOnClause() == null) continue;

        List<Expr> onClauseCorrelatedPreds = new ArrayList<>();
        Expr newOnClause =
            extractCorrelatedPredicates(tableRef.getOnClause(), subqueryTupleIds,
                onClauseCorrelatedPreds);
        if (onClauseCorrelatedPreds.isEmpty()) continue;

        correlatedPredicates.addAll(onClauseCorrelatedPreds);
        if (canEliminate(newOnClause)) {
          // After the extraction of correlated predicates from an ON clause,
          // the latter may only contain conjunctions of True BoolLiterals. In
          // this case, we can eliminate the ON clause and set the join type to
          // CROSS JOIN.
          tableRef.setJoinOp(JoinOperator.CROSS_JOIN);
          tableRef.setOnClause(null);
        } else {
          tableRef.setOnClause(newOnClause);
        }
      }
      return correlatedPredicates;
    }

    /**
     * Extract all correlated predicates from the expr tree rooted at 'root' and
     * replace them with true BoolLiterals. The modified expr tree is returned
     * and the extracted correlated predicates are added to 'matches'.
     */
    private static Expr extractCorrelatedPredicates(Expr root, List<TupleId> tupleIds,
        List<Expr> matches) {
      if (isCorrelatedPredicate(root, tupleIds)) {
        matches.add(root);
        return new BoolLiteral(true);
      }
      for (int i = 0; i < root.getChildren().size(); ++i) {
        root.getChildren()
            .set(i, extractCorrelatedPredicates(root.getChild(i), tupleIds, matches));
      }
      return root;
    }

    /**
     * Checks if an expr containing a correlated subquery is eligible for rewrite by
     * transforming into a join. Throws an AnalysisException if 'expr' is not eligible for
     * rewrite.
     * TODO: Merge all the rewrite eligibility tests into a single function.
     */
    private static void validateCorrelatedSubqueryStmt(Expr expr)
        throws AnalysisException {
      Preconditions.checkNotNull(expr);
      Preconditions.checkState(expr.contains(Subquery.class));
      SelectStmt stmt = (SelectStmt) expr.getSubquery().getStatement();
      Preconditions.checkNotNull(stmt);
      // Grouping and/or aggregation is not allowed on correlated scalar and IN subqueries
      if ((expr instanceof BinaryPredicate
              && (stmt.hasGroupByClause() || stmt.hasAnalyticInfo()))
          || (expr instanceof InPredicate
                 && (stmt.hasMultiAggInfo() || stmt.hasAnalyticInfo()))) {
        throw new AnalysisException(
            "Unsupported correlated subquery with grouping " + "and/or aggregation: " +
                stmt.toSql());
      }
      // TODO: instead of this check, implement IMPALA-6315
      if (!expr.getSubquery().isScalarSubquery() &&
          !(expr instanceof InPredicate || expr instanceof ExistsPredicate)) {
        throw new AnalysisException(
            "Unsupported correlated subquery with runtime scalar check: " + stmt.toSql());
      }
      // The following correlated subqueries with a limit clause are supported:
      // 1. EXISTS subqueries
      // 2. Scalar subqueries with aggregation
      if (stmt.hasLimit()
          && (!(expr instanceof BinaryPredicate) || !stmt.hasMultiAggInfo()
                 || stmt.selectList_.isDistinct())
          && !(expr instanceof ExistsPredicate)) {
        throw new AnalysisException(
            "Unsupported correlated subquery with a " + "LIMIT clause: " + stmt.toSql());
      }
    }

    /**
     * Checks if all the 'correlatedPredicates' extracted from the subquery of 'expr' can be
     * added to the ON-clause of the join that results from the subquery rewrite. It throws
     * an AnalysisException if this is not the case. 'inlineView' is the generated inline
     * view that will replace the subquery in the rewritten statement.
     */
    private static void validateCorrelatedPredicates(Expr expr, InlineViewRef inlineView,
        List<Expr> correlatedPredicates) throws AnalysisException {
      Preconditions.checkNotNull(expr);
      Preconditions.checkNotNull(correlatedPredicates);
      Preconditions.checkState(inlineView.isAnalyzed());
      SelectStmt stmt = (SelectStmt) expr.getSubquery().getStatement();
      final com.google.common.base.Predicate<Expr> isSingleSlotRef =
          new com.google.common.base.Predicate<Expr>() {
            @Override
            public boolean apply(Expr arg) { return arg.unwrapSlotRef(false) != null; }
          };

      // A HAVING clause is only allowed on correlated EXISTS subqueries with
      // correlated binary predicates of the form Slot = Slot (see IMPALA-2734)
      // TODO Handle binary predicates with IS NOT DISTINCT op
      if (expr instanceof ExistsPredicate && stmt.hasHavingClause()
          && !correlatedPredicates.isEmpty()
          && (!stmt.hasMultiAggInfo()
                 || !Iterables.all(correlatedPredicates,
                        Predicates.or(Expr.IS_EQ_BINARY_PREDICATE, isSingleSlotRef)))) {
        throw new AnalysisException(
            "Unsupported correlated EXISTS subquery with a " + "HAVING clause: " +
                stmt.toSql());
      }

      // We only support equality correlated predicates in aggregate subqueries
      // (see IMPALA-5531). This check needs to be performed after the inline view
      // has been analyzed to make sure we don't incorrectly reject non-equality
      // correlated predicates from nested collections.
      if (expr instanceof BinaryPredicate && !inlineView.isCorrelated() &&
          !correlatedPredicates.isEmpty()) {
        final List<TupleId> subqueryTblIds = stmt.getTableRefIds();
        final com.google.common.base.Predicate<Expr> isBoundBySubqueryTids =
            new com.google.common.base.Predicate<Expr>() {
              @Override
              public boolean apply(Expr arg) {
                List<TupleId> tids = new ArrayList<>();
                arg.getIds(tids, null);
                return !Collections.disjoint(tids, subqueryTblIds);
              }
            };

        List<Expr> unsupportedPredicates = Lists.newArrayList(Iterables
            .filter(correlatedPredicates,
                Predicates.and(Expr.IS_NOT_EQ_BINARY_PREDICATE, isBoundBySubqueryTids)));
        if (!unsupportedPredicates.isEmpty()) {
          throw new AnalysisException("Unsupported aggregate subquery with "
              + "non-equality correlated predicates: "
              + Expr.listToSql(unsupportedPredicates, DEFAULT));
        }
      }
    }

    /**
     * Update the subquery within an inline view by expanding its select list with exprs
     * from a correlated predicate 'expr' that will be 'moved' to an ON clause in the
     * subquery's parent query block. We need to make sure that every expr extracted from
     * the subquery references an item in the subquery's select list. If 'updateGroupBy'
     * is true, the exprs extracted from 'expr' are also added in stmt's GROUP BY clause.
     * Throws an AnalysisException if we need to update the GROUP BY clause but
     * both the lhs and rhs of 'expr' reference a tuple of the subquery stmt.
     */
    private static void updateInlineView(InlineViewRef inlineView, Expr expr,
        List<TupleId> parentQueryTids, List<Expr> lhsExprs, List<Expr> rhsExprs,
        boolean updateGroupBy) throws AnalysisException {
      SelectStmt stmt = (SelectStmt) inlineView.getViewStmt();
      List<TupleId> subqueryTblIds = stmt.getTableRefIds();
      List<Expr> groupByExprs = null;
      if (updateGroupBy) groupByExprs = new ArrayList<>();

      List<SelectListItem> items = stmt.selectList_.getItems();
      // Collect all the SlotRefs from 'expr' and identify those that are bound by
      // subquery tuple ids.
      List<Expr> slotRefs = new ArrayList<>();
      expr.collectAll(Predicates.instanceOf(SlotRef.class), slotRefs);
      List<Expr> exprsBoundBySubqueryTids = new ArrayList<>();
      for (Expr slotRef : slotRefs) {
        if (slotRef.isBoundByTupleIds(subqueryTblIds)) {
          exprsBoundBySubqueryTids.add(slotRef);
        }
      }
      // The correlated predicate only references slots from a parent block,
      // no need to update the subquery's select or group by list.
      if (exprsBoundBySubqueryTids.isEmpty()) return;
      if (updateGroupBy) {
        Preconditions.checkState(expr instanceof BinaryPredicate);
        Expr exprBoundBySubqueryTids;
        if (exprsBoundBySubqueryTids.size() > 1) {
          // If the predicate contains multiple SlotRefs bound by subquery tuple
          // ids, they must all be on the same side of that predicate.
          if (expr.getChild(0).isBoundByTupleIds(subqueryTblIds) &&
              expr.getChild(1).isBoundByTupleIds(parentQueryTids)) {
            exprBoundBySubqueryTids = expr.getChild(0);
          } else if (expr.getChild(0).isBoundByTupleIds(parentQueryTids) &&
              expr.getChild(1).isBoundByTupleIds(subqueryTblIds)) {
            exprBoundBySubqueryTids = expr.getChild(1);
          } else {
            throw new AnalysisException("All subquery columns " +
                "that participate in a predicate must be on the same side of " +
                "that predicate: " + expr.toSql());
          }
        } else {
          Preconditions.checkState(exprsBoundBySubqueryTids.size() == 1);
          exprBoundBySubqueryTids = exprsBoundBySubqueryTids.get(0);
        }
        exprsBoundBySubqueryTids.clear();
        exprsBoundBySubqueryTids.add(exprBoundBySubqueryTids);
      }

      // Add the exprs bound by subquery tuple ids to the select list and
      // register it for substitution. We use a temporary substitution map
      // because we cannot at this point analyze the new select list expr. Once
      // the new inline view is analyzed, the entries from this map will be
      // added to an ExprSubstitutionMap.
      for (Expr boundExpr : exprsBoundBySubqueryTids) {
        String colAlias = stmt.getColumnAliasGenerator().getNextAlias();
        items.add(new SelectListItem(boundExpr, null));
        inlineView.getExplicitColLabels().add(colAlias);
        lhsExprs.add(boundExpr);
        rhsExprs
            .add(new SlotRef(Lists.newArrayList(inlineView.getUniqueAlias(), colAlias)));
        if (groupByExprs != null) groupByExprs.add(boundExpr);
      }

      // Update the subquery's select list.
      boolean isDistinct = stmt.selectList_.isDistinct();
      stmt.selectList_ =
          new SelectList(items, isDistinct, stmt.selectList_.getPlanHints());
      // Update subquery's GROUP BY clause
      if (groupByExprs != null && !groupByExprs.isEmpty()) {
        if (stmt.hasGroupByClause()) {
          stmt.groupingExprs_.addAll(groupByExprs);
        } else {
          stmt.groupingExprs_ = groupByExprs;
        }
      }
    }

    /**
     * Returns true if we can extract the correlated predicates from 'expr'. A
     * correlated predicate cannot be extracted if it is part of a disjunction.
     */
    private static boolean canExtractCorrelatedPredicates(Expr expr,
        List<TupleId> subqueryTupleIds) {
      if (!(expr instanceof CompoundPredicate)) return true;
      if (Expr.IS_OR_PREDICATE.apply(expr)) {
        return !containsCorrelatedPredicate(expr, subqueryTupleIds);
      }
      for (Expr child : expr.getChildren()) {
        if (!canExtractCorrelatedPredicates(child, subqueryTupleIds)) {
          return false;
        }
      }
      return true;
    }

    /**
     * Return true if the expr tree rooted at 'root' contains a correlated
     * predicate.
     */
    private static boolean containsCorrelatedPredicate(Expr root,
        List<TupleId> tupleIds) {
      if (isCorrelatedPredicate(root, tupleIds)) return true;
      for (Expr child : root.getChildren()) {
        if (containsCorrelatedPredicate(child, tupleIds)) return true;
      }
      return false;
    }

    /**
     * Returns true if 'expr' is a correlated predicate. A predicate is
     * correlated if at least one of its SlotRefs belongs to an ancestor
     * query block (i.e. is not bound by the given 'tupleIds').
     */
    private static boolean isCorrelatedPredicate(Expr expr, List<TupleId> tupleIds) {
      return (expr instanceof BinaryPredicate || expr instanceof SlotRef) &&
          !expr.isBoundByTupleIds(tupleIds);
    }

    /**
     * Converts an expr containing a subquery into an analyzed conjunct to be
     * used in a join. The conversion is performed in place by replacing the
     * subquery with the first expr from the select list of 'inlineView'.
     * If 'isCorrelated' is true and the first expr from the inline view contains
     * an aggregate function that returns non-null on an empty input,
     * the aggregate function is wrapped into a 'zeroifnull' function.
     */
    private static Expr createJoinConjunct(Expr exprWithSubquery,
        InlineViewRef inlineView, Analyzer analyzer, boolean isCorrelated)
        throws AnalysisException {
      Preconditions.checkNotNull(exprWithSubquery);
      Preconditions.checkNotNull(inlineView);
      Preconditions.checkState(exprWithSubquery.contains(Subquery.class));
      if (exprWithSubquery instanceof ExistsPredicate) return null;
      // Create a SlotRef from the first item of inlineView's select list
      SlotRef slotRef = new SlotRef(Lists
          .newArrayList(inlineView.getUniqueAlias(), inlineView.getColLabels().get(0)));
      slotRef.analyze(analyzer);
      Expr subquerySubstitute = slotRef;
      if (exprWithSubquery instanceof InPredicate) {
        BinaryPredicate pred =
            new BinaryPredicate(BinaryPredicate.Operator.EQ, exprWithSubquery.getChild(0),
                slotRef);
        pred.analyze(analyzer);
        return pred;
      }
      Subquery subquery = exprWithSubquery.getSubquery();
      Preconditions.checkState(subquery.getType().isScalarType());
      ExprSubstitutionMap smap = new ExprSubstitutionMap();
      SelectListItem item =
          ((SelectStmt) inlineView.getViewStmt()).getSelectList().getItems().get(0);
      if (isCorrelated && item.getExpr().contains(Expr.IS_UDA_FN)) {
        throw new AnalysisException(
            "UDAs are not supported in the select list of " + "correlated subqueries: " +
                subquery.toSql());
      }
      if (isCorrelated && item.getExpr().contains(Expr.NON_NULL_EMPTY_AGG)) {
        // TODO: Add support for multiple agg functions that return non-null on an
        // empty input, by wrapping them with zeroifnull functions before the inline
        // view is analyzed.
        if (!Expr.NON_NULL_EMPTY_AGG.apply(item.getExpr()) &&
            (!(item.getExpr() instanceof CastExpr) ||
                !Expr.NON_NULL_EMPTY_AGG.apply(item.getExpr().getChild(0)))) {
          throw new AnalysisException("Aggregate function that returns non-null on " +
              "an empty input cannot be used in an expression in a " +
              "correlated subquery's select list: " + subquery.toSql());
        }

        List<Expr> aggFns = new ArrayList<>();
        item.getExpr().collectAll(Expr.NON_NULL_EMPTY_AGG, aggFns);
        // TODO Generalize this by making the aggregate functions aware of the
        // literal expr that they return on empty input, e.g. max returns a
        // NullLiteral whereas count returns a NumericLiteral.
        if (((FunctionCallExpr) aggFns.get(0)).getReturnType().isNumericType()) {
          FunctionCallExpr zeroIfNull =
              new FunctionCallExpr("zeroifnull", Lists.newArrayList((Expr) slotRef));
          zeroIfNull.analyze(analyzer);
          subquerySubstitute = zeroIfNull;
        } else if (((FunctionCallExpr) aggFns.get(0)).getReturnType().isStringType()) {
          List<Expr> params = new ArrayList<>();
          params.add(slotRef);
          params.add(new StringLiteral(""));
          FunctionCallExpr ifnull = new FunctionCallExpr("ifnull", params);
          ifnull.analyze(analyzer);
          subquerySubstitute = ifnull;
        } else {
          throw new AnalysisException("Unsupported aggregate function used in " +
              "a correlated subquery's select list: " + subquery.toSql());
        }
      }
      smap.put(subquery, subquerySubstitute);
      return exprWithSubquery.substitute(smap, analyzer, false);
    }

    /**
     * Rewrite all the subqueries of a SelectStmt in place. Subqueries are currently
     * supported in the FROM clause, WHERE clause and SELECT list. The rewrite is
     * performed in place and not in a clone of SelectStmt because it requires the stmt to
     * be analyzed.
     */
    @Override
    protected void rewriteSelectStmtHook(SelectStmt stmt, Analyzer analyzer)
        throws AnalysisException {
      // Rewrite all the subqueries in the HAVING clause.
      if (stmt.hasHavingClause() && stmt.havingClause_.getSubquery() != null) {
        if (hasSubqueryInDisjunction(stmt.havingClause_)) {
          throw new AnalysisException("Subqueries in OR predicates are not supported: "
              + stmt.havingClause_.toSql());
        }
        rewriteHavingClauseSubqueries(stmt, analyzer);
      }

      // Rewrite all the subqueries in the WHERE clause.
      if (stmt.hasWhereClause()) {
        // Push negation to leaf operands.
        stmt.whereClause_ = Expr.pushNegationToOperands(stmt.whereClause_);
        // Check if we can rewrite the subqueries in the WHERE clause. OR predicates with
        // subqueries are not supported.
        if (hasSubqueryInDisjunction(stmt.whereClause_)) {
          throw new AnalysisException("Subqueries in OR predicates are not supported: " +
              stmt.whereClause_.toSql());
        }
        rewriteWhereClauseSubqueries(stmt, analyzer);
      }
      rewriteSelectListSubqueries(stmt, analyzer);
    }

    /**
     * Rewrite all subqueries of a stmt's WHERE clause. Initially, all the
     * conjuncts containing subqueries are extracted from the WHERE clause and are
     * replaced with true BoolLiterals. Subsequently, each extracted conjunct is
     * merged into its parent select block by converting it into a join.
     * Conjuncts with subqueries that themselves contain conjuncts with subqueries are
     * recursively rewritten in a bottom up fashion.
     *
     * The following example illustrates the bottom up rewriting of nested queries.
     * Suppose we have the following three level nested query Q0:
     *
     * SELECT *
     * FROM T1                                            : Q0
     * WHERE T1.a IN (SELECT a
     *                FROM T2 WHERE T2.b IN (SELECT b
     *                                       FROM T3))
     * AND T1.c < 10;
     *
     * This query will be rewritten as follows. Initially, the IN predicate
     * T1.a IN (SELECT a FROM T2 WHERE T2.b IN (SELECT b FROM T3)) is extracted
     * from the top level block (Q0) since it contains a subquery and is
     * replaced by a true BoolLiteral, resulting in the following query Q1:
     *
     * SELECT * FROM T1 WHERE TRUE : Q1
     *
     * Since the stmt in the extracted predicate contains a conjunct with a subquery,
     * it is also rewritten. As before, rewriting stmt SELECT a FROM T2
     * WHERE T2.b IN (SELECT b FROM T3) works by first extracting the conjunct that
     * contains the subquery (T2.b IN (SELECT b FROM T3)) and substituting it with
     * a true BoolLiteral, producing the following stmt Q2:
     *
     * SELECT a FROM T2 WHERE TRUE : Q2
     *
     * The predicate T2.b IN (SELECT b FROM T3) is then merged with Q2,
     * producing the following unnested query Q3:
     *
     * SELECT a FROM T2 LEFT SEMI JOIN (SELECT b FROM T3) $a$1 ON T2.b = $a$1.b : Q3
     *
     * The extracted IN predicate becomes:
     *
     * T1.a IN (SELECT a FROM T2 LEFT SEMI JOIN (SELECT b FROM T3) $a$1 ON T2.b = $a$1.b)
     *
     * Finally, the rewritten IN predicate is merged with query block Q1,
     * producing the following unnested query (WHERE clauses that contain only
     * conjunctions of true BoolLiterals are eliminated):
     *
     * SELECT *
     * FROM T1 LEFT SEMI JOIN (SELECT a
     *                         FROM T2 LEFT SEMI JOIN (SELECT b FROM T3) $a$1
     *                         ON T2.b = $a$1.b) $a$1
     * ON $a$1.a = T1.a
     * WHERE T1.c < 10;
     *
     */
    private void rewriteWhereClauseSubqueries(SelectStmt stmt, Analyzer analyzer)
        throws AnalysisException {
      int numTableRefs = stmt.fromClause_.size();
      List<Expr> exprsWithSubqueries = new ArrayList<>();
      ExprSubstitutionMap smap = new ExprSubstitutionMap();
      // Check if all the conjuncts in the WHERE clause that contain subqueries
      // can currently be rewritten as a join.
      for (Expr conjunct : stmt.whereClause_.getConjuncts()) {
        List<Subquery> subqueries = new ArrayList<>();
        conjunct.collectAll(Predicates.instanceOf(Subquery.class), subqueries);
        if (subqueries.size() == 0) continue;
        if (subqueries.size() > 1) {
          throw new AnalysisException(
              "Multiple subqueries are not supported in " + "expression: " +
                  conjunct.toSql());
        }
        if (!(conjunct instanceof InPredicate) &&
            !(conjunct instanceof ExistsPredicate) &&
            !(conjunct instanceof BinaryPredicate) &&
            !conjunct.getSubquery().getType().isScalarType()) {
          throw new AnalysisException(
              "Non-scalar subquery is not supported in " + "expression: " +
                  conjunct.toSql());
        }

        Expr rewrittenConjunct = conjunct;
        if (conjunct instanceof InPredicate && conjunct.getChild(0).isConstant()) {
          Expr newConjunct = rewriteInConstant(stmt, (InPredicate) conjunct);
          if (newConjunct != null) {
            newConjunct.analyze(analyzer);
            rewrittenConjunct = newConjunct;
          }
        }

        if (rewrittenConjunct instanceof ExistsPredicate) {
          // Check if we can determine the result of an ExistsPredicate during analysis.
          // If so, replace the predicate with a BoolLiteral predicate and remove it from
          // the list of predicates to be rewritten.
          BoolLiteral boolLiteral =
              replaceExistsPredicate((ExistsPredicate) rewrittenConjunct);
          if (boolLiteral != null) {
            boolLiteral.analyze(analyzer);
            smap.put(conjunct, boolLiteral);
            continue;
          }
        }

        // Replace all the supported exprs with subqueries with true BoolLiterals
        // using an smap.
        BoolLiteral boolLiteral = new BoolLiteral(true);
        boolLiteral.analyze(analyzer);
        smap.put(conjunct, boolLiteral);
        exprsWithSubqueries.add(rewrittenConjunct);
      }
      stmt.whereClause_ = stmt.whereClause_.substitute(smap, analyzer, false);

      boolean hasNewVisibleTuple = false;
      // Recursively rewrite all the exprs that contain subqueries and merge them
      // with 'stmt'.
      for (Expr expr : exprsWithSubqueries) {
        if (mergeExpr(stmt, rewriteExpr(expr, analyzer), analyzer)) {
          hasNewVisibleTuple = true;
        }
      }
      if (canEliminate(stmt.whereClause_)) stmt.whereClause_ = null;
      if (hasNewVisibleTuple) replaceUnqualifiedStarItems(stmt, numTableRefs);
    }

    /**
     * Modifies in place an expr that contains a subquery by rewriting its
     * subquery stmt. The modified analyzed expr is returned.
     */
    private Expr rewriteExpr(Expr expr, Analyzer analyzer) throws AnalysisException {
      // Extract the subquery and rewrite it.
      Subquery subquery = expr.getSubquery();
      Preconditions.checkNotNull(subquery);
      rewriteSelectStatement((SelectStmt) subquery.getStatement(),
          subquery.getAnalyzer());
      // Create a new Subquery with the rewritten stmt and use a substitution map
      // to replace the original subquery from the expr.
      QueryStmt rewrittenStmt = subquery.getStatement().clone();
      rewrittenStmt.reset();
      Subquery newSubquery = new Subquery(rewrittenStmt);
      newSubquery.analyze(analyzer);
      ExprSubstitutionMap smap = new ExprSubstitutionMap();
      smap.put(subquery, newSubquery);
      return expr.substitute(smap, analyzer, false);
    }

    /**
     * Rewrite subqueries of a stmt's SELECT list. Scalar subqueries are the only type
     * of subquery supported in the select list.  Scalar subqueries return a single column
     * and at most 1 row, a runtime error should be thrown if more than one row is
     * returned. Generally these subqueries can be evaluated once for every row of the
     * outer query however for performance reasons we want to rewrite evaluation to use
     * joins where possible.
     *
     * 1) Uncorrelated Scalar Aggregate Query
     *
     *    SELECT T1.a, (SELECT avg(T2.a) from T2) FROM T1;
     *
     *    This is implemented by flattening into a join.
     *
     *    SELECT T1.a, $a$1.$c$1 FROM T1, (SELECT avg(T2.a) $c$1 FROM T2) $a$1
     *
     *    Currently we only support very simple subqueries which return a single aggregate
     *    function with no group by columns unless a LIMIT 1 is given. TODO: IMPALA-1285
     *
     * 2) Correlated Scalar Aggregate
     *
     *    TODO: IMPALA-8955
     *    SELECT id, (SELECT count(*) FROM T2 WHERE id=a.id ) FROM T1 a
     *
     *    This can be flattened with a LEFT OUTER JOIN
     *
     *    SELECT T1.a, $a$1.$c$1 FROM T1 LEFT OUTER JOIN
     *      (SELECT id, count(*) $c$1 FROM T2 GROUP BY id) $a$1 ON T1.id = $a$1.id
     *
     * 3) Correlated Scalar
     *
     *    TODO: IMPALA-6315
     *    SELECT id, (SELECT cost FROM T2 WHERE id=a.id ) FROM T1 a
     *
     *    In this case there is no aggregate function to guarantee only a single row is
     *    returned per group so a run time cardinality check must be applied. An exception
     *    would be if the correlated predicates had primary key constraints.
     *
     * 4) Runtime Scalar Subqueries
     *
     *    TODO: IMPALA-5100
     *    We do have a {@link CardinalityCheckNode} for runtime checks however queries
     *    can't always be rewritten into an NLJ without special care. For example with
     *    conditional expression like below:
     *
     *    SELECT T1.a,
     *      IF((SELECT max(T2.a) from T2 > 10,
     *         (SELECT T2.a from T2 WHERE id=T1.id),
     *         (SELECT T3.a from T2 WHERE if=T1.id)
     *    FROM T1;
     *
     *    If rewritten to joins with cardinality checks then both legs of the conditional
     *    expression would be evaluated regardless of the condition. If the false case
     *    were to return a runtime error while when the true doesn't and the condition
     *    evaluates to true then we'd have incorrect behavior.
     */
    private void rewriteSelectListSubqueries(SelectStmt stmt, Analyzer analyzer)
        throws AnalysisException {
      Preconditions.checkNotNull(stmt);
      Preconditions.checkNotNull(analyzer);
      final int numTableRefs = stmt.fromClause_.size();
      final boolean parentHasAgg = stmt.hasMultiAggInfo();
      // Track any new inline views so we later ensure they are rewritten if needed.
      // An improvement would be to have a pre/post order abstract rewriter class.
      final List<InlineViewRef> newViews = new ArrayList<>();
      for (SelectListItem selectItem : stmt.getSelectList().getItems()) {
        if (selectItem.isStar()) {
          continue;
        }

        final Expr expr = selectItem.getExpr();
        final List<Subquery> subqueries = new ArrayList<>();
        // Use collect as opposed to collectAll in order to allow nested subqueries to be
        // rewritten as needed. For example a subquery in the select list which contains
        // its own subquery in the where clause.
        expr.collect(Predicates.instanceOf(Subquery.class), subqueries);
        if (subqueries.size() == 0) {
          continue;
        }
        final ExprSubstitutionMap smap = new ExprSubstitutionMap();
        for (Subquery sq : subqueries) {
          final SelectStmt subqueryStmt = (SelectStmt) sq.getStatement();
          // TODO: Handle correlated subqueries IMPALA-8955
          if (isCorrelated(subqueryStmt)) {
            throw new AnalysisException("A correlated scalar subquery is not supported "
                + "in the expression: " + expr.toSql());
          }
          Preconditions.checkState(sq.getType().isScalarType());

          // Existential subqueries in Impala aren't really execution time expressions,
          // they are either checked at plan time or expected to be handled by the
          // subquery rewrite into a join. In the case of the select list we will only
          // support plan time evaluation.
          boolean replacedExists = false;
          final List<ExistsPredicate> existsPredicates = new ArrayList<>();
          expr.collect(ExistsPredicate.class, existsPredicates);
          for (ExistsPredicate ep : existsPredicates) {
            // Check to see if the current subquery is the child of an exists predicate.
            if (ep.contains(sq)) {
              final BoolLiteral boolLiteral = replaceExistsPredicate(ep);
              if (boolLiteral != null) {
                boolLiteral.analyze(analyzer);
                smap.put(ep, boolLiteral);
                replacedExists = true;
                break;
              } else {
                throw new AnalysisException(
                    "Unsupported subquery with runtime scalar check: " + ep.toSql());
              }
            }
          }
          if (replacedExists) {
            continue;
          }

          List<String> colLabels = new ArrayList<>();
          for (int i = 0; i < subqueryStmt.getColLabels().size(); ++i) {
            colLabels.add(subqueryStmt.getColumnAliasGenerator().getNextAlias());
          }
          // Create a new inline view from the subquery stmt aliasing the columns.
          InlineViewRef inlineView = new InlineViewRef(
              stmt.getTableAliasGenerator().getNextAlias(), subqueryStmt, colLabels);
          inlineView.reset();
          inlineView.analyze(analyzer);

          // For uncorrelated scalar subqueries we rewrite with a CROSS_JOIN. This makes
          // it simpler to further optimize by merging subqueries without worrying about
          // join ordering as in IMPALA-9796. For correlated subqueries we'd want to
          // rewrite to a LOJ.
          inlineView.setJoinOp(JoinOperator.CROSS_JOIN);
          stmt.fromClause_.add(inlineView);
          newViews.add(inlineView);

          SlotRef slotRef = new SlotRef(Lists.newArrayList(
              inlineView.getUniqueAlias(), inlineView.getColLabels().get(0)));
          slotRef.analyze(analyzer);
          Expr substitute = slotRef;
          // Need to wrap the expression with a no-op aggregate function if the stmt does
          // any aggregation, using MAX() given no explicit function to return any value
          // in a group.
          if (parentHasAgg) {
            final FunctionCallExpr aggWrapper =
                new FunctionCallExpr("max", Lists.newArrayList((Expr) slotRef));
            aggWrapper.analyze(analyzer);
            substitute = aggWrapper;
          }
          // Substitute original subquery expression with a reference to the inline view.
          smap.put(sq, substitute);
        }
        // Update select list with any new slot references.
        selectItem.setExpr(expr.substitute(smap, analyzer, false));
      }
      // Rewrite any new views
      for (InlineViewRef v : newViews) {
        rewriteQueryStatement(v.getViewStmt(), v.getAnalyzer());
      }
      // Only applies to the original list of TableRefs, not any as a result of the
      // rewrite.
      if (!newViews.isEmpty()) {
        replaceUnqualifiedStarItems(stmt, numTableRefs);
      }
    }

    /**
     * Rewrite subqueries of a stmt's HAVING clause. The stmt is rewritten into two
     * separate statements; an inner statement which performs all sql operations that
     * evaluated before the HAVING clause and an outer statement which projects the inner
     * stmt's results with the HAVING clause rewritten as a WHERE clause and also performs
     * the remainder of the sql operations (ORDER BY, LIMIT). We then rely on the WHERE
     * clause rewrite rule to handle the subqueries that were originally in the HAVING
     * clause.
     *
     * SELECT a, sum(b) FROM T1 GROUP BY a HAVING count(b) > (SELECT max(c) FROM T2)
     * ORDER BY 2 LIMIT 10
     *
     * Inner Stmt becomes:
     *
     * SELECT a, sum(b), count(b) FROM T1 GROUP BY a
     *
     * Notice we augment the select list with any aggregates in the HAVING clause that are
     * missing in the original select list.
     *
     * Outer Stmt becomes:
     *
     * SELECT $a$1.$c$1 a, $a$1.$c$2 sum(b) FROM
     * (SELECT a, sum(b), count(b) FROM T1 GROUP BY a) $a$1 ($c$1, $c$2, $c$3) WHERE
     * $a$1.$c$3 > (SELECT max(c) FROM T2) ORDER BY 2 LIMIT 10
     *
     * The query should would then be rewritten by the caller using
     * rewriteWhereClauseSubqueries()
     *
     */
    private void rewriteHavingClauseSubqueries(SelectStmt stmt, Analyzer analyzer)
        throws AnalysisException {
      // Generate the inner query from the current statement pulling up the order by,
      // limit, and any aggregates in the having clause that aren't projected in the
      // select list.
      final SelectStmt innerStmt = stmt.clone();
      final List<FunctionCallExpr> aggExprs = stmt.hasMultiAggInfo() ?
          stmt.getMultiAggInfo().getAggExprs() :
          new ArrayList<>();
      for (FunctionCallExpr agg : aggExprs) {
        boolean contains = false;
        for (SelectListItem selectListItem : stmt.getSelectList().getItems()) {
          contains = selectListItem.getExpr().equals(agg);
          if (contains) {
            break;
          }
        }
        if (!contains) {
          innerStmt.selectList_.getItems().add(
              new SelectListItem(agg.clone().reset(), null));
        }
      }

      // Remove clauses that will go into the outer statement.
      innerStmt.havingClause_ = null;
      innerStmt.limitElement_ = new LimitElement(null, null);
      if (innerStmt.hasOrderByClause()) {
        innerStmt.orderByElements_ = null;
      }
      innerStmt.reset();

      // Used in the substitution map, as post analyze() exprs won't match.
      final List<SelectListItem> preAnalyzeSelectList =
          innerStmt.getSelectList().clone().getItems();
      final ExprSubstitutionMap smap = new ExprSubstitutionMap();
      List<String> colLabels =
          Lists.newArrayListWithCapacity(innerStmt.getSelectList().getItems().size());

      for (int i = 0; i < innerStmt.getSelectList().getItems().size(); ++i) {
        String colAlias = stmt.getColumnAliasGenerator().getNextAlias();
        colLabels.add(colAlias);
      }

      final String innerAlias = stmt.getTableAliasGenerator().getNextAlias();
      final InlineViewRef innerView = new InlineViewRef(innerAlias, innerStmt, colLabels);
      innerView.analyze(analyzer);

      // Rewrite the new inline view.
      rewriteSelectStatement(
          (SelectStmt) innerView.getViewStmt(), innerView.getViewStmt().getAnalyzer());

      for (int i = 0; i < preAnalyzeSelectList.size(); ++i) {
        final Expr slot = new SlotRef(Lists.newArrayList(innerAlias, colLabels.get(i)));
        slot.analyze(analyzer);
        smap.put(preAnalyzeSelectList.get(i).getExpr(), slot);
      }

      // Create the new outer statement's select list.
      final List<SelectListItem> outerSelectList = new ArrayList<>();
      for (int i = 0; i < stmt.getSelectList().getItems().size(); ++i) {
        // Project the original select list items and labels
        final SelectListItem si = new SelectListItem(
            stmt.getSelectList().getItems().get(i).getExpr().clone().reset().substitute(
                smap, analyzer, false),
            stmt.getColLabels().get(i));
        si.getExpr().analyze(analyzer);
        outerSelectList.add(si);
      }

      // Clear out the old stmt properties.
      stmt.whereClause_ = stmt.havingClause_.reset().substitute(smap, analyzer, false);
      stmt.whereClause_.analyze(analyzer);
      stmt.havingClause_ = null;
      stmt.groupingExprs_ = null;
      stmt.selectList_.getItems().clear();
      stmt.selectList_.getItems().addAll(outerSelectList);
      stmt.fromClause_.getTableRefs().clear();
      stmt.fromClause_.add(innerView);

      stmt.analyze(analyzer);
      if (LOG.isTraceEnabled())
        LOG.trace("Rewritten HAVING Clause SQL: " + stmt.toSql(REWRITTEN));
    }
  }
}
