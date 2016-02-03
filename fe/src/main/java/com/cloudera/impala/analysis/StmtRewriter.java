// Copyright 2014 Cloudera Inc.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext.AnalysisResult;
import com.cloudera.impala.analysis.UnionStmt.UnionOperand;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.TreeNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Class representing a statement rewriter. A statement rewriter performs subquery
 * unnesting on an analyzed parse tree.
 * TODO: Now that we have a nested-loop join supporting all join modes we could
 * allow more rewrites, although it is not clear we would always want to.
 */
public class StmtRewriter {
  private final static Logger LOG = LoggerFactory.getLogger(StmtRewriter.class);

  /**
   * Rewrite the statement of an analysis result. The unanalyzed rewritten
   * statement is returned.
   */
  public static StatementBase rewrite(AnalysisResult analysisResult)
      throws AnalysisException {
    // Analyzed stmt that contains a query statement with subqueries to be rewritten.
    StatementBase stmt = analysisResult.getStmt();
    Preconditions.checkState(stmt.isAnalyzed());
    // Analyzed query statement to be rewritten.
    QueryStmt queryStmt = null;
    if (stmt instanceof QueryStmt) {
      queryStmt = (QueryStmt) analysisResult.getStmt();
    } else if (stmt instanceof InsertStmt) {
      queryStmt = ((InsertStmt) analysisResult.getStmt()).getQueryStmt();
    } else if (stmt instanceof CreateTableAsSelectStmt) {
      queryStmt = ((CreateTableAsSelectStmt) analysisResult.getStmt()).getQueryStmt();
    } else {
      throw new AnalysisException("Unsupported statement containing subqueries: " +
          stmt.toSql());
    }
    rewriteQueryStatement(queryStmt, queryStmt.getAnalyzer());
    stmt.reset();
    return stmt;
  }

  /**
   * Calls the appropriate rewrite method based on the specific type of query stmt. See
   * rewriteSelectStatement() and rewriteUnionStatement() documentation.
   */
  public static void rewriteQueryStatement(QueryStmt stmt, Analyzer analyzer)
      throws AnalysisException {
    Preconditions.checkNotNull(stmt);
    Preconditions.checkNotNull(stmt.isAnalyzed());
    if (stmt instanceof SelectStmt) {
      rewriteSelectStatement((SelectStmt)stmt, analyzer);
    } else if (stmt instanceof UnionStmt) {
      rewriteUnionStatement((UnionStmt)stmt, analyzer);
    } else {
      throw new AnalysisException("Subqueries not supported for " +
          stmt.getClass().getSimpleName() + " statements");
    }
  }

  /**
   * Rewrite all the subqueries of a SelectStmt in place. Subqueries
   * are currently supported in FROM and WHERE clauses. The rewrite is performed in
   * place and not in a clone of SelectStmt because it requires the stmt to be analyzed.
   */
  private static void rewriteSelectStatement(SelectStmt stmt, Analyzer analyzer)
      throws AnalysisException {
    // Rewrite all the subqueries in the FROM clause.
    for (TableRef tblRef: stmt.tableRefs_) {
      if (!(tblRef instanceof InlineViewRef)) continue;
      InlineViewRef inlineViewRef = (InlineViewRef)tblRef;
      rewriteQueryStatement(inlineViewRef.getViewStmt(), inlineViewRef.getAnalyzer());
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
    stmt.sqlString_ = null;
    LOG.trace("rewritten stmt: " + stmt.toSql());
  }

  /**
   * Rewrite all operands in a UNION. The conditions that apply to SelectStmt rewriting
   * also apply here.
   */
  private static void rewriteUnionStatement(UnionStmt stmt, Analyzer analyzer)
      throws AnalysisException {
    for (UnionOperand operand: stmt.getOperands()) {
      Preconditions.checkState(operand.getQueryStmt() instanceof SelectStmt);
      StmtRewriter.rewriteSelectStatement(
          (SelectStmt)operand.getQueryStmt(), operand.getAnalyzer());
    }
  }

  /**
   * Returns true if the Expr tree rooted at 'expr' has at least one subquery
   * that participates in a disjunction.
   */
  private static boolean hasSubqueryInDisjunction(Expr expr) {
    if (!(expr instanceof CompoundPredicate)) return false;
    if (Expr.IS_OR_PREDICATE.apply(expr)) {
      return expr.contains(Subquery.class);
    }
    for (Expr child: expr.getChildren()) {
      if (hasSubqueryInDisjunction(child)) return true;
    }
    return false;
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
  private static void rewriteWhereClauseSubqueries(SelectStmt stmt, Analyzer analyzer)
     throws AnalysisException {
    int numTableRefs = stmt.tableRefs_.size();
    ArrayList<Expr> exprsWithSubqueries = Lists.newArrayList();
    ExprSubstitutionMap smap = new ExprSubstitutionMap();
    // Replace all BetweenPredicates that contain subqueries with their
    // equivalent compound predicates.
    stmt.whereClause_ = replaceBetweenPredicates(stmt.whereClause_);
    // Check if all the conjuncts in the WHERE clause that contain subqueries
    // can currently be rewritten as a join.
    for (Expr conjunct: stmt.whereClause_.getConjuncts()) {
      List<Subquery> subqueries = Lists.newArrayList();
      conjunct.collectAll(Predicates.instanceOf(Subquery.class), subqueries);
      if (subqueries.size() == 0) continue;
      if (subqueries.size() > 1) {
        throw new AnalysisException("Multiple subqueries are not supported in " +
            "expression: " + conjunct.toSql());
      }
      if (!(conjunct instanceof InPredicate) && !(conjunct instanceof ExistsPredicate) &&
          !(conjunct instanceof BinaryPredicate) &&
          !conjunct.contains(Expr.IS_SCALAR_SUBQUERY)) {
        throw new AnalysisException("Non-scalar subquery is not supported in " +
            "expression: " + conjunct.toSql());
      }

      if (conjunct instanceof ExistsPredicate) {
        // Check if we can determine the result of an ExistsPredicate during analysis.
        // If so, replace the predicate with a BoolLiteral predicate and remove it from
        // the list of predicates to be rewritten.
        BoolLiteral boolLiteral = replaceExistsPredicate((ExistsPredicate) conjunct);
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
      exprsWithSubqueries.add(conjunct);
    }
    stmt.whereClause_ = stmt.whereClause_.substitute(smap, analyzer, false);

    boolean hasNewVisibleTuple = false;
    // Recursively rewrite all the exprs that contain subqueries and merge them
    // with 'stmt'.
    for (Expr expr: exprsWithSubqueries) {
      if (mergeExpr(stmt, rewriteExpr(expr, analyzer), analyzer)) {
        hasNewVisibleTuple = true;
      }
    }
    if (canEliminate(stmt.whereClause_)) stmt.whereClause_ = null;
    if (hasNewVisibleTuple) replaceUnqualifiedStarItems(stmt, numTableRefs);
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
    } else if (subqueryStmt.hasAggInfo() && subqueryStmt.getAggInfo().hasAggregateExprs()
          && !subqueryStmt.hasAnalyticInfo() && subqueryStmt.getHavingPred() == null) {
      boolLiteral = new BoolLiteral(!predicate.isNotExists());
    }
    return boolLiteral;
  }

  /**
   * Replace all BetweenPredicates containing subqueries with their
   * equivalent compound predicates from the expr tree rooted at 'expr'.
   * The modified expr tree is returned.
   */
  private static Expr replaceBetweenPredicates(Expr expr) {
    if (expr instanceof BetweenPredicate && expr.contains(Subquery.class)) {
      return ((BetweenPredicate)expr).getRewrittenPredicate();
    }
    for (int i = 0; i < expr.getChildren().size(); ++i) {
      expr.setChild(i, replaceBetweenPredicates(expr.getChild(i)));
    }
    return expr;
  }

  /**
   * Modifies in place an expr that contains a subquery by rewriting its
   * subquery stmt. The modified analyzed expr is returned.
   */
  private static Expr rewriteExpr(Expr expr, Analyzer analyzer)
      throws AnalysisException {
    // Extract the subquery and rewrite it.
    Subquery subquery = expr.getSubquery();
    Preconditions.checkNotNull(subquery);
    rewriteSelectStatement((SelectStmt) subquery.getStatement(), subquery.getAnalyzer());
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
   * Merge an expr containing a subquery with a SelectStmt 'stmt' by
   * converting the subquery stmt of the former into an inline view and
   * creating a join between the new inline view and the right-most table
   * from 'stmt'. Return true if the rewrite introduced a new visible tuple
   * due to a CROSS JOIN or a LEFT OUTER JOIN.
   *
   * This process works as follows:
   * 1. Create a new inline view with the subquery as the view's stmt. Changes
   *    made to the subquery's stmt will affect the inline view.
   * 2. Extract all correlated predicates from the subquery's WHERE
   *    clause; the subquery's select list may be extended with new items and a
   *    GROUP BY clause may be added.
   * 3. Add the inline view to stmt's tableRefs and create a
   *    join (left semi join, anti-join, left outer join for agg functions
   *    that return a non-NULL value for an empty input, or cross-join) with
   *    stmt's right-most table.
   * 4. Initialize the ON clause of the new join from the original subquery
   *    predicate and the new inline view.
   * 5. Apply expr substitutions such that the extracted correlated predicates
   *    refer to columns of the new inline view.
   * 6. Add all extracted correlated predicates to the ON clause.
   */
  private static boolean mergeExpr(SelectStmt stmt, Expr expr,
      Analyzer analyzer) throws AnalysisException {
    Preconditions.checkNotNull(expr);
    Preconditions.checkNotNull(analyzer);
    boolean updateSelectList = false;

    SelectStmt subqueryStmt = (SelectStmt)expr.getSubquery().getStatement();
    // Create a new inline view from the subquery stmt. The inline view will be added
    // to the stmt's table refs later. Explicitly set the inline view's column labels
    // to eliminate any chance that column aliases from the parent query could reference
    // select items from the inline view after the rewrite.
    List<String> colLabels = Lists.newArrayList();
    for (int i = 0; i < subqueryStmt.getColLabels().size(); ++i) {
      colLabels.add(subqueryStmt.getColumnAliasGenerator().getNextAlias());
    }
    InlineViewRef inlineView = new InlineViewRef(
        stmt.getTableAliasGenerator().getNextAlias(), subqueryStmt, colLabels);

    // Extract all correlated predicates from the subquery.
    List<Expr> onClauseConjuncts = extractCorrelatedPredicates(subqueryStmt);
    if (!onClauseConjuncts.isEmpty()) {
      canRewriteCorrelatedSubquery(expr, onClauseConjuncts);
      // For correlated subqueries that are eligible for rewrite by transforming
      // into a join, a LIMIT clause has no effect on the results, so we can
      // safely remove it.
      subqueryStmt.limitElement_ = new LimitElement(null, null);
    }

    // Update the subquery's select list and/or its GROUP BY clause by adding
    // exprs from the extracted correlated predicates.
    boolean updateGroupBy = expr.getSubquery().isScalarSubquery()
        || (expr instanceof ExistsPredicate
            && !subqueryStmt.getSelectList().isDistinct()
            && subqueryStmt.hasAggInfo());
    List<Expr> lhsExprs = Lists.newArrayList();
    List<Expr> rhsExprs = Lists.newArrayList();
    for (Expr conjunct: onClauseConjuncts) {
      updateInlineView(inlineView, conjunct, stmt.getTableRefIds(),
          lhsExprs, rhsExprs, updateGroupBy);
    }

    // Analyzing the inline view triggers reanalysis of the subquery's select statement.
    // However the statement is already analyzed and since statement analysis is not
    // idempotent, the analysis needs to be reset.
    inlineView.reset();
    inlineView.analyze(analyzer);
    inlineView.setLeftTblRef(stmt.tableRefs_.get(stmt.tableRefs_.size() - 1));
    stmt.tableRefs_.add(inlineView);
    JoinOperator joinOp = JoinOperator.LEFT_SEMI_JOIN;

    // Create a join conjunct from the expr that contains a subquery.
    Expr joinConjunct = createJoinConjunct(expr, inlineView, analyzer,
        !onClauseConjuncts.isEmpty());
    if (joinConjunct != null) {
      SelectListItem firstItem =
          ((SelectStmt) inlineView.getViewStmt()).getSelectList().getItems().get(0);
      if (!onClauseConjuncts.isEmpty() &&
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

    // Create the ON clause from the extracted correlated predicates.
    Expr onClausePredicate =
        CompoundPredicate.createConjunctivePredicate(onClauseConjuncts);

    if (onClausePredicate == null) {
      Preconditions.checkState(expr instanceof ExistsPredicate);
      ExistsPredicate existsPred = (ExistsPredicate) expr;
      // Note that the concept of a 'correlated inline view' is similar but not the same
      // as a 'correlated subquery', i.e., a subquery with a correlated predicate.
      if (inlineView.isCorrelated()) {
        if (existsPred.isNotExists()) {
          inlineView.setJoinOp(JoinOperator.LEFT_ANTI_JOIN);
        } else {
          inlineView.setJoinOp(JoinOperator.LEFT_SEMI_JOIN);
        }
        // No visible tuples added.
        return false;
      } else {
        // TODO: Remove this when we support independent subquery evaluation.
        if (existsPred.isNotExists()) {
          throw new AnalysisException("Unsupported uncorrelated NOT EXISTS subquery: " +
              subqueryStmt.toSql());
        }
        // For uncorrelated subqueries, we limit the number of rows returned by the
        // subquery.
        subqueryStmt.setLimit(1);
        // We don't have an ON clause predicate to create an equi-join. Rewrite the
        // subquery using a CROSS JOIN.
        // TODO This is very expensive. Remove it when we implement independent
        // subquery evaluation.
        inlineView.setJoinOp(JoinOperator.CROSS_JOIN);
        LOG.warn("uncorrelated subquery rewritten using a cross join");
        // Indicate that new visible tuples may be added in stmt's select list.
        return true;
      }
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
      throw new AnalysisException("Unsupported correlated subquery: " +
          subqueryStmt.toSql());
    }

    // Check if we have a valid ON clause for an equi-join.
    boolean hasEqJoinPred = false;
    for (Expr conjunct: onClausePredicate.getConjuncts()) {
      if (!(conjunct instanceof BinaryPredicate)) continue;
      BinaryPredicate.Operator operator = ((BinaryPredicate) conjunct).getOp();
      if (!operator.isEquivalence()) continue;
      List<TupleId> lhsTupleIds = Lists.newArrayList();
      conjunct.getChild(0).getIds(lhsTupleIds, null);
      if (lhsTupleIds.isEmpty()) continue;
      List<TupleId> rhsTupleIds = Lists.newArrayList();
      conjunct.getChild(1).getIds(rhsTupleIds, null);
      if (rhsTupleIds.isEmpty()) continue;
      // Check if columns from the outer query block (stmt) appear in both sides
      // of the binary predicate.
      if ((lhsTupleIds.contains(inlineView.getDesc().getId()) && lhsTupleIds.size() > 1)
          || (rhsTupleIds.contains(inlineView.getDesc().getId())
              && rhsTupleIds.size() > 1)) {
        continue;
      }
      hasEqJoinPred = true;
      break;
    }

    if (!hasEqJoinPred && !inlineView.isCorrelated()) {
      // TODO: Remove this when independent subquery evaluation is implemented.
      // TODO: Requires support for non-equi joins.
      boolean hasGroupBy = ((SelectStmt) inlineView.getViewStmt()).hasGroupByClause();
      if (!expr.getSubquery().isScalarSubquery() ||
          (!(hasGroupBy && stmt.selectList_.isDistinct()) && hasGroupBy)) {
        throw new AnalysisException("Unsupported predicate with subquery: " +
            expr.toSql());
      }

      // TODO: Requires support for null-aware anti-join mode in nested-loop joins
      if (expr.getSubquery().isScalarSubquery() && expr instanceof InPredicate
          && ((InPredicate) expr).isNotIn()) {
        throw new AnalysisException("Unsupported NOT IN predicate with subquery: " +
            expr.toSql());
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
    if (expr instanceof InPredicate && ((InPredicate)expr).isNotIn() ||
        expr instanceof ExistsPredicate && ((ExistsPredicate)expr).isNotExists()) {
      // For the case of a NOT IN with an eq join conjunct, replace the join
      // conjunct with a conjunct that uses the null-matching eq operator.
      if (expr instanceof InPredicate) {
        joinOp = JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
        List<TupleId> tIds = Lists.newArrayList();
        joinConjunct.getIds(tIds, null);
        if (tIds.size() <= 1 || !tIds.contains(inlineView.getDesc().getId())) {
          throw new AnalysisException("Unsupported NOT IN predicate with subquery: " +
              expr.toSql());
        }
        // Replace the EQ operator in the generated join conjunct with a
        // null-matching EQ operator.
        for (Expr conjunct: onClausePredicate.getConjuncts()) {
          if (conjunct.equals(joinConjunct)) {
            Preconditions.checkState(conjunct instanceof BinaryPredicate);
            BinaryPredicate binaryPredicate = (BinaryPredicate)conjunct;
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
   * in stmt. 'tableIndx' indicates the maximum tableRef ordinal to consider when
   * replacing an unqualified star item.
   */
  private static void replaceUnqualifiedStarItems(SelectStmt stmt, int tableIdx) {
    Preconditions.checkState(tableIdx < stmt.tableRefs_.size());
    ArrayList<SelectListItem> newItems = Lists.newArrayList();
    for (int i = 0; i < stmt.selectList_.getItems().size(); ++i) {
      SelectListItem item = stmt.selectList_.getItems().get(i);
      if (!item.isStar() || item.getRawPath() != null) {
        newItems.add(item);
        continue;
      }
      // '*' needs to be replaced by tbl1.*,...,tbln.*, where
      // tbl1,...,tbln are the visible tableRefs in stmt.
      for (int j = 0; j < tableIdx; ++j) {
        TableRef tableRef = stmt.tableRefs_.get(j);
        if (tableRef.getJoinOp() == JoinOperator.LEFT_SEMI_JOIN ||
            tableRef.getJoinOp() == JoinOperator.LEFT_ANTI_JOIN) {
          continue;
        }
        newItems.add(SelectListItem.createStarItem(
            Lists.newArrayList(tableRef.getUniqueAlias())));
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
    for (Expr conjunct: expr.getConjuncts()) {
      if (!Expr.IS_TRUE_LITERAL.apply(conjunct)) return false;
    }
    return true;
  }

  /**
   * Extract all correlated predicates of a subquery.
   *
   * TODO Handle correlated predicates in a HAVING clause.
   */
  private static ArrayList<Expr> extractCorrelatedPredicates(SelectStmt subqueryStmt)
      throws AnalysisException {
    List<TupleId> subqueryTupleIds = subqueryStmt.getTableRefIds();
    ArrayList<Expr> correlatedPredicates = Lists.newArrayList();

    if (subqueryStmt.hasWhereClause()) {
      if (!canExtractCorrelatedPredicates(subqueryStmt.getWhereClause(),
          subqueryTupleIds)) {
        throw new AnalysisException("Disjunctions with correlated predicates " +
            "are not supported: " + subqueryStmt.getWhereClause().toSql());
      }
      // Extract the correlated predicates from the subquery's WHERE clause and
      // replace them with true BoolLiterals.
      Expr newWhereClause = extractCorrelatedPredicates(subqueryStmt.getWhereClause(),
          subqueryTupleIds, correlatedPredicates);
      if (canEliminate(newWhereClause)) newWhereClause = null;
      subqueryStmt.setWhereClause(newWhereClause);
    }

    // Process all correlated predicates from subquery's ON clauses.
    for (TableRef tableRef: subqueryStmt.getTableRefs()) {
      if (tableRef.getOnClause() == null) continue;

      ArrayList<Expr> onClauseCorrelatedPreds = Lists.newArrayList();
      Expr newOnClause = extractCorrelatedPredicates(tableRef.getOnClause(),
          subqueryTupleIds, onClauseCorrelatedPreds);
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
      ArrayList<Expr> matches) {
    if (isCorrelatedPredicate(root, tupleIds)) {
      matches.add(root);
      return new BoolLiteral(true);
    }
    for (int i = 0; i < root.getChildren().size(); ++i) {
      root.getChildren().set(i, extractCorrelatedPredicates(root.getChild(i), tupleIds,
          matches));
    }
    return root;
  }

  /**
   * Checks if an expr containing a correlated subquery is eligible for rewrite by
   * tranforming into a join. 'correlatedPredicates' contains the correlated
   * predicates identified in the subquery. Throws an AnalysisException if 'expr'
   * is not eligible for rewrite.
   * TODO: Merge all the rewrite eligibility tests into a single function.
   */
  private static void canRewriteCorrelatedSubquery(Expr expr,
      List<Expr> correlatedPredicates) throws AnalysisException {
    Preconditions.checkNotNull(expr);
    Preconditions.checkNotNull(correlatedPredicates);
    Preconditions.checkState(expr.contains(Subquery.class));
    SelectStmt stmt = (SelectStmt) expr.getSubquery().getStatement();
    Preconditions.checkNotNull(stmt);
    // Grouping and/or aggregation is not allowed on correlated scalar and IN subqueries
    if ((expr instanceof BinaryPredicate
          && (stmt.hasGroupByClause() || stmt.hasAnalyticInfo()))
        || (expr instanceof InPredicate
            && (stmt.hasAggInfo() || stmt.hasAnalyticInfo()))) {
      throw new AnalysisException("Unsupported correlated subquery with grouping " +
          "and/or aggregation: " + stmt.toSql());
    }

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
        && (!stmt.hasAggInfo()
            || !Iterables.all(correlatedPredicates,
                Predicates.or(Expr.IS_EQ_BINARY_PREDICATE, isSingleSlotRef)))) {
      throw new AnalysisException("Unsupported correlated EXISTS subquery with a " +
          "HAVING clause: " + stmt.toSql());
    }

    // The following correlated subqueries with a limit clause are supported:
    // 1. EXISTS subqueries
    // 2. Scalar subqueries with aggregation
    if (stmt.hasLimit() &&
        (!(expr instanceof BinaryPredicate) || !stmt.hasAggInfo() ||
         stmt.selectList_.isDistinct()) &&
        !(expr instanceof ExistsPredicate)) {
      throw new AnalysisException("Unsupported correlated subquery with a " +
          "LIMIT clause: " + stmt.toSql());
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
    SelectStmt stmt = (SelectStmt)inlineView.getViewStmt();
    List<TupleId> subqueryTblIds = stmt.getTableRefIds();
    ArrayList<Expr> groupByExprs = null;
    if (updateGroupBy) groupByExprs = Lists.newArrayList();

    List<SelectListItem> items = stmt.selectList_.getItems();
    // Collect all the SlotRefs from 'expr' and identify those that are bound by
    // subquery tuple ids.
    ArrayList<Expr> slotRefs = Lists.newArrayList();
    expr.collectAll(Predicates.instanceOf(SlotRef.class), slotRefs);
    List<Expr> exprsBoundBySubqueryTids = Lists.newArrayList();
    for (Expr slotRef: slotRefs) {
      if (slotRef.isBoundByTupleIds(subqueryTblIds)) {
        exprsBoundBySubqueryTids.add(slotRef);
      }
    }
    // The correlated predicate only references slots from a parent block,
    // no need to update the subquery's select or group by list.
    if (exprsBoundBySubqueryTids.isEmpty()) return;
    if (updateGroupBy) {
      Preconditions.checkState(expr instanceof BinaryPredicate);
      Expr exprBoundBySubqueryTids = null;
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
    for (Expr boundExpr: exprsBoundBySubqueryTids) {
      String colAlias = stmt.getColumnAliasGenerator().getNextAlias();
      items.add(new SelectListItem(boundExpr, null));
      inlineView.getExplicitColLabels().add(colAlias);
      lhsExprs.add(boundExpr);
      rhsExprs.add(new SlotRef(Lists.newArrayList(inlineView.getUniqueAlias(), colAlias)));
      if (groupByExprs != null) groupByExprs.add(boundExpr);
    }

    // Update the subquery's select list.
    boolean isDistinct = stmt.selectList_.isDistinct();
    stmt.selectList_ = new SelectList(
        items, isDistinct, stmt.selectList_.getPlanHints());
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
    for (Expr child: expr.getChildren()) {
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
  private static boolean containsCorrelatedPredicate(Expr root, List<TupleId> tupleIds) {
    if (isCorrelatedPredicate(root, tupleIds)) return true;
    for (Expr child: root.getChildren()) {
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
    return (expr instanceof BinaryPredicate || expr instanceof SlotRef)
        && !expr.isBoundByTupleIds(tupleIds);
  }

  /**
   * Converts an expr containing a subquery into an analyzed conjunct to be
   * used in a join. The conversion is performed in place by replacing the
   * subquery with the first expr from the select list of 'inlineView'.
   * If 'isCorrelated' is true and the first expr from the inline view contains
   * an aggregate function that returns non-null on an empty input,
   * the aggregate function is wrapped into a 'zeroifnull' function.
   */
  private static Expr createJoinConjunct(Expr exprWithSubquery, InlineViewRef inlineView,
      Analyzer analyzer, boolean isCorrelated) throws AnalysisException {
    Preconditions.checkNotNull(exprWithSubquery);
    Preconditions.checkNotNull(inlineView);
    Preconditions.checkState(exprWithSubquery.contains(Subquery.class));
    if (exprWithSubquery instanceof ExistsPredicate) return null;
    // Create a SlotRef from the first item of inlineView's select list
    SlotRef slotRef = new SlotRef(Lists.newArrayList(
        inlineView.getUniqueAlias(), inlineView.getColLabels().get(0)));
    slotRef.analyze(analyzer);
    Expr subquerySubstitute = slotRef;
    if (exprWithSubquery instanceof InPredicate) {
      BinaryPredicate pred = new BinaryPredicate(BinaryPredicate.Operator.EQ,
          exprWithSubquery.getChild(0), slotRef);
      pred.analyze(analyzer);
      return pred;
    }
    // Only scalar subqueries are supported
    Subquery subquery = exprWithSubquery.getSubquery();
    if (!subquery.isScalarSubquery()) {
      throw new AnalysisException("Unsupported predicate with a non-scalar subquery: "
          + subquery.toSql());
    }
    ExprSubstitutionMap smap = new ExprSubstitutionMap();
    SelectListItem item =
      ((SelectStmt) inlineView.getViewStmt()).getSelectList().getItems().get(0);
    if (isCorrelated && !item.getExpr().contains(Expr.IS_BUILTIN_AGG_FN)) {
      throw new AnalysisException("UDAs are not supported in the select list of " +
          "correlated subqueries: " + subquery.toSql());
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

      List<Expr> aggFns = Lists.newArrayList();
      item.getExpr().collectAll(Expr.NON_NULL_EMPTY_AGG, aggFns);
      // TODO Generalize this by making the aggregate functions aware of the
      // literal expr that they return on empty input, e.g. max returns a
      // NullLiteral whereas count returns a NumericLiteral.
      if (((FunctionCallExpr)aggFns.get(0)).getReturnType().isNumericType()) {
        FunctionCallExpr zeroIfNull = new FunctionCallExpr("zeroifnull",
            Lists.newArrayList((Expr) slotRef));
        zeroIfNull.analyze(analyzer);
        subquerySubstitute = zeroIfNull;
      } else if (((FunctionCallExpr)aggFns.get(0)).getReturnType().isStringType()) {
        List<Expr> params = Lists.newArrayList();
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
}
