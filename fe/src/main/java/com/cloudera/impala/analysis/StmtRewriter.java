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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext.AnalysisResult;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Class representing a statement rewriter. A statement rewriter performs subquery
 * unnesting on an analyzed parse tree.
 */
public class StmtRewriter {
  private final static Logger LOG = LoggerFactory.getLogger(StmtRewriter.class);

  /**
   * Rewrite the statement of an analysis result. The unanalyzed rewritten
   * statement is returned.
   */
  public static StatementBase rewrite(AnalysisResult analysisResult)
      throws AnalysisException {
    StatementBase rewrittenStmt = null;
    if (analysisResult.getStmt() instanceof SelectStmt) {
      SelectStmt analyzedStmt = (SelectStmt)analysisResult.getStmt();
      rewriteStatement(analyzedStmt, analysisResult.getAnalyzer());
      rewrittenStmt = analyzedStmt.clone();
    } else if (analysisResult.getStmt() instanceof InsertStmt) {
      // For an InsertStmt, rewrites are performed during its analysis.
      // Clone the insert stmt to reset its analysis state.
      rewrittenStmt = ((InsertStmt)analysisResult.getStmt()).clone();
    } else if (analysisResult.getStmt() instanceof CreateTableAsSelectStmt) {
      // For a CTAS, rewrites are performed during its analysis.
      CreateTableAsSelectStmt ctasStmt =
          (CreateTableAsSelectStmt)analysisResult.getStmt();
      Preconditions.checkState(ctasStmt.getQueryStmt() instanceof SelectStmt);
      // Create a new CTAS from the original create statement and the
      // rewritten insert statement.
      Preconditions.checkNotNull(analysisResult.getTmpCreateTableStmt());
      rewrittenStmt = new CreateTableAsSelectStmt(analysisResult.getTmpCreateTableStmt(),
          ctasStmt.getQueryStmt().clone());
    } else {
      throw new AnalysisException("Unsupported statement containing subqueries: " +
          analysisResult.getStmt().toSql());
    }
    return rewrittenStmt;
  }

  /**
   * Rewrite all the subqueries of a SelectStmt in place. Subqueries
   * are currently supported in FROM and WHERE clauses. The rewrite is performed in
   * place and not in a clone of SelectStmt because it requires the stmt to be analyzed.
   */
  public static void rewriteStatement(SelectStmt stmt, Analyzer analyzer)
      throws AnalysisException {
    // Rewrite all the subqueries in the FROM clause.
    for (TableRef tblRef: stmt.tableRefs_) {
      if (!(tblRef instanceof InlineViewRef)) continue;
      ((InlineViewRef)tblRef).rewrite();
    }
    // Rewrite all the subqueries in the WHERE clause.
    if (stmt.hasWhereClause()) {
      // Push negation to leaf operands.
      stmt.whereClause_ = Expr.pushNegationToOperands(stmt.whereClause_);
      // Check if we can rewrite the subqueries in the WHERE clause. OR predicates with
      // subqueries are not supported.
      if (!canRewriteSubqueries(stmt.whereClause_)) {
        throw new AnalysisException("Subqueries in OR predicates are not supported: " +
            stmt.whereClause_.toSql());
      }
      rewriteWhereClauseSubqueries(stmt, analyzer);
    }
    stmt.sqlString_ = null;
    LOG.info("rewritten stmt: " + stmt.toSql());
  }

  /**
   * Returns true if all subqueries in the Expr tree rooted at 'expr' can be
   * rewritten. A subquery can be rewritten if it is not part of a disjunction.
   */
  private static boolean canRewriteSubqueries(Expr expr) {
    if (!(expr instanceof CompoundPredicate)) return true;
    if (Expr.IS_OR_PREDICATE.apply(expr)) {
      return !expr.contains(Subquery.class);
    }
    for (Expr child: expr.getChildren()) {
      if (!canRewriteSubqueries(child)) return false;
    }
    return true;
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

      // Replace all the supported exprs with subqueries with true BoolLiterals
      // using an smap.
      BoolLiteral boolLiteral = new BoolLiteral(true);
      boolLiteral.analyze(analyzer);
      smap.put(conjunct, boolLiteral);
      exprsWithSubqueries.add(conjunct);
    }
    stmt.whereClause_ = stmt.whereClause_.substitute(smap, analyzer);

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
    rewriteStatement((SelectStmt)subquery.getStatement(), subquery.getAnalyzer());
    // Create a new Subquery with the rewritten stmt and use a substitution map
    // to replace the original subquery from the expr.
    Subquery newSubquery = new Subquery(subquery.getStatement().clone());
    newSubquery.analyze(analyzer);
    ExprSubstitutionMap smap = new ExprSubstitutionMap();
    smap.put(subquery, newSubquery);
    return expr.substitute(smap, analyzer);
  }

  /**
   * Merge an expr containing a subquery with a SelectStmt 'stmt' by
   * converting the subquery stmt of the former into an inline view and
   * creating a join between the new inline view and the right-most table
   * from 'stmt'. Return true if the rewrite introduced a new visible tuple
   * due to a CROSS JOIN or a LEFT OUTER JOIN.
   *
   * This process works as follows:
   * 1. Extract all correlated predicates from the subquery's WHERE
   *    clause; the subquery's select list may be extended with new items and a
   *    GROUP BY clause may be added.
   * 2. Create a new inline view using the transformed subquery stmt; the new
   *    inline view is analyzed.
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
    // Generate an alias for the new inline view.
    String inlineViewAlias = stmt.getTableAliasGenerator().getNextAlias();

    // Extract all correlated predicates from the subquery.
    List<Expr> onClauseConjuncts = extractCorrelatedPredicates(subqueryStmt);
    if (!onClauseConjuncts.isEmpty()) {
      // Check for correlated subqueries that are not eligible for rewrite by
      // transforming into a join.
      if ((expr instanceof BinaryPredicate
            && (subqueryStmt.hasGroupByClause() || subqueryStmt.hasAnalyticInfo()))
          || ((expr instanceof InPredicate || expr instanceof ExistsPredicate)
            && (subqueryStmt.hasAggInfo() || subqueryStmt.hasAnalyticInfo()))) {
        throw new AnalysisException("Unsupported correlated subquery with grouping " +
            "and/or aggregation: " + subqueryStmt.toSql());
      }
      if (subqueryStmt.hasLimit()) {
        if (expr instanceof BinaryPredicate) {
          // The limit does not affect the results of a non-grouping aggregate
          // subquery, so we can safely remove it.
          subqueryStmt.limitElement_ = null;
        } else {
          // For all other cases, throw an error.
          throw new AnalysisException("Unsupported correlated subquery with a LIMIT " +
              "clause: " + subqueryStmt.toSql());
        }
      }
    }

    // Update the subquery's select list and/or its GROUP BY clause by adding
    // SlotRefs from the extracted correlated predicates.
    boolean updateGroupBy = expr.getSubquery().isScalarSubquery();
    Map<Expr, Expr> exprMap = Maps.newHashMap();
    List<TupleId> subqueryTupleIds = subqueryStmt.getTableRefIds();
    for (Expr conjunct: onClauseConjuncts) {
      updateSubquery(subqueryStmt, conjunct, subqueryTupleIds, inlineViewAlias,
          exprMap, updateGroupBy);
    }

    if (expr instanceof ExistsPredicate) {
      // Modify the select list of an EXISTS subquery to avoid potential name
      // clashes with stmt's select list items in case we rewrite it using a
      // CROSS JOIN. Also, we reduce the overhead of computing the CROSS JOIN by
      // eliminating unecessary columns from the subquery's select list; for
      // uncorrelated subqueries, we limit the number of rows returned by the subquery.
      pruneSelectList(subqueryStmt);
      if (onClauseConjuncts.isEmpty()) subqueryStmt.setLimit(1);
    } else {
      // Create an alias for the first item of the subquery's select list; to be
      // used later for the construction of a join conjunct.
      List<SelectListItem> selectList = subqueryStmt.getSelectList().getItems();
      if (selectList.get(0).getAlias() == null) {
        selectList.get(0).setAlias(subqueryStmt.getColumnAliasGenerator().getNextAlias());
      }
    }

    // Create a new inline view from the modified subquery stmt and append it to
    // stmt's tableRefs.
    InlineViewRef inlineView = new InlineViewRef(inlineViewAlias, subqueryStmt.clone());
    inlineView.analyze(analyzer);
    inlineView.setLeftTblRef(stmt.tableRefs_.get(stmt.tableRefs_.size() - 1));
    stmt.tableRefs_.add(inlineView);
    JoinOperator joinOp = JoinOperator.LEFT_SEMI_JOIN;

    // Create a join conjunct from the expr that contains a subquery.
    Expr joinConjunct = expr.createJoinConjunct(inlineView, analyzer);
    if (joinConjunct != null) {
      if (expr instanceof BinaryPredicate && !onClauseConjuncts.isEmpty()) {
        com.google.common.base.Predicate<Expr> isCountAggFn =
          new com.google.common.base.Predicate<Expr>() {
            public boolean apply(Expr expr) {
              return expr instanceof FunctionCallExpr &&
                  ((FunctionCallExpr)expr).getFnName().getFunction().equals("count");
            }
          };

        // Correlated aggregate subqueries with 'count' are rewritten using
        // a LEFT OUTER JOIN because we need to ensure that there is one count
        // value for every tuple of 'stmt' (parent select block). The value of count
        // is 0 for every tuple of 'stmt' that gets rejected by the subquery
        // due to some predicate.
        //
        // TODO Handle other aggregate functions and UDAs that return a non-NULL value
        // on an empty set.
        SelectListItem item =
            ((SelectStmt)inlineView.getViewStmt()).getSelectList().getItems().get(0);
        if (item.getExpr().contains(isCountAggFn)) {
          FunctionCallExpr zeroIfNull = new FunctionCallExpr("zeroifnull",
              Lists.newArrayList(joinConjunct.getChild(1)));
          zeroIfNull.analyze(analyzer);
          Preconditions.checkState(joinConjunct instanceof BinaryPredicate);
          joinConjunct = new BinaryPredicate(((BinaryPredicate)joinConjunct).getOp(),
              joinConjunct.getChild(0), zeroIfNull);
          // Add the new join conjunct to stmt's WHERE clause because it needs
          // to be applied to the result of the LEFT OUTER JOIN (both matched
          // and unmatched tuples).
          stmt.whereClause_ =
              CompoundPredicate.addConjunct(joinConjunct, stmt.whereClause_);
          joinConjunct = null;
          joinOp = JoinOperator.LEFT_OUTER_JOIN;
          updateSelectList = true;
        }
      }

      if (joinConjunct != null) onClauseConjuncts.add(joinConjunct);
    }

    // Create the ON clause from the extracted correlated predicates.
    Expr onClausePredicate =
        CompoundPredicate.createConjunctivePredicate(onClauseConjuncts);

    if (onClausePredicate == null) {
      Preconditions.checkState(expr instanceof ExistsPredicate);
      // TODO: Remove this when we support independent subquery evaluation.
      if (((ExistsPredicate)expr).isNotExists()) {
        throw new AnalysisException("Unsupported uncorrelated NOT EXISTS subquery: " +
            subqueryStmt.toSql());
      }
      // We don't have an ON clause predicate to create an equi-join. Rewrite the
      // subquery using a CROSS JOIN.
      // TODO This is very expensive. Remove it when we implement independent
      // subquery evaluation.
      inlineView.setJoinOp(JoinOperator.CROSS_JOIN);
      if (stmt.hasAggInfo()) {
        throw new AnalysisException("Unsupported uncorrelated subquery in statement " +
            "with aggregate functions or GROUP BY: " + stmt.toSql());
      }
      LOG.warn("uncorrelated subquery rewritten using a cross join");
      // Indicate that new visible tuples may be added in stmt's select list.
      return true;
    }

    // Create an smap from the original select-list exprs of the select list to
    // the corresponding inline-view columns.
    ExprSubstitutionMap smap = new ExprSubstitutionMap();
    for (Map.Entry<Expr, Expr> entry: exprMap.entrySet()) {
      entry.getValue().analyze(analyzer);
      smap.put(entry.getKey(), entry.getValue());
    }
    onClausePredicate = onClausePredicate.substitute(smap, analyzer);

    // Check for references to ancestor query blocks (cycles in the dependency
    // graph of query blocks are not supported).
    if (!onClausePredicate.isBoundByTupleIds(stmt.getTableRefIds())) {
      throw new AnalysisException("Unsupported correlated subquery: " +
          subqueryStmt.toSql());
    }

    // Check if we have a valid ON clause for an equi-join.
    boolean hasEqJoinPred = false;
    for (Expr conjunct: onClausePredicate.getConjuncts()) {
      if (!(conjunct instanceof BinaryPredicate) ||
          ((BinaryPredicate)conjunct).getOp() != BinaryPredicate.Operator.EQ) {
        continue;
      }
      List<TupleId> tupleIds = Lists.newArrayList();
      conjunct.getIds(tupleIds, null);
      if (tupleIds.size() > 1 && tupleIds.contains(inlineView.getDesc().getId())) {
        hasEqJoinPred = true;
        break;
      }
    }

    if (!hasEqJoinPred) {
      // TODO: Remove this when independent subquery evaluation is implemented.
      // TODO: Requires support for non-equi joins.
      if (!(expr.getSubquery().isScalarSubquery()) || onClauseConjuncts.size() != 1) {
        throw new AnalysisException("Unsupported correlated subquery: " +
            subqueryStmt.toSql());
      }

      // We can rewrite the aggregate subquery using a cross join. All conjuncts
      // that were extracted from the subquery are added to stmt's WHERE clause.
      stmt.whereClause_ =
          CompoundPredicate.addConjunct(onClausePredicate, stmt.whereClause_);
      inlineView.setJoinOp(JoinOperator.CROSS_JOIN);
      // Indicate that the CROSS JOIN may add a new visible tuple to stmt's
      // select list (if the latter contains an unqualified star item '*')
      return true;
    }

    // We have a valid equi-join conjunct.
    if (expr instanceof InPredicate && ((InPredicate)expr).isNotIn() ||
        expr instanceof ExistsPredicate && ((ExistsPredicate)expr).isNotExists()) {
      joinOp = JoinOperator.LEFT_ANTI_JOIN;
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
      if (!item.isStar() || item.getTblName() != null) {
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
        newItems.add(SelectListItem.createStarItem(tableRef.getAliasAsName()));
      }
    }
    Preconditions.checkState(!newItems.isEmpty());
    boolean isDistinct = stmt.selectList_.isDistinct();
    stmt.selectList_ =
        new SelectList(newItems, isDistinct, stmt.selectList_.getPlanHints());
  }

  /**
   * Prune stmt's select list by keeping the minimum set of items. Star items
   * are replaced by a constant expr. An alias is generated for all other (non-star)
   * items. We need to maintain non-star items because the select list may
   * have been expanded with new items from correlated predicates during
   * subquery rewrite.
   */
  private static void pruneSelectList(SelectStmt stmt) throws AnalysisException {
    ArrayList<SelectListItem> newItems = Lists.newArrayList();
    boolean replacedStarItem = false;
    for (int i = 0; i < stmt.selectList_.getItems().size(); ++i) {
      SelectListItem item = stmt.selectList_.getItems().get(i);
      if (item.isStar()) {
        if (replacedStarItem) continue;
        newItems.add(new SelectListItem(new NumericLiteral(Long.toString(1),
            Type.BIGINT), stmt.getColumnAliasGenerator().getNextAlias()));
        replacedStarItem = true;
        continue;
      }
      if (item.getAlias() == null) {
        newItems.add(new SelectListItem(item.getExpr(),
            stmt.getColumnAliasGenerator().getNextAlias()));
      } else {
        newItems.add(new SelectListItem(item.getExpr(), item.getAlias()));
      }
    }
    boolean isDistinct = stmt.selectList_.isDistinct();
    stmt.selectList_ = new SelectList(
        newItems, isDistinct, stmt.selectList_.getPlanHints());
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
   * Update a subquery by expanding its select list with SlotRefs from 'expr'.
   * If 'updateGroupBy' is true, the SlotRefs from 'expr' are also added in
   * stmt's GROUP BY clause. 'expr' is a correlated predicate from the
   * subquery that will be 'moved' to an ON clause in the subquery's parent query
   * block. Hence, we need to make sure that every SlotRef of 'expr' originating from
   * the subquery references an item in the subquery's select list.
   */
  private static void updateSubquery(SelectStmt stmt, Expr expr,
      List<TupleId> subqueryTblIds, String inlineViewAlias, Map<Expr, Expr> exprMap,
      boolean updateGroupBy) {
    ArrayList<Expr> groupByExprs = null;
    if (updateGroupBy) groupByExprs = Lists.newArrayList();
    ArrayList<Expr> slotRefs = Lists.newArrayList();
    expr.collectAll(Predicates.instanceOf(SlotRef.class), slotRefs);
    List<SelectListItem> items = stmt.selectList_.getItems();
    for (Expr slotRef: slotRefs) {
      // If the slotRef belongs to the subquery, add it to the select list and
      // register it for substitution. We use a temporary substitution map
      // because we cannot at this point analyze the new select list expr. Once
      // the new inline view is analyzed, the entries from this map will be
      // added to an ExprSubstitutionMap.
      if (slotRef.isBoundByTupleIds(subqueryTblIds)) {
        String colAlias = stmt.getColumnAliasGenerator().getNextAlias();
        items.add(new SelectListItem(slotRef, colAlias));
        exprMap.put(slotRef, new SlotRef(new TableName(null, inlineViewAlias), colAlias));
        if (groupByExprs != null) groupByExprs.add(slotRef);
      }
    }

    // Update the subquery's select list.
    boolean isDistinct = stmt.selectList_.isDistinct();
    Preconditions.checkState(!isDistinct);
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
}
