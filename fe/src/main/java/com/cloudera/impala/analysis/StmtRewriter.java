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

import com.cloudera.impala.analysis.AnalysisContext.AnalysisResult;
import com.cloudera.impala.analysis.BinaryPredicate.Operator;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * Rewrite all subqueries of a stmt's WHERE clause. Initially, all the subquery
   * predicates are extracted from the WHERE clause and are replaced with
   * true BoolLiterals. Subsequently, each subquery predicate is
   * merged into its parent select block by converting it into a join.
   * Subquery predicates that themselves contain subquery predicates are
   * recursively rewritten in a bottom up fashion.
   *
   * The following example illustrates the bottom up rewriting of subquery
   * predicates (nested queries). Suppose we have the following three level
   * nested query Q0:
   *
   * SELECT *
   * FROM T1                                            : Q0
   * WHERE T1.a IN (SELECT a
   *                FROM T2 WHERE T2.b IN (SELECT b
   *                                       FROM T3))
   * AND T1.c < 10;
   *
   * This query will be rewritten as follows. Initially, the subquery predicate
   * T1.a IN (SELECT a FROM T2 WHERE T2.b IN (SELECT b FROM T3)) is extracted
   * from the top level block (Q0) and is replaced by a true BoolLiteral,
   * resulting in the following query Q1:
   *
   * SELECT * FROM T1 WHERE TRUE : Q1
   *
   * Since the stmt in the subquery predicate contains a subquery
   * predicate, it is also rewritten. As before, rewriting stmt SELECT a FROM T2
   * WHERE T2.b IN (SELECT b FROM T3) works by first extracting the subquery predicate
   * (T2.b IN (SELECT b FROM T3)) and substituting it with a true BoolLiteral,
   * producing the following stmt Q2:
   *
   * SELECT a FROM T2 WHERE TRUE : Q2
   *
   * The subquery predicate T2.b IN (SELECT b FROM T3) is then merged with Q2,
   * producing the following unnested query Q3:
   *
   * SELECT a FROM T2 LEFT SEMI JOIN (SELECT b FROM T3) $a$1 ON T2.b = $a$1.b : Q3
   *
   * The extracted subquery predicate becomes:
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
    ArrayList<Expr> subqueryPredicates = Lists.newArrayList();
    stmt.whereClause_.collectAll(Expr.IS_SUBQUERY_PREDICATE, subqueryPredicates);
    // Replace all subquery predicates with true BoolLiterals using an smap.
    ExprSubstitutionMap smap = new ExprSubstitutionMap();
    for (Expr subqueryPred: subqueryPredicates) {
      BoolLiteral boolLiteral = new BoolLiteral(true);
      boolLiteral.analyze(analyzer);
      smap.put(subqueryPred, boolLiteral);
    }
    stmt.whereClause_ = stmt.whereClause_.substitute(smap, analyzer);

    boolean hasNewVisibleTuple = false;
    // Recursively rewrite all subquery predicates and merge them with 'stmt'.
    for (Expr pred: subqueryPredicates) {
      Preconditions.checkState(pred instanceof Predicate);
      if (mergeSubqueryPredicate(stmt, rewriteSubqueryPredicate((Predicate)pred, analyzer),
          analyzer)) {
        hasNewVisibleTuple = true;
      }
    }
    if (canEliminate(stmt.whereClause_)) stmt.whereClause_ = null;
    if (hasNewVisibleTuple) replaceUnqualifiedStarItems(stmt, numTableRefs);
  }

  /**
   * Modifies a subquery predicate in place by rewriting its subquery stmt.
   * The modified subquery predicate is returned.
   */
  private static Predicate rewriteSubqueryPredicate(Predicate pred, Analyzer analyzer)
      throws AnalysisException {
    Preconditions.checkState(pred.isSubqueryPredicate());
    // Extract the subquery from this predicate and rewrite it.
    Subquery subquery = pred.getSubquery();
    Preconditions.checkNotNull(subquery);
    rewriteStatement((SelectStmt)subquery.getStatement(), subquery.getAnalyzer());
    // Create a new Subquery with the rewritten stmt and use a substitution map
    // to replace the original subquery from the subquery predicate.
    Subquery newSubquery = new Subquery(subquery.getStatement().clone());
    newSubquery.analyze(analyzer);
    ExprSubstitutionMap smap = new ExprSubstitutionMap();
    smap.put(subquery, newSubquery);
    return (Predicate)pred.substitute(smap, analyzer);
  }

  /**
   * Merge a subquery predicate 'pred' with a SelectStmt 'stmt' by converting
   * the former into an inline view and creating a join between the new inline
   * view and the right-most table from 'stmt'. Return true if the rewrite
   * introduced a new visible tuple due to a CROSS JOIN or a LEFT OUTER JOIN.
   *
   * This process works as follows:
   * 1. Extract all correlated predicates from the subquery's WHERE
   *    clause; the subquery's select list may be extended with new items and a
   *    GROUP BY clause may be added.
   * 2. Create a new inline view using the transformed subquery stmt; the new
   *    inline view is analyzed.
   * 3. Add the inline view to stmt's tableRefs and create a
   *    join (left semi join, anti-join or left outer join for agg functions
   *    that return a non-NULL value for an empty input) with stmt's right-most
   *    table.
   * 4. Initialize the ON clause of the new join from the original subquery
   *    predicate and the new inline view.
   * 5. Apply expr substitutions such that the extracted correlated predicates
   *    refer to columns of the new inline view.
   * 6. Add all extracted correlated predicates to the ON clause.
   */
  private static boolean mergeSubqueryPredicate(SelectStmt stmt, Predicate pred,
      Analyzer analyzer) throws AnalysisException {
    Preconditions.checkNotNull(pred);
    Preconditions.checkNotNull(analyzer);
    boolean updateSelectList = false;
    List<TupleId> stmtTupleIds = stmt.getTableRefIds();

    SelectStmt subqueryStmt = (SelectStmt)pred.getSubquery().getStatement();
    // Generate an alias for the new inline view.
    String inlineViewAlias = stmt.getTableAliasGenerator().getNextAlias();

    // Extract all correlated predicates from the subquery.
    List<Expr> onClauseConjuncts = extractCorrelatedPredicates(subqueryStmt);
    if (!onClauseConjuncts.isEmpty() && !(pred instanceof BinaryPredicate)) {
      // Correlated EXISTS and IN subqueries that contain the following clauses
      // are not eligible for rewriting:
      // - GROUP BY
      // - Aggregate functions
      // - DISTINCT
      if (subqueryStmt.hasAggInfo()) {
        throw new AnalysisException("Unsupported correlated subquery with grouping " +
            "and/or aggregation: " + subqueryStmt.toSql());
      }
    }

    // Update the subquery's select list and/or its GROUP BY clause by adding
    // SlotRefs from the extracted correlated predicates.
    boolean updateGroupBy = pred instanceof BinaryPredicate;
    Map<Expr, Expr> exprMap = Maps.newHashMap();
    List<TupleId> subqueryTupleIds = subqueryStmt.getTableRefIds();
    for (Expr conjunct: onClauseConjuncts) {
      updateSubquery(subqueryStmt, conjunct, subqueryTupleIds, inlineViewAlias,
          exprMap, updateGroupBy);
    }

    // Create an alias for the first item of the subquery's select list; to be
    // used later for the construction of a join conjunct.
    if (!(pred instanceof ExistsPredicate)) {
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

    // Create a join conjunct from the comparison expr of the subquery predicate and
    // the first expr from the subquery's select list.
    Expr joinConjunct = pred.createJoinConjunct(inlineView);
    if (joinConjunct != null) {
      if (pred instanceof BinaryPredicate && !onClauseConjuncts.isEmpty()) {
        com.google.common.base.Predicate<Expr> isCountAggFn =
          new com.google.common.base.Predicate<Expr>() {
            public boolean apply(Expr expr) {
              return expr instanceof FunctionCallExpr &&
                  ((FunctionCallExpr)expr).getFnName().getFunction().equals("count");
            }
          };

        // Correlated aggregate subquery predicates with 'count' are rewritten using
        // a LEFT OUTER JOIN because we need to ensure that there is one count
        // value for every tuple in the parent select block. For every tuple in
        // the outer select block that gets rejected by the subquery (due to
        // some predicate), the value of count is 0.
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

    // TODO Uncorrelared EXISTS are not supported.
    if (onClausePredicate == null) {
      Preconditions.checkState(pred instanceof ExistsPredicate);
      throw new AnalysisException("Unsupported uncorrelated EXISTS subquery: " +
          subqueryStmt.toSql());
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

    // Check if we have a valid ON clause.
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
      if (!(pred instanceof BinaryPredicate)) {
        // The subquery is not correlated if we have an invalid ON clause.
        throw new AnalysisException("Unsupported uncorrelated subquery: " +
            subqueryStmt.toSql());
      }

      // Uncorrelated aggregate subquery predicates are converted into a CROSS JOIN.
      // All conjuncts that were extracted from the subquery are added to stmt's
      // WHERE clause.
      stmt.whereClause_ =
          CompoundPredicate.addConjunct(onClausePredicate, stmt.whereClause_);
      inlineView.setJoinOp(JoinOperator.CROSS_JOIN);
      // Indicate that the CROSS JOIN may change the select list of 'stmt' (if the latter
      // contains a '*').
      return true;
    }

    if (pred instanceof InPredicate && ((InPredicate)pred).isNotIn() ||
        pred instanceof ExistsPredicate && ((ExistsPredicate)pred).isNotExists()) {
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
    boolean isStraightJoin = stmt.selectList_.isStraightJoin();
    stmt.selectList_ = new SelectList(newItems, isDistinct, isStraightJoin);
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
    ArrayList<SelectListItem> items = stmt.selectList_.getItems();
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
    boolean isStraightJoin = stmt.selectList_.isStraightJoin();
    boolean isDistinct = stmt.selectList_.isDistinct();
    Preconditions.checkState(!isDistinct);
    stmt.selectList_ = new SelectList(items, isDistinct, isStraightJoin);
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
