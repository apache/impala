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
import java.util.Set;

import org.apache.impala.analysis.Path.PathType;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ColumnAliasGenerator;
import org.apache.impala.common.TableAliasGenerator;
import org.apache.impala.common.TreeNode;
import org.apache.impala.rewrite.ExprRewriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Representation of a single select block, including GROUP BY, ORDER BY and HAVING
 * clauses.
 */
public class SelectStmt extends QueryStmt {
  private final static Logger LOG = LoggerFactory.getLogger(SelectStmt.class);

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  protected SelectList selectList_;
  protected final ArrayList<String> colLabels_; // lower case column labels
  protected final FromClause fromClause_;
  protected Expr whereClause_;
  protected ArrayList<Expr> groupingExprs_;
  protected Expr havingClause_;  // original having clause

  // havingClause with aliases and agg output resolved
  private Expr havingPred_;

  // set if we have any kind of aggregation operation, include SELECT DISTINCT
  private AggregateInfo aggInfo_;

  // set if we have AnalyticExprs in the select list/order by clause
  private AnalyticInfo analyticInfo_;

  // substitutes all exprs in this select block to reference base tables
  // directly
  private ExprSubstitutionMap baseTblSmap_ = new ExprSubstitutionMap();

  // END: Members that need to be reset()
  /////////////////////////////////////////

  // SQL string of this SelectStmt before inline-view expression substitution.
  // Set in analyze().
  protected String sqlString_;

  SelectStmt(SelectList selectList,
             FromClause fromClause,
             Expr wherePredicate, ArrayList<Expr> groupingExprs,
             Expr havingPredicate, ArrayList<OrderByElement> orderByElements,
             LimitElement limitElement) {
    super(orderByElements, limitElement);
    selectList_ = selectList;
    if (fromClause == null) {
      fromClause_ = new FromClause();
    } else {
      fromClause_ = fromClause;
    }
    whereClause_ = wherePredicate;
    groupingExprs_ = groupingExprs;
    havingClause_ = havingPredicate;
    colLabels_ = Lists.newArrayList();
    havingPred_ = null;
    aggInfo_ = null;
    sortInfo_ = null;
  }

  /**
   * @return the original select list items from the query
   */
  public SelectList getSelectList() { return selectList_; }

  /**
   * @return the HAVING clause post-analysis and with aliases resolved
   */
  public Expr getHavingPred() { return havingPred_; }

  @Override
  public List<String> getColLabels() { return colLabels_; }

  public List<TableRef> getTableRefs() { return fromClause_.getTableRefs(); }
  public boolean hasWhereClause() { return whereClause_ != null; }
  public boolean hasGroupByClause() { return groupingExprs_ != null; }
  public Expr getWhereClause() { return whereClause_; }
  public void setWhereClause(Expr whereClause) { whereClause_ = whereClause; }
  public AggregateInfo getAggInfo() { return aggInfo_; }
  public boolean hasAggInfo() { return aggInfo_ != null; }
  public AnalyticInfo getAnalyticInfo() { return analyticInfo_; }
  public boolean hasAnalyticInfo() { return analyticInfo_ != null; }
  public boolean hasHavingClause() { return havingClause_ != null; }
  public ExprSubstitutionMap getBaseTblSmap() { return baseTblSmap_; }

  // Column alias generator used during query rewriting.
  private ColumnAliasGenerator columnAliasGenerator_ = null;
  public ColumnAliasGenerator getColumnAliasGenerator() {
    if (columnAliasGenerator_ == null) {
      columnAliasGenerator_ = new ColumnAliasGenerator(colLabels_, null);
    }
    return columnAliasGenerator_;
  }

  // Table alias generator used during query rewriting.
  private TableAliasGenerator tableAliasGenerator_ = null;
  public TableAliasGenerator getTableAliasGenerator() {
    if (tableAliasGenerator_ == null) {
      tableAliasGenerator_ = new TableAliasGenerator(analyzer_, null);
    }
    return tableAliasGenerator_;
  }

  /**
   * Creates resultExprs and baseTblResultExprs.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);

    // Start out with table refs to establish aliases.
    fromClause_.analyze(analyzer);

    // Generate !empty() predicates to filter out empty collections.
    // Skip this step when analyzing a WITH-clause because CollectionTableRefs
    // do not register collection slots in their parent in that context
    // (see CollectionTableRef.analyze()).
    if (!analyzer.isWithClause()) registerIsNotEmptyPredicates(analyzer);

    // analyze plan hints from select list
    selectList_.analyzePlanHints(analyzer);

    // populate resultExprs_, aliasSmap_, and colLabels_
    for (int i = 0; i < selectList_.getItems().size(); ++i) {
      SelectListItem item = selectList_.getItems().get(i);
      if (item.isStar()) {
        if (item.getRawPath() != null) {
          Path resolvedPath = analyzeStarPath(item.getRawPath(), analyzer);
          expandStar(resolvedPath, analyzer);
        } else {
          expandStar(analyzer);
        }
      } else {
        // Analyze the resultExpr before generating a label to ensure enforcement
        // of expr child and depth limits (toColumn() label may call toSql()).
        item.getExpr().analyze(analyzer);
        if (item.getExpr().contains(Predicates.instanceOf(Subquery.class))) {
          throw new AnalysisException(
              "Subqueries are not supported in the select list.");
        }
        resultExprs_.add(item.getExpr());
        String label = item.toColumnLabel(i, analyzer.useHiveColLabels());
        SlotRef aliasRef = new SlotRef(label);
        Expr existingAliasExpr = aliasSmap_.get(aliasRef);
        if (existingAliasExpr != null && !existingAliasExpr.equals(item.getExpr())) {
          // If we have already seen this alias, it refers to more than one column and
          // therefore is ambiguous.
          ambiguousAliasList_.add(aliasRef);
        }
        aliasSmap_.put(aliasRef, item.getExpr().clone());
        colLabels_.add(label);
      }
    }

    // Star exprs only expand to the scalar-typed columns/fields, so
    // the resultExprs_ could be empty.
    if (resultExprs_.isEmpty()) {
      throw new AnalysisException("The star exprs expanded to an empty select list " +
          "because the referenced tables only have complex-typed columns.\n" +
          "Star exprs only expand to scalar-typed columns because complex-typed exprs " +
          "are currently not supported in the select list.\n" +
          "Affected select statement:\n" + toSql());
    }

    for (Expr expr: resultExprs_) {
      // Complex types are currently not supported in the select list because we'd need
      // to serialize them in a meaningful way.
      if (expr.getType().isComplexType()) {
        throw new AnalysisException(String.format(
            "Expr '%s' in select list returns a complex type '%s'.\n" +
            "Only scalar types are allowed in the select list.",
            expr.toSql(), expr.getType().toSql()));
      }
      if (!expr.getType().isSupported()) {
        throw new AnalysisException("Unsupported type '"
            + expr.getType().toSql() + "' in '" + expr.toSql() + "'.");
      }
    }

    if (TreeNode.contains(resultExprs_, AnalyticExpr.class)) {
      if (fromClause_.isEmpty()) {
        throw new AnalysisException("Analytic expressions require FROM clause.");
      }

      // do this here, not after analyzeAggregation(), otherwise the AnalyticExprs
      // will get substituted away
      if (selectList_.isDistinct()) {
        throw new AnalysisException(
            "cannot combine SELECT DISTINCT with analytic functions");
      }
    }

    if (whereClause_ != null) {
      whereClause_.analyze(analyzer);
      if (whereClause_.contains(Expr.isAggregatePredicate())) {
        throw new AnalysisException(
            "aggregate function not allowed in WHERE clause");
      }
      whereClause_.checkReturnsBool("WHERE clause", false);
      Expr e = whereClause_.findFirstOf(AnalyticExpr.class);
      if (e != null) {
        throw new AnalysisException(
            "WHERE clause must not contain analytic expressions: " + e.toSql());
      }
      analyzer.registerConjuncts(whereClause_, false);
    }

    createSortInfo(analyzer);
    analyzeAggregation(analyzer);
    createAnalyticInfo(analyzer);
    if (evaluateOrderBy_) createSortTupleInfo(analyzer);

    // Remember the SQL string before inline-view expression substitution.
    sqlString_ = toSql();
    resolveInlineViewRefs(analyzer);

    // If this block's select-project-join portion returns an empty result set and the
    // block has no aggregation, then mark this block as returning an empty result set.
    if (analyzer.hasEmptySpjResultSet() && aggInfo_ == null) {
      analyzer.setHasEmptyResultSet();
    }

    ColumnLineageGraph graph = analyzer.getColumnLineageGraph();
    if (aggInfo_ != null && !aggInfo_.getAggregateExprs().isEmpty()) {
      graph.addDependencyPredicates(aggInfo_.getGroupingExprs());
    }
    if (sortInfo_ != null && hasLimit()) {
      // When there is a LIMIT clause in conjunction with an ORDER BY, the ordering exprs
      // must be added in the column lineage graph.
      graph.addDependencyPredicates(sortInfo_.getOrderingExprs());
    }

    if (aggInfo_ != null) {
      if (LOG.isTraceEnabled()) LOG.trace("post-analysis " + aggInfo_.debugString());
    }
  }

  /**
   * Generates and registers !empty() predicates to filter out empty collections directly
   * in the parent scan of collection table refs. This is a performance optimization to
   * avoid the expensive processing of empty collections inside a subplan that would
   * yield an empty result set.
   *
   * For correctness purposes, the predicates are generated in cases where we can ensure
   * that they will be assigned only to the parent scan, and no other plan node.
   *
   * The conditions are as follows:
   * - collection table ref is relative and non-correlated
   * - collection table ref represents the rhs of an inner/cross/semi join
   * - collection table ref's parent tuple is not outer joined
   *
   * Example: table T has field A which is of type array<array<int>>.
   * 1) ... T join T.A a join a.item a_nest ... : all nodes on the path T -> a -> a_nest
   *                                              are required so are checked for !empty.
   * 2) ... T left outer join T.A a join a.item a_nest ... : no !empty.
   * 3) ... T join T.A a left outer join a.item a_nest ... : a checked for !empty.
   * 4) ... T left outer join T.A a left outer join a.item a_nest ... : no !empty.
   *
   *
   * TODO: In some cases, it is possible to generate !empty() predicates for a correlated
   * table ref, but in general, that is not correct for non-trivial query blocks.
   * For example, if the block with the correlated ref has an aggregation then adding a
   * !empty() predicate would incorrectly discard rows from the final result set.
   * TODO: Evaluating !empty() predicates at non-scan nodes interacts poorly with our BE
   * projection of collection slots. For example, rows could incorrectly be filtered if
   * a !empty() predicate is assigned to a plan node that comes after the unnest of the
   * collection that also performs the projection.
   */
  private void registerIsNotEmptyPredicates(Analyzer analyzer) throws AnalysisException {
    for (TableRef tblRef: fromClause_.getTableRefs()) {
      Preconditions.checkState(tblRef.isResolved());
      if (!(tblRef instanceof CollectionTableRef)) continue;
      CollectionTableRef ref = (CollectionTableRef) tblRef;
      // Skip non-relative and correlated refs.
      if (!ref.isRelative() || ref.isCorrelated()) continue;
      // Skip outer and anti joins.
      if (ref.getJoinOp().isOuterJoin() || ref.getJoinOp().isAntiJoin()) continue;
      // Do not generate a predicate if the parent tuple is outer joined.
      if (analyzer.isOuterJoined(ref.getResolvedPath().getRootDesc().getId())) continue;
      IsNotEmptyPredicate isNotEmptyPred =
          new IsNotEmptyPredicate(ref.getCollectionExpr().clone());
      isNotEmptyPred.analyze(analyzer);
      // Register the predicate as an On-clause conjunct because it should only
      // affect the result of this join and not the whole FROM clause.
      analyzer.registerOnClauseConjuncts(
          Lists.<Expr>newArrayList(isNotEmptyPred), ref);
    }
  }

  /**
   * Marks all unassigned join predicates as well as exprs in aggInfo and sortInfo.
   */
  @Override
  public void materializeRequiredSlots(Analyzer analyzer) {
    // Mark unassigned join predicates. Some predicates that must be evaluated by a join
    // can also be safely evaluated below the join (picked up by getBoundPredicates()).
    // Such predicates will be marked twice and that is ok.
    List<Expr> unassigned =
        analyzer.getUnassignedConjuncts(getTableRefIds(), true);
    List<Expr> unassignedJoinConjuncts = Lists.newArrayList();
    for (Expr e: unassigned) {
      if (analyzer.evalAfterJoin(e)) unassignedJoinConjuncts.add(e);
    }
    List<Expr> baseTblJoinConjuncts =
        Expr.substituteList(unassignedJoinConjuncts, baseTblSmap_, analyzer, false);
    materializeSlots(analyzer, baseTblJoinConjuncts);

    if (evaluateOrderBy_) {
      // mark ordering exprs before marking agg/analytic exprs because they could contain
      // agg/analytic exprs that are not referenced anywhere but the ORDER BY clause
      sortInfo_.materializeRequiredSlots(analyzer, baseTblSmap_);
    }

    if (hasAnalyticInfo()) {
      // Mark analytic exprs before marking agg exprs because they could contain agg
      // exprs that are not referenced anywhere but the analytic expr.
      // Gather unassigned predicates and mark their slots. It is not desirable
      // to account for propagated predicates because if an analytic expr is only
      // referenced by a propagated predicate, then it's better to not materialize the
      // analytic expr at all.
      ArrayList<TupleId> tids = Lists.newArrayList();
      getMaterializedTupleIds(tids); // includes the analytic tuple
      List<Expr> conjuncts = analyzer.getUnassignedConjuncts(tids, false);
      materializeSlots(analyzer, conjuncts);
      analyticInfo_.materializeRequiredSlots(analyzer, baseTblSmap_);
    }

    if (aggInfo_ != null) {
      // mark all agg exprs needed for HAVING pred and binding predicates as materialized
      // before calling AggregateInfo.materializeRequiredSlots(), otherwise they won't
      // show up in AggregateInfo.getMaterializedAggregateExprs()
      ArrayList<Expr> havingConjuncts = Lists.newArrayList();
      if (havingPred_ != null) havingConjuncts.add(havingPred_);
      // Ignore predicates bound to a group-by slot because those
      // are already evaluated below this agg node (e.g., in a scan).
      Set<SlotId> groupBySlots = Sets.newHashSet();
      for (int i = 0; i < aggInfo_.getGroupingExprs().size(); ++i) {
        groupBySlots.add(aggInfo_.getOutputTupleDesc().getSlots().get(i).getId());
      }
      // Binding predicates are assigned to the final output tuple of the aggregation,
      // which is the tuple of the 2nd phase agg for distinct aggs.
      ArrayList<Expr> bindingPredicates =
          analyzer.getBoundPredicates(aggInfo_.getResultTupleId(), groupBySlots, false);
      havingConjuncts.addAll(bindingPredicates);
      havingConjuncts.addAll(
          analyzer.getUnassignedConjuncts(aggInfo_.getResultTupleId().asList(), false));
      materializeSlots(analyzer, havingConjuncts);
      aggInfo_.materializeRequiredSlots(analyzer, baseTblSmap_);
    }
  }

  /**
    * Populates baseTblSmap_ with our combined inline view smap and creates
    * baseTblResultExprs.
    */
  protected void resolveInlineViewRefs(Analyzer analyzer)
      throws AnalysisException {
    // Gather the inline view substitution maps from the enclosed inline views
    for (TableRef tblRef: fromClause_) {
      if (tblRef instanceof InlineViewRef) {
        InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
        baseTblSmap_ =
            ExprSubstitutionMap.combine(baseTblSmap_, inlineViewRef.getBaseTblSmap());
      }
    }
    baseTblResultExprs_ =
        Expr.trySubstituteList(resultExprs_, baseTblSmap_, analyzer, false);
    if (LOG.isTraceEnabled()) {
      LOG.trace("baseTblSmap_: " + baseTblSmap_.debugString());
      LOG.trace("resultExprs: " + Expr.debugString(resultExprs_));
      LOG.trace("baseTblResultExprs: " + Expr.debugString(baseTblResultExprs_));
    }
  }

  public List<TupleId> getTableRefIds() {
    List<TupleId> result = Lists.newArrayList();
    for (TableRef ref: fromClause_) {
      result.add(ref.getId());
    }
    return result;
  }

  /**
   * Resolves the given raw path as a STAR path and checks its legality.
   * Returns the resolved legal path, or throws if the raw path could not
   * be resolved or is an illegal star path.
   */
  private Path analyzeStarPath(List<String> rawPath, Analyzer analyzer)
      throws AnalysisException {
    Path resolvedPath = null;
    try {
      resolvedPath = analyzer.resolvePath(rawPath, PathType.STAR);
    } catch (TableLoadingException e) {
      // Should never happen because we only check registered table aliases.
      Preconditions.checkState(false);
    }
    Preconditions.checkNotNull(resolvedPath);
    return resolvedPath;
  }

  /**
   * Expand "*" select list item, ignoring semi-joined tables as well as
   * complex-typed fields because those are currently illegal in any select
   * list (even for inline views, etc.)
   */
  private void expandStar(Analyzer analyzer) throws AnalysisException {
    if (fromClause_.isEmpty()) {
      throw new AnalysisException("'*' expression in select list requires FROM clause.");
    }
    // expand in From clause order
    for (TableRef tableRef: fromClause_) {
      if (analyzer.isSemiJoined(tableRef.getId())) continue;
      Path resolvedPath = new Path(tableRef.getDesc(), Collections.<String>emptyList());
      Preconditions.checkState(resolvedPath.resolve());
      expandStar(resolvedPath, analyzer);
    }
  }

  /**
   * Expand "path.*" from a resolved path, ignoring complex-typed fields because those
   * are currently illegal in any select list (even for inline views, etc.)
   */
  private void expandStar(Path resolvedPath, Analyzer analyzer)
      throws AnalysisException {
    Preconditions.checkState(resolvedPath.isResolved());
    if (resolvedPath.destTupleDesc() != null &&
        resolvedPath.destTupleDesc().getTable() != null &&
        resolvedPath.destTupleDesc().getPath().getMatchedTypes().isEmpty()) {
      // The resolved path targets a registered tuple descriptor of a catalog
      // table. Expand the '*' based on the Hive-column order.
      TupleDescriptor tupleDesc = resolvedPath.destTupleDesc();
      Table table = tupleDesc.getTable();
      for (Column c: table.getColumnsInHiveOrder()) {
        addStarResultExpr(resolvedPath, analyzer, c.getName());
      }
    } else {
      // The resolved path does not target the descriptor of a catalog table.
      // Expand '*' based on the destination type of the resolved path.
      Preconditions.checkState(resolvedPath.destType().isStructType());
      StructType structType = (StructType) resolvedPath.destType();
      Preconditions.checkNotNull(structType);

      // Star expansion for references to nested collections.
      // Collection Type                    Star Expansion
      // array<int>                     --> item
      // array<struct<f1,f2,...,fn>>    --> f1, f2, ..., fn
      // map<int,int>                   --> key, value
      // map<int,struct<f1,f2,...,fn>>  --> key, f1, f2, ..., fn
      if (structType instanceof CollectionStructType) {
        CollectionStructType cst = (CollectionStructType) structType;
        if (cst.isMapStruct()) {
          addStarResultExpr(resolvedPath, analyzer, Path.MAP_KEY_FIELD_NAME);
        }
        if (cst.getOptionalField().getType().isStructType()) {
          structType = (StructType) cst.getOptionalField().getType();
          for (StructField f: structType.getFields()) {
            addStarResultExpr(
                resolvedPath, analyzer, cst.getOptionalField().getName(), f.getName());
          }
        } else if (cst.isMapStruct()) {
          addStarResultExpr(resolvedPath, analyzer, Path.MAP_VALUE_FIELD_NAME);
        } else {
          addStarResultExpr(resolvedPath, analyzer, Path.ARRAY_ITEM_FIELD_NAME);
        }
      } else {
        // Default star expansion.
        for (StructField f: structType.getFields()) {
          addStarResultExpr(resolvedPath, analyzer, f.getName());
        }
      }
    }
  }

  /**
   * Helper function used during star expansion to add a single result expr
   * based on a given raw path to be resolved relative to an existing path.
   * Ignores paths with a complex-typed destination because they are currently
   * illegal in any select list (even for inline views, etc.)
   */
  private void addStarResultExpr(Path resolvedPath, Analyzer analyzer,
      String... relRawPath) throws AnalysisException {
    Path p = Path.createRelPath(resolvedPath, relRawPath);
    Preconditions.checkState(p.resolve());
    if (p.destType().isComplexType()) return;
    SlotDescriptor slotDesc = analyzer.registerSlotRef(p);
    SlotRef slotRef = new SlotRef(slotDesc);
    slotRef.analyze(analyzer);
    resultExprs_.add(slotRef);
    colLabels_.add(relRawPath[relRawPath.length - 1]);
  }

  /**
   * Analyze aggregation-relevant components of the select block (Group By clause,
   * select list, Order By clause), substitute AVG with SUM/COUNT, create the
   * AggregationInfo, including the agg output tuple, and transform all post-agg exprs
   * given AggregationInfo's smap.
   */
  private void analyzeAggregation(Analyzer analyzer) throws AnalysisException {
    // Analyze the HAVING clause first so we can check if it contains aggregates.
    // We need to analyze/register it even if we are not computing aggregates.
    if (havingClause_ != null) {
      // can't contain subqueries
      if (havingClause_.contains(Predicates.instanceOf(Subquery.class))) {
        throw new AnalysisException(
            "Subqueries are not supported in the HAVING clause.");
      }
      havingPred_ = substituteOrdinalOrAlias(havingClause_, "HAVING", analyzer);
      // can't contain analytic exprs
      Expr analyticExpr = havingPred_.findFirstOf(AnalyticExpr.class);
      if (analyticExpr != null) {
        throw new AnalysisException(
            "HAVING clause must not contain analytic expressions: "
               + analyticExpr.toSql());
      }
      havingPred_.checkReturnsBool("HAVING clause", true);
    }

    if (groupingExprs_ == null && !selectList_.isDistinct()
        && !TreeNode.contains(resultExprs_, Expr.isAggregatePredicate())
        && (havingPred_ == null
            || !havingPred_.contains(Expr.isAggregatePredicate()))
        && (sortInfo_ == null
            || !TreeNode.contains(sortInfo_.getOrderingExprs(),
                                  Expr.isAggregatePredicate()))) {
      // We're not computing aggregates but we still need to register the HAVING
      // clause which could, e.g., contain a constant expression evaluating to false.
      if (havingPred_ != null) analyzer.registerConjuncts(havingPred_, true);
      return;
    }

    // If we're computing an aggregate, we must have a FROM clause.
    if (fromClause_.isEmpty()) {
      throw new AnalysisException(
          "aggregation without a FROM clause is not allowed");
    }

    if (selectList_.isDistinct()
        && (groupingExprs_ != null
            || TreeNode.contains(resultExprs_, Expr.isAggregatePredicate())
            || (havingPred_ != null
                && havingPred_.contains(Expr.isAggregatePredicate())))) {
      throw new AnalysisException(
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    }

    // Disallow '*' with explicit GROUP BY or aggregation function (we can't group by
    // '*', and if you need to name all star-expanded cols in the group by clause you
    // might as well do it in the select list).
    if (groupingExprs_ != null ||
        TreeNode.contains(resultExprs_, Expr.isAggregatePredicate())) {
      for (SelectListItem item : selectList_.getItems()) {
        if (item.isStar()) {
          throw new AnalysisException(
              "cannot combine '*' in select list with grouping or aggregation");
        }
      }
    }

    // disallow subqueries in the GROUP BY clause
    if (groupingExprs_ != null) {
      for (Expr expr: groupingExprs_) {
        if (expr.contains(Predicates.instanceOf(Subquery.class))) {
          throw new AnalysisException(
              "Subqueries are not supported in the GROUP BY clause.");
        }
      }
    }

    // analyze grouping exprs
    ArrayList<Expr> groupingExprsCopy = Lists.newArrayList();
    if (groupingExprs_ != null) {
      // make a deep copy here, we don't want to modify the original
      // exprs during analysis (in case we need to print them later)
      groupingExprsCopy = Expr.cloneList(groupingExprs_);

      substituteOrdinalsAndAliases(groupingExprsCopy, "GROUP BY", analyzer);

      for (int i = 0; i < groupingExprsCopy.size(); ++i) {
        groupingExprsCopy.get(i).analyze(analyzer);
        if (groupingExprsCopy.get(i).contains(Expr.isAggregatePredicate())) {
          // reference the original expr in the error msg
          throw new AnalysisException(
              "GROUP BY expression must not contain aggregate functions: "
                  + groupingExprs_.get(i).toSql());
        }
        if (groupingExprsCopy.get(i).contains(AnalyticExpr.class)) {
          // reference the original expr in the error msg
          throw new AnalysisException(
              "GROUP BY expression must not contain analytic expressions: "
                  + groupingExprsCopy.get(i).toSql());
        }
      }
    }

    // Collect the aggregate expressions from the SELECT, HAVING and ORDER BY clauses
    // of this statement.
    ArrayList<FunctionCallExpr> aggExprs = Lists.newArrayList();
    TreeNode.collect(resultExprs_, Expr.isAggregatePredicate(), aggExprs);
    if (havingPred_ != null) {
      havingPred_.collect(Expr.isAggregatePredicate(), aggExprs);
    }
    if (sortInfo_ != null) {
      // TODO: Avoid evaluating aggs in ignored order-bys
      TreeNode.collect(sortInfo_.getOrderingExprs(), Expr.isAggregatePredicate(),
          aggExprs);
    }

    // Optionally rewrite all count(distinct <expr>) into equivalent NDV() calls.
    ExprSubstitutionMap ndvSmap = null;
    if (analyzer.getQueryCtx().client_request.query_options.appx_count_distinct) {
      ndvSmap = new ExprSubstitutionMap();
      for (FunctionCallExpr aggExpr: aggExprs) {
        if (!aggExpr.isDistinct()
            || !aggExpr.getFnName().getFunction().equals("count")
            || aggExpr.getParams().size() != 1) {
          continue;
        }
        FunctionCallExpr ndvFnCall =
            new FunctionCallExpr("ndv", aggExpr.getParams().exprs());
        ndvFnCall.analyzeNoThrow(analyzer);
        Preconditions.checkState(ndvFnCall.getType().equals(aggExpr.getType()));
        ndvSmap.put(aggExpr, ndvFnCall);
      }
      // Replace all count(distinct <expr>) with NDV(<expr>).
      List<Expr> substAggExprs = Expr.substituteList(aggExprs, ndvSmap, analyzer, false);
      aggExprs.clear();
      for (Expr aggExpr: substAggExprs) {
        Preconditions.checkState(aggExpr instanceof FunctionCallExpr);
        aggExprs.add((FunctionCallExpr) aggExpr);
      }
    }

    // When DISTINCT aggregates are present, non-distinct (i.e. ALL) aggregates are
    // evaluated in two phases (see AggregateInfo for more details). In particular,
    // COUNT(c) in "SELECT COUNT(c), AGG(DISTINCT d) from R" is transformed to
    // "SELECT SUM(cnt) FROM (SELECT COUNT(c) as cnt from R group by d ) S".
    // Since a group-by expression is added to the inner query it returns no rows if
    // R is empty, in which case the SUM of COUNTs will return NULL.
    // However the original COUNT(c) should have returned 0 instead of NULL in this case.
    // Therefore, COUNT([ALL]) is transformed into zeroifnull(COUNT([ALL]) if
    // i) There is no GROUP-BY clause, and
    // ii) Other DISTINCT aggregates are present.
    ExprSubstitutionMap countAllMap = createCountAllMap(aggExprs, analyzer);
    countAllMap = ExprSubstitutionMap.compose(ndvSmap, countAllMap, analyzer);
    List<Expr> substitutedAggs =
        Expr.substituteList(aggExprs, countAllMap, analyzer, false);
    aggExprs.clear();
    TreeNode.collect(substitutedAggs, Expr.isAggregatePredicate(), aggExprs);
    createAggInfo(groupingExprsCopy, aggExprs, analyzer);

    // combine avg smap with the one that produces the final agg output
    AggregateInfo finalAggInfo =
        aggInfo_.getSecondPhaseDistinctAggInfo() != null
          ? aggInfo_.getSecondPhaseDistinctAggInfo()
          : aggInfo_;

    ExprSubstitutionMap combinedSmap =
        ExprSubstitutionMap.compose(countAllMap, finalAggInfo.getOutputSmap(), analyzer);

    // change select list, having and ordering exprs to point to agg output. We need
    // to reanalyze the exprs at this point.
    if (LOG.isTraceEnabled()) {
      LOG.trace("combined smap: " + combinedSmap.debugString());
      LOG.trace("desctbl: " + analyzer.getDescTbl().debugString());
      LOG.trace("resultexprs: " + Expr.debugString(resultExprs_));
    }
    resultExprs_ = Expr.substituteList(resultExprs_, combinedSmap, analyzer, false);
    if (LOG.isTraceEnabled()) {
      LOG.trace("post-agg selectListExprs: " + Expr.debugString(resultExprs_));
    }
    if (havingPred_ != null) {
      // Make sure the predicate in the HAVING clause does not contain a
      // subquery.
      Preconditions.checkState(!havingPred_.contains(
          Predicates.instanceOf(Subquery.class)));
      havingPred_ = havingPred_.substitute(combinedSmap, analyzer, false);
      analyzer.registerConjuncts(havingPred_, true);
      if (LOG.isTraceEnabled()) {
        LOG.trace("post-agg havingPred: " + havingPred_.debugString());
      }
    }
    if (sortInfo_ != null) {
      sortInfo_.substituteOrderingExprs(combinedSmap, analyzer);
      if (LOG.isTraceEnabled()) {
        LOG.trace("post-agg orderingExprs: " +
            Expr.debugString(sortInfo_.getOrderingExprs()));
      }
    }

    // check that all post-agg exprs point to agg output
    for (int i = 0; i < selectList_.getItems().size(); ++i) {
      if (!resultExprs_.get(i).isBound(finalAggInfo.getOutputTupleId())) {
        SelectListItem selectListItem = selectList_.getItems().get(i);
        throw new AnalysisException(
            "select list expression not produced by aggregation output "
            + "(missing from GROUP BY clause?): "
            + selectListItem.toSql());
      }
    }
    if (orderByElements_ != null) {
      for (int i = 0; i < orderByElements_.size(); ++i) {
        if (!sortInfo_.getOrderingExprs().get(i).isBound(
            finalAggInfo.getOutputTupleId())) {
          throw new AnalysisException(
              "ORDER BY expression not produced by aggregation output "
              + "(missing from GROUP BY clause?): "
              + orderByElements_.get(i).getExpr().toSql());
        }
      }
    }
    if (havingPred_ != null) {
      if (!havingPred_.isBound(finalAggInfo.getOutputTupleId())) {
        throw new AnalysisException(
            "HAVING clause not produced by aggregation output "
            + "(missing from GROUP BY clause?): "
            + havingClause_.toSql());
      }
    }
  }

  /**
   * Create a map from COUNT([ALL]) -> zeroifnull(COUNT([ALL])) if
   * i) There is no GROUP-BY, and
   * ii) There are other distinct aggregates to be evaluated.
   * This transformation is necessary for COUNT to correctly return 0 for empty
   * input relations.
   */
  private ExprSubstitutionMap createCountAllMap(
      List<FunctionCallExpr> aggExprs, Analyzer analyzer)
      throws AnalysisException {
    ExprSubstitutionMap scalarCountAllMap = new ExprSubstitutionMap();

    if (groupingExprs_ != null && !groupingExprs_.isEmpty()) {
      // There are grouping expressions, so no substitution needs to be done.
      return scalarCountAllMap;
    }

    com.google.common.base.Predicate<FunctionCallExpr> isNotDistinctPred =
        new com.google.common.base.Predicate<FunctionCallExpr>() {
          public boolean apply(FunctionCallExpr expr) {
            return !expr.isDistinct();
          }
        };
    if (Iterables.all(aggExprs, isNotDistinctPred)) {
      // Only [ALL] aggs, so no substitution needs to be done.
      return scalarCountAllMap;
    }

    com.google.common.base.Predicate<FunctionCallExpr> isCountPred =
        new com.google.common.base.Predicate<FunctionCallExpr>() {
          public boolean apply(FunctionCallExpr expr) {
            return expr.getFnName().getFunction().equals("count");
          }
        };

    Iterable<FunctionCallExpr> countAllAggs =
        Iterables.filter(aggExprs, Predicates.and(isCountPred, isNotDistinctPred));
    for (FunctionCallExpr countAllAgg: countAllAggs) {
      // Replace COUNT(ALL) with zeroifnull(COUNT(ALL))
      ArrayList<Expr> zeroIfNullParam = Lists.newArrayList(countAllAgg.clone());
      FunctionCallExpr zeroIfNull =
          new FunctionCallExpr("zeroifnull", zeroIfNullParam);
      zeroIfNull.analyze(analyzer);
      scalarCountAllMap.put(countAllAgg, zeroIfNull);
    }

    return scalarCountAllMap;
  }

  /**
   * Create aggInfo for the given grouping and agg exprs.
   */
  private void createAggInfo(ArrayList<Expr> groupingExprs,
      ArrayList<FunctionCallExpr> aggExprs, Analyzer analyzer)
          throws AnalysisException {
    if (selectList_.isDistinct()) {
       // Create aggInfo for SELECT DISTINCT ... stmt:
       // - all select list items turn into grouping exprs
       // - there are no aggregate exprs
      Preconditions.checkState(groupingExprs.isEmpty());
      Preconditions.checkState(aggExprs.isEmpty());
      ArrayList<Expr> distinctGroupingExprs = Expr.cloneList(resultExprs_);
      aggInfo_ =
          AggregateInfo.create(distinctGroupingExprs, null, null, analyzer);
    } else {
      aggInfo_ = AggregateInfo.create(groupingExprs, aggExprs, null, analyzer);
    }
  }

  /**
   * If the select list contains AnalyticExprs, create AnalyticInfo and substitute
   * AnalyticExprs using the AnalyticInfo's smap.
   */
  private void createAnalyticInfo(Analyzer analyzer)
      throws AnalysisException {
    // collect AnalyticExprs from the SELECT and ORDER BY clauses
    ArrayList<Expr> analyticExprs = Lists.newArrayList();
    TreeNode.collect(resultExprs_, AnalyticExpr.class, analyticExprs);
    if (sortInfo_ != null) {
      TreeNode.collect(sortInfo_.getOrderingExprs(), AnalyticExpr.class,
          analyticExprs);
    }
    if (analyticExprs.isEmpty()) return;
    ExprSubstitutionMap rewriteSmap = new ExprSubstitutionMap();
    for (Expr expr: analyticExprs) {
      AnalyticExpr toRewrite = (AnalyticExpr)expr;
      Expr newExpr = AnalyticExpr.rewrite(toRewrite);
      if (newExpr != null) {
        newExpr.analyze(analyzer);
        if (!rewriteSmap.containsMappingFor(toRewrite)) {
          rewriteSmap.put(toRewrite, newExpr);
        }
      }
    }
    if (rewriteSmap.size() > 0) {
      // Substitute the exprs with their rewritten versions.
      ArrayList<Expr> updatedAnalyticExprs =
          Expr.substituteList(analyticExprs, rewriteSmap, analyzer, false);
      // This is to get rid the original exprs which have been rewritten.
      analyticExprs.clear();
      // Collect the new exprs introduced through the rewrite and the non-rewrite exprs.
      TreeNode.collect(updatedAnalyticExprs, AnalyticExpr.class, analyticExprs);
    }

    analyticInfo_ = AnalyticInfo.create(analyticExprs, analyzer);

    ExprSubstitutionMap smap = analyticInfo_.getSmap();
    // If 'exprRewritten' is true, we have to compose the new smap with the existing one.
    if (rewriteSmap.size() > 0) {
      smap = ExprSubstitutionMap.compose(
          rewriteSmap, analyticInfo_.getSmap(), analyzer);
    }
    // change select list and ordering exprs to point to analytic output. We need
    // to reanalyze the exprs at this point.
    resultExprs_ = Expr.substituteList(resultExprs_, smap, analyzer, false);
    if (LOG.isTraceEnabled()) {
      LOG.trace("post-analytic selectListExprs: " + Expr.debugString(resultExprs_));
    }
    if (sortInfo_ != null) {
      sortInfo_.substituteOrderingExprs(smap, analyzer);
      if (LOG.isTraceEnabled()) {
        LOG.trace("post-analytic orderingExprs: " +
            Expr.debugString(sortInfo_.getOrderingExprs()));
      }
    }
  }

  /**
   * If given expr is rewritten into an integer literal, then return the original expr,
   * otherwise return the rewritten expr.
   * Used for GROUP BY, ORDER BY, and HAVING where we don't want to create an ordinal
   * from a constant arithmetic expr, e.g. 1 * 2 =/=> 2
   */
  private Expr rewriteCheckOrdinalResult(ExprRewriter rewriter, Expr expr)
      throws AnalysisException {
    Expr rewrittenExpr = rewriter.rewrite(expr, analyzer_);
    if (rewrittenExpr.isLiteral() && rewrittenExpr.getType().isIntegerType()) {
      return expr;
    }
    else {
      return rewrittenExpr;
    }
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    Preconditions.checkState(isAnalyzed());
    selectList_.rewriteExprs(rewriter, analyzer_);
    for (TableRef ref: fromClause_.getTableRefs()) ref.rewriteExprs(rewriter, analyzer_);
    if (whereClause_ != null) {
      whereClause_ = rewriter.rewrite(whereClause_, analyzer_);
      // Also rewrite exprs in the statements of subqueries.
      List<Subquery> subqueryExprs = Lists.newArrayList();
      whereClause_.collect(Subquery.class, subqueryExprs);
      for (Subquery s: subqueryExprs) s.getStatement().rewriteExprs(rewriter);
    }
    if (havingClause_ != null) {
      havingClause_ = rewriteCheckOrdinalResult(rewriter, havingClause_);
    }
    if (groupingExprs_ != null) {
      for (int i = 0; i < groupingExprs_.size(); ++i) {
        groupingExprs_.set(i, rewriteCheckOrdinalResult(
            rewriter, groupingExprs_.get(i)));
      }
    }
    if (orderByElements_ != null) {
      for (OrderByElement orderByElem: orderByElements_) {
        orderByElem.setExpr(rewriteCheckOrdinalResult(rewriter, orderByElem.getExpr()));
      }
    }
  }

  /**
   * Returns the SQL string corresponding to this SelectStmt.
   */
  @Override
  public String toSql() {
    // Return the SQL string before inline-view expression substitution.
    if (sqlString_ != null) return sqlString_;

    StringBuilder strBuilder = new StringBuilder();
    if (withClause_ != null) {
      strBuilder.append(withClause_.toSql());
      strBuilder.append(" ");
    }

    // Select list
    strBuilder.append("SELECT ");
    if (selectList_.isDistinct()) {
      strBuilder.append("DISTINCT ");
    }
    if (selectList_.hasPlanHints()) {
      strBuilder.append(ToSqlUtils.getPlanHintsSql(selectList_.getPlanHints()) + " ");
    }
    for (int i = 0; i < selectList_.getItems().size(); ++i) {
      strBuilder.append(selectList_.getItems().get(i).toSql());
      strBuilder.append((i+1 != selectList_.getItems().size()) ? ", " : "");
    }
    // From clause
    if (!fromClause_.isEmpty()) { strBuilder.append(fromClause_.toSql()); }
    // Where clause
    if (whereClause_ != null) {
      strBuilder.append(" WHERE ");
      strBuilder.append(whereClause_.toSql());
    }
    // Group By clause
    if (groupingExprs_ != null) {
      strBuilder.append(" GROUP BY ");
      for (int i = 0; i < groupingExprs_.size(); ++i) {
        strBuilder.append(groupingExprs_.get(i).toSql());
        strBuilder.append((i+1 != groupingExprs_.size()) ? ", " : "");
      }
    }
    // Having clause
    if (havingClause_ != null) {
      strBuilder.append(" HAVING ");
      strBuilder.append(havingClause_.toSql());
    }
    // Order By clause
    if (orderByElements_ != null) {
      strBuilder.append(" ORDER BY ");
      for (int i = 0; i < orderByElements_.size(); ++i) {
        strBuilder.append(orderByElements_.get(i).toSql());
        strBuilder.append((i+1 != orderByElements_.size()) ? ", " : "");
      }
    }
    // Limit clause.
    strBuilder.append(limitElement_.toSql());
    return strBuilder.toString();
  }

  /**
   * If the select statement has a sort/top that is evaluated, then the sort tuple
   * is materialized. Else, if there is aggregation then the aggregate tuple id is
   * materialized. Otherwise, all referenced tables are materialized as long as they are
   * not semi-joined. If there are analytics and no sort, then the returned tuple
   * ids also include the logical analytic output tuple.
   */
  @Override
  public void getMaterializedTupleIds(ArrayList<TupleId> tupleIdList) {
    if (evaluateOrderBy_) {
      tupleIdList.add(sortInfo_.getSortTupleDescriptor().getId());
    } else if (aggInfo_ != null) {
      // Return the tuple id produced in the final aggregation step.
      tupleIdList.add(aggInfo_.getResultTupleId());
    } else {
      for (TableRef tblRef: fromClause_) {
        // Don't include materialized tuple ids from semi-joined table
        // refs (see IMPALA-1526)
        if (tblRef.getJoinOp().isLeftSemiJoin()) continue;
        // Remove the materialized tuple ids of all the table refs that
        // are semi-joined by the right semi/anti join.
        if (tblRef.getJoinOp().isRightSemiJoin()) tupleIdList.clear();
        tupleIdList.addAll(tblRef.getMaterializedTupleIds());
      }
    }
    // We materialize the agg tuple or the table refs together with the analytic tuple.
    if (hasAnalyticInfo() && !evaluateOrderBy_) {
      tupleIdList.add(analyticInfo_.getOutputTupleId());
    }
  }

  /**
   * C'tor for cloning.
   */
  private SelectStmt(SelectStmt other) {
    super(other);
    selectList_ = other.selectList_.clone();
    fromClause_ = other.fromClause_.clone();
    whereClause_ = (other.whereClause_ != null) ? other.whereClause_.clone() : null;
    groupingExprs_ =
        (other.groupingExprs_ != null) ? Expr.cloneList(other.groupingExprs_) : null;
    havingClause_ = (other.havingClause_ != null) ? other.havingClause_.clone() : null;
    colLabels_ = Lists.newArrayList(other.colLabels_);
    aggInfo_ = (other.aggInfo_ != null) ? other.aggInfo_.clone() : null;
    analyticInfo_ = (other.analyticInfo_ != null) ? other.analyticInfo_.clone() : null;
    sqlString_ = (other.sqlString_ != null) ? new String(other.sqlString_) : null;
    baseTblSmap_ = other.baseTblSmap_.clone();
  }

  @Override
  protected void collectTableRefs(List<TableRef> tblRefs, boolean fromClauseOnly) {
    super.collectTableRefs(tblRefs, fromClauseOnly);
    if (fromClauseOnly) {
      fromClause_.collectFromClauseTableRefs(tblRefs);
    } else {
      fromClause_.collectTableRefs(tblRefs);
    }
    if (!fromClauseOnly && whereClause_ != null) {
      // Collect TableRefs in WHERE-clause subqueries.
      List<Subquery> subqueries = Lists.newArrayList();
      whereClause_.collect(Subquery.class, subqueries);
      for (Subquery sq: subqueries) {
        sq.getStatement().collectTableRefs(tblRefs, fromClauseOnly);
      }
    }
  }

  @Override
  public void reset() {
    super.reset();
    selectList_.reset();
    colLabels_.clear();
    fromClause_.reset();
    if (whereClause_ != null) whereClause_.reset();
    if (groupingExprs_ != null) Expr.resetList(groupingExprs_);
    if (havingClause_ != null) havingClause_.reset();
    havingPred_ = null;
    aggInfo_ = null;
    analyticInfo_ = null;
    baseTblSmap_.clear();
  }

  @Override
  public SelectStmt clone() { return new SelectStmt(this); }

  /**
   * Check if the stmt returns a single row. This can happen
   * in the following cases:
   * 1. select stmt with a 'limit 1' clause
   * 2. select stmt with an aggregate function and no group by.
   * 3. select stmt with no from clause.
   *
   * This function may produce false negatives because the cardinality of the
   * result set also depends on the data a stmt is processing.
   */
  public boolean returnsSingleRow() {
    // limit 1 clause
    if (limitElement_ != null && limitElement_.getLimit() == 1) return true;
    // No from clause (base tables or inline views)
    if (fromClause_.isEmpty()) return true;
    // Aggregation with no group by and no DISTINCT
    if (hasAggInfo() && !hasGroupByClause() && !selectList_.isDistinct()) return true;
    // In all other cases, return false.
    return false;
  }
}
