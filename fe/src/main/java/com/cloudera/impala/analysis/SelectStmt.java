// Copyright 2012 Cloudera Inc.
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
import java.util.ListIterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.ColumnType;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Representation of a single select block, including GROUP BY, ORDER BY and HAVING
 * clauses.
 */
public class SelectStmt extends QueryStmt {
  private final static Logger LOG = LoggerFactory.getLogger(SelectStmt.class);

  private final SelectList selectList_;
  private final ArrayList<String> colLabels_; // lower case column labels
  private final List<TableRef> tableRefs_;
  private final Expr whereClause_;
  private final ArrayList<Expr> groupingExprs_;
  private final Expr havingClause_;  // original having clause

  // havingClause with aliases and agg output resolved
  private Expr havingPred_;

  // set if we have any kind of aggregation operation, include SELECT DISTINCT
  private AggregateInfo aggInfo_;

  // SQL string of this SelectStmt before inline-view expression substitution.
  // Set in analyze().
  private String sqlString_;

  // substitutes all exprs in this select block to reference base tables
  // directly
  private Expr.SubstitutionMap baseTblSmap_ = new Expr.SubstitutionMap();

  SelectStmt(SelectList selectList,
             List<TableRef> tableRefList,
             Expr wherePredicate, ArrayList<Expr> groupingExprs,
             Expr havingPredicate, ArrayList<OrderByElement> orderByElements,
             LimitElement limitElement) {
    super(orderByElements, limitElement);
    this.selectList_ = selectList;
    if (tableRefList == null) {
      this.tableRefs_ = Lists.newArrayList();
    } else {
      this.tableRefs_ = tableRefList;
    }
    this.whereClause_ = wherePredicate;
    this.groupingExprs_ = groupingExprs;
    this.havingClause_ = havingPredicate;
    this.colLabels_ = Lists.newArrayList();
    this.havingPred_ = null;
    this.aggInfo_ = null;
    this.sortInfo_ = null;
    // Set left table refs to ensure correct toSql() before analysis.
    for (int i = 1; i < tableRefs_.size(); ++i) {
      tableRefs_.get(i).setLeftTblRef(tableRefs_.get(i - 1));
    }
  }

  /**
   * @return the original select list items from the query
   */
  public SelectList getSelectList() { return selectList_; }

  /**
   * @return the HAVING clause post-analysis and with aliases resolved
   */
  public Expr getHavingPred() { return havingPred_; }

  public List<TableRef> getTableRefs() { return tableRefs_; }
  public Expr getWhereClause() { return whereClause_; }
  public AggregateInfo getAggInfo() { return aggInfo_; }
  @Override
  public SortInfo getSortInfo() { return sortInfo_; }
  @Override
  public ArrayList<String> getColLabels() { return colLabels_; }
  public Expr.SubstitutionMap getBaseTblSmap() { return baseTblSmap_; }

  /**
   * Creates resultExprs and baseTblResultExprs.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    super.analyze(analyzer);

    // Replace BaseTableRefs with ViewRefs.
    substituteViews(analyzer, tableRefs_);

    // start out with table refs to establish aliases
    TableRef leftTblRef = null;  // the one to the left of tblRef
    for (TableRef tblRef: tableRefs_) {
      tblRef.setLeftTblRef(leftTblRef);
      try {
        tblRef.analyze(analyzer);
      } catch (AnalysisException e) {
        // Only re-throw the exception if no tables are missing.
        if (analyzer.getMissingTbls().isEmpty()) throw e;
      }
      leftTblRef = tblRef;
    }

    // All tableRefs have been analyzed, but at least one table was found missing.
    // There is no reason to proceed with analysis past this point.
    if (!analyzer.getMissingTbls().isEmpty()) {
      throw new AnalysisException("Found missing tables. Aborting analysis.");
    }

    // populate selectListExprs, aliasSMap, and colNames
    for (int i = 0; i < selectList_.getItems().size(); ++i) {
      SelectListItem item = selectList_.getItems().get(i);
      if (item.isStar()) {
        TableName tblName = item.getTblName();
        if (tblName == null) {
          expandStar(analyzer);
        } else {
          expandStar(analyzer, tblName);
        }
      } else {
        // Analyze the resultExpr before generating a label to ensure enforcement
        // of expr child and depth limits (toColumn() label may call toSql()).
        item.getExpr().analyze(analyzer);
        resultExprs_.add(item.getExpr());
        String label = item.toColumnLabel(i, analyzer.useHiveColLabels());
        SlotRef aliasRef = new SlotRef(null, label);
        if (aliasSmap_.containsMappingFor(aliasRef)) {
          // If we have already seen this alias, it refers to more than one column and
          // therefore is ambiguous.
          ambiguousAliasList_.add(aliasRef);
        }
        aliasSmap_.addMapping(aliasRef, item.getExpr().clone(null));
        colLabels_.add(label);
      }
    }

    if (whereClause_ != null) {
      whereClause_.analyze(analyzer);
      if (whereClause_.containsAggregate()) {
        throw new AnalysisException(
            "aggregate function not allowed in WHERE clause");
      }
      whereClause_.checkReturnsBool("WHERE clause", false);
      analyzer.registerConjuncts(whereClause_, null, true);
    }

    createSortInfo(analyzer);
    analyzeAggregation(analyzer);

    // Remember the SQL string before inline-view expression substitution.
    sqlString_ = toSql();

    resolveInlineViewRefs(analyzer);

    if (aggInfo_ != null) LOG.debug("post-analysis " + aggInfo_.debugString());
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
      if (analyzer.evalByJoin(e)) unassignedJoinConjuncts.add(e);
    }
    List<Expr> baseTblJoinConjuncts =
        Expr.cloneList(unassignedJoinConjuncts, baseTblSmap_);
    materializeSlots(analyzer, baseTblJoinConjuncts);

    if (sortInfo_ != null) {
      // mark ordering exprs before marking agg exprs because the ordering exprs
      // may contain agg exprs that are not referenced anywhere but the ORDER BY clause
      List<Expr> resolvedExprs =
          Expr.cloneList(sortInfo_.getOrderingExprs(), baseTblSmap_);
      materializeSlots(analyzer, resolvedExprs);
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
        groupBySlots.add(aggInfo_.getAggTupleDesc().getSlots().get(i).getId());
      }
      ArrayList<Expr> bindingPredicates =
          analyzer.getBoundPredicates(aggInfo_.getAggTupleId(), groupBySlots);
      havingConjuncts.addAll(bindingPredicates);
      havingConjuncts.addAll(
          analyzer.getUnassignedConjuncts(aggInfo_.getAggTupleId().asList(), false));
      materializeSlots(analyzer, havingConjuncts);
      aggInfo_.materializeRequiredSlots(analyzer, baseTblSmap_);
    }
  }

  /**
   * Replaces BaseTableRefs in tblRefs whose alias matches a view registered in
   * the given analyzer or its parent analyzers with a clone of the matching inline view.
   * The cloned inline view inherits the context-dependent attributes such as the
   * on-clause, join hints, etc. from the original BaseTableRef.
   *
   * Matches views from the inside out, i.e., we first look
   * in this analyzer then in the parentAnalyzer then and its parent, etc.,
   * and finally consult the catalog for matching views (the global scope).
   *
   * This method is used for substituting views from WITH clauses
   * and views from the catalog.
   */
  public void substituteViews(Analyzer analyzer, List<TableRef> tblRefs)
      throws AuthorizationException, AnalysisException {
    for (int i = 0; i < tblRefs.size(); ++i) {
      if (!(tblRefs.get(i) instanceof BaseTableRef)) continue;
      BaseTableRef tblRef = (BaseTableRef) tblRefs.get(i);
      ViewRef viewDefinition = analyzer.findViewDefinition(tblRef, true);
      if (viewDefinition == null) continue;
      // Instantiate the view to replace the original BaseTableRef.
      ViewRef viewRef = viewDefinition.instantiate(tblRef);
      viewRef.getViewStmt().setIsExplain(isExplain_);
      tblRefs.set(i, viewRef);
    }
  }

  /**
    * Populates baseTblSmap_ with our combined inline view smap and creates
    * baseTblResultExprs.
    */
  protected void resolveInlineViewRefs(Analyzer analyzer) {
    // Gather the inline view substitution maps from the enclosed inline views
    for (TableRef tblRef: tableRefs_) {
      if (tblRef instanceof InlineViewRef) {
        InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
        baseTblSmap_ =
            Expr.SubstitutionMap.combine(baseTblSmap_, inlineViewRef.getBaseTblSmap());
      }
    }
    baseTblResultExprs_ = Expr.cloneList(resultExprs_, baseTblSmap_);
    LOG.trace("baseTblSmap_: " + baseTblSmap_.debugString());
    LOG.trace("resultExprs: " + Expr.debugString(resultExprs_));
    LOG.trace("baseTblResultExprs: " + Expr.debugString(baseTblResultExprs_));
  }

  public List<TupleId> getTableRefIds() {
    List<TupleId> result = Lists.newArrayList();
    for (TableRef ref: tableRefs_) {
      result.add(ref.getId());
    }
    return result;
  }

  /**
   * Expand "*" select list item.
   */
  private void expandStar(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (tableRefs_.isEmpty()) {
      throw new AnalysisException("'*' expression in select list requires FROM clause.");
    }
    // expand in From clause order
    for (TableRef tableRef: tableRefs_) {
      expandStar(analyzer, tableRef.getAliasAsName(), tableRef.getDesc());
    }
  }

  /**
   * Expand "<tbl>.*" select list item.
   */
  private void expandStar(Analyzer analyzer, TableName tblName)
      throws AnalysisException, AuthorizationException {
    TupleDescriptor tupleDesc = analyzer.getDescriptor(tblName);
    if (tupleDesc == null) {
      throw new AnalysisException("unknown table: " + tblName.toString());
    }
    expandStar(analyzer, tblName, tupleDesc);
  }

  /**
   * Expand "*" for a particular tuple descriptor by appending
   * analyzed slot refs for each column to selectListExprs.
   */
  private void expandStar(Analyzer analyzer, TableName tblName, TupleDescriptor desc)
      throws AnalysisException, AuthorizationException {
    for (Column col: desc.getTable().getColumnsInHiveOrder()) {
      SlotRef slotRef = new SlotRef(tblName, col.getName());
      slotRef.analyze(analyzer);
      resultExprs_.add(slotRef);
      colLabels_.add(col.getName().toLowerCase());
    }
  }

  /**
   * Analyze aggregation-relevant components of the select block (Group By clause,
   * select list, Order By clause), substitute AVG with SUM/COUNT, create the
   * AggregationInfo, including the agg output tuple, and transform all post-agg exprs
   * given AggregationInfo's smap.
   */
  private void analyzeAggregation(Analyzer analyzer)
      throws AnalysisException, AuthorizationException {
    if (groupingExprs_ == null && !selectList_.isDistinct()
        && !Expr.containsAggregate(resultExprs_)) {
      // we're not computing aggregates
      return;
    }

    // If we're computing an aggregate, we must have a FROM clause.
    if (tableRefs_.size() == 0) {
      throw new AnalysisException(
          "aggregation without a FROM clause is not allowed");
    }

    if ((groupingExprs_ != null || Expr.containsAggregate(resultExprs_))
        && selectList_.isDistinct()) {
      throw new AnalysisException(
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    }

    // disallow '*' and explicit GROUP BY (we can't group by '*', and if you need to
    // name all star-expanded cols in the group by clause you might as well do it
    // in the select list)
    if (groupingExprs_ != null) {
      for (SelectListItem item : selectList_.getItems()) {
        if (item.isStar()) {
          throw new AnalysisException(
              "cannot combine '*' in select list with GROUP BY: " + item.toSql());
        }
      }
    }

    // analyze grouping exprs
    ArrayList<Expr> groupingExprsCopy = Lists.newArrayList();
    if (groupingExprs_ != null) {
      // make a deep copy here, we don't want to modify the original
      // exprs during analysis (in case we need to print them later)
      groupingExprsCopy = Expr.cloneList(groupingExprs_, null);
      substituteOrdinals(groupingExprsCopy, "GROUP BY");
      Expr ambiguousAlias = getFirstAmbiguousAlias(groupingExprsCopy);
      if (ambiguousAlias != null) {
        throw new AnalysisException("Column " + ambiguousAlias.toSql() +
            " in group by clause is ambiguous");
      }
      Expr.substituteList(groupingExprsCopy, aliasSmap_);
      for (int i = 0; i < groupingExprsCopy.size(); ++i) {
        groupingExprsCopy.get(i).analyze(analyzer);
        if (groupingExprsCopy.get(i).containsAggregate()) {
          // reference the original expr in the error msg
          throw new AnalysisException(
              "GROUP BY expression must not contain aggregate functions: "
                  + groupingExprs_.get(i).toSql());
        }
      }
    }

    // analyze having clause
    if (havingClause_ != null) {
      // substitute aliases in place (ordinals not allowed in having clause)
      havingPred_ = havingClause_.clone(aliasSmap_);
      havingPred_.analyze(analyzer);
      havingPred_.checkReturnsBool("HAVING clause", true);
    }

    List<Expr> orderingExprs = null;
    if (sortInfo_ != null) orderingExprs = sortInfo_.getOrderingExprs();

    ArrayList<FunctionCallExpr> aggExprs = collectAggExprs();
    Expr.SubstitutionMap avgSMap = createAvgSMap(aggExprs, analyzer);

    // substitute AVG before constructing AggregateInfo
    Expr.substituteList(aggExprs, avgSMap);
    ArrayList<FunctionCallExpr> nonAvgAggExprs = Lists.newArrayList();
    Expr.collectAggregateExprs(aggExprs, nonAvgAggExprs);
    aggExprs = nonAvgAggExprs;
    try {
      createAggInfo(groupingExprsCopy, aggExprs, analyzer);
    } catch (InternalException e) {
      throw new AnalysisException(e.getMessage(), e);
    }

    // combine avg smap with the one that produces the final agg output
    AggregateInfo finalAggInfo =
        aggInfo_.getSecondPhaseDistinctAggInfo() != null
          ? aggInfo_.getSecondPhaseDistinctAggInfo()
          : aggInfo_;
    Expr.SubstitutionMap combinedSMap =
        Expr.SubstitutionMap.compose(avgSMap, finalAggInfo.getSMap());
    LOG.debug("combined smap: " + combinedSMap.debugString());

    // change select list, having and ordering exprs to point to agg output
    Expr.substituteList(resultExprs_, combinedSMap);
    LOG.debug("post-agg selectListExprs: " + Expr.debugString(resultExprs_));
    if (havingPred_ != null) {
      havingPred_ = havingPred_.substitute(combinedSMap);
      analyzer.registerConjuncts(havingPred_, null, false);
      LOG.debug("post-agg havingPred: " + havingPred_.debugString());
    }
    Expr.substituteList(orderingExprs, combinedSMap);
    LOG.debug("post-agg orderingExprs: " + Expr.debugString(orderingExprs));

    // check that all post-agg exprs point to agg output
    for (int i = 0; i < selectList_.getItems().size(); ++i) {
      if (!resultExprs_.get(i).isBound(finalAggInfo.getAggTupleId())) {
        throw new AnalysisException(
            "select list expression not produced by aggregation output "
            + "(missing from GROUP BY clause?): "
            + selectList_.getItems().get(i).getExpr().toSql());
      }
    }
    if (orderByElements_ != null) {
      for (int i = 0; i < orderByElements_.size(); ++i) {
        if (!orderingExprs.get(i).isBound(finalAggInfo.getAggTupleId())) {
          throw new AnalysisException(
              "ORDER BY expression not produced by aggregation output "
              + "(missing from GROUP BY clause?): "
              + orderByElements_.get(i).getExpr().toSql());
        }
      }
    }
    if (havingPred_ != null) {
      if (!havingPred_.isBound(finalAggInfo.getAggTupleId())) {
        throw new AnalysisException(
            "HAVING clause not produced by aggregation output "
            + "(missing from GROUP BY clause?): "
            + havingClause_.toSql());
      }
    }
  }

  private ArrayList<FunctionCallExpr> collectAggExprs() {
    ArrayList<FunctionCallExpr> result = Lists.newArrayList();
    Expr.collectAggregateExprs(resultExprs_, result);
    if (havingPred_ != null) {
      havingPred_.collectAggregateExprs(result);
    }
    if (sortInfo_ != null) {
      Expr.collectAggregateExprs(sortInfo_.getOrderingExprs(), result);
    }
    return result;
  }

  /**
   * Build smap AVG -> SUM/COUNT;
   * assumes that select list and having clause have been analyzed.
   */
  private Expr.SubstitutionMap createAvgSMap(
      ArrayList<FunctionCallExpr> aggExprs, Analyzer analyzer)
      throws AnalysisException, AuthorizationException {
    Expr.SubstitutionMap result = new Expr.SubstitutionMap();
    for (FunctionCallExpr aggExpr : aggExprs) {
      if (!aggExpr.getFnName().getFunction().equals("avg")) continue;
      // Transform avg(TIMESTAMP) to cast(avg(cast(TIMESTAMP as DOUBLE)) as TIMESTAMP)
      CastExpr inCastExpr = null;
      if (aggExpr.getChild(0).type_.getPrimitiveType() == PrimitiveType.TIMESTAMP) {
        inCastExpr =
            new CastExpr(ColumnType.DOUBLE, aggExpr.getChild(0).clone(null), false);
      }

      List<Expr> sumInputExprs =
          Lists.newArrayList(
              aggExpr.getChild(0).type_.getPrimitiveType() == PrimitiveType.TIMESTAMP ?
              inCastExpr : aggExpr.getChild(0).clone(null));
      List<Expr> countInputExpr = Lists.newArrayList(aggExpr.getChild(0).clone(null));

      FunctionCallExpr sumExpr =
          new FunctionCallExpr("sum",
              new FunctionParams(aggExpr.isDistinct(), sumInputExprs));
      FunctionCallExpr countExpr =
          new FunctionCallExpr("count",
              new FunctionParams(aggExpr.isDistinct(), countInputExpr));
      ArithmeticExpr divExpr =
          new ArithmeticExpr(ArithmeticExpr.Operator.DIVIDE, sumExpr, countExpr);

      if (aggExpr.getChild(0).type_.getPrimitiveType() == PrimitiveType.TIMESTAMP) {
        CastExpr outCastExpr = new CastExpr(ColumnType.TIMESTAMP, divExpr, false);
        outCastExpr.analyze(analyzer);
        result.addMapping(aggExpr, outCastExpr);
      } else {
        divExpr.analyze(analyzer);
        result.addMapping(aggExpr, divExpr);
      }
    }
    LOG.debug("avg smap: " + result.debugString());
    return result;
  }

  /**
   * Create aggInfo for the given grouping and agg exprs.
   */
  private void createAggInfo(ArrayList<Expr> groupingExprs,
      ArrayList<FunctionCallExpr> aggExprs, Analyzer analyzer)
      throws AnalysisException, InternalException, AuthorizationException {
    if (selectList_.isDistinct()) {
       // Create aggInfo for SELECT DISTINCT ... stmt:
       // - all select list items turn into grouping exprs
       // - there are no aggregate exprs
      Preconditions.checkState(groupingExprs.isEmpty());
      Preconditions.checkState(aggExprs.isEmpty());
      aggInfo_ =
          AggregateInfo.create(Expr.cloneList(resultExprs_, null), null, null, analyzer);
    } else {
      aggInfo_ = AggregateInfo.create(groupingExprs, aggExprs, null, analyzer);
    }
  }

  /**
   * Substitute exprs of the form "<number>"  with the corresponding
   * expressions from select list
   */
  @Override
  protected void substituteOrdinals(List<Expr> exprs, String errorPrefix)
      throws AnalysisException {
    // substitute ordinals
    ListIterator<Expr> i = exprs.listIterator();
    while (i.hasNext()) {
      Expr expr = i.next();
      if (!(expr instanceof IntLiteral)) {
        continue;
      }
      long pos = ((IntLiteral) expr).getValue();
      if (pos < 1) {
        throw new AnalysisException(
            errorPrefix + ": ordinal must be >= 1: " + expr.toSql());
      }
      if (pos > selectList_.getItems().size()) {
        throw new AnalysisException(
            errorPrefix + ": ordinal exceeds number of items in select list: "
            + expr.toSql());
      }
      if (selectList_.getItems().get((int) pos - 1).isStar()) {
        throw new AnalysisException(
            errorPrefix + ": ordinal refers to '*' in select list: "
            + expr.toSql());
      }
      // create copy to protect against accidentally shared state
      i.set(selectList_.getItems().get((int)pos - 1).getExpr().clone(null));
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
    for (int i = 0; i < selectList_.getItems().size(); ++i) {
      strBuilder.append(selectList_.getItems().get(i).toSql());
      strBuilder.append((i+1 != selectList_.getItems().size()) ? ", " : "");
    }
    // From clause
    if (!tableRefs_.isEmpty()) {
      strBuilder.append(" FROM ");
      for (int i = 0; i < tableRefs_.size(); ++i) {
        strBuilder.append(tableRefs_.get(i).toSql());
      }
    }
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

  @Override
  public void getMaterializedTupleIds(ArrayList<TupleId> tupleIdList) {
    // If select statement has an aggregate, then the aggregate tuple id is materialized.
    // Otherwise, all referenced tables are materialized.
    if (aggInfo_ != null) {
      // Return the tuple id produced in the final aggregation step.
      if (aggInfo_.isDistinctAgg()) {
        tupleIdList.add(aggInfo_.getSecondPhaseDistinctAggInfo().getAggTupleId());
      } else {
        tupleIdList.add(aggInfo_.getAggTupleId());
      }
    } else {
      for (TableRef tblRef: tableRefs_) {
        tupleIdList.addAll(tblRef.getMaterializedTupleIds());
      }
    }
  }

  private ArrayList<TableRef> cloneTableRefs() {
    ArrayList<TableRef> clone = Lists.newArrayList();
    for (TableRef tblRef : tableRefs_) {
      clone.add(tblRef.clone());
    }
    return clone;
  }

  @Override
  public QueryStmt clone() {
    SelectStmt selectClone = new SelectStmt(selectList_.clone(), cloneTableRefs(),
        (whereClause_ != null) ? whereClause_.clone(null) : null,
        (groupingExprs_ != null) ? Expr.cloneList(groupingExprs_, null) : null,
        (havingClause_ != null) ? havingClause_.clone(null) : null,
        cloneOrderByElements(),
        (limitElement_ != null) ? limitElement_.clone(null) : null);
    selectClone.setWithClause(cloneWithClause());
    return selectClone;
  }
}
