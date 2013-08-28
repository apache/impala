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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Representation of a single select block, including GROUP BY, ORDER BY and HAVING
 * clauses.
 */
public class SelectStmt extends QueryStmt {
  private final static Logger LOG = LoggerFactory.getLogger(SelectStmt.class);

  private final SelectList selectList;
  private final ArrayList<String> colLabels; // lower case column labels
  private final List<TableRef> tableRefs;
  private final Expr whereClause;
  private final ArrayList<Expr> groupingExprs;
  private final Expr havingClause;  // original having clause

  // havingClause with aliases and agg output resolved
  private Expr havingPred;

  // set if we have any kind of aggregation operation, include SELECT DISTINCT
  private AggregateInfo aggInfo;

  // SQL string of this SelectStmt before inline-view expression substitution.
  // Set in analyze().
  private String sqlString;

  // substitutes all exprs in this select block to reference base tables
  // directly
  private Expr.SubstitutionMap baseTblSmap_ = new Expr.SubstitutionMap();

  SelectStmt(SelectList selectList,
             List<TableRef> tableRefList,
             Expr wherePredicate, ArrayList<Expr> groupingExprs,
             Expr havingPredicate, ArrayList<OrderByElement> orderByElements,
             LimitElement limitElement) {
    super(orderByElements, limitElement);
    this.selectList = selectList;
    if (tableRefList == null) {
      this.tableRefs = Lists.newArrayList();
    } else {
      this.tableRefs = tableRefList;
    }
    this.whereClause = wherePredicate;
    this.groupingExprs = groupingExprs;
    this.havingClause = havingPredicate;

    this.colLabels = Lists.newArrayList();
    this.havingPred = null;
    this.aggInfo = null;
    this.sortInfo_ = null;
  }

  /**
   * @return the original select list items from the query
   */
  public SelectList getSelectList() { return selectList; }

  /**
   * @return the HAVING clause post-analysis and with aliases resolved
   */
  public Expr getHavingPred() { return havingPred; }

  public List<TableRef> getTableRefs() { return tableRefs; }
  public Expr getWhereClause() { return whereClause; }
  public AggregateInfo getAggInfo() { return aggInfo; }
  @Override
  public SortInfo getSortInfo() { return sortInfo_; }
  @Override
  public ArrayList<String> getColLabels() { return colLabels; }
  public Expr.SubstitutionMap getBaseTblSmap() { return baseTblSmap_; }

  /**
   * Creates resultExprs and baseTblResultExprs.
   */
  @Override
  public void analyze(Analyzer analyzer)
      throws AnalysisException, AuthorizationException {
    super.analyze(analyzer);

    // Replace BaseTableRefs with ViewRefs.
    substituteViews(analyzer, tableRefs);

    // start out with table refs to establish aliases
    TableRef leftTblRef = null;  // the one to the left of tblRef
    for (TableRef tblRef: tableRefs) {
      tblRef.setLeftTblRef(leftTblRef);
      tblRef.analyze(analyzer);
      leftTblRef = tblRef;
    }

    // populate selectListExprs, aliasSMap, and colNames
    for (int i = 0; i < selectList.getItems().size(); ++i) {
      SelectListItem item = selectList.getItems().get(i);
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
        colLabels.add(label);
      }
    }

    if (whereClause != null) {
      whereClause.analyze(analyzer);
      if (whereClause.containsAggregate()) {
        throw new AnalysisException(
            "aggregate function not allowed in WHERE clause");
      }
      whereClause.checkReturnsBool("WHERE clause", false);
      analyzer.registerConjuncts(whereClause, null, true);
    }

    createSortInfo(analyzer);
    analyzeAggregation(analyzer);

    // Remember the SQL string before inline-view expression substitution.
    sqlString = toSql();

    resolveInlineViewRefs(analyzer);

    if (aggInfo != null) LOG.debug("post-analysis " + aggInfo.debugString());
  }

  /**
   * Marks all unassigned join predicates as well as exprs in aggInfo and sortInfo.
   */
  @Override
  public void materializeRequiredSlots(Analyzer analyzer) {
    // mark unassigned join predicates
    List<Expr> unassigned =
        analyzer.getUnassignedConjuncts(getTableRefIds(), true);
    List<Expr> unassignedJoinConjuncts = Lists.newArrayList();
    for (Expr e: unassigned) {
      List<TupleId> tids = Lists.newArrayList();
      e.getIds(tids, null);
      if (tids.isEmpty()) continue;

      // isOjConjunct: mark this here, getBoundPredicates() won't return it
      // for the ScanNode;
      // same if this is for an outer-joined tid and e is from the Where clause
      // TODO: figure out a better way to do this (should the ScanNode mark
      // all slots from single-tid predicates as being referenced?)
      if (tids.size() > 1
          || analyzer.isOjConjunct(e)
          || analyzer.isOuterJoined(tids.get(0))
             && e.isWhereClauseConjunct()
             && e.contains(IsNullPredicate.class)) {
        unassignedJoinConjuncts.add(e);
      }
      //if (tids.size() > 1) unassignedJoinConjuncts.add(e);
    }
    List<Expr> baseTblJoinConjuncts =
        Expr.cloneList(unassignedJoinConjuncts, baseTblSmap_);
    materializeSlots(analyzer, baseTblJoinConjuncts);

    if (aggInfo != null) {
      // mark all agg exprs needed for HAVING pred as materialized before calling
      // AggregateInfo.materializeRequiredSlots(), otherwise they won't show up in
      // AggregateInfo.getMaterializedAggregateExprs()
      if (havingPred != null) materializeSlots(analyzer, Lists.newArrayList(havingPred));
      aggInfo.materializeRequiredSlots(analyzer, baseTblSmap_);
    }

    if (sortInfo_ != null) {
      // mark ordering exprs
      List<Expr> resolvedExprs =
          Expr.cloneList(sortInfo_.getOrderingExprs(), baseTblSmap_);
      materializeSlots(analyzer, resolvedExprs);
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
    for (TableRef tblRef: tableRefs) {
      if (tblRef instanceof InlineViewRef) {
        InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
        baseTblSmap_ =
            Expr.SubstitutionMap.combine(baseTblSmap_, inlineViewRef.getBaseTblSmap());
      }
    }
    baseTblResultExprs_ = Expr.cloneList(resultExprs_, baseTblSmap_);
    LOG.info("baseTblSmap_: " + baseTblSmap_.debugString());
    LOG.info("resultExprs: " + Expr.debugString(resultExprs_));
    LOG.info("baseTblResultExprs: " + Expr.debugString(baseTblResultExprs_));
  }

  public List<TupleId> getTableRefIds() {
    List<TupleId> result = Lists.newArrayList();
    for (TableRef ref: tableRefs) {
      result.add(ref.getId());
    }
    return result;
  }

  /**
   * Expand "*" select list item.
   */
  private void expandStar(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (tableRefs.isEmpty()) {
      throw new AnalysisException("'*' expression in select list requires FROM clause.");
    }
    // expand in From clause order
    for (TableRef tableRef: tableRefs) {
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
      colLabels.add(col.getName().toLowerCase());
    }
  }

  /**
   * Analyze aggregation-relevant components of the select block (Group By clause,
   * select list, Order By clause), substitute AVG with SUM/COUNT, create the
   * AggregationInfo, including the agg output tuple, and transform all post-agg exprs
   * given AggregationInfo's smap.
   *
   * @param analyzer
   * @throws AnalysisException
   */
  private void analyzeAggregation(Analyzer analyzer)
      throws AnalysisException, AuthorizationException {
    if (groupingExprs == null && !selectList.isDistinct()
        && !Expr.containsAggregate(resultExprs_)) {
      // we're not computing aggregates
      return;
    }

    // If we're computing an aggregate, we must have a FROM clause.
    if (tableRefs.size() == 0) {
      throw new AnalysisException(
          "aggregation without a FROM clause is not allowed");
    }

    if ((groupingExprs != null || Expr.containsAggregate(resultExprs_))
        && selectList.isDistinct()) {
      throw new AnalysisException(
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    }

    // disallow '*' and explicit GROUP BY (we can't group by '*', and if you need to
    // name all star-expanded cols in the group by clause you might as well do it
    // in the select list)
    if (groupingExprs != null) {
      for (SelectListItem item : selectList.getItems()) {
        if (item.isStar()) {
          throw new AnalysisException(
              "cannot combine '*' in select list with GROUP BY: " + item.toSql());
        }
      }
    }

    // analyze grouping exprs
    ArrayList<Expr> groupingExprsCopy = Lists.newArrayList();
    if (groupingExprs != null) {
      // make a deep copy here, we don't want to modify the original
      // exprs during analysis (in case we need to print them later)
      groupingExprsCopy = Expr.cloneList(groupingExprs, null);
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
                  + groupingExprs.get(i).toSql());
        }
      }
    }

    // analyze having clause
    if (havingClause != null) {
      // substitute aliases in place (ordinals not allowed in having clause)
      havingPred = havingClause.clone(aliasSmap_);
      havingPred.analyze(analyzer);
      havingPred.checkReturnsBool("HAVING clause", true);
      analyzer.registerConjuncts(havingPred, null, false);
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
        aggInfo.getSecondPhaseDistinctAggInfo() != null
          ? aggInfo.getSecondPhaseDistinctAggInfo()
          : aggInfo;
    Expr.SubstitutionMap combinedSMap =
        Expr.SubstitutionMap.compose(avgSMap, finalAggInfo.getSMap());
    LOG.debug("combined smap: " + combinedSMap.debugString());

    // change select list, having and ordering exprs to point to agg output
    Expr.substituteList(resultExprs_, combinedSMap);
    LOG.debug("post-agg selectListExprs: " + Expr.debugString(resultExprs_));
    if (havingPred != null) {
      havingPred = havingPred.substitute(combinedSMap);
      LOG.debug("post-agg havingPred: " + havingPred.debugString());
    }
    Expr.substituteList(orderingExprs, combinedSMap);
    LOG.debug("post-agg orderingExprs: " + Expr.debugString(orderingExprs));

    // check that all post-agg exprs point to agg output
    for (int i = 0; i < selectList.getItems().size(); ++i) {
      if (!resultExprs_.get(i).isBound(finalAggInfo.getAggTupleId())) {
        throw new AnalysisException(
            "select list expression not produced by aggregation output "
            + "(missing from GROUP BY clause?): "
            + selectList.getItems().get(i).getExpr().toSql());
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
    if (havingPred != null) {
      if (!havingPred.isBound(finalAggInfo.getAggTupleId())) {
        throw new AnalysisException(
            "HAVING clause not produced by aggregation output "
            + "(missing from GROUP BY clause?): "
            + havingClause.toSql());
      }
    }
  }

  private ArrayList<FunctionCallExpr> collectAggExprs() {
    ArrayList<FunctionCallExpr> result = Lists.newArrayList();
    Expr.collectAggregateExprs(resultExprs_, result);
    if (havingPred != null) {
      havingPred.collectAggregateExprs(result);
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
      if (aggExpr.getAggOp() != BuiltinAggregateFunction.Operator.AVG) {
        continue;
      }
      // Transform avg(TIMESTAMP) to cast(avg(cast(TIMESTAMP as DOUBLE)) as TIMESTAMP)
      CastExpr inCastExpr = null;
      if (aggExpr.getChild(0).type == PrimitiveType.TIMESTAMP) {
        inCastExpr =
            new CastExpr(PrimitiveType.DOUBLE, aggExpr.getChild(0).clone(null), false);
      }

      List<Expr> sumInputExprs =
          Lists.newArrayList(aggExpr.getChild(0).type == PrimitiveType.TIMESTAMP ?
              inCastExpr : aggExpr.getChild(0).clone(null));
      List<Expr> countInputExpr = Lists.newArrayList(aggExpr.getChild(0).clone(null));

      FunctionCallExpr sumExpr =
          new FunctionCallExpr(BuiltinAggregateFunction.Operator.SUM,
              new FunctionParams(aggExpr.isDistinct(), sumInputExprs));
      FunctionCallExpr countExpr =
          new FunctionCallExpr(BuiltinAggregateFunction.Operator.COUNT,
              new FunctionParams(aggExpr.isDistinct(), countInputExpr));
      ArithmeticExpr divExpr =
          new ArithmeticExpr(ArithmeticExpr.Operator.DIVIDE, sumExpr, countExpr);

      if (aggExpr.getChild(0).type == PrimitiveType.TIMESTAMP) {
        CastExpr outCastExpr = new CastExpr(PrimitiveType.TIMESTAMP, divExpr, false);
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
    if (selectList.isDistinct()) {
       // Create aggInfo for SELECT DISTINCT ... stmt:
       // - all select list items turn into grouping exprs
       // - there are no aggregate exprs
      Preconditions.checkState(groupingExprs.isEmpty());
      Preconditions.checkState(aggExprs.isEmpty());
      aggInfo =
          AggregateInfo.create(Expr.cloneList(resultExprs_, null), null, null, analyzer);
    } else {
      aggInfo = AggregateInfo.create(groupingExprs, aggExprs, null, analyzer);
    }
  }

  /**
   * Substitute exprs of the form "<number>"  with the corresponding
   * expressions from select list
   * @param exprs
   * @param errorPrefix
   * @throws AnalysisException
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
      if (pos > selectList.getItems().size()) {
        throw new AnalysisException(
            errorPrefix + ": ordinal exceeds number of items in select list: "
            + expr.toSql());
      }
      if (selectList.getItems().get((int) pos - 1).isStar()) {
        throw new AnalysisException(
            errorPrefix + ": ordinal refers to '*' in select list: "
            + expr.toSql());
      }
      // create copy to protect against accidentally shared state
      i.set(selectList.getItems().get((int)pos - 1).getExpr().clone(null));
    }
  }

  /**
   * Returns the SQL string corresponding to this SelectStmt.
   */
  @Override
  public String toSql() {
    // Return the SQL string before inline-view expression substitution.
    if (sqlString != null) return sqlString;

    StringBuilder strBuilder = new StringBuilder();
    if (withClause_ != null) {
      strBuilder.append(withClause_.toSql());
      strBuilder.append(" ");
    }

    // Select list
    strBuilder.append("SELECT ");
    if (selectList.isDistinct()) {
      strBuilder.append("DISTINCT ");
    }
    for (int i = 0; i < selectList.getItems().size(); ++i) {
      strBuilder.append(selectList.getItems().get(i).toSql());
      strBuilder.append((i+1 != selectList.getItems().size()) ? ", " : "");
    }
    // From clause
    if (!tableRefs.isEmpty()) {
      strBuilder.append(" FROM ");
      for (int i = 0; i < tableRefs.size(); ++i) {
        strBuilder.append(tableRefs.get(i).toSql());
      }
    }
    // Where clause
    if (whereClause != null) {
      strBuilder.append(" WHERE ");
      strBuilder.append(whereClause.toSql());
    }
    // Group By clause
    if (groupingExprs != null) {
      strBuilder.append(" GROUP BY ");
      for (int i = 0; i < groupingExprs.size(); ++i) {
        strBuilder.append(groupingExprs.get(i).toSql());
        strBuilder.append((i+1 != groupingExprs.size()) ? ", " : "");
      }
    }
    // Having clause
    if (havingClause != null) {
      strBuilder.append(" HAVING ");
      strBuilder.append(havingClause.toSql());
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
    if (aggInfo != null) {
      // Return the tuple id produced in the final aggregation step.
      if (aggInfo.isDistinctAgg()) {
        tupleIdList.add(aggInfo.getSecondPhaseDistinctAggInfo().getAggTupleId());
      } else {
        tupleIdList.add(aggInfo.getAggTupleId());
      }
    } else {
      for (TableRef tblRef: tableRefs) {
        tupleIdList.addAll(tblRef.getMaterializedTupleIds());
      }
    }
  }

  private ArrayList<TableRef> cloneTableRefs() {
    ArrayList<TableRef> clone = Lists.newArrayList();
    for (TableRef tblRef : tableRefs) {
      clone.add(tblRef.clone());
    }
    return clone;
  }

  @Override
  public QueryStmt clone() {
    SelectStmt selectClone = new SelectStmt(selectList.clone(), cloneTableRefs(),
        (whereClause != null) ? whereClause.clone(null) : null,
        (groupingExprs != null) ? Expr.cloneList(groupingExprs, null) : null,
        (havingClause != null) ? havingClause.clone(null) : null,
        cloneOrderByElements(),
        (limitElement_ != null) ? limitElement_.clone(null) : null);
    selectClone.setWithClause(cloneWithClause());
    return selectClone;
  }
}
