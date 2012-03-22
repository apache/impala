// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Representation of a single select block, including GROUP BY, ORDERY BY and HAVING clauses.
 *
 */
public class SelectStmt extends ParseNodeBase {
  private final static Logger LOG = LoggerFactory.getLogger(SelectStmt.class);

  private final SelectList selectList;
  private final ArrayList<String> colLabels; // lower case column labels
  private final List<TableRef> tableRefs;
  private final Predicate whereClause;
  private final ArrayList<Expr> groupingExprs;
  private final Predicate havingClause;  // original having clause
  private final ArrayList<OrderByElement> orderByElements;
  private final long limit;

  /**  map from SlotRef(alias) to corresp. select list expr */
  private final Expr.SubstitutionMap aliasSMap;

  /**
   * list of executable exprs in select clause (star-expanded, ordinals and
   * aliases substituted, agg output substituted
   */
  private final ArrayList<Expr> selectListExprs;

  /**  havingClause with aliases and agg output resolved */
  private Predicate havingPred;

  /** set if we have any kind of aggregation operation, include SELECT DISTINCT */
  private AggregateInfo aggInfo;

  /** set if we have DISTINCT aggregate functions */
  private AggregateInfo mergeAggInfo;

  private SortInfo sortInfo;

  SelectStmt(SelectList selectList,
             List<TableRef> tableRefList,
             Predicate wherePredicate, ArrayList<Expr> groupingExprs,
             Predicate havingPredicate, ArrayList<OrderByElement> orderByElements,
             long limit) {
    this.selectList = selectList;
    if (tableRefList == null) {
      this.tableRefs = Lists.newArrayList();
    } else {
      this.tableRefs = tableRefList;
    }
    this.whereClause = wherePredicate;
    this.groupingExprs = groupingExprs;
    this.havingClause = havingPredicate;
    this.orderByElements = orderByElements;
    this.limit = limit;

    this.aliasSMap = new Expr.SubstitutionMap();
    this.selectListExprs = Lists.newArrayList();
    this.colLabels = Lists.newArrayList();
    this.havingPred = null;
    this.aggInfo = null;
    this.sortInfo = null;
  }

  /**
   * @return the original select list items from the query
   */
  public SelectList getSelectList() {
    return selectList;
  }

  /**
   * @return the list of post-analysis exprs corresponding to the
   * select list from the original query ('*'-expanded)
   */
  public ArrayList<Expr> getSelectListExprs() {
    return selectListExprs;
  }

  /**
   * @return the HAVING clause post-analysis and with aliases resolved
   */
  public Predicate getHavingPred() {
    return havingPred;
  }

  public List<TableRef> getTableRefs() {
    return tableRefs;
  }

  public Predicate getWhereClause() {
    return whereClause;
  }

  public ArrayList<OrderByElement> getOrderByElements() {
    return orderByElements;
  }

  public boolean hasOrderByClause() {
    return orderByElements != null;
  }

  public long getLimit() {
    return limit;
  }

  public AggregateInfo getAggInfo() {
    return aggInfo;
  }

  public AggregateInfo getMergeAggInfo() {
    return mergeAggInfo;
  }

  public SortInfo getSortInfo() {
    return sortInfo;
  }

  public ArrayList<String> getColLabels() {
    return colLabels;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
    // start out with table refs to establish aliases
    TableRef leftTblRef = null;  // the one to the left of tblRef
    for (TableRef tblRef: tableRefs) {
      tblRef.setLeftTblRef(leftTblRef);
      tblRef.analyze(analyzer);
      leftTblRef = tblRef;
    }

    // populate selectListExprs, aliasSMap, and colNames
    for (SelectListItem item: selectList.getItems()) {
      if (item.isStar()) {
        TableName tblName = item.getTblName();
        if (tblName == null) {
          expandStar(analyzer);
        } else {
          expandStar(analyzer, tblName);
        }
      } else {
        selectListExprs.add(item.getExpr());
        if (item.getAlias() != null) {
          aliasSMap.lhs.add(
              new SlotRef(null, item.getAlias().toLowerCase()));
          aliasSMap.rhs.add(item.getExpr().clone(null));
          colLabels.add(item.getAlias().toLowerCase());
        } else {
          colLabels.add(item.toSql().toLowerCase());
        }
      }
    }

    // analyze selectListExprs
    Expr.analyze(selectListExprs, analyzer);

    if (whereClause != null) {
      whereClause.analyze(analyzer);
      if (whereClause.contains(AggregateExpr.class)) {
        throw new AnalysisException(
            "aggregation function not allowed in WHERE clause");
      }
      analyzer.registerConjuncts(whereClause);
    }

    createSortInfo(analyzer);
    analyzeAggregation(analyzer);
  }

  /**
   * Expand "*" select list item.
   * @param analyzer
   * @throws AnalysisException
   */
  private void expandStar(Analyzer analyzer) throws AnalysisException {
    // expand in From clause order
    for (TableRef tableRef: tableRefs) {
      expandStar(analyzer, tableRef.getAlias(), tableRef.getDesc());
    }
  }

  /**
   * Expand "<tbl>.*" select list item.
   * @param analyzer
   * @param tblName
   * @throws AnalysisException
   */
  private void expandStar(Analyzer analyzer, TableName tblName)
      throws AnalysisException {
    TupleDescriptor d = analyzer.getDescriptor(tblName);
    if (d == null) {
      throw new AnalysisException("unknown table: " + tblName.toString());
    }
    expandStar(analyzer, tblName.toString(), d);
  }

  /**
   * Expand "*" for a particular tuple descriptor by appending
   * refs for each column to selectListExprs.
   * @param analyzer
   * @param alias
   * @param desc
   * @throws AnalysisException
   */
  private void expandStar(Analyzer analyzer, String alias,
                          TupleDescriptor desc)
      throws AnalysisException {
    for (Column col: desc.getTable().getColumns()) {
      selectListExprs.add(
          new SlotRef(new TableName(null, alias), col.getName()));
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
      throws AnalysisException, InternalException {
    if (groupingExprs == null && !selectList.isDistinct()
        && !Expr.contains(selectListExprs, AggregateExpr.class)) {
      // we're not computing aggregates
      return;
    }

    if ((groupingExprs != null || Expr.contains(selectListExprs, AggregateExpr.class))
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
      Expr.substituteList(groupingExprsCopy, aliasSMap);
      for (int i = 0; i < groupingExprsCopy.size(); ++i) {
        groupingExprsCopy.get(i).analyze(analyzer);
        if (groupingExprsCopy.get(i).contains(AggregateExpr.class)) {
          // reference the original expr in the error msg
          throw new AnalysisException(
              "GROUP BY expression must not contain aggregate functions: "
                  + groupingExprs.get(i).toSql());
        }
        if (groupingExprsCopy.get(i).getType().isFloatingPointType()) {
          throw new AnalysisException(
              "GROUP BY expression must have a discrete (non-floating point) type: "
                  + groupingExprs.get(i).toSql());
        }
      }
    }

    // analyze having clause
    if (havingClause != null) {
      // substitute aliases in place (ordinals not allowed in having clause)
      havingPred = (Predicate) havingClause.clone(aliasSMap);
      havingPred.analyze(analyzer);
    }

    List<Expr> orderingExprs = null;
    if (sortInfo != null) {
      orderingExprs = sortInfo.getOrderingExprs();
    }

    ArrayList<AggregateExpr> aggExprs = collectAggExprs();
    Expr.SubstitutionMap avgSMap = createAvgSMap(aggExprs, analyzer);

    // substitute AVG before constructing AggregateInfo
    Expr.substituteList(aggExprs, avgSMap);
    ArrayList<AggregateExpr> nonAvgAggExprs = Lists.newArrayList();
    Expr.collectList(aggExprs, AggregateExpr.class, nonAvgAggExprs);
    aggExprs = nonAvgAggExprs;
    LOG.info("aggExprs=" + Expr.debugString(aggExprs));
    createAggInfo(groupingExprsCopy, aggExprs, analyzer);
    LOG.info("agg smap: " + aggInfo.getSMap().debugString());

    // combine avg smap with the one that produces the final agg output
    AggregateInfo finalAggInfo = mergeAggInfo != null ? mergeAggInfo : aggInfo;
    Expr.SubstitutionMap combinedSMap =
        Expr.SubstitutionMap.combine(avgSMap, finalAggInfo.getSMap());
    LOG.info("combined smap: " + combinedSMap.debugString());

    // change select list, having and ordering exprs to point to agg output
    Expr.substituteList(selectListExprs, combinedSMap);
    LOG.info("post-agg selectListExprs: " + Expr.debugString(selectListExprs));
    if (havingPred != null) {
      havingPred = (Predicate) havingPred.substitute(combinedSMap);
      LOG.info("post-agg havingPred: " + havingPred.debugString());
    }
    Expr.substituteList(orderingExprs, combinedSMap);
    LOG.info("post-agg orderingExprs: " + Expr.debugString(orderingExprs));

    // check that all post-agg exprs point to agg output
    for (int i = 0; i < selectList.getItems().size(); ++i) {
      if (!selectListExprs.get(i).isBound(finalAggInfo.getAggTupleId())) {
        throw new AnalysisException(
            "select list expression not produced by aggregation output "
            + "(missing from GROUP BY clause?): "
            + selectList.getItems().get(i).getExpr().toSql());
      }
    }
    if (orderByElements != null) {
      for (int i = 0; i < orderByElements.size(); ++i) {
        if (!orderingExprs.get(i).isBound(finalAggInfo.getAggTupleId())) {
          throw new AnalysisException(
              "ORDER BY expression not produced by aggregation output "
              + "(missing from GROUP BY clause?): "
              + orderByElements.get(i).getExpr().toSql());
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

  private ArrayList<AggregateExpr> collectAggExprs() {
    ArrayList<AggregateExpr> result = Lists.newArrayList();
    Expr.collectList(selectListExprs, AggregateExpr.class, result);
    if (havingPred != null) {
      havingPred.collect(AggregateExpr.class, result);
    }
    if (sortInfo != null) {
      Expr.collectList(sortInfo.getOrderingExprs(), AggregateExpr.class, result);
    }
    return result;
  }

  /**
   * Build smap AVG -> SUM/COUNT;
   * assumes that select list and having clause have been analyzed.
   */
  private Expr.SubstitutionMap createAvgSMap(
      ArrayList<AggregateExpr> aggExprs, Analyzer analyzer) throws AnalysisException {
    Expr.SubstitutionMap result = new Expr.SubstitutionMap();
    for (AggregateExpr aggExpr : aggExprs) {
      if (aggExpr.getOp() != AggregateExpr.Operator.AVG) {
        continue;
      }
      // Transform avg(TIMESTAMP) to cast(avg(cast(TIMESTAMP as DOUBLE)) as TIMESTAMP)
      CastExpr inCastExpr = null;
      if (aggExpr.getChild(0).type == PrimitiveType.TIMESTAMP) {
        inCastExpr =
            new CastExpr(PrimitiveType.DOUBLE, aggExpr.getChild(0).clone(), false);
      }

      AggregateExpr sumExpr =
          new AggregateExpr(AggregateExpr.Operator.SUM, false, aggExpr.isDistinct(),
                Lists.newArrayList(aggExpr.getChild(0).type == PrimitiveType.TIMESTAMP ?
                  inCastExpr : aggExpr.getChild(0).clone()));
      AggregateExpr countExpr =
          new AggregateExpr(AggregateExpr.Operator.COUNT, false, aggExpr.isDistinct(),
                            Lists.newArrayList(aggExpr.getChild(0).clone()));
      ArithmeticExpr divExpr =
          new ArithmeticExpr(ArithmeticExpr.Operator.DIVIDE, sumExpr, countExpr);

      if (aggExpr.getChild(0).type == PrimitiveType.TIMESTAMP) {
        CastExpr outCastExpr = new CastExpr(PrimitiveType.TIMESTAMP, divExpr, false);
        outCastExpr.analyze(analyzer);
        result.rhs.add(outCastExpr);
      } else {
        divExpr.analyze(analyzer);
        result.rhs.add(divExpr);
      }
      result.lhs.add(aggExpr);
    }
    LOG.info("avg smap: " + result.debugString());
    return result;
  }

  /**
   * Create aggInfo and possibly mergeAggInfo for the given grouping and agg exprs.
   */
  private void createAggInfo(ArrayList<Expr> groupingExprs,
      ArrayList<AggregateExpr> aggExprs, Analyzer analyzer)
      throws AnalysisException, InternalException {
    if (selectList.isDistinct()) {
      Preconditions.checkState(groupingExprs.isEmpty());
      Preconditions.checkState(aggExprs.isEmpty());
      createSelectDistinctInfo(analyzer.getDescTbl());
    } else {
      Preconditions.checkState(!aggExprs.isEmpty());
      // collect agg exprs with DISTINCT clause
      ArrayList<AggregateExpr> distinctAggExprs = Lists.newArrayList();
      for (AggregateExpr aggExpr: aggExprs) {
        if (aggExpr.isDistinct()) {
          distinctAggExprs.add(aggExpr);
        }
      }

      if (distinctAggExprs.isEmpty()) {
        aggInfo = new AggregateInfo(groupingExprs, aggExprs);
        aggInfo.createAggTuple(analyzer.getDescTbl());
      } else {
        createDistinctAggInfo(groupingExprs, distinctAggExprs, aggExprs, analyzer);
      }
    }
  }

  /**
   * Create aggInfo for SELECT DISTINCT ... stmt:
   * - all select list items turn into grouping exprs
   * - there are no aggregate exprs
   */
  private void createSelectDistinctInfo(DescriptorTable descTbl) {
    ArrayList<Expr> groupingExprs = Expr.cloneList(selectListExprs, null);
    aggInfo = new AggregateInfo(groupingExprs, null);
    aggInfo.createAggTuple(descTbl);
  }

  /**
   * Create aggregate info for select block containing aggregate exprs with
   * DISTINCT clause. At the moment, we require that all distinct aggregate
   * functions be applied to the same set of exprs (ie, we can't do something
   * like SELECT COUNT(DISTINCT id), COUNT(DISTINCT address)).
   * Aggregation happens in two successive phases:
   * - the first phase aggregates by all grouping exprs plus all parameter exprs
   *   of DISTINCT aggregate functions
   * - the second phase re-aggregates the output of the first phase by
   *   grouping by the original grouping exprs and performing a merge aggregation
   *   (ie, COUNT turns into SUM)
   *
   * Example:
   *   SELECT a, COUNT(DISTINCT b, c), MIN(d), COUNT(*) FROM T GROUP BY a
   * - 1st phase grouping exprs: a, b, c
   * - 1st phase agg exprs: MIN(d), COUNT(*)
   * - 2nd phase grouping exprs: a
   * - 2nd phase agg exprs: COUNT(*), MIN(<MIN(d) from 1st phase>),
   *     SUM(<COUNT(*) from 1st phase>)
   *
   * TODO: expand implementation to cover the general case; this will require
   * a different execution strategy
   */
  private void createDistinctAggInfo(ArrayList<Expr> groupingExprs,
      ArrayList<AggregateExpr> distinctAggExprs, ArrayList<AggregateExpr> aggExprs,
      Analyzer analyzer) throws AnalysisException, InternalException {
    Preconditions.checkState(!distinctAggExprs.isEmpty());
    // make sure that all DISTINCT params are the same;
    // ignore top-level implicit casts in the comparison, we might have inserted
    // those during analysis
    ArrayList<Expr> expr0Children = Lists.newArrayList();
    for (Expr expr: distinctAggExprs.get(0).getChildren()) {
      expr0Children.add(expr.ignoreImplicitCast());
    }
    for (int i = 1; i < distinctAggExprs.size(); ++i) {
      ArrayList<Expr> exprIChildren = Lists.newArrayList();
      for (Expr expr: distinctAggExprs.get(i).getChildren()) {
        exprIChildren.add(expr.ignoreImplicitCast());
      }
      if (!Expr.equalLists(expr0Children, exprIChildren)) {
        throw new AnalysisException(
            "all DISTINCT aggregate functions need to have the same set of parameters: "
            + distinctAggExprs.get(1).toSql());
      }
    }

    // add DISTINCT parameters to grouping exprs
    ArrayList<Expr> phase1GroupingExprs = Lists.newArrayList();
    phase1GroupingExprs.addAll(groupingExprs);
    phase1GroupingExprs.addAll(distinctAggExprs.get(0).getChildren());

    // remove DISTINCT aggregate functions from aggExprs
    ArrayList<AggregateExpr> phase1AggExprs = Lists.newArrayList();
    phase1AggExprs.addAll(aggExprs);
    phase1AggExprs.removeAll(distinctAggExprs);

    aggInfo = new AggregateInfo(phase1GroupingExprs, phase1AggExprs);
    aggInfo.createAggTuple(analyzer.getDescTbl());
    LOG.info("distinct agg smap, phase 1: " + aggInfo.getSMap().debugString());

    // create merge agg info
    mergeAggInfo =
        AggregateInfo.createDistinctMergeAggInfo(aggInfo, distinctAggExprs, analyzer);
    LOG.info("distinct agg smap, phase 2: " + mergeAggInfo.getSMap().debugString());
  }

  private void createSortInfo(Analyzer analyzer) throws AnalysisException {
    if (orderByElements == null) {
      // not computing order by
      return;
    }

    ArrayList<Expr> orderingExprs = Lists.newArrayList();
    ArrayList<Boolean> isAscOrder = Lists.newArrayList();

    // extract exprs
    for (OrderByElement orderByElement: orderByElements) {
      // create copies, we don't want to modify the original parse node, in case
      // we need to print it
      orderingExprs.add(orderByElement.getExpr().clone());
      isAscOrder.add(Boolean.valueOf(orderByElement.getIsAsc()));
    }
    substituteOrdinals(orderingExprs, "ORDER BY");
    Expr.substituteList(orderingExprs, aliasSMap);
    Expr.analyze(orderingExprs, analyzer);

    sortInfo = new SortInfo(orderingExprs, isAscOrder);
  }

  /**
   * Substitute exprs of the form "<number>"  with the corresponding
   * expressions from select list
   * @param exprs
   * @param errorPrefix
   * @throws AnalysisException
   */
  private void substituteOrdinals(List<Expr> exprs, String errorPrefix)
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

  @Override
  public String toSql() {
    StringBuilder strBuilder = new StringBuilder();
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
    strBuilder.append(" FROM ");
    for (int i = 0; i < tableRefs.size(); ++i) {
      strBuilder.append(tableRefs.get(i).toSql());
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
        strBuilder.append(groupingExprs.get(i). toSql());
        strBuilder.append((i+1 != groupingExprs.size()) ? ", " : "");
      }
    }
    // Having clause
    if (havingClause != null) {
      strBuilder.append(" HAVING ");
      strBuilder.append(havingClause.toSql());
    }
    // Order By clause
    if (orderByElements != null) {
      strBuilder.append(" ORDER BY ");
      for (int i = 0; i < orderByElements.size(); ++i) {
        strBuilder.append(orderByElements.get(i).getExpr().toSql());
        strBuilder.append((sortInfo.getIsAscOrder().get(i)) ? " ASC" : " DESC");
        strBuilder.append((i+1 != orderByElements.size()) ? ", " : "");
      }
    }
    return strBuilder.toString();
  }
}
