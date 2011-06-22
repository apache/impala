// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.collect.Lists;

/**
 * Representation of a single select block, including GROUP BY, ORDERY BY and HAVING clauses.
 *
 */
public class SelectStmt extends ParseNodeBase {
  private final static Logger LOG = LoggerFactory.getLogger(SelectStmt.class);

  private final ArrayList<SelectListItem> selectList;
  private final List<TableRef> tableRefs;
  private final Predicate whereClause;
  private final ArrayList<Expr> groupingExprs;
  private final Predicate havingClause;  // original having clause
  private final ArrayList<OrderByElement> orderByElements;
  private final long limit;

  /**  map from SlotRef(alias) to corresp. select list expr */
  private final Expr.SubstitutionMap aliasSubstMap;

  /**
   * list of executable exprs in select clause (star-expanded, ordinals and
   * aliases substituted, agg output substituted
   */
  private final ArrayList<Expr> selectListExprs;

  /**  list of ordering exprs with ordinals, aliases and agg output resolved */
  private List<Expr> orderingExprs;

  /** list of ordering directions; mirrors orderingExprs */
  private List<Boolean> isAscOrder;

  /**  havingClause with aliases and agg output resolved */
  private Predicate havingPred;

  private AggregateInfo aggInfo;

  SelectStmt(ArrayList<SelectListItem> selectList,
             List<TableRef> tableRefList,
             Predicate wherePredicate, ArrayList<Expr> groupingExprs,
             Predicate havingPredicate, ArrayList<OrderByElement> orderByElements,
             long limit) {
    this.selectList = selectList;
    this.tableRefs = tableRefList;
    this.whereClause = wherePredicate;
    this.groupingExprs = groupingExprs;
    this.havingClause = havingPredicate;
    this.orderByElements = orderByElements;
    this.limit = limit;

    this.aliasSubstMap = new Expr.SubstitutionMap();
    this.selectListExprs = Lists.newArrayList();
    this.orderingExprs = null;
    this.isAscOrder = null;
    this.havingPred = null;
    this.aggInfo = null;
  }

  /**
   * @return the original select list items from the query
   */
  public ArrayList<SelectListItem> getSelectList() {
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
   * @return the list of post-analysis exprs corresponding to the
   * ORDER BY clause (aliases and ordinals resolved)
   */
  public List<Expr> getOrderingExprs() {
    return orderingExprs;
  }

  /**
   * @return a list of bools corresponding to the explicit or implicit
   * ordering directions of the ORDER BY clause; true = ASC, false = DESC
   */
  public List<Boolean> getOrderingDirections() {
    return isAscOrder;
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

  public long getLimit() {
    return limit;
  }

  public AggregateInfo getAggInfo() {
    return aggInfo;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // start out with table refs to establish aliases
    TableRef leftTblRef = null;  // the one to the left of tblRef
    for (TableRef tblRef: tableRefs) {
      tblRef.setDesc(analyzer.registerTableRef(tblRef));
      tblRef.expandUsingClause(leftTblRef, analyzer.getCatalog());
      if (tblRef.getOnClause() != null) {
        tblRef.getOnClause().analyze(analyzer);
        analyzer.registerPredicate(tblRef.getOnClause());
      }
      leftTblRef = tblRef;
    }

    // populate selectListExprs and aliasSubstMap
    for (SelectListItem item: selectList) {
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
          aliasSubstMap.lhs.add(
              new SlotRef(null, item.getAlias().toLowerCase()));
          aliasSubstMap.rhs.add(item.getExpr().clone(null));
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
      analyzer.registerPredicate(whereClause);
    }
    if (orderByElements != null) {
      analyzeOrderByClause(analyzer);
    }
    createAggInfo(analyzer);
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
    }
  }

  /**
   * Analyze aggregation-relevant components of the select block (Group By clause,
   * select list, Order By clause), substite AVG with SUM/COUNT, create the
   * AggregationInfo, including the agg output tuple, and transform all post-agg exprs given
   * AggregationInfo's substmap.
   *
   * @param analyzer
   * @throws AnalysisException
   */
  private void createAggInfo(Analyzer analyzer) throws AnalysisException {
    if (groupingExprs == null && !Expr.contains(selectListExprs, AggregateExpr.class)) {
      // we're not computing aggregates
      return;
    }

    // disallow '*' and aggregation (we can't group by '*', and if you need to
    // name all star-expanded cols in the group by clause you might as well do it
    // in the select list)
    for (SelectListItem item : selectList) {
      if (item.isStar()) {
        throw new AnalysisException(
            "cannot combine '*' in select list with aggregation: " + item.toSql());
      }
    }

    // analyze grouping exprs
    ArrayList<Expr> groupingExprsCopy = null;
    if (groupingExprs != null) {
      // make a deep copy here, we don't want to modify the original
      // exprs during analysis (in case we need to print them)
      groupingExprsCopy = Expr.cloneList(groupingExprs, null);
      substituteOrdinals(groupingExprsCopy, "GROUP BY");
      Expr.substituteList(groupingExprsCopy, aliasSubstMap);
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
      havingPred = (Predicate) havingClause.clone(aliasSubstMap);
      havingPred.analyze(analyzer);
    }

    // build substmap AVG -> SUM/COUNT;
    // assumes that select list and having clause have been analyzed
    ArrayList<AggregateExpr> aggExprs = Lists.newArrayList();
    Expr.collectList(selectListExprs, AggregateExpr.class, aggExprs);
    if (havingPred != null) {
      havingPred.collect(AggregateExpr.class, aggExprs);
    }
    if (orderingExprs != null) {
      Expr.collectList(orderingExprs, AggregateExpr.class, aggExprs);
    }

    Expr.SubstitutionMap avgSubstMap = new Expr.SubstitutionMap();
    for (AggregateExpr aggExpr : aggExprs) {
      if (aggExpr.getOp() != AggregateExpr.Operator.AVG) {
        continue;
      }
      AggregateExpr sumExpr =
          new AggregateExpr(AggregateExpr.Operator.SUM, false, false,
                            Lists.newArrayList(aggExpr.getChild(0).clone()));
      AggregateExpr countExpr =
          new AggregateExpr(AggregateExpr.Operator.COUNT, false, false,
                            Lists.newArrayList(aggExpr.getChild(0).clone()));
      ArithmeticExpr divExpr =
          new ArithmeticExpr(ArithmeticExpr.Operator.DIVIDE, sumExpr, countExpr);
      divExpr.analyze(analyzer);
      avgSubstMap.lhs.add(aggExpr);
      avgSubstMap.rhs.add(divExpr);
    }

    // substitute select list, having clause, order by clause
    Expr.substituteList(selectListExprs, avgSubstMap);
    if (havingPred != null) {
      havingPred = (Predicate) havingPred.substitute(avgSubstMap);
    }
    Expr.substituteList(orderingExprs, avgSubstMap);

    // collect agg exprs again
    aggExprs.clear();
    Expr.collectList(selectListExprs, AggregateExpr.class, aggExprs);
    if (havingPred != null) {
      havingPred.collect(AggregateExpr.class, aggExprs);
    }
    if (orderingExprs != null) {
      Expr.collectList(orderingExprs, AggregateExpr.class, aggExprs);
    }
    aggInfo = new AggregateInfo(groupingExprsCopy, aggExprs);
    aggInfo.createAggTuple(analyzer.getDescTbl());

    // change select list, having and ordering exprs to point to agg output
    LOG.debug("agg substmap: " + aggInfo.getAggTupleSubstMap().debugString());
    Expr.substituteList(selectListExprs, aggInfo.getAggTupleSubstMap());
    LOG.debug("post-agg selectListExprs: " + Expr.debugString(selectListExprs));
    if (havingPred != null) {
      havingPred = (Predicate) havingPred.substitute(aggInfo.getAggTupleSubstMap());
      LOG.debug("post-agg havingPred: " + havingPred.debugString());
    }
    Expr.substituteList(orderingExprs, aggInfo.getAggTupleSubstMap());
    LOG.debug("post-agg orderingExprs: " + Expr.debugString(orderingExprs));

    // check that all post-agg exprs point to agg output
    for (int i = 0; i < selectList.size(); ++i) {
      if (!selectListExprs.get(i).isBound(aggInfo.getAggTupleId())) {
        throw new AnalysisException(
            "select list expression not produced by aggregation output "
            + "(missing from GROUP BY clause?): "
            + selectList.get(i).getExpr().toSql());
      }
    }
    if (orderByElements != null) {
      for (int i = 0; i < orderByElements.size(); ++i) {
        if (!orderingExprs.get(i).isBound(aggInfo.getAggTupleId())) {
          throw new AnalysisException(
              "ORDER BY expression not produced by aggregation output "
              + "(missing from GROUP BY clause?): "
              + orderByElements.get(i).getExpr().toSql());
        }
      }
    }
    if (havingPred != null) {
      if (!havingPred.isBound(aggInfo.getAggTupleId())) {
        throw new AnalysisException(
            "HAVING clause not produced by aggregation output "
            + "(missing from GROUP BY clause?): "
            + havingClause.toSql());
      }
    }
  }

  private void analyzeOrderByClause(Analyzer analyzer) throws AnalysisException {
    orderingExprs = Lists.newArrayList();
    isAscOrder = Lists.newArrayList();
    // extract exprs
    for (OrderByElement orderByElement: orderByElements) {
      // create copies, we don't want to modify the original parse node, in case
      // we need to print it
      orderingExprs.add(orderByElement.getExpr().clone());
      isAscOrder.add(Boolean.valueOf(orderByElement.getIsAsc()));
    }
    substituteOrdinals(orderingExprs, "ORDER BY");
    Expr.substituteList(orderingExprs, aliasSubstMap);
    Expr.analyze(orderingExprs, analyzer);
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
      if (pos > selectList.size()) {
        throw new AnalysisException(
            errorPrefix + ": ordinal exceeds number of items in select list: "
            + expr.toSql());
      }
      if (selectList.get((int) pos - 1).isStar()) {
        throw new AnalysisException(
            errorPrefix + ": ordinal refers to '*' in select list: "
            + expr.toSql());
      }
      // create copy to protect against accidentally shared state
      i.set(selectList.get((int)pos - 1).getExpr().clone(null));
    }
  }
}
