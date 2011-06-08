// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.collect.Lists;

class SelectStmt extends ParseNodeBase {
  ArrayList<SelectListItem> selectList;
  List<TableRef> tableRefs;
  Predicate whereClause;
  ArrayList<Expr> groupingExprs;
  Predicate havingClause;
  ArrayList<OrderByElement> orderByElememts;
  long limit;

  // map from SlotRef(alias) to corresp. select list expr
  Expr.SubstitutionMap aliasSubstMap;

  // star-expanded list of exprs in select clause
  ArrayList<Expr> selectListExprs;

  AggregateInfo aggInfo;

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
    this.orderByElememts = orderByElements;
    this.limit = limit;

    this.aliasSubstMap = new Expr.SubstitutionMap();
    this.selectListExprs = Lists.newArrayList();
  }

  public List<TableRef> getTableRefs() {
    return tableRefs;
  }

  public Predicate getWhereClause() {
    return whereClause;
  }

  public Predicate getHavingClause() {
    return havingClause;
  }

  public ArrayList<OrderByElement> getOrderByElememts() {
    return orderByElememts;
  }

  public long getLimit() {
    return limit;
  }

  public ArrayList<Expr> getSelectListExprs() {
    return selectListExprs;
  }

  public AggregateInfo getAggInfo() {
    return aggInfo;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // start out with table refs to establish aliases
    for (TableRef tblRef: tableRefs) {
      tblRef.setDesc(analyzer.registerTableRef(tblRef));
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
    }
    createAggInfo(analyzer);

    if (orderByElememts != null) {
      analyzeOrderByClause(analyzer);
    }
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
   * select list), substite AVG with SUM/COUNT, create the AggregationInfo, including
   * the agg output tuple, and transform all post-agg exprs given
   * AggregationInfo's substmap.
   * @param analyzer
   * @throws AnalysisException
   */
  private void createAggInfo(Analyzer analyzer) throws AnalysisException {
    if (groupingExprs == null && !Expr.contains(selectListExprs, AggregateExpr.class)) {
      // we're not computing aggregates
      return;
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
      havingClause = (Predicate) havingClause.substitute(aliasSubstMap);
      havingClause.analyze(analyzer);
    }

    // build substmap AVG -> SUM/COUNT;
    // assumes that select list and having clause have been analyzed
    ArrayList<AggregateExpr> aggExprs = Lists.newArrayList();
    Expr.collectList(selectListExprs, AggregateExpr.class, aggExprs);
    if (havingClause != null) {
      havingClause.collect(AggregateExpr.class, aggExprs);
    }

    Expr.SubstitutionMap avgSubstMap = new Expr.SubstitutionMap();
    for (AggregateExpr aggExpr: aggExprs) {
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

    // substitute select list and having clause
    Expr.substituteList(selectListExprs, avgSubstMap);
    if (havingClause != null) {
      havingClause = (Predicate) havingClause.substitute(avgSubstMap);
    }

    // collect agg exprs again
    aggExprs.clear();
    Expr.collectList(selectListExprs, AggregateExpr.class, aggExprs);
    if (havingClause != null) {
      havingClause.collect(AggregateExpr.class, aggExprs);
    }
    aggInfo = new AggregateInfo(groupingExprsCopy, aggExprs);
    aggInfo.createAggTuple(analyzer.getDescTbl());

    // change all post-agg exprs to point to agg output
    Expr.substituteList(selectListExprs, aggInfo.getAggTupleSubstMap());
    if (havingClause != null) {
      havingClause = (Predicate) havingClause.substitute(aggInfo.getAggTupleSubstMap());
    }
  }

  private void analyzeOrderByClause(Analyzer analyzer) throws AnalysisException {
    ArrayList<Expr> orderingExprs = Lists.newArrayList();
    // extract exprs
    for (OrderByElement orderByExpr: orderByElememts) {
      // create copies, we don't want to modify the original parse node, in case
      // we need to print it
      orderingExprs.add(orderByExpr.getExpr().clone());
    }
    substituteOrdinals(orderingExprs, "ORDER BY");
    Expr.substituteList(orderingExprs, aliasSubstMap);
    Expr.analyze(orderingExprs, analyzer);
  }

  /**
   * Substitute exprs of the form "<number>"  with the corresponding
   *expressions from select list
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
