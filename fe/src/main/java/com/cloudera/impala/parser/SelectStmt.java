// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.cloudera.impala.catalog.Column;
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
  List<Expr> selectListExprs;

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
    this.selectListExprs = new ArrayList<Expr>();
  }

  @Override
  public void analyze(Analyzer analyzer) throws Analyzer.Exception {
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
    }
    createAggInfo(analyzer);

    if (orderByElememts != null) {
      analyzeOrderByClause(analyzer);
    }
  }

  // Expand "*" select list item
  private void expandStar(Analyzer analyzer) throws Analyzer.Exception {
    // expand in From clause order
    for (TableRef tableRef: tableRefs) {
      expandStar(analyzer, tableRef.getAlias(), tableRef.getDesc());
    }
  }

  // Expand "<tbl>.*" select list item
  private void expandStar(Analyzer analyzer, TableName tblName)
      throws Analyzer.Exception {
    TupleDescriptor d = analyzer.getDescriptor(tblName);
    if (d == null) {
      throw new Analyzer.Exception("unknown table: " + tblName.toString());
    }
    expandStar(analyzer, tblName.toString(), d);
  }

  // Expand "*" for a particular tuple descriptor by appending
  // refs for each column to selectListExprs.
  private void expandStar(Analyzer analyzer, String alias,
                          TupleDescriptor desc)
      throws Analyzer.Exception {
    for (Column col: desc.getTable().getColumns()) {
      selectListExprs.add(
          new SlotRef(new TableName(null, alias), col.getName()));
    }
  }

  private void createAggInfo(Analyzer analyzer) throws Analyzer.Exception {
    if (groupingExprs == null && !Expr.contains(selectListExprs, AggregateExpr.class)) {
      // we're not computing aggregates
      return;
    }

    ArrayList<Expr> groupingExprsCopy = null;
    if (groupingExprs != null) {
      // make a deep copy here, we don't want to modify the original
      // exprs during analysis (in case we need to print the statement)
      groupingExprsCopy = Expr.cloneList(groupingExprs, null);
      substituteOrdinals(groupingExprsCopy, "GROUP BY");
      Expr.substituteList(groupingExprsCopy, aliasSubstMap);
      for (int i = 0; i < groupingExprsCopy.size(); ++i) {
        groupingExprsCopy.get(i).analyze(analyzer);
        if (groupingExprsCopy.get(i).contains(AggregateExpr.class)) {
          // reference the original expr in the error msg
          throw new Analyzer.Exception(
              "GROUP BY expression must not contain aggregate functions: "
              + groupingExprs.get(i).toSql());
        }
        if (groupingExprsCopy.get(i).getType().isFloatingPointType()) {
          throw new Analyzer.Exception(
              "GROUP BY expression must have a discrete (non-floating point) type: "
              + groupingExprs.get(i).toSql());
        }
      }
    }

    ArrayList<AggregateExpr> aggExprs = null;
    if (Expr.contains(selectListExprs, AggregateExpr.class)) {
      aggExprs = Lists.newArrayList();
      Expr.collectList(selectListExprs, AggregateExpr.class, aggExprs);
    }

    aggInfo = new AggregateInfo(groupingExprsCopy, aggExprs);
    aggInfo.createAggTuple(analyzer.getDescTbl());

  }

  private void analyzeOrderByClause(Analyzer analyzer) throws Analyzer.Exception {
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

  // Substitute exprs of the form "<number>"  with the corresponding
  // expressions from select list
  private void substituteOrdinals(List<Expr> exprs, String errorPrefix)
      throws Analyzer.Exception{
    // substitute ordinals
    ListIterator<Expr> i = exprs.listIterator();
    while (i.hasNext()) {
      Expr expr = i.next();
      if (!(expr instanceof IntLiteral)) {
        continue;
      }
      long pos = ((IntLiteral) expr).getValue();
      if (pos < 1) {
        throw new Analyzer.Exception(
            errorPrefix + ": ordinal must be >= 1: " + expr.toSql());
      }
      if (pos > selectList.size()) {
        throw new Analyzer.Exception(
            errorPrefix + ": ordinal exceeds number of items in select list: "
            + expr.toSql());
      }
      if (selectList.get((int) pos - 1).isStar()) {
        throw new Analyzer.Exception(
            errorPrefix + ": ordinal refers to '*' in select list: "
            + expr.toSql());
      }
      // create copy to protect against accidentally shared state
      i.set(selectList.get((int)pos - 1).getExpr().clone(null));
    }
  }
}
