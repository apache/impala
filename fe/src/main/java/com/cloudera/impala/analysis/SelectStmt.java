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

  SelectStmt(SelectList selectList,
             List<TableRef> tableRefList,
             Expr wherePredicate, ArrayList<Expr> groupingExprs,
             Expr havingPredicate, ArrayList<OrderByElement> orderByElements,
             long limit) {
    super(orderByElements, limit);
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
    this.sortInfo = null;
  }

  /**
   * @return the original select list items from the query
   */
  public SelectList getSelectList() {
    return selectList;
  }

  /**
   * @return the HAVING clause post-analysis and with aliases resolved
   */
  public Expr getHavingPred() {
    return havingPred;
  }

  public List<TableRef> getTableRefs() {
    return tableRefs;
  }

  public Expr getWhereClause() {
    return whereClause;
  }

  public AggregateInfo getAggInfo() {
    return aggInfo;
  }

  @Override
  public SortInfo getSortInfo() {
    return sortInfo;
  }

  @Override
  public ArrayList<String> getColLabels() {
    return colLabels;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
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
        resultExprs.add(item.getExpr());
        SlotRef aliasRef = new SlotRef(null, item.toColumnLabel());
        if (aliasSMap.lhs.contains(aliasRef)) {
          // If we have already seen this alias, it refers to more than one column and
          // therefore is ambiguous.
          ambiguousAliasList.add(aliasRef);
        }
        aliasSMap.lhs.add(aliasRef);
        aliasSMap.rhs.add(item.getExpr().clone(null));
        colLabels.add(item.toColumnLabel());
      }
    }

    // analyze selectListExprs
    Expr.analyze(resultExprs, analyzer);

    if (whereClause != null) {
      whereClause.analyze(analyzer);
      if (whereClause.contains(AggregateExpr.class)) {
        throw new AnalysisException(
            "aggregation function not allowed in WHERE clause");
      }
      whereClause.checkReturnsBool("WHERE clause", false);
      analyzer.registerConjuncts(whereClause, null, true);
    }

    createSortInfo(analyzer);
    analyzeAggregation(analyzer);

    // Substitute expressions to the underlying inline view expressions
    substituteInlineViewExprs(analyzer);

    if (aggInfo != null) {
      LOG.debug("post-analysis " + aggInfo.debugString());
    }
  }

  /**
   * This select block might contain inline views.
   * Substitute all exprs (result of the analysis) of this select block referencing any
   * of our inlined views, including everything registered with the analyzer.
   * Expressions created during parsing (such as whereClause) are not touched.
   */
  protected void substituteInlineViewExprs(Analyzer analyzer) {
    // Gather the inline view substitution maps from the enclosed inline views
    Expr.SubstitutionMap sMap = new Expr.SubstitutionMap();
    for (TableRef tblRef: tableRefs) {
      if (tblRef instanceof InlineViewRef) {
        InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
        sMap = Expr.SubstitutionMap.combine(sMap, inlineViewRef.getExprSMap());
      }
    }

    // we might not have anything to substitute
    if (sMap.lhs.size() == 0) {
      return;
    }

    // Substitute select list, join clause, where clause, aggregate, order by
    // and this select block's analyzer expressions

    // select
    Expr.substituteList(resultExprs, sMap);

    // aggregation (group by and aggregation expr)
    if (aggInfo != null) {
      aggInfo.substitute(sMap);
    }

    // having
    if (havingPred != null) {
      havingPred.substitute(sMap);
    }

    // ordering
    if (sortInfo != null) {
      sortInfo.substitute(sMap);
    }

    // expressions registered inside the analyzer
    analyzer.substitute(sMap);
  }

  /**
   * Expand "*" select list item.
   */
  private void expandStar(Analyzer analyzer) throws AnalysisException {
    if (tableRefs.isEmpty()) {
      throw new AnalysisException("'*' expression in select list requires FROM clause.");
    }
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
  private void expandStar(Analyzer analyzer, String alias, TupleDescriptor desc)
      throws AnalysisException {
    for (Column col: desc.getTable().getColumnsInHiveOrder()) {
      resultExprs.add(new SlotRef(new TableName(null, alias), col.getName()));
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
      throws AnalysisException {
    if (groupingExprs == null && !selectList.isDistinct()
        && !Expr.contains(resultExprs, AggregateExpr.class)) {
      // we're not computing aggregates
      return;
    }

    // If we're computing an aggregate, we must have a FROM clause.
    if (tableRefs.size() == 0) {
      throw new AnalysisException(
          "aggregation without a FROM clause is not allowed");
    }

    if ((groupingExprs != null || Expr.contains(resultExprs, AggregateExpr.class))
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
      Expr.substituteList(groupingExprsCopy, aliasSMap);
      for (int i = 0; i < groupingExprsCopy.size(); ++i) {
        groupingExprsCopy.get(i).analyze(analyzer);
        if (groupingExprsCopy.get(i).contains(AggregateExpr.class)) {
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
      havingPred = havingClause.clone(aliasSMap);
      havingPred.analyze(analyzer);
      havingPred.checkReturnsBool("HAVING clause", true);
      analyzer.registerConjuncts(havingPred, null, false);
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
        Expr.SubstitutionMap.combine(avgSMap, finalAggInfo.getSMap());
    LOG.debug("combined smap: " + combinedSMap.debugString());

    // change select list, having and ordering exprs to point to agg output
    Expr.substituteList(resultExprs, combinedSMap);
    LOG.debug("post-agg selectListExprs: " + Expr.debugString(resultExprs));
    if (havingPred != null) {
      havingPred = havingPred.substitute(combinedSMap);
      LOG.debug("post-agg havingPred: " + havingPred.debugString());
    }
    Expr.substituteList(orderingExprs, combinedSMap);
    LOG.debug("post-agg orderingExprs: " + Expr.debugString(orderingExprs));

    // check that all post-agg exprs point to agg output
    for (int i = 0; i < selectList.getItems().size(); ++i) {
      if (!resultExprs.get(i).isBound(finalAggInfo.getAggTupleId())) {
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
    Expr.collectList(resultExprs, AggregateExpr.class, result);
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
    LOG.debug("avg smap: " + result.debugString());
    return result;
  }

  /**
   * Create aggInfo for the given grouping and agg exprs.
   */
  private void createAggInfo(ArrayList<Expr> groupingExprs,
      ArrayList<AggregateExpr> aggExprs, Analyzer analyzer)
      throws AnalysisException, InternalException {
    if (selectList.isDistinct()) {
       // Create aggInfo for SELECT DISTINCT ... stmt:
       // - all select list items turn into grouping exprs
       // - there are no aggregate exprs
      Preconditions.checkState(groupingExprs.isEmpty());
      Preconditions.checkState(aggExprs.isEmpty());
      aggInfo =
          AggregateInfo.create(Expr.cloneList(resultExprs, null), null, null, analyzer);
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
    // Limit clause.
    if (hasLimitClause()) {
      strBuilder.append(" LIMIT ");
      strBuilder.append(limit);
    }
    return strBuilder.toString();
  }

  @Override
  public void getMaterializedTupleIds(ArrayList<TupleId> tupleIdList) {
    // If select statement has an aggregate, then the aggregate tuple id is materialized.
    // Otherwise, all referenced tables are materialized.
    if (aggInfo != null) {
      tupleIdList.add(aggInfo.getAggTupleId());
    } else {
      for (TableRef tblRef: tableRefs) {
        tupleIdList.addAll(tblRef.getMaterializedTupleIds());
      }
    }
  }
}
