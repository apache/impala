// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.parser.AggregateInfo;
import com.cloudera.impala.parser.Analyzer;
import com.cloudera.impala.parser.Expr;
import com.cloudera.impala.parser.SelectStmt;
import com.cloudera.impala.parser.TableRef;

/**
 * The planner is responsible for turning parse trees into plan fragments that
 * can be shipped off to backends for execution.
 *
 */
public class Planner {
  public Planner() {
  }

  public PlanNode createPlan(SelectStmt selectStmt, Analyzer analyzer)
      throws NotImplementedException {
    if (selectStmt.getTableRefs().size() > 1) {
      throw new NotImplementedException("FROM clause limited to a single table");
    }
    TableRef tblRef = selectStmt.getTableRefs().get(0);
    PlanNode scanNode = new ScanNode(tblRef.getTable());
    scanNode.setConjuncts(analyzer.getConjuncts(tblRef.getId().asList()));
    PlanNode topNode = scanNode;

    AggregateInfo aggInfo = selectStmt.getAggInfo();
    if (aggInfo != null) {
      topNode = new AggregationNode(topNode, aggInfo);
      if (selectStmt.getHavingPred() != null) {
        topNode.setConjuncts(selectStmt.getHavingPred().getConjuncts());
      }
    }

    List<Expr> orderingExprs = selectStmt.getOrderingExprs();
    if (orderingExprs != null) {
      topNode =
          new SortNode(topNode, orderingExprs, selectStmt.getOrderingDirections());
    }

    topNode.setLimit(selectStmt.getLimit());
    return topNode;
  }
}
