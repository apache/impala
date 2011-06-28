// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.AggregateInfo;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SelectStmt;
import com.cloudera.impala.analysis.TableRef;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.NotImplementedException;
import com.google.common.collect.Lists;

/**
 * The planner is responsible for turning parse trees into plan fragments that
 * can be shipped off to backends for execution.
 *
 */
public class Planner {
  public Planner() {
  }

  /**
   * Create node for scanning all data files of a particular table.
   * @param analyzer
   * @param tblRef
   * @return
   */
  private PlanNode createScanNode(Analyzer analyzer, TableRef tblRef) {
    List<String> filePaths = Lists.newArrayList();
    for (Table.Partition p: tblRef.getTable().getPartitions()) {
      filePaths.addAll(p.filePaths);
    }
    PlanNode scanNode = new ScanNode(tblRef.getDesc(), filePaths);
    scanNode.setConjuncts(analyzer.getConjuncts(tblRef.getId().asList()));
    return scanNode;
  }

  /**
   * Create tree of PlanNodes that implements the given selectStmt.
   * Currently only supports single-table queries plus aggregation.
   * @param selectStmt
   * @param analyzer
   * @return root node of plan tree
   * @throws NotImplementedException if selectStmt contains joins
   */
  public PlanNode createPlan(SelectStmt selectStmt, Analyzer analyzer)
      throws NotImplementedException {
    if (selectStmt.getTableRefs().size() > 1) {
      throw new NotImplementedException("FROM clause limited to a single table");
    }
    TableRef tblRef = selectStmt.getTableRefs().get(0);
    PlanNode topNode = createScanNode(analyzer, tblRef);

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
