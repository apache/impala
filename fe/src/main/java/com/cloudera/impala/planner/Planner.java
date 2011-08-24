// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.AggregateExpr;
import com.cloudera.impala.analysis.AggregateInfo;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.analysis.SelectStmt;
import com.cloudera.impala.analysis.TableRef;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.NotImplementedException;
import com.google.common.base.Preconditions;
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
   * @throws NotImplementedException
   */
  private PlanNode createScanNode(Analyzer analyzer, TableRef tblRef) {
    // HBase or Test Scan Node?
    PlanNode scanNode = null;
    if (tblRef.getTable() instanceof HdfsTable) {
      // HDFS table
      HdfsTable hdfsTable = (HdfsTable)tblRef.getTable();
      List<String> filePaths = Lists.newArrayList();
      List<LiteralExpr> keyValues = Lists.newArrayList();
      for (HdfsTable.Partition p: hdfsTable.getPartitions()) {
        filePaths.addAll(p.filePaths);
        // Make sure we are adding exactly numClusteringCols LiteralExprs.
        Preconditions.checkState(p.keyValues.size() == hdfsTable.getNumClusteringCols());
        keyValues.addAll(p.keyValues);
      }
      scanNode = new HdfsScanNode(tblRef.getDesc(), filePaths, keyValues);
    } else {
      // HBase table
      scanNode = new HBaseScanNode(tblRef.getDesc());
    }
    scanNode.setConjuncts(analyzer.getConjuncts(tblRef.getId().asList()));
    return scanNode;
  }

  /**
   * Create tree of PlanNodes that implements the given selectStmt.
   * Currently only supports single-table queries plus aggregation.
   * @param selectStmt
   * @param analyzer
   * @return root node of plan tree
   * @throws NotImplementedException if selectStmt contains joins, order by or aggregates
   */
  public PlanNode createPlan(SelectStmt selectStmt, Analyzer analyzer)
      throws NotImplementedException {
    if (selectStmt.getTableRefs().size() > 1) {
      throw new NotImplementedException("FROM clause limited to a single table");
    }
    if (selectStmt.getTableRefs().isEmpty()) {
      // no from clause -> nothing to plan
      return null;
    }
    TableRef tblRef = selectStmt.getTableRefs().get(0);
    PlanNode topNode = createScanNode(analyzer, tblRef);
    // TODO:
    //ArrayList<Int> tupleIdxMap =
        //computeTblRefIdxMap(selectStmt.getTableRefs(), analyzer.getDescTbl());
    //topNode.setTupleIdxMap(tupleIdxMap);

    AggregateInfo aggInfo = selectStmt.getAggInfo();
    if (aggInfo != null) {
      // we can't aggregate strings at the moment
      // TODO: fix this
      for (AggregateExpr aggExpr: aggInfo.getAggregateExprs()) {
        if (aggExpr.hasChild(0) && aggExpr.getChild(0).getType() == PrimitiveType.STRING) {
          throw new NotImplementedException(
              aggExpr.getOp().toString() + " currently not supported for strings");
        }
      }
      topNode = new AggregationNode(topNode, aggInfo);
      if (selectStmt.getHavingPred() != null) {
        topNode.setConjuncts(selectStmt.getHavingPred().getConjuncts());
      }
    }

    List<Expr> orderingExprs = selectStmt.getOrderingExprs();
    if (orderingExprs != null) {
      throw new NotImplementedException("ORDER BY currently not supported.");
      // TODO: Uncomment once SortNode is implemented.
      // topNode =
      //    new SortNode(topNode, orderingExprs, selectStmt.getOrderingDirections());
    }

    topNode.setLimit(selectStmt.getLimit());
    return topNode;
  }

}
