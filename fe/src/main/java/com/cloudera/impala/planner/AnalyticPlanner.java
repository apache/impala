// Copyright 2014 Cloudera Inc.
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

package com.cloudera.impala.planner;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalyticExpr;
import com.cloudera.impala.analysis.AnalyticInfo;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.ExprSubstitutionMap;
import com.cloudera.impala.analysis.OrderByElement;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.SortInfo;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.common.IdGenerator;
import com.cloudera.impala.common.ImpalaException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


/**
 * The analytic planner adds plan nodes to an existing set of PlanFragments in order to
 * implement the AnalyticInfo of a given query stmt.
 * TODO: this is only the minimum code needed to generate a plan for a single
 * analytic expr
 *
 */
public class AnalyticPlanner {
  private final static Logger LOG = LoggerFactory.getLogger(AnalyticPlanner.class);

  private final Analyzer analyzer_;
  private final IdGenerator<PlanNodeId> idGenerator_;

  public AnalyticPlanner(Analyzer analyzer, IdGenerator<PlanNodeId> idGenerator) {
    analyzer_ = analyzer;
    idGenerator_ = idGenerator;
  }

  /**
   * Augment planFragment with plan nodes that implement single-node evaluation of
   * the AnalyticExprs in analyticInfo.
   * TODO: this only works for non-nested, non-Union select stmts; add the rest
   * TODO: missing limits
   * TODO: prune non-materialized output slots
   */
  public PlanNode createSingleNodePlan(
      PlanNode root, AnalyticInfo analyticInfo) throws ImpalaException {
    Preconditions.checkState(analyticInfo.getAnalyticExprs().size() == 1);
    AnalyticExpr e = analyticInfo.getAnalyticExprs().get(0);

    // sort on partition + ordering exprs
    List<Expr> partitionExprs = e.getPartitionExprs();
    List<Expr> orderByExprs = e.getOrderByExprs();
    SortNode sortNode = null;
    if (!partitionExprs.isEmpty() || !orderByExprs.isEmpty()) {
      // first sort on partitionExprs (direction doesn't matter)
      List<Expr> sortExprs = Lists.newArrayList(partitionExprs);
      List<Boolean> isAsc =
          Lists.newArrayList(Collections.nCopies(sortExprs.size(), new Boolean(true)));
      // TODO: should nulls come first or last?
      List<Boolean> nullsFirst =
          Lists.newArrayList(Collections.nCopies(sortExprs.size(), new Boolean(true)));

      // then sort on orderByExprs
      for (OrderByElement orderByElement: e.getOrderByElements()) {
        sortExprs.add(orderByElement.getExpr());
        isAsc.add(orderByElement.getIsAsc());
        nullsFirst.add(orderByElement.getNullsFirstParam());
      }

      SortInfo sortInfo = createSortInfo(root, sortExprs, isAsc, nullsFirst);
      sortNode = new SortNode(idGenerator_.getNextId(), root, sortInfo, false, 0);
      root = sortNode;
      root.init(analyzer_);
    }

    root = new AnalyticEvalNode(idGenerator_.getNextId(), root,
        Lists.newArrayList((Expr) e.getFnCall()), partitionExprs, orderByExprs,
        e.getWindow(), analyticInfo.getTupleDesc(), analyticInfo.getSmap());
    root.init(analyzer_);
    if (sortNode != null) sortNode.setAnalyticParent((AnalyticEvalNode) root);
    return root;
  }

  /**
   * Create SortInfo, including sort tuple, to sort entire input row
   * on sortExprs.
   */
  private SortInfo createSortInfo(
      PlanNode input, List<Expr> sortExprs, List<Boolean> isAsc,
      List<Boolean> nullsFirst) {
    // create tuple desc for sort output = the entire materialized input in a single
    // tuple
    TupleDescriptor sortTupleDesc = analyzer_.getDescTbl().createTupleDescriptor();
    ExprSubstitutionMap sortSmap = new ExprSubstitutionMap();
    List<Expr> sortSlotExprs = Lists.newArrayList();
    sortTupleDesc.setIsMaterialized(true);
    for (TupleId tid: input.getTupleIds()) {
      TupleDescriptor tupleDesc = analyzer_.getTupleDesc(tid);
      for (SlotDescriptor inputSlotDesc: tupleDesc.getSlots()) {
        if (!inputSlotDesc.isMaterialized()) continue;
        SlotDescriptor sortSlotDesc = analyzer_.addSlotDescriptor(sortTupleDesc);
        if (inputSlotDesc.getColumn() != null) {
          sortSlotDesc.setColumn(inputSlotDesc.getColumn());
        } else {
          sortSlotDesc.setType(inputSlotDesc.getType());
        }
        sortSlotDesc.setLabel(inputSlotDesc.getLabel());
        // all output slots need to be materialized
        sortSlotDesc.setIsMaterialized(true);
        sortSmap.put(new SlotRef(inputSlotDesc), new SlotRef(sortSlotDesc));
        sortSlotExprs.add(new SlotRef(inputSlotDesc));
      }
    }

    SortInfo sortInfo = new SortInfo(
        Expr.substituteList(sortExprs, sortSmap, analyzer_), isAsc, nullsFirst);
    LOG.info("sortinfo exprs: " + Expr.debugString(sortInfo.getOrderingExprs()));
    sortInfo.setMaterializedTupleInfo(sortTupleDesc, sortSlotExprs);
    return sortInfo;
  }
}
