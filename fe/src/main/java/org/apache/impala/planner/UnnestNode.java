// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.planner;

import java.util.Comparator;
import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.CollectionTableRef;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.ToSqlUtils;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TUnnestNode;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.base.Preconditions;

/**
 * An UnnestNode scans over collections materialized in memory, and returns
 * one row per item in the collection if a single collection is provided or it returns as
 * many rows as the length of the longest collection this node handles. For the shorter
 * collections the missing items are filled with nulls.
 * An UnnestNode can only appear in the plan tree of a SubplanNode.
 */
public class UnnestNode extends PlanNode {
  private final SubplanNode containingSubplanNode_;
  private final List<CollectionTableRef> tblRefs_;
  private final List<Expr> collectionExprs_;

  public UnnestNode(PlanNodeId id, SubplanNode containingSubplanNode,
      CollectionTableRef tblRef) {
    this(id, containingSubplanNode, Lists.newArrayList(tblRef));
  }

  public UnnestNode(PlanNodeId id, SubplanNode containingSubplanNode,
      List<CollectionTableRef> tblRefs) {
    super(id, "UNNEST");
    containingSubplanNode_ = containingSubplanNode;
    tblRefs_ = tblRefs;
    Preconditions.checkState(tblRefs.size() > 0);
    collectionExprs_ = Lists.newArrayList();
    for (CollectionTableRef ref : tblRefs) {
      SlotRef collectionSlotRef = (SlotRef)ref.getCollectionExpr();
      collectionExprs_.add(collectionSlotRef);
      tupleIds_.add(collectionSlotRef.getDesc().getItemTupleDesc().getId());
      tblRefIds_.add(collectionSlotRef.getDesc().getItemTupleDesc().getId());
    }
    // Assume the collection exprs have been fully resolved in analysis.
    for (Expr collectionExpr : collectionExprs_) {
      Preconditions.checkState(
          collectionExpr.isBoundByTupleIds(containingSubplanNode.getChild(0).tupleIds_));
    }
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    super.init(analyzer);
    conjuncts_ = orderConjunctsByCost(conjuncts_);

    // Unnest is like a scan and must materialize the slots of its conjuncts.
    analyzer.materializeSlots(conjuncts_);
    computeMemLayout(analyzer);
  }

  @Override
  protected boolean shouldPickUpZippingUnnestConjuncts() { return true; }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality_ = PlannerContext.AVG_COLLECTION_SIZE;
    // The containing SubplanNode has not yet been initialized, so get the number
    // of nodes from the SubplanNode's input.
    numNodes_ = containingSubplanNode_.getChild(0).getNumNodes();
    numInstances_ = containingSubplanNode_.getChild(0).getNumInstances();
    cardinality_ = capCardinalityAtLimit(cardinality_);
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    // TODO: add an estimate
    nodeResourceProfile_ = ResourceProfile.noReservation(0);
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s [%s]\n", prefix, getDisplayLabel(),
        getDisplayLabelDetail()));
    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(String.format(
          "%sparent-subplan=%s\n", detailPrefix, containingSubplanNode_.getId()));
    }
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix
            + "predicates: " + Expr.getExplainString(conjuncts_, detailLevel) + "\n");
      }
    }
    return output.toString();
  }

  @Override
  protected String getDisplayLabelDetail() {
    StringBuilder strBuilder = new StringBuilder();
    boolean first = true;
    tblRefs_.sort( (CollectionTableRef t1, CollectionTableRef t2) -> {
      String path1 = ToSqlUtils.getPathSql(t1.getPath());
      String path2 = ToSqlUtils.getPathSql(t2.getPath());
      return path1.compareTo(path2);
    });
    for (CollectionTableRef tblRef : tblRefs_) {
      if (!first) strBuilder.append(", ");
      strBuilder.append(Joiner.on(".").join(tblRef.getPath()));
      if (tblRef.hasExplicitAlias()) {
        strBuilder.append(" " + tblRef.getExplicitAlias());
      }
      first = false;
    }
    return strBuilder.toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.UNNEST_NODE;
    TUnnestNode unnestNode = new TUnnestNode();
    for (Expr expr : collectionExprs_) {
      unnestNode.addToCollection_exprs(expr.treeToThrift());
    }
    msg.setUnnest_node(unnestNode);
  }
}
