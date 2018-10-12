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

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.CollectionTableRef;
import org.apache.impala.analysis.Expr;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TUnnestNode;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * An UnnestNode scans over a collection materialized in memory, and returns
 * one row per item in the collection.
 * An UnnestNode can only appear in the plan tree of a SubplanNode.
 */
public class UnnestNode extends PlanNode {
  private final SubplanNode containingSubplanNode_;
  private final CollectionTableRef tblRef_;
  private final Expr collectionExpr_;

  public UnnestNode(PlanNodeId id, SubplanNode containingSubplanNode,
      CollectionTableRef tblRef) {
    super(id, tblRef.getDesc().getId().asList(), "UNNEST");
    containingSubplanNode_ = containingSubplanNode;
    tblRef_ = tblRef;
    collectionExpr_ = tblRef_.getCollectionExpr();
    // Assume the collection expr has been fully resolved in analysis.
    Preconditions.checkState(
        collectionExpr_.isBoundByTupleIds(containingSubplanNode.getChild(0).tupleIds_));
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    // Do not assign binding predicates or predicates for enforcing slot equivalences
    // because they must have been assigned in the scan node materializing the
    // collection-typed slot.
    super.init(analyzer);
    conjuncts_ = orderConjunctsByCost(conjuncts_);

    // Unnest is like a scan and must materialize the slots of its conjuncts.
    analyzer.materializeSlots(conjuncts_);
    computeMemLayout(analyzer);
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality_ = PlannerContext.AVG_COLLECTION_SIZE;
    // The containing SubplanNode has not yet been initialized, so get the number
    // of nodes from the SubplanNode's input.
    numNodes_ = containingSubplanNode_.getChild(0).getNumNodes();
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
            + "predicates: " + getExplainString(conjuncts_, detailLevel) + "\n");
      }
    }
    return output.toString();
  }

  @Override
  protected String getDisplayLabelDetail() {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append(Joiner.on(".").join(tblRef_.getPath()));
    if (tblRef_.hasExplicitAlias()) strBuilder.append(" " + tblRef_.getExplicitAlias());
    return strBuilder.toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.UNNEST_NODE;
    msg.setUnnest_node(new TUnnestNode(collectionExpr_.treeToThrift()));
  }
}
