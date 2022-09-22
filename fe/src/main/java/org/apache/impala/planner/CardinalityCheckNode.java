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
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TCardinalityCheckNode;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;

import com.google.common.base.Preconditions;

/**
 * Node that returns an error if its child produces more than a single row.
 * If successful, this node returns a deep copy of its single input row.
 *
 * Note that this node must be a blocking node. It would be incorrect to return rows
 * before the single row constraint has been validated because downstream exec nodes
 * might produce results and incorrectly return them to the client. If the child of this
 * node produces more than one row it means the SQL query is semantically invalid, so no
 * rows must be returned to the client.
 */
public class CardinalityCheckNode extends PlanNode {
  private final String displayStatement_;

  public CardinalityCheckNode(PlanNodeId id, PlanNode child, String displayStmt) {
    super(id, "CARDINALITY CHECK");
    Preconditions.checkState(child.getLimit() <= 2);
    cardinality_ = 1;
    limit_ = 1;
    displayStatement_ = displayStmt;
    addChild(child);
    computeTupleIds();
  }

  /**
   * Same as PlanNode.init(), except we don't assign conjuncts.
   */
  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    computeStats(analyzer);
    createDefaultSmap(analyzer);
  }

  @Override
  public boolean isBlockingNode() { return true; }

  @Override
  public void computeTupleIds() {
    clearTupleIds();
    tblRefIds_.addAll(getChild(0).getTblRefIds());
    tupleIds_.addAll(getChild(0).getTupleIds());
    nullableTupleIds_.addAll(getChild(0).getNullableTupleIds());
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.CARDINALITY_CHECK_NODE;
    TCardinalityCheckNode cardinalityCheckNode = new TCardinalityCheckNode(
        displayStatement_);
    msg.setCardinality_check_node(cardinalityCheckNode);
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    processingCost_ = computeDefaultProcessingCost();
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    nodeResourceProfile_ = ResourceProfile.noReservation(0);
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    return String.format("%s%s:%s\n", prefix, id_.toString(), displayName_);
  }
}
