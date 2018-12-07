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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;

import com.google.common.base.Preconditions;

/**
 * Node that applies conjuncts and a limit clause. Has exactly one child.
 */
public class SelectNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(SelectNode.class);

  protected SelectNode(PlanNodeId id, PlanNode child, List<Expr> conjuncts) {
    super(id, "SELECT");
    addChild(child);
    conjuncts_.addAll(conjuncts);
    computeTupleIds();
  }

  @Override
  public void computeTupleIds() {
    clearTupleIds();
    tblRefIds_.addAll(getChild(0).getTblRefIds());
    tupleIds_.addAll(getChild(0).getTupleIds());
    nullableTupleIds_.addAll(getChild(0).getNullableTupleIds());
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.SELECT_NODE;
  }

  @Override
  public void init(Analyzer analyzer) {
    analyzer.markConjunctsAssigned(conjuncts_);
    conjuncts_ = orderConjunctsByCost(conjuncts_);
    computeStats(analyzer);
    createDefaultSmap(analyzer);
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    if (getChild(0).cardinality_ == -1) {
      cardinality_ = -1;
    } else {
      cardinality_ =
          Math.round(((double) getChild(0).cardinality_) * computeSelectivity());
      Preconditions.checkState(cardinality_ >= 0);
    }
    cardinality_ = capCardinalityAtLimit(cardinality_);
    if (LOG.isTraceEnabled()) {
      LOG.trace("stats Select: cardinality=" + Long.toString(cardinality_));
    }
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    // The select node initializes a single row-batch which it recycles on every
    // GetNext() call made to its child node. The memory attached to that
    // row-batch is the only memory counted against this node. Since that
    // attached memory depends on how the nodes under it manage memory
    // ownership, it becomes increasingly difficult to accurately estimate this
    // node's peak mem usage. Considering that, we estimate zero bytes for it to
    // make sure it does not affect overall estimations in any way.
    nodeResourceProfile_ = ResourceProfile.noReservation(0);
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s:%s\n", prefix, id_.toString(), displayName_));
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix
            + "predicates: " + getExplainString(conjuncts_, detailLevel) + "\n");
      }
    }
    return output.toString();
  }
}
