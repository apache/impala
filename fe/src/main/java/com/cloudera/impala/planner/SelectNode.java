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

package com.cloudera.impala.planner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.google.common.base.Preconditions;

/**
 * Node that applies conjuncts and a limit clause. Has exactly one child.
 */
public class SelectNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(SelectNode.class);

  protected SelectNode(PlanNodeId id, PlanNode child) {
    super(id, child.getTupleIds(), "SELECT");
    addChild(child);
    this.tblRefIds_ = child.tblRefIds_;
    this.rowTupleIds = child.rowTupleIds;
    this.nullableTupleIds = child.nullableTupleIds;
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.SELECT_NODE;
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    if (getChild(0).cardinality == -1) {
      cardinality = -1;
    } else {
      cardinality = Math.round(((double) getChild(0).cardinality) * computeSelectivity());
      Preconditions.checkState(cardinality >= 0);
    }
    LOG.debug("stats Select: cardinality=" + Long.toString(cardinality));
  }

  @Override
  protected String getNodeExplainString(String prefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    if (!conjuncts.isEmpty()) {
      output.append(prefix + "predicates: " + getExplainString(conjuncts) + "\n");
    }
    return output.toString();
  }
}
