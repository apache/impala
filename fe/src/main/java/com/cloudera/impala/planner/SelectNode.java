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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.google.common.base.Preconditions;

/**
 * Node that applies conjuncts and a limit clause. Has exactly one child.
 */
public class SelectNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(SelectNode.class);

  protected SelectNode(PlanNodeId id, PlanNode child, List<Expr> conjuncts) {
    super(id, child.getTupleIds(), "SELECT");
    addChild(child);
    this.tblRefIds_ = child.tblRefIds_;
    this.nullableTupleIds_ = child.nullableTupleIds_;
    conjuncts_.addAll(conjuncts);
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.SELECT_NODE;
  }

  @Override
  public void init(Analyzer analyzer) {
    analyzer.markConjunctsAssigned(conjuncts_);
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
    LOG.debug("stats Select: cardinality=" + Long.toString(cardinality_));
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s:%s\n", prefix, id_.toString(), displayName_));
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix + "predicates: " +
            getExplainString(conjuncts_) + "\n");
      }
    }
    return output.toString();
  }
}
