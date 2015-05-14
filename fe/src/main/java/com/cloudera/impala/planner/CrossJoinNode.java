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
import com.cloudera.impala.analysis.TableRef;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TQueryOptions;
import com.google.common.base.Objects;

/**
 * Cross join between left child and right child.
 * TODO: Rename to NestedLoopsJoin or similar when adding support for more join modes.
 * TODO: Adjust computeStats() and computeCosts() for outer and semi joins once
 * those join modes are supported.
 */
public class CrossJoinNode extends JoinNode {
  private final static Logger LOG = LoggerFactory.getLogger(CrossJoinNode.class);

  public CrossJoinNode(PlanNode outer, PlanNode inner, TableRef tblRef,
      List<Expr> otherJoinConjuncts) {
    super(outer, inner, tblRef, otherJoinConjuncts, "CROSS JOIN");
  }

  @Override
  public void init(Analyzer analyzer) throws InternalException {
    super.init(analyzer);
    assignedConjuncts_ = analyzer.getAssignedConjuncts();
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    if (getChild(0).cardinality_ == -1 || getChild(1).cardinality_ == -1) {
      cardinality_ = -1;
    } else {
      cardinality_ = multiplyCardinalities(getChild(0).cardinality_,
          getChild(1).cardinality_);
      if (computeSelectivity() != -1) {
        cardinality_ = Math.round(((double) cardinality_) * computeSelectivity());
      }
    }
    LOG.debug("stats CrossJoin: cardinality=" + Long.toString(cardinality_));
  }

  @Override
  public void computeCosts(TQueryOptions queryOptions) {
    if (getChild(1).getCardinality() == -1 || getChild(1).getAvgRowSize() == -1
        || numNodes_ == 0) {
      perHostMemCost_ = DEFAULT_PER_HOST_MEM;
      return;
    }
    perHostMemCost_ = (long) Math.ceil(getChild(1).cardinality_ * getChild(1).avgRowSize_);
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    String labelDetail = getDisplayLabelDetail();
    if (labelDetail == null) {
      output.append(prefix + getDisplayLabel() + "\n");
    } else {
      output.append(String.format("%s%s:%s [%s]\n", prefix, id_.toString(),
          displayName_, getDisplayLabelDetail()));
    }
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      if (!otherJoinConjuncts_.isEmpty()) {
        output.append(detailPrefix + "join predicates: ")
        .append(getExplainString(otherJoinConjuncts_) + "\n");
      }
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix + "predicates: ")
        .append(getExplainString(conjuncts_) + "\n");
      }
    }
    return output.toString();
  }

  @Override
  protected String getDisplayLabelDetail() {
    if (distrMode_ == DistributionMode.NONE) return null;
    return distrMode_.toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.CROSS_JOIN_NODE;
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .addValue(super.debugString())
        .toString();
  }
}
