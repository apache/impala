// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

/**
 * Receiver side of a 1:n data stream.
 *
 * TODO: merging of sorted inputs.
 */
public class ExchangeNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(ExchangeNode.class);

  // TODO: remove after transitioning to new planner
  private int numSenders;

  public void setNumSenders(int numSenders) {
    this.numSenders = numSenders;
  }

  /**
   * Create ExchangeNode that consumes output of inputNode.
   */
  public ExchangeNode(PlanNodeId id, PlanNode inputNode, boolean copyConjuncts) {
    super(id, inputNode);
    if (!copyConjuncts) {
      this.conjuncts = Lists.newArrayList();
    }
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.EXCHANGE_NODE;
  }

  @Override
  protected String getExplainString(String prefix, TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "EXCHANGE (" + id.toString() + ")");
    output.append("\n");
    output.append(super.getExplainString(prefix + "  ", detailLevel));
    return output.toString();
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("numSenders", numSenders)
        .addValue(super.debugString())
        .toString();
  }

}
