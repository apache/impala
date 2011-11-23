// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.ArrayList;

import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.thrift.TPlanNode;
import com.google.common.base.Objects;

/**
 * Receiver side of a 1:n data stream.
 *
 * TODO: merging of sorted inputs.
 */
public class ExchangeNode extends PlanNode {

  public ExchangeNode(ArrayList<TupleId> tupleIds) {
    super(tupleIds);
  }

  @Override
  protected void toThrift(TPlanNode msg) {
  }

  @Override
  protected String getExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "EXCHANGE ID: " + Integer.toString(id));
    output.append(super.getExplainString(prefix));
    output.append("\n");
    return output.toString();
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .addValue(super.debugString())
        .toString();
  }

}
