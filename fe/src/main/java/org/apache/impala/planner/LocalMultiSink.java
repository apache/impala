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

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.Expr;

import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TQueryOptions;

/**
 * LocalMultiSink adds results to a local exchanger that can be read by multiple
 * consumers.
 */
public class LocalMultiSink extends DataSink {
  // Coefficient for estimating exchange receiver CPU costs.
  private static final double COST_COEFFICIENT_LOCAL_XCHG_SNDR_BYTES = 0.001;

  private final CTEProducerNode buffer_;
  private final List<PlanNode> consumers_;

  public LocalMultiSink(CTEProducerNode buffer, List<PlanNode> consumers) {
    buffer_ = buffer;
    consumers_ = consumers;
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    long outputCardinality = Math.max(0, buffer_.getFilteredCardinality());
    long outputSize = (long) (buffer_.getAvgRowSize() * outputCardinality);
    double totalCost = outputSize * COST_COEFFICIENT_LOCAL_XCHG_SNDR_BYTES;
    processingCost_ = ProcessingCost.basicCost(
        getLabel() + "(" + buffer_.getDisplayLabel() + ")", totalCost);
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    long rowBatchSize = PlanNode.getRowBatchSize(queryOptions);
    resourceProfile_ = ResourceProfile.noReservation(rowBatchSize);
  }

  @Override
  public void appendSinkExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel explainLevel, StringBuilder output) {
    output.append(String.format("%sLOCAL MULTI SINK [FRAGMENT=%s, EXCHANGE=%s, %s]\n",
        prefix, buffer_.getFragment().getId().toString(),
        buffer_.getId().toString(), buffer_.getDisplayLabelDetail()));
  }

  @Override
  protected String getLabel() {
    return "LOCAL MULTI SINK";
  }

  @Override
  protected void toThriftImpl(TDataSink tdsink) {
    List<Integer> consumerNodeIds = new ArrayList<>();
    for (PlanNode consumer : consumers_) {
      consumerNodeIds.add(consumer.getId().asInt());
    }
    tdsink.setDest_node_ids(consumerNodeIds);
  }

  @Override
  protected TDataSinkType getSinkType() {
    return TDataSinkType.LOCAL_MULTI_SINK;
  }

  @Override
  public void collectExprsForLineage(List<Expr> exprs) {
  }
}
