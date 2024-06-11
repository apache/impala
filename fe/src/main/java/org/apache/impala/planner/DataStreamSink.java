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

import org.apache.impala.analysis.Expr;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TDataStreamSink;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Data sink that forwards data to an exchange node.
 */
public class DataStreamSink extends DataSink {
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamSink.class);

  // Coefficients for estimating exchange receiver CPU costs.  Derived from benchmarking.
  private static final double COST_COEFFICIENT_MERGING_XCHG_SNDR_ROWS = 0.1047;
  private static final double COST_COEFFICIENT_MERGING_XCHG_SNDR_BYTES = 0.0262;
  private static final double COST_COEFFICIENT_BCAST_XCHG_SNDR_BYTES = 0.0027;
  private static final double COST_COEFFICIENT_PART_XCHG_SNDR_BYTES = 0.0644;

  private final ExchangeNode exchNode_;
  private final DataPartition outputPartition_;

  public DataStreamSink(ExchangeNode exchNode, DataPartition partition) {
    Preconditions.checkNotNull(exchNode);
    Preconditions.checkNotNull(partition);
    exchNode_ = exchNode;
    outputPartition_ = partition;
  }

  @Override
  public void appendSinkExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel detailLevel, StringBuilder output) {
    output.append(
        String.format("%sDATASTREAM SINK [FRAGMENT=%s, EXCHANGE=%s, %s]",
        prefix, exchNode_.getFragment().getId().toString(),
        exchNode_.getId().toString(), exchNode_.getDisplayLabelDetail()));
    output.append("\n");
  }

  @Override
  protected String getLabel() {
    return "EXCHANGE SENDER";
  }

  /**
   * This method estimate total buffer size needed for outbound_batches_ in
   * KrpcDataStreamSender. The total buffer size follow this formula:
   *
   *   buffer_size = num_channels * 2 * (tuple_buffer_length + compressed_buffer_length)
   *
   * This method estimate that both tuple_buffer_length and compressed_buffer_length are
   * equal to avgOutboundRowBatchSize. If outputPartiton_ is partitioned, all of the
   * channel's OutboundRowBatches are used. Otherwise, only a pair of OutboundRowBatches
   * in KrpcDataStreamSender class are used.
   * TODO: this function can both over and under estimate the memory need
   *       see IMPALA-12594 + IMPALA-12433
   */
  private long estimateOutboundRowBatchBuffers(TQueryOptions queryOptions) {
    int numChannels =
        outputPartition_.isPartitioned() ? exchNode_.getFragment().getNumInstances() : 1;
    long rowBatchSize = PlanNode.getRowBatchSize(queryOptions);
    long avgOutboundRowBatchSize = Math.min(
        (long) Math.ceil(rowBatchSize * ExchangeNode.getAvgSerializedRowSize(exchNode_)),
        PlanNode.ROWBATCH_MAX_MEM_USAGE);
    // Each channel has 2 OutboundRowBatch (KrpcDataStreamSender::NUM_OUTBOUND_BATCHES).
    int outboundBatchesPerChannel = 2;
    // Each OutboundRowBatch has 2 TrackedString, tuple_data_ and compressed_scratch_.
    int bufferPerOutboundBatch = 2;
    long bufferSize = numChannels * outboundBatchesPerChannel * bufferPerOutboundBatch
        * avgOutboundRowBatchSize;
    return bufferSize;
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    // The sending part of the processing cost for the exchange node.

    long outputCardinality = Math.max(0, exchNode_.getFilteredCardinality());
    long outputSize = (long) (exchNode_.getAvgDeserializedRowSize() * outputCardinality);
    double totalCost = 0.0;
    String exchType;

    if (exchNode_.isMergingExchange()) {
      exchType = "MERGING";
      totalCost = (outputCardinality * COST_COEFFICIENT_MERGING_XCHG_SNDR_ROWS)
          + (outputSize * COST_COEFFICIENT_MERGING_XCHG_SNDR_BYTES);
    } else if (exchNode_.isBroadcastExchange()) {
      exchType = "BROADCAST";
      totalCost = outputSize * COST_COEFFICIENT_BCAST_XCHG_SNDR_BYTES;
    } else {
      exchType = exchNode_.isDirectedExchange() ? "DIRECTED" : "PARTITIONED";
      // Use the partitioned exchange costing for all other cases incuding DIRECTED.
      // TODO: Add specific costing for DIRECTED exchange based on benchmarks.
      totalCost = outputSize * COST_COEFFICIENT_PART_XCHG_SNDR_BYTES;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Total CPU cost estimate: " + totalCost + ", Exchange Type: " + exchType
          + ", Output Card: " + outputCardinality + ", Output Size: " + outputSize);
    }
    processingCost_ = ProcessingCost.basicCost(
        getLabel() + "(" + exchNode_.getDisplayLabel() + ")", totalCost);
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    long estimatedMem = estimateOutboundRowBatchBuffers(queryOptions);
    resourceProfile_ = ResourceProfile.noReservation(estimatedMem);
  }

  @Override
  protected void toThriftImpl(TDataSink tsink) {
    TDataStreamSink tStreamSink =
        new TDataStreamSink(exchNode_.getId().asInt(), outputPartition_.toThrift());
    tsink.setStream_sink(tStreamSink);
  }

  @Override
  protected TDataSinkType getSinkType() {
    return TDataSinkType.DATA_STREAM_SINK;
  }

  public DataPartition getOutputPartition() { return outputPartition_; }

  @Override
  public void collectExprs(List<Expr> exprs) {
    exprs.addAll(outputPartition_.getPartitionExprs());
  }
}
