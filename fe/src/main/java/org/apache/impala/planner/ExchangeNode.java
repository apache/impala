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

import java.util.Stack;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SortInfo;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExchangeNode;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TSortInfo;
import org.apache.impala.util.ExprUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Receiver side of a 1:n data stream. Logically, an ExchangeNode consumes the data
 * produced by its children. For each of the sending child nodes the actual data
 * transmission is performed by the DataStreamSink of the PlanFragment housing
 * that child node. Typically, an ExchangeNode only has a single sender child but,
 * e.g., for distributed union queries an ExchangeNode may have one sender child per
 * union operand.
 *
 * If a (optional) SortInfo field is set, the ExchangeNode will merge its
 * inputs on the parameters specified in the SortInfo object. It is assumed that the
 * inputs are also sorted individually on the same SortInfo parameter.
 */
public class ExchangeNode extends PlanNode {
  private static final Logger LOG = LoggerFactory.getLogger(ExchangeNode.class);

  // The serialization overhead per tuple in bytes when sent over an exchange.
  // Currently it accounts only for the tuple_offset entry per tuple (4B) in a
  // BE TRowBatch. If we modify the RowBatch serialization, then we need to
  // update this constant as well.
  private static final double PER_TUPLE_SERIALIZATION_OVERHEAD = 4.0;

  // The overhead in bytes per tuple for a deserialized row batch
  // TODO: The per-row sizes calculated from profile counters suggest we
  // may actually have an overhead of 9 bytes per tuple. Investigate this
  // further.
  private static final double PER_TUPLE_DESERIALIZED_OVERHEAD = 8.0;

  // Empirically derived minimum estimate (in bytes) for the exchange node.
  private static final int MIN_ESTIMATE_BYTES = 16 * 1024;

  // Coefficients for estimating exchange receiver CPU costs.  Derived from benchmarking.
  private static final double COST_COEFFICIENT_MERGING_XCHG_RCVR_ROWS = 0.2369;
  private static final double COST_COEFFICIENT_MERGING_XCHG_RCVR_BYTES = 0.0020;
  private static final double COST_COEFFICIENT_BCAST_XCHG_RCVR_ROWS = 0.1329;
  private static final double COST_COEFFICIENT_PART_XCHG_RCVR_ROWS = 0.0743;
  private static final double COST_COEFFICIENT_PART_XCHG_RCVR_BYTES = 0.0046;

  // The parameters based on which sorted input streams are merged by this
  // exchange node. Null if this exchange does not merge sorted streams
  private SortInfo mergeInfo_;

  // Offset after which the exchange begins returning rows. Currently valid
  // only if mergeInfo_ is non-null, i.e. this is a merging exchange node.
  private long offset_;

  protected boolean isMergingExchange() { return mergeInfo_ != null; }

  protected boolean isBroadcastExchange() {
    // If the output of the sink is not partitioned but the target fragment is
    // partitioned, then the data exchange is broadcast.
    Preconditions.checkState(!children_.isEmpty());
    // Has to examine isDirectedExchange() too because for a DIRECTED exchange the below
    // code would also return true.
    if (isDirectedExchange()) return false;
    DataSink sink = getChild(0).getFragment().getSink();
    if (sink == null) return false;
    Preconditions.checkState(sink instanceof DataStreamSink);
    DataStreamSink streamSink = (DataStreamSink) sink;
    return !streamSink.getOutputPartition().isPartitioned() && fragment_.isPartitioned();
  }

  protected boolean isDirectedExchange() {
    if (fragment_.getSink().getSinkType() == TDataSinkType.ICEBERG_DELETE_BUILDER) {
      // If this EXCHANGE is using a JoinBuildSink to send to an IcebergDeleteNode in a
      // separate fragment.
      return true;
    }
    // If this EXCHANGE is right below an IcebergDeleteNode in the same fragment.
    return isChildOfIcebergDeleteNode(fragment_.getPlanRoot());
  }

  protected boolean isChildOfIcebergDeleteNode(PlanNode currNode) {
    if (currNode instanceof IcebergDeleteNode) {
      Preconditions.checkState(currNode.getChildCount() == 2);
      if (currNode.getChild(1) == this) return true;
    }
    for (PlanNode child : currNode.getChildren()) {
      if (isChildOfIcebergDeleteNode(child)) return true;
    }
    return false;
  }

  public ExchangeNode(PlanNodeId id, PlanNode input) {
    super(id, "EXCHANGE");
    offset_ = 0;
    children_.add(input);
    // Only apply the limit at the receiver if there are multiple senders.
    if (input.getFragment().isPartitioned() &&
        !(input instanceof AggregationNode && !input.isBlockingNode())) {
      limit_ = input.limit_;
    }
    computeTupleIds();
  }

  @Override
  public void computeTupleIds() {
    clearTupleIds();
    tupleIds_.addAll(getChild(0).getTupleIds());
    tblRefIds_.addAll(getChild(0).getTblRefIds());
    nullableTupleIds_.addAll(getChild(0).getNullableTupleIds());
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    super.init(analyzer);
    Preconditions.checkState(conjuncts_.isEmpty());
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    Preconditions.checkState(children_.size() == 1);
    cardinality_ = capCardinalityAtLimit(children_.get(0).getCardinality());
    // Apply the offset correction if there's a valid cardinality
    if (cardinality_ > -1) cardinality_ = Math.max(0, cardinality_ - offset_);
    hasHardEstimates_ = children_.get(0).hasHardEstimates_;
  }

  /**
   * Set the parameters used to merge sorted input streams. This can be called
   * after init().
   */
  public void setMergeInfo(SortInfo info, long offset) {
    mergeInfo_ = info;
    offset_ = offset;
    displayName_ = "MERGING-EXCHANGE";
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s [%s]\n", prefix,
        getDisplayLabel(), getDisplayLabelDetail()));

    if (offset_ > 0) {
      output.append(detailPrefix + "offset: ").append(offset_).append("\n");
    }

    if (isMergingExchange() && detailLevel.ordinal() > TExplainLevel.MINIMAL.ordinal()) {
      output.append(detailPrefix + "order by: ");
      output.append(getSortingOrderExplainString(mergeInfo_.getSortExprs(),
          mergeInfo_.getIsAscOrder(), mergeInfo_.getNullsFirstParams(),
          mergeInfo_.getSortingOrder(), mergeInfo_.getNumLexicalKeysInZOrder()));
    }
    return output.toString();
  }

  /**
   * An Exchange simply moves rows over the network: its row width
   * and cardinality are identical to its input. So, for standard
   * level, there is no need to repeat these values. Retained in
   * higher levels for backward compatibility.
   */
  @Override
  protected boolean displayCardinality(TExplainLevel detailLevel) {
    return detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal();
  }

  @Override
  protected String getDisplayLabelDetail() {
    // For the non-fragmented explain levels, print the data partition
    // of the data stream sink that sends to this exchange node.
    Preconditions.checkState(!children_.isEmpty());
    DataSink sink = getChild(0).getFragment().getSink();
    if (sink == null) return "";
    if (isDirectedExchange()) {
      return "DIRECTED";
    } else if (isBroadcastExchange()) {
      return "BROADCAST";
    } else {
      Preconditions.checkState(sink instanceof DataStreamSink);
      DataStreamSink streamSink = (DataStreamSink) sink;
      return streamSink.getOutputPartition().getExplainString();
    }
  }

  /**
   * Returns the average size of rows produced by 'exchInput' when serialized for
   * being sent through an exchange. Static method because we call this in some
   * cases where we can't be 100% sure the PlanNode is actually an ExchangeNode.
   */
  public static double getAvgSerializedRowSize(PlanNode exchInput) {
    return exchInput.getAvgRowSize()
        + (exchInput.getTupleIds().size() * PER_TUPLE_SERIALIZATION_OVERHEAD);
  }

  /**
   * Returns the average size of rows produced by 'exchInput' after deserialization.
   */
  public double getAvgDeserializedRowSize() {
    return getAvgRowSize() + (getTupleIds().size() * PER_TUPLE_DESERIALIZED_OVERHEAD);
  }

  // Return the number of sending instances of this exchange.
  public int getNumSenders() {
    Preconditions.checkState(!children_.isEmpty());
    Preconditions.checkNotNull(children_.get(0).getFragment());
    return children_.get(0).getFragment().getNumInstances();
  }

  // Return the number of receiving instances of this exchange.
  public int getNumReceivers() {
    DataSink sink = fragment_.getSink();
    if (sink == null) return 1;
    return sink.getFragment().getNumInstances();
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    // The computation for the processing cost for exchange splits into two parts:
    //   1. The sending processing cost which is computed in the DataStreamSink of the
    //      bottom sending fragment;
    //   2. The receiving processing cost in the top receiving fragment which is computed
    //      here.
    long inputCardinality = Math.max(0, getChild(0).getFilteredCardinality());

    // It's not obvious whether the per-byte CPU costs are more accurately estimated
    // using the serialized or deserialized sizes, but the coefficients were determined
    // using deserialized sizes since those are more readily available in query profiles.
    // So for consistency we used deserialized size to calculate costs here.
    long inputSize = (long) (getAvgDeserializedRowSize() * inputCardinality);
    double totalCost = 0.0;
    String exchType;

    if (isMergingExchange()) {
      exchType = "MERGING";
      totalCost = (inputCardinality * COST_COEFFICIENT_MERGING_XCHG_RCVR_ROWS)
          + (inputSize * COST_COEFFICIENT_MERGING_XCHG_RCVR_BYTES);
    } else if (isBroadcastExchange()) {
      exchType = "BROADCAST";
      totalCost = inputCardinality * COST_COEFFICIENT_BCAST_XCHG_RCVR_ROWS;
    } else {
      exchType = isDirectedExchange() ? "DIRECTED" : "PARTITIONED";
      // Use the partitioned exchange costing for all other cases incuding DIRECTED.
      // TODO: Add specific costing for DIRECTED exchange based on benchmarks.
      totalCost = (inputCardinality * COST_COEFFICIENT_PART_XCHG_RCVR_ROWS)
          + (inputSize * COST_COEFFICIENT_PART_XCHG_RCVR_BYTES);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Total CPU cost estimate: " + totalCost + ", ExchangeType: " + exchType
          + ", Input Card: " + inputCardinality + ", Input Size: " + inputSize);
    }
    processingCost_ = ProcessingCost.basicCost(getDisplayLabel(), totalCost);

    if (isBroadcastExchange()) {
      processingCost_ = ProcessingCost.broadcastCost(processingCost_,
          ()
              -> fragment_.hasAdjustedInstanceCount() ?
              fragment_.getAdjustedInstanceCount() :
              getNumReceivers());
    }
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    // For non-merging exchanges, one row batch queue is maintained for row
    // batches from all sender fragment instances. For merging exchange, one
    // queue is created for the batches from each distinct sender. There is a
    // soft limit on every row batch queue of FLAGS_exchg_node_buffer_size_bytes
    // (default 10MB). There is also a deferred rpc queue which queues at max
    // one rpc payload (containing the rowbatch) per sender in-case the row
    // batch queue hits the previously mentioned soft limit. Actual memory used
    // depends on the row size (that can vary a lot due to presence of var len
    // strings) and on the rate at which rows are received and consumed from the
    // exchange node which in turn depends on the complexity of the query and
    // the system load. This makes it difficult to accurately estimate the
    // memory usage at runtime. The following estimates assume that memory usage will
    // lean towards the soft limits.
    int numSenders = getNumSenders();
    long estimatedTotalQueueByteSize = estimateTotalQueueByteSize(numSenders);
    long estimatedDeferredRPCQueueSize = estimateDeferredRPCQueueSize(queryOptions,
        numSenders);
    long estimatedMem = Math.max(
        checkedAdd(estimatedTotalQueueByteSize, estimatedDeferredRPCQueueSize),
        MIN_ESTIMATE_BYTES);
    nodeResourceProfile_ = ResourceProfile.noReservation(estimatedMem);
  }

  // Returns the estimated size of the deferred batch queue (in bytes) by
  // assuming that at least one row batch rpc payload per sender is queued.
  private long estimateDeferredRPCQueueSize(TQueryOptions queryOptions,
      int numSenders) {
    long rowBatchSize = getRowBatchSize(queryOptions);
    // Set an upper limit based on estimated cardinality.
    if (getCardinality() > 0) rowBatchSize = Math.min(rowBatchSize, getCardinality());
    long avgRowBatchByteSize =
        Math.min((long) Math.ceil(rowBatchSize * getAvgSerializedRowSize(this)),
            ROWBATCH_MAX_MEM_USAGE);
    long deferredBatchQueueSize = avgRowBatchByteSize * numSenders;
    return deferredBatchQueueSize;
  }

  // Returns the total estimated size (in bytes) of the row batch queues by
  // assuming enough batches can be queued such that it hits the row batch
  // queue's soft mem limit.
  private long estimateTotalQueueByteSize(int numSenders) {
    int numQueues = isMergingExchange() ? numSenders : 1;
    long maxQueueByteSize = BackendConfig.INSTANCE.getBackendCfg().
        exchg_node_buffer_size_bytes;
    // TODO: Should we set a better default size here? This might be alot for
    // queries without stats.
    long estimatedTotalQueueByteSize = numQueues * maxQueueByteSize;
    // Set an upper limit based on estimated cardinality.
    if (hasValidStats()) {
      long totalBytesToReceive = (long) Math.ceil(getAvgRowSize() * getCardinality());
      // Assuming no skew in distribution during data shuffling.
      long bytesToReceivePerExchNode = isBroadcastExchange() ? totalBytesToReceive
          : totalBytesToReceive / getNumNodes();
      estimatedTotalQueueByteSize = Math.min(bytesToReceivePerExchNode,
          estimatedTotalQueueByteSize);
    }
    return estimatedTotalQueueByteSize;
  }

  @Override
  public ExecPhaseResourceProfiles computeTreeResourceProfiles(
      TQueryOptions queryOptions) {
    // Don't include resources of child in different plan fragment.
    return new ExecPhaseResourceProfiles(nodeResourceProfile_, nodeResourceProfile_);
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    if (processingCost_.isValid() && processingCost_ instanceof BroadcastProcessingCost) {
      Preconditions.checkState(
          getNumReceivers() == processingCost_.getNumInstancesExpected());
    }
    msg.node_type = TPlanNodeType.EXCHANGE_NODE;
    msg.exchange_node = new TExchangeNode();
    for (TupleId tid: tupleIds_) {
      msg.exchange_node.addToInput_row_tuples(tid.asInt());
    }

    if (isMergingExchange()) {
      TSortInfo sortInfo = new TSortInfo(
          Expr.treesToThrift(mergeInfo_.getSortExprs()), mergeInfo_.getIsAscOrder(),
          mergeInfo_.getNullsFirst(), mergeInfo_.getSortingOrder());
      msg.exchange_node.setSort_info(sortInfo);
      msg.exchange_node.setOffset(offset_);
    }
  }

  @Override
  protected void reduceCardinalityByRuntimeFilter(
      Stack<PlanNode> nodeStack, double reductionScale) {
    if (!nodeStack.isEmpty()) nodeStack.add(this);
    getChild(0).reduceCardinalityByRuntimeFilter(nodeStack, reductionScale);
  }
}
