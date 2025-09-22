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
import java.util.stream.Collectors;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TTupleCacheNode;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.HashCode;

/**
 * Node that serves as a caching intermediary with its child node. If the cache contains
 * a matching entry, this node serves up tuples from the cache and prevents its child
 * from executing. If the cache does not contain a matching entry, this executes its child
 * and mirrors the tuples into the cache.
 */
public class TupleCacheNode extends PlanNode {

  protected String compileTimeKey_;
  protected boolean displayCorrectnessCheckingInfo_;
  protected boolean skipCorrectnessVerification_;
  protected final List<Integer> inputScanNodeIds_ = new ArrayList<Integer>();
  protected final TupleCacheInfo childTupleCacheInfo_;

  public TupleCacheNode(PlanNodeId id, PlanNode child,
      boolean displayCorrectnessCheckingInfo) {
    super(id, "TUPLE CACHE");
    addChild(child);
    cardinality_ = child.getCardinality();
    if (child.getFilteredCardinality() != cardinality_) {
      setFilteredCardinality(child.getFilteredCardinality());
    }
    limit_ = child.limit_;

    childTupleCacheInfo_ = child.getTupleCacheInfo();
    Preconditions.checkState(childTupleCacheInfo_.isEligible());
    compileTimeKey_ = childTupleCacheInfo_.getHashString();
    // If there is variability due to a streaming agg, skip the correctness verification
    // for this location.
    skipCorrectnessVerification_ = childTupleCacheInfo_.getStreamingAggVariability();
    displayCorrectnessCheckingInfo_ = displayCorrectnessCheckingInfo;
    for (HdfsScanNode scanNode : childTupleCacheInfo_.getInputScanNodes()) {
      // Inputs into the tuple cache need to use deterministic scan range assignment
      scanNode.setDeterministicScanRangeAssignment(true);
      // To improve cache hits when data is appended to a table, modify scheduling
      // to schedule in order of increasing modification time. This will limit the
      // impact of a new scan range. For example, if there is only one new scan range,
      // then only one node should have a different runtime cache key.
      scanNode.setScheduleScanRangesOldestToNewest(true);
      inputScanNodeIds_.add(scanNode.getId().asInt());
    }
    computeTupleIds();
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    super.init(analyzer);
    Preconditions.checkState(conjuncts_.isEmpty());
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
    msg.node_type = TPlanNodeType.TUPLE_CACHE_NODE;
    // TupleCacheNode does not enforce limits itself. It returns its child's output
    // unchanged and it expects other parts of the plan to enforce limits,
    // Assert that there is no limit placed on this node.
    Preconditions.checkState(!hasLimit(),
        "TupleCacheNode does not enforce limits itself and cannot have a limit set.");
    TTupleCacheNode tupleCacheNode = new TTupleCacheNode();
    tupleCacheNode.setCompile_time_key(compileTimeKey_);
    tupleCacheNode.setInput_scan_node_ids(inputScanNodeIds_);
    tupleCacheNode.setSkip_correctness_verification(skipCorrectnessVerification_);
    msg.setTuple_cache_node(tupleCacheNode);
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    Preconditions.checkNotNull(
        fragment_, "PlanNode must be placed into a fragment before calling this method.");
    long perInstanceMemEstimate = 0;

    long bufferSize = computeMaxSpillableBufferSize(
        queryOptions.getDefault_spillable_buffer_size(), queryOptions.getMax_row_size());

    // Adequate space to serialize/deserialize a RowBatch.
    long perInstanceMinMemReservation = 2 * bufferSize;
    nodeResourceProfile_ = new ResourceProfileBuilder()
        .setMemEstimateBytes(perInstanceMemEstimate)
        .setMinMemReservationBytes(perInstanceMinMemReservation)
        .setSpillableBufferBytes(bufferSize).setMaxRowBufferBytes(bufferSize).build();
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s:%s\n", prefix, id_.toString(), displayName_));
    output.append(detailPrefix + "cache key: " + compileTimeKey_ + "\n");
    // Only display information about correctness verification if it is enabled
    if (displayCorrectnessCheckingInfo_) {
      output.append(detailPrefix + "skip correctness verification: " +
          skipCorrectnessVerification_ + "\n");
    }
    List<String> input_scan_node_ids_strs =
        inputScanNodeIds_.stream().map(Object::toString).collect(Collectors.toList());
    output.append(detailPrefix + "input scan node ids: " +
        String.join(",", input_scan_node_ids_strs) + "\n");
    output.append(childTupleCacheInfo_.getCostExplainString(detailPrefix));
    return output.toString();
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    processingCost_ = ProcessingCost.basicCost(getDisplayLabel(), getCardinality(), 0);
  }

}
