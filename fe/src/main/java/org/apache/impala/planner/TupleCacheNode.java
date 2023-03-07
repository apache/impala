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

  protected String subtreeHash_;
  protected String hashTrace_;

  public TupleCacheNode(PlanNodeId id, PlanNode child) {
    super(id, "TUPLE CACHE");
    addChild(child);
    cardinality_ = child.getCardinality();
    limit_ = child.limit_;

    TupleCacheInfo childCacheInfo = child.getTupleCacheInfo();
    Preconditions.checkState(childCacheInfo.isEligible());
    subtreeHash_ = childCacheInfo.getHashString();
    hashTrace_ = childCacheInfo.getHashTrace();
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    super.init(analyzer);
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
    msg.node_type = TPlanNodeType.TUPLE_CACHE_NODE;
    // TupleCacheNode does not enforce limits itself. It returns its child's output
    // unchanged and it expects other parts of the plan to enforce limits,
    // Assert that there is no limit placed on this node.
    Preconditions.checkState(!hasLimit(),
        "TupleCacheNode does not enforce limits itself and cannot have a limit set.");
    TTupleCacheNode tupleCacheNode = new TTupleCacheNode();
    tupleCacheNode.setSubtree_hash(subtreeHash_);
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
    output.append(detailPrefix + "cache key: " + subtreeHash_ + "\n");

    // For debuggability, always print the hash trace until the cache key calculation
    // matures. Print trace in chunks to avoid excessive wrapping and padding in
    // impala-shell. There are other explain lines at VERBOSE level that are
    // over 100 chars long so we limit the key chunk length similarly here.
    final int keyFormatWidth = 100;
    for(int idx = 0; idx < hashTrace_.length(); idx += keyFormatWidth) {
      int stop_idx = Math.min(hashTrace_.length(), idx + keyFormatWidth);
      output.append(detailPrefix + "[" + hashTrace_.substring(idx, stop_idx) + "]\n");
    }
    return output.toString();
  }

  public String getSubtreeHash() { return subtreeHash_; }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    processingCost_ = ProcessingCost.basicCost(getDisplayLabel(), getCardinality(), 0);
  }

}
