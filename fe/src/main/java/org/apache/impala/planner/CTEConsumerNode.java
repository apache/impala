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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.RuntimeFilterGenerator.RuntimeFilter;
import org.apache.impala.thrift.TCTEConsumer;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TRuntimeFilterType;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Scan of a Common Table Expression produced by CTEProducerNode.
 */
public class CTEConsumerNode extends PlanNode {
  private final String cteName_;
  private final PlanNode ctePlan_;
  private final List<Expr> cteExprs_;

  public CTEConsumerNode(PlanNodeId id, TupleDescriptor desc, String cteName,
      PlanNode ctePlan, List<Expr> cteExprs) {
    // Descriptor for the target view, including id
    super(id, desc.getId().asList(), "CTE CONSUMER");
    conjuncts_ = Lists.newArrayList();
    cteName_ = cteName;
    ctePlan_ = ctePlan;
    cteExprs_ = cteExprs;
    Preconditions.checkArgument(cteExprs_.size() == desc.getSlots().size());
    Preconditions.checkArgument(
        desc.getSlots().stream().allMatch(SlotDescriptor::isMaterialized));
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    super.init(analyzer);
    computeMemLayout(analyzer);
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    avgRowSize_ = ctePlan_.avgRowSize_;
    rowPadSize_ = ctePlan_.rowPadSize_;
    getFixedLenRowSize_ = ctePlan_.getFixedLenRowSize_;
    cardinality_ = capCardinalityAtLimit(ctePlan_.cardinality_);
    numNodes_ = ctePlan_.numNodes_;
    numInstances_ = ctePlan_.numInstances_;
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    processingCost_ = computeDefaultProcessingCost();
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    // This node fetches rows from a BufferedTupleStream. Allocate sufficient capacity
    // to fetch a row and materialize it into the destination.
    long bufferSize = computeMaxSpillableBufferSize(
        queryOptions.getDefault_spillable_buffer_size(), queryOptions.getMax_row_size());
    nodeResourceProfile_ = new ResourceProfileBuilder()
        .setMemEstimateBytes(2 * bufferSize)
        .setMinMemReservationBytes(2 * bufferSize)
        .setMaxMemReservationBytes(2 * bufferSize)
        .setSpillableBufferBytes(bufferSize).setMaxRowBufferBytes(bufferSize).build();
  }

  @Override
  public ExecPhaseResourceProfiles computeTreeResourceProfiles(
      TQueryOptions queryOptions) {
    // Don't include resources of child in different plan fragment.
    return new ExecPhaseResourceProfiles(nodeResourceProfile_, nodeResourceProfile_);
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.CTE_CONSUMER_NODE;
    List<Integer> tupleIds = new ArrayList<>();
    List<Boolean> nullableTuples = new ArrayList<>();
    Set<TupleId> nullableTupleIds = ctePlan_.getNullableTupleIds();
    for (TupleId tupleId : ctePlan_.getTupleIds()) {
      tupleIds.add(tupleId.asInt());
      nullableTuples.add(nullableTupleIds.contains(tupleId));
    }
    msg.cte_consumer = new TCTEConsumer(
        cteName_, tupleIds, nullableTuples, Expr.treesToThrift(cteExprs_));
  }

  @Override
  protected String debugString() {
    Preconditions.checkState(tupleIds_.size() == 1);
    return MoreObjects.toStringHelper(this)
        .add("tid", tupleIds_.get(0).asInt())
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected String getDisplayLabelDetail() {
    return cteName_;
  }

  /**
   * Sort filters in runtimeFilters_: min/max first followed by bloom.
   */
  public void arrangeRuntimeFilters() {
    Collections.sort(runtimeFilters_, new Comparator<RuntimeFilter>() {
      @Override
      public int compare(RuntimeFilter a, RuntimeFilter b) {
        if (a.getType() == b.getType()) return 0;
        if (a.getType() == TRuntimeFilterType.MIN_MAX) return -1;
        if (b.getType() == TRuntimeFilterType.MIN_MAX) return 1;
        return 0;
      }
    });
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s [%s]\n", prefix, getDisplayLabel(), cteName_));

    if (!conjuncts_.isEmpty()) {
      output.append(detailPrefix + "predicates: " +
          Expr.getExplainString(conjuncts_, detailLevel) + "\n");
    }
    if (!runtimeFilters_.isEmpty()) {
      output.append(detailPrefix + "runtime filters: ");
      output.append(getRuntimeFilterExplainString(false, detailLevel));
    }
    return output.toString();
  }
}
