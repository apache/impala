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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.SortInfo;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TSortInfo;
import org.apache.impala.thrift.TSortNode;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Node that implements a sort with or without a limit. useTopN_ is true for sorts
 * with limits that are implemented by a TopNNode in the backend. SortNode is used
 * otherwise.
 * Will always materialize the new tuple info_.sortTupleDesc_.
 */
public class SortNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(SortNode.class);

  private final SortInfo info_;

  // if set, this SortNode requires its input to have this data partition
  private DataPartition inputPartition_;

  // if true, the output of this node feeds an AnalyticNode
  private boolean isAnalyticSort_;

  // info_.sortTupleSlotExprs_ substituted with the outputSmap_ for materialized slots
  // in init().
  private List<Expr> resolvedTupleExprs_;
  private final boolean useTopN_;
  // The offset of the first row to return.
  protected long offset_;

  public SortNode(PlanNodeId id, PlanNode input, SortInfo info, boolean useTopN,
      long offset) {
    super(id, info.getSortTupleDescriptor().getId().asList(),
        getDisplayName(useTopN, false));
    info_ = info;
    useTopN_ = useTopN;
    children_.add(input);
    offset_ = offset;
  }

  public long getOffset() { return offset_; }
  public void setOffset(long offset) { offset_ = offset; }
  public boolean hasOffset() { return offset_ > 0; }
  public boolean useTopN() { return useTopN_; }
  public SortInfo getSortInfo() { return info_; }
  public void setInputPartition(DataPartition inputPartition) {
    inputPartition_ = inputPartition;
  }
  public DataPartition getInputPartition() { return inputPartition_; }
  public boolean isAnalyticSort() { return isAnalyticSort_; }
  public void setIsAnalyticSort(boolean v) { isAnalyticSort_ = v; }

  @Override
  public boolean isBlockingNode() { return true; }

  @Override
  public void init(Analyzer analyzer) throws InternalException {
    // Do not assignConjuncts() here, so that conjuncts bound by this SortNode's tuple id
    // can be placed in a downstream SelectNode. A SortNode cannot evaluate conjuncts.
    Preconditions.checkState(conjuncts_.isEmpty());
    // Compute the memory layout for the generated tuple.
    computeMemLayout(analyzer);
    computeStats(analyzer);

    // populate resolvedTupleExprs_ and outputSmap_
    List<SlotDescriptor> sortTupleSlots = info_.getSortTupleDescriptor().getSlots();
    List<Expr> slotExprs = info_.getSortTupleSlotExprs();
    Preconditions.checkState(sortTupleSlots.size() == slotExprs.size());
    resolvedTupleExprs_ = Lists.newArrayList();
    outputSmap_ = new ExprSubstitutionMap();
    for (int i = 0; i < slotExprs.size(); ++i) {
      if (!sortTupleSlots.get(i).isMaterialized()) continue;
      resolvedTupleExprs_.add(slotExprs.get(i));
      outputSmap_.put(slotExprs.get(i), new SlotRef(sortTupleSlots.get(i)));
    }
    ExprSubstitutionMap childSmap = getCombinedChildSmap();
    resolvedTupleExprs_ =
        Expr.substituteList(resolvedTupleExprs_, childSmap, analyzer, false);

    // Remap the ordering exprs to the tuple materialized by this sort node. The mapping
    // is a composition of the childSmap and the outputSmap_ because the child node may
    // have also remapped its input (e.g., as in a a series of (sort->analytic)* nodes).
    // Parent nodes have have to do the same so set the composition as the outputSmap_.
    outputSmap_ = ExprSubstitutionMap.compose(childSmap, outputSmap_, analyzer);

    info_.substituteOrderingExprs(outputSmap_, analyzer);
    info_.checkConsistency();

    if (LOG.isTraceEnabled()) {
      LOG.trace("sort id " + tupleIds_.get(0).toString() + " smap: "
          + outputSmap_.debugString());
      LOG.trace("sort input exprs: " + Expr.debugString(resolvedTupleExprs_));
    }
  }

  @Override
  protected void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality_ = capAtLimit(getChild(0).cardinality_);
    if (LOG.isTraceEnabled()) {
      LOG.trace("stats Sort: cardinality=" + Long.toString(cardinality_));
    }
  }

  @Override
  protected String debugString() {
    List<String> strings = Lists.newArrayList();
    for (Boolean isAsc : info_.getIsAscOrder()) {
      strings.add(isAsc ? "a" : "d");
    }
    return Objects.toStringHelper(this)
        .add("ordering_exprs", Expr.debugString(info_.getOrderingExprs()))
        .add("is_asc", "[" + Joiner.on(" ").join(strings) + "]")
        .add("nulls_first", "[" + Joiner.on(" ").join(info_.getNullsFirst()) + "]")
        .add("offset_", offset_)
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.SORT_NODE;
    TSortInfo sort_info = new TSortInfo(Expr.treesToThrift(info_.getOrderingExprs()),
        info_.getIsAscOrder(), info_.getNullsFirst());
    Preconditions.checkState(tupleIds_.size() == 1,
        "Incorrect size for tupleIds_ in SortNode");
    sort_info.sort_tuple_slot_exprs = Expr.treesToThrift(resolvedTupleExprs_);
    TSortNode sort_node = new TSortNode(sort_info, useTopN_);
    sort_node.setOffset(offset_);
    msg.sort_node = sort_node;
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s:%s%s\n", prefix, id_.toString(),
        displayName_, getNodeExplainDetail(detailLevel)));
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      output.append(detailPrefix + "order by: ");
      for (int i = 0; i < info_.getOrderingExprs().size(); ++i) {
        if (i > 0) output.append(", ");
        output.append(info_.getOrderingExprs().get(i).toSql() + " ");
        output.append(info_.getIsAscOrder().get(i) ? "ASC" : "DESC");

        Boolean nullsFirstParam = info_.getNullsFirstParams().get(i);
        if (nullsFirstParam != null) {
          output.append(nullsFirstParam ? " NULLS FIRST" : " NULLS LAST");
        }
      }
      output.append("\n");
    }
    return output.toString();
  }

  private String getNodeExplainDetail(TExplainLevel detailLevel) {
    if (!hasLimit()) return "";
    if (hasOffset()) {
      return String.format(" [LIMIT=%s OFFSET=%s]", limit_, offset_);
    } else {
      return String.format(" [LIMIT=%s]", limit_);
    }
  }

  @Override
  protected String getOffsetExplainString(String prefix) {
    return offset_ != 0 ? prefix + "offset: " + Long.toString(offset_) + "\n" : "";
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    Preconditions.checkState(hasValidStats());
    if (useTopN_) {
      long perInstanceMemEstimate =
              (long) Math.ceil((cardinality_ + offset_) * avgRowSize_);
      resourceProfile_ = new ResourceProfile(perInstanceMemEstimate, 0);
      return;
    }

    // For an external sort, set the memory cost to be what is required for a 2-phase
    // sort. If the input to be sorted would take up N blocks in memory, then the
    // memory required for a 2-phase sort is sqrt(N) blocks. A single run would be of
    // size sqrt(N) blocks, and we could merge sqrt(N) such runs with sqrt(N) blocks
    // of memory.
    double fullInputSize = getChild(0).cardinality_ * avgRowSize_;
    boolean hasVarLenSlots = false;
    for (SlotDescriptor slotDesc: info_.getSortTupleDescriptor().getSlots()) {
      if (slotDesc.isMaterialized() && !slotDesc.getType().isFixedLengthType()) {
        hasVarLenSlots = true;
        break;
      }
    }

    // The block size used by the sorter is the same as the configured I/O read size.
    long blockSize = BackendConfig.INSTANCE.getReadSize();
    // The external sorter writes fixed-len and var-len data in separate sequences of
    // blocks on disk and reads from both sequences when merging. This effectively
    // doubles the block size when there are var-len columns present.
    if (hasVarLenSlots) blockSize *= 2;
    double numInputBlocks = Math.ceil(fullInputSize / blockSize);
    long perInstanceMemEstimate = blockSize * (long) Math.ceil(Math.sqrt(numInputBlocks));

    // Must be kept in sync with min_buffers_required in Sorter in be.
    long perInstanceMinReservation = 3 * SPILLABLE_BUFFER_BYTES;
    if (info_.getSortTupleDescriptor().hasVarLenSlots()) {
      perInstanceMinReservation *= 2;
    }
    resourceProfile_ =
        new ResourceProfile(perInstanceMemEstimate, perInstanceMinReservation);
  }

  private static String getDisplayName(boolean isTopN, boolean isMergeOnly) {
    if (isTopN) {
      return "TOP-N";
    } else {
      return "SORT";
    }
  }
}
