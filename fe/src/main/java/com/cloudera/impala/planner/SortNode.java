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
import com.cloudera.impala.analysis.SortInfo;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TQueryOptions;
import com.cloudera.impala.thrift.TSortNode;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Sorting.
 *
 */
public class SortNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(SortNode.class);
  private final SortInfo info_;
  private final boolean useTopN_;
  private final boolean isDefaultLimit_;

  protected long offset_; // The offset of the first row to return

  // set in init() or c'tor
  private List<Expr> baseTblOrderingExprs_;

  public SortNode(PlanNodeId id, PlanNode input, SortInfo info, boolean useTopN,
      boolean isDefaultLimit, long offset) {
    super(id, "TOP-N");
    // If this is the default limit_, we shouldn't have a non-zero offset set.
    Preconditions.checkArgument(!isDefaultLimit || offset == 0);
    info_ = info;
    useTopN_ = useTopN;
    isDefaultLimit_ = isDefaultLimit;
    tupleIds_.addAll(input.getTupleIds());
    nullableTupleIds_.addAll(input.getNullableTupleIds());
    children_.add(input);
    offset_ = offset;
  }

  /**
   * Clone 'inputSortNode' for distributed Top-N
   */
  public SortNode(PlanNodeId id, SortNode inputSortNode, PlanNode child) {
    super(id, inputSortNode, "TOP-N");
    info_ = inputSortNode.info_;
    // set this directly (and don't reassign in init()): inputSortNode's smap
    // may not be able to remap info_.orderingExprs
    baseTblOrderingExprs_ = inputSortNode.baseTblOrderingExprs_;
    useTopN_ = inputSortNode.useTopN_;
    isDefaultLimit_ = inputSortNode.isDefaultLimit_;
    children_.add(child);
    offset_ = inputSortNode.offset_;
  }

  public long getOffset() { return offset_; }
  public void setOffset(long offset) { offset_ = offset; }

  @Override
  public void setCompactData(boolean on) { compactData_ = on; }

  @Override
  public boolean isBlockingNode() { return true; }

  @Override
  public void init(Analyzer analyzer) throws InternalException {
    assignConjuncts(analyzer);
    computeStats(analyzer);
    baseTblSmap_ = getChild(0).getBaseTblSmap();
    // don't set the ordering exprs if they're already set (they were assigned in the
    // clone c'tor)
    if (baseTblOrderingExprs_ == null) {
      baseTblOrderingExprs_ = Expr.cloneList(info_.getOrderingExprs(), baseTblSmap_);
    }
  }

  @Override
  protected void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality_ = capAtLimit(getChild(0).cardinality_);
    LOG.debug("stats Sort: cardinality=" + Long.toString(cardinality_));
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
    msg.sort_node = new TSortNode(
        Expr.treesToThrift(baseTblOrderingExprs_), info_.getIsAscOrder(), useTopN_,
        isDefaultLimit_);
    msg.sort_node.setNulls_first(info_.getNullsFirst());
    msg.sort_node.setOffset(offset_);
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s:%s [LIMIT=%s]\n", prefix, id_.toString(),
        displayName_, limit_));
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

  @Override
  protected String getOffsetExplainString(String prefix) {
    return offset_ != 0 ? prefix + "offset: " + Long.toString(offset_) + "\n" : "";
  }

  @Override
  public void computeCosts(TQueryOptions queryOptions) {
    Preconditions.checkState(hasValidStats());
    Preconditions.checkState(useTopN_);
    perHostMemCost_ = (long) Math.ceil((cardinality_ + offset_) * avgRowSize_);
  }
}
