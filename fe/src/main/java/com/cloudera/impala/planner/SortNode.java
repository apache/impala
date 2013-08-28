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
  private final SortInfo info;
  private final boolean useTopN;
  private final boolean isDefaultLimit;

  protected long offset; // The offset of the first row to return

  // set in init() or c'tor
  private List<Expr> baseTblOrderingExprs_;

  public SortNode(PlanNodeId id, PlanNode input, SortInfo info, boolean useTopN,
      boolean isDefaultLimit, long offset) {
    super(id, "TOP-N");
    // If this is the default limit, we shouldn't have a non-zero offset set.
    Preconditions.checkArgument(!isDefaultLimit || offset == 0);
    this.info = info;
    this.useTopN = useTopN;
    this.isDefaultLimit = isDefaultLimit;
    this.tupleIds.addAll(input.getTupleIds());
    this.nullableTupleIds.addAll(input.getNullableTupleIds());
    this.children.add(input);
    this.offset = offset;
  }

  /**
   * Clone 'inputSortNode' for distributed Top-N
   */
  public SortNode(PlanNodeId id, SortNode inputSortNode, PlanNode child) {
    super(id, inputSortNode, "TOP-N");
    this.info = inputSortNode.info;
    // set this directly (and don't reassign in init()): inputSortNode's smap
    // may not be able to remap info.orderingExprs
    baseTblOrderingExprs_ = inputSortNode.baseTblOrderingExprs_;
    this.useTopN = inputSortNode.useTopN;
    this.isDefaultLimit = inputSortNode.isDefaultLimit;
    this.children.add(child);
    this.offset = inputSortNode.offset;
  }

  public long getOffset() { return offset; }
  public void setOffset(long offset) { this.offset = offset; }

  @Override
  public void setCompactData(boolean on) { this.compactData = on; }

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
      baseTblOrderingExprs_ = Expr.cloneList(info.getOrderingExprs(), baseTblSmap_);
    }
  }

  @Override
  protected void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality = getChild(0).cardinality;
    if (hasLimit()) {
      if (cardinality == -1) {
        cardinality = limit;
      } else {
        cardinality = Math.min(cardinality, limit);
      }
    }
    LOG.debug("stats Sort: cardinality=" + Long.toString(cardinality));
  }

  @Override
  protected String debugString() {
    List<String> strings = Lists.newArrayList();
    for (Boolean isAsc : info.getIsAscOrder()) {
      strings.add(isAsc ? "a" : "d");
    }
    return Objects.toStringHelper(this)
        .add("ordering_exprs", Expr.debugString(info.getOrderingExprs()))
        .add("is_asc", "[" + Joiner.on(" ").join(strings) + "]")
        .add("nulls_first", "[" + Joiner.on(" ").join(info.getNullsFirst()) + "]")
        .add("offset", offset)
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.SORT_NODE;
    msg.sort_node = new TSortNode(
        Expr.treesToThrift(baseTblOrderingExprs_), info.getIsAscOrder(), useTopN,
        isDefaultLimit);
    msg.sort_node.setNulls_first(info.getNullsFirst());
    msg.sort_node.setOffset(offset);
  }

  @Override
  protected String getNodeExplainString(String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(detailPrefix + "order by: ");
    for (int i = 0; i < info.getOrderingExprs().size(); ++i) {
      if (i > 0) {
        output.append(", ");
      }
      output.append(info.getOrderingExprs().get(i).toSql() + " ");
      output.append(info.getIsAscOrder().get(i) ? "ASC" : "DESC");

      Boolean nullsFirstParam = info.getNullsFirstParams().get(i);
      if (nullsFirstParam != null) {
        output.append(nullsFirstParam ? " NULLS FIRST" : " NULLS LAST");
      }
    }
    output.append("\n");
    return output.toString();
  }

  @Override
  protected String getOffsetExplainString(String prefix) {
    return offset != 0 ? prefix + "offset: " + Long.toString(offset) + "\n" : "";
  }

  @Override
  public void computeCosts(TQueryOptions queryOptions) {
    Preconditions.checkState(hasValidStats());
    Preconditions.checkState(useTopN);
    perHostMemCost = (long) Math.ceil((cardinality + offset) * avgRowSize);
  }
}
