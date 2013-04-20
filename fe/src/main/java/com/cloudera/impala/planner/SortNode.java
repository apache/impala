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

import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SortInfo;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
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

  public SortNode(PlanNodeId id, PlanNode input, SortInfo info, boolean useTopN,
      boolean isDefaultLimit) {
    super(id, "TOP-N");
    this.info = info;
    this.useTopN = useTopN;
    this.isDefaultLimit = isDefaultLimit;
    this.tupleIds.addAll(input.getTupleIds());
    this.nullableTupleIds.addAll(input.getNullableTupleIds());
    this.children.add(input);
    Preconditions.checkArgument(
        info.getOrderingExprs().size() == info.getIsAscOrder().size());
  }

  /**
   * Clone 'inputSortNode' for distributed Top-N
   */
  public SortNode(PlanNodeId id, SortNode inputSortNode, PlanNode child) {
    super(id, inputSortNode, "TOP-N");
    this.info = inputSortNode.info;
    this.useTopN = inputSortNode.useTopN;
    this.isDefaultLimit = inputSortNode.isDefaultLimit;
    this.children.add(child);
  }

  @Override
  public void getMaterializedIds(Analyzer analyzer, List<SlotId> ids) {
    super.getMaterializedIds(analyzer, ids);
    Expr.getIds(info.getOrderingExprs(), null, ids);
  }

  @Override
  public void setCompactData(boolean on) {
    this.compactData = on;
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
    LOG.info("stats Sort: cardinality=" + Long.toString(cardinality));
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
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.SORT_NODE;
    msg.sort_node = new TSortNode(
        Expr.treesToThrift(info.getOrderingExprs()), info.getIsAscOrder(), useTopN,
        isDefaultLimit);
  }

  @Override
  protected String getNodeExplainString(String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(detailPrefix + "order by: ");
    Iterator<Expr> expr = info.getOrderingExprs().iterator();
    Iterator<Boolean> isAsc = info.getIsAscOrder().iterator();
    boolean start = true;
    while (expr.hasNext()) {
      if (start) {
        start = false;
      } else {
        output.append(", ");
      }
      output.append(expr.next().toSql() + " ");
      output.append(isAsc.next() ? "ASC" : "DESC");
    }
    output.append("\n");
    return output.toString();
  }
}
