// Copyright 2014 Cloudera Inc.
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

import com.cloudera.impala.analysis.AnalyticWindow;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.ExprSubstitutionMap;
import com.cloudera.impala.analysis.OrderByElement;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.thrift.TAnalyticNode;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TQueryOptions;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Computation of analytic exprs.
 */
public class AnalyticEvalNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(AnalyticEvalNode.class);

  // List of tuple ids materialized by the originating SelectStmt (i.e., what is returned
  // by SelectStmt.getMaterializedTupleIds()).
  // Needed for getting unassigned conjuncts from the analyzer.
  private final List<TupleId> stmtTupleIds_;

  private List<Expr> analyticFnCalls_;

  // Partitioning exprs from the AnalyticInfo
  private final List<Expr> partitionExprs_;

  // TODO: Remove when the BE uses partitionByLessThan rather than the exprs
  private List<Expr> substitutedPartitionExprs_;
  private List<OrderByElement> orderByElements_;
  private final AnalyticWindow analyticWindow_;

  // Logical output tuple for the analytic exprs of the originating stmt.
  private final TupleDescriptor logicalTupleDesc_;

  // Physical tuples used/produced by this analytic node.
  private final TupleDescriptor intermediateTupleDesc_;
  private final TupleDescriptor outputTupleDesc_;

  // maps from the logical output slots in logicalTupleDesc_ to their corresponding
  // physical output slots in outputTupleDesc_
  private final ExprSubstitutionMap logicalToPhysicalSmap_;

  // predicates constructed from partitionExprs_/orderingExprs_ to
  // compare input to buffered tuples
  private final Expr partitionByEq_;
  private final Expr orderByEq_;
  private final TupleDescriptor bufferedTupleDesc_;

  public AnalyticEvalNode(
      PlanNodeId id, PlanNode input, List<TupleId> stmtTupleIds,
      List<Expr> analyticFnCalls, List<Expr> partitionExprs,
      List<OrderByElement> orderByElements, AnalyticWindow analyticWindow,
      TupleDescriptor logicalTupleDesc, TupleDescriptor intermediateTupleDesc,
      TupleDescriptor outputTupleDesc, ExprSubstitutionMap logicalToPhysicalSmap,
      Expr partitionByEq, Expr orderByEq, TupleDescriptor bufferedTupleDesc) {
    super(id, input.getTupleIds(), "ANALYTIC");
    Preconditions.checkState(!tupleIds_.contains(outputTupleDesc.getId()));
    // we're materializing the input row augmented with the analytic output tuple
    tupleIds_.add(outputTupleDesc.getId());
    stmtTupleIds_ = stmtTupleIds;
    analyticFnCalls_ = analyticFnCalls;
    partitionExprs_ = partitionExprs;
    orderByElements_ = orderByElements;
    analyticWindow_ = analyticWindow;
    logicalTupleDesc_ = logicalTupleDesc;
    intermediateTupleDesc_ = intermediateTupleDesc;
    outputTupleDesc_ = outputTupleDesc;
    logicalToPhysicalSmap_ = logicalToPhysicalSmap;
    partitionByEq_ = partitionByEq;
    orderByEq_ = orderByEq;
    bufferedTupleDesc_ = bufferedTupleDesc;
    children_.add(input);
    nullableTupleIds_.addAll(input.getNullableTupleIds());
  }

  @Override
  public boolean isBlockingNode() { return true; }
  public List<Expr> getPartitionExprs() { return partitionExprs_; }
  public List<OrderByElement> getOrderByElements() { return orderByElements_; }

  @Override
  public void init(Analyzer analyzer) {
    computeMemLayout(analyzer);
    intermediateTupleDesc_.computeMemLayout();

    // we add the analyticInfo's smap to the combined smap of our child
    outputSmap_ = logicalToPhysicalSmap_;
    createDefaultSmap(analyzer);

    // Do not assign any conjuncts here: the conjuncts out of our SelectStmt's
    // Where clause have already been assigned, and conjuncts coming out of an
    // enclosing scope need to be evaluated *after* all analytic computations.

    // do this at the end so it can take all conjuncts into account
    computeStats(analyzer);

    LOG.trace("desctbl: " + analyzer.getDescTbl().debugString());

    // point fn calls, partition and ordering exprs at our input
    ExprSubstitutionMap childSmap = getCombinedChildSmap();
    analyticFnCalls_ = Expr.substituteList(analyticFnCalls_, childSmap, analyzer, false);
    substitutedPartitionExprs_ = Expr.substituteList(partitionExprs_, childSmap,
        analyzer, false);
    orderByElements_ = OrderByElement.substitute(orderByElements_, childSmap, analyzer);
    LOG.trace("evalnode: " + debugString());
  }

  @Override
  protected void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality_ = getChild(0).cardinality_;
  }

  @Override
  protected String debugString() {
    List<String> orderByElementStrs = Lists.newArrayList();
    for (OrderByElement element: orderByElements_) {
      orderByElementStrs.add(element.toSql());
    }
    return Objects.toStringHelper(this)
        .add("analyticFnCalls", Expr.debugString(analyticFnCalls_))
        .add("partitionExprs", Expr.debugString(partitionExprs_))
        .add("subtitutedPartitionExprs", Expr.debugString(substitutedPartitionExprs_))
        .add("orderByElements", Joiner.on(", ").join(orderByElementStrs))
        .add("window", analyticWindow_)
        .add("intermediateTid", intermediateTupleDesc_.getId())
        .add("outputTid", outputTupleDesc_.getId())
        .add("partitionByEq",
            partitionByEq_ != null ? partitionByEq_.debugString() : "null")
        .add("orderByEq",
            orderByEq_ != null ? orderByEq_.debugString() : "null")
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.ANALYTIC_EVAL_NODE;
    msg.analytic_node = new TAnalyticNode();
    msg.analytic_node.setIntermediate_tuple_id(intermediateTupleDesc_.getId().asInt());
    msg.analytic_node.setOutput_tuple_id(outputTupleDesc_.getId().asInt());
    msg.analytic_node.setPartition_exprs(Expr.treesToThrift(substitutedPartitionExprs_));
    msg.analytic_node.setOrder_by_exprs(
        Expr.treesToThrift(OrderByElement.getOrderByExprs(orderByElements_)));
    msg.analytic_node.setAnalytic_functions(Expr.treesToThrift(analyticFnCalls_));
    if (analyticWindow_ == null) {
      if (!orderByElements_.isEmpty()) {
        msg.analytic_node.setWindow(AnalyticWindow.DEFAULT_WINDOW.toThrift());
      }
    } else {
      // TODO: Window boundaries should have range_offset_predicate set
      msg.analytic_node.setWindow(analyticWindow_.toThrift());
    }
    if (partitionByEq_ != null) {
      msg.analytic_node.setPartition_by_eq(partitionByEq_.treeToThrift());
    }
    if (orderByEq_ != null) {
      msg.analytic_node.setOrder_by_eq(orderByEq_.treeToThrift());
    }
    if (bufferedTupleDesc_ != null) {
      msg.analytic_node.setBuffered_tuple_id(bufferedTupleDesc_.getId().asInt());
    }
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s", prefix, getDisplayLabel()));
    output.append("\n");
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      output.append(detailPrefix + "functions: ");
      List<String> strings = Lists.newArrayList();
      for (Expr fnCall: analyticFnCalls_) {
        strings.add(fnCall.toSql());
      }
      output.append(Joiner.on(", ").join(strings));
      output.append("\n");

      if (!partitionExprs_.isEmpty()) {
        output.append(detailPrefix + "partition by: ");
        strings.clear();
        for (Expr partitionExpr: partitionExprs_) {
          strings.add(partitionExpr.toSql());
        }
        output.append(Joiner.on(", ").join(strings));
        output.append("\n");
      }

      if (!orderByElements_.isEmpty()) {
        output.append(detailPrefix + "order by: ");
        strings.clear();
        for (OrderByElement element: orderByElements_) {
          strings.add(element.toSql());
        }
        output.append(Joiner.on(", ").join(strings));
        output.append("\n");
      }

      if (analyticWindow_ != null) {
        output.append(detailPrefix + "window: ");
        output.append(analyticWindow_.toSql());
        output.append("\n");
      }

      if (!conjuncts_.isEmpty()) {
        output.append(
            detailPrefix + "predicates: " + getExplainString(conjuncts_) + "\n");
      }
    }
    return output.toString();
  }

  @Override
  public void computeCosts(TQueryOptions queryOptions) {
    Preconditions.checkNotNull(fragment_,
        "PlanNode must be placed into a fragment before calling this method.");
    // TODO: come up with estimate based on window
    perHostMemCost_ = 0;
  }
}
