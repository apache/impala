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
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.ExprSubstitutionMap;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
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
 * TODO: expand to handle multiple exprs in a single group
 */
public class AnalyticEvalNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(AnalyticEvalNode.class);

  private List<Expr> analyticFnCalls_;
  // Partitioning exprs from the AnalyticInfo
  private final List<Expr> partitionExprs_;
  // TODO: Remove when the BE uses partitionByLessThan rather than the exprs
  private List<Expr> substitutedPartitionExprs_;
  private List<Expr> orderingExprs_;
  private final AnalyticWindow analyticWindow_;
  private final TupleDescriptor intermediateTupleDesc_;
  private final TupleDescriptor outputTupleDesc_;
  private final ExprSubstitutionMap analyticSmap_;

  // id of buffered copy of input tuple; null if no partition and ordering exprs
  private TupleDescriptor bufferedTupleDesc_;
  // map from input to buffered tuple; empty if no bufferedTupleDesc_
  private final ExprSubstitutionMap bufferedTupleSmap_ = new ExprSubstitutionMap();

  // predicates constructed from partitionExprs_/orderingExprs_ to
  // compare input to buffered tuples; created in init()
  private Expr partitionByLessThan_;
  private Expr orderByLessThan_;

  public AnalyticEvalNode(
      PlanNodeId id, PlanNode input, List<Expr> analyticFnCalls,
      List<Expr> partitionExprs, List<Expr> orderingExprs,
      AnalyticWindow analyticWindow, TupleDescriptor intermediateTupleDesc,
      TupleDescriptor outputTupleDesc,
      ExprSubstitutionMap analyticSmap) {
    super(id, input.getTupleIds(), "ANALYTIC");
    // we assume to get our input from a SortNode if there are partition
    // or ordering exprs
    Preconditions.checkState(
        (partitionExprs.isEmpty() && orderingExprs.isEmpty())
          || input.getTupleIds().size() == 1);
    // we're materializing the input row augmented with the analytic tuple
    tupleIds_.add(outputTupleDesc.getId());
    analyticFnCalls_ = analyticFnCalls;
    partitionExprs_ = partitionExprs;
    orderingExprs_ = orderingExprs;
    analyticWindow_ = analyticWindow;
    intermediateTupleDesc_ = intermediateTupleDesc;
    outputTupleDesc_ = outputTupleDesc;
    analyticSmap_ = analyticSmap;
    children_.add(input);
    nullableTupleIds_.addAll(input.getNullableTupleIds());
  }

  @Override
  public boolean isBlockingNode() { return true; }
  public List<Expr> getPartitionExprs() { return partitionExprs_; }
  public List<Expr> getOrderingExprs() { return orderingExprs_; }

  @Override
  public void init(Analyzer analyzer) throws InternalException {
    // TODO: deal with conjuncts
    computeMemLayout(analyzer);

    if (!partitionExprs_.isEmpty() || !orderingExprs_.isEmpty()) {
      // register copy of input now, before serializing descriptor table
      bufferedTupleDesc_ = analyzer.getDescTbl().copyTupleDescriptor(tupleIds_.get(0));
      Preconditions.checkState(
          analyzer.getDescTbl().getTupleDesc(tupleIds_.get(0)).getByteSize() ==
            bufferedTupleDesc_.getByteSize());
    }
    // do this at the end so it can take all conjuncts into account
    computeStats(analyzer);

    if (bufferedTupleDesc_ != null) {
      // populate bufferedTupleMap
      List<SlotDescriptor> inputSlots =
          analyzer.getDescTbl().getTupleDesc(tupleIds_.get(0)).getSlots();
      List<SlotDescriptor> bufferedSlots = bufferedTupleDesc_.getSlots();
      Preconditions.checkState(inputSlots.size() == bufferedSlots.size());
      for (int i = 0; i < inputSlots.size(); ++i) {
        bufferedTupleSmap_.put(
            new SlotRef(inputSlots.get(i)), new SlotRef(bufferedSlots.get(i)));
      }
    }

    // we add the analyticInfo's smap to the combined smap of our child
    baseTblSmap_ = analyticSmap_;
    createDefaultSmap(analyzer);

    // point fn calls, partition and ordering exprs at our input
    ExprSubstitutionMap childSmap = getCombinedChildSmap();
    analyticFnCalls_ = Expr.substituteList(analyticFnCalls_, childSmap, analyzer);
    substitutedPartitionExprs_ = Expr.substituteList(partitionExprs_, childSmap,
        analyzer);
    orderingExprs_ = Expr.substituteList(orderingExprs_, childSmap, analyzer);

    // create partition-by/order-by predicates post-substitution
    if (!substitutedPartitionExprs_.isEmpty()) {
      partitionByLessThan_ = createLessThan(analyzer, substitutedPartitionExprs_);
    }
    if (!orderingExprs_.isEmpty()) {
      orderByLessThan_ = createLessThan(analyzer, orderingExprs_);
    }

    // TODO: remove (eventually)
    if (partitionByLessThan_ != null) {
      LOG.info("partitionByLt: " + partitionByLessThan_.debugString());
    }
    if (orderByLessThan_ != null) {
      LOG.info("orderByLt: " + orderByLessThan_.debugString());
    }
    LOG.info("evalnode: " + debugString());
  }

  @Override
  protected void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality_ = getChild(0).cardinality_;
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("analyticFnCalls", Expr.debugString(analyticFnCalls_))
        .add("partitionExprs", Expr.debugString(partitionExprs_))
        .add("subtitutedPartitionExprs", Expr.debugString(substitutedPartitionExprs_))
        .add("orderingExprs", Expr.debugString(orderingExprs_))
        .add("window", analyticWindow_)
        .add("intermediateTid", intermediateTupleDesc_.getId())
        .add("outputTid", outputTupleDesc_.getId())
        .add("bufferedTid",
          bufferedTupleDesc_ != null ? bufferedTupleDesc_.getId() : "null")
        .add("partitionByLt",
          partitionByLessThan_ != null ? partitionByLessThan_.debugString() : "null")
        .add("orderByLt",
          orderByLessThan_ != null ? orderByLessThan_.debugString() : "null")
        .addValue(super.debugString())
        .toString();
  }

  /**
   * Create '<' predicate between exprs of input tuple and buffered tuple
   * ('exprs' refers to input tuple).
   * Example:
   * (tid0_slot0 < tid1_slot0)
   *   || (tid0_slot0 == tid1_slot0 && tid0_slot1 < tid1_slot1)
   *   || ...
   */
  private Expr createLessThan(Analyzer analyzer, List<Expr> exprs) {
    Preconditions.checkState(!exprs.isEmpty());
    TupleId inputTid = tupleIds_.get(0);
    Preconditions.checkState(exprs.get(0).isBound(inputTid));
    Expr result = new BinaryPredicate(BinaryPredicate.Operator.LT,
        exprs.get(0).clone(), exprs.get(0).substitute(bufferedTupleSmap_, analyzer));
    for (int i = 1; i < exprs.size(); ++i) {
      Expr eqClause = new BinaryPredicate(BinaryPredicate.Operator.EQ,
          exprs.get(i - 1).clone(),
          exprs.get(i - 1).substitute(bufferedTupleSmap_, analyzer));
      Preconditions.checkState(exprs.get(i).isBound(inputTid));
      Expr ltClause = new BinaryPredicate(BinaryPredicate.Operator.LT,
          exprs.get(i).clone(), exprs.get(i).substitute(bufferedTupleSmap_, analyzer));
      result = new CompoundPredicate(CompoundPredicate.Operator.OR,
          result,
          new CompoundPredicate(CompoundPredicate.Operator.AND, eqClause, ltClause));
    }
    try {
      result.analyze(analyzer);
    } catch (AnalysisException e) {
      throw new IllegalStateException(e.getMessage());
    }
    return result;
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.ANALYTIC_EVAL_NODE;
    msg.analytic_node = new TAnalyticNode();
    msg.analytic_node.setIntermediate_tuple_id(intermediateTupleDesc_.getId().asInt());
    msg.analytic_node.setOutput_tuple_id(outputTupleDesc_.getId().asInt());
    msg.analytic_node.setPartition_exprs(Expr.treesToThrift(substitutedPartitionExprs_));
    msg.analytic_node.setOrder_by_exprs(Expr.treesToThrift(orderingExprs_));
    msg.analytic_node.setAnalytic_functions(Expr.treesToThrift(analyticFnCalls_));
    if (analyticWindow_ != null) {
      msg.analytic_node.setWindow(analyticWindow_.toThrift());
    }
    if (bufferedTupleDesc_ != null) {
      msg.analytic_node.setBuffered_tuple_id(bufferedTupleDesc_.getId().asInt());
    }
    if (partitionByLessThan_ != null) {
      msg.analytic_node.setPartition_by_lt(partitionByLessThan_.treeToThrift());
    }
    if (orderByLessThan_ != null) {
      msg.analytic_node.setOrder_by_lt(orderByLessThan_.treeToThrift());
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

      if (!orderingExprs_.isEmpty()) {
        output.append(detailPrefix + "order by: ");
        strings.clear();
        for (Expr orderingExpr: orderingExprs_) {
          strings.add(orderingExpr.toSql());
        }
        output.append(Joiner.on(", ").join(strings));
        output.append("\n");
      }

      if (analyticWindow_ != null) {
        output.append(detailPrefix + "window: ");
        output.append(analyticWindow_.toSql());
        output.append("\n");
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
