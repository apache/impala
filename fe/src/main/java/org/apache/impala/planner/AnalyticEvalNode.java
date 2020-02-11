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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.impala.analysis.AnalyticWindow;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.IsNullPredicate;
import org.apache.impala.analysis.OrderByElement;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.analysis.CompoundPredicate.Operator;
import org.apache.impala.thrift.TAnalyticNode;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * Computation of analytic exprs.
 */
public class AnalyticEvalNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(AnalyticEvalNode.class);

  private List<Expr> analyticFnCalls_;

  // Partitioning exprs from the AnalyticInfo
  private final List<Expr> partitionExprs_;

  // TODO: Remove when the BE uses partitionByLessThan rather than the exprs
  private List<Expr> substitutedPartitionExprs_;
  private List<OrderByElement> orderByElements_;
  private final AnalyticWindow analyticWindow_;

  // Physical tuples used/produced by this analytic node.
  private final TupleDescriptor intermediateTupleDesc_;
  private final TupleDescriptor outputTupleDesc_;

  // maps from the logical output slots in logicalTupleDesc_ to their corresponding
  // physical output slots in outputTupleDesc_
  private final ExprSubstitutionMap logicalToPhysicalSmap_;

  // Predicates constructed from partitionExprs_/orderingExprs_ to
  // compare input to buffered tuples. Initialized by constructEqExprs().
  // Must be recomputed if the sort tuple is changed, e.g. by projection.
  private Expr partitionByEq_;
  private Expr orderByEq_;
  private TupleDescriptor bufferedTupleDesc_;

  public AnalyticEvalNode(
      PlanNodeId id, PlanNode input, List<Expr> analyticFnCalls,
      List<Expr> partitionExprs, List<OrderByElement> orderByElements,
      AnalyticWindow analyticWindow, TupleDescriptor intermediateTupleDesc,
      TupleDescriptor outputTupleDesc, ExprSubstitutionMap logicalToPhysicalSmap) {
    super(id, "ANALYTIC");
    Preconditions.checkState(!tupleIds_.contains(outputTupleDesc.getId()));
    analyticFnCalls_ = analyticFnCalls;
    partitionExprs_ = partitionExprs;
    orderByElements_ = orderByElements;
    analyticWindow_ = analyticWindow;
    intermediateTupleDesc_ = intermediateTupleDesc;
    outputTupleDesc_ = outputTupleDesc;
    logicalToPhysicalSmap_ = logicalToPhysicalSmap;
    children_.add(input);
    computeTupleIds();
  }

  @Override
  public void computeTupleIds() {
    clearTupleIds();
    tupleIds_.addAll(getChild(0).getTupleIds());
    // we're materializing the input row augmented with the analytic output tuple
    tupleIds_.add(outputTupleDesc_.getId());
    nullableTupleIds_.addAll(getChild(0).getNullableTupleIds());
  }

  @Override
  public boolean isBlockingNode() { return false; }
  public List<Expr> getPartitionExprs() { return partitionExprs_; }
  public List<OrderByElement> getOrderByElements() { return orderByElements_; }

  @Override
  public void init(Analyzer analyzer) {
    Preconditions.checkState(conjuncts_.isEmpty());
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

    if (LOG.isTraceEnabled()) {
      LOG.trace("desctbl: " + analyzer.getDescTbl().debugString());
    }

    // point fn calls, partition and ordering exprs at our input
    ExprSubstitutionMap childSmap = getCombinedChildSmap();
    analyticFnCalls_ = Expr.substituteList(analyticFnCalls_, childSmap, analyzer, false);
    substitutedPartitionExprs_ = Expr.substituteList(partitionExprs_, childSmap,
        analyzer, false);
    orderByElements_ = OrderByElement.substitute(orderByElements_, childSmap, analyzer);
    constructEqExprs(analyzer);
    if (LOG.isTraceEnabled()) LOG.trace("evalnode: " + debugString());
  }

  /**
   * Create partition-by (pb) and order-by (ob) less-than predicates between the input
   * tuple (the output of the preceding sort, which is always the first tuple in the
   * input row) and a buffered tuple that is identical to the input tuple. We need a
   * different tuple descriptor for the buffered tuple because the generated predicates
   * should compare two different tuple instances from the same input stream (i.e., the
   * predicates should be evaluated over a row that is composed of the input and the
   * buffered tuple).
   *
   * Requires 'substitutedPartitionExprs_' and 'orderByElements_' to be initialized.
   * Sets 'partitionByEq_', 'orderByEq_' and 'bufferedTupleDesc_'.
   */
  private void constructEqExprs(Analyzer analyzer) {
    // we need to remap the pb/ob exprs to a) the sort output, b) our buffer of the
    // sort input
    ExprSubstitutionMap bufferedSmap = new ExprSubstitutionMap();
    TupleId sortTupleId = getChild(0).getTupleIds().get(0);
    boolean hasActivePartition = !Expr.allConstant(substitutedPartitionExprs_);
    boolean hasOrderBy = !orderByElements_.isEmpty();
    if (hasActivePartition || hasOrderBy) {
      // create bufferedTupleDesc and bufferedSmap
      bufferedTupleDesc_ =
          analyzer.getDescTbl().copyTupleDescriptor(sortTupleId, "buffered-tuple");
      if (LOG.isTraceEnabled()) {
        LOG.trace("desctbl: " + analyzer.getDescTbl().debugString());
      }

      List<SlotDescriptor> inputSlots = analyzer.getTupleDesc(sortTupleId).getSlots();
      List<SlotDescriptor> bufferedSlots = bufferedTupleDesc_.getSlots();
      for (int i = 0; i < inputSlots.size(); ++i) {
        bufferedSmap.put(
            new SlotRef(inputSlots.get(i)), new SlotRef(bufferedSlots.get(i)));
      }
    }
    if (hasActivePartition) {
      partitionByEq_ =
          createNullMatchingEquals(analyzer, substitutedPartitionExprs_, sortTupleId, bufferedSmap);
      if (LOG.isTraceEnabled()) {
        LOG.trace("partitionByEq: " + partitionByEq_.debugString());
      }
    }
    if (hasOrderBy) {
      orderByEq_ = createNullMatchingEquals(analyzer,
          OrderByElement.getOrderByExprs(orderByElements_), sortTupleId, bufferedSmap);
      if (LOG.isTraceEnabled()) {
        LOG.trace("orderByEq: " + orderByEq_.debugString());
      }
    }
  }

  /**
   * Create a predicate that checks if all exprs are equal or both sides are null.
   */
  private Expr createNullMatchingEquals(Analyzer analyzer, List<Expr> exprs,
      TupleId inputTid, ExprSubstitutionMap bufferedSmap) {
    Preconditions.checkState(!exprs.isEmpty());
    Expr result = createNullMatchingEqualsAux(analyzer, exprs, 0, inputTid, bufferedSmap);
    result.analyzeNoThrow(analyzer);
    return result;
  }

  /**
   * Create an unanalyzed predicate that checks if elements >= i are equal or
   * both sides are null.
   *
   * The predicate has the form
   * ((lhs[i] is null && rhs[i] is null) || (
   *   lhs[i] is not null && rhs[i] is not null && lhs[i] = rhs[i]))
   * && <createEqualsAux(i + 1)>
   */
  private Expr createNullMatchingEqualsAux(Analyzer analyzer, List<Expr> elements, int i,
      TupleId inputTid, ExprSubstitutionMap bufferedSmap) {
    if (i > elements.size() - 1) return new BoolLiteral(true);

    // compare elements[i]
    Expr lhs = elements.get(i);
    Preconditions.checkState(lhs.isBound(inputTid));
    Expr rhs = lhs.substitute(bufferedSmap, analyzer, false);

    Expr bothNull = new CompoundPredicate(
        Operator.AND, new IsNullPredicate(lhs, false), new IsNullPredicate(rhs, false));
    Expr lhsEqRhsNotNull = new CompoundPredicate(Operator.AND,
        new CompoundPredicate(
            Operator.AND, new IsNullPredicate(lhs, true), new IsNullPredicate(rhs, true)),
        new BinaryPredicate(BinaryPredicate.Operator.EQ, lhs, rhs));
    Expr remainder =
        createNullMatchingEqualsAux(analyzer, elements, i + 1, inputTid, bufferedSmap);
    return new CompoundPredicate(CompoundPredicate.Operator.AND,
        new CompoundPredicate(Operator.OR, bothNull, lhsEqRhsNotNull), remainder);
  }

  @Override
  protected void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality_ = getChild(0).cardinality_;
    cardinality_ = capCardinalityAtLimit(cardinality_);
  }

  @Override
  protected String debugString() {
    List<String> orderByElementStrs = new ArrayList<>();
    for (OrderByElement element: orderByElements_) {
      orderByElementStrs.add(element.toSql());
    }
    return MoreObjects.toStringHelper(this)
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
      List<String> strings = new ArrayList<>();
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
        output.append(detailPrefix
            + "predicates: " + Expr.getExplainString(conjuncts_, detailLevel) + "\n");
      }
    }
    return output.toString();
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    Preconditions.checkNotNull(
        fragment_, "PlanNode must be placed into a fragment before calling this method.");
    // TODO: come up with estimate based on window
    long perInstanceMemEstimate = 0;

    // Analytic uses a single buffer size.
    long bufferSize = computeMaxSpillableBufferSize(
        queryOptions.getDefault_spillable_buffer_size(), queryOptions.getMax_row_size());

    // Must be kept in sync with MIN_REQUIRED_BUFFERS in AnalyticEvalNode in be.
    long perInstanceMinMemReservation = 2 * bufferSize;
    nodeResourceProfile_ = new ResourceProfileBuilder()
        .setMemEstimateBytes(perInstanceMemEstimate)
        .setMinMemReservationBytes(perInstanceMinMemReservation)
        .setSpillableBufferBytes(bufferSize).setMaxRowBufferBytes(bufferSize).build();
  }
}
