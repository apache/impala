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

import org.apache.impala.analysis.AnalyticExpr;
import org.apache.impala.analysis.AnalyticWindow;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.CompoundPredicate.Operator;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.IsNullPredicate;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.OrderByElement;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.SortInfo;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.catalog.Function;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TAnalyticNode;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.ExprUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  /**
   * Returns whether it should be computed in a single unpartitioned fragment.
   * True when Partition-By and Order-By exprs are all empty or constant.
   */
  public boolean requiresUnpartitionedEval() {
    // false when any Partition-By/Order-By exprs are non-constant
    if (!Expr.allConstant(partitionExprs_)) return false;
    for (OrderByElement orderBy : orderByElements_) {
      if (!orderBy.getExpr().isConstant()) return false;
    }
    return true;
  }

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
    Preconditions.checkState(lhs.isBound(inputTid), lhs.debugString());
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
  public void computeStats(Analyzer analyzer) {
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
  public void computeProcessingCost(TQueryOptions queryOptions) {
    // The total cost per row is the sum of the evaluation costs for analytic functions,
    // partition by equal and order by equal predicate. 'partitionByEq_' and 'orderByEq_'
    // are excluded since the input data stream is already partitioned and sorted within
    // each partition (see notes on class AnalyticEvalNode in analytic-eval-node.h).
    float totalCostToEvalOneRow = ExprUtil.computeExprsTotalCost(analyticFnCalls_);
    processingCost_ = ProcessingCost.basicCost(
        getDisplayLabel(), getCardinality(), totalCostToEvalOneRow);
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

  public static class LimitPushdownInfo {
    public LimitPushdownInfo(boolean includeTies, int additionalLimit) {
      this.includeTies = includeTies;
      this.additionalLimit = additionalLimit;
    }

    // Whether ties need to be returned from the top-n operator.
    public final boolean includeTies;

    // Amount that needs to be added to limit pushed down.
    public final int additionalLimit;
  }

  /**
   * Check if it is safe to push down limit to the Sort node of this AnalyticEval.
   * Qualifying checks:
   *  - The analytic node is evaluating a single analytic function which must be
   *    a ranking function.
   *  - The partition-by exprs must be a prefix of the sort exprs in sortInfo
   *  - If there is a predicate on the analytic function (provided through the
   *    selectNode), the predicate's eligibility is checked (see further below)
   * @param sortInfo The sort info from the outer sort node
   * @param sortInputSmap the input smap that can be applied to the sort exprs from
   *        sortInfo to get exprs referencing its input tuple.
   * @param selectNode The selection node with predicates on analytic function.
   *    This can be null if no such predicate is present.
   * @param limit Limit value from the outer sort node
   * @param analyticNodeSort The analytic sort associated with this analytic node
   * @param analyzer analyzer instance
   * @return parameters for limit pushdown into analytic sort if allowed, null if not
   */
  public LimitPushdownInfo checkForLimitPushdown(SortInfo sortInfo,
          ExprSubstitutionMap sortInputSmap, SelectNode selectNode, long limit,
          SortNode analyticNodeSort, Analyzer analyzer) {
    // Pushing is only supported for certain types of sort.
    if (!analyticNodeSort.isPartitionedTopN() && !analyticNodeSort.isTotalSort()) {
      return null;
    }
    if (analyticFnCalls_.size() != 1) return null;
    Expr expr = analyticFnCalls_.get(0);
    if (!(expr instanceof FunctionCallExpr) ||
         (!AnalyticExpr.isRankingFn(((FunctionCallExpr) expr).getFn()))) {
      return null;
    }
    List<Expr> analyticSortSortExprs = analyticNodeSort.getSortInfo().getSortExprs();

    // In the mapping below, we use the original sort exprs that the sortInfo was
    // created with, not the sort exprs that got mapped in SortInfo.createSortTupleInfo().
    // This allows us to substitute it so that it references the analytic sort tuple.
    List<Expr> origSortExprs = sortInfo != null ? sortInfo.getOrigSortExprs() :
            new ArrayList<>();
    List<Expr> sortExprs = Expr.substituteList(origSortExprs, sortInputSmap,
            analyzer, false);
    // Also use substituted partition exprs such that they can be compared with the
    // sort exprs
    List<Expr> pbExprs = substitutedPartitionExprs_;

    if (sortExprs.size() == 0) {
      return checkForUnorderedLimitPushdown(selectNode, limit, analyticNodeSort);
    }

    Preconditions.checkArgument(analyticSortSortExprs.size() >= pbExprs.size());
    // Check if pby exprs are a prefix of the top level sort exprs
    if (sortExprs.size() > 0) {
      if (!analyticSortExprsArePrefix(
              sortInfo, sortExprs, analyticNodeSort.getSortInfo(), pbExprs)) {
        return null;
      }
    }
    // check that the window frame is UNBOUNDED PRECEDING to CURRENT ROW
    if (!(analyticWindow_.getLeftBoundary().getType() ==
          AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING
          && analyticWindow_.getRightBoundary().getType() ==
            AnalyticWindow.BoundaryType.CURRENT_ROW)) {
      return null;
    }

    if (selectNode == null) {
      // Do not support pushing additional limit to partitioned top-n, unless it was
      // the predicate that the partitioned top-n was originally created from.
      if (analyticNodeSort.isPartitionedTopN()) return null;
      Preconditions.checkState(analyticNodeSort.isTotalSort());
      // Limit pushdown to total sort is valid if the pre-analytic sort puts rows into
      // the same order as sortExprs. We check the prefix match, since extra analytic
      // sort exprs do not affect the compatibility of the ordering with sortExprs.
      if (!analyticSortExprsArePrefix(sortInfo, sortExprs,
              analyticNodeSort.getSortInfo(), analyticSortSortExprs)) {
        return null;
      }
      return new LimitPushdownInfo(false, 0);
    } else {
      Pair<LimitPushdownInfo, Double> status = checkPredEligibleForLimitPushdown(
              analyticNodeSort, selectNode.getConjuncts(), limit);
      if (status.first != null) {
        selectNode.setSelectivity(status.second);
        return status.first;
      }
    }
    return null;
  }

  /**
   * Helper for {@link #checkForLimitPushdown()} that handles the case of pushing
   * down a limit into an total sort or partitioned top N where there are no
   * ordering expressions.
   */
  private LimitPushdownInfo checkForUnorderedLimitPushdown(SelectNode selectNode,
          long limit, SortNode analyticNodeSort) {
    // Only support transforming total sort for now.
    if (!analyticNodeSort.isTotalSort()) return null;
    // if there is no sort expr in the parent sort but only limit, we can push
    // the limit to the sort below if there is no selection node or if
    // the predicate in the selection node is eligible
    if (selectNode == null) {
      return new LimitPushdownInfo(false, 0);
    }
    Pair<LimitPushdownInfo, Double> status = checkPredEligibleForLimitPushdown(
            analyticNodeSort, selectNode.getConjuncts(), limit);
    if (status.first != null) {
      selectNode.setSelectivity(status.second);
      return status.first;
    }
    return null;
  }

  /**
   * Checks if 'analyticSortExprs' is a prefix of the 'sortExprs' from an outer sort.
   * @param sortInfo sort info from the outer sort
   * @param sortExprs sort exprs from the outer sort. Must be a prefix of the full
   *                sort exprs from sortInfo.
   * @param analyticSortInfo sort info from the analytic sort
   * @param analyticSortExprs sort expressions from the analytic sort. Must be a prefix
   *                of the full sort exprs from analyticSortInfo.
   */
  private boolean analyticSortExprsArePrefix(SortInfo sortInfo, List<Expr> sortExprs,
          SortInfo analyticSortInfo, List<Expr> analyticSortExprs) {
    if (analyticSortExprs.size() > sortExprs.size()) return false;
    for (int i = 0; i < analyticSortExprs.size(); i++) {
      if (!analyticSortExprs.get(i).equals(sortExprs.get(i))) return false;
      // check the ASC/DESC and NULLS FIRST/LAST compatibility.
      if (!sortInfo.getIsAscOrder().get(i).equals(
              analyticSortInfo.getIsAscOrder().get(i))) {
        return false;
      }
      if (!sortInfo.getNullsFirst().get(i).equals(
              analyticSortInfo.getNullsFirst().get(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check eligibility of a predicate (provided as list of conjuncts) for limit
   * pushdown optimization.
   *
   * Pushing down the limit past the predicate is not, in general, possible because
   * the predicate can filter out an arbitrary number of rows flowing returned from
   * the analytic sort. However, if the predicate is a rank() or row_number() analytic
   * predicate, in some cases we can determine an upper bound on the number of rows
   * returned from the analytic sort that will end up in the output of the final top-n.
   *
   * The following conditions must be met:
   * 1. The final top-n sort order must match the analytic partition sort order.
   *    The order within the partition doesn't matter for the purposes of the pushdown
   *    decision.
   * 2. A single rank() or row_number() predicate evaluated by the analytic operator
   *   between the final top-n and analytic sort.
   * 3. The max rows returned from each analytic partition - P - must be >= the limit
   *    being pushed down - N. (Note: ties in rank() means that P is not a strict limit
   *    on rows returned from each partition, but extra rows don't invalidate the
   *    argument below).
   *
   * In this case the rows returned from the final sort are going to come from the
   * first analytic partitions because of #1. I.e. if we go through the partitions
   * in order, until the row count adds up to N, then that is the set of partitions
   * that can possibly end up in the output of the final top-n.
   * If P >= N, we will never hit a partition limit before we hit N.
   *
   * If P < N, we could hit a partition limit and filter out an arbitrary number of
   * rows before we find the next row to be returned, therefore the optimization
   * can't be applied.
   *
   * The ordering within a partition in the analytic sort may be different from the
   * final order, so we need to make sure to include all of the relevant rows in the
   * final partition, which we can do by pushing down a limit of P + N, and including
   * ties if the predicate is rank().
   *
   * @param conjuncts list of conjuncts from the predicate
   * @param limit limit from final sort
   * @return a Pair whose first value is non-null if the conjuncts+limit allows pushdown,
   *   null otherwise. Second value is the predicate's estimated selectivity
   */
  private Pair<LimitPushdownInfo, Double> checkPredEligibleForLimitPushdown(
          SortNode analyticNodeSort, List<Expr> conjuncts, long limit) {
    Pair<LimitPushdownInfo, Double> falseStatus = new Pair<>(null, -1.0);
    // Currently, single conjuncts are supported.  In the future, multiple conjuncts
    // involving a range e.g 'col >= 10 AND col <= 20' could potentially be supported
    if (conjuncts.size() > 1) return falseStatus;
    Expr conj = conjuncts.get(0);
    if (!(Expr.IS_BINARY_PREDICATE.apply(conj))) return falseStatus;
    BinaryPredicate pred = (BinaryPredicate) conj;
    Expr lhs = pred.getChild(0);
    Expr rhs = pred.getChild(1);
    // Lhs of the binary predicate must be a ranking function.
    // Also, it must be bound to the output tuple of this analytic eval node
    if (!(lhs instanceof SlotRef) ||
            !lhs.isBound(outputTupleDesc_.getId())) {
      return falseStatus;
    }
    List<Expr> lhsSourceExprs = ((SlotRef) lhs).getDesc().getSourceExprs();
    if (lhsSourceExprs.size() != 1 ||
          !(lhsSourceExprs.get(0) instanceof AnalyticExpr)) {
      return falseStatus;
    }

    boolean includeTies;
    Function fn = ((AnalyticExpr) lhsSourceExprs.get(0)).getFnCall().getFn();
    if (AnalyticExpr.isAnalyticFn(fn, AnalyticExpr.RANK)) {
      // RANK() assigns equal values to rows that compare equal. Thus if there are
      // ties for the max RANK() value to be returned, they all pass the filter.
      // E.g. if the predicate is RANK() <= 3, and the values we are ordering by are
      // 1, 42, 99, 99, 100, then the RANK() values are 1, 2, 3, 3, 5 and we need
      // to return the top 4 rows:
      // 1, 42, 99, 99. We do not need special handling for ties except at the limit
      // value. E.g. if the ordering values were 1, 1, 42, 99, 100, then the RANK()
      // values would be 1, 1, 3, 4, 5 and we only return the top 3.
      includeTies = true;
    } else if (AnalyticExpr.isAnalyticFn(fn, AnalyticExpr.ROWNUMBER)) {
      includeTies = false;
    } else {
      return falseStatus;
    }

    // Restrict the pushdown for =, <, <= predicates because these ensure the
    // qualifying rows are fully 'contained' within the LIMIT value. Other
    // types of predicates would select rows that fall outside the LIMIT range.
    if (!(pred.getOp() == BinaryPredicate.Operator.EQ ||
          pred.getOp() == BinaryPredicate.Operator.LT ||
          pred.getOp() == BinaryPredicate.Operator.LE)) {
      return falseStatus;
    }
    // Rhs of the predicate must be a numeric literal we need to add the value
    // to the limit to get correct results when the ordering expressions don't
    // exactly match between the analytic sort and the parent sort.
    if (!(rhs instanceof NumericLiteral)) return falseStatus;
    long analyticLimit = ((NumericLiteral)rhs).getLongValue();
    if (pred.getOp() == BinaryPredicate.Operator.LT) analyticLimit--;

    // Equality predicates can filter out an arbitrary number of rows from
    // each analytic partition, so it is not safe to push the limit down.
    if (pred.getOp() == BinaryPredicate.Operator.EQ && limit > 1) return falseStatus;

    // Check that the partition predicate matches the partition limits already pushed
    // to a partitioned top-n node.
    if (analyticNodeSort.isPartitionedTopN() &&
            (analyticLimit != analyticNodeSort.getPerPartitionLimit() ||
             includeTies != analyticNodeSort.isIncludeTies())) {
      return falseStatus;
    }

    LimitPushdownInfo pushdownInfo = checkLimitEligibleForPushdown(
            limit, includeTies, analyticLimit);
    if (pushdownInfo == null) return falseStatus;

    double selectivity = Expr.DEFAULT_SELECTIVITY;
    // Since the predicate is qualified for limit pushdown, estimate its selectivity.
    // For EQ conditions, leave it as the default.  For LT and LE, assume all of the
    // 'limit' rows will be returned.
    if (pred.getOp() == BinaryPredicate.Operator.LT ||
            pred.getOp() == BinaryPredicate.Operator.LE) {
      selectivity = 1.0;
    }
    return new Pair<LimitPushdownInfo, Double>(pushdownInfo, selectivity);
  }

  /**
   * Implements the same check described in
   * {@link #checkLimitEligibleForPushdown(long, boolean, long)} where we
   * see if 'limit' can be pushed down into the pre-analytic sort where the
   * there is a per-partition limit for the analytic 'analyticLimit', i.e.
   * a row_number() < 10 predicate or similar.
   */
  private LimitPushdownInfo checkLimitEligibleForPushdown(long limit,
          boolean includeTies, long analyticLimit) {
    // See method comment for explanation of why the analytic partition limit
    // must be >= the pushed down limit.
    if (analyticLimit < limit) return null;

    // Avoid overflow of limit.
    if (analyticLimit + (long)limit > Integer.MAX_VALUE) return null;

    return new LimitPushdownInfo(includeTies, (int)analyticLimit);
  }

  public TupleDescriptor getOutputTupleDesc() { return outputTupleDesc_; }
  public List<Expr> getAnalyticFnCalls() { return analyticFnCalls_; }
  public AnalyticWindow getAnalyticWindow() { return analyticWindow_; }

}
