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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.impala.analysis.AggregateInfo;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.CaseExpr;
import org.apache.impala.analysis.CaseWhenClause;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.MultiAggregateInfo.AggPhase;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.analysis.ValidTupleIdExpr;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.ThriftSerializationCtx;
import org.apache.impala.thrift.QueryConstants;
import org.apache.impala.thrift.TAggregationNode;
import org.apache.impala.thrift.TAggregator;
import org.apache.impala.thrift.TBackendResourceProfile;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.BitUtil;
import org.apache.impala.util.MathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Aggregation computation.
 *
 */
public class AggregationNode extends PlanNode implements SpillableOperator {
  private static final Logger LOG = LoggerFactory.getLogger(AggregationNode.class);

  // Default per-instance memory requirement used if no valid stats are available.
  // TODO: Come up with a more useful heuristic.
  private final static long DEFAULT_PER_INSTANCE_MEM = 128L * 1024L * 1024L;

  // Default skew factor to account for data skew among fragment instances.
  private final static double DEFAULT_SKEW_FACTOR = 1.5;

  // Non-grouping aggregation class always results in one group even if there are
  // zero input rows.
  private final static long NON_GROUPING_AGG_NUM_GROUPS = 1;

  private final MultiAggregateInfo multiAggInfo_;
  private final AggPhase aggPhase_;

  // Aggregation-class infos derived from 'multiAggInfo_' and 'aggPhase_' in c'tor.
  private final List<AggregateInfo> aggInfos_;

  // If true, this node produces intermediate aggregation tuples.
  private boolean useIntermediateTuple_ = false;

  // If true, this node performs the finalize step.
  private boolean needsFinalize_ = false;

  // If true, this is a preagg node. Set by setIsPreagg().
  private boolean isPreagg_ = false;

  // If true, this node uses streaming preaggregation. Invalid if this is a merge agg.
  private boolean useStreamingPreagg_ = false;

  // Resource profiles for each aggregation class.
  // Set in computeNodeResourceProfile().
  private List<ResourceProfile> resourceProfiles_;

  // If the group clause is empty ( aggInfo.getGroupingExprs() is empty ),
  // the hash table will not be created.
  // Peak memory is at least 16k, which is an empirical value
  protected final static long MIN_PLAIN_AGG_MEM = 16L * 1024L;

  // The output is from a non-correlated scalar subquery returning at most one value.
  protected boolean isNonCorrelatedScalarSubquery_ = false;

  // The aggregate input cardinality of this node.
  // Initialized in computeStats(). Depending on the aggPhase_ of this node and value of
  // multiAggInfo_.getIsGroupingSet(), it can be initialized with either the input
  // cardinality of the related FIRST phase aggregation or this node's immediate input
  // cardinality (this child's cardinality).
  private long aggInputCardinality_ = -1;

  // Hold the estimated number of groups that will be produced by each aggregation class.
  // In other words, this is a global NDV of grouping columns produced by each
  // aggregation class, regardless of number of fragment instances.
  // Initialized in computeStats(). May contain -1 if an aggregation class num group
  // can not be estimated.
  private List<Long> aggClassNumGroups_;

  // Hold the estimated number of rows that will be produced by each aggregation class.
  // The numbers in this list can be higher than the number in the same indices at
  // aggClassNumGroups_ because it may account for duplicate keys across fragments
  // instances. These numbers do account for limit if canCompleteEarly(), but do not
  // account for row filtering by conjunct.
  // Initialized in computeStats().
  private List<Long> aggClassOutputCardinality_;

  // Determine whether to do tuple-based cardinality estimation or skip it.
  // This flag is here to avoid severe cardinality and memory estimation after
  // introduction of tuple-based analysis (IMPALA-13405).
  // May set to true in computeStats() and will stay true during lifetime of this
  // AggregationNode.
  // TODO: IMPALA-13542
  private boolean skipTupleAnalysis_ = false;

  public AggregationNode(
      PlanNodeId id, PlanNode input, MultiAggregateInfo multiAggInfo, AggPhase aggPhase) {
    super(id, "AGGREGATE");
    children_.add(input);
    multiAggInfo_ = multiAggInfo;
    aggInfos_ = multiAggInfo_.getMaterializedAggInfos(aggPhase);
    aggPhase_ = aggPhase;
    needsFinalize_ = true;
    computeTupleIds();
  }

  /**
   * Copy c'tor used in clone().
   */
  private AggregationNode(PlanNodeId id, AggregationNode src) {
    super(id, src, "AGGREGATE");
    multiAggInfo_ = src.multiAggInfo_;
    aggPhase_ = src.aggPhase_;
    aggInfos_ = src.aggInfos_;
    needsFinalize_ = src.needsFinalize_;
    useIntermediateTuple_ = src.useIntermediateTuple_;
    isNonCorrelatedScalarSubquery_ = src.isNonCorrelatedScalarSubquery_;
  }

  @Override
  public void computeTupleIds() {
    clearTupleIds();
    for (AggregateInfo aggInfo : aggInfos_) {
      TupleId aggClassTupleId = null;
      if (useIntermediateTuple_) {
        aggClassTupleId = aggInfo.getIntermediateTupleId();
      } else {
        aggClassTupleId = aggInfo.getOutputTupleId();
      }
      tupleIds_.add(aggClassTupleId);
      tblRefIds_.add(aggClassTupleId);
      // Nullable tuples are only required to distinguish between multiple
      // aggregation classes.
      if (aggInfos_.size() > 1) {
        nullableTupleIds_.add(aggClassTupleId);
      }
    }
  }

  /**
   * Sets this node as a preaggregation. Must recompute stats if called.
   */
  public void setIsPreagg(PlannerContext ctx) {
    isPreagg_ = true;
    if (ctx.getQueryOptions().disable_streaming_preaggregations) {
      useStreamingPreagg_ = false;
      return;
    }
    for (AggregateInfo aggInfo : aggInfos_) {
      if (aggInfo.getGroupingExprs().size() > 0) {
        useStreamingPreagg_ = true;
        return;
      }
    }
  }

  /**
   * Unsets this node as requiring finalize. Only valid to call this if it is
   * currently marked as needing finalize.
   */
  public void unsetNeedsFinalize() {
    Preconditions.checkState(needsFinalize_);
    needsFinalize_ = false;
  }

  public void setIntermediateTuple() {
    useIntermediateTuple_ = true;
    computeTupleIds();
  }

  public MultiAggregateInfo getMultiAggInfo() { return multiAggInfo_; }
  public AggPhase getAggPhase() { return aggPhase_; }
  public boolean hasGrouping() { return multiAggInfo_.hasGrouping(); }
  public boolean isSingleClassAgg() { return aggInfos_.size() == 1; }

  public boolean isDistinctAgg() {
    for (AggregateInfo aggInfo : aggInfos_) {
      if (aggInfo.isDistinctAgg()) return true;
    }
    return false;
  }

  @Override
  public boolean isBlockingNode() { return !useStreamingPreagg_; }

  @Override
  public void init(Analyzer analyzer) throws InternalException {
    init(analyzer, null);
  }

  public void init(Analyzer analyzer, List<Expr> transferredConjuncts)
      throws InternalException {
    Preconditions.checkState(tupleIds_.size() == aggInfos_.size());
    // Assign conjuncts to the top-most agg in the single-node plan. They are transferred
    // to the proper place in the distributed plan via transferConjuncts argument.
    if (aggPhase_ == multiAggInfo_.getConjunctAssignmentPhase()) {
      conjuncts_.clear();
      // TODO: If this is the transposition phase, then we can push conjuncts that
      // reference a single aggregation class down into the aggregators of the
      // previous phase.
      conjuncts_.addAll(multiAggInfo_.collectConjuncts(analyzer, true));
    }
    if (transferredConjuncts != null) conjuncts_.addAll(transferredConjuncts);
    conjuncts_ = orderConjunctsByCost(conjuncts_);

    // Compute the mem layout for both tuples here for simplicity.
    for (AggregateInfo aggInfo : aggInfos_) {
      aggInfo.getOutputTupleDesc().computeMemLayout();
      aggInfo.getIntermediateTupleDesc().computeMemLayout();
    }

    // Do at the end so it can take all conjuncts into account
    computeStats(analyzer);

    // don't call createDefaultSMap(), it would point our conjuncts (= Having clause)
    // to our input; our conjuncts don't get substituted because they already
    // refer to our output
    outputSmap_ = getCombinedChildSmap();

    // Substitute exprs and check consistency.
    // All of the AggregationNodes corresponding to a MultiAggregationInfo will have the
    // same outputSmap_, so just substitute it once.
    if (aggPhase_ == AggPhase.FIRST) multiAggInfo_.substitute(outputSmap_, analyzer);
    for (AggregateInfo aggInfo : aggInfos_) {
      aggInfo.substitute(outputSmap_, analyzer);
      aggInfo.checkConsistency();
    }
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality_ = 0;
    aggInputCardinality_ = getFirstAggInputCardinality();
    Preconditions.checkState(aggInputCardinality_ >= -1, aggInputCardinality_);

    AggregationNode preaggNode = null;
    if (aggPhase_.isMerge()) {
      preaggNode = getPrevAggNode(this);
      Preconditions.checkState(preaggNode.aggInfos_.size() == aggInfos_.size(),
          "Merge aggregation %s have mismatch aggInfo count compared to the "
              + "preaggregation %s (%s vs %s)",
          getId(), preaggNode.getId(), aggInfos_.size(), preaggNode.aggInfos_.size());
    }

    // DistributedPlanner.java may transfer conjunct to merge phase aggregation later.
    // Keep skipping tuple-based analysis to maintain same number as single node plan.
    // TODO: IMPALA-13542
    skipTupleAnalysis_ |= !conjuncts_.isEmpty();
    skipTupleAnalysis_ |=
        !analyzer.getQueryOptions().isEnable_tuple_analysis_in_aggregate();

    boolean unknownEstimate = false;
    final boolean canCompleteEarly = canCompleteEarly();
    final boolean estimatePreaggDuplicate =
        isPreagg_ && analyzer.getQueryOptions().isEstimate_duplicate_in_preagg();
    aggClassNumGroups_ = Lists.newArrayList();
    aggClassOutputCardinality_ = Lists.newArrayList();
    int aggIdx = 0;
    for (AggregateInfo aggInfo : aggInfos_) {
      // Compute the cardinality for this set of grouping exprs.
      long numGroups = -1;
      long preaggNumgroup = -1;
      List<Expr> groupingExprs = aggInfo.getGroupingExprs();
      if (preaggNode != null) {
        preaggNumgroup = preaggNode.aggClassNumGroups_.get(aggIdx);
        numGroups =
            estimateNumGroups(groupingExprs, aggInputCardinality_, preaggNumgroup);
      } else {
        numGroups =
            estimateNumGroups(groupingExprs, aggInputCardinality_, this, analyzer);
      }
      Preconditions.checkState(numGroups >= -1, "numGroups is invalid: %s", numGroups);

      if (LOG.isTraceEnabled()) {
        LOG.trace("{} aggPhase={} aggInputCardinality={} aggIdx={} numGroups={} "
                + "preaggNumGroup={} aggInfo={}",
            getDisplayLabel(), aggPhase_, aggInputCardinality_, aggIdx, numGroups,
            preaggNumgroup, aggInfo.debugString());
      }

      aggClassNumGroups_.add(numGroups);
      if (numGroups == -1) {
        // No estimate of the number of groups is possible, can't even make a
        // conservative estimate.
        unknownEstimate = true;
        aggClassOutputCardinality_.add(-1L);
      } else {
        long aggOutputCard = numGroups;
        if (isPreagg_) {
          // Account for duplicate keys on multiple instances in a pre-aggregation.
          // Only do this if input is not partitioned, or if data partition of input
          // is not a subset of grouping expression, or if canCompleteEarly.
          List<Expr> inputPartExprs =
              getFragment().getDataPartition().getPartitionExprs();
          if (numGroups > 0
              && (inputPartExprs.isEmpty()
                  || !Expr.isSubset(inputPartExprs, groupingExprs) || canCompleteEarly)) {
            aggOutputCard = estimatePreaggCardinality(
                fragment_.getNumInstancesForCosting(), numGroups, aggInputCardinality_,
                groupingExprs.isEmpty(), canCompleteEarly, getLimit());
          }
        } else if (canCompleteEarly) {
          aggOutputCard = MathUtil.smallestValidCardinality(aggOutputCard, getLimit());
        }
        aggClassOutputCardinality_.add(aggOutputCard);
        // IMPALA-2945: Behavior change if estimatePreaggDuplicate is true.
        cardinality_ = MathUtil.addCardinalities(
            cardinality_, estimatePreaggDuplicate ? aggOutputCard : numGroups);
      }
      aggIdx++;
    }
    if (unknownEstimate) {
      cardinality_ = -1;
    }

    // Take conjuncts into account.
    long cardBeforeConjunct = cardinality_;
    long cardAfterConjunct = cardinality_;
    if (cardinality_ > 0 && !getConjuncts().isEmpty()) {
      Preconditions.checkState(
          !canCompleteEarly, "canCompleteEarly is true but conjuncts_ is not empty");
      cardinality_ = applyConjunctsSelectivity(cardinality_);
      cardAfterConjunct = cardinality_;
    }

    // IMPALA-2945: Behavior change if estimatePreaggDuplicate is true.
    // limit is already accounted by estimatePreaggCardinality().
    if (!estimatePreaggDuplicate) {
      // IMPALA-2581: preAgg node can have limit.
      cardinality_ = capCardinalityAtLimit(cardinality_);
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("{} cardinality=[BeforeConjunct={} AfterConjunct={} AfterLimit={}]",
          getDisplayLabel(), cardBeforeConjunct, cardAfterConjunct, cardinality_);
    }
  }

  /**
   * Trace back expr until source column for that expr found and then return the
   * TupleDescriptor that holds the column. Return null if no resolution can be found.
   */
  private static @Nullable TupleDescriptor findSourceTupleId(Expr expr) {
    Expr exprToFind = expr;
    Expr lastExpr = null;
    while (exprToFind != null && exprToFind != lastExpr) {
      // Memorize last expression to defend against self-reference expression.
      // There should be no circular reference across expressions.
      lastExpr = exprToFind;
      SlotRef slotRef = exprToFind.unwrapSlotRef(true);

      if (slotRef != null && slotRef.getDesc() != null) {
        SlotDescriptor sd = slotRef.getDesc();

        if (sd.getParent() != null && sd.getType().isScalarType()
            && (sd.getColumn() != null)) {
          LOG.trace("Tracing Slot={}, a simple column slot", slotRef);
          return sd.getParent();
        } else if (sd.getParent() != null && sd.getParent().getSourceView() != null) {
          LOG.trace("Tracing Slot={}, a view slot", slotRef);
          exprToFind = sd.getParent().getSourceView().getBaseTblSmap().get(slotRef);
        } else if (sd.getSourceExprs().size() == 1) {
          LOG.trace("Tracing Slot={}, an intermediate slot with single source", slotRef);
          exprToFind = sd.getSourceExprs().get(0);
        } else if (sd.getSourceExprs().size() > 1 && sd.getParent() != null
            && sd.getType().isScalarType()) {
          LOG.trace("Tracing Slot={}, an intermediate slot with {} sources", slotRef,
              sd.getSourceExprs().size());
          return sd.getParent();
        } else {
          exprToFind = null;
        }
      } else {
        exprToFind = null;
      }
    }
    return null;
  }

  /**
   * Account for duplicate keys on multiple instances in a pre-aggregation.
   * IMPALA-2945: Assuming uniform distribution, if 'perInstanceInputCard' is
   * input cardinality of each AggNode instance, then number of distinct rows among
   * 'perInstanceInputCard' is:
   *
   * (1.0 - ((globalNdv - 1.0) / globalNdv) ^ perInstanceInputCard) * globalNdv
   *
   * Skip this estimation if 'inputCardinality' is less than 'globalNdv' because
   * input rows are most likely unique already.
   * If 'isNonGroupingAggregation' true, simply multiply 'globalNdv' with
   * 'totalInstances'.
   * If 'canCompleteEarly' is true (preagg has limit), return the minimum between
   * ('limit' * 'totalInstances') vs ('globalNdv' * 'totalInstances').
   * In all case, output should not exceed 'inputCardinality' if it is not unknown.
   */
  protected static long estimatePreaggCardinality(long totalInstances, long globalNdv,
      long inputCardinality, boolean isNonGroupingAggregation, boolean canCompleteEarly,
      long limit) {
    Preconditions.checkArgument(totalInstances > 0);
    Preconditions.checkArgument(globalNdv > 0);

    if (canCompleteEarly) {
      Preconditions.checkArgument(limit > -1, "limit must not be negative.");
      long limitMultiple = MathUtil.multiplyCardinalities(limit, totalInstances);
      long ndvMultiple = MathUtil.multiplyCardinalities(globalNdv, totalInstances);
      return MathUtil.smallestValidCardinality(inputCardinality,
          MathUtil.smallestValidCardinality(ndvMultiple, limitMultiple));
    } else if (isNonGroupingAggregation) {
      return MathUtil.smallestValidCardinality(
          inputCardinality, MathUtil.multiplyCardinalities(globalNdv, totalInstances));
    } else if (totalInstances > 1 && inputCardinality > globalNdv) {
      double perInstanceInputCard = Math.ceil((double) inputCardinality / totalInstances);
      double globalNdvInDouble = (double) globalNdv;
      double probValExist = 1.0
          - Math.pow((globalNdvInDouble - 1.0) / globalNdvInDouble, perInstanceInputCard);
      double perInstanceNdv = Math.ceil(probValExist * globalNdvInDouble);
      long preaggOutputCard =
          MathUtil.multiplyCardinalities(Math.round(perInstanceNdv), totalInstances);
      // keep bounding at aggInputCardinality_ max.
      preaggOutputCard =
          MathUtil.smallestValidCardinality(inputCardinality, preaggOutputCard);
      LOG.trace("inputCardinality={} perInstanceInputCard={} globalNdv={} probValExist={}"
              + " perInstanceNdv={} preaggOutputCard={}",
          inputCardinality, perInstanceInputCard, globalNdv, probValExist, perInstanceNdv,
          preaggOutputCard);
      return preaggOutputCard;
    }
    // Input is likely unique already.
    return MathUtil.smallestValidCardinality(inputCardinality, globalNdv);
  }

  /**
   * Estimate the number of groups that will be present for the provided grouping
   * expressions, input cardinality, and num group estimate from preaggregation phase.
   */
  public static long estimateNumGroups(
      List<Expr> groupingExprs, long aggInputCardinality, long preaggNumGroup) {
    Preconditions.checkArgument(aggInputCardinality >= -1, aggInputCardinality);
    if (groupingExprs.isEmpty()) {
      return NON_GROUPING_AGG_NUM_GROUPS;
    } else {
      return MathUtil.smallestValidCardinality(preaggNumGroup, aggInputCardinality);
    }
  }

  /**
   * Estimate the number of groups that will be present for the provided grouping
   * expressions and input cardinality.
   * Returns -1 if a reasonable cardinality estimate cannot be produced.
   */
  public static long estimateNumGroups(List<Expr> groupingExprs, long aggInputCardinality,
      PlanNode planNode, Analyzer analyzer) {
    Preconditions.checkArgument(aggInputCardinality >= -1, aggInputCardinality);
    if (groupingExprs.isEmpty()) return NON_GROUPING_AGG_NUM_GROUPS;
    if (planNode instanceof AggregationNode
        && ((AggregationNode) planNode).skipTupleAnalysis_) {
      // This AggregationNode has been planned with non-empty conjunct before.
      // Skip tuple based to avoid severe underestimation.
      // TODO: IMPALA-13542
      return estimateNumGroups(groupingExprs, aggInputCardinality);
    }

    // This is prone to overestimation, because we keep multiplying cardinalities,
    // even if the grouping exprs are functionally dependent (example:
    // group by the primary key of a table plus a number of other columns from that
    // same table). We limit the estimate to the estimated number of input row to
    // limit the potential overestimation. We also do simple TupleId analysis to
    // lower estimate for expressions that share the same TupleId.

    // Group 'groupingExprs' elements that have the same TupleId.
    Map<TupleDescriptor, List<Expr>> tupleDescToExprs = new HashMap<>();
    List<Expr> exprsWithUniqueTupleId = new ArrayList<>();
    for (Expr expr : groupingExprs) {
      TupleDescriptor tupleDesc = findSourceTupleId(expr);

      if (tupleDesc != null) {
        tupleDescToExprs.putIfAbsent(tupleDesc, new ArrayList<>());
        tupleDescToExprs.get(tupleDesc).add(expr);
        LOG.trace("Slot {} match with tuple {}", expr, tupleDesc.getId());
      } else {
        // expr is not a simple SlotRef.
        exprsWithUniqueTupleId.add(expr);
      }
    }

    List<Long> tupleBasedNumGroups = new ArrayList<>();
    if (tupleDescToExprs.isEmpty()) {
      Preconditions.checkState(exprsWithUniqueTupleId.size() == groupingExprs.size(),
          "Missing expression after TupleId analysis! Expect %s but found %s",
          groupingExprs.size(), exprsWithUniqueTupleId.size());
    } else {
      // Find cardinality of tuple that referenced by multiple expressions.
      // Search the first PlanNode that materialize a specific tupleId.
      // This is done via memo lookup through analyzer.getProducingNode().
      for (Map.Entry<TupleDescriptor, List<Expr>> entry : tupleDescToExprs.entrySet()) {
        List<Expr> exprs = new ArrayList<>(entry.getValue());
        PlanNode producerNode = analyzer.getProducingNode(entry.getKey().getId());
        if (producerNode == null) {
          // Move expressions with unknown PlanNode origin to exprsWithUniqueTupleId.
          exprsWithUniqueTupleId.addAll(entry.getValue());
        } else {
          Preconditions.checkNotNull(exprs, "exprs must not be null");

          // Find the maximum cardinality if conjunct is not applied.
          // TODO: Use producerNode.getCardinality() directly if predicate selectivity
          // is highly accurate (ie., histogram support from HIVE-26221).
          long producerCardinality = -1;
          long filteredNdv = 1;
          if (producerNode.hasHardEstimates_ || producerNode instanceof UnionNode) {
            // UnionNode cardinality is not impacted by its conjuncts_.
            producerCardinality = producerNode.getCardinality();
          } else {
            // It is cheap to still account for limit.
            producerCardinality =
                producerNode.capCardinalityAtLimit(producerNode.getInputCardinality());

            // Check if we can find constant NDV predicates from previous computation in
            // HdfsScanNode.computeStatsTupleAndConjuncts().
            if (producerNode instanceof HdfsScanNode) {
              filteredNdv = ((HdfsScanNode) producerNode)
                                .filterExprWithStatsConjunct(entry.getKey(), exprs);
            }
          }

          // Pick the minimum between NDV multiple of the expressions vs max
          // cardinality of tuple.
          long ndvBasedNumGroup = 1;
          if (!exprs.isEmpty()) {
            ndvBasedNumGroup = estimateNumGroups(exprs, aggInputCardinality);
          }
          if (ndvBasedNumGroup > -1) {
            Preconditions.checkState(
                filteredNdv > 0, "filteredNdv must be greater than 0.");
            ndvBasedNumGroup =
                MathUtil.multiplyCardinalities(ndvBasedNumGroup, filteredNdv);
          }
          long numGroupFromCommonTuple =
              MathUtil.smallestValidCardinality(producerCardinality, ndvBasedNumGroup);

          if (numGroupFromCommonTuple < 0) {
            // Can not reason about tuple cardinality.
            // Move all exprs to exprsWithUniqueTupleId.
            exprsWithUniqueTupleId.addAll(entry.getValue());
          } else {
            tupleBasedNumGroups.add(numGroupFromCommonTuple);
          }
        }
      }
    }

    long numGroups = 1;
    if (!exprsWithUniqueTupleId.isEmpty()) {
      numGroups = estimateNumGroups(exprsWithUniqueTupleId, aggInputCardinality);
    }
    if (numGroups < 0) return numGroups;
    for (Long entry : tupleBasedNumGroups) {
      numGroups = MathUtil.multiplyCardinalities(numGroups, entry);
    }
    return MathUtil.smallestValidCardinality(numGroups, aggInputCardinality);
  }

  /**
   * Estimate the number of groups that will be present for the provided grouping
   * expressions and input cardinality.
   * Returns -1 if a reasonable cardinality estimate cannot be produced.
   */
  private static long estimateNumGroups(
      List<Expr> groupingExprs, long aggInputCardinality) {
    Preconditions.checkArgument(
        !groupingExprs.isEmpty(), "groupingExprs must not be empty");
    long numGroups = Expr.getNumDistinctValues(groupingExprs);
    // Return the least & valid cardinality between numGroups vs aggInputCardinality.
    // Grouping aggregation cannot increase output cardinality.
    // Also, worst-case output cardinality is better than an unknown output cardinality.
    // Note that this will still be -1 (unknown) if both numGroups
    // and aggInputCardinality is unknown.
    return MathUtil.smallestValidCardinality(numGroups, aggInputCardinality);
  }

  /**
   * Compute the input cardinality to the distributed aggregation. This searches down
   * for the FIRST phase aggregation node, and returns the minimum between input
   * cardinality of that FIRST phase aggregation vs this node's input cardinality.
   * However, if this is a TRANSPOSE phase of grouping set aggregation (such as ROLLUP),
   * it immediately returns this node's input cardinality since output cardinality of
   * the whole aggregation chain can be higher than input cardinality of the FIRST phase
   * aggregation. Return -1 if unknown.
   */
  private long getFirstAggInputCardinality() {
    long inputCardinality = getChild(0).getCardinality();
    if (multiAggInfo_.getIsGroupingSet() && aggPhase_ == AggPhase.TRANSPOSE) {
      return inputCardinality;
    }
    AggregationNode firstAgg = this;
    while (firstAgg.getAggPhase() != AggPhase.FIRST) {
      firstAgg = getPrevAggNode(firstAgg);
    }
    return Math.min(inputCardinality, firstAgg.getChild(0).getCardinality());
  }

  private AggregationNode getPrevAggNode(AggregationNode aggNode) {
    Preconditions.checkArgument(aggNode.getAggPhase() != AggPhase.FIRST);
    PlanNode child = aggNode.getChild(0);
    while (child instanceof ExchangeNode || child instanceof TupleCacheNode) {
      child = child.getChild(0);
    }
    Preconditions.checkState(child instanceof AggregationNode);
    return (AggregationNode) child;
  }

  /**
   * Return the associated PreAggNode child if this is a MERGE phase Agg node.
   * Otherwise, return null.
   */
  private @Nullable AggregationNode getPreAggNodeChild() {
    AggregationNode prevAgg = null;
    if (aggPhase_.isMerge()) {
      prevAgg = getPrevAggNode(this);
      Preconditions.checkState(aggInfos_.size() == prevAgg.aggInfos_.size());
      Preconditions.checkState(aggInfos_.size() == prevAgg.aggClassNumGroups_.size());
      Preconditions.checkState(
          aggInfos_.size() == prevAgg.aggClassOutputCardinality_.size());
    }
    return prevAgg;
  }

  /**
   * Returns a list of exprs suitable for hash partitioning the output of this node
   * before the merge aggregation step. Only valid to call if this node is not a merge
   * or transposing aggregation. The returned exprs are bound by the intermediate tuples.
   * Takes the SHUFFLE_DISTINCT_EXPRS query option into account.
   *
   * For single-class aggregations the returned exprs are typically the grouping exprs.
   * With SHUFFLE_DISTINCT_EXPRS=true the distinct exprs are also included if this is the
   * non-merge first-phase aggregation of a distinct aggregation.
   *
   * For multi-class aggregations the returned exprs are a list of CASE exprs which you
   * can think of as a "union" of the merge partition exprs of each class. Each CASE
   * switches on the valid tuple id of an input row to determine the aggregation class,
   * and selects the corresponding partition expr.
   * The main challenges with crafting these exprs are:
   * 1. Different aggregation classes can have a different number of distinct exprs
   *    Solution: The returned list is maximally wide to accommodate the widest
   *    aggregation class. For classes that have fewer than the max distinct exprs we add
   *    constant dummy exprs in the corresponding branch of the CASE.
   * 2. A CASE expr must return a single output type, but different aggregation classes
   *    may have incompatible distinct exprs, so selecting the distinct exprs directly
   *    in the CASE branches would not always work (unless we cast everything to STRING,
   *    which we try to avoid). Instead, we call MURMUR_HASH() on the exprs to produce
   *    a hash value. That way, all branches of a CASE return the same type.
   *    Considering that the backend will do another round of hashing, there's an
   *    unnecessary double hashing here that we deemed acceptable for now and has
   *    potential for cleanup (maybe the FE should always add a MURMUR_HASH()).
   * The handling of SHUFFLE_DISTINCT_EXPRS is analogous to the single-class case.
   *
   * Example:
   * SELECT COUNT(DISTINCT a,b), COUNT(DISTINCT c) FROM t GROUP BY d
   * Suppose the two aggregation classes have intermediate tuple ids 0 and 1.
   *
   * Challenges explained on this example:
   * 1. First class has distinct exprs a,b and second class has c. We need to accommodate
   *    the widest class (a,b) and also hash on the grouping expr (d), so there will be
   *    three cases.
   * 2. The types of a and c might be incompatible
   *
   * The first-phase partition exprs are a list of the following 3 exprs:
   * CASE valid_tid()
   *   WHEN 0 THEN murmur_hash(d) <-- d SlotRef into tuple 0
   *   WHEN 1 THEN murmur_hash(d) <-- d SlotRef into tuple 1
   * END,
   * CASE valid_tid()
   *   WHEN 0 THEN murmur_hash(a)
   *   WHEN 1 THEN murmur_hash(c)
   * END,
   * CASE valid_tid()
   *   WHEN 0 THEN murmur_hash(b)
   *   WHEN 1 THEN 0 <-- dummy constant integer
   * END
   */
  public List<Expr> getMergePartitionExprs(Analyzer analyzer) {
    Preconditions.checkState(!tupleIds_.isEmpty());
    Preconditions.checkState(!aggPhase_.isMerge() && !aggPhase_.isTranspose());

    boolean shuffleDistinctExprs = analyzer.getQueryOptions().shuffle_distinct_exprs;
    if (aggInfos_.size() == 1) {
      AggregateInfo aggInfo = aggInfos_.get(0);
      List<Expr> groupingExprs = null;
      if (aggPhase_.isFirstPhase() && hasGrouping() && !shuffleDistinctExprs) {
        groupingExprs = multiAggInfo_.getSubstGroupingExprs();
      } else {
        groupingExprs = aggInfo.getPartitionExprs();
        if (groupingExprs == null) groupingExprs = aggInfo.getGroupingExprs();
      }
      return Expr.substituteList(
          groupingExprs, aggInfo.getIntermediateSmap(), analyzer, false);
    }

    int maxNumExprs = 0;
    for (AggregateInfo aggInfo : aggInfos_) {
      if (aggInfo.getGroupingExprs() == null) continue;
      maxNumExprs = Math.max(maxNumExprs, aggInfo.getGroupingExprs().size());
    }
    if (maxNumExprs == 0) return Collections.emptyList();

    List<Expr> result = new ArrayList<>();
    for (int i = 0; i < maxNumExprs; ++i) {
      List<CaseWhenClause> caseWhenClauses = new ArrayList<>();
      for (AggregateInfo aggInfo : aggInfos_) {
        TupleId tid;
        if (aggInfo.isDistinctAgg()) {
          tid = aggInfo.getOutputTupleId();
        } else {
          tid = aggInfo.getIntermediateTupleId();
        }
        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        if (aggPhase_.isFirstPhase() && hasGrouping() && !shuffleDistinctExprs) {
          groupingExprs = multiAggInfo_.getSubstGroupingExprs();
        }
        Expr whenExpr = NumericLiteral.create(tid.asInt());
        Expr thenExpr;
        if (groupingExprs == null || i >= groupingExprs.size()) {
          thenExpr = NumericLiteral.create(0);
        } else {
          thenExpr = new FunctionCallExpr(
              "murmur_hash", Lists.newArrayList(groupingExprs.get(i).clone()));
          thenExpr.analyzeNoThrow(analyzer);
          thenExpr = thenExpr.substitute(aggInfo.getIntermediateSmap(), analyzer, true);
        }
        caseWhenClauses.add(new CaseWhenClause(whenExpr, thenExpr));
      }
      CaseExpr caseExpr =
          new CaseExpr(new ValidTupleIdExpr(tupleIds_), caseWhenClauses, null);
      caseExpr.analyzeNoThrow(analyzer);
      result.add(caseExpr);
    }
    return result;
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    Preconditions.checkState(false, "Unexpected use of old toThrift() signature.");
  }

  @Override
  protected void toThrift(TPlanNode msg, ThriftSerializationCtx serialCtx) {
    msg.agg_node = new TAggregationNode();
    msg.node_type = TPlanNodeType.AGGREGATION_NODE;
    boolean replicateInput = aggPhase_ == AggPhase.FIRST && aggInfos_.size() > 1;
    msg.agg_node.setReplicate_input(replicateInput);
    // Normalize input cardinality estimate for caching in case stats change.
    // Cache key of scan ensures we detect changes to actual input data.
    msg.agg_node.setEstimated_input_cardinality(serialCtx.isTupleCache() ?
        1 : getChild(0).getCardinality());
    msg.agg_node.setFast_limit_check(canCompleteEarly());
    for (int i = 0; i < aggInfos_.size(); ++i) {
      AggregateInfo aggInfo = aggInfos_.get(i);
      List<TExpr> aggregateFunctions = new ArrayList<>();
      for (FunctionCallExpr e : aggInfo.getMaterializedAggregateExprs()) {
        aggregateFunctions.add(e.treeToThrift(serialCtx));
      }
      // At the point when TupleCachePlanner runs, the resource profile has not been
      // calculated yet. They should not be in the cache key anyway, so mask them out.
      TBackendResourceProfile resourceProfile = serialCtx.isTupleCache()
          ? ResourceProfile.noReservation(0).toThrift()
          : resourceProfiles_.get(i).toThrift();
      // Ensure both tuple IDs are registered. Only one is added to tupleIds_.
      serialCtx.registerTuple(aggInfo.getIntermediateTupleId());
      serialCtx.registerTuple(aggInfo.getOutputTupleId());
      TAggregator taggregator = new TAggregator(aggregateFunctions,
          serialCtx.translateTupleId(aggInfo.getIntermediateTupleId()).asInt(),
          serialCtx.translateTupleId(aggInfo.getOutputTupleId()).asInt(),
          needsFinalize_, useStreamingPreagg_, resourceProfile);
      List<Expr> groupingExprs = aggInfo.getGroupingExprs();
      if (!groupingExprs.isEmpty()) {
        taggregator.setGrouping_exprs(Expr.treesToThrift(groupingExprs, serialCtx));
      }
      msg.agg_node.addToAggregators(taggregator);
    }
  }

  @Override
  protected String getDisplayLabelDetail() {
    if (useStreamingPreagg_) return "STREAMING";
    if (needsFinalize_) return "FINALIZE";
    return null;
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    String nameDetail = getDisplayLabelDetail();
    output.append(String.format("%s%s", prefix, getDisplayLabel()));
    if (nameDetail != null) output.append(" [" + nameDetail + "]");
    output.append("\n");

    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      if (aggInfos_.size() == 1) {
        output.append(
            getAggInfoExplainString(detailPrefix, aggInfos_.get(0), detailLevel));
      } else {
        for (int i = 0; i < aggInfos_.size(); ++i) {
          AggregateInfo aggInfo = aggInfos_.get(i);
          output.append(String.format("%sClass %d\n", detailPrefix, i));
          output.append(
              getAggInfoExplainString(detailPrefix + "  ", aggInfo, detailLevel));
        }
      }
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix)
            .append("having: ")
            .append(Expr.getExplainString(conjuncts_, detailLevel))
            .append("\n");
      }
    }
    return output.toString();
  }

  private StringBuilder getAggInfoExplainString(
      String prefix, AggregateInfo aggInfo, TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    List<FunctionCallExpr> aggExprs = aggInfo.getMaterializedAggregateExprs();
    List<Expr> groupingExprs = aggInfo.getGroupingExprs();
    if (!aggExprs.isEmpty()) {
      output.append(prefix)
          .append("output: ")
          .append(Expr.getExplainString(aggExprs, detailLevel))
          .append("\n");
    }
    if (!groupingExprs.isEmpty()) {
      output.append(prefix)
          .append("group by: ")
          .append(Expr.getExplainString(groupingExprs, detailLevel))
          .append("\n");
    }
    return output;
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    processingCost_ = ProcessingCost.zero();
    AggregationNode preaggNode = getPreAggNodeChild();
    int aggIdx = 0;
    for (AggregateInfo aggInfo : aggInfos_) {
      // The cost is a function of both the input size and the output size (NDV). For
      // estimating cost for any pre-aggregation we need to account for the likelihood of
      // duplicate keys across fragments.  Calculate an overall "intermediate" output
      // cardinality that attempts to account for the dups. Cap it at the input
      // cardinality because an aggregation cannot increase the cardinality.
      long inputCardinality;
      long intermediateOutputCardinality;
      if (preaggNode != null) {
        inputCardinality = preaggNode.aggClassOutputCardinality_.get(aggIdx);
        intermediateOutputCardinality = aggClassNumGroups_.get(aggIdx);
      } else {
        inputCardinality = getChild(0).getCardinality();
        intermediateOutputCardinality = aggClassOutputCardinality_.get(aggIdx);
      }
      // Normalize to 1 if unknown. 0 is OK.
      if (inputCardinality < 0) inputCardinality = 1;
      if (intermediateOutputCardinality < 0) intermediateOutputCardinality = 1;
      ProcessingCost aggCost = aggInfo.computeProcessingCost(
          getDisplayLabel(), inputCardinality, intermediateOutputCardinality);
      processingCost_ = ProcessingCost.sumCost(processingCost_, aggCost);
      aggIdx++;
    }
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    computeResourceProfileIfSpill(queryOptions, Long.MAX_VALUE);
  }

  @Override
  public void computeResourceProfileIfSpill(
      TQueryOptions queryOptions, long maxMemoryEstimatePerInstance) {
    boolean biasToSpill = shouldComputeResourcesWithSpill(queryOptions);
    if (!biasToSpill) {
      Preconditions.checkArgument(maxMemoryEstimatePerInstance == Long.MAX_VALUE);
    }
    resourceProfiles_ = Lists.newArrayListWithCapacity(aggInfos_.size());
    resourceProfiles_.clear();
    // IMPALA-2945: Behavior change if estimatePreaggDuplicate is true.
    boolean estimatePreaggDuplicate = queryOptions.isEstimate_duplicate_in_preagg();
    AggregationNode prevAggNode = getPreAggNodeChild();
    int aggIdx = 0;
    for (AggregateInfo aggInfo : aggInfos_) {
      long inputCardinality = aggInputCardinality_;
      if (prevAggNode != null) {
        long aggClassOutputCardinality = estimatePreaggDuplicate ?
            prevAggNode.aggClassOutputCardinality_.get(aggIdx) :
            prevAggNode.aggClassNumGroups_.get(aggIdx);
        inputCardinality = MathUtil.smallestValidCardinality(
            inputCardinality, aggClassOutputCardinality);
      }
      resourceProfiles_.add(computeAggClassResourceProfile(
          queryOptions, aggInfo, inputCardinality, maxMemoryEstimatePerInstance));
      aggIdx++;
    }
    ResourceProfile totalResource = ResourceProfile.noReservation(0);
    if (aggInfos_.size() == 1) {
      totalResource = resourceProfiles_.get(0);
    } else {
      for (ResourceProfile aggProfile : resourceProfiles_) {
        totalResource = totalResource.sum(aggProfile);
      }
    }

    if (aggInfos_.size() > 1 && biasToSpill) {
      // Total mem estimate might still exceed maxMemoryEstimatePerInstance.
      // Do another capping over the sum ResourceProfile, but maintain total mem estimate
      // higher than total min mem reservation.
      ResourceProfileBuilder builder =
          new ResourceProfileBuilder()
              .setMemEstimateBytes(totalResource.getMemEstimateBytes())
              .setMinMemReservationBytes(totalResource.getMinMemReservationBytes())
              .setThreadReservation(totalResource.getThreadReservation())
              .setMemEstimateScale(
                  queryOptions.getMem_estimate_scale_for_spilling_operator(),
                  maxMemoryEstimatePerInstance);
      if (totalResource.getMaxMemReservationBytes() > 0) {
        builder.setMaxMemReservationBytes(totalResource.getMaxMemReservationBytes());
      }
      nodeResourceProfile_ = builder.build();
    } else {
      nodeResourceProfile_ = totalResource;
    }
  }

  private long estimatePerInstanceDataBytes(
      long perInstanceCardinality, long inputCardinality) {
    Preconditions.checkArgument(perInstanceCardinality > -1);
    // Per-instance cardinality cannot be greater than the total input cardinality
    // going into this aggregation class.
    if (inputCardinality != -1) {
      // Calculate the input cardinality distributed across fragment instances.
      long numInstances = fragment_.getNumInstances();
      long perInstanceInputCardinality;
      if (numInstances > 1) {
        if (useStreamingPreagg_) {
          // A skew factor was added to account for data skew among
          // multiple fragment instances.
          // This number was derived using empirical analysis of real-world
          // and benchmark (tpch, tpcds) queries.
          perInstanceInputCardinality = (long) Math.ceil(
              ((double) inputCardinality / numInstances) * DEFAULT_SKEW_FACTOR);
        } else {
          // The data is distributed through hash, it will be more balanced.
          perInstanceInputCardinality =
              (long) Math.ceil((double) inputCardinality / numInstances);
        }
      } else {
        // When numInstances is 1 or unknown(-1), perInstanceInputCardinality is the
        // same as inputCardinality.
        perInstanceInputCardinality = inputCardinality;
      }

      if (useStreamingPreagg_) {
        // A reduction factor of 2 (input rows divided by output rows) was
        // added to grow hash tables. If the reduction factor is lower than 2,
        // only part of the data will be inserted into the hash table.
        perInstanceCardinality =
            Math.min(perInstanceCardinality, perInstanceInputCardinality / 2);
      } else {
        perInstanceCardinality =
            Math.min(perInstanceCardinality, perInstanceInputCardinality);
      }
    }
    // The memory of the data stored in hash table and the memory of the
    // hash table‘s structure
    long perInstanceDataBytes = (long) Math.ceil(
        perInstanceCardinality * (avgRowSize_ + PlannerContext.SIZE_OF_BUCKET));
    return perInstanceDataBytes;
  }

  private ResourceProfile computeAggClassResourceProfile(TQueryOptions queryOptions,
      AggregateInfo aggInfo, long inputCardinality, long maxMemoryEstimatePerInstance) {
    Preconditions.checkNotNull(
        fragment_, "PlanNode must be placed into a fragment before calling this method.");
    long perInstanceCardinality =
        fragment_.getPerInstanceNdv(aggInfo.getGroupingExprs(), false);
    long perInstanceMemEstimate;
    long perInstanceDataBytes = -1;

    // Determine threshold for large aggregation node.
    long largeAggMemThreshold = Long.MAX_VALUE;
    if (useStreamingPreagg_ && queryOptions.getPreagg_bytes_limit() > 0) {
      largeAggMemThreshold = queryOptions.getPreagg_bytes_limit();
    } else if (queryOptions.getLarge_agg_mem_threshold() > 0) {
      largeAggMemThreshold = queryOptions.getLarge_agg_mem_threshold();
    }

    if (perInstanceCardinality == -1) {
      perInstanceMemEstimate = DEFAULT_PER_INSTANCE_MEM;
    } else {
      perInstanceDataBytes =
          estimatePerInstanceDataBytes(perInstanceCardinality, inputCardinality);
      if (perInstanceDataBytes > largeAggMemThreshold) {
        // Should try to schedule agg node with lower memory estimation than
        // current perInstanceDataBytes. This is fine since preagg node can passthrough
        // row under memory pressure, while final agg node can spill to disk.
        // Try to come up with lower memory requirement by using max(NDV) and
        // AGG_MEM_CORRELATION_FACTOR to estimate a lower perInstanceCardinality rather
        // than the default NDV multiplication method.
        long lowPerInstanceCardinality =
            fragment_.getPerInstanceNdv(aggInfo.getGroupingExprs(), true);
        Preconditions.checkState(lowPerInstanceCardinality > -1);
        long lowPerInstanceDataBytes = Math.max(largeAggMemThreshold,
            estimatePerInstanceDataBytes(lowPerInstanceCardinality, inputCardinality));
        Preconditions.checkState(lowPerInstanceDataBytes <= perInstanceDataBytes);

        // Given N as number of non-literal grouping expressions,
        //   memScale = (1.0 - AGG_MEM_CORRELATION_FACTOR) ^ max(0, N - 1)
        // Note that high value of AGG_MEM_CORRELATION_FACTOR value (close to 1.0) means
        // there is high correlation between grouping expressions / columns, while low
        // value (close to 0.0) means there is low correlation between them.
        // High correlation means aggregation node can be scheduled with lower memory
        // estimation (lower memScale).
        long nonLiteralExprCount = aggInfo.getGroupingExprs()
                                       .stream()
                                       .filter(e -> !(e instanceof LiteralExpr))
                                       .count();
        double corrFactor = queryOptions.getAgg_mem_correlation_factor();
        double memScale =
            Math.pow(1.0 - corrFactor, Math.max(0, nonLiteralExprCount - 1));
        long resolvedPerInstanceDataBytes = lowPerInstanceDataBytes
            + Math.round(memScale * (perInstanceDataBytes - lowPerInstanceDataBytes));
        if (LOG.isTraceEnabled() && perInstanceDataBytes > resolvedPerInstanceDataBytes) {
          LOG.trace("Node " + getDisplayLabel() + " reduce perInstanceDataBytes from "
              + perInstanceDataBytes + " to " + resolvedPerInstanceDataBytes);
        }
        perInstanceDataBytes = resolvedPerInstanceDataBytes;
      }
      if (aggInfo.getGroupingExprs().isEmpty()) {
        perInstanceMemEstimate = MIN_PLAIN_AGG_MEM;
      } else {
        perInstanceMemEstimate =
            Math.max(perInstanceDataBytes, QueryConstants.MIN_HASH_TBL_MEM);
      }
    }

    // Must be kept in sync with GroupingAggregator::MinReservation() in backend.
    long perInstanceMinMemReservation;
    long bufferSize = queryOptions.getDefault_spillable_buffer_size();
    long maxRowBufferSize =
        computeMaxSpillableBufferSize(bufferSize, queryOptions.getMax_row_size());
    if (aggInfo.getGroupingExprs().isEmpty()) {
      perInstanceMinMemReservation = 0;
    } else {
      // This is a grouping pre-aggregation or merge aggregation.
      final int PARTITION_FANOUT = 16;
      if (perInstanceDataBytes != -1) {
        long bytesPerPartition = perInstanceDataBytes / PARTITION_FANOUT;
        // Scale down the buffer size if we think there will be excess free space with the
        // default buffer size, e.g. with small dimension tables.
        bufferSize = Math.min(bufferSize, Math.max(
            queryOptions.getMin_spillable_buffer_size(),
            BitUtil.roundUpToPowerOf2(bytesPerPartition)));
        // Recompute the max row buffer size with the smaller buffer.
        maxRowBufferSize =
            computeMaxSpillableBufferSize(bufferSize, queryOptions.getMax_row_size());
      }
      if (useStreamingPreagg_) {
        // We can execute a streaming preagg without any buffers by passing through rows,
        // but that is a very low performance mode of execution if the aggregation reduces
        // its input significantly. Instead reserve memory for one buffer per partition
        // and at least 64kb for hash tables per partition. We must reserve at least one
        // full buffer for hash tables for the suballocator to subdivide. We don't need to
        // reserve memory for large rows since they can be passed through if needed.
        perInstanceMinMemReservation = bufferSize * PARTITION_FANOUT +
            Math.max(64 * 1024 * PARTITION_FANOUT, bufferSize);
      } else {
        long minBuffers = PARTITION_FANOUT + 1 + (aggInfo.needsSerialize() ? 1 : 0);
        // Two of the buffers need to be buffers large enough to hold the maximum-sized
        // row to serve as input and output buffers while repartitioning.
        perInstanceMinMemReservation = bufferSize * (minBuffers - 2) + maxRowBufferSize * 2;
      }
    }

    ResourceProfileBuilder builder = new ResourceProfileBuilder()
        .setMemEstimateBytes(perInstanceMemEstimate)
        .setMinMemReservationBytes(perInstanceMinMemReservation)
        .setSpillableBufferBytes(bufferSize)
        .setMaxRowBufferBytes(maxRowBufferSize);
    if (useStreamingPreagg_ && queryOptions.getPreagg_bytes_limit() > 0) {
      long maxReservationBytes =
          Math.max(perInstanceMinMemReservation, queryOptions.getPreagg_bytes_limit());
      builder.setMaxMemReservationBytes(maxReservationBytes);
      // Aggregations should generally not use significantly more than the max
      // reservation, since the bulk of the memory is reserved.
      builder.setMemEstimateBytes(
          Math.min(perInstanceMemEstimate, maxReservationBytes));
    }
    if (shouldComputeResourcesWithSpill(queryOptions)) {
      builder.setMemEstimateScale(
          queryOptions.getMem_estimate_scale_for_spilling_operator(),
          maxMemoryEstimatePerInstance);
    }
    return builder.build();
  }

  public void setIsNonCorrelatedScalarSubquery(boolean val) {
    isNonCorrelatedScalarSubquery_ = val;
  }

  public boolean isNonCorrelatedScalarSubquery() {
    return isNonCorrelatedScalarSubquery_;
  }

  // When both conditions below are true, aggregation can complete early
  //    a) aggregation node has no aggregate function
  //    b) aggregation node has no predicate
  // E.g. SELECT DISTINCT f1,f2,...fn FROM t LIMIT n
  public boolean canCompleteEarly() {
    return isSingleClassAgg() && hasLimit() && hasGrouping()
        && !multiAggInfo_.hasAggregateExprs() && getConjuncts().isEmpty();
  }

  @Override
  public boolean isTupleCachingImplemented() { return true; }
}
