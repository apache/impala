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

package org.apache.impala.analysis;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.MultiAggregateInfo.AggPhase;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.planner.ProcessingCost;
import org.apache.impala.util.ExprUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Encapsulates all the information needed to compute a list of aggregate functions with
 * compatible grouping including their distributed execution.
 *
 * Each SELECT block containing aggregates will have a single MultiAggregateInfo which
 * will contain one AggregateInfo per unique list of DISTINCT expressions. If there is
 * only a single DISTINCT grouping, a single AggregateInfo will be created which will
 * represent that grouping and any non-DISTINCT aggregates. If there is more than one
 * DISTINCT grouping, the non-DISTINCT aggregates will be grouped together in their own
 * AggregateInfo.
 *
 * Execution is modeled as a tree of AggregateInfo objects which express the local and
 * merging aggregate computations. The tree structure looks as follows:
 * - for non-distinct aggregation:
 *   - aggInfo: contains the original aggregation functions and grouping exprs
 *   - aggInfo.mergeAggInfo: contains the merging aggregation functions (grouping
 *     exprs are identical)
 * - for distinct aggregation (also see createDistinctAggInfo()):
 *   - aggInfo: contains the phase 1 aggregate functions and grouping exprs
 *   - aggInfo.secondPhaseDistinctAggInfo: contains the phase 2 aggregate functions and
 *     grouping exprs, and the merging aggregate functions for any non-distinct aggs
 *   - aggInfo.mergeAggInfo: contains the merging aggregate functions for the phase 1
 *     computation (grouping exprs are identical to aggInfo)
 *   - aggInfo.secondPhaseDistinctAggInfo.mergeAggInfo: contains the merging aggregate
 *     functions for the phase 2 computation (grouping exprs are identical to
 *     aggInfo.secondPhaseDistinctAggInfo)
 *
 * Merging aggregate computations are idempotent. In other words,
 * aggInfo.mergeAggInfo == aggInfo.mergeAggInfo.mergeAggInfo.
 */
public class AggregateInfo extends AggregateInfoBase {
  private final static Logger LOG = LoggerFactory.getLogger(AggregateInfo.class);

  // Coefficients for estimating CPU processing cost.  Derived from benchmarking.
  // Cost per input row for each additional agg function evaluated
  private static final double COST_COEFFICIENT_AGG_EXPR = 0.07;
  // Cost per input row with single grouping expr with single agg expr
  private static final double COST_COEFFICIENT_AGG_INPUT_SINGLE_GROUP = 0.2925;
  // Cost per output row produced with single grouping expr and single agg expr
  private static final double COST_COEFFICIENT_AGG_OUTPUT_SINGLE_GROUP = 2.6072;
  // Cost per input row with multiple grouping exprs and single agg expr
  private static final double COST_COEFFICIENT_AGG_INPUT_MULTI_GROUP = 1.3741;
  // Cost per output row produced with multiple grouping exprs and single agg expr
  private static final double COST_COEFFICIENT_AGG_OUTPUT_MULTI_GROUP = 4.5285;

  // created by createMergeAggInfo()
  private AggregateInfo mergeAggInfo_;

  // created by createDistinctAggInfo()
  private AggregateInfo secondPhaseDistinctAggInfo_;

  private final AggPhase aggPhase_;

  // Map from all grouping and aggregate exprs to a SlotRef referencing the corresp. slot
  // in the intermediate tuple. Identical to outputTupleSmap_ if no aggregateExpr has an
  // output type that is different from its intermediate type.
  protected ExprSubstitutionMap intermediateTupleSmap_ = new ExprSubstitutionMap();

  // Map from all grouping and aggregate exprs to a SlotRef referencing the corresp. slot
  // in the output tuple.
  protected ExprSubstitutionMap outputTupleSmap_ = new ExprSubstitutionMap();

  // if set, a subset of groupingExprs_; set and used during planning
  private List<Expr> partitionExprs_;

  // C'tor creates copies of groupingExprs and aggExprs.
  private AggregateInfo(
      List<Expr> groupingExprs, List<FunctionCallExpr> aggExprs, AggPhase aggPhase) {
    super(groupingExprs, aggExprs);
    aggPhase_ = aggPhase;
  }

  /**
   * C'tor for cloning.
   */
  private AggregateInfo(AggregateInfo other) {
    super(other);
    if (other.mergeAggInfo_ != null) {
      mergeAggInfo_ = other.mergeAggInfo_.clone();
    }
    if (other.secondPhaseDistinctAggInfo_ != null) {
      secondPhaseDistinctAggInfo_ = other.secondPhaseDistinctAggInfo_.clone();
    }
    aggPhase_ = other.aggPhase_;
    outputTupleSmap_ = other.outputTupleSmap_.clone();
    if (other.requiresIntermediateTuple()) {
      intermediateTupleSmap_ = other.intermediateTupleSmap_.clone();
    } else {
      Preconditions.checkState(other.intermediateTupleDesc_ == other.outputTupleDesc_);
      intermediateTupleSmap_ = outputTupleSmap_;
    }
    partitionExprs_ =
        (other.partitionExprs_ != null) ? Expr.cloneList(other.partitionExprs_) : null;
  }

  public List<Expr> getPartitionExprs() { return partitionExprs_; }
  public void setPartitionExprs(List<Expr> exprs) { partitionExprs_ = exprs; }

  /**
   * Creates complete AggregateInfo for groupingExprs and aggExprs, including
   * aggTupleDesc and aggTupleSMap. If parameter tupleDesc != null, sets aggTupleDesc to
   * that instead of creating a new descriptor (after verifying that the passed-in
   * descriptor is correct for the given aggregation).
   * Also creates mergeAggInfo and secondPhaseDistinctAggInfo, if needed.
   * If an aggTupleDesc is created, also registers eq predicates between the
   * grouping exprs and their respective slots with 'analyzer'.
   */
  static public AggregateInfo create(List<Expr> groupingExprs,
      List<FunctionCallExpr> aggExprs, TupleDescriptor tupleDesc, Analyzer analyzer)
      throws AnalysisException {
    Preconditions.checkState(
        (groupingExprs != null && !groupingExprs.isEmpty())
        || (aggExprs != null && !aggExprs.isEmpty()));
    AggregateInfo result = new AggregateInfo(groupingExprs, aggExprs, AggPhase.FIRST);

    // collect agg exprs with DISTINCT clause
    List<FunctionCallExpr> distinctAggExprs = new ArrayList<>();
    if (aggExprs != null) {
      for (FunctionCallExpr aggExpr: aggExprs) {
        if (aggExpr.isDistinct()) distinctAggExprs.add(aggExpr);
      }
    }

    if (distinctAggExprs.isEmpty()) {
      if (tupleDesc == null) {
        result.createTupleDescs(analyzer);
        result.createSmaps(analyzer);
      } else {
        // A tupleDesc should only be given for UNION DISTINCT.
        Preconditions.checkState(aggExprs == null);
        result.outputTupleDesc_ = tupleDesc;
        result.intermediateTupleDesc_ = tupleDesc;
      }
      result.createMergeAggInfo(analyzer);
    } else {
      // we don't allow you to pass in a descriptor for distinct aggregation
      // (we need two descriptors)
      Preconditions.checkState(tupleDesc == null);
      result.createDistinctAggInfo(groupingExprs, distinctAggExprs, analyzer);
    }
    if (LOG.isTraceEnabled()) LOG.trace("agg info:\n" + result.debugString());
    return result;
  }

  public static AggregateInfo create(List<Expr> groupingExprs,
      List<FunctionCallExpr> aggExprs, Analyzer analyzer) throws AnalysisException {
    return create(groupingExprs, aggExprs, null, analyzer);
  }

  /**
   * Create aggregate info for a single distinct grouping. All distinct aggregate
   * function in 'distinctAggExprs' must be applied to the same set of exprs.
   * This creates:
   * - aggTupleDesc
   * - a complete secondPhaseDistinctAggInfo
   * - mergeAggInfo
   *
   * Aggregation happens in two successive phases:
   * - the first phase aggregates by all grouping exprs plus all parameter exprs
   *   of DISTINCT aggregate functions
   * - the second phase is created in createSecondPhaseAggInfo()
   *
   * Example:
   *   SELECT a, COUNT(DISTINCT b, c), MIN(d), COUNT(*) FROM T GROUP BY a
   * - 1st phase grouping exprs: a, b, c
   * - 1st phase agg exprs: MIN(d), COUNT(*)
   * - 2nd phase grouping exprs: a
   * - 2nd phase agg exprs: COUNT(*), MIN(<MIN(d) from 1st phase>),
   *     SUM(<COUNT(*) from 1st phase>)
   */
  private void createDistinctAggInfo(List<Expr> origGroupingExprs,
      List<FunctionCallExpr> distinctAggExprs, Analyzer analyzer)
      throws AnalysisException {
    Preconditions.checkState(!distinctAggExprs.isEmpty());
    // make sure that all DISTINCT params are the same;
    // ignore top-level implicit casts in the comparison, we might have inserted
    // those during analysis
    List<Expr> expr0Children =
        AggregateFunction.getCanonicalDistinctAggChildren(distinctAggExprs.get(0));
    for (int i = 1; i < distinctAggExprs.size(); ++i) {
      List<Expr> exprIChildren =
          AggregateFunction.getCanonicalDistinctAggChildren(distinctAggExprs.get(i));
      Preconditions.checkState(Expr.equalLists(expr0Children, exprIChildren));
    }

    // add DISTINCT parameters to grouping exprs
    groupingExprs_.addAll(expr0Children);

    // remove DISTINCT aggregate functions from aggExprs
    aggregateExprs_.removeAll(distinctAggExprs);

    createTupleDescs(analyzer);
    createSmaps(analyzer);
    createMergeAggInfo(analyzer);
    createSecondPhaseAggInfo(origGroupingExprs, distinctAggExprs, analyzer);
  }

  public AggregateInfo getMergeAggInfo() { return mergeAggInfo_; }
  public AggregateInfo getSecondPhaseDistinctAggInfo() {
    return secondPhaseDistinctAggInfo_;
  }
  public boolean isMerge() { return aggPhase_.isMerge(); }
  public boolean isDistinctAgg() { return secondPhaseDistinctAggInfo_ != null; }
  public ExprSubstitutionMap getIntermediateSmap() { return intermediateTupleSmap_; }
  public ExprSubstitutionMap getOutputSmap() { return outputTupleSmap_; }

  public boolean hasAggregateExprs() {
    return !aggregateExprs_.isEmpty() ||
        (secondPhaseDistinctAggInfo_ != null &&
         !secondPhaseDistinctAggInfo_.getAggregateExprs().isEmpty());
  }

  /**
   * Return the tuple id produced in the final aggregation step.
   */
  public TupleId getResultTupleId() {
    if (isDistinctAgg()) return secondPhaseDistinctAggInfo_.getOutputTupleId();
    return getOutputTupleId();
  }

  public TupleDescriptor getResultTupleDesc() {
    if (isDistinctAgg()) return secondPhaseDistinctAggInfo_.getOutputTupleDesc();
    return getOutputTupleDesc();
  }

  public ExprSubstitutionMap getResultSmap() {
    if (isDistinctAgg()) return secondPhaseDistinctAggInfo_.getOutputSmap();
    return getOutputSmap();
  }

  public List<FunctionCallExpr> getMaterializedAggregateExprs() {
    List<FunctionCallExpr> result = new ArrayList<>();
    for (Integer i: materializedSlots_) {
      result.add(aggregateExprs_.get(i));
    }
    return result;
  }

  /**
   * Substitute all the expressions (grouping expr, aggregate expr) and update our
   * substitution map according to the given substitution map:
   * - smap typically maps from tuple t1 to tuple t2 (example: the smap of an
   *   inline view maps the virtual table ref t1 into a base table ref t2)
   * - our grouping and aggregate exprs need to be substituted with the given
   *   smap so that they also reference t2
   * - aggTupleSMap needs to be recomputed to map exprs based on t2
   *   onto our aggTupleDesc (ie, the left-hand side needs to be substituted with
   *   smap)
   * - mergeAggInfo: this is not affected, because
   *   * its grouping and aggregate exprs only reference aggTupleDesc_
   *   * its smap is identical to aggTupleSMap_
   * - 2ndPhaseDistinctAggInfo:
   *   * its grouping and aggregate exprs also only reference aggTupleDesc_
   *     and are therefore not affected
   *   * its smap needs to be recomputed to map exprs based on t2 to its own
   *     aggTupleDesc
   */
  public void substitute(ExprSubstitutionMap smap, Analyzer analyzer)
      throws InternalException {

    // Preserve the root type for NULL literals.
    groupingExprs_ = Expr.substituteList(groupingExprs_, smap, analyzer, true);
    if (LOG.isTraceEnabled()) {
      LOG.trace("AggInfo: grouping_exprs=" + Expr.debugString(groupingExprs_));
    }

    // The smap in this case should not substitute the aggs themselves, only
    // their subexpressions.
    List<Expr> substitutedAggs =
        Expr.substituteList(aggregateExprs_, smap, analyzer, false);
    aggregateExprs_.clear();
    for (Expr substitutedAgg: substitutedAggs) {
      aggregateExprs_.add((FunctionCallExpr) substitutedAgg);
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("AggInfo: agg_exprs=" + Expr.debugString(aggregateExprs_));
    }
    outputTupleSmap_.substituteLhs(smap, analyzer);
    intermediateTupleSmap_.substituteLhs(smap, analyzer);
    if (secondPhaseDistinctAggInfo_ != null) {
      secondPhaseDistinctAggInfo_.substitute(smap, analyzer);
    }
  }

  /**
   * Create the info for an aggregation node that merges its pre-aggregated inputs:
   * - pre-aggregation is computed by 'this'
   * - tuple desc and smap are the same as that of the input (we're materializing
   *   the same logical tuple)
   * - grouping exprs: slotrefs to the input's grouping slots
   * - aggregate exprs: aggregation of the input's aggregateExprs slots
   *
   * The returned AggregateInfo shares its descriptor and smap with the input info;
   * createAggTupleDesc() must not be called on it.
   */
  private void createMergeAggInfo(Analyzer analyzer) {
    Preconditions.checkState(mergeAggInfo_ == null);
    TupleDescriptor inputDesc = intermediateTupleDesc_;
    // construct grouping exprs
    List<Expr> groupingExprs = new ArrayList<>();
    for (int i = 0; i < getGroupingExprs().size(); ++i) {
      SlotRef slotRef = new SlotRef(inputDesc.getSlots().get(i));
      groupingExprs.add(slotRef);
    }

    // construct agg exprs
    List<FunctionCallExpr> aggExprs = new ArrayList<>();
    for (int i = 0; i < getAggregateExprs().size(); ++i) {
      FunctionCallExpr inputExpr = getAggregateExprs().get(i);
      Preconditions.checkState(inputExpr.isAggregateFunction());
      Expr aggExprParam =
          new SlotRef(inputDesc.getSlots().get(i + getGroupingExprs().size()));
      FunctionCallExpr aggExpr = FunctionCallExpr.createMergeAggCall(
          inputExpr, Lists.newArrayList(aggExprParam));
      aggExpr.analyzeNoThrow(analyzer);
      aggExprs.add(aggExpr);
    }

    AggPhase aggPhase =
        (aggPhase_ == AggPhase.FIRST) ? AggPhase.FIRST_MERGE : AggPhase.SECOND_MERGE;
    mergeAggInfo_ = new AggregateInfo(groupingExprs, aggExprs, aggPhase);
    mergeAggInfo_.intermediateTupleDesc_ = intermediateTupleDesc_;
    mergeAggInfo_.outputTupleDesc_ = outputTupleDesc_;
    mergeAggInfo_.intermediateTupleSmap_ = intermediateTupleSmap_;
    mergeAggInfo_.outputTupleSmap_ = outputTupleSmap_;
    mergeAggInfo_.materializedSlots_ = materializedSlots_;
  }

  /**
   * Creates an IF function call that returns NULL if any of the slots
   * at indexes [firstIdx, lastIdx] return NULL.
   * For example, the resulting IF function would like this for 3 slots:
   * IF(IsNull(slot1), NULL, IF(IsNull(slot2), NULL, slot3))
   * Returns null if firstIdx is greater than lastIdx.
   * Returns a SlotRef to the last slot if there is only one slot in range.
   */
  private Expr createCountDistinctAggExprParam(int firstIdx, int lastIdx,
      List<SlotDescriptor> slots) {
    if (firstIdx > lastIdx) return null;

    Expr elseExpr = new SlotRef(slots.get(lastIdx));
    if (firstIdx == lastIdx) return elseExpr;

    for (int i = lastIdx - 1; i >= firstIdx; --i) {
      List<Expr> ifArgs = new ArrayList<>();
      SlotRef slotRef = new SlotRef(slots.get(i));
      // Build expr: IF(IsNull(slotRef), NULL, elseExpr)
      Expr isNullPred = new IsNullPredicate(slotRef, false);
      ifArgs.add(isNullPred);
      ifArgs.add(new NullLiteral());
      ifArgs.add(elseExpr);
      elseExpr = new FunctionCallExpr("if", ifArgs);
    }
    return elseExpr;
  }

  /**
   * Create the info for an aggregation node that computes the second phase of
   * DISTINCT aggregate functions.
   * (Refer to createDistinctAggInfo() for an explanation of the phases.)
   * - 'this' is the phase 1 aggregation
   * - grouping exprs are those of the original query (param origGroupingExprs)
   * - aggregate exprs for the DISTINCT agg fns: these are aggregating the grouping
   *   slots that were added to the original grouping slots in phase 1;
   *   count is mapped to count(*) and sum is mapped to sum
   * - other aggregate exprs: same as the non-DISTINCT merge case
   *   (count is mapped to sum, everything else stays the same)
   *
   * This call also creates the tuple descriptor and smap for the returned AggregateInfo.
   */
  private void createSecondPhaseAggInfo(List<Expr> origGroupingExprs,
      List<FunctionCallExpr> distinctAggExprs, Analyzer analyzer)
      throws AnalysisException {
    Preconditions.checkState(secondPhaseDistinctAggInfo_ == null);
    Preconditions.checkState(!distinctAggExprs.isEmpty());
    // The output of the 1st phase agg is the 1st phase intermediate.
    TupleDescriptor inputDesc = intermediateTupleDesc_;

    // construct agg exprs for original DISTINCT aggregate functions
    // (these aren't part of aggExprs_)
    List<FunctionCallExpr> secondPhaseAggExprs = new ArrayList<>();
    for (FunctionCallExpr inputExpr: distinctAggExprs) {
      Preconditions.checkState(inputExpr.isAggregateFunction());
      FunctionCallExpr aggExpr = null;
      if (inputExpr.getFnName().getFunction().equals("count")) {
        // COUNT(DISTINCT ...) ->
        // COUNT(IF(IsNull(<agg slot 1>), NULL, IF(IsNull(<agg slot 2>), NULL, ...)))
        // We need the nested IF to make sure that we do not count
        // column-value combinations if any of the distinct columns are NULL.
        // This behavior is consistent with MySQL.
        Expr ifExpr = createCountDistinctAggExprParam(origGroupingExprs.size(),
            origGroupingExprs.size() + inputExpr.getChildren().size() - 1,
            inputDesc.getSlots());
        Preconditions.checkNotNull(ifExpr);
        ifExpr.analyzeNoThrow(analyzer);
        aggExpr = new FunctionCallExpr("count", Lists.newArrayList(ifExpr));
      } else if (inputExpr.getFnName().getFunction().equals("group_concat")) {
        // Syntax: GROUP_CONCAT([DISTINCT] expression [, separator])
        List<Expr> exprList = new ArrayList<>();
        // Add "expression" parameter. Need to get it from the inputDesc's slots so the
        // tuple reference is correct.
        exprList.add(new SlotRef(inputDesc.getSlots().get(origGroupingExprs.size())));
        // Check if user provided a custom separator
        if (inputExpr.getChildren().size() == 2) exprList.add(inputExpr.getChild(1));
        aggExpr = new FunctionCallExpr(inputExpr.getFnName(), exprList);
      } else {
        // SUM(DISTINCT <expr>) -> SUM(<last grouping slot>);
        // (MIN(DISTINCT ...) and MAX(DISTINCT ...) have their DISTINCT turned
        // off during analysis, and AVG() is changed to SUM()/COUNT())
        Expr aggExprParam =
            new SlotRef(inputDesc.getSlots().get(origGroupingExprs.size()));
        aggExpr = new FunctionCallExpr(inputExpr.getFnName(),
            Lists.newArrayList(aggExprParam));
      }
      secondPhaseAggExprs.add(aggExpr);
    }

    // map all the remaining agg fns
    for (int i = 0; i < aggregateExprs_.size(); ++i) {
      FunctionCallExpr inputExpr = aggregateExprs_.get(i);
      Preconditions.checkState(inputExpr.isAggregateFunction());
      // we're aggregating an intermediate slot of the 1st agg phase
      Expr aggExprParam =
          new SlotRef(inputDesc.getSlots().get(i + getGroupingExprs().size()));
      FunctionCallExpr aggExpr = FunctionCallExpr.createMergeAggCall(
          inputExpr, Lists.newArrayList(aggExprParam));
      secondPhaseAggExprs.add(aggExpr);
    }
    Preconditions.checkState(
        secondPhaseAggExprs.size() == aggregateExprs_.size() + distinctAggExprs.size());

    for (FunctionCallExpr aggExpr: secondPhaseAggExprs) {
      aggExpr.analyzeNoThrow(analyzer);
      Preconditions.checkState(aggExpr.isAggregateFunction());
    }

    List<Expr> substGroupingExprs =
        Expr.substituteList(origGroupingExprs, intermediateTupleSmap_, analyzer, false);
    secondPhaseDistinctAggInfo_ =
        new AggregateInfo(substGroupingExprs, secondPhaseAggExprs, AggPhase.SECOND);
    secondPhaseDistinctAggInfo_.createTupleDescs(analyzer);
    secondPhaseDistinctAggInfo_.createSecondPhaseAggSMap(this, distinctAggExprs);
    secondPhaseDistinctAggInfo_.createMergeAggInfo(analyzer);
  }

  /**
   * Populates the intermediate and output smaps. The output smap maps the original
   * grouping and aggregate exprs onto the final output of the second phase distinct
   * aggregation.
   */
  private void createSecondPhaseAggSMap(
      AggregateInfo inputAggInfo, List<FunctionCallExpr> distinctAggExprs) {
    outputTupleSmap_.clear();
    int slotIdx = 0;
    List<SlotDescriptor> slotDescs = outputTupleDesc_.getSlots();

    int numDistinctParams = distinctAggExprs.get(0).getChildren().size();
    // If we are counting distinct params of group_concat, we cannot include the custom
    // separator since it is not a distinct param.
    if (distinctAggExprs.get(0).getFnName().getFunction().equalsIgnoreCase(
        "group_concat")
        && numDistinctParams == 2) {
      --numDistinctParams;
    }
    int numOrigGroupingExprs =
        inputAggInfo.getGroupingExprs().size() - numDistinctParams;
    Preconditions.checkState(slotDescs.size() ==
        numOrigGroupingExprs + distinctAggExprs.size() +
        inputAggInfo.getAggregateExprs().size());

    // original grouping exprs -> first m slots
    for (int i = 0; i < numOrigGroupingExprs; ++i, ++slotIdx) {
      Expr groupingExpr = inputAggInfo.getGroupingExprs().get(i);
      outputTupleSmap_.put(
          groupingExpr.clone(), new SlotRef(slotDescs.get(slotIdx)));
    }

    // distinct agg exprs -> next n slots
    for (int i = 0; i < distinctAggExprs.size(); ++i, ++slotIdx) {
      Expr aggExpr = distinctAggExprs.get(i);
      outputTupleSmap_.put(
          aggExpr.clone(), (new SlotRef(slotDescs.get(slotIdx))));
    }

    // remaining agg exprs -> remaining slots
    for (int i = 0; i < inputAggInfo.getAggregateExprs().size(); ++i, ++slotIdx) {
      Expr aggExpr = inputAggInfo.getAggregateExprs().get(i);
      outputTupleSmap_.put(aggExpr.clone(), new SlotRef(slotDescs.get(slotIdx)));
    }

    // Populate intermediate smap even if this step does not require an
    // intermediate tuple. The intermediate smap is always different from the
    // output smap because the latter maps from original input exprs, whereas
    // the former maps from the input exprs of this second phase aggregation.
    List<Expr> exprs =
        Lists.newArrayListWithCapacity(groupingExprs_.size() + aggregateExprs_.size());
    exprs.addAll(groupingExprs_);
    exprs.addAll(aggregateExprs_);
    for (int i = 0; i < exprs.size(); ++i) {
      intermediateTupleSmap_.put(
          exprs.get(i).clone(), new SlotRef(intermediateTupleDesc_.getSlots().get(i)));
    }
  }

  /**
   * Populates the output and intermediate smaps based on the output and intermediate
   * tuples that are assumed to be set. If an intermediate tuple is required, also
   * populates the output-to-intermediate smap and registers auxiliary equivalence
   * predicates between the grouping slots of the two tuples.
   */
  public void createSmaps(Analyzer analyzer) {
    Preconditions.checkNotNull(outputTupleDesc_);
    Preconditions.checkNotNull(intermediateTupleDesc_);

    List<Expr> exprs = Lists.newArrayListWithCapacity(
        groupingExprs_.size() + aggregateExprs_.size());
    exprs.addAll(groupingExprs_);
    exprs.addAll(aggregateExprs_);
    for (int i = 0; i < exprs.size(); ++i) {
      outputTupleSmap_.put(exprs.get(i).clone(),
          new SlotRef(outputTupleDesc_.getSlots().get(i)));
      if (!requiresIntermediateTuple()) continue;
      intermediateTupleSmap_.put(exprs.get(i).clone(),
          new SlotRef(intermediateTupleDesc_.getSlots().get(i)));
      if (i < groupingExprs_.size()) {
        analyzer.createAuxEqPredicate(
            new SlotRef(outputTupleDesc_.getSlots().get(i)),
            new SlotRef(intermediateTupleDesc_.getSlots().get(i)));
      }
    }
    if (!requiresIntermediateTuple()) intermediateTupleSmap_ = outputTupleSmap_;

    if (LOG.isTraceEnabled()) {
      LOG.trace("output smap=" + outputTupleSmap_.debugString());
      LOG.trace("intermediate smap=" + intermediateTupleSmap_.debugString());
    }
  }

  /**
   * Mark slots required for this aggregation as materialized:
   * - all grouping output slots as well as grouping exprs
   * - for non-distinct aggregation: the aggregate exprs of materialized aggregate slots;
   *   this assumes that the output slots corresponding to aggregate exprs have already
   *   been marked by the consumer of this select block
   * - for distinct aggregation, we mark all aggregate output slots in order to keep
   *   things simple
   * Also computes materializedAggregateExprs.
   * This call must be idempotent because it may be called more than once for Union stmt.
   */
  @Override
  public void materializeRequiredSlots(Analyzer analyzer, ExprSubstitutionMap smap) {
    for (int i = 0; i < groupingExprs_.size(); ++i) {
      outputTupleDesc_.getSlots().get(i).setIsMaterialized(true);
      intermediateTupleDesc_.getSlots().get(i).setIsMaterialized(true);
    }

    // collect input exprs: grouping exprs plus aggregate exprs that need to be
    // materialized
    materializedSlots_.clear();
    List<Expr> exprs = new ArrayList<>();
    exprs.addAll(groupingExprs_);
    for (int i = 0; i < aggregateExprs_.size(); ++i) {
      SlotDescriptor slotDesc =
          outputTupleDesc_.getSlots().get(groupingExprs_.size() + i);
      SlotDescriptor intermediateSlotDesc =
          intermediateTupleDesc_.getSlots().get(groupingExprs_.size() + i);
      if (isDistinctAgg()) {
        slotDesc.setIsMaterialized(true);
        intermediateSlotDesc.setIsMaterialized(true);
      }
      if (!slotDesc.isMaterialized()) continue;
      intermediateSlotDesc.setIsMaterialized(true);
      exprs.add(aggregateExprs_.get(i));
      materializedSlots_.add(i);
    }
    List<Expr> resolvedExprs = Expr.substituteList(exprs, smap, analyzer, false);
    analyzer.materializeSlots(resolvedExprs);

    if (isDistinctAgg()) {
      secondPhaseDistinctAggInfo_.materializeRequiredSlots(analyzer, null);
    }
  }

  /**
   * Checks if all materialized aggregate expressions have distinct semantics.
   * It returns true if either of the following is true:
   * (1) all materialized aggregate expressions have distinct semantics
   *     (e.g. MIN, MAX, NDV). In other words, this optimization will work
   *     for COUNT(DISTINCT c) but not COUNT(c).
   * (2) there are no aggregate expressions but only grouping expressions.
   */
  public boolean hasAllDistinctAgg() {
    if (hasAggregateExprs()) {
      for (FunctionCallExpr aggExpr : getMaterializedAggregateExprs()) {
        if (!aggExpr.isDistinct() && !aggExpr.ignoresDistinct()) return false;
      }
    } else {
      Preconditions.checkState(!groupingExprs_.isEmpty());
    }
    return true;
  }

  /**
   * Returns true if there is a single count(*) materialized aggregate expression.
   */
  public boolean hasCountStarOnly() {
    if (getMaterializedAggregateExprs().size() != 1) return false;
    if (isDistinctAgg()) return false;
    FunctionCallExpr origExpr = getMaterializedAggregateExprs().get(0);
    if (!origExpr.getFnName().getFunction().equalsIgnoreCase("count")) return false;
    return origExpr.getParams().isStar();
  }

  /**
   * Validates the internal state of this agg info: Checks that the number of
   * materialized slots of the output tuple corresponds to the number of materialized
   * aggregate functions plus the number of grouping exprs. Also checks that the return
   * types of the aggregate and grouping exprs correspond to the slots in the output
   * tuple and that the input types stored in the merge aggregation are consistent
   * with the input exprs.
   */
  public void checkConsistency() {
    List<SlotDescriptor> slots = outputTupleDesc_.getSlots();

    // Check materialized slots.
    int numMaterializedSlots = 0;
    for (SlotDescriptor slotDesc: slots) {
      if (slotDesc.isMaterialized()) ++numMaterializedSlots;
    }
    Preconditions.checkState(numMaterializedSlots ==
        materializedSlots_.size() + groupingExprs_.size());

    // Check that grouping expr return types match the slot descriptors.
    int slotIdx = 0;
    for (int i = 0; i < groupingExprs_.size(); ++i) {
      Expr groupingExpr = groupingExprs_.get(i);
      Type slotType = slots.get(slotIdx).getType();
      Preconditions.checkState(groupingExpr.getType().equals(slotType),
          String.format("Grouping expr %s returns type %s but its output tuple " +
              "slot has type %s", groupingExpr.toSql(),
              groupingExpr.getType().toString(), slotType.toString()));
      ++slotIdx;
    }
    // Check that aggregate expr return types match the slot descriptors.
    for (int i = 0; i < aggregateExprs_.size(); ++i) {
      Expr aggExpr = aggregateExprs_.get(i);
      Type slotType = slots.get(slotIdx).getType();
      Preconditions.checkState(aggExpr.getType().equals(slotType),
          String.format("Agg expr %s returns type %s but its output tuple " +
              "slot has type %s", aggExpr.toSql(), aggExpr.getType().toString(),
              slotType.toString()));
      ++slotIdx;
    }
    if (mergeAggInfo_ != null) {
      // Check that the argument types in mergeAggInfo_ are consistent with input exprs.
      for (int i = 0; i < aggregateExprs_.size(); ++i) {
        FunctionCallExpr mergeAggExpr = mergeAggInfo_.aggregateExprs_.get(i);
        mergeAggExpr.validateMergeAggFn(aggregateExprs_.get(i));
      }
    }
  }

  /// Return true if any aggregate functions have a serialize function.
  /// Only valid to call once analyzed.
  public boolean needsSerialize() {
    for (FunctionCallExpr aggregateExpr: aggregateExprs_) {
      Preconditions.checkState(aggregateExpr.isAnalyzed());
      AggregateFunction fn = (AggregateFunction)aggregateExpr.getFn();
      if (fn.getSerializeFnSymbol() != null) return true;
    }
    return false;
  }

  @Override
  public String debugString() {
    StringBuilder out = new StringBuilder(super.debugString());
    out.append(MoreObjects.toStringHelper(this)
        .add("phase", aggPhase_)
        .add("intermediate_smap", intermediateTupleSmap_.debugString())
        .add("output_smap", outputTupleSmap_.debugString())
        .toString());
    if (mergeAggInfo_ != this && mergeAggInfo_ != null) {
      out.append("\nmergeAggInfo:\n" + mergeAggInfo_.debugString());
    }
    if (secondPhaseDistinctAggInfo_ != null) {
      out.append("\nsecondPhaseDistinctAggInfo:\n"
          + secondPhaseDistinctAggInfo_.debugString());
    }
    return out.toString();
  }

  @Override
  protected String tupleDebugName() { return "agg-tuple"; }

  @Override
  public AggregateInfo clone() { return new AggregateInfo(this); }

  public ProcessingCost computeProcessingCost(
      String label, long inputCardinality, long intermediateOutputCardinality) {
    Preconditions.checkArgument(
        inputCardinality >= 0, "inputCardinality should not be negative!");
    Preconditions.checkArgument(intermediateOutputCardinality >= 0,
        "intermediateOutputCardinality should not be negative!");
    // Benchmarking suggests we can estimate the processing cost as a linear function
    // based the probe input cardinality, the "intermediate" output cardinality, and
    // an incremental cost per input row for each additional aggregate function.
    // The intermediate output cardinality is an attempt to account for the fact that
    // there are typically duplicate keys across multiple fragments in any preaggregation.
    // The coefficients are derived from benchmarking and are different for different
    // numbers of grouping columns.
    int numGroupingExprs = getGroupingExprs().size();
    int numAggExprs = getMaterializedAggregateExprs().size();
    int numExtraAggExprs = numAggExprs > 1 ? numAggExprs - 1 : 0;
    double totalCost = 0.0;
    if (numGroupingExprs == 0) {
      totalCost = (inputCardinality * numAggExprs * COST_COEFFICIENT_AGG_EXPR);
    } else if (numGroupingExprs == 1) {
      totalCost = (inputCardinality * COST_COEFFICIENT_AGG_INPUT_SINGLE_GROUP)
          + (intermediateOutputCardinality * COST_COEFFICIENT_AGG_OUTPUT_SINGLE_GROUP)
          + (inputCardinality * numExtraAggExprs * COST_COEFFICIENT_AGG_EXPR);
    } else {
      totalCost = (inputCardinality * COST_COEFFICIENT_AGG_INPUT_MULTI_GROUP)
          + (intermediateOutputCardinality * COST_COEFFICIENT_AGG_OUTPUT_MULTI_GROUP)
          + (inputCardinality * numExtraAggExprs * COST_COEFFICIENT_AGG_EXPR);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Total CPU cost estimate: " + totalCost
          + ", Grouping Exprs Count: " + numGroupingExprs
          + ", Agg Exprs Count: " + numAggExprs + ", Input Card: " + inputCardinality
          + ", Intermediate Output Card: " + intermediateOutputCardinality);
    }

    return ProcessingCost.basicCost(label, totalCost);
  }
}
