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

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.planner.DataPartition;
import com.cloudera.impala.thrift.TPartitionType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Encapsulates all the information needed to compute the aggregate functions of a single
 * Select block, including a possible 2nd phase aggregation step for DISTINCT aggregate
 * functions and merge aggregation steps needed for distributed execution.
 *
 * The latter requires a tree structure of AggregateInfo objects which express the
 * original aggregate computations as well as the necessary merging aggregate
 * computations.
 * TODO: get rid of this by transforming
 *   SELECT COUNT(DISTINCT a, b, ..) GROUP BY x, y, ...
 * into an equivalent query with a inline view:
 *   SELECT COUNT(*) FROM (SELECT DISTINCT a, b, ..., x, y, ...) GROUP BY x, y, ...
 *
 * The tree structure looks as follows:
 * - for non-distinct aggregation:
 *   - aggInfo: contains the original aggregation functions and grouping exprs
 *   - aggInfo.mergeAggInfo: contains the merging aggregation functions (grouping
 *     exprs are identical)
 * - for distinct aggregation (for an explanation of the phases, see
 *   SelectStmt.createDistinctAggInfo()):
 *   - aggInfo: contains the phase 1 aggregate functions and grouping exprs
 *   - aggInfo.2ndPhaseDistinctAggInfo: contains the phase 2 aggregate functions and
 *     grouping exprs
 *   - aggInfo.mergeAggInfo: contains the merging aggregate functions for the phase 1
 *     computation (grouping exprs are identical)
 *   - aggInfo.2ndPhaseDistinctAggInfo.mergeAggInfo: contains the merging aggregate
 *     functions for the phase 2 computation (grouping exprs are identical)
 *
 * In general, merging aggregate computations are idempotent; in other words,
 * aggInfo.mergeAggInfo == aggInfo.mergeAggInfo.mergeAggInfo.
 *
 * TODO: move the merge construction logic from SelectStmt into AggregateInfo
 * TODO: Add query tests for aggregation with intermediate tuples with num_nodes=1.
 */
public class AggregateInfo extends AggregateInfoBase {
  private final static Logger LOG = LoggerFactory.getLogger(AggregateInfo.class);

  public enum AggPhase {
    FIRST,
    FIRST_MERGE,
    SECOND,
    SECOND_MERGE;

    public boolean isMerge() { return this == FIRST_MERGE || this == SECOND_MERGE; }
  };

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

  // Map from slots of outputTupleSmap_ to the corresponding slot in
  // intermediateTupleSmap_.
  protected ExprSubstitutionMap outputToIntermediateTupleSmap_ =
      new ExprSubstitutionMap();

  // if set, a subset of groupingExprs_; set and used during planning
  private List<Expr> partitionExprs_;

  // C'tor creates copies of groupingExprs and aggExprs.
  private AggregateInfo(ArrayList<Expr> groupingExprs,
      ArrayList<FunctionCallExpr> aggExprs, AggPhase aggPhase)  {
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
  static public AggregateInfo create(
      ArrayList<Expr> groupingExprs, ArrayList<FunctionCallExpr> aggExprs,
      TupleDescriptor tupleDesc, Analyzer analyzer)
          throws AnalysisException {
    Preconditions.checkState(
        (groupingExprs != null && !groupingExprs.isEmpty())
        || (aggExprs != null && !aggExprs.isEmpty()));
    Expr.removeDuplicates(groupingExprs);
    Expr.removeDuplicates(aggExprs);
    AggregateInfo result = new AggregateInfo(groupingExprs, aggExprs, AggPhase.FIRST);

    // collect agg exprs with DISTINCT clause
    ArrayList<FunctionCallExpr> distinctAggExprs = Lists.newArrayList();
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
    LOG.debug("agg info:\n" + result.debugString());
    return result;
  }

  /**
   * Create aggregate info for select block containing aggregate exprs with
   * DISTINCT clause.
   * This creates:
   * - aggTupleDesc
   * - a complete secondPhaseDistinctAggInfo
   * - mergeAggInfo
   *
   * At the moment, we require that all distinct aggregate
   * functions be applied to the same set of exprs (ie, we can't do something
   * like SELECT COUNT(DISTINCT id), COUNT(DISTINCT address)).
   * Aggregation happens in two successive phases:
   * - the first phase aggregates by all grouping exprs plus all parameter exprs
   *   of DISTINCT aggregate functions
   *
   * Example:
   *   SELECT a, COUNT(DISTINCT b, c), MIN(d), COUNT(*) FROM T GROUP BY a
   * - 1st phase grouping exprs: a, b, c
   * - 1st phase agg exprs: MIN(d), COUNT(*)
   * - 2nd phase grouping exprs: a
   * - 2nd phase agg exprs: COUNT(*), MIN(<MIN(d) from 1st phase>),
   *     SUM(<COUNT(*) from 1st phase>)
   *
   * TODO: expand implementation to cover the general case; this will require
   * a different execution strategy
   */
  private void createDistinctAggInfo(
      ArrayList<Expr> origGroupingExprs,
      ArrayList<FunctionCallExpr> distinctAggExprs, Analyzer analyzer)
          throws AnalysisException {
    Preconditions.checkState(!distinctAggExprs.isEmpty());
    // make sure that all DISTINCT params are the same;
    // ignore top-level implicit casts in the comparison, we might have inserted
    // those during analysis
    ArrayList<Expr> expr0Children = Lists.newArrayList();

    if (distinctAggExprs.get(0).getFnName().getFunction().equalsIgnoreCase(
        "group_concat")) {
      // Ignore separator parameter, otherwise the same would have to be present for all
      // other distinct aggregates as well.
      // TODO: Deal with constant exprs more generally, instead of special-casing
      // group_concat().
      expr0Children.add(distinctAggExprs.get(0).getChild(0).ignoreImplicitCast());
    } else {
      for (Expr expr : distinctAggExprs.get(0).getChildren()) {
        expr0Children.add(expr.ignoreImplicitCast());
      }
    }
    for (int i = 1; i < distinctAggExprs.size(); ++i) {
      ArrayList<Expr> exprIChildren = Lists.newArrayList();
      if (distinctAggExprs.get(i).getFnName().getFunction().equalsIgnoreCase(
          "group_concat")) {
        exprIChildren.add(distinctAggExprs.get(i).getChild(0).ignoreImplicitCast());
      } else {
        for (Expr expr : distinctAggExprs.get(i).getChildren()) {
          exprIChildren.add(expr.ignoreImplicitCast());
        }
      }
      if (!Expr.equalLists(expr0Children, exprIChildren)) {
        throw new AnalysisException(
            "all DISTINCT aggregate functions need to have the same set of "
            + "parameters as " + distinctAggExprs.get(0).toSql()
            + "; deviating function: " + distinctAggExprs.get(i).toSql());
      }
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
  public AggPhase getAggPhase() { return aggPhase_; }
  public boolean isMerge() { return aggPhase_.isMerge(); }
  public boolean isDistinctAgg() { return secondPhaseDistinctAggInfo_ != null; }
  public ExprSubstitutionMap getIntermediateSmap() { return intermediateTupleSmap_; }
  public ExprSubstitutionMap getOutputSmap() { return outputTupleSmap_; }
  public ExprSubstitutionMap getOutputToIntermediateSmap() {
    return outputToIntermediateTupleSmap_;
  }

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

  public ArrayList<FunctionCallExpr> getMaterializedAggregateExprs() {
    ArrayList<FunctionCallExpr> result = Lists.newArrayList();
    for (Integer i: materializedSlots_) {
      result.add(aggregateExprs_.get(i));
    }
    return result;
  }

  /**
   * Append ids of all slots that are being referenced in the process
   * of performing the aggregate computation described by this AggregateInfo.
   */
  public void getRefdSlots(List<SlotId> ids) {
    Preconditions.checkState(outputTupleDesc_ != null);
    if (groupingExprs_ != null) {
      Expr.getIds(groupingExprs_, null, ids);
    }
    Expr.getIds(aggregateExprs_, null, ids);
    // The backend assumes that the entire aggTupleDesc is materialized
    for (int i = 0; i < outputTupleDesc_.getSlots().size(); ++i) {
      ids.add(outputTupleDesc_.getSlots().get(i).getId());
    }
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
    groupingExprs_ = Expr.substituteList(groupingExprs_, smap, analyzer, false);
    LOG.trace("AggInfo: grouping_exprs=" + Expr.debugString(groupingExprs_));

    // The smap in this case should not substitute the aggs themselves, only
    // their subexpressions.
    List<Expr> substitutedAggs =
        Expr.substituteList(aggregateExprs_, smap, analyzer, false);
    aggregateExprs_.clear();
    for (Expr substitutedAgg: substitutedAggs) {
      aggregateExprs_.add((FunctionCallExpr) substitutedAgg);
    }

    LOG.trace("AggInfo: agg_exprs=" + Expr.debugString(aggregateExprs_));
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
    ArrayList<Expr> groupingExprs = Lists.newArrayList();
    for (int i = 0; i < getGroupingExprs().size(); ++i) {
      SlotRef slotRef = new SlotRef(inputDesc.getSlots().get(i));
      groupingExprs.add(slotRef);
    }

    // construct agg exprs
    ArrayList<FunctionCallExpr> aggExprs = Lists.newArrayList();
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
      ArrayList<SlotDescriptor> slots) {
    if (firstIdx > lastIdx) return null;

    Expr elseExpr = new SlotRef(slots.get(lastIdx));
    if (firstIdx == lastIdx) return elseExpr;

    for (int i = lastIdx - 1; i >= firstIdx; --i) {
      ArrayList<Expr> ifArgs = Lists.newArrayList();
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
  private void createSecondPhaseAggInfo(
      ArrayList<Expr> origGroupingExprs,
      ArrayList<FunctionCallExpr> distinctAggExprs, Analyzer analyzer)
      throws AnalysisException {
    Preconditions.checkState(secondPhaseDistinctAggInfo_ == null);
    Preconditions.checkState(!distinctAggExprs.isEmpty());
    // The output of the 1st phase agg is the 1st phase intermediate.
    TupleDescriptor inputDesc = intermediateTupleDesc_;

    // construct agg exprs for original DISTINCT aggregate functions
    // (these aren't part of aggExprs_)
    ArrayList<FunctionCallExpr> secondPhaseAggExprs = Lists.newArrayList();
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
        ArrayList<Expr> exprList = Lists.newArrayList();
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

    ArrayList<Expr> substGroupingExprs =
        Expr.substituteList(origGroupingExprs, intermediateTupleSmap_, analyzer, false);
    secondPhaseDistinctAggInfo_ =
        new AggregateInfo(substGroupingExprs, secondPhaseAggExprs, AggPhase.SECOND);
    secondPhaseDistinctAggInfo_.createTupleDescs(analyzer);
    secondPhaseDistinctAggInfo_.createSecondPhaseAggSMap(this, distinctAggExprs);
    secondPhaseDistinctAggInfo_.createMergeAggInfo(analyzer);
  }

  /**
   * Create smap to map original grouping and aggregate exprs onto output
   * of secondPhaseDistinctAggInfo.
   */
  private void createSecondPhaseAggSMap(
      AggregateInfo inputAggInfo, ArrayList<FunctionCallExpr> distinctAggExprs) {
    outputTupleSmap_.clear();
    int slotIdx = 0;
    ArrayList<SlotDescriptor> slotDescs = outputTupleDesc_.getSlots();

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
      outputToIntermediateTupleSmap_.put(
          new SlotRef(outputTupleDesc_.getSlots().get(i)),
          new SlotRef(intermediateTupleDesc_.getSlots().get(i)));
      if (i < groupingExprs_.size()) {
        analyzer.createAuxEquivPredicate(
            new SlotRef(outputTupleDesc_.getSlots().get(i)),
            new SlotRef(intermediateTupleDesc_.getSlots().get(i)));
      }
    }
    if (!requiresIntermediateTuple()) intermediateTupleSmap_ = outputTupleSmap_;

    LOG.trace("output smap=" + outputTupleSmap_.debugString());
    LOG.trace("intermediate smap=" + intermediateTupleSmap_.debugString());
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
    List<Expr> exprs = Lists.newArrayList();
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
   * Validates the internal state of this agg info: Checks that the number of
   * materialized slots of the output tuple corresponds to the number of materialized
   * aggregate functions plus the number of grouping exprs. Also checks that the return
   * types of the aggregate and grouping exprs correspond to the slots in the output
   * tuple.
   */
  public void checkConsistency() {
    ArrayList<SlotDescriptor> slots = outputTupleDesc_.getSlots();

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
  }

  /**
   * Returns DataPartition derived from grouping exprs.
   * Returns unpartitioned spec if no grouping.
   * TODO: this won't work when we start supporting range partitions,
   * because we could derive both hash and order-based partitions
   */
  public DataPartition getPartition() {
    if (groupingExprs_.isEmpty()) {
      return DataPartition.UNPARTITIONED;
    } else {
      return new DataPartition(TPartitionType.HASH_PARTITIONED, groupingExprs_);
    }
  }

  @Override
  public String debugString() {
    StringBuilder out = new StringBuilder(super.debugString());
    out.append(Objects.toStringHelper(this)
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
}
