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

import com.cloudera.impala.catalog.ColumnStats;
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
 */
public class AggregateInfo {
  private final static Logger LOG = LoggerFactory.getLogger(AggregateInfo.class);

  // all exprs from Group By clause, duplicates removed
  private ArrayList<Expr> groupingExprs_;

  // all agg exprs from select block, duplicates removed
  private final ArrayList<FunctionCallExpr> aggregateExprs_;

  // indices into aggregateExprs for those that need to be materialized;
  // shared between this, mergeAggInfo and secondPhaseDistinctAggInfo
  private ArrayList<Integer> materializedAggregateSlots_ = Lists.newArrayList();

  // The tuple into which the output of the aggregation computation is materialized;
  // contains groupingExprs.size() + aggregateExprs.size() slots, the first
  // groupingExprs.size() of which contain the values of the grouping exprs, followed by
  // slots for the values of the aggregate exprs.
  private TupleDescriptor aggTupleDesc_;

  // map from all grouping and aggregate exprs to a SlotRef referencing the corresp. slot
  // in the agg tuple
  private ExprSubstitutionMap aggTupleSMap_;

  // created by createMergeAggInfo()
  private AggregateInfo mergeAggInfo_;

  // created by createDistinctAggInfo()
  private AggregateInfo secondPhaseDistinctAggInfo_;

  // if true, this AggregateInfo is the first phase of a 2-phase DISTINCT computation
  private boolean isDistinctAgg_ = false;

  // If true, this is doing the merge phase of the aggregation.
  private final boolean isMerge_;

  // C'tor creates copies of groupingExprs and aggExprs.
  // Does *not* set aggTupleDesc, aggTupleSMap, mergeAggInfo, secondPhaseDistinctAggInfo.
  private AggregateInfo(ArrayList<Expr> groupingExprs,
      ArrayList<FunctionCallExpr> aggExprs, boolean isMerge)  {
    isMerge_ = isMerge;
    aggTupleSMap_ = new ExprSubstitutionMap();
    groupingExprs_ =
        (groupingExprs != null
          ? Expr.cloneList(groupingExprs)
          : new ArrayList<Expr>());
    aggregateExprs_ =
        (aggExprs != null
          ? Expr.cloneList(aggExprs)
          : new ArrayList<FunctionCallExpr>());
  }

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
          throws AnalysisException, InternalException {
    Preconditions.checkState(
        (groupingExprs != null && !groupingExprs.isEmpty())
        || (aggExprs != null && !aggExprs.isEmpty()));
    Expr.removeDuplicates(groupingExprs);
    Expr.removeDuplicates(aggExprs);
    AggregateInfo result = new AggregateInfo(groupingExprs, aggExprs, false);

    // collect agg exprs with DISTINCT clause
    ArrayList<FunctionCallExpr> distinctAggExprs = Lists.newArrayList();
    if (aggExprs != null) {
      for (FunctionCallExpr aggExpr: aggExprs) {
        Preconditions.checkState(aggExpr.isAggregateFunction());
        if (aggExpr.isDistinct()) distinctAggExprs.add(aggExpr);
      }
    }

    if (distinctAggExprs.isEmpty()) {
      if (tupleDesc == null) {
        result.aggTupleDesc_ = result.createAggTupleDesc(analyzer);
      } else {
        result.aggTupleDesc_ = tupleDesc;
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
          throws AnalysisException, InternalException {
    Preconditions.checkState(!distinctAggExprs.isEmpty());
    // make sure that all DISTINCT params are the same;
    // ignore top-level implicit casts in the comparison, we might have inserted
    // those during analysis
    ArrayList<Expr> expr0Children = Lists.newArrayList();
    for (Expr expr: distinctAggExprs.get(0).getChildren()) {
      expr0Children.add(expr.ignoreImplicitCast());
    }
    for (int i = 1; i < distinctAggExprs.size(); ++i) {
      ArrayList<Expr> exprIChildren = Lists.newArrayList();
      for (Expr expr: distinctAggExprs.get(i).getChildren()) {
        exprIChildren.add(expr.ignoreImplicitCast());
      }
      if (!Expr.equalLists(expr0Children, exprIChildren)) {
        throw new AnalysisException(
            "all DISTINCT aggregate functions need to have the same set of "
            + "parameters as " + distinctAggExprs.get(0).toSql()
            + "; deviating function: " + distinctAggExprs.get(i).toSql());
      }
    }
    isDistinctAgg_ = true;

    // add DISTINCT parameters to grouping exprs
    groupingExprs_.addAll(expr0Children);

    // remove DISTINCT aggregate functions from aggExprs
    aggregateExprs_.removeAll(distinctAggExprs);

    aggTupleDesc_ = createAggTupleDesc(analyzer);
    createMergeAggInfo(analyzer);
    createSecondPhaseDistinctAggInfo(origGroupingExprs, distinctAggExprs, analyzer);
  }


  public ArrayList<Expr> getGroupingExprs() { return groupingExprs_; }
  public ArrayList<FunctionCallExpr> getAggregateExprs() { return aggregateExprs_; }
  public boolean isDistinctAgg() { return isDistinctAgg_; }
  public TupleId getAggTupleId() { return aggTupleDesc_.getId(); }
  public ExprSubstitutionMap getSMap() { return aggTupleSMap_; }
  public AggregateInfo getMergeAggInfo() { return mergeAggInfo_; }
  public boolean isMerge() { return isMerge_; }
  public AggregateInfo getSecondPhaseDistinctAggInfo() {
    return secondPhaseDistinctAggInfo_;
  }
  public TupleDescriptor getAggTupleDesc() { return aggTupleDesc_; }
  public void setAggTupleDesc(TupleDescriptor aggTupleDesc) {
    aggTupleDesc_ = aggTupleDesc;
  }

  /**
   * Return the tuple id produced in the final aggregation step.
   */
  public TupleId getOutputTupleId() {
    if (isDistinctAgg()) return secondPhaseDistinctAggInfo_.getAggTupleId();
    return getAggTupleId();
  }

  public ArrayList<FunctionCallExpr> getMaterializedAggregateExprs() {
    ArrayList<FunctionCallExpr> result = Lists.newArrayList();
    for (Integer i: materializedAggregateSlots_) {
      result.add(aggregateExprs_.get(i));
    }
    return result;
  }

  /**
   * Append ids of all slots that are being referenced in the process
   * of performing the aggregate computation described by this AggregateInfo.
   */
  public void getRefdSlots(List<SlotId> ids) {
    Preconditions.checkState(aggTupleDesc_ != null);
    if (groupingExprs_ != null) {
      Expr.getIds(groupingExprs_, null, ids);
    }
    Expr.getIds(aggregateExprs_, null, ids);
    // The backend assumes that the entire aggTupleDesc is materialized
    for (int i = 0; i < aggTupleDesc_.getSlots().size(); ++i) {
      ids.add(aggTupleDesc_.getSlots().get(i).getId());
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
    groupingExprs_ = Expr.substituteList(groupingExprs_, smap, analyzer);
    LOG.trace("AggInfo: grouping_exprs=" + Expr.debugString(groupingExprs_));

    // The smap in this case should not substitute the aggs themselves, only
    // their subexpressions.
    List<Expr> substitutedAggs = Expr.substituteList(aggregateExprs_, smap, analyzer);
    aggregateExprs_.clear();
    for (Expr substitutedAgg: substitutedAggs) {
      aggregateExprs_.add((FunctionCallExpr) substitutedAgg);
    }

    LOG.trace("AggInfo: agg_exprs=" + Expr.debugString(aggregateExprs_));
    aggTupleSMap_.substituteLhs(smap, analyzer);
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
  private void createMergeAggInfo(Analyzer analyzer) throws InternalException {
    Preconditions.checkState(mergeAggInfo_ == null);
    TupleDescriptor inputDesc = aggTupleDesc_;
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
      List<Expr> aggExprParamList = Lists.newArrayList(aggExprParam);
      FunctionCallExpr aggExpr = null;
      if (inputExpr.getFnName().getFunction().equals("count")) {
        aggExpr = new FunctionCallExpr("sum", new FunctionParams(aggExprParamList));
      } else {
        aggExpr = new FunctionCallExpr(inputExpr, new FunctionParams(aggExprParamList));
      }
      try {
        aggExpr.setIsMergeAggFn();
        aggExpr.analyze(analyzer);
      } catch (Exception e) {
        // we shouldn't see this
        throw new InternalException(
            "error constructing merge aggregation node: " + e.getMessage());
      }
      aggExprs.add(aggExpr);
    }

    mergeAggInfo_ = new AggregateInfo(groupingExprs, aggExprs, true);
    mergeAggInfo_.aggTupleDesc_ = aggTupleDesc_;
    mergeAggInfo_.aggTupleSMap_ = aggTupleSMap_;
    mergeAggInfo_.mergeAggInfo_ = mergeAggInfo_;
    mergeAggInfo_.materializedAggregateSlots_ = materializedAggregateSlots_;
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
   * Create the info for an aggregation node that computes the second phase of of
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
  private void createSecondPhaseDistinctAggInfo(
      ArrayList<Expr> origGroupingExprs,
      ArrayList<FunctionCallExpr> distinctAggExprs, Analyzer analyzer)
      throws AnalysisException, InternalException {
    Preconditions.checkState(secondPhaseDistinctAggInfo_ == null);
    Preconditions.checkState(!distinctAggExprs.isEmpty());
    TupleDescriptor inputDesc = aggTupleDesc_;

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
        try {
          ifExpr.analyze(analyzer);
        } catch (Exception e) {
          throw new InternalException("Failed to analyze 'IF' function " +
              "in second phase count distinct aggregation.", e);
        }
        aggExpr = new FunctionCallExpr("count",
            new FunctionParams(Lists.newArrayList(ifExpr)));
      } else {
        // SUM(DISTINCT <expr>) -> SUM(<last grouping slot>);
        // (MIN(DISTINCT ...) and MAX(DISTINCT ...) have their DISTINCT turned
        // off during analysis, and AVG() is changed to SUM()/COUNT())
        Preconditions.checkState(inputExpr.getFnName().getFunction().equals("sum"));
        Expr aggExprParam =
            new SlotRef(inputDesc.getSlots().get(origGroupingExprs.size()));
        aggExpr = new FunctionCallExpr("sum",
            new FunctionParams(Lists.newArrayList(aggExprParam)));
      }
      secondPhaseAggExprs.add(aggExpr);
    }

    // map all the remaining agg fns
    for (int i = 0; i < aggregateExprs_.size(); ++i) {
      FunctionCallExpr inputExpr = aggregateExprs_.get(i);
      Preconditions.checkState(inputExpr.isAggregateFunction());
      // we're aggregating an output slot of the 1st agg phase
      Expr aggExprParam =
          new SlotRef(inputDesc.getSlots().get(i + getGroupingExprs().size()));
      List<Expr> aggExprParamList = Lists.newArrayList(aggExprParam);
      FunctionCallExpr aggExpr = null;
      if (inputExpr.getFnName().getFunction().equals("count")) {
        aggExpr = new FunctionCallExpr("sum",
            new FunctionParams(aggExprParamList));
      } else {
        // TODO: remap types here. The inserted agg expr doesn't need to be the same
        // type. e.g. original expr takes bigint and returns bigint, but after
        // inserting the new aggexpr, it would be bigint -> string. string -> bigint.
        aggExpr = new FunctionCallExpr(inputExpr, new FunctionParams(aggExprParamList));
        aggExpr.setIsMergeAggFn();
      }
      secondPhaseAggExprs.add(aggExpr);
    }
    Preconditions.checkState(
        secondPhaseAggExprs.size() == aggregateExprs_.size() + distinctAggExprs.size());

    for (FunctionCallExpr aggExpr: secondPhaseAggExprs) {
      try {
        aggExpr.analyze(analyzer);
        Preconditions.checkState(aggExpr.isAggregateFunction());
      } catch (Exception e) {
        // we shouldn't see this
        throw new InternalException(
            "error constructing merge aggregation node", e);
      }
    }

    ArrayList<Expr> substGroupingExprs =
        Expr.substituteList(origGroupingExprs, aggTupleSMap_, analyzer);
    secondPhaseDistinctAggInfo_ =
        new AggregateInfo(substGroupingExprs, secondPhaseAggExprs, true);
    secondPhaseDistinctAggInfo_.aggTupleDesc_ =
        secondPhaseDistinctAggInfo_.createAggTupleDesc(analyzer);
    secondPhaseDistinctAggInfo_.createSecondPhaseDistinctAggSMap(this, distinctAggExprs);
    secondPhaseDistinctAggInfo_.createMergeAggInfo(analyzer);
  }

  /**
   * Create smap to map original grouping and aggregate exprs onto output
   * of secondPhaseDistinctAggInfo.
   */
  private void createSecondPhaseDistinctAggSMap(
      AggregateInfo inputAggInfo, ArrayList<FunctionCallExpr> distinctAggExprs) {
    aggTupleSMap_.clear();
    int slotIdx = 0;
    ArrayList<SlotDescriptor> slotDescs = aggTupleDesc_.getSlots();

    int numDistinctParams = distinctAggExprs.get(0).getChildren().size();
    int numOrigGroupingExprs =
        inputAggInfo.getGroupingExprs().size() - numDistinctParams;
    Preconditions.checkState(slotDescs.size() ==
        numOrigGroupingExprs + distinctAggExprs.size() +
        inputAggInfo.getAggregateExprs().size());

    // original grouping exprs -> first m slots
    for (int i = 0; i < numOrigGroupingExprs; ++i, ++slotIdx) {
      Expr groupingExpr = inputAggInfo.getGroupingExprs().get(i);
      aggTupleSMap_.put(
          groupingExpr.clone(), new SlotRef(slotDescs.get(slotIdx)));
    }

    // distinct agg exprs -> next n slots
    for (int i = 0; i < distinctAggExprs.size(); ++i, ++slotIdx) {
      Expr aggExpr = distinctAggExprs.get(i);
      aggTupleSMap_.put(
          aggExpr.clone(), (new SlotRef(slotDescs.get(slotIdx))));
    }

    // remaining agg exprs -> remaining slots
    for (int i = 0; i < inputAggInfo.getAggregateExprs().size(); ++i, ++slotIdx) {
      Expr aggExpr = inputAggInfo.getAggregateExprs().get(i);
      aggTupleSMap_.put(aggExpr.clone(), new SlotRef(slotDescs.get(slotIdx)));
    }
  }

  /**
   * Returns descriptor for output tuples created by aggregation computation.
   * Also creates and register auxiliary equality predicates between the grouping slots
   * and the grouping exprs.
   */
  public TupleDescriptor createAggTupleDesc(Analyzer analyzer) {
    LOG.trace("createAggTupleDesc()");
    TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor();
    List<Expr> exprs = Lists.newLinkedList();
    exprs.addAll(groupingExprs_);
    exprs.addAll(aggregateExprs_);

    int aggregateExprStartIndex = groupingExprs_.size();
    for (int i = 0; i < exprs.size(); ++i) {
      Expr expr = exprs.get(i);
      SlotDescriptor outputSlotDesc = analyzer.addSlotDescriptor(result);
      outputSlotDesc.setLabel(expr.toSql());
      Preconditions.checkState(expr.getType().isValid());
      outputSlotDesc.setType(expr.getType());
      outputSlotDesc.setStats(ColumnStats.fromExpr(expr));
      LOG.trace(outputSlotDesc.debugString());
      // count(*) is non-nullable.
      if (i < aggregateExprStartIndex) {
        // register equivalence between grouping slot and grouping expr;
        // do this only when the grouping expr isn't a constant, otherwise
        // it'll simply show up as a gratuitous HAVING predicate
        // (which would actually be incorrect of the constant happens to be NULL)
        if (!expr.isConstant()) {
          analyzer.createAuxEquivPredicate(
              new SlotRef(outputSlotDesc), expr.clone());
        }
      } else {
        Preconditions.checkArgument(expr instanceof FunctionCallExpr);
        FunctionCallExpr aggExpr = (FunctionCallExpr)expr;
        if (aggExpr.getFnName().getFunction().equals("count")) {
          outputSlotDesc.setIsNullable(false);
        }
      }
      aggTupleSMap_.put(expr.clone(), new SlotRef(outputSlotDesc));
    }
    LOG.trace("aggtuple=" + result.debugString());
    LOG.trace("aggtuplesmap=" + aggTupleSMap_.debugString());
    return result;
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
  public void materializeRequiredSlots(Analyzer analyzer, ExprSubstitutionMap smap)
      throws InternalException {
    for (int i = 0; i < groupingExprs_.size(); ++i) {
      aggTupleDesc_.getSlots().get(i).setIsMaterialized(true);
    }

    // collect input exprs: grouping exprs plus aggregate exprs that need to be
    // materialized
    materializedAggregateSlots_.clear();
    List<Expr> exprs = Lists.newArrayList();
    exprs.addAll(groupingExprs_);
    for (int i = 0; i < aggregateExprs_.size(); ++i) {
      SlotDescriptor slotDesc = aggTupleDesc_.getSlots().get(groupingExprs_.size() + i);
      if (isDistinctAgg_) slotDesc.setIsMaterialized(true);
      if (!slotDesc.isMaterialized()) continue;
      exprs.add(aggregateExprs_.get(i));
      materializedAggregateSlots_.add(i);
    }
    List<Expr> resolvedExprs = Expr.substituteList(exprs, smap, analyzer);
    analyzer.materializeSlots(resolvedExprs);

    if (isDistinctAgg_) {
      secondPhaseDistinctAggInfo_.materializeRequiredSlots(analyzer, null);
    }
  }

  /**
   * Validates the internal state of this agg info: Checks that the number of
   * materialized slots of the agg tuple corresponds to the number of materialized
   * aggregate functions plus the number of grouping exprs. Also checks that the return
   * types of the aggregate and grouping exprs correspond to the slots in the agg tuple.
   */
  public void checkConsistency() {
    ArrayList<SlotDescriptor> slots = aggTupleDesc_.getSlots();

    // Check materialized slots.
    int numMaterializedSlots = 0;
    for (SlotDescriptor slotDesc: slots) {
      if (slotDesc.isMaterialized()) ++numMaterializedSlots;
    }
    Preconditions.checkState(numMaterializedSlots ==
        materializedAggregateSlots_.size() + groupingExprs_.size());

    // Check that grouping expr return types match the slot descriptors.
    int slotIdx = 0;
    for (int i = 0; i < groupingExprs_.size(); ++i) {
      Expr groupingExpr = groupingExprs_.get(i);
      Type slotType = slots.get(slotIdx).getType();
      Preconditions.checkState(groupingExpr.getType().equals(slotType),
          String.format("Grouping expr %s returns type %s but its agg tuple " +
              "slot has type %s", groupingExpr.toSql(),
              groupingExpr.getType().toString(), slotType.toString()));
      ++slotIdx;
    }
    // Check that aggregate expr return types match the slot descriptors.
    for (int i = 0; i < aggregateExprs_.size(); ++i) {
      Expr aggExpr = aggregateExprs_.get(i);
      Type slotType = slots.get(slotIdx).getType();
      Preconditions.checkState(aggExpr.getType().equals(slotType),
          String.format("Agg expr %s returns type %s but its agg tuple " +
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

  public String debugString() {
    StringBuilder out = new StringBuilder();
    out.append(Objects.toStringHelper(this)
        .add("merging", isMerge_)
        .add("grouping_exprs", Expr.debugString(groupingExprs_))
        .add("aggregate_exprs", Expr.debugString(aggregateExprs_))
        .add("agg_tuple", (aggTupleDesc_ == null ? "null" : aggTupleDesc_.debugString()))
        .add("smap", aggTupleSMap_.debugString())
        .toString());
    if (mergeAggInfo_ != this) {
      out.append("\nmergeAggInfo:\n" + mergeAggInfo_.debugString());
    }
    if (secondPhaseDistinctAggInfo_ != null) {
      out.append("\nsecondPhaseDistinctAggInfo:\n"
          + secondPhaseDistinctAggInfo_.debugString());
    }

    return out.toString();
  }
}
