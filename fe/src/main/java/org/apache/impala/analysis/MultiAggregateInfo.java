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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.kudu.shaded.com.google.common.collect.Lists;

import com.google.common.base.Preconditions;

/**
 * Encapsulates all the information needed to compute the aggregate functions of a single
 * SELECT block including their distributed execution by wrapping a list of
 * AggregateInfos.
 *
 * The aggregate functions are organized into aggregation classes. An aggregation class
 * contains a list of aggregate functions that have the same list of DISTINCT exprs.
 * All non-distinct aggregate functions are placed into a single non-distinct class.
 * Each aggregation class is associated with its own AggregateInfo and corresponding
 * intermediate and output tuples.
 *
 * The phases of aggregate computation are as follows. Also see AggregateInfo.
 * - Only a non-distinct class:
 *   - Example: SELECT max(a) FROM...
 *   - 1-phase aggregation
 * - One distinct class, and optionally a non-distinct class:
 *   - Example: SELECT count(distinct a)[, max(b)] FROM...
 *   - coalesced into a single AggregateInfo to preserve the pre-IMPALA-110 behavior
 *   - 2-phase aggregation, 1st phase groups by GROUP BY plus DISTINCT exprs, 2nd phase
 *     groups by GROUP BY
 *   - the non-distinct class is carried along the two phases, aggregated in 1st phase and
 *     merged in 2nd phase
 * - Multiple distinct classes, and optionally a non-distinct class
 *   - Example: SELECT count(distinct a), count(distinct b)[, max(c)] FROM...
 *   - 2-phase aggregation followed by a transposition aggregation
 *   - aggregation nodes update and maintain the state of all aggregation classes at once
 *   - aggregation nodes produce the union of results of all aggregation classes;
 *     the output cardinality is the sum of the aggregation class cardinalities;
 *     each output row belongs to exactly one aggregation class (see row composition)
 *   - the first aggregation node of the first-phase updates the state of all classes by
 *     replicating its input; all aggregation classes receive identical input
 *   - the remaining aggregation steps of the 2-phase aggregation process their input by
 *     updating the aggregation class that each row belongs to (see row composition)
 *   - the last step of the 2-phase aggregation produces the union of aggregation results
 *     of all classes; a final aggregation is performed to transpose them into the form
 *     requested by the query, where for each group (in the GROUP BY sense) the results
 *     of all classes are in a single row
 *
 * Row composition:
 * Each intermediate aggregation step produces rows with one tuple per aggregation class,
 * where only a single tuple is non-NULL for each row. You can think of a row as
 * a union-type of tuples where the union branch is determined by which tuple is non-NULL
 * in a row.
 *
 * In a distributed plan we need to send the tuple rows over the network to some merge
 * aggregators, therefore we need to partition our rows based on the actual values. To
 * distinguish between the aggregation classes we craft CASE exprs that switch on the
 * union branch with an internal valid_tid() expr. Such CASE exprs are used in hash
 * exchanges to select the appropriate slots to hash depending on which tuple is set.
 * These CASE exprs are also used in the grouping exprs of the final transposition step
 * to allow us to group on the proper slots.
 *
 * Regardless of which phases an aggregation goes through, the final result rows only
 * consist of one tuple. In other words, the tuple union and the exprs that are aware of
 * the union are completely contained in the aggregation plan, and have no effect on the
 * rest of query execution.
 *
 * Analysis:
 * An AggregateInfo per class and, if needed, an AggregateInfo for the transposition are
 * created in analyze(). The analysis is scoped to a single SELECT block, so does not
 * consider that enclosing blocks might not reference the output of some aggregations.
 *
 * Conjunct registration and assignment:
 * Conjuncts are expected to be registered against the result tuple of this
 * MultiAggregateInfo (see getResultTupleId()). The conjuncts which should be evaluated
 * on the aggregation result can be collected by collectConjuncts().
 *
 * Slot materialization and aggregate simplification:
 * The method materializeRequiredSlots() should be called in the slot materialization
 * phase (see SingleNodePlanner) to eliminate aggregations that are not needed for the
 * final query result. Entire aggregation classes may be removed and even reduce the
 * aggregation to a single class. If as a result of simplification the transposition step
 * becomes unnecessary an expr substitution map is populated to remap exprs referencing
 * the transposition to the output of the new simplified aggregation.
 */
public class MultiAggregateInfo {
  public static enum AggPhase {
    FIRST,
    FIRST_MERGE,
    SECOND,
    SECOND_MERGE,
    TRANSPOSE;

    public boolean isFirstPhase() { return this == FIRST || this == FIRST_MERGE; };
    public boolean isMerge() { return this == FIRST_MERGE || this == SECOND_MERGE; }
    public boolean isTranspose() { return this == TRANSPOSE; }
  }

  // Original grouping and aggregate exprs from the containing query block.
  // Duplicate free.
  private final List<Expr> groupingExprs_;
  private final List<FunctionCallExpr> aggExprs_;

  // Result of substituting 'groupingExprs_' with the output smap of the AggregationNode.
  private List<Expr> substGroupingExprs_;

  // Results of analyze():

  // Aggregation classes and their AggregateInfos. If there is a class with non-distinct
  // aggs, then it comes last in these lists.
  private List<List<FunctionCallExpr>> aggClasses_;
  private List<AggregateInfo> aggInfos_;

  // Info for the final transposition step. Null if no such step is needed.
  private AggregateInfo transposeAggInfo_;

  // Maps from the original grouping exprs and aggregate exprs to the final output
  // of this aggregation, regardless of how many phases there are.
  private ExprSubstitutionMap outputSmap_;
  private boolean isAnalyzed_;

  // Results of materializeRequiredSlots():

  // Subset of aggregation classes that need to be materialized.
  private List<AggregateInfo> materializedAggInfos_;

  // Set if this aggregation was thought to require a transposition step, but was
  // simplified to a single class in materializeRequiredSlots().
  // Maps from slots in the transposition result tuple to the slots of the new
  // result tuple of the simplified aggregation.
  private ExprSubstitutionMap simplifiedAggSmap_;

  public MultiAggregateInfo(List<Expr> groupingExprs, List<FunctionCallExpr> aggExprs) {
    groupingExprs_ = Expr.cloneList(Preconditions.checkNotNull(groupingExprs));
    aggExprs_ = Expr.cloneList(Preconditions.checkNotNull(aggExprs));
  }

  /**
   * Creates a pre-analyzed MultiAggregateInfo from the given AggregateInfo expected
   * to have grouping and no aggregation.
   */
  private MultiAggregateInfo(AggregateInfo distinctAggInfo) {
    Preconditions.checkState(distinctAggInfo.getAggregateExprs().isEmpty());
    groupingExprs_ = distinctAggInfo.getGroupingExprs();
    aggExprs_ = distinctAggInfo.getAggregateExprs();
    aggInfos_ = Lists.newArrayList(distinctAggInfo);
    outputSmap_ = distinctAggInfo.getResultSmap();
    isAnalyzed_ = true;
  }

  /**
   * C'tor for cloning.
   */
  private MultiAggregateInfo(MultiAggregateInfo other) {
    groupingExprs_ = Expr.cloneList(other.groupingExprs_);
    aggExprs_ = Expr.cloneList(other.aggExprs_);
    if (other.aggInfos_ != null) {
      aggInfos_ = new ArrayList<>();
      for (AggregateInfo aggInfo : other.aggInfos_) aggInfos_.add(aggInfo.clone());
    }
    if (other.transposeAggInfo_ != null) {
      transposeAggInfo_ = other.transposeAggInfo_.clone();
    }
    if (other.outputSmap_ != null) {
      outputSmap_ = other.outputSmap_.clone();
    }
  }

  /**
   * Creates a pre-analyzed MultiAggregateInfo that only has grouping and materializes
   * its result into the given tuple descriptor which must already have all required
   * slots.
   */
  public static MultiAggregateInfo createDistinct(List<Expr> groupingExprs,
      TupleDescriptor tupleDesc, Analyzer analyzer) throws AnalysisException {
    AggregateInfo distinctAggInfo =
        AggregateInfo.create(groupingExprs, null, tupleDesc, analyzer);
    return new MultiAggregateInfo(distinctAggInfo);
  }

  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    isAnalyzed_ = true;

    // Group the agg exprs by their DISTINCT exprs and move the non-distinct agg exprs
    // into a separate list.
    // List of all DISTINCT expr lists and the grouped agg exprs.
    // distinctExprs[i] corresponds to groupedDistinctAggExprs[i]
    List<List<Expr>> distinctExprs = new ArrayList<>();
    List<List<FunctionCallExpr>> groupedDistinctAggExprs = new ArrayList<>();
    List<FunctionCallExpr> nonDistinctAggExprs = new ArrayList<>();
    for (FunctionCallExpr aggExpr : aggExprs_) {
      if (aggExpr.isDistinct()) {
        List<Expr> children = AggregateFunction.getCanonicalDistinctAggChildren(aggExpr);
        int groupIdx = distinctExprs.indexOf(children);
        List<FunctionCallExpr> groupAggFns;
        if (groupIdx == -1) {
          distinctExprs.add(children);
          groupAggFns = new ArrayList<>();
          groupedDistinctAggExprs.add(groupAggFns);
        } else {
          groupAggFns = groupedDistinctAggExprs.get(groupIdx);
        }
        groupAggFns.add(aggExpr);
      } else {
        nonDistinctAggExprs.add(aggExpr);
      }
    }
    Preconditions.checkState(distinctExprs.size() == groupedDistinctAggExprs.size());

    // Populate aggregation classes.
    aggClasses_ = new ArrayList<>();
    boolean hasNonDistinctAggExprs = !nonDistinctAggExprs.isEmpty();

    if (groupedDistinctAggExprs.size() == 0) {
      if (hasNonDistinctAggExprs) {
        // Only a non-distinct agg class.
        aggClasses_.add(nonDistinctAggExprs);
      } else {
        // Only grouping with no aggregation (e.g. SELECT DISTINCT)
        Preconditions.checkState(!groupingExprs_.isEmpty());
        aggClasses_.add(null);
      }
    } else if (groupedDistinctAggExprs.size() == 1) {
      // Exactly one distinct class and optionally a non-distinct one.
      List<FunctionCallExpr> aggClass =
          Lists.newArrayList(groupedDistinctAggExprs.get(0));
      // Coalesce into one class to preserve the pre-IMPALA-110 behavior.
      aggClass.addAll(nonDistinctAggExprs);
      aggClasses_.add(aggClass);
    } else {
      // Multiple distinct classes and an optional non-distinct one.
      aggClasses_.addAll(groupedDistinctAggExprs);
      if (hasNonDistinctAggExprs) aggClasses_.add(nonDistinctAggExprs);
    }

    // Create agg infos for the agg classes.
    aggInfos_ = Lists.newArrayListWithCapacity(aggClasses_.size());
    for (List<FunctionCallExpr> aggClass : aggClasses_) {
      aggInfos_.add(AggregateInfo.create(groupingExprs_, aggClass, analyzer));
    }
    if (aggInfos_.size() == 1) {
      // Only a single aggregation class, no transposition step is needed.
      outputSmap_ = aggInfos_.get(0).getResultSmap();
      return;
    }

    // Create agg info for the final transposition step.
    transposeAggInfo_ = createTransposeAggInfo(groupingExprs_, aggInfos_, analyzer);
    // Register value transfers to allow predicate assignment through GROUP BY.
    List<SlotDescriptor> outputSlots = transposeAggInfo_.getResultTupleDesc().getSlots();
    Preconditions.checkState(groupingExprs_.size() <= outputSlots.size());
    for (int i = 0; i < groupingExprs_.size(); ++i) {
      analyzer.createAuxEqPredicate(
          new SlotRef(outputSlots.get(i)), groupingExprs_.get(i));
    }

    // Maps from the original grouping and aggregate exprs in all aggregation classes to
    // the final output produced by the transposing aggregation.
    outputSmap_ = new ExprSubstitutionMap();
    // Add mappings for grouping exprs.
    for (int i = 0; i < groupingExprs_.size(); ++i) {
      outputSmap_.put(groupingExprs_.get(i).clone(), new SlotRef(outputSlots.get(i)));
    }
    // Add mappings for aggregate functions.
    int outputSlotIdx = groupingExprs_.size();
    for (AggregateInfo aggInfo : aggInfos_) {
      ExprSubstitutionMap aggSmap = aggInfo.getResultSmap();
      for (int i = groupingExprs_.size(); i < aggSmap.size(); ++i) {
        outputSmap_.put(
            aggSmap.getLhs().get(i).clone(), new SlotRef(outputSlots.get(outputSlotIdx)));
        ++outputSlotIdx;
      }
    }
  }

  /**
   * Returns a new AggregateInfo for transposing the union of results of the given input
   * AggregateInfos, with the given grouping. The AggregateInfos must have been created
   * based on the same grouping exprs.
   *
   * The transposing AggregateInfo is created as follows.
   * - Grouping exprs:
   *   For each expr in 'groupingExprs' a CASE expr is crafted that selects the
   *   appropriate slot to group on based on which tuple is non-NULL in an input row.
   * - Aggregate exprs:
   *   For each aggregate expr in all aggregate infos, create an AGGIF() expr that
   *   selects the value of a single aggregate expr of a single aggregation class.
   *   For input rows that do not belong to the targeted aggregation class the AGGIF()
   *   function is a no-op.
   *
   * Example: SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) GROUP BY c, d
   *
   * AggregateInfo 0:
   * - Result tuple id 0 having 3 slots with ids 0, 1, 2 corresponding to c, d, and a
   * AggregateInfo 1:
   * - Result tuple id 1 having 3 slots with ids 3, 4, 5 corresponding to c, d, and b
   *
   * Transposition AggregateInfo:
   *
   * Grouping exprs:
   *   CASE valid_tid()
   *     WHEN 0 THEN SlotRef<slot id 0>
   *     WHEN 1 THEN SlotRef<slot id 3>
   *   END,
   *   CASE valid_tid()
   *     WHEN 0 THEN SlotRef<slot id 1>
   *     WHEN 1 THEN SlotRef<slot id 4>
   *   END,
   *
   * Aggregate exprs:
   * AGGIF(valid_tid() = 0, SlotRef<slot id 2>),
   * AGGIF(valid_tid() = 1, SlotRef<slot id 5>)
   *
   * The exprs above use an internal valid_tid() expr that returns the tuple id of the
   * non-NULL tuple in the input row. See ValidTupleIdExpr.java
   */
  private AggregateInfo createTransposeAggInfo(List<Expr> groupingExprs,
      List<AggregateInfo> aggInfos, Analyzer analyzer) throws AnalysisException {
    List<TupleId> aggTids = new ArrayList<>();
    for (AggregateInfo aggInfo : aggInfos) aggTids.add(aggInfo.getResultTupleId());
    List<FunctionCallExpr> transAggExprs = new ArrayList<>();
    for (AggregateInfo aggInfo : aggInfos) {
      TupleDescriptor aggTuple = aggInfo.getResultTupleDesc();
      int numGroupingExprs = groupingExprs.size();
      for (int i = numGroupingExprs; i < aggTuple.getSlots().size(); ++i) {
        // AGGIF(valid_tid() == aggTid, agg_slot)
        BinaryPredicate tidCmp =
            new BinaryPredicate(Operator.EQ, new ValidTupleIdExpr(aggTids),
                new NumericLiteral(new BigDecimal(aggTuple.getId().asInt())));
        SlotRef matchSlotRef = new SlotRef(aggTuple.getSlots().get(i));
        List<Expr> aggFnParams = Lists.<Expr>newArrayList(tidCmp, matchSlotRef);
        FunctionCallExpr aggExpr = new FunctionCallExpr("aggif", aggFnParams);
        aggExpr.analyze(analyzer);
        transAggExprs.add(aggExpr);
      }
    }

    // Create grouping exprs for the transposing agg.
    List<Expr> transGroupingExprs =
        getTransposeGroupingExprs(groupingExprs, aggInfos, analyzer);
    AggregateInfo result =
        AggregateInfo.create(transGroupingExprs, transAggExprs, analyzer);
    return result;
  }

  /**
   * Wraps the given groupingExprs into a CASE that switches on the valid tuple id.
   */
  private List<Expr> getTransposeGroupingExprs(
      List<Expr> groupingExprs, List<AggregateInfo> aggInfos, Analyzer analyzer) {
    List<TupleId> resultTids = new ArrayList<>();
    for (AggregateInfo aggInfo : aggInfos) resultTids.add(aggInfo.getResultTupleId());
    List<Expr> result = new ArrayList<>();
    for (int i = 0; i < groupingExprs.size(); ++i) {
      List<CaseWhenClause> caseWhenClauses = new ArrayList<>();
      for (AggregateInfo aggInfo : aggInfos) {
        TupleDescriptor tupleDesc = aggInfo.getResultTupleDesc();
        Expr whenExpr = NumericLiteral.create(tupleDesc.getId().asInt());
        Expr thenExpr = new SlotRef(tupleDesc.getSlots().get(i));
        caseWhenClauses.add(new CaseWhenClause(whenExpr, thenExpr));
      }
      CaseExpr caseExpr =
          new CaseExpr(new ValidTupleIdExpr(resultTids), caseWhenClauses, null);
      caseExpr.analyzeNoThrow(analyzer);
      result.add(caseExpr);
    }
    return result;
  }

  /**
   * Determines which aggregate exprs are required to produce the final query result.
   * Eliminates irrelevant aggregation classes and simplifies the aggregation plan,
   * if possible. Calls materializeRequiredSlots() on the agg infos of all surviving
   * classes.
   * If as a result of simplification the transposition step becomes unnecessary,
   * populates 'simplifiedAggSmap_' to remap exprs referencing the transposition to the
   * output of the new simplified aggregation.
   */
  public void materializeRequiredSlots(Analyzer analyzer, ExprSubstitutionMap smap) {
    if (aggInfos_.size() == 1) {
      aggInfos_.get(0).materializeRequiredSlots(analyzer, smap);
      materializedAggInfos_ = Lists.newArrayList(aggInfos_.get(0));
      return;
    }

    // Perform slot materialization of the transposition aggregation first to determine
    // which aggregation classes are required to produce the final output.
    Preconditions.checkNotNull(transposeAggInfo_);
    transposeAggInfo_.materializeRequiredSlots(analyzer, smap);

    // Determine which aggregation classes are required based on which slots
    // are materialized in the transposition aggregation.
    List<Integer> classIdxByAggExprIdx =
        Lists.newArrayListWithCapacity(aggClasses_.size());
    for (int i = 0; i < aggClasses_.size(); ++i) {
      classIdxByAggExprIdx.addAll(Collections.nCopies(aggClasses_.get(i).size(), i));
    }
    List<Integer> materializedClassIdxs =
        Lists.newArrayListWithCapacity(aggClasses_.size());
    materializedAggInfos_ = Lists.newArrayListWithCapacity(aggClasses_.size());
    for (Integer aggExprIdx : transposeAggInfo_.getMaterializedSlots()) {
      Integer classIdx = classIdxByAggExprIdx.get(aggExprIdx);
      int size = materializedClassIdxs.size();
      if (size != 0 && materializedClassIdxs.get(size - 1) == classIdx) continue;
      materializedClassIdxs.add(classIdx);
      aggInfos_.get(classIdx).materializeRequiredSlots(analyzer, smap);
      materializedAggInfos_.add(aggInfos_.get(classIdx));
    }

    if (materializedAggInfos_.size() == 2
        && (!materializedAggInfos_.get(0).isDistinctAgg()
               || !materializedAggInfos_.get(1).isDistinctAgg())) {
      // There are two remaining aggregation classes: one distinct and one non-distinct,
      // Coalesce them into a single aggregation class.
      List<FunctionCallExpr> newAggExprs = new ArrayList<>();
      for (Integer classIdx : materializedClassIdxs) {
        List<FunctionCallExpr> aggClass = aggClasses_.get(classIdx);
        AggregateInfo aggInfo = aggInfos_.get(classIdx);
        if (aggInfo.getSecondPhaseDistinctAggInfo() != null) {
          aggInfo = aggInfo.getSecondPhaseDistinctAggInfo();
        }
        Preconditions.checkState(aggInfo.getAggregateExprs().size() == aggClass.size());
        for (Integer aggExprIdx : aggInfo.getMaterializedSlots()) {
          newAggExprs.add(aggClass.get(aggExprIdx));
        }
      }

      AggregateInfo newAggInfo = null;
      try {
        newAggInfo = AggregateInfo.create(groupingExprs_, newAggExprs, analyzer);
      } catch (Exception e) {
        // Should never happen because the initial analysis should guarantee success.
        throw new IllegalStateException("Failed to simplify aggregation", e);
      }
      // Based on our previous analysis we know that all aggregate exprs this new
      // aggregation must be materialized.
      newAggInfo.getResultTupleDesc().materializeSlots();
      newAggInfo.materializeRequiredSlots(analyzer, smap);

      materializedAggInfos_.clear();
      materializedAggInfos_.add(newAggInfo);
    }

    if (materializedAggInfos_.size() == 0) {
      // To produce the correct number of result rows we still perform an empty
      // aggregation using the last aggregation class. If any, the non-distinct agg
      // class comes last in the 'aggClasses_' list so we prefer the simplest
      // aggregation plan.
      AggregateInfo lastAggInfo = aggInfos_.get(aggInfos_.size() - 1);
      // Materialize group by slots, if any.
      lastAggInfo.materializeRequiredSlots(analyzer, smap);
      materializedAggInfos_.add(lastAggInfo);
    }

    if (materializedAggInfos_.size() == 1) {
      // The aggregation has been reduced to a single class so no transposition step is
      // required. We need to substitute SlotRefs into the transposition aggregation
      // tuple with SlotRefs into the tuple of the single remaining aggregation class.
      simplifiedAggSmap_ = createSimplifiedAggSmap(
          groupingExprs_, transposeAggInfo_, materializedAggInfos_.get(0));
    } else if (materializedAggInfos_.size() != aggInfos_.size()) {
      // Recompute grouping exprs since they contain references to grouping slots
      // belonging to non-materialized agg classes.
      List<Expr> newGroupingExprs =
          getTransposeGroupingExprs(groupingExprs_, materializedAggInfos_, analyzer);
      // Validate the new grouping exprs.
      List<Expr> groupingExprs = transposeAggInfo_.getGroupingExprs();
      Preconditions.checkState(groupingExprs.size() == newGroupingExprs.size());
      for (int i = 0; i < groupingExprs.size(); ++i) {
        Type newType = newGroupingExprs.get(i).getType();
        Type oldType = groupingExprs.get(i).getType();
        Preconditions.checkState(newType.equals(oldType));
      }
      // Replace existing grouping exprs with new ones.
      groupingExprs.clear();
      groupingExprs.addAll(newGroupingExprs);
    }
  }

  /**
   * Returns a new substitution map from slots in the transposition result tuple to
   * slots in the tuple of the simplified aggregation.
   */
  private ExprSubstitutionMap createSimplifiedAggSmap(List<Expr> groupingExprs,
      AggregateInfo transposeAggInfo, AggregateInfo simplifiedAggInfo) {
    TupleDescriptor transTupleDesc = transposeAggInfo.getResultTupleDesc();
    TupleDescriptor matAggClassTupleDesc = simplifiedAggInfo.getResultTupleDesc();
    List<Expr> lhsExprs = new ArrayList<>();
    List<Expr> rhsExprs = new ArrayList<>();
    int numGroupingExprs = groupingExprs.size();
    for (int i = 0; i < numGroupingExprs; ++i) {
      lhsExprs.add(new SlotRef(transTupleDesc.getSlots().get(i)));
      rhsExprs.add(new SlotRef(matAggClassTupleDesc.getSlots().get(i)));
    }
    for (Integer idx : transposeAggInfo.getMaterializedSlots()) {
      lhsExprs.add(new SlotRef(transTupleDesc.getSlots().get(numGroupingExprs + idx)));
    }
    if (simplifiedAggInfo.getSecondPhaseDistinctAggInfo() != null) {
      simplifiedAggInfo = simplifiedAggInfo.getSecondPhaseDistinctAggInfo();
    }
    for (Integer idx : simplifiedAggInfo.getMaterializedSlots()) {
      rhsExprs.add(
          new SlotRef(matAggClassTupleDesc.getSlots().get(numGroupingExprs + idx)));
    }
    Preconditions.checkState(lhsExprs.size() == rhsExprs.size());
    return new ExprSubstitutionMap(lhsExprs, rhsExprs);
  }

  public void substitute(ExprSubstitutionMap smap, Analyzer analyzer)
      throws InternalException {
    Preconditions.checkState(substGroupingExprs_ == null);
    substGroupingExprs_ = Expr.substituteList(groupingExprs_, smap, analyzer, true);
  }

  /**
   * Returns the list of aggregate infos corresponding to all materialized aggregation
   * classes for the given aggregation phase.
   */
  public List<AggregateInfo> getMaterializedAggInfos(AggPhase aggPhase) {
    Preconditions.checkNotNull(materializedAggInfos_);
    List<AggregateInfo> result = new ArrayList<>();
    switch (aggPhase) {
      case FIRST: {
        result.addAll(materializedAggInfos_);
        break;
      }
      case FIRST_MERGE: {
        result.addAll(getMergeAggInfos(materializedAggInfos_));
        break;
      }
      case SECOND: {
        for (AggregateInfo aggInfo : materializedAggInfos_) {
          if (aggInfo.getSecondPhaseDistinctAggInfo() != null) {
            result.add(aggInfo.getSecondPhaseDistinctAggInfo());
          } else {
            result.add(aggInfo.getMergeAggInfo());
          }
        }
        break;
      }
      case SECOND_MERGE: {
        result.addAll(getMergeAggInfos(getMaterializedAggInfos(AggPhase.SECOND)));
        break;
      }
      case TRANSPOSE: {
        result.add(Preconditions.checkNotNull(transposeAggInfo_));
        break;
      }
    }
    return result;
  }

  private List<AggregateInfo> getMergeAggInfos(List<AggregateInfo> aggInfos) {
    List<AggregateInfo> result = new ArrayList<>();
    for (AggregateInfo aggInfo : aggInfos) {
      if (!aggInfo.isMerge()) {
        result.add(Preconditions.checkNotNull(aggInfo.getMergeAggInfo()));
      } else {
        // Ok to keep merging since merges are idempotent.
        result.add(aggInfo);
      }
    }
    return result;
  }

  public boolean hasFirstPhase() {
    Preconditions.checkNotNull(materializedAggInfos_);
    return materializedAggInfos_.size() > 0;
  }

  public boolean hasSecondPhase() {
    Preconditions.checkNotNull(materializedAggInfos_);
    for (AggregateInfo aggInfo : materializedAggInfos_) {
      if (aggInfo.isDistinctAgg()) return true;
    }
    return false;
  }

  public boolean hasTransposePhase() {
    Preconditions.checkNotNull(materializedAggInfos_);
    if (materializedAggInfos_.size() > 1) {
      Preconditions.checkNotNull(transposeAggInfo_);
      return true;
    }
    return false;
  }

  /**
   * Return true if all materialized aggregate expressions have distinct semantics.
   */
  public boolean hasAllDistinctAgg() {
    for (AggregateInfo ai : materializedAggInfos_) {
      if (!ai.hasAllDistinctAgg()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns all unassigned, bound, and equivalence conjuncts that should be evaluated
   * against the aggregation output. The conjuncts are bound by the result tuple id, as
   * returned by getResultTupleId().
   * If 'markAssigned' is true the returned conjuncts are marked as assigned.
   */
  public List<Expr> collectConjuncts(Analyzer analyzer, boolean markAssigned) {
    List<Expr> result = new ArrayList<>();
    TupleId tid = getResultTupleId();
    TupleDescriptor tupleDesc = analyzer.getTupleDesc(tid);
    // Ignore predicates bound by a grouping slot produced by a SlotRef grouping expr.
    // Those predicates are already evaluated below this agg node (e.g., in a scan),
    // because the grouping slot must be in the same equivalence class as another slot
    // below this agg node. We must not ignore other grouping slots in order to retain
    // conjuncts bound by those grouping slots in createEquivConjuncts() (IMPALA-2089).
    // Those conjuncts cannot be redundant because our equivalence classes do not
    // capture dependencies with non-SlotRef exprs.
    Set<SlotId> groupBySids = new HashSet<>();
    for (int i = 0; i < groupingExprs_.size(); ++i) {
      if (groupingExprs_.get(i).unwrapSlotRef(true) == null) continue;
      groupBySids.add(tupleDesc.getSlots().get(i).getId());
    }
    // Binding predicates are assigned to the final output tuple of the aggregation,
    // which is the tuple of the 2nd phase agg for distinct aggs.
    List<Expr> bindingPredicates =
        analyzer.getBoundPredicates(tid, groupBySids, markAssigned);
    List<Expr> unassignedConjuncts = analyzer.getUnassignedConjuncts(tid.asList());
    if (markAssigned) analyzer.markConjunctsAssigned(unassignedConjuncts);
    result.addAll(bindingPredicates);
    result.addAll(unassignedConjuncts);
    analyzer.createEquivConjuncts(tid, result, groupBySids);
    Expr.removeDuplicates(result);
    return result;
  }

  /**
   * Returns the aggregation phase that produces the result tuple id and should evaluate
   * all conjuncts in the single-node plan.
   */
  public AggPhase getConjunctAssignmentPhase() {
    if (transposeAggInfo_ != null) return AggPhase.TRANSPOSE;
    for (AggregateInfo aggInfo : aggInfos_) {
      if (aggInfo.isDistinctAgg()) return AggPhase.SECOND;
    }
    Preconditions.checkState(aggInfos_.size() == 1);
    return AggPhase.FIRST;
  }

  public List<AggregateInfo> getAggClasses() { return aggInfos_; }
  public AggregateInfo getAggClass(int i) { return aggInfos_.get(i); }
  public AggregateInfo getTransposeAggInfo() { return transposeAggInfo_; }
  public boolean hasGrouping() { return !groupingExprs_.isEmpty(); }
  public List<Expr> getGroupingExprs() { return groupingExprs_; }
  public List<Expr> getSubstGroupingExprs() { return substGroupingExprs_; }
  public List<FunctionCallExpr> getAggExprs() { return aggExprs_; }
  public ExprSubstitutionMap getOutputSmap() { return outputSmap_; }
  public List<AggregateInfo> getMaterializedAggClasses() { return materializedAggInfos_; }
  public AggregateInfo getMaterializedAggClass(int i) {
    return materializedAggInfos_.get(i);
  }
  public ExprSubstitutionMap getSimplifiedAggSmap() { return simplifiedAggSmap_; }
  public boolean hasAggregateExprs() { return !aggExprs_.isEmpty(); }

  public TupleId getResultTupleId() {
    if (transposeAggInfo_ != null) return transposeAggInfo_.getResultTupleId();
    Preconditions.checkState(aggInfos_.size() == 1);
    return getAggClass(0).getResultTupleId();
  }

  @Override
  public MultiAggregateInfo clone() { return new MultiAggregateInfo(this); }
}
