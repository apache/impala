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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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
  private final static Logger LOG = LoggerFactory.getLogger(MultiAggregateInfo.class);

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

  // Indicates if this MultiAggregateInfo is associated with grouping sets.
  private boolean isGroupingSet_;

  // Grouping sets provided in the constructor. Null if 'isGroupingSet_' is false or if
  // the list of grouping sets will be provided later in analyzeCustomClasses(). Each
  // list must have the same number of expressions, and the expression types must match.
  // TODO: could generalise this to allow omitting NULL expressions.
  private List<List<Expr>> groupingSets_;

  public MultiAggregateInfo(List<Expr> groupingExprs, List<FunctionCallExpr> aggExprs,
      List<List<Expr>> groupingSets) {
    groupingExprs_ = Expr.cloneList(Preconditions.checkNotNull(groupingExprs));
    aggExprs_ = Expr.cloneList(Preconditions.checkNotNull(aggExprs));
    groupingSets_ = groupingSets == null ? null : Expr.deepCopy(groupingSets);
    isGroupingSet_ = groupingSets != null;
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
    isGroupingSet_ = other.isGroupingSet_;
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

    if (groupingSets_ != null) {
      analyzeGroupingSets(analyzer);
      return;
    }

    isAnalyzed_ = true;

    // Group the agg exprs by their DISTINCT exprs and move the non-distinct agg exprs
    // into a separate list.
    // List of all DISTINCT expr lists and the grouped agg exprs.
    // distinctExprs[i] corresponds to groupedDistinctAggExprs[i]
    // grouping() and grouping_id() expressions will be replaced by constants so are
    // not included in either list.
    List<List<Expr>> distinctExprs = new ArrayList<>();
    List<List<FunctionCallExpr>> groupedDistinctAggExprs = new ArrayList<>();
    List<FunctionCallExpr> nonDistinctAggExprs = new ArrayList<>();
    List<FunctionCallExpr> groupingBuiltinExprs = new ArrayList<>();
    for (FunctionCallExpr aggExpr : aggExprs_) {
      if (aggExpr.isGroupingBuiltin() || aggExpr.isGroupingIdBuiltin()) {
        if (!hasGrouping()) {
          throw new AnalysisException("grouping() or grouping_id() function requires a " +
              "GROUP BY clause: '" + aggExpr.toSql() + "'");
        }
        groupingBuiltinExprs.add(aggExpr);
      } else if (aggExpr.isDistinct()) {
        List<Expr> children = AggregateFunction.getCanonicalDistinctAggChildren(aggExpr);

        // Complex types are not supported as DISTINCT parameters of aggregate functions.
        checkComplexDistinctParams(aggExpr, children);

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

    // Set up outputSmap_, which maps from the original grouping and aggregate exprs in
    // all aggregation classes to the output of the final aggregation.
    outputSmap_ = aggInfos_.size() == 1 ?
        aggInfos_.get(0).getResultSmap().clone() : new ExprSubstitutionMap();
    if (groupingBuiltinExprs.size() > 0) {
      // grouping() and grouping_id() must be replaced with constants.
      for (FunctionCallExpr aggExpr : groupingBuiltinExprs) {
        outputSmap_.put(aggExpr.clone(),
            getGroupingId(aggExpr, aggInfos_.get(0), groupingExprs_));
      }
    }

    if (aggInfos_.size() == 1) {
      // Only a single aggregation class, no transposition step is needed.
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

    // Set up outputSmap_ to map from the original grouping and aggregate exprs in
    // all aggregation classes to the final output produced by the transposing
    // aggregation.
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

  private static void checkComplexDistinctParams(FunctionCallExpr aggExpr,
      List<Expr> params) throws AnalysisException {
    for (Expr child : params) {
      if (child.getType().isComplexType()) {
        throw new AnalysisException("Complex types are not supported " +
            "as DISTINCT parameters of aggregate functions. Distinct parameter: '" +
            child.toSql() + "', type: '" + child.getType().toSql() +
            "' in aggregate function '" + aggExpr.toSql() + "'.");
      }
    }
  }

  /**
   * Implementation of analyze() for aggregation with grouping sets.
   * Does not handle distinct aggregate functions yet.
   *
   * @throws AnalysisException if a distinct aggregation function is present or any other
   *    analysis error occurs.
   */
  private void analyzeGroupingSets(Analyzer analyzer) throws AnalysisException {
    // Regular aggregate expressions that are evaluated over input rows.
    List<FunctionCallExpr> inputAggExprs = new ArrayList<>();
    // grouping() and grouping_id() expressions that are evaluated during transpose.
    for (FunctionCallExpr aggExpr : aggExprs_) {
      if (aggExpr.isDistinct()) {
        // We can't handle this now - it would require enumerating more aggregation
        // classes.
        throw new AnalysisException("Distinct aggregate functions and grouping sets " +
            "are not supported in the same query block.");
      }
      if (!aggExpr.isGroupingBuiltin() && !aggExpr.isGroupingIdBuiltin()) {
        inputAggExprs.add(aggExpr);
      }
    }

    // Enumerate the aggregate classes that need to be evaluated before the final
    // transposition.
    List<List<FunctionCallExpr>> aggClasses = new ArrayList<>(groupingSets_.size());
    List<AggregateInfo> aggInfos = new ArrayList<>(groupingSets_.size());
    for (List<Expr> groupingSet : groupingSets_) {
      aggClasses.add(inputAggExprs);
      aggInfos.add(AggregateInfo.create(groupingSet, inputAggExprs, analyzer));
    }
    // analyzeCustomClasses() will do the rest of the analysis and set 'isAnalyzed_'
    analyzeCustomClasses(analyzer, aggClasses, aggInfos);
  }

  /**
   * Version of analyze method that accepts list of aggregation class and aggregation
   * info. This is needed for supporting grouping sets/rollup functionality. Does not
   * handle distinct aggregate functions.
   */
  public void analyzeCustomClasses(Analyzer analyzer,
      List<List<FunctionCallExpr>> aggClasses,
      List<AggregateInfo> aggInfos) throws AnalysisException {
    if (isAnalyzed_) return;
    isAnalyzed_ = true;

    if (aggClasses.size() == 0) return;
    Preconditions.checkState(isGroupingSet_);
    Preconditions.checkState(aggClasses.size() == aggInfos.size());

    aggClasses_ = new ArrayList<>();
    aggInfos_ = new ArrayList<>();

    aggClasses_.addAll(aggClasses);
    aggInfos_.addAll(aggInfos);

    if (aggInfos_.size() == 1) {
      // Only a single aggregation class, no transposition step is needed.
      // Grouping builtins like grouping_id() would otherwise be handled in transpose agg.
      // With only one agg class, we can replace them with constants.
      for (FunctionCallExpr aggExpr : aggExprs_) {
        if (aggExpr.isGroupingBuiltin() || aggExpr.isGroupingIdBuiltin()) {
          if (outputSmap_ == null) outputSmap_ = aggInfos_.get(0).getResultSmap().clone();
          outputSmap_.put(aggExpr.clone(),
              getGroupingId(aggExpr, aggInfos_.get(0), groupingExprs_));
        }
      }
      if (outputSmap_ == null) outputSmap_ = aggInfos_.get(0).getResultSmap();
      return;
    }

    // Create agg info for the final transposition step.
    transposeAggInfo_ = createTransposeAggInfoForGroupingSet(groupingExprs_,
        aggExprs_, aggInfos_, analyzer);
    List<SlotDescriptor> outputSlots = transposeAggInfo_.getResultTupleDesc().getSlots();
    Preconditions.checkState(groupingExprs_.size() <= outputSlots.size());

    // Maps from the original grouping and aggregate exprs in all aggregation classes to
    // the final output produced by the transposing aggregation.
    outputSmap_ = new ExprSubstitutionMap();
    // Add mappings for grouping exprs.
    for (int i = 0; i < groupingExprs_.size(); ++i) {
      outputSmap_.put(groupingExprs_.get(i).clone(), new SlotRef(outputSlots.get(i)));
    }
    // Add mappings for aggregate functions.
    int outputSlotIdx = groupingExprs_.size() + 1; // add 1 to account for the tid column
    for (int i = 0; i < aggExprs_.size(); i++) {
      outputSmap_.put(aggExprs_.get(i).clone(),
        new SlotRef(outputSlots.get(outputSlotIdx)));
      outputSlotIdx++;
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
   * Returns a new AggregateInfo for transposing the union of results of the given input
   * AggregateInfos, each of which may belong to a different grouping set.
   *
   * Example:
   *  SELECT a1, b1, SUM(c1), MIN(d1) FROM t1 GROUP BY ROLLUP(a1, b1)
   *
   * For example, the above statement results in the following 3 Grouping Sets:
   * {(a1, b1), (a1), ()}
   * We will map these to the following aggregation classes:
   * Class 1:
   *  Aggregate output exprs: SUM(c1), MIN(d1)
   *  Grouping exprs: a1, b1
   * Class 2:
   *  Aggregate output exprs: SUM(c1), MIN(d1)
   *  Grouping exprs: a1, NULL
   * Class 3:
   *  Aggregate output exprs: SUM(c1), MIN(d1)
   *  Grouping exprs: NULL, NULL
   *
   * Note that all classes have the same aggregate output exprs but different
   * grouping exprs.
   * This is different from the multiple count distinct case. Also, the transpose phase is
   * different as described below.
   * The Transpose phase chooses the appropriate aggregate slots and grouping slots from
   * each aggregation class based on the valid tuple id:
   *   Aggregate output exprs:
   *     AGGIF(valid_tid(1, 2, 3) IN (1, 2, 3),
   *            CASE valid_tid(1, 2, 3)
   *              WHEN 1 THEN SUM(c1)        // these agg exprs are same but point to
   *              WHEN 2 THEN SUM(c1)        // different materialized slots
   *              WHEN 3 THEN SUM(c1)
   *            END),
   *     AGGIF(valid_tid(1, 2, 3) IN (1, 2, 3),
   *                CASE valid_tid(1, 2, 3)
   *                  WHEN 1 THEN MIN(d1)
   *                  WHEN 2 THEN MIN(d1)
   *                  WHEN 3 THEN MIN(d1)
   *                END)
   *  Grouping output exprs:
   *    CASE valid_tid(1, 2, 3)
   *      WHEN 1 THEN a1                      // these grouping exprs may be same but
   *      WHEN 2 THEN a1                      // point to different materialized slots
   *      WHEN 3 THEN NULL
   *    END,
   *    CASE valid_tid(1, 2, 3)
   *      WHEN 1 THEN b1
   *      WHEN 2 THEN NULL
   *      WHEN 3 THEN NULL
   *    END
   *
   *    Notes:
   *      - This method handles grouping() and grouping_id() aggregate functions
   *        by generating CASE expressions with the same structure as above that
   *        return the appropriate grouping()/grouping_id() value for the agg class.
   *      - The max number of grouping exprs allowed per grouping set is 64
   *      - For the transpose phase grouping exprs we add the TupleId also - this
   *        ensures that NULL values in the data are differentiated from NULLs in the
   *        grouping set.
   */
  private AggregateInfo createTransposeAggInfoForGroupingSet(
      List<Expr> groupingExprs, List<FunctionCallExpr> aggExprs,
      List<AggregateInfo> aggInfos, Analyzer analyzer) throws AnalysisException {
    List<TupleId> aggTids = new ArrayList<>();
    for (AggregateInfo aggInfo : aggInfos) {
      // we use a bigint type for the grouping_id later, so we allow up to
      // max 64 grouping exprs in a grouping set
      Preconditions.checkState(aggInfo.getGroupingExprs().size() <= 64,
          "Exceeded the limit of 64 grouping exprs in a grouping set");
      aggTids.add(aggInfo.getResultTupleId());
    }
    List<FunctionCallExpr> transAggExprs = new ArrayList<>();

    int numGroupingExprs = groupingExprs.size();
    int numSlots = numGroupingExprs + aggExprs.size();
    List<Expr> inList = new ArrayList<>();
    for (TupleId tid : aggTids) {
      inList.add(NumericLiteral.create(tid.asInt()));
    }
    InPredicate tidInPred =
      new InPredicate(new ValidTupleIdExpr(aggTids), inList, false);

    List<CaseWhenClause> caseWhenClauses = new ArrayList<>();

    // Create aggregate exprs for the transposing agg.
    int inputAggSlotIdx = numGroupingExprs;
    for (int i = 0; i < aggExprs.size(); ++i) {
      caseWhenClauses.clear();
      FunctionCallExpr aggExpr = aggExprs.get(i);
      boolean isGroupingFn = aggExpr.isGroupingBuiltin() || aggExpr.isGroupingIdBuiltin();
      if (isGroupingFn) {
        for (Expr childExpr : aggExpr.getChildren()) {
          if (!groupingExprs.contains(childExpr)) {
            throw new AnalysisException(childExpr.toSql() + " is not a grouping " +
                "expression in " + aggExpr.toSql());
          }
        }
      }
      for (int j = 0; j < aggInfos.size(); ++j) {
        AggregateInfo aggInfo = aggInfos.get(j);
        TupleDescriptor aggTuple = aggInfo.getResultTupleDesc();
        Expr whenExpr = NumericLiteral.create(aggTuple.getId().asInt());
        Expr thenExpr;
        if (isGroupingFn) {
          thenExpr = getGroupingId(aggExpr, aggInfo, groupingExprs);
        } else {
          Preconditions.checkState(inputAggSlotIdx < aggTuple.getSlots().size(),
              inputAggSlotIdx + " >= " + aggTuple.getSlots().size() + ": " +
              aggTuple.debugString());
          thenExpr = new SlotRef(aggTuple.getSlots().get(inputAggSlotIdx));
        }
        caseWhenClauses.add(new CaseWhenClause(whenExpr, thenExpr));
      }
      CaseExpr caseExpr =
        new CaseExpr(new ValidTupleIdExpr(aggTids), caseWhenClauses, null);
      caseExpr.analyzeNoThrow(analyzer);
      List<Expr> aggFnParams = Lists.<Expr>newArrayList(tidInPred, caseExpr);
      FunctionCallExpr transAggExpr = new FunctionCallExpr("aggif", aggFnParams);
      transAggExpr.analyzeNoThrow(analyzer);
      transAggExprs.add(transAggExpr);
      if (!isGroupingFn) ++inputAggSlotIdx;
    }

    // Create grouping exprs for the transposing agg.
    List<Expr> transGroupingExprs = new ArrayList<>();
    for (int gbIndex = 0; gbIndex < groupingExprs.size(); ++gbIndex) {
      caseWhenClauses.clear();
      for (AggregateInfo aggInfo : aggInfos) {
        // for a particular aggInfo, we only need to consider the group-by
        // exprs relevant to that aggInfo
        TupleDescriptor aggTuple = aggInfo.getResultTupleDesc();
        Preconditions.checkState(gbIndex < aggTuple.getSlots().size(),
            groupingExprs.toString() + " " + aggTuple.debugString());
        Expr whenExpr = NumericLiteral.create(aggTuple.getId().asInt());
        if (aggInfo.getGroupingExprs().size() == 0) {
          Type nullType = groupingExprs.get(gbIndex).getType();
          NullLiteral nullLiteral = new NullLiteral();
          Expr thenExpr = nullLiteral.uncheckedCastTo(nullType);
          caseWhenClauses.add(new CaseWhenClause(whenExpr, thenExpr));
        } else {
          Expr thenExpr = new SlotRef(aggTuple.getSlots().get(gbIndex));
          caseWhenClauses.add(new CaseWhenClause(whenExpr, thenExpr));
        }
      }
      CaseExpr caseExpr =
        new CaseExpr(new ValidTupleIdExpr(aggTids), caseWhenClauses, null);
      caseExpr.analyzeNoThrow(analyzer);
      transGroupingExprs.add(caseExpr);
    }

    // add the tuple id of the aggregate class to the grouping exprs
    caseWhenClauses.clear();
    for (AggregateInfo aggInfo : aggInfos) {
      TupleDescriptor aggTuple = aggInfo.getResultTupleDesc();
      Expr whenExpr = NumericLiteral.create(aggTuple.getId().asInt(), Type.INT);
      Expr thenExpr = whenExpr;
      caseWhenClauses.add(new CaseWhenClause(whenExpr, thenExpr));
    }
    CaseExpr caseExprForTids =
      new CaseExpr(new ValidTupleIdExpr(aggTids), caseWhenClauses, null);
    caseExprForTids.analyzeNoThrow(analyzer);
    transGroupingExprs.add(caseExprForTids);

    AggregateInfo result =
        AggregateInfo.create(transGroupingExprs, transAggExprs, analyzer);
    return result;
  }

  /**
   * Return the value for grouping() or grouping_id() for a particular aggregation class.
   * @param aggExpr the grouping() or grouping_id() function
   * @param aggInfo the aggregate info for the aggregation class.
   * @param groupingExprs all grouping expressions
   */
  private static NumericLiteral getGroupingId(FunctionCallExpr aggExpr,
      AggregateInfo aggInfo, List<Expr> groupingExprs) {
    if (aggExpr.isGroupingBuiltin()) {
      boolean inSet = aggInfo.getGroupingExprs().contains(aggExpr.getChild(0));
      // grouping(col) returns 1 if this is the aggregate across all values of
      // the grouping expression, i.e. if the expression is *not* one of the
      // grouping expressions for this aggregate class.
      return NumericLiteral.create(inSet ? 0 : 1, Type.TINYINT);
    } else {
      Preconditions.checkState(aggExpr.isGroupingIdBuiltin());
      // grouping_id() returns a bit vector of the groups exprs included in this
      // agg class. The last grouping expression is the least significant bit in
      // the returned value. The bit vector has 1 if this is the aggregate across
      // all values of the grouping expression, the same as for grouping().
      // The zero-argument version of grouping_id() generates the bit vector for all
      // the grouping expressions.
      List<Expr> inputExprs = aggExpr.getChildren().size() > 0 ?
          aggExpr.getChildren() : groupingExprs;
      long val = 0;
      for (Expr child : inputExprs) {
        val <<= 1;
        val |= aggInfo.getGroupingExprs().contains(child) ? 0 : 1;
      }
      return NumericLiteral.create(val, Type.BIGINT);
    }
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

  public void setIsGroupingSet(boolean isGroupingSet) { isGroupingSet_ = isGroupingSet; }

  public boolean getIsGroupingSet() { return isGroupingSet_; }

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

    // For grouping sets, materialize the slots from each aggregate info.
    // The rationale is that the final transposition phase needs the grouping
    // and agg expr slots from each grouping set. More investigation is needed
    // to see if we can optimize this by reducing the number of materializations
    // (similar to the multiple count(distinct) case).
    if (isGroupingSet_) {
      materializedAggInfos_ = new ArrayList<>();
      for (AggregateInfo aggInfo : aggInfos_) {
        aggInfo.materializeRequiredSlots(analyzer, smap);
      }
      materializedAggInfos_.addAll(aggInfos_);
      return;
    }

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
