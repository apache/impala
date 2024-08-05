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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.apache.impala.analysis.AggregateInfoBase;
import org.apache.impala.analysis.AnalyticExpr;
import org.apache.impala.analysis.AnalyticInfo;
import org.apache.impala.analysis.AnalyticWindow;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.OrderByElement;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.SortInfo;
import org.apache.impala.analysis.ToSqlOptions;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.analysis.TupleIsNullPredicate;
import org.apache.impala.catalog.Function;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TSortingOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;


/**
 * The analytic planner adds plan nodes to an existing plan tree in order to
 * implement the AnalyticInfo of a given query stmt. The resulting plan reflects
 * similarities among analytic exprs with respect to partitioning, ordering and
 * windowing to reduce data exchanges and sorts (the exchanges and sorts are currently
 * not minimal). The generated plan has the following structure:
 * ...
 * (
 *  (
 *    (
 *      analytic node  <-- group of analytic exprs with compatible window
 *    )+               <-- group of analytic exprs with compatible ordering
 *    sort node?
 *  )+                 <-- group of analytic exprs with compatible partitioning
 *  hash exchange?
 * )*                  <-- group of analytic exprs that have different partitioning
 * input plan node
 * ...
 */
public class AnalyticPlanner {
  private final static Logger LOG = LoggerFactory.getLogger(AnalyticPlanner.class);

  private final AnalyticInfo analyticInfo_;
  private final Analyzer analyzer_;
  private final PlannerContext ctx_;

  public AnalyticPlanner(AnalyticInfo analyticInfo, Analyzer analyzer,
      PlannerContext ctx) {
    analyticInfo_ = analyticInfo;
    analyzer_ = analyzer;
    ctx_ = ctx;
  }

  /**
   * Return plan tree that augments 'root' with plan nodes that implement single-node
   * evaluation of the AnalyticExprs in analyticInfo.
   * This plan takes into account a possible hash partition of its input on
   * 'groupingExprs'; if this is non-null, it returns in 'inputPartitionExprs'
   * a subset of the grouping exprs which should be used for the aggregate
   * hash partitioning during the parallelization of 'root'.
   * Any unassigned conjuncts from 'analyzer_' are applied after analytic functions are
   * evaluated.
   * TODO: when generating sort orders for the sort groups, optimize the ordering
   * of the partition exprs (so that subsequent sort operations see the input sorted
   * on a prefix of their required sort exprs)
   * TODO: when merging sort groups, recognize equivalent exprs
   * (using the equivalence classes) rather than looking for expr equality
   */
  public PlanNode createSingleNodePlan(PlanNode root,
      List<Expr> groupingExprs, List<Expr> inputPartitionExprs) throws ImpalaException {
    // If the plan node is not an EmptySetNode, identify predicates that reference the
    // logical analytic tuple (this logical analytic tuple is replaced by different
    // physical ones during planning)
    List<TupleId> tids = new ArrayList<>();
    if (!(root instanceof EmptySetNode)) {
      tids.addAll(root.getTupleIds());
    }
    tids.add(analyticInfo_.getOutputTupleId());
    List<Expr> analyticConjs = analyzer_.getUnassignedConjuncts(tids);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Analytic conjuncts: " +
            Expr.listToSql(analyticConjs, ToSqlOptions.SHOW_IMPLICIT_CASTS));
    }

    List<PartitionLimit> perPartitionLimits = inferPartitionLimits(analyticConjs);
    List<WindowGroup> windowGroups = collectWindowGroups();
    for (int i = 0; i < windowGroups.size(); ++i) {
      windowGroups.get(i).init(analyzer_, "wg-" + i);
    }
    List<SortGroup> sortGroups = collectSortGroups(windowGroups);
    mergeSortGroups(sortGroups);
    for (SortGroup g: sortGroups) {
      g.init();
    }
    List<PartitionGroup> partitionGroups = collectPartitionGroups(sortGroups);
    mergePartitionGroups(partitionGroups, root.getNumInstances());
    orderGroups(partitionGroups);
    if (groupingExprs != null) {
      Preconditions.checkNotNull(inputPartitionExprs);
      computeInputPartitionExprs(
          partitionGroups, groupingExprs, root.getNumInstances(), inputPartitionExprs);
    }

    for (int i = 0; i < partitionGroups.size(); ++i) {
      PartitionGroup partitionGroup = partitionGroups.get(i);
      for (int j = 0; j < partitionGroup.sortGroups.size(); ++j) {
        boolean lastSortGroup = (i == partitionGroups.size() - 1) &&
                (j == partitionGroup.sortGroups.size() - 1);
        root = createSortGroupPlan(root, partitionGroup.sortGroups.get(j),
            j == 0 ? partitionGroup.partitionByExprs : null,
            lastSortGroup ? perPartitionLimits : null);
      }
    }

    List<Expr> substAnalyticConjs =
        Expr.substituteList(analyticConjs, root.getOutputSmap(), analyzer_, false);
    overrideSelectivityPushedLimits(analyticConjs, perPartitionLimits,
            substAnalyticConjs);
    return root.addConjunctsToNode(ctx_, analyzer_, tids, substAnalyticConjs);
  }

  /**
   * Update selectivity of conjuncts in 'substAnalyticConjs' to reflect those that
   * were pushed to a partitioned top-n.
   *
   * 'analyticConjs' and 'substAnalyticConjs' are the original conjuncts (matching
   * the conjuncts in 'perPartitionLimits' and the substituted conjuncts and
   * correspond to each other one to one.
   */
  private void overrideSelectivityPushedLimits(List<Expr> analyticConjs,
          List<PartitionLimit> perPartitionLimits, List<Expr> substAnalyticConjs) {
    for (PartitionLimit limit : perPartitionLimits) {
      if (limit.pushed && limit.isLessThan) {
        int idx = analyticConjs.indexOf(limit.conjunct);
        if (idx >= 0) {
          substAnalyticConjs.set(idx,
                  substAnalyticConjs.get(idx).cloneAndOverrideSelectivity(1.0));
        }
      }
    }
  }

  /**
   * Coalesce sort groups that have compatible partition-by exprs and
   * have a prefix relationship.
   */
  private void mergeSortGroups(List<SortGroup> sortGroups) {
    boolean hasMerged = false;
    do {
      hasMerged = false;
      for (SortGroup sg1: sortGroups) {
        for (SortGroup sg2: sortGroups) {
          if (sg1 != sg2 && sg1.isPrefixOf(sg2)) {
            sg1.absorb(sg2);
            sortGroups.remove(sg2);
            hasMerged = true;
            break;
          }
        }
        if (hasMerged) break;
      }
    } while (hasMerged);
  }

  /**
   * Coalesce partition groups for which the intersection of their
   * partition exprs has ndv estimate > numInstances, so that the resulting plan
   * still parallelizes across all instances.
   */
  private void mergePartitionGroups(
      List<PartitionGroup> partitionGroups, int numInstances) {
    boolean hasMerged = false;
    do {
      hasMerged = false;
      for (PartitionGroup pg1: partitionGroups) {
        for (PartitionGroup pg2: partitionGroups) {
          if (pg1 != pg2) {
            long ndv = Expr.getNumDistinctValues(
                Expr.intersect(pg1.partitionByExprs, pg2.partitionByExprs));
            if (ndv == -1 || ndv < 0 || ndv < numInstances) {
              // didn't get a usable value or the number of partitions is too small
              continue;
            }
            pg1.merge(pg2);
            partitionGroups.remove(pg2);
            hasMerged = true;
            break;
          }
        }
        if (hasMerged) break;
      }
    } while (hasMerged);
  }

  /**
   * Determine the partition group that has the maximum intersection in terms
   * of the estimated ndv of the partition exprs with groupingExprs.
   * That partition group is placed at the front of partitionGroups, with its
   * partition exprs reduced to the intersection, and the intersecting groupingExprs
   * are returned in inputPartitionExprs.
   */
  private void computeInputPartitionExprs(List<PartitionGroup> partitionGroups,
      List<Expr> groupingExprs, int numInstances, List<Expr> inputPartitionExprs) {
    inputPartitionExprs.clear();
    Preconditions.checkState(numInstances != -1);
    // find partition group with maximum intersection
    long maxNdv = 0;
    PartitionGroup maxPg = null;
    List<Expr> maxGroupingExprs = null;
    for (PartitionGroup pg: partitionGroups) {
      List<Expr> l1 = new ArrayList<>();
      List<Expr> l2 = new ArrayList<>();
      analyzer_.exprIntersect(pg.partitionByExprs, groupingExprs, l1, l2);
      // TODO: also look at l2 and take the max?
      long ndv = Expr.getNumDistinctValues(l1);
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format("Partition group: %s, intersection: %s. " +
                "GroupingExprs: %s, intersection: %s. ndv: %d, numInstances: %d, " +
                "maxNdv: %d.",
            Expr.debugString(pg.partitionByExprs), Expr.debugString(l1),
            Expr.debugString(groupingExprs), Expr.debugString(l2),
            ndv, numInstances, maxNdv));
      }
      if (ndv < 0 || ndv < numInstances || ndv < maxNdv) continue;
      // found a better partition group
      maxPg = pg;
      maxPg.partitionByExprs = l1;
      maxGroupingExprs = l2;
      maxNdv = ndv;
    }

    if (maxNdv > numInstances) {
      Preconditions.checkNotNull(maxPg);
      // we found a partition group that gives us enough parallelism;
      // move it to the front
      partitionGroups.remove(maxPg);
      partitionGroups.add(0, maxPg);
      inputPartitionExprs.addAll(maxGroupingExprs);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Optimized partition exprs: " + Expr.debugString(inputPartitionExprs));
      }
    }
  }

  /**
   * Order partition groups (and the sort groups within them) by increasing
   * totalOutputTupleSize. This minimizes the total volume of data that needs to be
   * repartitioned and sorted.
   * Also move the non-partitioning partition group to the end.
   */
  private void orderGroups(List<PartitionGroup> partitionGroups) {
    // remove the non-partitioning group from partitionGroups
    PartitionGroup nonPartitioning = null;
    for (PartitionGroup pg: partitionGroups) {
      if (Expr.allConstant(pg.partitionByExprs)) {
        nonPartitioning = pg;
        break;
      }
    }
    if (nonPartitioning != null) partitionGroups.remove(nonPartitioning);

    // order by ascending combined output tuple size
    Collections.sort(partitionGroups,
        new Comparator<PartitionGroup>() {
          @Override
          public int compare(PartitionGroup pg1, PartitionGroup pg2) {
            Preconditions.checkState(pg1.totalOutputTupleSize > 0);
            Preconditions.checkState(pg2.totalOutputTupleSize > 0);
            int diff = pg1.totalOutputTupleSize - pg2.totalOutputTupleSize;
            return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
          }
        });
    if (nonPartitioning != null) partitionGroups.add(nonPartitioning);

    for (PartitionGroup pg: partitionGroups) {
      pg.orderSortGroups();
    }
  }

  /**
   * Create SortInfo, including sort tuple, to sort entire input row
   * on sortExprs.
   */
  private SortInfo createSortInfo(PlanNode input, List<Expr> sortExprs,
      List<Boolean> isAsc, List<Boolean> nullsFirst) {
    return createSortInfo(input, sortExprs, isAsc, nullsFirst, TSortingOrder.LEXICAL);
  }

  /**
   * Same as above, but with extra parameter, sorting order.
   */
  private SortInfo createSortInfo(PlanNode input, List<Expr> sortExprs,
      List<Boolean> isAsc, List<Boolean> nullsFirst, TSortingOrder sortingOrder) {
    List<Expr> inputSlotRefs = new ArrayList<>();
    for (TupleId tid: input.getTupleIds()) {
      TupleDescriptor tupleDesc = analyzer_.getTupleDesc(tid);
      for (SlotDescriptor inputSlotDesc: tupleDesc.getSlots()) {
        // Only add fully materialized slot descriptors. It is possible that a slot desc
        // shows up that is materialized but not all of its children are, see
        // IMPALA-13272. This can happen for example if only a subset of the fields of a
        // struct is queried - the overall struct needs to be marked materialized to allow
        // the required children to be materialized where they are needed, but the struct
        // as a whole does not need to be added to the sorting tuple (or any other tuple)
        // - the required fields are added separately in their own right.
        if (inputSlotDesc.isFullyMaterialized()) {
          // Project out collection slots that are not supported in the sorting tuple
          // (collections within structs).
          if (SortInfo.isValidInSortingTuple(inputSlotDesc.getType())) {
            inputSlotRefs.add(new SlotRef(inputSlotDesc));
          } else {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Project out unsupported collection slot in " +
                  "sort tuple of analytic: slot={}", inputSlotDesc.debugString());
            }
          }
        }
      }
    }

    // The decision to materialize ordering exprs should be based on exprs that are
    // fully resolved against our input (IMPALA-5270).
    ExprSubstitutionMap inputSmap = input.getOutputSmap();
    List<Expr> resolvedSortExprs =
        Expr.substituteList(sortExprs, inputSmap, analyzer_, true);
    SortInfo sortInfo = new SortInfo(resolvedSortExprs, isAsc, nullsFirst,
        sortingOrder);
    sortInfo.createSortTupleInfo(inputSlotRefs, analyzer_);

    // Lhs exprs to be substituted in ancestor plan nodes could have a rhs that contains
    // TupleIsNullPredicates. TupleIsNullPredicates require specific tuple ids for
    // evaluation. Since this sort materializes a new tuple, it's impossible to evaluate
    // TupleIsNullPredicates referring to this sort's input after this sort,
    // To preserve the information whether an input tuple was null or not this sort node,
    // we materialize those rhs TupleIsNullPredicates, which are then substituted
    // by a SlotRef into the sort's tuple in ancestor nodes (IMPALA-1519).
    if (inputSmap != null) {
      List<TupleIsNullPredicate> tupleIsNullPreds = new ArrayList<>();
      for (Expr rhsExpr: inputSmap.getRhs()) {
        // Ignore substitutions that are irrelevant at this plan node and its ancestors.
        if (!rhsExpr.isBoundByTupleIds(input.getTupleIds())) continue;
        rhsExpr.collect(TupleIsNullPredicate.class, tupleIsNullPreds);
      }
      Expr.removeDuplicates(tupleIsNullPreds);
      sortInfo.addMaterializedExprs(tupleIsNullPreds, analyzer_);
    }
    sortInfo.getSortTupleDescriptor().materializeSlots();
    return sortInfo;
  }

  /**
   * Create plan tree for the entire sort group, including all contained window groups.
   * Marks the SortNode as requiring its input to be partitioned if partitionExprs
   * is not null (partitionExprs represent the data partition of the entire partition
   * group of which this sort group is a part).
   *
   * If 'perPartitionLimits' is non-null, attempt to place these limits in the sort.
   * Any placed limits have 'pushed' set to true.
   */
  private PlanNode createSortGroupPlan(PlanNode root, SortGroup sortGroup,
      List<Expr> partitionExprs, List<PartitionLimit> perPartitionLimits)
              throws ImpalaException {
    List<Expr> partitionByExprs = sortGroup.partitionByExprs;
    List<OrderByElement> orderByElements = sortGroup.orderByElements;
    boolean hasActivePartition = !Expr.allConstant(partitionByExprs);

    // IMPALA-8069: Ignore something like ORDER BY 0
    boolean isConstSort = true;
    for (OrderByElement elmt : orderByElements) {
      isConstSort = isConstSort && elmt.getExpr().isConstant();
    }

    SortNode sortNode = null;
    // sort on partition by (pb) + order by (ob) exprs and create pb/ob predicates
    if (hasActivePartition || !isConstSort) {
      // first sort on partitionExprs (direction doesn't matter)
      List<Expr> sortExprs = Lists.newArrayList(partitionByExprs);
      // for PB exprs use ASC, NULLS LAST to match the behavior of the default
      // order-by and to ensure that limit pushdown works correctly
      List<Boolean> isAsc = Lists.newArrayList(
          Collections.nCopies(sortExprs.size(), Boolean.valueOf(true)));
      List<Boolean> nullsFirst = Lists.newArrayList(
          Collections.nCopies(sortExprs.size(), Boolean.valueOf(false)));

      // then sort on orderByExprs
      for (OrderByElement orderByElement: sortGroup.orderByElements) {
        // If the expr is in the PARTITION BY and already in 'sortExprs', but also in
        // the ORDER BY, its unnecessary to add it to 'sortExprs' again.
        if (!sortExprs.contains(orderByElement.getExpr())) {
          sortExprs.add(orderByElement.getExpr());
          isAsc.add(orderByElement.isAsc());
          nullsFirst.add(orderByElement.getNullsFirstParam());
        }
      }

      SortInfo sortInfo = createSortInfo(root, sortExprs, isAsc, nullsFirst);
      // IMPALA-8533: Avoid generating sort with empty tuple descriptor
      if(sortInfo.getSortTupleDescriptor().getSlots().size() > 0) {
        // Select the lowest limit to push into the sort. Other limit conjuncts will
        // be added later, if needed. We could try to merge limits together when one
        // dominates the other, e.g. RANK() < 10 and RANK() < 20. However, for
        // simplicity, and in particular to avoid the need to handle cases where neither
        // dominates the other, e.g. ROW_NUMBER() < 10 and RANK() < 20, we only pick one
        // limit. Remaining limits will be assigned as predicates later in analytic
        // planning.
        PartitionLimit limit = null;
        if (perPartitionLimits != null && sortGroup.windowGroups.size() == 1) {
          for (PartitionLimit p : perPartitionLimits) {
            if (sortGroup.windowGroups.get(0).analyticExprs.contains(p.analyticExpr)) {
              if (limit == null || p.limit < limit.limit) {
                limit = p;
              }
            }
          }
        }

        if (limit == null ||
            limit.limit > analyzer_.getQueryOptions().analytic_rank_pushdown_threshold) {
          // Generate a full sort if no limit is known, or if the limit is large enough
          // to disable rank pushed. Even if a limit is known, the full sort can be more
          // efficient the the in-memory top-n if the limit is large enough.
          sortNode = SortNode.createTotalSortNode(
               ctx_.getNextNodeId(), root, sortInfo, 0);
        } else {
          // The backend can't handle limits < 1. We need to apply a limit of 1, and
          // evaluate the predicate later.
          // TODO: IMPALA-10015: we should instead generate an empty set plan that
          // produces the appropriate tuples. This would require short-circuiting plan
          // generation to avoid generating the whole select plan.
          long planNodeLimit = Math.max(1, limit.limit);
          // Convert to standard top-N if only one partition.
          if (Iterables.all(partitionByExprs, Expr.IS_LITERAL_VALUE)) {
            sortNode = SortNode.createTopNSortNode(ctx_.getQueryOptions(),
                    ctx_.getNextNodeId(), root, sortInfo, 0, planNodeLimit,
                    limit.includeTies);
          } else {
            sortNode = SortNode.createPartitionedTopNSortNode(
                    ctx_.getNextNodeId(), root, sortInfo, partitionByExprs.size(),
                    planNodeLimit, limit.includeTies);
          }
          sortNode.setLimitSrcPred(limit.conjunct);
          limit.markPushed();
        }

        // if this sort group does not have partitioning exprs, we want the sort
        // to be executed like a regular distributed sort
        if (hasActivePartition) sortNode.setIsAnalyticSort(true);

        if (partitionExprs != null) {
          // create required input partition
          DataPartition inputPartition = DataPartition.UNPARTITIONED;
          if (hasActivePartition) {
            inputPartition = DataPartition.hashPartitioned(partitionExprs);
          }
          sortNode.setInputPartition(inputPartition);
        }
        root = sortNode;
        root.init(analyzer_);
      }
    }

    AnalyticEvalNode lowestAnalyticNode = null;
    // create one AnalyticEvalNode per window group
    for (WindowGroup windowGroup: sortGroup.windowGroups) {
      root = new AnalyticEvalNode(ctx_.getNextNodeId(), root,
          windowGroup.analyticFnCalls, windowGroup.partitionByExprs,
          windowGroup.orderByElements, windowGroup.window,
          windowGroup.physicalIntermediateTuple, windowGroup.physicalOutputTuple,
          windowGroup.logicalToPhysicalSmap);
      root.init(analyzer_);
      if (lowestAnalyticNode == null) {
        lowestAnalyticNode = (AnalyticEvalNode) root;
        if (sortNode != null) sortNode.setAnalyticEvalNode(lowestAnalyticNode);
      }
    }

    return root;
  }

  /**
   * Collection of AnalyticExprs that share the same partition-by/order-by/window
   * specification. The AnalyticExprs are stored broken up into their constituent parts.
   */
  private static class WindowGroup {
    public final List<Expr> partitionByExprs;
    public final List<OrderByElement> orderByElements;
    public final AnalyticWindow window; // not null

    // Analytic exprs belonging to this window group and their corresponding logical
    // intermediate and output slots from AnalyticInfo.intermediateTupleDesc_
    // and AnalyticInfo.outputTupleDesc_.
    public final List<AnalyticExpr> analyticExprs = new ArrayList<>();
    // Result of getFnCall() for every analytic expr.
    public final List<Expr> analyticFnCalls = new ArrayList<>();
    public final List<SlotDescriptor> logicalOutputSlots = new ArrayList<>();
    public final List<SlotDescriptor> logicalIntermediateSlots = new ArrayList<>();

    // Physical output and intermediate tuples as well as an smap that maps the
    // corresponding logical output slots to their physical slots in physicalOutputTuple.
    // Set in init().
    public TupleDescriptor physicalOutputTuple;
    public TupleDescriptor physicalIntermediateTuple;
    public final ExprSubstitutionMap logicalToPhysicalSmap = new ExprSubstitutionMap();

    public WindowGroup(AnalyticExpr analyticExpr, SlotDescriptor logicalOutputSlot,
        SlotDescriptor logicalIntermediateSlot) {
      partitionByExprs = analyticExpr.getPartitionExprs();
      orderByElements = analyticExpr.getOrderByElements();
      window = analyticExpr.getWindow();
      analyticExprs.add(analyticExpr);
      analyticFnCalls.add(analyticExpr.getFnCall());
      logicalOutputSlots.add(logicalOutputSlot);
      logicalIntermediateSlots.add(logicalIntermediateSlot);
    }

    /**
     * True if this analytic function must be evaluated in its own WindowGroup.
     */
    private static boolean requiresIndependentEval(AnalyticExpr analyticExpr) {
      return analyticExpr.getFnCall().getFnName().getFunction().equals(
          AnalyticExpr.FIRST_VALUE_REWRITE);
    }

    /**
     * True if the partition exprs and ordering elements and the window of analyticExpr
     * match ours.
     */
    public boolean isCompatible(AnalyticExpr analyticExpr) {
      if (requiresIndependentEval(analyticExprs.get(0)) ||
          requiresIndependentEval(analyticExpr)) {
        return false;
      }

      if (!Expr.equalSets(analyticExpr.getPartitionExprs(), partitionByExprs)) {
        return false;
      }
      if (!analyticExpr.getOrderByElements().equals(orderByElements)) return false;
      if ((window == null) != (analyticExpr.getWindow() == null)) return false;
      if (window == null) return true;
      return analyticExpr.getWindow().equals(window);
    }

    /**
     * Adds the given analytic expr and its logical slots to this window group.
     * Assumes the corresponding analyticExpr is compatible with 'this'.
     */
    public void add(AnalyticExpr analyticExpr, SlotDescriptor logicalOutputSlot,
        SlotDescriptor logicalIntermediateSlot) {
      Preconditions.checkState(isCompatible(analyticExpr));
      analyticExprs.add(analyticExpr);
      analyticFnCalls.add(analyticExpr.getFnCall());
      logicalOutputSlots.add(logicalOutputSlot);
      logicalIntermediateSlots.add(logicalIntermediateSlot);
    }

    /**
     * Creates the physical output and intermediate tuples as well as the logical to
     * physical smap for this window group. Computes the mem layout for the tuple
     * descriptors.
     */
    public void init(Analyzer analyzer, String tupleName) {
      Preconditions.checkState(physicalOutputTuple == null);
      Preconditions.checkState(physicalIntermediateTuple == null);
      Preconditions.checkState(analyticFnCalls.size() == analyticExprs.size());

      // If needed, create the intermediate tuple first to maintain
      // intermediateTupleId < outputTupleId for debugging purposes and consistency with
      // tuple creation for aggregations.
      boolean requiresIntermediateTuple =
          AggregateInfoBase.requiresIntermediateTuple(analyticFnCalls);
      if (requiresIntermediateTuple) {
        physicalIntermediateTuple =
            analyzer.getDescTbl().createTupleDescriptor(tupleName + "intermed");
        physicalOutputTuple =
            analyzer.getDescTbl().createTupleDescriptor(tupleName + "out");
      } else {
        physicalOutputTuple =
            analyzer.getDescTbl().createTupleDescriptor(tupleName + "out");
        physicalIntermediateTuple = physicalOutputTuple;
      }

      Preconditions.checkState(analyticExprs.size() == logicalIntermediateSlots.size());
      Preconditions.checkState(analyticExprs.size() == logicalOutputSlots.size());
      for (int i = 0; i < analyticExprs.size(); ++i) {
        SlotDescriptor logicalOutputSlot = logicalOutputSlots.get(i);
        SlotDescriptor physicalOutputSlot =
            analyzer.copySlotDescriptor(logicalOutputSlot, physicalOutputTuple);
        physicalOutputSlot.setIsMaterialized(true);
        if (requiresIntermediateTuple) {
          SlotDescriptor logicalIntermediateSlot = logicalIntermediateSlots.get(i);
          SlotDescriptor physicalIntermediateSlot = analyzer.copySlotDescriptor(
              logicalIntermediateSlot, physicalIntermediateTuple);
          physicalIntermediateSlot.setIsMaterialized(true);
        }
        logicalToPhysicalSmap.put(
            new SlotRef(logicalOutputSlot), new SlotRef(physicalOutputSlot));
      }
      physicalOutputTuple.computeMemLayout();
      if (requiresIntermediateTuple) physicalIntermediateTuple.computeMemLayout();
    }
  }

  /**
   * Extract a minimal set of WindowGroups from analyticExprs.
   */
  private List<WindowGroup> collectWindowGroups() {
    List<AnalyticExpr> analyticExprs = analyticInfo_.getAnalyticExprs();
    List<WindowGroup> groups = new ArrayList<>();
    for (int i = 0; i < analyticExprs.size(); ++i) {
      AnalyticExpr analyticExpr = analyticExprs.get(i);
      // Do not generate the plan for non-materialized analytic exprs.
      if (!analyticInfo_.getOutputTupleDesc().getSlots().get(i).isMaterialized()) {
        continue;
      }
      boolean match = false;
      for (WindowGroup group: groups) {
        if (group.isCompatible(analyticExpr)) {
          group.add(analyticInfo_.getAnalyticExprs().get(i),
              analyticInfo_.getOutputTupleDesc().getSlots().get(i),
              analyticInfo_.getIntermediateTupleDesc().getSlots().get(i));
          match = true;
          break;
        }
      }
      if (!match) {
        groups.add(new WindowGroup(
            analyticInfo_.getAnalyticExprs().get(i),
            analyticInfo_.getOutputTupleDesc().getSlots().get(i),
            analyticInfo_.getIntermediateTupleDesc().getSlots().get(i)));
      }
    }
    return groups;
  }

  /**
   * Collection of WindowGroups that share the same partition-by/order-by
   * specification.
   */
  private static class SortGroup {
    public List<Expr> partitionByExprs;
    public List<OrderByElement> orderByElements;
    public List<WindowGroup> windowGroups = new ArrayList<>();

    // sum of windowGroups.physicalOutputTuple.getByteSize()
    public int totalOutputTupleSize = -1;

    public SortGroup(WindowGroup windowGroup) {
      partitionByExprs = windowGroup.partitionByExprs;
      orderByElements = windowGroup.orderByElements;
      windowGroups.add(windowGroup);
    }

    /**
     * True if the partition and ordering exprs of windowGroup match ours.
     */
    public boolean isCompatible(WindowGroup windowGroup) {
      return Expr.equalSets(windowGroup.partitionByExprs, partitionByExprs)
          && windowGroup.orderByElements.equals(orderByElements);
    }

    public void add(WindowGroup windowGroup) {
      Preconditions.checkState(isCompatible(windowGroup));
      windowGroups.add(windowGroup);
    }

    /**
     * Return true if 'this' and other have compatible partition exprs and
     * our orderByElements are a prefix of other's.
     */
    public boolean isPrefixOf(SortGroup other) {
      if (other.orderByElements.size() > orderByElements.size()) return false;
      if (!Expr.equalSets(partitionByExprs, other.partitionByExprs)) return false;
      for (int i = 0; i < other.orderByElements.size(); ++i) {
        OrderByElement ob = orderByElements.get(i);
        OrderByElement otherOb = other.orderByElements.get(i);
        // TODO: compare equiv classes by comparing each equiv class's placeholder
        // slotref
        if (!ob.getExpr().equals(otherOb.getExpr())) return false;
        if (ob.isAsc() != otherOb.isAsc()) return false;
        if (ob.nullsFirst() != otherOb.nullsFirst()) return false;
      }
      return true;
    }

    /**
     * Adds other's window groups to ours, assuming that we're a prefix of other.
     */
    public void absorb(SortGroup other) {
      Preconditions.checkState(isPrefixOf(other));
      windowGroups.addAll(other.windowGroups);
    }

    /**
     * Compute totalOutputTupleSize.
     */
    public void init() {
      totalOutputTupleSize = 0;
      for (WindowGroup g: windowGroups) {
        TupleDescriptor outputTuple = g.physicalOutputTuple;
        Preconditions.checkState(outputTuple.isMaterialized());
        Preconditions.checkState(outputTuple.getByteSize() != -1);
        totalOutputTupleSize += outputTuple.getByteSize();
      }
    }

    private static class SizeLt implements Comparator<WindowGroup> {
      @Override
      public int compare(WindowGroup wg1, WindowGroup wg2) {
        Preconditions.checkState(wg1.physicalOutputTuple != null
            && wg1.physicalOutputTuple.getByteSize() != -1);
        Preconditions.checkState(wg2.physicalOutputTuple != null
            && wg2.physicalOutputTuple.getByteSize() != -1);
        int diff = wg1.physicalOutputTuple.getByteSize()
            - wg2.physicalOutputTuple.getByteSize();
        return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
      }
    }

    private static final SizeLt SIZE_LT;
    static {
      SIZE_LT = new SizeLt();
    }

    /**
     * Order window groups by increasing size of the output tuple. This minimizes
     * the total volume of data that needs to be buffered.
     */
    public void orderWindowGroups() {
      Collections.sort(windowGroups, SIZE_LT);
    }
  }

  /**
   * Partitions the windowGroups into SortGroups based on compatible order by exprs.
   */
  private List<SortGroup> collectSortGroups(List<WindowGroup> windowGroups) {
    List<SortGroup> sortGroups = new ArrayList<>();
    for (WindowGroup windowGroup: windowGroups) {
      boolean match = false;
      for (SortGroup sortGroup: sortGroups) {
        if (sortGroup.isCompatible(windowGroup)) {
          sortGroup.add(windowGroup);
          match = true;
          break;
        }
      }
      if (!match) sortGroups.add(new SortGroup(windowGroup));
    }
    return sortGroups;
  }

  /**
   * Collection of SortGroups that have compatible partition-by specifications.
   */
  private static class PartitionGroup {
    public List<Expr> partitionByExprs;
    public List<SortGroup> sortGroups = new ArrayList<>();

    // sum of sortGroups.windowGroups.physicalOutputTuple.getByteSize()
    public int totalOutputTupleSize = -1;

    public PartitionGroup(SortGroup sortGroup) {
      partitionByExprs = sortGroup.partitionByExprs;
      sortGroups.add(sortGroup);
      totalOutputTupleSize = sortGroup.totalOutputTupleSize;
    }

    /**
     * True if the partition exprs of sortGroup are compatible with ours.
     * For now that means equality.
     */
    public boolean isCompatible(SortGroup sortGroup) {
      return Expr.equalSets(sortGroup.partitionByExprs, partitionByExprs);
    }

    public void add(SortGroup sortGroup) {
      Preconditions.checkState(isCompatible(sortGroup));
      sortGroups.add(sortGroup);
      totalOutputTupleSize += sortGroup.totalOutputTupleSize;
    }

    /**
     * Merge 'other' into 'this'
     * - partitionByExprs is the intersection of the two
     * - sortGroups becomes the union
     */
    public void merge(PartitionGroup other) {
      partitionByExprs = Expr.intersect(partitionByExprs, other.partitionByExprs);
      Preconditions.checkState(Expr.getNumDistinctValues(partitionByExprs) >= 0);
      sortGroups.addAll(other.sortGroups);
    }

    /**
     * Order sort groups by increasing totalOutputTupleSize. This minimizes the total
     * volume of data that needs to be sorted.
     */
    public void orderSortGroups() {
      Collections.sort(sortGroups,
          new Comparator<SortGroup>() {
            @Override
            public int compare(SortGroup sg1, SortGroup sg2) {
              Preconditions.checkState(sg1.totalOutputTupleSize > 0);
              Preconditions.checkState(sg2.totalOutputTupleSize > 0);
              int diff = sg1.totalOutputTupleSize - sg2.totalOutputTupleSize;
              return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
            }
          });
      for (SortGroup sortGroup: sortGroups) {
        sortGroup.orderWindowGroups();
      }
    }
  }

  /**
   * Extract a minimal set of PartitionGroups from sortGroups.
   */
  private List<PartitionGroup> collectPartitionGroups(List<SortGroup> sortGroups) {
    List<PartitionGroup> partitionGroups = new ArrayList<>();
    for (SortGroup sortGroup: sortGroups) {
      boolean match = false;
      for (PartitionGroup partitionGroup: partitionGroups) {
        if (partitionGroup.isCompatible(sortGroup)) {
          partitionGroup.add(sortGroup);
          match = true;
          break;
        }
      }
      if (!match) partitionGroups.add(new PartitionGroup(sortGroup));
    }
    return partitionGroups;
  }

  private static class PartitionLimit {
    public PartitionLimit(Expr conjunct, AnalyticExpr analyticExpr, long limit,
            boolean includeTies, boolean isLessThan) {
      this.conjunct = conjunct;
      this.analyticExpr = analyticExpr;
      this.limit = limit;
      this.includeTies = includeTies;
      this.isLessThan = isLessThan;
      this.pushed = false;
    }

    /// The conjunct that this was derived from.
    public final Expr conjunct;

    /// The ranking analytic expr that this limit was inferred from.
    public final AnalyticExpr analyticExpr;

    /// The limit on rows per partition returned.
    public final long limit;

    /// Whether ties for last place need to be included.
    public final boolean includeTies;

    /// Whether the source predicate was a simple < or <= inequality.
    /// I.e. false for = and true for < and <=.
    public final boolean isLessThan;

    /// Whether the limit was pushed to a top-n operator.
    private boolean pushed;

    public boolean isPushed() {
      return pushed;
    }

    public void markPushed() {
      this.pushed = true;
    }
  }

  /**
   * Extract per-partition limits from 'conjuncts'.
   */
  private List<PartitionLimit> inferPartitionLimits(List<Expr> conjuncts) {
    List<PartitionLimit> result = new ArrayList<>();
    if (analyzer_.getQueryOptions().analytic_rank_pushdown_threshold <= 0) return result;
    for (Expr conj : conjuncts) {
      if (!(Expr.IS_BINARY_PREDICATE.apply(conj))) continue;
      BinaryPredicate pred = (BinaryPredicate) conj;
      Expr lhs = pred.getChild(0);
      Expr rhs = pred.getChild(1);
      // Lhs of the binary predicate must be a ranking function.
      // Also, it must be bound to the output tuple of this analytic eval node
      if (!(lhs instanceof SlotRef)) continue;

      List<Expr> lhsSourceExprs = ((SlotRef) lhs).getDesc().getSourceExprs();
      if (lhsSourceExprs.size() != 1 ||
            !(lhsSourceExprs.get(0) instanceof AnalyticExpr)) {
        continue;
      }

      boolean includeTies;
      AnalyticExpr analyticExpr = (AnalyticExpr) lhsSourceExprs.get(0);
      Function fn = analyticExpr.getFnCall().getFn();
      // Check if this a predicate that we can convert to a limit in the sort.
      // We do not have the runtime support that would be required to handle
      // DENSE_RANK().
      if (AnalyticExpr.isAnalyticFn(fn, AnalyticExpr.RANK)) {
        // RANK() assigns equal values to rows where the ORDER BY expressions are equal.
        // Thus if there are ties for the max RANK() value to be returned, we need to
        // return all of them. E.g. if the predicate is RANK() <= 3, and the values we are
        // ordering by are 1, 42, 99, 99, 100, then the RANK() values are 1, 2, 3, 3, 5
        // and we need to return the top 4 rows: 1, 42, 99, 99.
        // We do not need special handling for ties except at the limit value. E.g. if
        // the ordering values were 1, 1, 42, 99, 100, then the RANK() values would be
        // 1, 1, 3, 4, 5 and we only return the top 3.
        includeTies = true;
      } else if (AnalyticExpr.isAnalyticFn(fn, AnalyticExpr.ROWNUMBER)) {
        includeTies = false;
      } else {
        continue;
      }

      AnalyticWindow window = analyticExpr.getWindow();
      // Check that the window frame is UNBOUNDED PRECEDING to CURRENT ROW,
      // i.e. that the function monotonically increases from 1 within the window.
      if (window.getLeftBoundary().getType() !=
            AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING
          || window.getRightBoundary().getType() !=
              AnalyticWindow.BoundaryType.CURRENT_ROW) {
        continue;
      }

      // Next, try to extract a numeric limit that will apply to the analytic function.
      if (!(rhs instanceof NumericLiteral)) continue;
      BigDecimal val = ((NumericLiteral)rhs).getValue();
      if (val.compareTo(new BigDecimal(Long.MAX_VALUE)) > 0) {
        continue;
      }
      // Round down to the nearest long.
      long longVal = val.longValue();
      long perPartitionLimit;
      // currently, restrict the pushdown for =, <, <= predicates
      if (pred.getOp() == BinaryPredicate.Operator.EQ ||
              pred.getOp() == BinaryPredicate.Operator.LE) {
        perPartitionLimit = longVal;
      } else if (pred.getOp() == BinaryPredicate.Operator.LT) {
        perPartitionLimit = longVal - 1;
      } else {
        continue;
      }
      boolean isLessThan = pred.getOp() == BinaryPredicate.Operator.LE ||
              pred.getOp() == BinaryPredicate.Operator.LT;
      if (LOG.isTraceEnabled()) {
        LOG.trace(analyticExpr.debugString() + " implies per-partition limit " +
             perPartitionLimit + " includeTies=" + includeTies);
      }
      result.add(new PartitionLimit(
              conj, analyticExpr, perPartitionLimit, includeTies, isLessThan));
    }
    return result;
  }
}
