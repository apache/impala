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

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalyticExpr;
import com.cloudera.impala.analysis.AnalyticInfo;
import com.cloudera.impala.analysis.AnalyticWindow;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.ExprSubstitutionMap;
import com.cloudera.impala.analysis.OrderByElement;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.SortInfo;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.IdGenerator;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.TPartitionType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


/**
 * The analytic planner adds plan nodes to an existing set of PlanFragments in order to
 * implement the AnalyticInfo of a given query stmt.
 */
public class AnalyticPlanner {
  private final static Logger LOG = LoggerFactory.getLogger(AnalyticPlanner.class);

  private final AnalyticInfo analyticInfo_;
  private final Analyzer analyzer_;
  private final IdGenerator<PlanNodeId> idGenerator_;

  public AnalyticPlanner(AnalyticInfo analyticInfo, Analyzer analyzer,
      IdGenerator<PlanNodeId> idGenerator) {
    analyticInfo_ = analyticInfo;
    analyzer_ = analyzer;
    idGenerator_ = idGenerator;
  }

  /**
   * Augment planFragment with plan nodes that implement single-node evaluation of
   * the AnalyticExprs in analyticInfo.
   * TODO: this only works for non-nested, non-Union select stmts; add the rest
   * TODO: missing limits
   * TODO: deal with OrderByElements, not just ordering exprs
   * TODO: prune non-materialized output slots
   * TODO: create partition groups that are not based on set identity: the subset
   * of partition exprs common to the partition group should be large enough to
   * parallelize across all machines
   * TODO: unpartitioned partition groups should go last
   */
  public PlanNode createSingleNodePlan(PlanNode root) throws ImpalaException {
    List<WindowGroup> windowGroups = collectWindowGroups();
    List<SortGroup> sortGroups = collectSortGroups(windowGroups);
    List<PartitionGroup> partitionGroups = collectPartitionGroups(sortGroups);

    for (PartitionGroup partitionGroup: partitionGroups) {
      for (int i = 0; i < partitionGroup.sortGroups.size(); ++i) {
        root = createSortGroupPlan(root, partitionGroup.sortGroups.get(i), i == 0);
      }
    }
    return root;
  }

  /**
   * Create SortInfo, including sort tuple, to sort entire input row
   * on sortExprs.
   */
  private SortInfo createSortInfo(
      PlanNode input, List<Expr> sortExprs, List<Boolean> isAsc,
      List<Boolean> nullsFirst) {
    // create tuple for sort output = the entire materialized input in a single tuple
    TupleDescriptor sortTupleDesc = analyzer_.getDescTbl().createTupleDescriptor();
    ExprSubstitutionMap sortSmap = new ExprSubstitutionMap();
    List<Expr> sortSlotExprs = Lists.newArrayList();
    sortTupleDesc.setIsMaterialized(true);
    for (TupleId tid: input.getTupleIds()) {
      TupleDescriptor tupleDesc = analyzer_.getTupleDesc(tid);
      for (SlotDescriptor inputSlotDesc: tupleDesc.getSlots()) {
        if (!inputSlotDesc.isMaterialized()) continue;
        SlotDescriptor sortSlotDesc =
            analyzer_.addSlotDescriptor(inputSlotDesc, sortTupleDesc);
        // all output slots need to be materialized
        sortSlotDesc.setIsMaterialized(true);
        sortSmap.put(new SlotRef(inputSlotDesc), new SlotRef(sortSlotDesc));
        sortSlotExprs.add(new SlotRef(inputSlotDesc));
      }
    }

    SortInfo sortInfo = new SortInfo(
        Expr.substituteList(sortExprs, sortSmap, analyzer_), isAsc, nullsFirst);
    LOG.info("sortinfo exprs: " + Expr.debugString(sortInfo.getOrderingExprs()));
    sortInfo.setMaterializedTupleInfo(sortTupleDesc, sortSlotExprs);
    return sortInfo;
  }

  /**
   * Create plan tree for the entire sort group, including all contained window groups.
   * Marks the SortNode as requiring its input to be partitioned if isFirstInPartition.
   */
  private PlanNode createSortGroupPlan(PlanNode root, SortGroup sortGroup,
      boolean isFirstInPartition) throws ImpalaException {
    List<Expr> partitionByExprs = sortGroup.partitionByExprs;
    List<OrderByElement> orderByElements = sortGroup.orderByElements;
    List<Expr> orderByExprs = Lists.newArrayList();
    for (OrderByElement e: sortGroup.orderByElements) {
      orderByExprs.add(e.getExpr());
    }
    SortNode sortNode = null;
    Expr partitionByLessThan = null;
    Expr orderByLessThan = null;
    TupleDescriptor bufferedTupleDesc = null;

    // sort on partition by (pb) + order by (ob) exprs and create pb/ob predicates
    if (!partitionByExprs.isEmpty() || !orderByElements.isEmpty()) {
      // first sort on partitionExprs (direction doesn't matter)
      List<Expr> sortExprs = Lists.newArrayList(partitionByExprs);
      List<Boolean> isAsc =
          Lists.newArrayList(Collections.nCopies(sortExprs.size(), new Boolean(true)));
      // TODO: should nulls come first or last?
      List<Boolean> nullsFirst =
          Lists.newArrayList(Collections.nCopies(sortExprs.size(), new Boolean(true)));

      // then sort on orderByExprs
      for (OrderByElement orderByElement: sortGroup.orderByElements) {
        sortExprs.add(orderByElement.getExpr());
        isAsc.add(orderByElement.getIsAsc());
        nullsFirst.add(orderByElement.getNullsFirstParam());
      }

      SortInfo sortInfo = createSortInfo(root, sortExprs, isAsc, nullsFirst);
      sortNode = new SortNode(idGenerator_.getNextId(), root, sortInfo, false, 0);
      sortNode.setIsAnalyticSort(true);

      if (isFirstInPartition) {
        // create required input partition
        DataPartition inputPartition = DataPartition.UNPARTITIONED;
        if (!partitionByExprs.isEmpty()) {
          inputPartition = new DataPartition(TPartitionType.HASH_PARTITIONED,
              partitionByExprs);
        }
        sortNode.setRequiredInputPartition(inputPartition);
      }

      root = sortNode;
      root.init(analyzer_);

      // Create partition-by (pb) and order-by (ob) less-than predicates between the
      // input tuple (the output of the preceding sort) and a buffered tuple that is
      // identical to the input tuple. We need a different tuple descriptor for the
      // buffered tuple because the generated predicates should compare two different
      // tuple instances from the same input stream (i.e., the predicates should be
      // evaluated over a row that is composed of the input and the buffered tuple).
      // First, create the output tuple for this particular group of AnalyticExprs.
      TupleId sortTupleId = sortNode.tupleIds_.get(0);
      bufferedTupleDesc = analyzer_.getDescTbl().copyTupleDescriptor(sortTupleId);
      LOG.trace("desctbl: " + analyzer_.getDescTbl().debugString());

      // map from input to buffered tuple
      ExprSubstitutionMap bufferedSmap = new ExprSubstitutionMap();
      List<SlotDescriptor> inputSlots = analyzer_.getTupleDesc(sortTupleId).getSlots();
      List<SlotDescriptor> bufferedSlots = bufferedTupleDesc.getSlots();
      for (int i = 0; i < inputSlots.size(); ++i) {
        bufferedSmap.put(
            new SlotRef(inputSlots.get(i)), new SlotRef(bufferedSlots.get(i)));
      }

      // pb/ob predicates compare the output of the preceding sort to our
      // buffered input; we need to remap the pb/ob exprs to a) the sort output,
      // b) our buffer of the sort input
      ExprSubstitutionMap sortSmap = sortNode.getBaseTblSmap();
      LOG.trace("sort smap: " + sortSmap.debugString());
      if (sortNode.getChild(0).getBaseTblSmap() != null) {
        LOG.trace("sort child smap: " +
            sortNode.getChild(0).getBaseTblSmap().debugString());
      }
      if (!partitionByExprs.isEmpty()) {
        partitionByLessThan = createLessThan(
            Expr.substituteList(partitionByExprs, sortSmap, analyzer_),
            sortTupleId, bufferedSmap);
        LOG.trace("partitionByLt: " + partitionByLessThan.debugString());
      }
      if (!orderByExprs.isEmpty()) {
        // TODO: take asc/desc, nulls first/last into account
        orderByLessThan = createLessThan(
            Expr.substituteList(orderByExprs, sortSmap, analyzer_),
            sortTupleId, bufferedSmap);
        LOG.trace("orderByLt: " + orderByLessThan.debugString());
      }
    }

    // Create physical intermediate and output tuples plus smap for every window group.
    // TODO: The current solution is easy to handle and reason about in the FE/BE.
    // Consider creating only a single physical output tuple for all window
    // groups, although it's not always a clear win due to spilling.
    for (WindowGroup windowGroup: sortGroup.windowGroups) {
      // child[0] of every analytic expr in this window group
      List<Expr> analyticFnCalls = Lists.newArrayList();
      // maps from the logical output tuple (analyticInfo_.outputTupleDesc_) to
      // the physical output tuple materialized by the plan implementing this
      // window group
      ExprSubstitutionMap logicalToPhysicalSmap = new ExprSubstitutionMap();
      Pair<TupleDescriptor, TupleDescriptor> physicalTuples =
          createPhysicalTuples(windowGroup, logicalToPhysicalSmap, analyticFnCalls);

      // create one AnalyticEvalNode per window group
      root = new AnalyticEvalNode(idGenerator_.getNextId(), root,
          analyticFnCalls, windowGroup.partitionByExprs, orderByExprs,
          windowGroup.window, physicalTuples.first, physicalTuples.second,
          logicalToPhysicalSmap, partitionByLessThan, orderByLessThan,
          bufferedTupleDesc);
      root.init(analyzer_);
    }
    return root;
  }

  /**
   * Returns the physical intermediate and output tuples for the given window group in
   * pair.first and pair.second, respectively. Populates an smap from the logical
   * analytic output slots to the physical ones, as well as the given list of analytic
   * function calls (child[0] of every analytic expr in the window group).
   * Computes the mem layout for the returned tuple descriptors.
   */
  private Pair<TupleDescriptor, TupleDescriptor> createPhysicalTuples(WindowGroup windowGroup,
      ExprSubstitutionMap logicalToPhysicalSmap, List<Expr> analyticFnCalls) {
    // Physical intermediate and output tuples. If needed, create the intermediate
    // tuple first to maintain intermediateTupleId < outputTupleId for debugging
    // purposes and consistency with tuple creation for aggregations.
    TupleDescriptor intermediateTuple = null;
    TupleDescriptor outputTuple = null;
    if (analyticInfo_.hasDiffIntermediateTuple()) {
      intermediateTuple = analyzer_.getDescTbl().createTupleDescriptor();
      outputTuple = analyzer_.getDescTbl().createTupleDescriptor();
    } else {
      outputTuple = analyzer_.getDescTbl().createTupleDescriptor();
      intermediateTuple = outputTuple;
    }

    Preconditions.checkState(windowGroup.analyticExprs.size() ==
        windowGroup.logicalIntermediateSlots.size());
    Preconditions.checkState(windowGroup.analyticExprs.size() ==
        windowGroup.logicalOutputSlots.size());
    for (int i = 0; i < windowGroup.analyticExprs.size(); ++i) {
      analyticFnCalls.add(windowGroup.analyticExprs.get(i).getChild(0));
      SlotDescriptor logicalOutputSlot = windowGroup.logicalOutputSlots.get(i);
      SlotDescriptor physicalOutputSlot =
          analyzer_.addSlotDescriptor(logicalOutputSlot, outputTuple);
      physicalOutputSlot.setIsMaterialized(true);
      if (analyticInfo_.hasDiffIntermediateTuple()) {
        SlotDescriptor logicalIntermediateSlot =
            windowGroup.logicalIntermediateSlots.get(i);
        analyzer_.addSlotDescriptor(logicalIntermediateSlot, intermediateTuple);
        logicalIntermediateSlot.setIsMaterialized(true);
      }
      logicalToPhysicalSmap.put(new SlotRef(logicalOutputSlot),
          new SlotRef(physicalOutputSlot));
    }
    outputTuple.computeMemLayout();
    if (analyticInfo_.hasDiffIntermediateTuple()) intermediateTuple.computeMemLayout();

    return new Pair<TupleDescriptor, TupleDescriptor>(intermediateTuple, outputTuple);
  }

  /**
   * Create '<' predicate between exprs of input row and buffered row
   * ('exprs' refers to input row).
   * Example:
   * (input_expr0 < buffered_expr0)
   *   || (input_expr0 == buffered_expr0 && input_expr1 < buffered_expr1)
   *   || ...
   */
  private Expr createLessThan(List<Expr> inputRowExprs, TupleId inputTid,
      ExprSubstitutionMap bufferedSmap) {
    Preconditions.checkState(!inputRowExprs.isEmpty());
    LOG.trace("expr0: " + inputRowExprs.get(0).debugString());
    Preconditions.checkState(inputRowExprs.get(0).isBound(inputTid));
    Expr result = new BinaryPredicate(BinaryPredicate.Operator.LT,
        inputRowExprs.get(0).clone(),
        inputRowExprs.get(0).substitute(bufferedSmap, analyzer_));
    for (int i = 1; i < inputRowExprs.size(); ++i) {
      Expr eqClause = new BinaryPredicate(BinaryPredicate.Operator.EQ,
          inputRowExprs.get(i - 1).clone(),
          inputRowExprs.get(i - 1).substitute(bufferedSmap, analyzer_));
      Preconditions.checkState(inputRowExprs.get(i).isBound(inputTid));
      Expr ltClause = new BinaryPredicate(BinaryPredicate.Operator.LT,
          inputRowExprs.get(i).clone(),
          inputRowExprs.get(i).substitute(bufferedSmap, analyzer_));
      result = new CompoundPredicate(CompoundPredicate.Operator.OR,
          result,
          new CompoundPredicate(CompoundPredicate.Operator.AND, eqClause, ltClause));
    }
    try {
      result.analyze(analyzer_);
    } catch (AnalysisException e) {
      throw new IllegalStateException(e.getMessage());
    }
    return result;
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
    // intermediate and output slots.
    public final List<AnalyticExpr> analyticExprs = Lists.newArrayList();
    public final List<SlotDescriptor> logicalOutputSlots = Lists.newArrayList();
    public final List<SlotDescriptor> logicalIntermediateSlots = Lists.newArrayList();

    public WindowGroup(AnalyticExpr analyticExpr, SlotDescriptor logicalOutputSlot,
        SlotDescriptor logicalIntermediateSlot) {
      partitionByExprs = analyticExpr.getPartitionExprs();
      orderByElements = analyticExpr.getOrderByElements();
      window = analyticExpr.getWindow();
      analyticExprs.add(analyticExpr);
      logicalOutputSlots.add(logicalOutputSlot);
      logicalIntermediateSlots.add(logicalIntermediateSlot);
    }

    /**
     * True if the partition exprs and ordering elements and the window of analyticExpr
     * match ours.
     */
    public boolean isCompatible(AnalyticExpr analyticExpr) {
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
      logicalOutputSlots.add(logicalOutputSlot);
      logicalIntermediateSlots.add(logicalIntermediateSlot);
    }
  }

  /**
   * Extract a minimal set of WindowGroups from analyticExprs.
   * TODO: Handle non-materialized analytic expr output slots.
   */
  private List<WindowGroup> collectWindowGroups() {
    List<Expr> analyticExprs = analyticInfo_.getAnalyticExprs();
    List<WindowGroup> groups = Lists.newArrayList();
    for (int i = 0; i < analyticExprs.size(); ++i) {
      AnalyticExpr analyticExpr = (AnalyticExpr) analyticExprs.get(i);
      boolean match = false;
      for (WindowGroup group: groups) {
        if (group.isCompatible(analyticExpr)) {
          group.add((AnalyticExpr) analyticInfo_.getAnalyticExprs().get(i),
              analyticInfo_.getIntermediateTupleDesc().getSlots().get(i),
              analyticInfo_.getOutputTupleDesc().getSlots().get(i));
          match = true;
          break;
        }
      }
      if (!match) {
        groups.add(new WindowGroup(
            (AnalyticExpr) analyticInfo_.getAnalyticExprs().get(i),
            analyticInfo_.getIntermediateTupleDesc().getSlots().get(i),
            analyticInfo_.getOutputTupleDesc().getSlots().get(i)));
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
    public List<WindowGroup> windowGroups = Lists.newArrayList();

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
  }

  /**
   * Partitions the windowGroups into SortGroups based on compatible order by exprs.
   */
  private List<SortGroup> collectSortGroups(List<WindowGroup> windowGroups) {
    List<SortGroup> sortGroups = Lists.newArrayList();
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
    public List<SortGroup> sortGroups = Lists.newArrayList();

    public PartitionGroup(SortGroup sortGroup) {
      partitionByExprs = sortGroup.partitionByExprs;
      sortGroups.add(sortGroup);
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
    }
  }

  /**
   * Extract a minimal set of PartitionGroups from sortGroups.
   */
  private List<PartitionGroup> collectPartitionGroups(List<SortGroup> sortGroups) {
    List<PartitionGroup> partitionGroups = Lists.newArrayList();
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
}
