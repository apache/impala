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
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AggregateInfoBase;
import com.cloudera.impala.analysis.AnalyticExpr;
import com.cloudera.impala.analysis.AnalyticInfo;
import com.cloudera.impala.analysis.AnalyticWindow;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.BoolLiteral;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.ExprSubstitutionMap;
import com.cloudera.impala.analysis.IsNullPredicate;
import com.cloudera.impala.analysis.OrderByElement;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.SortInfo;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.IdGenerator;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.thrift.TPartitionType;
import com.google.common.base.Preconditions;
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

  // List of tuple ids materialized by the originating SelectStmt (i.e., what is returned
  // by SelectStmt.getMaterializedTupleIds()). During analysis, conjuncts have been
  // registered against these tuples, so we need them to find unassigned conjuncts during
  // plan generation.
  // If the size of this list is 1 it means the stmt has a sort after the analytics.
  // Otherwise, the last tuple id must be analyticInfo_.getOutputTupleDesc().
  private final List<TupleId> stmtTupleIds_;

  private final AnalyticInfo analyticInfo_;
  private final Analyzer analyzer_;
  private final IdGenerator<PlanNodeId> idGenerator_;

  public AnalyticPlanner(List<TupleId> stmtTupleIds,
      AnalyticInfo analyticInfo, Analyzer analyzer,
      IdGenerator<PlanNodeId> idGenerator) {
    Preconditions.checkState(!stmtTupleIds.isEmpty());
    TupleId lastStmtTupleId = stmtTupleIds.get(stmtTupleIds.size() - 1);
    Preconditions.checkState(stmtTupleIds.size() == 1 ||
        lastStmtTupleId.equals(analyticInfo.getOutputTupleId()));
    stmtTupleIds_ = stmtTupleIds;
    analyticInfo_ = analyticInfo;
    analyzer_ = analyzer;
    idGenerator_ = idGenerator;
  }

  /**
   * Augment planFragment with plan nodes that implement single-node evaluation of
   * the AnalyticExprs in analyticInfo.
   * TODO: create partition groups that are not based on set identity: the subset
   * of partition exprs common to the partition group should be large enough to
   * parallelize across all machines
   */
  public PlanNode createSingleNodePlan(PlanNode root) throws ImpalaException {
    List<WindowGroup> windowGroups = collectWindowGroups();
    for (WindowGroup g: windowGroups) {
      g.init(analyzer_);
    }
    List<SortGroup> sortGroups = collectSortGroups(windowGroups);
    mergeSortGroups(sortGroups);
    for (SortGroup g: sortGroups) {
      g.init();
    }
    List<PartitionGroup> partitionGroups = collectPartitionGroups(sortGroups);
    orderGroups(partitionGroups);

    for (PartitionGroup partitionGroup: partitionGroups) {
      for (int i = 0; i < partitionGroup.sortGroups.size(); ++i) {
        root = createSortGroupPlan(root, partitionGroup.sortGroups.get(i), i == 0);
      }
    }

    // create equiv classes for newly added slots
    analyzer_.createIdentityEquivClasses();

    return root;
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
   * Order partition groups (and the sort groups within them) by increasing
   * totalOutputTupleSize. This minimizes the total volume of data that needs to be
   * repartitioned and sorted.
   * Also move the non-partitioning partition group to the end.
   */
  private void orderGroups(List<PartitionGroup> partitionGroups) {
    // remove the non-partitioning group from partitionGroups
    PartitionGroup nonPartitioning = null;
    for (PartitionGroup pg: partitionGroups) {
      if (pg.partitionByExprs.isEmpty()) {
        nonPartitioning = pg;
        break;
      }
    }
    if (nonPartitioning != null) partitionGroups.remove(nonPartitioning);

    // order by ascending combined output tuple size
    Collections.sort(partitionGroups,
        new Comparator<PartitionGroup>() {
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
            analyzer_.copySlotDescriptor(inputSlotDesc, sortTupleDesc);
        // all output slots need to be materialized
        sortSlotDesc.setIsMaterialized(true);
        sortSmap.put(new SlotRef(inputSlotDesc), new SlotRef(sortSlotDesc));
        sortSlotExprs.add(new SlotRef(inputSlotDesc));
      }
    }

    SortInfo sortInfo = new SortInfo(
        Expr.substituteList(sortExprs, sortSmap, analyzer_), isAsc, nullsFirst);
    LOG.trace("sortinfo exprs: " + Expr.debugString(sortInfo.getOrderingExprs()));
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
    ExprSubstitutionMap sortSmap = null;
    TupleId sortTupleId = null;
    TupleDescriptor bufferedTupleDesc = null;
    // map from input to buffered tuple
    ExprSubstitutionMap bufferedSmap = new ExprSubstitutionMap();

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
        isAsc.add(orderByElement.isAsc());
        nullsFirst.add(orderByElement.getNullsFirstParam());
      }

      SortInfo sortInfo = createSortInfo(root, sortExprs, isAsc, nullsFirst);
      SortNode sortNode =
          new SortNode(idGenerator_.getNextId(), root, sortInfo, false, 0);

      // if this sort group does not have partitioning exprs, we want the sort
      // to be executed like a regular distributed sort
      if (!partitionByExprs.isEmpty()) sortNode.setIsAnalyticSort(true);

      if (isFirstInPartition) {
        // create required input partition
        DataPartition inputPartition = DataPartition.UNPARTITIONED;
        if (!partitionByExprs.isEmpty()) {
          inputPartition = new DataPartition(TPartitionType.HASH_PARTITIONED,
              partitionByExprs);
        }
        sortNode.setInputPartition(inputPartition);
      }

      root = sortNode;
      root.init(analyzer_);
      sortSmap = sortNode.getOutputSmap();

      // create bufferedTupleDesc and bufferedSmap
      sortTupleId = sortNode.tupleIds_.get(0);
      bufferedTupleDesc = analyzer_.getDescTbl().copyTupleDescriptor(sortTupleId);
      LOG.trace("desctbl: " + analyzer_.getDescTbl().debugString());

      List<SlotDescriptor> inputSlots = analyzer_.getTupleDesc(sortTupleId).getSlots();
      List<SlotDescriptor> bufferedSlots = bufferedTupleDesc.getSlots();
      for (int i = 0; i < inputSlots.size(); ++i) {
        bufferedSmap.put(
            new SlotRef(inputSlots.get(i)), new SlotRef(bufferedSlots.get(i)));
      }

    }

    // create one AnalyticEvalNode per window group
    for (WindowGroup windowGroup: sortGroup.windowGroups) {
      // Create partition-by (pb) and order-by (ob) less-than predicates between the
      // input tuple (the output of the preceding sort) and a buffered tuple that is
      // identical to the input tuple. We need a different tuple descriptor for the
      // buffered tuple because the generated predicates should compare two different
      // tuple instances from the same input stream (i.e., the predicates should be
      // evaluated over a row that is composed of the input and the buffered tuple).

      // we need to remap the pb/ob exprs to a) the sort output, b) our buffer of the
      // sort input
      Expr partitionByLessThan = null;
      if (!windowGroup.partitionByExprs.isEmpty()) {
        partitionByLessThan = createExprLessThan(
            Expr.substituteList(windowGroup.partitionByExprs, sortSmap, analyzer_),
            sortTupleId, bufferedSmap);
        LOG.trace("partitionByLt: " + partitionByLessThan.debugString());
      }
      Expr orderByLessThan = null;
      if (!windowGroup.orderByElements.isEmpty()) {
        orderByLessThan = createLessThan(
            OrderByElement.substitute(windowGroup.orderByElements, sortSmap, analyzer_),
            sortTupleId, bufferedSmap);
        LOG.trace("orderByLt: " + orderByLessThan.debugString());
      }

      root = new AnalyticEvalNode(idGenerator_.getNextId(), root, stmtTupleIds_,
          windowGroup.analyticFnCalls, windowGroup.partitionByExprs,
          windowGroup.orderByElements,
          windowGroup.window, analyticInfo_.getOutputTupleDesc(),
          windowGroup.physicalIntermediateTuple,
          windowGroup.physicalOutputTuple, windowGroup.logicalToPhysicalSmap,
          partitionByLessThan, orderByLessThan, bufferedTupleDesc);
      root.init(analyzer_);
    }
    return root;
  }

  /**
   * Create '<input row> < <buffered row>' predicate.
   */
  private Expr createExprLessThan(List<Expr> orderByExprs, TupleId inputTid,
      ExprSubstitutionMap bufferedSmap) {
    List<OrderByElement> elements = Lists.newArrayList();
    for (Expr e: orderByExprs) {
      elements.add(new OrderByElement(e, true, true));
    }
    return createLessThan(elements, inputTid, bufferedSmap);
  }

  /**
   * Create '<input row> < <buffered row>' predicate.
   */
  private Expr createLessThan(List<OrderByElement> elements, TupleId inputTid,
      ExprSubstitutionMap bufferedSmap) {
    Preconditions.checkState(!elements.isEmpty());
    Expr result = createLessThanAux(elements, 0, inputTid, bufferedSmap);
    try {
      result.analyze(analyzer_);
    } catch (AnalysisException e) {
      throw new IllegalStateException(e);
    }
    return result;
  }

  /**
   * Create an unanalyzed '<' predicate for elements >= i.
   *
   * With asc/nulls first, the predicate has the form
   * rhs[i] is not null && (
   *   lhs[i] is null
   *   || lhs[i] < rhs[i]
   *   || (lhs[i] = rhs[i] && <createLessThanAux(i + 1)>)
   *  )
   *
   * TODO: this is extremely contorted; we need to introduce a builtin
   * compare(lhs, rhs, is_asc, nulls_first)
   */
  private Expr createLessThanAux(List<OrderByElement> elements, int i, TupleId inputTid,
      ExprSubstitutionMap bufferedSmap) {
    if (i > elements.size() - 1) return new BoolLiteral(false);

    // compare elements[i]
    Expr lhs = elements.get(i).getExpr();
    Preconditions.checkState(lhs.isBound(inputTid));
    Expr rhs = lhs.substitute(bufferedSmap, analyzer_);
    Expr rhsIsNotFirst = new IsNullPredicate(
        elements.get(i).nullsFirst() ? rhs : lhs, true);
    Expr lhsIsFirst = new IsNullPredicate(
        elements.get(i).nullsFirst() ? lhs : rhs, false);
    BinaryPredicate.Operator comparison = elements.get(i).isAsc()
        ? BinaryPredicate.Operator.LT
        : BinaryPredicate.Operator.GT;
    Expr lhsPrecedesRhs = new BinaryPredicate(comparison, lhs, rhs);
    Expr lhsEqRhs = new BinaryPredicate(BinaryPredicate.Operator.EQ, lhs, rhs);
    Expr remainder = createLessThanAux(elements, i + 1, inputTid, bufferedSmap);
    Expr result = new CompoundPredicate(CompoundPredicate.Operator.AND,
        rhsIsNotFirst,
        new CompoundPredicate(CompoundPredicate.Operator.OR,
          new CompoundPredicate(CompoundPredicate.Operator.OR, lhsIsFirst, lhsPrecedesRhs),
          new CompoundPredicate(CompoundPredicate.Operator.AND, lhsEqRhs, remainder)));
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
    // intermediate and output slots from AnalyticInfo.intermediateTupleDesc_
    // and AnalyticInfo.outputTupleDesc_.
    public final List<AnalyticExpr> analyticExprs = Lists.newArrayList();
    // Result of getFnCall() for every analytic expr.
    public final List<Expr> analyticFnCalls = Lists.newArrayList();
    public final List<SlotDescriptor> logicalOutputSlots = Lists.newArrayList();
    public final List<SlotDescriptor> logicalIntermediateSlots = Lists.newArrayList();

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
      analyticFnCalls.add(analyticExpr.getFnCall());
      logicalOutputSlots.add(logicalOutputSlot);
      logicalIntermediateSlots.add(logicalIntermediateSlot);
    }

    /**
     * Creates the physical output and intermediate tuples as well as the logical to
     * physical smap for this window group. Computes the mem layout for the tuple
     * descriptors.
     */
    public void init(Analyzer analyzer) {
      Preconditions.checkState(physicalOutputTuple == null);
      Preconditions.checkState(physicalIntermediateTuple == null);
      Preconditions.checkState(analyticFnCalls.size() == analyticExprs.size());

      // If needed, create the intermediate tuple first to maintain
      // intermediateTupleId < outputTupleId for debugging purposes and consistency with
      // tuple creation for aggregations.
      boolean requiresIntermediateTuple =
          AggregateInfoBase.requiresIntermediateTuple(analyticFnCalls);
      if (requiresIntermediateTuple) {
        physicalIntermediateTuple = analyzer.getDescTbl().createTupleDescriptor();
        physicalOutputTuple = analyzer.getDescTbl().createTupleDescriptor();
      } else {
        physicalOutputTuple = analyzer.getDescTbl().createTupleDescriptor();
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
    List<Expr> analyticExprs = analyticInfo_.getAnalyticExprs();
    List<WindowGroup> groups = Lists.newArrayList();
    for (int i = 0; i < analyticExprs.size(); ++i) {
      AnalyticExpr analyticExpr = (AnalyticExpr) analyticExprs.get(i);
      // Do not generate the plan for non-materialized analytic exprs.
      if (!analyticInfo_.getOutputTupleDesc().getSlots().get(i).isMaterialized()) {
        continue;
      }
      boolean match = false;
      for (WindowGroup group: groups) {
        if (group.isCompatible(analyticExpr)) {
          group.add((AnalyticExpr) analyticInfo_.getAnalyticExprs().get(i),
              analyticInfo_.getOutputTupleDesc().getSlots().get(i),
              analyticInfo_.getIntermediateTupleDesc().getSlots().get(i));
          match = true;
          break;
        }
      }
      if (!match) {
        groups.add(new WindowGroup(
            (AnalyticExpr) analyticInfo_.getAnalyticExprs().get(i),
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
    public List<WindowGroup> windowGroups = Lists.newArrayList();

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
      public int compare(WindowGroup wg1, WindowGroup wg2) {
        Preconditions.checkState(wg1.physicalIntermediateTuple != null
            && wg1.physicalIntermediateTuple.getByteSize() != -1);
        Preconditions.checkState(wg2.physicalIntermediateTuple != null
            && wg2.physicalIntermediateTuple.getByteSize() != -1);
        int diff = wg1.physicalIntermediateTuple.getByteSize()
            - wg2.physicalIntermediateTuple.getByteSize();
        return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
      }
    }

    private static final SizeLt SIZE_LT;
    static {
      SIZE_LT = new SizeLt();
    }

    /**
     * Order window groups by increasing size of the intermediate tuple. This minimizes
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
     * Order sort groups by increasing totalOutputTupleSize. This minimizes the total
     * volume of data that needs to be sorted.
     */
    public void orderSortGroups() {
      Collections.sort(sortGroups,
          new Comparator<SortGroup>() {
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
