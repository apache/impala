// Copyright 2016 Cloudera Inc.
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.ExprSubstitutionMap;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.IdGenerator;
import com.cloudera.impala.planner.PlanNode;
import com.cloudera.impala.thrift.TRuntimeFilterDesc;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used for generating and assigning runtime filters to a query plan using
 * runtime filter propagation. Runtime filter propagation is an optimization technique
 * used to filter scanned tuples or scan ranges based on information collected at
 * runtime. A runtime filter is constructed during the build phase of a join node, and is
 * applied at a scan node on the probe side of that join node. Runtime filters are
 * generated from equi-join predicates but they do not replace the original predicates.
 *
 * Example: select * from T1, T2 where T1.a = T2.b and T2.c = '1';
 * Assuming that T1 is a fact table and T2 is a significantly smaller dimension table, a
 * runtime filter is constructed at the join node between tables T1 and T2 while building
 * the hash table on the values of T2.b (rhs of the join condition) from the tuples of T2
 * that satisfy predicate T2.c = '1'. The runtime filter is subsequently sent to the
 * scan node of table T1 and is applied on the values of T1.a (lhs of the join condition)
 * to prune tuples of T2 that cannot be part of the join result.
 *
 * TODO: Consider combining multiple filters, that are applied to the same scan node,
 * into a single filter.
 */
public final class RuntimeFilterGenerator {
  private final static Logger LOG =
      LoggerFactory.getLogger(RuntimeFilterGenerator.class);

  // Map of base table tuple ids to a list of runtime filters that
  // can be applied at the corresponding scan nodes.
  private final Map<TupleId, List<RuntimeFilter>> runtimeFiltersByTid_ =
      Maps.newHashMap();

  // Generator for filter ids
  private final IdGenerator<RuntimeFilterId> filterIdGenerator =
      RuntimeFilterId.createGenerator();

  private RuntimeFilterGenerator() {};

  /**
   * Internal representation of a runtime filter. A runtime filter is generated from
   * an equi-join predicate of the form <lhs_expr> = <rhs_expr>, where lhs_expr is the
   * expr on which the filter is applied and must be bound by a single tuple id from
   * the left plan subtree of the associated join node, while rhs_expr is the expr on
   * which the filter is built and can be bound by any number of tuple ids from the
   * right plan subtree. Every runtime filter must record the join node that constructs
   * the filter and the scan node that applies the filter (destination node).
   */
  public static class RuntimeFilter {
    // Identifier of the filter (unique within a query)
    private final RuntimeFilterId id_;
    // Join node that builds the filter
    private final JoinNode src_;
    // Scan node that applies the filter
    private ScanNode target_;
    // Expr (rhs of join predicate) on which the filter is built
    private final Expr srcExpr_;
    // Expr (lhs of join predicate) on which the filter is applied
    private Expr targetExpr_;
    // Slots from base table tuples that are in the same equivalent classes as the slots
    // of 'targetExpr_'. The slots are grouped by tuple id.
    private Map<TupleId, List<SlotId>> targetSlotsByTid_;
    // If true, the join node building this filter is executed using a broadcast join;
    // set in the DistributedPlanner.createHashJoinFragment()
    private boolean isBroadcastJoin_;
    // If true, targetExpr_ is bound by only partition columns
    private boolean isBoundByPartitionColumns_;
    // If true, the filter is produced by a broadcast join and is applied on a scan which
    // is in the same plan fragment as the join; set in
    // DistributedPlanner.createHashJoinFragment().
    private boolean hasLocalTarget_ = false;

    private RuntimeFilter(RuntimeFilterId filterId, JoinNode filterSrcNode,
        Expr srcExpr, Expr targetExpr, Map<TupleId, List<SlotId>> targetSlots) {
      id_ = filterId;
      src_ = filterSrcNode;
      srcExpr_ = srcExpr;
      targetExpr_ = targetExpr;
      targetSlotsByTid_ = targetSlots;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof RuntimeFilter)) return false;
      return ((RuntimeFilter) obj).id_.equals(id_);
    }

    @Override
    public int hashCode() { return id_.hashCode(); }

    public TRuntimeFilterDesc toThrift() {
      TRuntimeFilterDesc tFilter = new TRuntimeFilterDesc();
      tFilter.setFilter_id(id_.asInt());
      tFilter.setTarget_expr(targetExpr_.treeToThrift());
      tFilter.setSrc_expr(srcExpr_.treeToThrift());
      tFilter.setIs_bound_by_partition_columns(isBoundByPartitionColumns_);
      tFilter.setIs_broadcast_join(isBroadcastJoin_);
      tFilter.setHas_local_target(hasLocalTarget_);
      List<SlotId> sids = Lists.newArrayList();
      targetExpr_.getIds(null, sids);
      List<Integer> tSlotIds = Lists.newArrayList();
      for (SlotId sid: sids) tSlotIds.add(sid.asInt());
      tFilter.setTarget_expr_slotids(tSlotIds);
      return tFilter;
    }

    /**
     * Static function to create a RuntimeFilter from 'joinPredicate' that is assigned
     * to the join node 'filterSrcNode'. Returns an instance of RuntimeFilter
     * or null if a runtime filter cannot be generated from the specified predicate.
     */
    public static RuntimeFilter create(IdGenerator<RuntimeFilterId> idGen,
        Analyzer analyzer, Expr joinPredicate, JoinNode filterSrcNode) {
      Preconditions.checkNotNull(idGen);
      Preconditions.checkNotNull(joinPredicate);
      Preconditions.checkNotNull(filterSrcNode);
      // Only consider binary equality predicates
      if (!Predicate.isEquivalencePredicate(joinPredicate)) return null;

      BinaryPredicate normalizedJoinConjunct =
          SingleNodePlanner.getNormalizedEqPred(joinPredicate,
              filterSrcNode.getChild(0).getTupleIds(),
              filterSrcNode.getChild(1).getTupleIds(), analyzer);
      if (normalizedJoinConjunct == null) return null;

      Expr targetExpr = normalizedJoinConjunct.getChild(0);
      Expr srcExpr = normalizedJoinConjunct.getChild(1);

      Map<TupleId, List<SlotId>> targetSlots = getTargetSlots(analyzer, targetExpr);
      Preconditions.checkNotNull(targetSlots);
      if (targetSlots.isEmpty()) return null;

      LOG.trace("Generating runtime filter from predicate " + joinPredicate);
      return new RuntimeFilter(idGen.getNextId(), filterSrcNode,
          srcExpr, targetExpr, targetSlots);
    }

    /**
     * Returns the ids of base table tuple slots on which a runtime filter expr can be
     * applied. Due to the existence of equivalence classes, a filter expr may be
     * applicable at multiple scan nodes. The returned slot ids are grouped by tuple id.
     * Returns an empty collection if the filter expr cannot be applied at a base table.
     */
    private static Map<TupleId, List<SlotId>> getTargetSlots(Analyzer analyzer,
        Expr expr) {
      // 'expr' is not a SlotRef and may contain multiple SlotRefs
      List<TupleId> tids = Lists.newArrayList();
      List<SlotId> sids = Lists.newArrayList();
      expr.getIds(tids, sids);
      Map<TupleId, List<SlotId>> slotsByTid = Maps.newHashMap();
      // We need to iterate over all the slots of 'expr' and check if they have
      // equivalent slots that are bound by the same base table tuple(s).
      for (SlotId slotId: sids) {
        Map<TupleId, List<SlotId>> currSlotsByTid =
            getBaseTblEquivSlots(analyzer, slotId);
        if (currSlotsByTid.isEmpty()) return Collections.emptyMap();
        if (slotsByTid.isEmpty()) {
          slotsByTid.putAll(currSlotsByTid);
          continue;
        }

        // Compute the intersection between tuple ids from 'slotsByTid' and
        // 'currSlotsByTid'. If the intersection is empty, an empty collection
        // is returned.
        Iterator<Map.Entry<TupleId, List<SlotId>>> iter =
            slotsByTid.entrySet().iterator();
        while (iter.hasNext()) {
          Map.Entry<TupleId, List<SlotId>> entry = iter.next();
          List<SlotId> slotIds = currSlotsByTid.get(entry.getKey());
          if (slotIds == null) {
            iter.remove();
          } else {
            entry.getValue().addAll(slotIds);
          }
        }
        if (slotsByTid.isEmpty()) return Collections.emptyMap();
      }
      return slotsByTid;
    }

    /**
     * Static function that returns the ids of slots bound by base table tuples for which
     * there is a value transfer from 'slotId'. The slots are grouped by tuple id.
     */
    private static Map<TupleId, List<SlotId>> getBaseTblEquivSlots(Analyzer analyzer,
        SlotId slotId) {
      Map<TupleId, List<SlotId>> slotsByTid = Maps.newHashMap();
      for (SlotId equivSlotId: analyzer.getEquivSlots(slotId)) {
        TupleDescriptor tupleDesc = analyzer.getSlotDesc(equivSlotId).getParent();
        if (tupleDesc.getTable() == null
            || !analyzer.hasValueTransfer(slotId, equivSlotId)) {
          continue;
        }
        List<SlotId> sids = slotsByTid.get(tupleDesc.getId());
        if (sids == null) {
          sids = Lists.newArrayList();
          slotsByTid.put(tupleDesc.getId(), sids);
        }
        sids.add(equivSlotId);
      }
      return slotsByTid;
    }

    public ScanNode getTarget() { return target_; }
    public boolean hasTarget() { return target_ != null; }
    public Expr getSrcExpr() { return srcExpr_; }
    public Expr getTargetExpr() { return targetExpr_; }
    public Map<TupleId, List<SlotId>> getTargetSlots() { return targetSlotsByTid_; }
    public RuntimeFilterId getFilterId() { return id_; }

    public void setFilterTarget(ScanNode node, Analyzer analyzer) {
      target_ = node;
      // Check if all the slots of targetExpr_ are bound by partition columns
      TupleDescriptor baseTblDesc = node.getTupleDesc();
      Table tbl = baseTblDesc.getTable();
      if (tbl.getNumClusteringCols() == 0) {
        isBoundByPartitionColumns_ = false;
        return;
      }
      List<SlotId> sids = Lists.newArrayList();
      targetExpr_.getIds(null, sids);
      for (SlotId sid: sids) {
        SlotDescriptor slotDesc = analyzer.getSlotDesc(sid);
        if (slotDesc.getColumn() == null
            || slotDesc.getColumn().getPosition() >= tbl.getNumClusteringCols()) {
          isBoundByPartitionColumns_ = false;
          return;
        }
      }
      isBoundByPartitionColumns_ = true;
    }

    public void setTargetExpr(Expr expr) {
      Preconditions.checkNotNull(expr);
      targetExpr_ = expr;
    }

    public void setIsBroadcast(boolean isBroadcast) { isBroadcastJoin_ = isBroadcast; }

    public void setHasLocalTarget(boolean hasLocalTarget) {
      hasLocalTarget_ = hasLocalTarget;
    }

    /**
     * Assigns this runtime filter to the corresponding plan nodes.
     */
    public void assignToPlanNodes() {
      Preconditions.checkNotNull(target_);
      src_.addRuntimeFilter(this);
      target_.addRuntimeFilter(this);
    }

    public String debugString() {
      StringBuilder output = new StringBuilder();
      output.append("FilterID: " + id_ + " ");
      output.append("Operator constructing the filter: " +
          src_.getId() + " ");
      output.append("Operator applying the filter: " + target_.getId() +
          " ");
      output.append("SrcExpr: " + getSrcExpr().debugString() +  " ");
      output.append("TargetExpr: " + getTargetExpr().debugString());
      return output.toString();
    }
  }

  /**
   * Generates and assigns runtime filters to a query plan tree.
   */
  public static void generateRuntimeFilters(Analyzer analyzer, PlanNode plan) {
    RuntimeFilterGenerator filterGenerator = new RuntimeFilterGenerator();
    filterGenerator.generateFilters(analyzer, plan);
  }

  /**
   * Generates the runtime filters for a query by recursively traversing the single-node
   * plan tree rooted at 'root'. In the top-down traversal of the plan tree, candidate
   * runtime filters are generated from equi-join predicates. In the bottom-up traversal
   * of the plan tree, the filters are assigned to destination (scan) nodes. Filters
   * that cannot be assigned to a scan node are discarded.
   */
  private void generateFilters(Analyzer analyzer, PlanNode root) {
    if (root instanceof JoinNode) {
      JoinNode joinNode = (JoinNode) root;
      List<Expr> joinConjuncts = Lists.newArrayList();
      if (!joinNode.getJoinOp().isLeftOuterJoin()
          && !joinNode.getJoinOp().isFullOuterJoin()
          && !joinNode.getJoinOp().isAntiJoin()) {
        // It's not correct to push runtime filters to the left side of a left outer,
        // full outer or anti join if the filter corresponds to an equi-join predicate
        // from the ON clause.
        joinConjuncts.addAll(joinNode.getEqJoinConjuncts());
      }
      joinConjuncts.addAll(joinNode.getConjuncts());
      List<RuntimeFilter> filters = Lists.newArrayList();
      for (Expr conjunct: joinConjuncts) {
        RuntimeFilter filter = RuntimeFilter.create(filterIdGenerator, analyzer,
            conjunct, joinNode);
        if (filter == null) continue;
        registerRuntimeFilter(filter);
        filters.add(filter);
      }
      generateFilters(analyzer, root.getChild(0));
      // Unregister all the runtime filters for which no destination scan node could be
      // found in the left subtree of the join node. This is to ensure that we don't
      // assign a filter to a scan node from the right subtree of joinNode or ancestor
      // join nodes in case we don't find a destination node in the left subtree.
      for (RuntimeFilter runtimeFilter: filters) {
        if (!runtimeFilter.hasTarget()) unregisterRuntimeFilter(runtimeFilter);
      }
      generateFilters(analyzer, root.getChild(1));
    } else if (root instanceof ScanNode) {
      assignRuntimeFilters(analyzer, (ScanNode) root);
    } else {
      for (PlanNode childNode: root.getChildren()) {
        generateFilters(analyzer, childNode);
      }
    }
  }

  /**
   * Registers a runtime filter with the tuple id of every scan node that is a candidate
   * destination node for that filter.
   */
  private void registerRuntimeFilter(RuntimeFilter filter) {
    Map<TupleId, List<SlotId>> targetSlotsByTid = filter.getTargetSlots();
    Preconditions.checkState(targetSlotsByTid != null && !targetSlotsByTid.isEmpty());
    for (TupleId tupleId: targetSlotsByTid.keySet()) {
      List<RuntimeFilter> filters = runtimeFiltersByTid_.get(tupleId);
      if (filters == null) {
        filters = Lists.newArrayList();
        runtimeFiltersByTid_.put(tupleId, filters);
      }
      filters.add(filter);
    }
  }

  private void unregisterRuntimeFilter(RuntimeFilter runtimeFilter) {
    for (TupleId tupleId: runtimeFilter.getTargetSlots().keySet()) {
      runtimeFiltersByTid_.get(tupleId).remove(runtimeFilter);
    }
  }

  /**
   * Assigns runtime filters to a specific scan node 'scanNode'.
   * The assigned filters are the ones for which 'scanNode' can be used a destination
   * node. A scan node may be used as a destination node for multiple runtime filters.
   */
  private void assignRuntimeFilters(Analyzer analyzer, ScanNode scanNode) {
    Preconditions.checkNotNull(scanNode);
    TupleId tid = scanNode.getTupleIds().get(0);
    // Return if no runtime filter is associated with this scan tuple.
    if (!runtimeFiltersByTid_.containsKey(tid)) return;
    for (RuntimeFilter filter: runtimeFiltersByTid_.get(tid)) {
      if (filter.getTarget() != null) continue;
      if (!filter.getTargetExpr().isBound(tid)) {
        Preconditions.checkState(filter.getTargetSlots().containsKey(tid));
        // Modify the filter target expr using the equivalent slots from the scan node
        // on which the filter will be applied.
        ExprSubstitutionMap smap = new ExprSubstitutionMap();
        Expr targetExpr = filter.getTargetExpr();
        List<SlotRef> exprSlots = Lists.newArrayList();
        targetExpr.collect(SlotRef.class, exprSlots);
        List<SlotId> sids = filter.getTargetSlots().get(tid);
        for (SlotRef slotRef: exprSlots) {
          for (SlotId sid: sids) {
            if (analyzer.hasValueTransfer(slotRef.getSlotId(), sid)) {
              SlotRef newSlotRef = new SlotRef(analyzer.getSlotDesc(sid));
              newSlotRef.analyzeNoThrow(analyzer);
              smap.put(slotRef, newSlotRef);
              break;
            }
          }
        }
        Preconditions.checkState(exprSlots.size() == smap.size());
        filter.setTargetExpr(targetExpr.substitute(smap, analyzer, false));
      }
      filter.setFilterTarget(scanNode, analyzer);
      filter.assignToPlanNodes();
    }
  }
}
