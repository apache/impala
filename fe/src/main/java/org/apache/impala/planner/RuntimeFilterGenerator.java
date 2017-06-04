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

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.Predicate;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.analysis.TupleIsNullPredicate;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.IdGenerator;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.thrift.TRuntimeFilterDesc;
import org.apache.impala.thrift.TRuntimeFilterTargetDesc;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used for generating and assigning runtime filters to a query plan using
 * runtime filter propagation. Runtime filter propagation is an optimization technique
 * used to filter scanned tuples or scan ranges based on information collected at
 * runtime. A runtime filter is constructed during the build phase of a join node, and is
 * applied at, potentially, multiple scan nodes on the probe side of that join node.
 * Runtime filters are generated from equi-join predicates but they do not replace the
 * original predicates.
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
   * the filter and the scan nodes that apply the filter (destination nodes).
   */
  public static class RuntimeFilter {
    // Identifier of the filter (unique within a query)
    private final RuntimeFilterId id_;
    // Join node that builds the filter
    private final JoinNode src_;
    // Expr (rhs of join predicate) on which the filter is built
    private final Expr srcExpr_;
    // Expr (lhs of join predicate) from which the targetExprs_ are generated.
    private final Expr origTargetExpr_;
    // Runtime filter targets
    private final List<RuntimeFilterTarget> targets_ = Lists.newArrayList();
    // Slots from base table tuples that have value transfer from the slots
    // of 'origTargetExpr_'. The slots are grouped by tuple id.
    private final Map<TupleId, List<SlotId>> targetSlotsByTid_;
    // If true, the join node building this filter is executed using a broadcast join;
    // set in the DistributedPlanner.createHashJoinFragment()
    private boolean isBroadcastJoin_;
    // Estimate of the number of distinct values that will be inserted into this filter,
    // globally across all instances of the source node. Used to compute an optimal size
    // for the filter. A value of -1 means no estimate is available, and default filter
    // parameters should be used.
    private long ndvEstimate_ = -1;
    // If true, the filter is produced by a broadcast join and there is at least one
    // destination scan node which is in the same fragment as the join; set in
    // DistributedPlanner.createHashJoinFragment().
    private boolean hasLocalTargets_ = false;
    // If true, there is at least one destination scan node which is not in the same
    // fragment as the join that produced the filter; set in
    // DistributedPlanner.createHashJoinFragment().
    private boolean hasRemoteTargets_ = false;
    // If set, indicates that the filter can't be assigned to another scan node.
    // Once set, it can't be unset.
    private boolean finalized_ = false;

    /**
     * Internal representation of a runtime filter target.
     */
    private static class RuntimeFilterTarget {
      // Scan node that applies the filter
      public ScanNode node;
      // Expr on which the filter is applied
      public Expr expr;
      // Indicates if 'expr' is bound only by partition columns
      public boolean isBoundByPartitionColumns = false;
      // Indicates if 'node' is in the same fragment as the join that produces the
      // filter
      public boolean isLocalTarget = false;

      public RuntimeFilterTarget(ScanNode targetNode, Expr targetExpr) {
        node = targetNode;
        expr = targetExpr;
      }

      public TRuntimeFilterTargetDesc toThrift() {
        TRuntimeFilterTargetDesc tFilterTarget = new TRuntimeFilterTargetDesc();
        tFilterTarget.setNode_id(node.getId().asInt());
        tFilterTarget.setTarget_expr(expr.treeToThrift());
        List<SlotId> sids = Lists.newArrayList();
        expr.getIds(null, sids);
        List<Integer> tSlotIds = Lists.newArrayListWithCapacity(sids.size());
        for (SlotId sid: sids) tSlotIds.add(sid.asInt());
        tFilterTarget.setTarget_expr_slotids(tSlotIds);
        tFilterTarget.setIs_bound_by_partition_columns(isBoundByPartitionColumns);
        tFilterTarget.setIs_local_target(isLocalTarget);
        return tFilterTarget;
      }

      @Override
      public String toString() {
        StringBuilder output = new StringBuilder();
        return output.append("Target Id: " + node.getId() + " ")
            .append("Target expr: " + expr.debugString() + " ")
            .append("Partition columns: " + isBoundByPartitionColumns)
            .append("Is local: " + isLocalTarget)
            .toString();
      }
    }

    private RuntimeFilter(RuntimeFilterId filterId, JoinNode filterSrcNode,
        Expr srcExpr, Expr origTargetExpr, Map<TupleId, List<SlotId>> targetSlots) {
      id_ = filterId;
      src_ = filterSrcNode;
      srcExpr_ = srcExpr;
      origTargetExpr_ = origTargetExpr;
      targetSlotsByTid_ = targetSlots;
      computeNdvEstimate();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof RuntimeFilter)) return false;
      return ((RuntimeFilter) obj).id_.equals(id_);
    }

    @Override
    public int hashCode() { return id_.hashCode(); }

    public void markFinalized() { finalized_ = true; }
    public boolean isFinalized() { return finalized_; }

    /**
     * Serializes a runtime filter to Thrift.
     */
    public TRuntimeFilterDesc toThrift() {
      TRuntimeFilterDesc tFilter = new TRuntimeFilterDesc();
      tFilter.setFilter_id(id_.asInt());
      tFilter.setSrc_expr(srcExpr_.treeToThrift());
      tFilter.setIs_broadcast_join(isBroadcastJoin_);
      tFilter.setNdv_estimate(ndvEstimate_);
      tFilter.setHas_local_targets(hasLocalTargets_);
      tFilter.setHas_remote_targets(hasRemoteTargets_);
      boolean appliedOnPartitionColumns = true;
      for (int i = 0; i < targets_.size(); ++i) {
        RuntimeFilterTarget target = targets_.get(i);
        tFilter.addToTargets(target.toThrift());
        tFilter.putToPlanid_to_target_ndx(target.node.getId().asInt(), i);
        appliedOnPartitionColumns =
            appliedOnPartitionColumns && target.isBoundByPartitionColumns;
      }
      tFilter.setApplied_on_partition_columns(appliedOnPartitionColumns);
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

      // Ensure that the targer expr does not contain TupleIsNull predicates as these
      // can't be evaluated at a scan node.
      targetExpr = TupleIsNullPredicate.unwrapExpr(targetExpr.clone());
      if (LOG.isTraceEnabled()) {
        LOG.trace("Generating runtime filter from predicate " + joinPredicate);
      }
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
     * there is a value transfer from 'srcSid'. The slots are grouped by tuple id.
     */
    private static Map<TupleId, List<SlotId>> getBaseTblEquivSlots(Analyzer analyzer,
        SlotId srcSid) {
      Map<TupleId, List<SlotId>> slotsByTid = Maps.newHashMap();
      for (SlotId targetSid: analyzer.getValueTransferTargets(srcSid)) {
        TupleDescriptor tupleDesc = analyzer.getSlotDesc(targetSid).getParent();
        if (tupleDesc.getTable() == null) continue;
        List<SlotId> sids = slotsByTid.get(tupleDesc.getId());
        if (sids == null) {
          sids = Lists.newArrayList();
          slotsByTid.put(tupleDesc.getId(), sids);
        }
        sids.add(targetSid);
      }
      return slotsByTid;
    }

    public Expr getTargetExpr(PlanNodeId targetPlanNodeId) {
      for (RuntimeFilterTarget target: targets_) {
        if (target.node.getId() != targetPlanNodeId) continue;
        return target.expr;
      }
      return null;
    }

    public List<RuntimeFilterTarget> getTargets() { return targets_; }
    public boolean hasTargets() { return !targets_.isEmpty(); }
    public Expr getSrcExpr() { return srcExpr_; }
    public Expr getOrigTargetExpr() { return origTargetExpr_; }
    public Map<TupleId, List<SlotId>> getTargetSlots() { return targetSlotsByTid_; }
    public RuntimeFilterId getFilterId() { return id_; }

    /**
     * Estimates the selectivity of a runtime filter as the cardinality of the
     * associated source join node over the cardinality of that join node's left
     * child.
     */
    public double getSelectivity() {
      if (src_.getCardinality() == -1
          || src_.getChild(0).getCardinality() == -1
          || src_.getChild(0).getCardinality() == 0) {
        return -1;
      }
      return src_.getCardinality() / (double) src_.getChild(0).getCardinality();
    }

    public void addTarget(ScanNode node, Analyzer analyzer, Expr targetExpr) {
      Preconditions.checkState(targetExpr.isBoundByTupleIds(node.getTupleIds()));
      RuntimeFilterTarget target = new RuntimeFilterTarget(node, targetExpr);
      targets_.add(target);
      // Check if all the slots of targetExpr_ are bound by partition columns
      TupleDescriptor baseTblDesc = node.getTupleDesc();
      Table tbl = baseTblDesc.getTable();
      if (tbl.getNumClusteringCols() == 0) return;
      List<SlotId> sids = Lists.newArrayList();
      targetExpr.getIds(null, sids);
      for (SlotId sid: sids) {
        SlotDescriptor slotDesc = analyzer.getSlotDesc(sid);
        if (slotDesc.getColumn() == null
            || slotDesc.getColumn().getPosition() >= tbl.getNumClusteringCols()) {
          return;
        }
      }
      target.isBoundByPartitionColumns = true;
    }

    public void setIsBroadcast(boolean isBroadcast) { isBroadcastJoin_ = isBroadcast; }

    public void computeNdvEstimate() { ndvEstimate_ = src_.getChild(1).getCardinality(); }

    public void computeHasLocalTargets() {
      Preconditions.checkNotNull(src_.getFragment());
      Preconditions.checkState(hasTargets());
      for (RuntimeFilterTarget target: targets_) {
        Preconditions.checkNotNull(target.node.getFragment());
        boolean isLocal =
            src_.getFragment().getId().equals(target.node.getFragment().getId());
        target.isLocalTarget = isLocal;
        hasLocalTargets_ = hasLocalTargets_ || isLocal;
        hasRemoteTargets_ = hasRemoteTargets_ || !isLocal;
      }
    }

    /**
     * Assigns this runtime filter to the corresponding plan nodes.
     */
    public void assignToPlanNodes() {
      Preconditions.checkState(hasTargets());
      src_.addRuntimeFilter(this);
      for (RuntimeFilterTarget target: targets_) target.node.addRuntimeFilter(this);
    }

    public String debugString() {
      StringBuilder output = new StringBuilder();
      return output.append("FilterID: " + id_ + " ")
          .append("Source: " + src_.getId() + " ")
          .append("SrcExpr: " + getSrcExpr().debugString() +  " ")
          .append("Target(s): ")
          .append(Joiner.on(", ").join(targets_) + " ")
          .append("Selectivity: " + getSelectivity()).toString();
    }
  }

  /**
   * Generates and assigns runtime filters to a query plan tree.
   */
  public static void generateRuntimeFilters(Analyzer analyzer, PlanNode plan,
      int maxNumFilters) {
    Preconditions.checkArgument(maxNumFilters >= 0);
    RuntimeFilterGenerator filterGenerator = new RuntimeFilterGenerator();
    filterGenerator.generateFilters(analyzer, plan);
    List<RuntimeFilter> filters = Lists.newArrayList(filterGenerator.getRuntimeFilters());
    if (filters.size() > maxNumFilters) {
      // If more than 'maxNumFilters' were generated, sort them by increasing selectivity
      // and keep the 'maxNumFilters' most selective.
      Collections.sort(filters, new Comparator<RuntimeFilter>() {
          public int compare(RuntimeFilter a, RuntimeFilter b) {
            double aSelectivity =
                a.getSelectivity() == -1 ? Double.MAX_VALUE : a.getSelectivity();
            double bSelectivity =
                b.getSelectivity() == -1 ? Double.MAX_VALUE : b.getSelectivity();
            return Double.compare(aSelectivity, bSelectivity);
          }
        }
      );
    }
    for (RuntimeFilter filter:
         filters.subList(0, Math.min(filters.size(), maxNumFilters))) {
      if (LOG.isTraceEnabled()) LOG.trace("Runtime filter: " + filter.debugString());
      filter.assignToPlanNodes();
    }
  }

  /**
   * Returns a list of all the registered runtime filters, ordered by filter ID.
   */
  public List<RuntimeFilter> getRuntimeFilters() {
    Set<RuntimeFilter> resultSet = Sets.newHashSet();
    for (List<RuntimeFilter> filters: runtimeFiltersByTid_.values()) {
      resultSet.addAll(filters);
    }
    List<RuntimeFilter> resultList = Lists.newArrayList(resultSet);
    Collections.sort(resultList, new Comparator<RuntimeFilter>() {
        public int compare(RuntimeFilter a, RuntimeFilter b) {
          return a.getFilterId().compareTo(b.getFilterId());
        }
      }
    );
    return resultList;
  }

  /**
   * Generates the runtime filters for a query by recursively traversing the single-node
   * plan tree rooted at 'root'. In the top-down traversal of the plan tree, candidate
   * runtime filters are generated from equi-join predicates assigned to hash-join nodes.
   * In the bottom-up traversal of the plan tree, the filters are assigned to destination
   * (scan) nodes. Filters that cannot be assigned to a scan node are discarded.
   */
  private void generateFilters(Analyzer analyzer, PlanNode root) {
    if (root instanceof HashJoinNode) {
      HashJoinNode joinNode = (HashJoinNode) root;
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
      // Finalize every runtime filter of that join. This is to ensure that we don't
      // assign a filter to a scan node from the right subtree of joinNode or ancestor
      // join nodes in case we don't find a destination node in the left subtree.
      for (RuntimeFilter runtimeFilter: filters) finalizeRuntimeFilter(runtimeFilter);
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
      registerRuntimeFilter(filter, tupleId);
    }
  }

  /**
   * Registers a runtime filter with a specific target tuple id.
   */
  private void registerRuntimeFilter(RuntimeFilter filter, TupleId targetTid) {
    Preconditions.checkState(filter.getTargetSlots().containsKey(targetTid));
    List<RuntimeFilter> filters = runtimeFiltersByTid_.get(targetTid);
    if (filters == null) {
      filters = Lists.newArrayList();
      runtimeFiltersByTid_.put(targetTid, filters);
    }
    Preconditions.checkState(!filter.isFinalized());
    filters.add(filter);
  }

  /**
   * Finalizes a runtime filter by disassociating it from all the candidate target scan
   * nodes that haven't been used as destinations for that filter. Also sets the
   * finalized_ flag of that filter so that it can't be assigned to any other scan nodes.
   */
  private void finalizeRuntimeFilter(RuntimeFilter runtimeFilter) {
    Set<TupleId> targetTupleIds = Sets.newHashSet();
    for (RuntimeFilter.RuntimeFilterTarget target: runtimeFilter.getTargets()) {
      targetTupleIds.addAll(target.node.getTupleIds());
    }
    for (TupleId tupleId: runtimeFilter.getTargetSlots().keySet()) {
      if (!targetTupleIds.contains(tupleId)) {
        runtimeFiltersByTid_.get(tupleId).remove(runtimeFilter);
      }
    }
    runtimeFilter.markFinalized();
  }

  /**
   * Assigns runtime filters to a specific scan node 'scanNode'.
   * The assigned filters are the ones for which 'scanNode' can be used a destination
   * node. A scan node may be used as a destination node for multiple runtime filters.
   * Currently, runtime filters can only be assigned to HdfsScanNodes.
   */
  private void assignRuntimeFilters(Analyzer analyzer, ScanNode scanNode) {
    if (!(scanNode instanceof HdfsScanNode)) return;
    TupleId tid = scanNode.getTupleIds().get(0);
    if (!runtimeFiltersByTid_.containsKey(tid)) return;
    for (RuntimeFilter filter: runtimeFiltersByTid_.get(tid)) {
      if (filter.isFinalized()) continue;
      Expr targetExpr = computeTargetExpr(filter, tid, analyzer);
      if (targetExpr == null) continue;
      filter.addTarget(scanNode, analyzer, targetExpr);
    }
  }

  /**
   * Computes the target expr for a specified runtime filter 'filter' to be applied at
   * the scan node with target tuple descriptor 'targetTid'.
   */
  private Expr computeTargetExpr(RuntimeFilter filter, TupleId targetTid,
      Analyzer analyzer) {
    Expr targetExpr = filter.getOrigTargetExpr();
    if (!targetExpr.isBound(targetTid)) {
      Preconditions.checkState(filter.getTargetSlots().containsKey(targetTid));
      // Modify the filter target expr using the equivalent slots from the scan node
      // on which the filter will be applied.
      ExprSubstitutionMap smap = new ExprSubstitutionMap();
      List<SlotRef> exprSlots = Lists.newArrayList();
      targetExpr.collect(SlotRef.class, exprSlots);
      List<SlotId> sids = filter.getTargetSlots().get(targetTid);
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
      try {
        targetExpr = targetExpr.substitute(smap, analyzer, true);
      } catch (Exception e) {
        // An exception is thrown if we cannot generate a target expr from this
        // scan node that has the same type as the lhs expr of the join predicate
        // from which the runtime filter was generated. We skip that scan node and will
        // try to assign the filter to a different scan node.
        //
        // TODO: Investigate if we can generate a type-compatible source/target expr
        // pair from that scan node instead of skipping it.
        return null;
      }
    }
    Preconditions.checkState(
        targetExpr.getType().matchesType(filter.getSrcExpr().getType()));
    return targetExpr;
  }
}
