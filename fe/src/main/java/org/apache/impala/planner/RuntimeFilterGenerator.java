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
import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.analysis.CastExpr;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.IsNullPredicate;
import org.apache.impala.analysis.Predicate;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.analysis.TupleIsNullPredicate;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.IdGenerator;
import org.apache.impala.common.InternalException;
import org.apache.impala.planner.JoinNode.DistributionMode;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TRuntimeFilterDesc;
import org.apache.impala.thrift.TRuntimeFilterMode;
import org.apache.impala.thrift.TRuntimeFilterTargetDesc;
import org.apache.impala.thrift.TRuntimeFilterType;
import org.apache.impala.util.BitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Class used for generating and assigning runtime filters to a query plan using
 * runtime filter propagation. Runtime filter propagation is an optimization technique
 * used to filter scanned tuples or scan ranges based on information collected at
 * runtime. A runtime filter is constructed during the build phase of a join node, and is
 * applied at, potentially, multiple scan nodes on the probe side of that join node.
 * Runtime filters are generated from equi-join predicates but they do not replace the
 * original predicates.
 *
 * MinMax filters are of a fixed size (except for those used for string type) and
 * therefore only sizes for bloom filters need to be calculated. These calculations are
 * based on the NDV estimates of the associated table columns, the min buffer size that
 * can be allocated by the bufferpool, and the query options. Moreover, it is also bound
 * by the MIN/MAX_BLOOM_FILTER_SIZE limits which are enforced on the query options before
 * this phase of planning.
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

  // Should be in sync with corresponding values in runtime-filter-bank.cc.
  private static final long MIN_BLOOM_FILTER_SIZE = 4 * 1024;
  private static final long MAX_BLOOM_FILTER_SIZE = 512 * 1024 * 1024;

  // Map of base table tuple ids to a list of runtime filters that
  // can be applied at the corresponding scan nodes.
  private final Map<TupleId, List<RuntimeFilter>> runtimeFiltersByTid_ =
      Maps.newHashMap();

  // Generator for filter ids
  private final IdGenerator<RuntimeFilterId> filterIdGenerator =
      RuntimeFilterId.createGenerator();

  /**
   * Internal class that encapsulates the max, min and default sizes used for creating
   * bloom filter objects.
   */
  private class FilterSizeLimits {
    // Maximum filter size, in bytes, rounded up to a power of two.
    public final long maxVal;

    // Minimum filter size, in bytes, rounded up to a power of two.
    public final long minVal;

    // Pre-computed default filter size, in bytes, rounded up to a power of two.
    public final long defaultVal;

    public FilterSizeLimits(TQueryOptions tQueryOptions) {
      // Round up all limits to a power of two and make sure filter size is more
      // than the min buffer size that can be allocated by the buffer pool.
      long maxLimit = tQueryOptions.getRuntime_filter_max_size();
      long minBufferSize = BackendConfig.INSTANCE.getMinBufferSize();
      maxVal = BitUtil.roundUpToPowerOf2(Math.max(maxLimit, minBufferSize));

      long minLimit = tQueryOptions.getRuntime_filter_min_size();
      minLimit = Math.max(minLimit, minBufferSize);
      // Make sure minVal <= defaultVal <= maxVal
      minVal = BitUtil.roundUpToPowerOf2(Math.min(minLimit, maxVal));

      long defaultValue = tQueryOptions.getRuntime_bloom_filter_size();
      defaultValue = Math.max(defaultValue, minVal);
      defaultVal = BitUtil.roundUpToPowerOf2(Math.min(defaultValue, maxVal));
    }
  };

  // Contains size limits for bloom filters.
  private FilterSizeLimits bloomFilterSizeLimits_;

  private RuntimeFilterGenerator(TQueryOptions tQueryOptions) {
    bloomFilterSizeLimits_ = new FilterSizeLimits(tQueryOptions);
  };

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
    // The operator comparing 'srcExpr_' and 'origTargetExpr_'.
    private final Operator exprCmpOp_;
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
    // Size of the filter (in Bytes). Should be greater than zero for bloom filters.
    private long filterSizeBytes_ = 0;
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
    // The type of filter to build.
    private final TRuntimeFilterType type_;

    /**
     * Internal representation of a runtime filter target.
     */
    private static class RuntimeFilterTarget {
      // Scan node that applies the filter
      public ScanNode node;
      // Expr on which the filter is applied
      public Expr expr;
      // Indicates if 'expr' is bound only by partition columns
      public final boolean isBoundByPartitionColumns;
      // Indicates if 'node' is in the same fragment as the join that produces the filter
      public final boolean isLocalTarget;

      public RuntimeFilterTarget(ScanNode targetNode, Expr targetExpr,
          boolean isBoundByPartitionColumns, boolean isLocalTarget) {
        Preconditions.checkState(targetExpr.isBoundByTupleIds(targetNode.getTupleIds()));
        node = targetNode;
        expr = targetExpr;
        this.isBoundByPartitionColumns = isBoundByPartitionColumns;
        this.isLocalTarget = isLocalTarget;
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
        if (node instanceof KuduScanNode) {
          // assignRuntimeFilters() only assigns KuduScanNode targets if the target expr
          // is a slot ref, possibly with an implicit cast, pointing to a column.
          SlotRef slotRef = expr.unwrapSlotRef(true);
          KuduColumn col = (KuduColumn) slotRef.getDesc().getColumn();
          tFilterTarget.setKudu_col_name(col.getKuduName());
          tFilterTarget.setKudu_col_type(col.getType().toThrift());
        }
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

    private RuntimeFilter(RuntimeFilterId filterId, JoinNode filterSrcNode, Expr srcExpr,
        Expr origTargetExpr, Operator exprCmpOp, Map<TupleId, List<SlotId>> targetSlots,
        TRuntimeFilterType type, FilterSizeLimits filterSizeLimits) {
      id_ = filterId;
      src_ = filterSrcNode;
      srcExpr_ = srcExpr;
      origTargetExpr_ = origTargetExpr;
      exprCmpOp_ = exprCmpOp;
      targetSlotsByTid_ = targetSlots;
      type_ = type;
      computeNdvEstimate();
      calculateFilterSize(filterSizeLimits);
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
      tFilter.setType(type_);
      tFilter.setFilter_size_bytes(filterSizeBytes_);
      return tFilter;
    }

    /**
     * Static function to create a RuntimeFilter from 'joinPredicate' that is assigned
     * to the join node 'filterSrcNode'. Returns an instance of RuntimeFilter
     * or null if a runtime filter cannot be generated from the specified predicate.
     */
    public static RuntimeFilter create(IdGenerator<RuntimeFilterId> idGen,
        Analyzer analyzer, Expr joinPredicate, JoinNode filterSrcNode,
        TRuntimeFilterType type, FilterSizeLimits filterSizeLimits) {
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

      // Ensure that the target expr does not contain TupleIsNull predicates as these
      // can't be evaluated at a scan node.
      Expr targetExpr =
          TupleIsNullPredicate.unwrapExpr(normalizedJoinConjunct.getChild(0).clone());
      Expr srcExpr = normalizedJoinConjunct.getChild(1);

      Map<TupleId, List<SlotId>> targetSlots = getTargetSlots(analyzer, targetExpr);
      Preconditions.checkNotNull(targetSlots);
      if (targetSlots.isEmpty()) return null;

      if (LOG.isTraceEnabled()) {
        LOG.trace("Generating runtime filter from predicate " + joinPredicate);
      }
      return new RuntimeFilter(idGen.getNextId(), filterSrcNode, srcExpr, targetExpr,
          normalizedJoinConjunct.getOp(), targetSlots, type, filterSizeLimits);
    }

    /**
     * Returns the ids of base table tuple slots on which a runtime filter expr can be
     * applied. Due to the existence of equivalence classes, a filter expr may be
     * applicable at multiple scan nodes. The returned slot ids are grouped by tuple id.
     * Returns an empty collection if the filter expr cannot be applied at a base table
     * or if applying the filter might lead to incorrect results.
     */
    private static Map<TupleId, List<SlotId>> getTargetSlots(Analyzer analyzer,
        Expr expr) {
      // 'expr' is not a SlotRef and may contain multiple SlotRefs
      List<TupleId> tids = Lists.newArrayList();
      List<SlotId> sids = Lists.newArrayList();
      expr.getIds(tids, sids);

      // IMPALA-6286: If the target expression evaluates to a non-NULL value for
      // outer-join non-matches, then assigning the filter below the nullable side of
      // an outer join may produce incorrect query results.
      // This check is conservative but correct to keep the code simple. In particular,
      // it would otherwise be difficult to identify incorrect runtime filter assignments
      // through outer-joined inline views because the 'expr' has already been fully
      // resolved. We rely on the value-transfer graph to check whether 'expr' could
      // potentially be assigned below an outer-joined inline view.
      if (analyzer.hasOuterJoinedValueTransferTarget(sids)) {
        Expr isNotNullPred = new IsNullPredicate(expr, true);
        isNotNullPred.analyzeNoThrow(analyzer);
        try {
          if (analyzer.isTrueWithNullSlots(isNotNullPred)) return Collections.emptyMap();
        } catch (InternalException e) {
          // Expr evaluation failed in the backend. Skip this runtime filter since we
          // cannot determine whether it is safe to assign it.
          LOG.warn("Skipping runtime filter because backend evaluation failed: "
              + isNotNullPred.toSql(), e);
          return Collections.emptyMap();
        }
      }

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
    public TRuntimeFilterType getType() { return type_; }
    public Operator getExprCompOp() { return exprCmpOp_; }
    public long getFilterSize() { return filterSizeBytes_; }

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

    public void addTarget(RuntimeFilterTarget target) { targets_.add(target); }

    public void setIsBroadcast(boolean isBroadcast) { isBroadcastJoin_ = isBroadcast; }

    public void computeNdvEstimate() { ndvEstimate_ = src_.getChild(1).getCardinality(); }

    public void computeHasLocalTargets() {
      Preconditions.checkNotNull(src_.getFragment());
      Preconditions.checkState(hasTargets());
      for (RuntimeFilterTarget target: targets_) {
        Preconditions.checkNotNull(target.node.getFragment());
        hasLocalTargets_ = hasLocalTargets_ || target.isLocalTarget;
        hasRemoteTargets_ = hasRemoteTargets_ || !target.isLocalTarget;
      }
    }

    /**
     * Sets the filter size (in bytes) required for a bloom filter to achieve the
     * configured maximum false-positive rate based on the expected NDV. Also bounds the
     * filter size between the max and minimum filter sizes supplied to it by
     * 'filterSizeLimits'.
     */
    private void calculateFilterSize(FilterSizeLimits filterSizeLimits) {
      if (type_ == TRuntimeFilterType.MIN_MAX) return;
      if (ndvEstimate_ == -1) {
        filterSizeBytes_ = filterSizeLimits.defaultVal;
        return;
      }
      double fpp = BackendConfig.INSTANCE.getMaxFilterErrorRate();
      int logFilterSize = FeSupport.GetMinLogSpaceForBloomFilter(ndvEstimate_, fpp);
      filterSizeBytes_ = 1L << logFilterSize;
      filterSizeBytes_ = Math.max(filterSizeBytes_, filterSizeLimits.minVal);
      filterSizeBytes_ = Math.min(filterSizeBytes_, filterSizeLimits.maxVal);
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
  public static void generateRuntimeFilters(PlannerContext ctx, PlanNode plan) {
    Preconditions.checkNotNull(ctx);
    Preconditions.checkNotNull(ctx.getQueryOptions());
    int maxNumBloomFilters = ctx.getQueryOptions().getMax_num_runtime_filters();
    Preconditions.checkState(maxNumBloomFilters >= 0);
    RuntimeFilterGenerator filterGenerator = new RuntimeFilterGenerator(
        ctx.getQueryOptions());
    filterGenerator.generateFilters(ctx, plan);
    List<RuntimeFilter> filters = Lists.newArrayList(filterGenerator.getRuntimeFilters());
    if (filters.size() > maxNumBloomFilters) {
      // If more than 'maxNumBloomFilters' were generated, sort them by increasing
      // selectivity and keep the 'maxNumBloomFilters' most selective bloom filters.
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
    // We only enforce a limit on the number of bloom filters as they are much more
    // heavy-weight than the other filter types.
    int numBloomFilters = 0;
    for (RuntimeFilter filter : filters) {
      if (filter.getType() == TRuntimeFilterType.BLOOM) {
        if (numBloomFilters >= maxNumBloomFilters) continue;
        ++numBloomFilters;
      }
      filter.setIsBroadcast(
          filter.src_.getDistributionMode() == DistributionMode.BROADCAST);
      filter.computeHasLocalTargets();
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
   * Generates the runtime filters for a query by recursively traversing the distributed
   * plan tree rooted at 'root'. In the top-down traversal of the plan tree, candidate
   * runtime filters are generated from equi-join predicates assigned to hash-join nodes.
   * In the bottom-up traversal of the plan tree, the filters are assigned to destination
   * (scan) nodes. Filters that cannot be assigned to a scan node are discarded.
   */
  private void generateFilters(PlannerContext ctx, PlanNode root) {
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
      for (TRuntimeFilterType type : TRuntimeFilterType.values()) {
        for (Expr conjunct : joinConjuncts) {
          RuntimeFilter filter = RuntimeFilter.create(filterIdGenerator,
              ctx.getRootAnalyzer(), conjunct, joinNode, type, bloomFilterSizeLimits_);
          if (filter == null) continue;
          registerRuntimeFilter(filter);
          filters.add(filter);
        }
      }
      generateFilters(ctx, root.getChild(0));
      // Finalize every runtime filter of that join. This is to ensure that we don't
      // assign a filter to a scan node from the right subtree of joinNode or ancestor
      // join nodes in case we don't find a destination node in the left subtree.
      for (RuntimeFilter runtimeFilter: filters) finalizeRuntimeFilter(runtimeFilter);
      generateFilters(ctx, root.getChild(1));
    } else if (root instanceof ScanNode) {
      assignRuntimeFilters(ctx, (ScanNode) root);
    } else {
      for (PlanNode childNode: root.getChildren()) {
        generateFilters(ctx, childNode);
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
   * The assigned filters are the ones for which 'scanNode' can be used as a destination
   * node. The following constraints are enforced when assigning filters to 'scanNode':
   * 1. If the DISABLE_ROW_RUNTIME_FILTERING query option is set, a filter is only
   *    assigned to 'scanNode' if the filter target expression is bound by partition
   *    columns.
   * 2. If the RUNTIME_FILTER_MODE query option is set to LOCAL, a filter is only assigned
   *    to 'scanNode' if the filter is produced within the same fragment that contains the
   *    scan node.
   * 3. Only Hdfs and Kudu scan nodes are supported:
   *     a. If the target is an HdfsScanNode, the filter must be type BLOOM.
   *     b. If the target is a KuduScanNode, the filter must be type MIN_MAX, the target
   *         must be a slot ref on a column, and the comp op cannot be 'not distinct'.
   * A scan node may be used as a destination node for multiple runtime filters.
   */
  private void assignRuntimeFilters(PlannerContext ctx, ScanNode scanNode) {
    if (!(scanNode instanceof HdfsScanNode || scanNode instanceof KuduScanNode)) return;
    TupleId tid = scanNode.getTupleIds().get(0);
    if (!runtimeFiltersByTid_.containsKey(tid)) return;
    Analyzer analyzer = ctx.getRootAnalyzer();
    boolean disableRowRuntimeFiltering =
        ctx.getQueryOptions().isDisable_row_runtime_filtering();
    TRuntimeFilterMode runtimeFilterMode = ctx.getQueryOptions().getRuntime_filter_mode();
    for (RuntimeFilter filter: runtimeFiltersByTid_.get(tid)) {
      if (filter.isFinalized()) continue;
      Expr targetExpr = computeTargetExpr(filter, tid, analyzer);
      if (targetExpr == null) continue;
      boolean isBoundByPartitionColumns = isBoundByPartitionColumns(analyzer, targetExpr,
          scanNode);
      if (disableRowRuntimeFiltering && !isBoundByPartitionColumns) continue;
      boolean isLocalTarget = isLocalTarget(filter, scanNode);
      if (runtimeFilterMode == TRuntimeFilterMode.LOCAL && !isLocalTarget) continue;

      // Check that the scan node supports applying filters of this type and targetExpr.
      if (scanNode instanceof HdfsScanNode
          && filter.getType() != TRuntimeFilterType.BLOOM) {
        continue;
      } else if (scanNode instanceof KuduScanNode) {
        if (filter.getType() != TRuntimeFilterType.MIN_MAX) continue;
        // TODO: IMPALA-6533: Support Kudu Decimal Min/Max Filters
        if (targetExpr.getType().isDecimal()) continue;
        SlotRef slotRef = targetExpr.unwrapSlotRef(true);
        // Kudu only supports targeting a single column, not general exprs, so the target
        // must be a SlotRef pointing to a column. We can allow implicit integer casts
        // by casting the min/max values before sending them to Kudu.
        // Kudu also cannot currently return nulls if a filter is applied, so it does not
        // work with "is not distinct".
        if (slotRef == null || slotRef.getDesc().getColumn() == null
            || (targetExpr instanceof CastExpr && !targetExpr.getType().isIntegerType())
            || filter.getExprCompOp() == Operator.NOT_DISTINCT) {
          continue;
        }
      }

      RuntimeFilter.RuntimeFilterTarget target = new RuntimeFilter.RuntimeFilterTarget(
          scanNode, targetExpr, isBoundByPartitionColumns, isLocalTarget);
      filter.addTarget(target);
    }
  }

  /**
   * Check if 'targetNode' is local to the source node of 'filter'.
   */
  static private boolean isLocalTarget(RuntimeFilter filter, ScanNode targetNode) {
    return targetNode.getFragment().getId().equals(filter.src_.getFragment().getId());
  }

  /**
   * Check if all the slots of 'targetExpr' are bound by partition columns.
   */
  static private boolean isBoundByPartitionColumns(Analyzer analyzer, Expr targetExpr,
      ScanNode targetNode) {
    Preconditions.checkState(targetExpr.isBoundByTupleIds(targetNode.getTupleIds()));
    TupleDescriptor baseTblDesc = targetNode.getTupleDesc();
    Table tbl = baseTblDesc.getTable();
    if (tbl.getNumClusteringCols() == 0) return false;
    List<SlotId> sids = Lists.newArrayList();
    targetExpr.getIds(null, sids);
    for (SlotId sid : sids) {
      SlotDescriptor slotDesc = analyzer.getSlotDesc(sid);
      if (slotDesc.getColumn() == null
          || slotDesc.getColumn().getPosition() >= tbl.getNumClusteringCols()) {
        return false;
      }
    }
    return true;
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
        targetExpr = targetExpr.substitute(smap, analyzer, false);
      } catch (Exception e) {
        return null;
      }
    }
    Type srcType = filter.getSrcExpr().getType();
    // Types of targetExpr and srcExpr must be exactly the same since runtime filters are
    // based on hashing.
    if (!targetExpr.getType().equals(srcType)) {
      try {
        targetExpr = targetExpr.castTo(srcType);
      } catch (Exception e) {
        return null;
      }
    }
    return targetExpr;
  }
}
