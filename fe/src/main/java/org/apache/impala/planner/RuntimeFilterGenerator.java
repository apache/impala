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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.analysis.CastExpr;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.IsNullPredicate;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.Predicate;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.analysis.TupleIsNullPredicate;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.IdGenerator;
import org.apache.impala.common.InternalException;
import org.apache.impala.planner.JoinNode.DistributionMode;
import org.apache.impala.planner.JoinNode.EqJoinConjunctScanSlots;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TRuntimeFilterDesc;
import org.apache.impala.thrift.TRuntimeFilterMode;
import org.apache.impala.thrift.TRuntimeFilterTargetDesc;
import org.apache.impala.thrift.TRuntimeFilterType;
import org.apache.impala.util.BitUtil;
import org.apache.impala.util.TColumnValueUtil;
import org.apache.impala.util.Visitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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
  // Should be in sync with the corresponding value in in-list-filter.h.
  private static final long IN_LIST_FILTER_STRING_SET_MAX_TOTAL_LENGTH = 4 * 1024 * 1024;

  private static final Set<PrimitiveType> IN_LIST_FILTER_SUPPORTED_TYPES =
      new HashSet<>(Arrays.asList(
          PrimitiveType.TINYINT, PrimitiveType.SMALLINT, PrimitiveType.INT,
          PrimitiveType.BIGINT, PrimitiveType.DATE, PrimitiveType.STRING,
          PrimitiveType.CHAR, PrimitiveType.VARCHAR));

  // Map of base table tuple ids to a list of runtime filters that
  // can be applied at the corresponding scan nodes.
  private final Map<TupleId, List<RuntimeFilter>> runtimeFiltersByTid_ =
      new HashMap<>();

  // Generator for filter ids
  private final IdGenerator<RuntimeFilterId> filterIdGenerator =
      RuntimeFilterId.createGenerator();

  // Map between a FragmentId to the height of plan subtree rooted at that FragmentId.
  // Can also be interpreted as the distance between FragmentId to its furthest leaf
  // fragment + 1. Leaf fragment has height 1.
  // Note that RuntimeFilterGenerator operate over a distributed plan tree.
  private final Map<PlanFragmentId, Integer> fragmentHeight_ = new HashMap<>();

  /**
   * Internal class that encapsulates the max, min and default sizes used for creating
   * bloom filter objects, and entry limit for in-list filters.
   */
  private class FilterSizeLimits {
    // Maximum bloom filter size, in bytes, rounded up to a power of two.
    public final long maxVal;

    // Minimum bloom filter size, in bytes, rounded up to a power of two.
    public final long minVal;

    // Pre-computed default bloom filter size, in bytes, rounded up to a power of two.
    public final long defaultVal;

    // Target false positive probability for bloom filters, between 0 and 1 exclusive.
    public final double targetFpp;

    // Maximum entry size for in-list filters.
    public final long inListFilterEntryLimit;

    public FilterSizeLimits(TQueryOptions tQueryOptions) {
      // Round up all limits to a power of two and make sure filter size is more
      // than the min buffer size that can be allocated by the buffer pool.
      long maxLimit = tQueryOptions.getRuntime_filter_max_size();
      long minBufferSize = BackendConfig.INSTANCE.getMinBufferSize();
      maxVal = BitUtil.roundUpToPowerOf2(Math.max(maxLimit, minBufferSize));
      Preconditions.checkState(maxVal <= MAX_BLOOM_FILTER_SIZE);

      long minLimit = tQueryOptions.getRuntime_filter_min_size();
      minLimit = Math.max(minLimit, minBufferSize);
      // Make sure minVal <= defaultVal <= maxVal
      minVal = BitUtil.roundUpToPowerOf2(Math.min(minLimit, maxVal));
      Preconditions.checkState(minVal >= MIN_BLOOM_FILTER_SIZE);

      long defaultValue = tQueryOptions.getRuntime_bloom_filter_size();
      defaultValue = Math.max(defaultValue, minVal);
      defaultVal = BitUtil.roundUpToPowerOf2(Math.min(defaultValue, maxVal));

      // Target FPP is determined by runtime_filter_error_rate query option, or if that
      // is not set, --max_filter_error_rate, which was the legacy option controlling
      // this that also had other effects.
      targetFpp = tQueryOptions.isSetRuntime_filter_error_rate() ?
          tQueryOptions.getRuntime_filter_error_rate() :
          BackendConfig.INSTANCE.getMaxFilterErrorRate();

      inListFilterEntryLimit = tQueryOptions.getRuntime_in_list_filter_entry_limit();
    }
  }

  // Contains size limits for runtime filters.
  final private FilterSizeLimits filterSizeLimits_;

  private RuntimeFilterGenerator(TQueryOptions tQueryOptions) {
    filterSizeLimits_ = new FilterSizeLimits(tQueryOptions);
  }

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
    private final List<RuntimeFilterTarget> targets_ = new ArrayList<>();
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
    // NDV of the build side key expression.
    private long buildKeyNdv_ = -1;
    // Size of the filter (in Bytes). Should be greater than zero for bloom filters.
    private long filterSizeBytes_ = 0;
    // Size of the filter (in Bytes) before applying min/max filter sizes.
    private long originalFilterSizeBytes_ = 0;
    // False positive probability of this filter. Only set for bloom filter.
    private double est_fpp_ = 0;
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
    // If set, indicates that the filter is targeted for Kudu scan node with source
    // timestamp truncation.
    private boolean isTimestampTruncation_ = false;
    // The level of this runtime filter.
    // Runtime filter level is defined as the height of build side subtree of the
    // join node that produce this filter.
    private int level_ = 1;

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
      // The low and high value of the column on which the filter is applied
      public final TColumnValue lowValue;
      public final TColumnValue highValue;
      // Indicates if the data is actually stored in the data files
      public final boolean isColumnInDataFile;

      public RuntimeFilterTarget(ScanNode targetNode, Expr targetExpr,
          boolean isBoundByPartitionColumns, boolean isColumnInDataFile,
          boolean isLocalTarget, TColumnValue lowValue, TColumnValue highValue) {
        Preconditions.checkState(targetExpr.isBoundByTupleIds(targetNode.getTupleIds()));
        node = targetNode;
        expr = targetExpr;
        this.isBoundByPartitionColumns = isBoundByPartitionColumns;
        this.isColumnInDataFile = isColumnInDataFile;
        this.isLocalTarget = isLocalTarget;
        this.lowValue = lowValue;
        this.highValue = highValue;
      }

      public TRuntimeFilterTargetDesc toThrift() {
        TRuntimeFilterTargetDesc tFilterTarget = new TRuntimeFilterTargetDesc();
        tFilterTarget.setNode_id(node.getId().asInt());
        tFilterTarget.setTarget_expr(expr.treeToThrift());
        List<SlotId> sids = new ArrayList<>();
        expr.getIds(null, sids);
        List<Integer> tSlotIds = Lists.newArrayListWithCapacity(sids.size());
        for (SlotId sid: sids) tSlotIds.add(sid.asInt());
        tFilterTarget.setTarget_expr_slotids(tSlotIds);
        tFilterTarget.setIs_bound_by_partition_columns(isBoundByPartitionColumns);
        tFilterTarget.setIs_local_target(isLocalTarget);
        if (node instanceof HdfsScanNode) {
          tFilterTarget.setIs_column_in_data_file(isColumnInDataFile);
        }
        if (node instanceof KuduScanNode) {
          // assignRuntimeFilters() only assigns KuduScanNode targets if the target expr
          // is a slot ref, possibly with an implicit cast, pointing to a column.
          SlotRef slotRef = expr.unwrapSlotRef(true);
          KuduColumn col = (KuduColumn) slotRef.getDesc().getColumn();
          tFilterTarget.setKudu_col_name(col.getKuduName());
          tFilterTarget.setKudu_col_type(col.getType().toThrift());
        }
        tFilterTarget.setLow_value(lowValue);
        tFilterTarget.setHigh_value(highValue);
        boolean minMaxValuePresent = false;
        if (node instanceof HdfsScanNode) {
          // Set the min/max value present flag to avoid repeated check of the presence
          // of the two values during execution.
          SlotRef slotRefInScan = expr.unwrapSlotRef(true);
          if (slotRefInScan != null) {
            Column col = slotRefInScan.getDesc().getColumn();
            if (col != null) {
              Type col_type = col.getType();
              minMaxValuePresent = TColumnValueUtil.isFieldSet(col_type, lowValue)
                  && TColumnValueUtil.isFieldSet(col_type, highValue);
            }
          }
        }
        tFilterTarget.setIs_min_max_value_present(minMaxValuePresent);
        return tFilterTarget;
      }

      @Override
      public String toString() {
        StringBuilder output = new StringBuilder();
        return output.append("Target Id: " + node.getId())
            .append(" Target expr: " + expr.debugString())
            .append(" Partition columns: " + isBoundByPartitionColumns)
            .append(" Is column stored in data files: " + isColumnInDataFile)
            .append(" Is local: " + isLocalTarget)
            .append(" lowValue: " + (lowValue != null ? lowValue.toString() : -1))
            .append(" highValue: " + (highValue != null ? highValue.toString() : -1))
            .toString();
      }
    }

    private RuntimeFilter(RuntimeFilterId filterId, JoinNode filterSrcNode, Expr srcExpr,
        Expr origTargetExpr, Operator exprCmpOp, Map<TupleId, List<SlotId>> targetSlots,
        TRuntimeFilterType type, FilterSizeLimits filterSizeLimits,
        boolean isTimestampTruncation, int level) {
      id_ = filterId;
      src_ = filterSrcNode;
      srcExpr_ = srcExpr;
      origTargetExpr_ = origTargetExpr;
      exprCmpOp_ = exprCmpOp;
      targetSlotsByTid_ = targetSlots;
      type_ = type;
      isTimestampTruncation_ = isTimestampTruncation;
      level_ = level;
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
      tFilter.setSrc_node_id(src_.getId().asInt());
      tFilter.setIs_broadcast_join(isBroadcastJoin_);
      tFilter.setNdv_estimate(ndvEstimate_);
      tFilter.setHas_local_targets(hasLocalTargets_);
      tFilter.setHas_remote_targets(hasRemoteTargets_);
      tFilter.setCompareOp(exprCmpOp_.getThriftOp());
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
        TRuntimeFilterType type, FilterSizeLimits filterSizeLimits,
        boolean isTimestampTruncation, int level) {
      Preconditions.checkNotNull(idGen);
      Preconditions.checkNotNull(joinPredicate);
      Preconditions.checkNotNull(filterSrcNode);
      // Only consider binary equality predicates under hash joins for runtime bloom
      // filters and in-list filters.
      if (type == TRuntimeFilterType.BLOOM || type == TRuntimeFilterType.IN_LIST) {
        if (!Predicate.isEquivalencePredicate(joinPredicate)
            || filterSrcNode instanceof NestedLoopJoinNode) {
          return null;
        }
      }
      if (type == TRuntimeFilterType.IN_LIST) {
        PrimitiveType lhsType = joinPredicate.getChild(0).getType().getPrimitiveType();
        PrimitiveType rhsType = joinPredicate.getChild(1).getType().getPrimitiveType();
        Preconditions.checkState(lhsType == rhsType, "Unanalyzed equivalence pred!");
        if (!IN_LIST_FILTER_SUPPORTED_TYPES.contains(lhsType)) {
          return null;
        }
      }

      BinaryPredicate normalizedJoinConjunct = null;
      if (type == TRuntimeFilterType.MIN_MAX) {
        if (filterSrcNode instanceof HashJoinNode) {
          if (!Predicate.isSqlEquivalencePredicate(joinPredicate)) {
            return null;
          } else {
            normalizedJoinConjunct = SingleNodePlanner.getNormalizedEqPred(joinPredicate,
                filterSrcNode.getChild(0).getTupleIds(),
                filterSrcNode.getChild(1).getTupleIds(), analyzer);
          }
        } else if (filterSrcNode instanceof NestedLoopJoinNode) {
          if (Predicate.isSingleRangePredicate(joinPredicate)) {
            PlanNode child1 = filterSrcNode.getChild(1);
            if (child1 instanceof ExchangeNode) {
              child1 = child1.getChild(0);
            }
            // When immediate or the indirect child below an Exchange is an
            // AggregationNode that implements a non-correlated scalar subquery
            // returning at most one value, a min/max filter can be generated.
            if (!(child1 instanceof AggregationNode)
                || !((AggregationNode) (child1)).isNonCorrelatedScalarSubquery()) {
              return null;
            }
          } else {
            return null;
          }
          normalizedJoinConjunct = SingleNodePlanner.getNormalizedSingleRangePred(
                joinPredicate, filterSrcNode.getChild(0).getTupleIds(),
                filterSrcNode.getChild(1).getTupleIds(), analyzer);
          if (normalizedJoinConjunct != null) {
            // MinMaxFilter can't handle range predicates with decimals stored
            // in __int128_t for the following reason:
            //  1) Both numeric_limits<__int128_t>::min() and
            //     numeric_limits<__int128_t>::max() return 0.
            if (normalizedJoinConjunct.getChild(0).getType().isDecimal()) {
              ScalarType decimalType =
                  (ScalarType) (normalizedJoinConjunct.getChild(0).getType());
              if (decimalType.storageBytesForDecimal() == 16) return null;
            }
          }
        }
      } else {
        normalizedJoinConjunct = SingleNodePlanner.getNormalizedEqPred(
          joinPredicate, filterSrcNode.getChild(0).getTupleIds(),
          filterSrcNode.getChild(1).getTupleIds(), analyzer);
      }

      if (normalizedJoinConjunct == null) return null;

      // Ensure that the target expr does not contain TupleIsNull predicates as these
      // can't be evaluated at a scan node.
      Expr targetExpr =
          TupleIsNullPredicate.unwrapExpr(normalizedJoinConjunct.getChild(0).clone());
      Expr srcExpr = normalizedJoinConjunct.getChild(1);

      if (isTimestampTruncation) {
        Preconditions.checkArgument(srcExpr.isAnalyzed());
        Preconditions.checkArgument(srcExpr.getType() == Type.TIMESTAMP);
        // The filter is targeted for Kudu scan node with source timestamp truncation.
        if (analyzer.getQueryOptions().isConvert_kudu_utc_timestamps()) {
          List<Expr> params = Lists.newArrayList(srcExpr,
              new StringLiteral(analyzer.getQueryCtx().getLocal_time_zone()));
          srcExpr = new FunctionCallExpr("to_utc_timestamp", params);
        }
        Expr toUnixTimeExpr =
            new FunctionCallExpr("utc_to_unix_micros", Lists.newArrayList(srcExpr));
        try {
          toUnixTimeExpr.analyze(analyzer);
        } catch (AnalysisException e) {
          // Expr analysis failed. Skip this runtime filter since we cannot serialize
          // it to thrift.
          LOG.warn("Skipping runtime filter because analysis failed: "
                  + toUnixTimeExpr.toSql(),
              e);
          return null;
        }
        srcExpr = toUnixTimeExpr;
      }

      Map<TupleId, List<SlotId>> targetSlots = getTargetSlots(analyzer, targetExpr);
      Preconditions.checkNotNull(targetSlots);
      if (targetSlots.isEmpty()) return null;

      if (filterSrcNode.getJoinOp().isLeftOuterJoin() ||
              filterSrcNode.getJoinOp().isFullOuterJoin()) {
        try {
          LiteralExpr nullSlotVal = analyzer.evalWithNullSlots(srcExpr);
          if (nullSlotVal == null || !(nullSlotVal instanceof NullLiteral)) {
            // IMPALA-10252: if the source expression returns a non-NULL value for a NULL
            // input then it is not safe to generate a runtime filter from a LEFT OUTER
            // JOIN or FULL OUTER JOIN because the generated filter would be missing a
            // possible non-NULL value of the expression.
            // E.g. If the source expression is zeroifnull(y), column y has values
            // [1, 2, 3], and y comes from the right input of a left outer join , then
            // the generated runtime filter would only contain the values [1, 2, 3] but
            // not the value zeroifnull(NULL) = 0 that would be present for an unmatched
            // row. For now we avoid the problem by skipping this filter. In future we
            // could generate valid runtime filters for these join types by adding
            // backend support to calculate and insert the missing value.
            // It's also possible that the expression did not evaluate to a constant after
            // null slot substitution. In this case, we also want to disable runtime
            // filtering.
            if (LOG.isTraceEnabled()) {
              LOG.trace("Skipping runtime filter because source expression returns " +
                      "non-null after null substitution: " + srcExpr.toSql());
            }
            return null;
          }
        } catch (AnalysisException e) {
          LOG.warn("Skipping runtime filter because analysis after null substitution " +
              "failed: " + srcExpr.toSql(), e);
          return null;
        }
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace("Generating runtime filter from predicate " + joinPredicate);
      }
      return new RuntimeFilter(idGen.getNextId(), filterSrcNode, srcExpr, targetExpr,
          normalizedJoinConjunct.getOp(), targetSlots, type, filterSizeLimits,
          isTimestampTruncation, level);
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
      List<TupleId> tids = new ArrayList<>();
      List<SlotId> sids = new ArrayList<>();
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

      Map<TupleId, List<SlotId>> slotsByTid = new HashMap<>();
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
      Map<TupleId, List<SlotId>> slotsByTid = new HashMap<>();
      for (SlotId targetSid: analyzer.getValueTransferTargets(srcSid)) {
        TupleDescriptor tupleDesc = analyzer.getSlotDesc(targetSid).getParent();
        if (tupleDesc.getTable() == null) continue;
        List<SlotId> sids = slotsByTid.get(tupleDesc.getId());
        if (sids == null) {
          sids = new ArrayList<>();
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

    public boolean isPartitionFilterAt(PlanNodeId targetPlanNodeId) {
      for (RuntimeFilterTarget target : targets_) {
        if (target.node.getId() == targetPlanNodeId) {
          return target.isBoundByPartitionColumns;
        }
      }
      return false;
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
    public boolean isTimestampTruncation() { return isTimestampTruncation_; }
    public boolean isBroadcast() { return isBroadcastJoin_; }
    public PlanNode getSrc() { return src_; }

    private long getBuildKeyNumRowStats() {
      long minNumRows = src_.getChild(1).getCardinality();
      SlotRef buildSlotRef = srcExpr_.unwrapSlotRef(true);
      if (buildSlotRef == null || !buildSlotRef.hasDesc()
          || buildSlotRef.getDesc().getParent() == null
          || buildSlotRef.getDesc().getParent().getTable() == null
          || buildSlotRef.getDesc().getParent().getTable().getNumRows() <= -1) {
        return minNumRows;
      }
      return buildSlotRef.getDesc().getParent().getTable().getNumRows();
    }

    /**
     * Return the estimated false-positive probability of this filter.
     * Bloom filter type will return value in [0.0 - 1.0] range. Otherwise, return 0.
     */
    public double getEstFpp() { return est_fpp_; }

    /**
     * Return TIMESTAMP if the isTimestampTruncation_ is set as true so that
     * the source expr type could be matching with target expr type when
     * assigning bloom filter to target scan node.
     */
    public Type getSrcExprType() {
      if (!isTimestampTruncation_) {
        return srcExpr_.getType();
      } else {
        return Type.TIMESTAMP;
      }
    }

    /**
     * Estimates the selectivity of a runtime filter as the cardinality of the
     * associated source join node over the cardinality of that join node's left
     * child.
     */
    public double getSelectivity() { return getJoinNodeSelectivity(src_); }

    public void addTarget(RuntimeFilterTarget target) { targets_.add(target); }

    public void setIsBroadcast(boolean isBroadcast) { isBroadcastJoin_ = isBroadcast; }

    public void computeNdvEstimate() {
      ndvEstimate_ = src_.getChild(1).getCardinality();
      buildKeyNdv_ = srcExpr_.getNumDistinctValues();

      // Build side selectiveness is calculated into the cardinality but not into
      // buildKeyNdv_. This can lead to overestimating NDV compared to how the Join node
      // estimates build side NDV (JoinNode.getGenericJoinCardinalityInternal()).
      if (buildKeyNdv_ >= 0) {
        ndvEstimate_ =
            ndvEstimate_ >= 0 ? Math.min(buildKeyNdv_, ndvEstimate_) : buildKeyNdv_;
      }
    }

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
     * Sets the filter size (in bytes) based on the filter type.
     * For bloom filters, the size should be able to achieve the configured maximum
     * false-positive rate based on the expected NDV. Also bounds the filter size between
     * the max and minimum filter sizes supplied to it by 'filterSizeLimits'.
     * For min-max filters, we ignore the size since each filter only keeps two values.
     * For in-list filters, the size is calculated based on the data types.
     */
    private void calculateFilterSize(FilterSizeLimits filterSizeLimits) {
      if (type_ == TRuntimeFilterType.MIN_MAX) return;
      if (type_ == TRuntimeFilterType.IN_LIST) {
        Type colType = srcExpr_.getType();
        if (colType.isStringType()) {
          filterSizeBytes_ = IN_LIST_FILTER_STRING_SET_MAX_TOTAL_LENGTH;
        } else {
          filterSizeBytes_ =
              filterSizeLimits.inListFilterEntryLimit * colType.getSlotSize();
        }
        return;
      }
      if (ndvEstimate_ == -1) {
        filterSizeBytes_ = filterSizeLimits.defaultVal;
        return;
      }
      double targetFpp = filterSizeLimits.targetFpp;
      int logFilterSize = FeSupport.GetMinLogSpaceForBloomFilter(ndvEstimate_, targetFpp);
      originalFilterSizeBytes_ = 1L << logFilterSize;
      filterSizeBytes_ = originalFilterSizeBytes_;
      filterSizeBytes_ = Math.max(filterSizeBytes_, filterSizeLimits.minVal);
      filterSizeBytes_ = Math.min(filterSizeBytes_, filterSizeLimits.maxVal);
      int actualLogFilterSize = (int) (Math.log(filterSizeBytes_) / Math.log(2));
      est_fpp_ =
          FeSupport.GetFalsePositiveProbForBloomFilter(ndvEstimate_, actualLogFilterSize);
    }

    /**
     * Assigns this runtime filter to the corresponding plan nodes.
     */
    public void assignToPlanNodes() {
      Preconditions.checkState(hasTargets());
      src_.addRuntimeFilter(this);
      for (RuntimeFilterTarget target: targets_) target.node.addRuntimeFilter(this);
    }

    /**
     * Return true if this filter is deemed highly selective, will arrive on-time, and
     * applied for all rows without ever being disabled by the scanner node.
     * Mainly used by {@link ScanNode#reduceCardinalityByRuntimeFilter(java.util.Stack,
     * double)} to lower the scan cardinality estimation. These properties are hard to
     * predict during planning, so this method is set very conservative to avoid severe
     * underestimation.
     */
    public boolean isHighlySelective() {
      return level_ <= 3 && (type_ != TRuntimeFilterType.BLOOM || est_fpp_ < 0.33);
    }

    /**
     * Return a reduced cardinality estimate for given scanNode.
     * @param scanNode the ScanNode to inspect.
     * @param scanCardinality the cardinality estimate before reducing through this
     *                        filter. This can be lower than scanNode.inputCardinality_.
     * @param partitionSelectivities a map of column name to selectivity on that column.
     *                               Will be populated if this filter target a partition
     *                               column.
     * @return a reduced cardinality estimate or -1 if unknown.
     */
    public long reducedCardinalityForScanNode(ScanNode scanNode, long scanCardinality,
        Map<String, Double> partitionSelectivities) {
      Preconditions.checkState(isHighlySelective());
      SlotRef buildSlot = srcExpr_.unwrapSlotRef(true);
      PlanNodeId scanNodeId = scanNode.getId();
      Expr targetExpr = getTargetExpr(scanNodeId);
      SlotRef targetSlot = targetExpr.unwrapSlotRef(true);
      long scanColumnNdv = targetExpr.getNumDistinctValues();
      if (scanColumnNdv < 0) {
        // For cardinality reduction, it is OK to skip this filter if 'scanColumnNdv'
        // is unknown rather than trying to estimate it.
        return -1;
      }

      long buildKeyNdv = ndvEstimate_;
      long buildKeyCard = src_.getChild(1).getCardinality();
      long buildKeyNumRows = getBuildKeyNumRowStats();
      // The raw input cardinality without applying scan conjuct and limits.
      long scanNumRows = scanNode.inputCardinality_;

      boolean isPartitionFilter = isPartitionFilterAt(scanNodeId);
      if (isPartitionFilter) {
        // Partition filter can apply at file-level filtering.
        // Keep the most selective partition filter per column to reduce
        // inputCardinality_. Ignore a column if its name is unknown.
        double thisPartSel = (double) buildKeyNdv / scanColumnNdv;
        if (targetSlot != null && targetSlot.hasDesc()
            && targetSlot.getDesc().getColumn() != null) {
          String colName = targetSlot.getDesc().getColumn().getName();
          double currentSel = partitionSelectivities.computeIfAbsent(colName, c -> 1.0);
          if (thisPartSel < currentSel) partitionSelectivities.put(colName, thisPartSel);
        }
      }

      List<EqJoinConjunctScanSlots> eligibleFkPkJoinConjuncts = new ArrayList<>();
      boolean isEvalAtStorageLayer =
          isPartitionFilter || (scanNode instanceof KuduScanNode);
      if (isEvalAtStorageLayer && src_.fkPkEqJoinConjuncts_ != null && targetSlot != null
          && targetSlot.hasDesc() && buildSlot != null && buildSlot.hasDesc()) {
        for (EqJoinConjunctScanSlots fkPk : src_.fkPkEqJoinConjuncts_) {
          if (targetSlot.getSlotId().equals(fkPk.lhs_.getId())
              && buildSlot.getSlotId().equals(fkPk.rhs_.getId())) {
            eligibleFkPkJoinConjuncts.add(fkPk);
          }
        }
      }

      if (eligibleFkPkJoinConjuncts.isEmpty()) {
        return JoinNode.computeGenericJoinCardinality(scanColumnNdv, buildKeyNdv,
            scanNumRows, buildKeyNumRows, scanCardinality, buildKeyCard);
      } else {
        // JoinNode.getFkPkJoinCardinality() can return lower number than
        // JoinNode.computeGenericJoinCardinality(). But be conservative to only use it
        // for filters that evaluate in storage layer (such as partition filter and
        // pushed-down Kudu filter) to avoid severe underestimation.
        return JoinNode.getFkPkJoinCardinality(
            eligibleFkPkJoinConjuncts, scanCardinality, buildKeyCard);
      }
    }

    public String debugString() {
      StringBuilder output = new StringBuilder();
      output.append("FilterID: " + id_)
          .append(" Type: " + type_)
          .append(" Source: " + src_.getId())
          .append(" SrcExpr: " + getSrcExpr().debugString())
          .append(" Target(s): " + Joiner.on(", ").join(targets_))
          .append(" Selectivity: " + getSelectivity())
          .append(" Build key NDV: " + buildKeyNdv_)
          .append(" NDV estimate " + ndvEstimate_)
          .append(" Filter size (bytes): " + filterSizeBytes_)
          .append(" Original filter size (bytes): " + originalFilterSizeBytes_)
          .append(" Level: " + level_);
      if (type_ == TRuntimeFilterType.BLOOM) {
        output.append(" Est fpp: " + est_fpp_);
      }
      return output.toString();
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
          @Override
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
        if (numBloomFilters >= maxNumBloomFilters) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Skip runtime filter (already reached max runtime filter count): "
                + filter.debugString());
          }
          continue;
        }
        ++numBloomFilters;
      }
      DistributionMode distMode = filter.src_.getDistributionMode();
      filter.setIsBroadcast(distMode == DistributionMode.BROADCAST);
      if (filter.getType() == TRuntimeFilterType.IN_LIST
          && distMode == DistributionMode.PARTITIONED) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Skip IN-list filter on partitioned join: {}", filter.debugString());
        }
        continue;
      }
      filter.computeHasLocalTargets();
      if (LOG.isTraceEnabled()) LOG.trace("Runtime filter: " + filter.debugString());
      filter.assignToPlanNodes();
    }

    arrangeRuntimefiltersForParquet(plan);
  }

  public static void arrangeRuntimefiltersForParquet(
      PlanNode root) {
    if (root instanceof HdfsScanNode) {
      ((HdfsScanNode)root).arrangeRuntimefiltersForParquet();
    } else {
      for (PlanNode childNode: root.getChildren()) {
        arrangeRuntimefiltersForParquet(childNode);
      }
    }
  }

  /**
   * Visitor class over PlanNode tree to check whether the build hand side of join is
   * eligible for runtime filter pruning based on full scan assumption.
   */
  private class BuildVisitor implements Visitor<PlanNode> {
    private boolean hasIncomingFilter_ = false;

    @Override
    public void visit(PlanNode a) {
      if (!isEligibleForPrunning() || !(a instanceof ScanNode)) return;

      ScanNode scan = (ScanNode) a;
      TupleId sourceTid = scan.getTupleIds().get(0);
      List<RuntimeFilter> incomingFilters = runtimeFiltersByTid_.get(sourceTid);
      hasIncomingFilter_ |= (incomingFilters != null && !incomingFilters.isEmpty());
    }

    public boolean isEligibleForPrunning() { return !hasIncomingFilter_; }

    public void reset() { hasIncomingFilter_ = false; }
  }

  /**
   * Remove filters that are unlikely to be useful out of allFilters list.
   */
  private void reduceFilters(Collection<RuntimeFilter> allFilters) {
    if (BackendConfig.INSTANCE.getMaxFilterErrorRateFromFullScan() < 0) return;

    // Filter out bloom filter with fpp > max_filter_error_rate_from_full_scan that
    // comes from full table scan without filter predicate or incoming runtime filter.
    // Only do this optimization for filter level 1 for simplicity.
    boolean hasEliminatedFilter = true;
    final double maxAllowedFpp =
        Math.max(BackendConfig.INSTANCE.getMaxFilterErrorRateFromFullScan(),
            filterSizeLimits_.targetFpp);
    List<RuntimeFilter> highFppFilters =
        allFilters.stream()
            .filter(f
                -> f.getType() == TRuntimeFilterType.BLOOM
                    && f.level_ <= 1
                    // Build hand node has hard cardinality estimates
                    // (no reduction before runtime filter addition).
                    && f.src_.getChild(1).hasHardEstimates_
                    // At least assumed to have fk/pk relationship.
                    && f.src_.fkPkEqJoinConjuncts_ != null
                    // TODO: FK/PK alone may not be enough to justify eliminating the
                    // filter because it could end up eliminating a filter that is
                    // effective because it filters a lot of NULLs. Therefore, fpp
                    // threshold check is added here to only consider the high fpp one.
                    // This last check can be removed once IMPALA-2743 is implemented
                    // to cover the NULL filtering.
                    && f.est_fpp_ > maxAllowedFpp)
            .collect(Collectors.toList());

    // As we eliminate a runtime filter in one iteration, there might be another runtime
    // filter that becomes eligible for removal. Loop this routine until it does eliminate
    // any other runtime filter.
    while (!highFppFilters.isEmpty() && hasEliminatedFilter) {
      hasEliminatedFilter = false;
      List<RuntimeFilter> skippedFilters = new ArrayList<>();
      BuildVisitor buildVisitor = new BuildVisitor();
      for (RuntimeFilter filter : highFppFilters) {
        // Check if the build side of filter.src_ has a filtering scan or not.
        buildVisitor.reset();
        filter.src_.getChild(1).accept(buildVisitor);
        if (buildVisitor.isEligibleForPrunning()) {
          // Build side of filter.src_ is all full scan.
          // Skip this filter and remove it from all of its target TupleIds in
          // runtimeFiltersByTid_ map.
          hasEliminatedFilter = true;
          skippedFilters.add(filter);
          for (RuntimeFilter.RuntimeFilterTarget target : filter.getTargets()) {
            TupleId targetTid = target.node.getTupleIds().get(0);
            runtimeFiltersByTid_.get(targetTid).remove(filter);
          }
        }
      }

      // Remove all skippedFilters from highFppFilters and allFilters for next iteration.
      for (RuntimeFilter filter : skippedFilters) {
        highFppFilters.remove(filter);
        allFilters.remove(filter);
      }
    }
  }

  /**
   * Returns a list of all the registered runtime filters, ordered by filter ID.
   */
  public List<RuntimeFilter> getRuntimeFilters() {
    Set<RuntimeFilter> resultSet = new HashSet<>();
    for (List<RuntimeFilter> filters: runtimeFiltersByTid_.values()) {
      resultSet.addAll(filters);
    }
    List<RuntimeFilter> resultList = Lists.newArrayList(resultSet);
    Collections.sort(resultList, new Comparator<RuntimeFilter>() {
        @Override
        public int compare(RuntimeFilter a, RuntimeFilter b) {
          return a.getFilterId().compareTo(b.getFilterId());
        }
      }
    );
    reduceFilters(resultList);
    return resultList;
  }

  private void generateFilters(PlannerContext ctx, PlanNode root) {
    root.getFragment().postAccept(f -> {
      int height = 0;
      for (PlanFragment child : f.getChildren()) {
        height = Math.max(height, fragmentHeight_.get(child.getId()));
      }
      fragmentHeight_.put(((PlanFragment) f).getId(), height + 1);
    });
    generateFiltersRecursive(ctx, root);
  }

  /**
   * Generates the runtime filters for a query by recursively traversing the distributed
   * plan tree rooted at 'root'. In the top-down traversal of the plan tree, candidate
   * runtime filters are generated from equi-join predicates assigned to hash-join nodes.
   * In the bottom-up traversal of the plan tree, the filters are assigned to destination
   * (scan) nodes. Filters that cannot be assigned to a scan node are discarded.
   */
  private void generateFiltersRecursive(PlannerContext ctx, PlanNode root) {
    if (root instanceof HashJoinNode || root instanceof NestedLoopJoinNode) {
      JoinNode joinNode = (JoinNode) root;

      // Determine level of filter produced by root.
      PlanNode buildHandNode = root.getChild(1);
      while (buildHandNode.getFragment() == root.getFragment()
          && buildHandNode.hasChild(0)) {
        buildHandNode = buildHandNode.getChild(0);
      }
      int level = fragmentHeight_.get(buildHandNode.getFragment().getId());

      List<Expr> joinConjuncts = new ArrayList<>();
      if (!joinNode.getJoinOp().isLeftOuterJoin()
          && !joinNode.getJoinOp().isFullOuterJoin()
          && !joinNode.getJoinOp().isAntiJoin()) {
        // It's not correct to push runtime filters to the left side of a left outer,
        // full outer or anti join if the filter corresponds to an equi-join predicate
        // from the ON clause.
        joinConjuncts.addAll(joinNode.getEqJoinConjuncts());
      }
      joinConjuncts.addAll(joinNode.getConjuncts());

      List<RuntimeFilter> filters = new ArrayList<>();
      Set<TRuntimeFilterType> enabledRuntimeFilterTypes =
          ctx.getQueryOptions().getEnabled_runtime_filter_types();
      for (TRuntimeFilterType filterType : TRuntimeFilterType.values()) {
        if (!enabledRuntimeFilterTypes.contains(filterType)) continue;
        for (Expr conjunct : joinConjuncts) {
          RuntimeFilter filter = RuntimeFilter.create(filterIdGenerator,
              ctx.getRootAnalyzer(), conjunct, joinNode, filterType, filterSizeLimits_,
              /* isTimestampTruncation */ false, level);
          if (filter != null) {
            registerRuntimeFilter(ctx, filter);
            filters.add(filter);
          }
          // For timestamp bloom filters, we also generate a RuntimeFilter with the
          // src timestamp truncated for Kudu scan node targets.
          if (filterType == TRuntimeFilterType.BLOOM
              && Predicate.isEquivalencePredicate(conjunct)
              && conjunct.getChild(0).getType().isTimestamp()
              && conjunct.getChild(1).getType().isTimestamp()) {
            RuntimeFilter filter2 = RuntimeFilter.create(filterIdGenerator,
                ctx.getRootAnalyzer(), conjunct, joinNode, filterType, filterSizeLimits_,
                /* isTimestampTruncation */ true, level);
            if (filter2 == null) continue;
            registerRuntimeFilter(ctx, filter2);
            filters.add(filter2);
          }
        }
      }
      generateFiltersRecursive(ctx, root.getChild(0));
      // Finalize every runtime filter of that join. This is to ensure that we don't
      // assign a filter to a scan node from the right subtree of joinNode or ancestor
      // join nodes in case we don't find a destination node in the left subtree.
      for (RuntimeFilter runtimeFilter: filters) finalizeRuntimeFilter(runtimeFilter);
      generateFiltersRecursive(ctx, root.getChild(1));
    } else if (root instanceof ScanNode) {
      assignRuntimeFilters(ctx, (ScanNode) root);
    } else {
      for (PlanNode childNode: root.getChildren()) {
        generateFiltersRecursive(ctx, childNode);
      }
    }
  }

  /**
   * Registers a runtime filter with the tuple id of every scan node that is a candidate
   * destination node for that filter.
   */
  private void registerRuntimeFilter(PlannerContext ctx, RuntimeFilter filter) {
    Map<TupleId, List<SlotId>> targetSlotsByTid = filter.getTargetSlots();
    Preconditions.checkState(targetSlotsByTid != null && !targetSlotsByTid.isEmpty());
    for (TupleId tupleId: targetSlotsByTid.keySet()) {
      registerRuntimeFilter(ctx, filter, tupleId);
    }
  }

  /**
   * Registers a runtime filter with a specific target tuple id.
   */
  private void registerRuntimeFilter(
      PlannerContext ctx, RuntimeFilter filter, TupleId targetTid) {
    Preconditions.checkArgument(filter.getTargetSlots().containsKey(targetTid),
        "filter does not contain target slot!");
    Preconditions.checkArgument(!filter.isFinalized(), "filter must not finalized yet!");
    if (ctx.getQueryOptions().isSetRuntime_filter_ids_to_skip()
        && ctx.getQueryOptions().getRuntime_filter_ids_to_skip().contains(
            filter.getFilterId().asInt())) {
      // Skip this filter because it is explicitly excluded via query option.
      return;
    }
    runtimeFiltersByTid_.computeIfAbsent(targetTid, t -> new ArrayList<>()).add(filter);
  }

  /**
   * Finalizes a runtime filter by disassociating it from all the candidate target scan
   * nodes that haven't been used as destinations for that filter. Also sets the
   * finalized_ flag of that filter so that it can't be assigned to any other scan nodes.
   */
  private void finalizeRuntimeFilter(RuntimeFilter runtimeFilter) {
    Set<TupleId> targetTupleIds = new HashSet<>();
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

  public static boolean enableOverlapFilter(TQueryOptions queryOptions) {
    return queryOptions.parquet_read_statistics
        && ((queryOptions.isMinmax_filter_sorted_columns()
                || queryOptions.getMinmax_filter_threshold() > 0.0)
            || queryOptions.isMinmax_filter_partition_columns());
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
   *     a. If the target is an HdfsScanNode, the filter must be type BLOOM/IN_LIST for
   *        ORC tables, or type BLOOM/MIN_MAX for Parquet tables, or type BLOOM for other
   *        kind of tables.
   *     b. If the target is a KuduScanNode, the filter could be type MIN_MAX, and/or
   *        BLOOM, the target must be a slot ref on a column, and the comp op cannot
   *        be 'not distinct'.
   * A scan node may be used as a destination node for multiple runtime filters. This
   * method is called once per scan node to process all filters accumulated for it
   * for the entire query, per top-down traversal nature of the calling method
   * generateFilters().
   */
  private void assignRuntimeFilters(PlannerContext ctx, ScanNode scanNode) {
    if (!(scanNode instanceof HdfsScanNode || scanNode instanceof KuduScanNode)) return;
    TupleId tid = scanNode.getTupleIds().get(0);
    if (!runtimeFiltersByTid_.containsKey(tid)) return;
    Analyzer analyzer = ctx.getRootAnalyzer();
    boolean disableRowRuntimeFiltering =
        ctx.getQueryOptions().isDisable_row_runtime_filtering();
    boolean enable_overlap_filter = enableOverlapFilter(ctx.getQueryOptions());
    TRuntimeFilterMode runtimeFilterMode = ctx.getQueryOptions().getRuntime_filter_mode();
    Set<TRuntimeFilterType> enabledRuntimeFilterTypes =
        ctx.getQueryOptions().getEnabled_runtime_filter_types();

    // Init the overlap predicate for the hdfs scan node.
    if (scanNode instanceof HdfsScanNode && enable_overlap_filter) {
      ((HdfsScanNode) scanNode).initOverlapPredicate(analyzer);
    }

    for (RuntimeFilter filter: runtimeFiltersByTid_.get(tid)) {
      if (filter.isFinalized()) continue;
      Expr targetExpr = computeTargetExpr(filter, tid, analyzer);
      if (targetExpr == null) continue;
      boolean isBoundByPartitionColumns = isBoundByPartitionColumns(analyzer, targetExpr,
          scanNode);
      if (disableRowRuntimeFiltering && !isBoundByPartitionColumns) continue;
      boolean isColumnInDataFile = isColumnInDataFile(scanNode.getTupleDesc().getTable(),
          isBoundByPartitionColumns);
      boolean isLocalTarget = isLocalTarget(filter, scanNode);
      if (runtimeFilterMode == TRuntimeFilterMode.LOCAL && !isLocalTarget) continue;

      // Check that the scan node supports applying filters of this type and targetExpr.
      if (scanNode instanceof HdfsScanNode) {
        if (filter.isTimestampTruncation()) {
          continue;
        }
        if (filter.getType() == TRuntimeFilterType.MIN_MAX) {
          Preconditions.checkState(
              enabledRuntimeFilterTypes.contains(TRuntimeFilterType.MIN_MAX),
              "MIN_MAX filters should not be generated");
          if (!enable_overlap_filter) continue;
          // Try to compute an overlap predicate for the filter. This predicate will be
          // used to filter out partitions, or row groups, pages or rows in Parquet data
          // files.
          if (!((HdfsScanNode) scanNode)
                   .tryToComputeOverlapPredicate(
                       analyzer, filter, targetExpr, isBoundByPartitionColumns)) {
            continue;
          }
        } else if (filter.getType() == TRuntimeFilterType.IN_LIST) {
          // Only assign IN-list filters on ORC tables.
          if (!((HdfsScanNode) scanNode).getFileFormats().contains(HdfsFileFormat.ORC)) {
            continue;
          }
        }
      } else {
        // assign filters to KuduScanNode
        if (filter.getType() == TRuntimeFilterType.BLOOM) {
          Preconditions.checkState(
              enabledRuntimeFilterTypes.contains(TRuntimeFilterType.BLOOM),
              "BLOOM filters should not be generated!");
          // TODO: Support Kudu VARCHAR Bloom Filter
          if (targetExpr.getType().isVarchar()) continue;
          // Kudu only supports targeting a single column, not general exprs, so the
          // target must be a SlotRef pointing to a column without casting.
          // For timestamp bloom filter, assign it to Kudu if it has src timestamp
          // truncation.
          if (!(targetExpr instanceof SlotRef)
              || filter.getExprCompOp() == Operator.NOT_DISTINCT
              || (targetExpr.getType().isTimestamp()
                     && !filter.isTimestampTruncation())) {
            continue;
          }
          SlotRef slotRef = (SlotRef) targetExpr;
          if (slotRef.getDesc().getColumn() == null) continue;
          if (filter.isTimestampTruncation() &&
              analyzer.getQueryOptions().isConvert_kudu_utc_timestamps() &&
              analyzer.getQueryOptions().isDisable_kudu_local_timestamp_bloom_filter()) {
            // Local timestamp convert to UTC could be ambiguous in the case of DST
            // change. We can only put one of the two possible UTC timestamps in the bloom
            // filter for now, which may cause missing rows that have the other UTC
            // timestamp.
            // For those regions that do not observe DST, could set this flag to false
            // to re-enable kudu local timestamp bloom filter.
            LOG.info("Skipping runtime filter because kudu local timestamp bloom filter "
                + "is disabled: " + filter.getSrcExpr().toSql());
            continue;
          }
        } else if (filter.getType() == TRuntimeFilterType.MIN_MAX) {
          Preconditions.checkState(
              enabledRuntimeFilterTypes.contains(TRuntimeFilterType.MIN_MAX),
              "MIN_MAX filters should not be generated!");
          // TODO: IMPALA-9580: Support Kudu VARCHAR Min/Max Filters
          if (targetExpr.getType().isVarchar()) continue;
          SlotRef slotRef = targetExpr.unwrapSlotRef(true);
          // Kudu only supports targeting a single column, not general exprs, so the
          // target must be a SlotRef pointing to a column. We can allow implicit
          // integer casts by casting the min/max values before sending them to Kudu.
          // Kudu also cannot currently return nulls if a filter is applied, so it
          // does not work with "is not distinct".
          if (slotRef == null || slotRef.getDesc().getColumn() == null
              || (targetExpr instanceof CastExpr && !targetExpr.getType().isIntegerType())
              || filter.getExprCompOp() == Operator.NOT_DISTINCT) {
            continue;
          }
        } else {
          Preconditions.checkState(filter.getType() == TRuntimeFilterType.IN_LIST);
          Preconditions.checkState(
              enabledRuntimeFilterTypes.contains(TRuntimeFilterType.IN_LIST),
              "IN_LIST filters should not be generated!");
          // TODO(IMPALA-11066): Runtime IN-list filters for Kudu tables
          continue;
        }
      }
      TColumnValue lowValue = null;
      TColumnValue highValue = null;
      if (scanNode instanceof HdfsScanNode) {
        SlotRef slotRefInScan = targetExpr.unwrapSlotRef(true);
        if (slotRefInScan != null) {
          Column col = slotRefInScan.getDesc().getColumn();
          if (col != null) {
            lowValue = col.getStats().getLowValue();
            highValue = col.getStats().getHighValue();
          }
        }
      }
      RuntimeFilter.RuntimeFilterTarget target =
          new RuntimeFilter.RuntimeFilterTarget(scanNode, targetExpr,
              isBoundByPartitionColumns, isColumnInDataFile, isLocalTarget,
              lowValue, highValue);
      filter.addTarget(target);
    }

    // finalize the overlap predicate for the hdfs scan node.
    if (scanNode instanceof HdfsScanNode && enable_overlap_filter) {
      ((HdfsScanNode) scanNode).finalizeOverlapPredicate();
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
    FeTable tbl = baseTblDesc.getTable();
    if (tbl.getNumClusteringCols() == 0 && !(tbl instanceof FeIcebergTable)) return false;
    List<SlotId> sids = new ArrayList<>();
    targetExpr.getIds(null, sids);
    for (SlotId sid : sids) {
      Column col = analyzer.getSlotDesc(sid).getColumn();
      if (col == null) return false;
      if (!tbl.isClusteringColumn(col) && !tbl.isComputedPartitionColumn(col)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Return true if the column is actually stored in the data file. E.g. partition
   * columns are not stored in most cases.
   */
  private static boolean isColumnInDataFile(FeTable tbl,
      boolean isBoundByPartitionColumns) {
    // Non-partition values are always sotred in the data files.
    if (!isBoundByPartitionColumns) return true;
    // Iceberg uses hidden partitioning which means all columns are stored in the data
    // files.
    if (tbl instanceof FeIcebergTable) return true;
    return false;
  }

  /**
   * Get selectivity of joinNode.
   */
  public static double getJoinNodeSelectivity(JoinNode joinNode) {
    if (joinNode.getCardinality() == -1 || joinNode.getChild(0).getCardinality() == -1
        || joinNode.getChild(0).getCardinality() == 0) {
      return -1;
    }
    return joinNode.getCardinality() / (double) joinNode.getChild(0).getCardinality();
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
      List<SlotRef> exprSlots = new ArrayList<>();
      targetExpr.collect(SlotRef.class, exprSlots);
      List<SlotId> sids = filter.getTargetSlots().get(targetTid);
      for (SlotRef slotRef: exprSlots) {
        boolean matched = false;
        for (SlotId sid: sids) {
          if (analyzer.hasValueTransfer(slotRef.getSlotId(), sid)) {
            SlotRef newSlotRef = new SlotRef(analyzer.getSlotDesc(sid));
            newSlotRef.analyzeNoThrow(analyzer);
            smap.put(slotRef, newSlotRef);
            matched = true;
            break;
          }
        }
        Preconditions.checkState(matched,
            "No SlotId found for RuntimeFilter %s SlotRef %s", filter, slotRef);
      }
      try {
        targetExpr = targetExpr.substitute(smap, analyzer, false);
      } catch (Exception e) {
        return null;
      }
    }
    Type srcType = filter.getSrcExprType();
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
