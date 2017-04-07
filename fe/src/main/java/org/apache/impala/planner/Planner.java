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
import java.util.Collections;
import java.util.List;

import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.ColumnLineageGraph;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.InsertStmt;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.analysis.QueryStmt;
import org.apache.impala.analysis.SortInfo;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.catalog.HBaseTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryExecRequest;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TRuntimeFilterMode;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.KuduUtil;
import org.apache.impala.util.MaxRowsProcessedVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Creates an executable plan from an analyzed parse tree and query options.
 */
public class Planner {
  private final static Logger LOG = LoggerFactory.getLogger(Planner.class);

  // Minimum per-host resource requirements to ensure that no plan node set can have
  // estimates of zero, even if the contained PlanNodes have estimates of zero.
  public static final long MIN_PER_HOST_MEM_ESTIMATE_BYTES = 10 * 1024 * 1024;

  public static final ResourceProfile MIN_PER_HOST_RESOURCES =
      new ResourceProfileBuilder().setMemEstimateBytes(MIN_PER_HOST_MEM_ESTIMATE_BYTES)
      .setMinReservationBytes(0).build();

  private final PlannerContext ctx_;

  public Planner(AnalysisResult analysisResult, TQueryCtx queryCtx,
      EventSequence timeline) {
    ctx_ = new PlannerContext(analysisResult, queryCtx, timeline);
  }

  public TQueryCtx getQueryCtx() { return ctx_.getQueryCtx(); }
  public AnalysisContext.AnalysisResult getAnalysisResult() {
    return ctx_.getAnalysisResult();
  }

  /**
   * Returns a list of plan fragments for executing an analyzed parse tree.
   * May return a single-node or distributed executable plan. If enabled (through a
   * query option), computes runtime filters for dynamic partition pruning.
   *
   * Plan generation may fail and throw for the following reasons:
   * 1. Expr evaluation failed, e.g., during partition pruning.
   * 2. A certain feature is not yet implemented, e.g., physical join implementation for
   *    outer/semi joins without equi conjuncts.
   * 3. Expr substitution failed, e.g., because an expr was substituted with a type that
   *    render the containing expr semantically invalid. Analysis should have ensured
   *    that such an expr substitution during plan generation never fails. If it does,
   *    that typically means there is a bug in analysis, or a broken/missing smap.
   */
  public ArrayList<PlanFragment> createPlan() throws ImpalaException {
    SingleNodePlanner singleNodePlanner = new SingleNodePlanner(ctx_);
    DistributedPlanner distributedPlanner = new DistributedPlanner(ctx_);
    PlanNode singleNodePlan = singleNodePlanner.createSingleNodePlan();
    ctx_.getTimeline().markEvent("Single node plan created");
    ArrayList<PlanFragment> fragments = null;

    checkForSmallQueryOptimization(singleNodePlan);

    // Join rewrites.
    invertJoins(singleNodePlan, ctx_.isSingleNodeExec());
    singleNodePlan = useNljForSingularRowBuilds(singleNodePlan, ctx_.getRootAnalyzer());

    singleNodePlanner.validatePlan(singleNodePlan);

    if (ctx_.isSingleNodeExec()) {
      // create one fragment containing the entire single-node plan tree
      fragments = Lists.newArrayList(new PlanFragment(
          ctx_.getNextFragmentId(), singleNodePlan, DataPartition.UNPARTITIONED));
    } else {
      // create distributed plan
      fragments = distributedPlanner.createPlanFragments(singleNodePlan);
    }

    // Create runtime filters.
    PlanFragment rootFragment = fragments.get(fragments.size() - 1);
    if (ctx_.getQueryOptions().getRuntime_filter_mode() != TRuntimeFilterMode.OFF) {
      RuntimeFilterGenerator.generateRuntimeFilters(ctx_, rootFragment.getPlanRoot());
      ctx_.getTimeline().markEvent("Runtime filters computed");
    }

    rootFragment.verifyTree();
    ExprSubstitutionMap rootNodeSmap = rootFragment.getPlanRoot().getOutputSmap();
    List<Expr> resultExprs = null;
    if (ctx_.isInsertOrCtas()) {
      InsertStmt insertStmt = ctx_.getAnalysisResult().getInsertStmt();
      insertStmt.substituteResultExprs(rootNodeSmap, ctx_.getRootAnalyzer());
      if (!ctx_.isSingleNodeExec()) {
        // repartition on partition keys
        rootFragment = distributedPlanner.createInsertFragment(
            rootFragment, insertStmt, ctx_.getRootAnalyzer(), fragments);
      }
      // Add optional sort node to the plan, based on clustered/noclustered plan hint.
      createPreInsertSort(insertStmt, rootFragment, ctx_.getRootAnalyzer());
      // set up table sink for root fragment
      rootFragment.setSink(insertStmt.createDataSink());
      resultExprs = insertStmt.getResultExprs();
    } else {
      if (ctx_.isUpdate()) {
        // Set up update sink for root fragment
        rootFragment.setSink(ctx_.getAnalysisResult().getUpdateStmt().createDataSink());
      } else if (ctx_.isDelete()) {
        // Set up delete sink for root fragment
        rootFragment.setSink(ctx_.getAnalysisResult().getDeleteStmt().createDataSink());
      } else if (ctx_.isQuery()) {
        rootFragment.setSink(ctx_.getAnalysisResult().getQueryStmt().createDataSink());
      }
      QueryStmt queryStmt = ctx_.getQueryStmt();
      queryStmt.substituteResultExprs(rootNodeSmap, ctx_.getRootAnalyzer());
      resultExprs = queryStmt.getResultExprs();
    }
    rootFragment.setOutputExprs(resultExprs);

    // The check for disabling codegen uses estimates of rows per node so must be done
    // on the distributed plan.
    checkForDisableCodegen(rootFragment.getPlanRoot());

    if (LOG.isTraceEnabled()) {
      LOG.trace("desctbl: " + ctx_.getRootAnalyzer().getDescTbl().debugString());
      LOG.trace("resultexprs: " + Expr.debugString(rootFragment.getOutputExprs()));
      LOG.trace("finalize plan fragments");
    }
    for (PlanFragment fragment: fragments) {
      fragment.finalizeExchanges(ctx_.getRootAnalyzer());
    }

    Collections.reverse(fragments);
    ctx_.getTimeline().markEvent("Distributed plan created");

    ColumnLineageGraph graph = ctx_.getRootAnalyzer().getColumnLineageGraph();
    if (BackendConfig.INSTANCE.getComputeLineage() || RuntimeEnv.INSTANCE.isTestEnv()) {
      // Lineage is disabled for UPDATE AND DELETE statements
      if (ctx_.isUpdateOrDelete()) return fragments;
      // Compute the column lineage graph
      if (ctx_.isInsertOrCtas()) {
        InsertStmt insertStmt = ctx_.getAnalysisResult().getInsertStmt();
        List<Expr> exprs = Lists.newArrayList();
        Table targetTable = insertStmt.getTargetTable();
        Preconditions.checkNotNull(targetTable);
        if (targetTable instanceof KuduTable) {
          if (ctx_.isInsert()) {
            // For insert statements on Kudu tables, we only need to consider
            // the labels of columns mentioned in the column list.
            List<String> mentionedColumns = insertStmt.getMentionedColumns();
            Preconditions.checkState(!mentionedColumns.isEmpty());
            List<String> targetColLabels = Lists.newArrayList();
            String tblFullName = targetTable.getFullName();
            for (String column: mentionedColumns) {
              targetColLabels.add(tblFullName + "." + column);
            }
            graph.addTargetColumnLabels(targetColLabels);
          } else {
            graph.addTargetColumnLabels(targetTable);
          }
          exprs.addAll(resultExprs);
        } else if (targetTable instanceof HBaseTable) {
          graph.addTargetColumnLabels(targetTable);
          exprs.addAll(resultExprs);
        } else {
          graph.addTargetColumnLabels(targetTable);
          exprs.addAll(ctx_.getAnalysisResult().getInsertStmt().getPartitionKeyExprs());
          exprs.addAll(resultExprs.subList(0,
              targetTable.getNonClusteringColumns().size()));
        }
        graph.computeLineageGraph(exprs, ctx_.getRootAnalyzer());
      } else {
        graph.addTargetColumnLabels(ctx_.getQueryStmt().getColLabels());
        graph.computeLineageGraph(resultExprs, ctx_.getRootAnalyzer());
      }
      if (LOG.isTraceEnabled()) LOG.trace("lineage: " + graph.debugString());
      ctx_.getTimeline().markEvent("Lineage info computed");
    }

    return fragments;
  }

  /**
   * Return a list of plans, each represented by the root of their fragment trees.
   * TODO: roll into createPlan()
   */
  public List<PlanFragment> createParallelPlans() throws ImpalaException {
    Preconditions.checkState(ctx_.getQueryOptions().mt_dop > 0);
    ArrayList<PlanFragment> distrPlan = createPlan();
    Preconditions.checkNotNull(distrPlan);
    ParallelPlanner planner = new ParallelPlanner(ctx_);
    List<PlanFragment> parallelPlans = planner.createPlans(distrPlan.get(0));
    // Only use one scanner thread per scan-node instance since intra-node
    // parallelism is achieved via multiple fragment instances.
    ctx_.getQueryOptions().setNum_scanner_threads(1);
    ctx_.getTimeline().markEvent("Parallel plans created");
    return parallelPlans;
  }

  /**
   * Return combined explain string for all plan fragments.
   * Includes the estimated resource requirements from the request if set.
   * Uses a default level of EXTENDED, unless overriden by the
   * 'explain_level' query option.
   */
  public String getExplainString(ArrayList<PlanFragment> fragments,
      TQueryExecRequest request) {
    // use EXTENDED by default for all non-explain statements
    TExplainLevel explainLevel = TExplainLevel.EXTENDED;
    // use the query option for explain stmts and tests (e.g., planner tests)
    if (ctx_.getAnalysisResult().isExplainStmt() || RuntimeEnv.INSTANCE.isTestEnv()) {
      explainLevel = ctx_.getQueryOptions().getExplain_level();
    }
    return getExplainString(fragments, request, explainLevel);
  }

  /**
   * Return combined explain string for all plan fragments and with an
   * explicit explain level.
   * Includes the estimated resource requirements from the request if set.
   */
  public String getExplainString(ArrayList<PlanFragment> fragments,
      TQueryExecRequest request, TExplainLevel explainLevel) {
    StringBuilder str = new StringBuilder();
    boolean hasHeader = false;
    if (request.isSetMax_per_host_min_reservation()) {
      str.append(String.format("Max Per-Host Resource Reservation: Memory=%s\n",
          PrintUtils.printBytes(request.getMax_per_host_min_reservation())));
      hasHeader = true;
    }
    if (request.isSetPer_host_mem_estimate()) {
      str.append(String.format("Per-Host Resource Estimates: Memory=%s\n",
          PrintUtils.printBytes(request.getPer_host_mem_estimate())));
      hasHeader = true;
    }
    if (request.query_ctx.disable_codegen_hint) {
      str.append("Codegen disabled by planner\n");
    }

    // IMPALA-1983 In the case of corrupt stats, issue a warning for all queries except
    // child queries of 'compute stats'.
    if (!request.query_ctx.isSetParent_query_id() &&
        request.query_ctx.isSetTables_with_corrupt_stats() &&
        !request.query_ctx.getTables_with_corrupt_stats().isEmpty()) {
      List<String> tableNames = Lists.newArrayList();
      for (TTableName tableName: request.query_ctx.getTables_with_corrupt_stats()) {
        tableNames.add(tableName.db_name + "." + tableName.table_name);
      }
      str.append(
          "WARNING: The following tables have potentially corrupt table statistics.\n" +
          "Drop and re-compute statistics to resolve this problem.\n" +
          Joiner.on(", ").join(tableNames) + "\n");
      hasHeader = true;
    }

    // Append warning about tables missing stats except for child queries of
    // 'compute stats'. The parent_query_id is only set for compute stats child queries.
    if (!request.query_ctx.isSetParent_query_id() &&
        request.query_ctx.isSetTables_missing_stats() &&
        !request.query_ctx.getTables_missing_stats().isEmpty()) {
      List<String> tableNames = Lists.newArrayList();
      for (TTableName tableName: request.query_ctx.getTables_missing_stats()) {
        tableNames.add(tableName.db_name + "." + tableName.table_name);
      }
      str.append("WARNING: The following tables are missing relevant table " +
          "and/or column statistics.\n" + Joiner.on(", ").join(tableNames) + "\n");
      hasHeader = true;
    }

    if (request.query_ctx.isSetTables_missing_diskids()) {
      List<String> tableNames = Lists.newArrayList();
      for (TTableName tableName: request.query_ctx.getTables_missing_diskids()) {
        tableNames.add(tableName.db_name + "." + tableName.table_name);
      }
      str.append("WARNING: The following tables have scan ranges with missing " +
          "disk id information.\n" + Joiner.on(", ").join(tableNames) + "\n");
      hasHeader = true;
    }

    if (request.query_ctx.isDisable_spilling()) {
      str.append("WARNING: Spilling is disabled for this query as a safety guard.\n" +
          "Reason: Query option disable_unsafe_spills is set, at least one table\n" +
          "is missing relevant stats, and no plan hints were given.\n");
      hasHeader = true;
    }
    if (hasHeader) str.append("\n");

    if (explainLevel.ordinal() < TExplainLevel.VERBOSE.ordinal()) {
      // Print the non-fragmented parallel plan.
      str.append(fragments.get(0).getExplainString(ctx_.getQueryOptions(), explainLevel));
    } else {
      // Print the fragmented parallel plan.
      for (int i = 0; i < fragments.size(); ++i) {
        PlanFragment fragment = fragments.get(i);
        str.append(fragment.getExplainString(ctx_.getQueryOptions(), explainLevel));
        if (i < fragments.size() - 1) str.append("\n");
      }
    }
    return str.toString();
  }

  /**
   * Computes the per-host resource profile for the given plans, i.e. the peak resources
   * consumed by all fragment instances belonging to the query per host. Sets the
   * per-host resource values in 'request'.
   */
  public void computeResourceReqs(List<PlanFragment> planRoots,
      TQueryCtx queryCtx, TQueryExecRequest request) {
    Preconditions.checkState(!planRoots.isEmpty());
    Preconditions.checkNotNull(request);
    TQueryOptions queryOptions = ctx_.getRootAnalyzer().getQueryOptions();
    int mtDop = queryOptions.getMt_dop();

    // Peak per-host peak resources for all plan fragments, assuming that all fragments
    // are scheduled on all nodes. The actual per-host resource requirements are computed
    // after scheduling.
    ResourceProfile maxPerHostPeakResources = ResourceProfile.invalid();

    // Do a pass over all the fragments to compute resource profiles. Compute the
    // profiles bottom-up since a fragment's profile may depend on its descendants.
    List<PlanFragment> allFragments = planRoots.get(0).getNodesPostOrder();
    for (PlanFragment fragment: allFragments) {
      // Compute the per-node, per-sink and aggregate profiles for the fragment.
      fragment.computeResourceProfile(ctx_.getRootAnalyzer());

      // Different fragments do not synchronize their Open() and Close(), so the backend
      // does not provide strong guarantees about whether one fragment instance releases
      // resources before another acquires them. Conservatively assume that all fragment
      // instances run on all backends with max DOP, and can consume their peak resources
      // at the same time, i.e. that the query-wide peak resources is the sum of the
      // per-fragment-instance peak resources.
      maxPerHostPeakResources = maxPerHostPeakResources.sum(
          fragment.getResourceProfile().multiply(fragment.getNumInstancesPerHost(mtDop)));
    }

    Preconditions.checkState(maxPerHostPeakResources.getMemEstimateBytes() >= 0,
        maxPerHostPeakResources.getMemEstimateBytes());
    Preconditions.checkState(maxPerHostPeakResources.getMinReservationBytes() >= 0,
        maxPerHostPeakResources.getMinReservationBytes());

    maxPerHostPeakResources = MIN_PER_HOST_RESOURCES.max(maxPerHostPeakResources);

    // TODO: Remove per_host_mem_estimate from the TQueryExecRequest when AC no longer
    // needs it.
    request.setPer_host_mem_estimate(maxPerHostPeakResources.getMemEstimateBytes());
    request.setMax_per_host_min_reservation(
        maxPerHostPeakResources.getMinReservationBytes());
    if (LOG.isTraceEnabled()) {
      LOG.trace("Max per-host min reservation: " +
          maxPerHostPeakResources.getMinReservationBytes());
      LOG.trace("Max estimated per-host memory: " +
          maxPerHostPeakResources.getMemEstimateBytes());
    }
  }


  /**
   * Traverses the plan tree rooted at 'root' and inverts joins in the following
   * situations:
   * 1. If the left-hand side is a SingularRowSrcNode then we invert the join because
   *    then the build side is guaranteed to have only a single row.
   * 2. There is no backend support for distributed non-equi right outer/semi joins,
   *    so we invert them (any distributed left semi/outer join is ok).
   * 3. If we estimate that the inverted join is cheaper (see isInvertedJoinCheaper()).
   *    Do not invert if relevant stats are missing.
   * The first two inversion rules are independent of the presence/absence of stats.
   * Left Null Aware Anti Joins are never inverted due to lack of backend support.
   * Joins that originate from query blocks with a straight join hint are not inverted.
   * The 'isLocalPlan' parameter indicates whether the plan tree rooted at 'root'
   * will be executed locally within one machine, i.e., without any data exchanges.
   */
  private void invertJoins(PlanNode root, boolean isLocalPlan) {
    if (root instanceof SubplanNode) {
      invertJoins(root.getChild(0), isLocalPlan);
      invertJoins(root.getChild(1), true);
    } else {
      for (PlanNode child: root.getChildren()) invertJoins(child, isLocalPlan);
    }

    if (root instanceof JoinNode) {
      JoinNode joinNode = (JoinNode) root;
      JoinOperator joinOp = joinNode.getJoinOp();

      if (!joinNode.isInvertible(isLocalPlan)) {
        // Re-compute tuple ids since their order must correspond to the order
        // of children.
        root.computeTupleIds();
        return;
      }

      if (joinNode.getChild(0) instanceof SingularRowSrcNode) {
        // Always place a singular row src on the build side because it
        // only produces a single row.
        joinNode.invertJoin();
      } else if (!isLocalPlan && joinNode instanceof NestedLoopJoinNode &&
          (joinOp.isRightSemiJoin() || joinOp.isRightOuterJoin())) {
        // The current join is a distributed non-equi right outer or semi join
        // which has no backend support. Invert the join to make it executable.
        joinNode.invertJoin();
      } else if (isInvertedJoinCheaper(joinNode, isLocalPlan)) {
        joinNode.invertJoin();
      }
    }

    // Re-compute tuple ids because the backend assumes that their order corresponds to
    // the order of children.
    root.computeTupleIds();
  }

  /**
   * Return true if we estimate that 'joinNode' will be cheaper to execute after
   * inversion. Returns false if any join input is missing relevant stats.
   *
   * For nested loop joins, we simply assume that the cost is determined by the size of
   * the build side.
   *
   * For hash joins, the cost model is more nuanced and depends on:
   * - est. number of rows in the build and probe: lhsCard and rhsCard
   * - est. size of the rows in the build and probe: lhsAvgRowSize and rhsAvgRowSize
   * - est. parallelism with which the lhs and rhs trees execute: lhsNumNodes
   *   and rhsNumNodes. The parallelism of the join is determined by the lhs.
   *
   * The assumptions are:
   * - the join strategy is PARTITIONED and rows are distributed evenly. We don't know
   *   what join strategy will be chosen until later in planning so this assumption
   *   simplifies the analysis. Generally if one input is small enough that broadcast
   *   join is viable then this formula will prefer to put that input on the right side
   *   anyway.
   * - processing a build row is twice as expensive as processing a probe row of the
   *   same size.
   * - the cost of processing each byte of a row has a fixed component (C) (e.g.
   *   hashing and comparing the row) and a variable component (e.g. looking up the
   *   hash table).
   * - The variable component grows proportionally to the log of the build side, to
   *   approximate the effect of accesses to the the hash table hitting slower levels
   *   of the memory hierarchy.
   *
   * The estimated per-host cost of a hash join before and after inversion, measured in
   * an arbitrary unit of time, is then:
   *
   *    (log_b(rhsBytes) + C) * (lhsBytes + 2 * rhsBytes) / lhsNumNodes
   *    vs.
   *    (log_b(lhsBytes) + C) * (rhsBytes + 2 * lhsBytes) / rhsNumNodes
   *
   * where lhsBytes = lhsCard * lhsAvgRowSize and rhsBytes = rhsCard * rhsAvgRowSize
   *
   * We choose b = 10 and C = 5 empirically because it seems to give reasonable
   * results for a range of inputs. The model is not particularly sensitive to the
   * parameters.
   *
   * If the parallelism of both sides is the same then this reduces to comparing
   * the size of input on both sides. Otherwise, if inverting a hash join reduces
   * parallelism significantly, then a significant difference between lhs and rhs
   * bytes is needed to justify inversion.
   */
  private boolean isInvertedJoinCheaper(JoinNode joinNode, boolean isLocalPlan) {
    long lhsCard = joinNode.getChild(0).getCardinality();
    long rhsCard = joinNode.getChild(1).getCardinality();
    // Need cardinality estimates to make a decision.
    if (lhsCard == -1 || rhsCard == -1) return false;
    double lhsBytes = lhsCard * joinNode.getChild(0).getAvgRowSize();
    double rhsBytes = rhsCard * joinNode.getChild(1).getAvgRowSize();
    if (joinNode instanceof NestedLoopJoinNode) {
      // For NLJ, simply try to minimize the size of the build side, since it needs to
      // be broadcast to all participating nodes.
      return lhsBytes < rhsBytes;
    }
    Preconditions.checkState(joinNode instanceof HashJoinNode);
    int lhsNumNodes = isLocalPlan ? 1 : joinNode.getChild(0).getNumNodes();
    int rhsNumNodes = isLocalPlan ? 1 : joinNode.getChild(1).getNumNodes();
    // Need parallelism to determine whether inverting a hash join is profitable.
    if (lhsNumNodes <= 0 || rhsNumNodes <= 0) return false;

    final long CONSTANT_COST_PER_BYTE = 5;
    // Add 1 to the log argument to avoid taking log of 0.
    double totalCost =
        (Math.log10(rhsBytes + 1) + CONSTANT_COST_PER_BYTE) * (lhsBytes + 2 * rhsBytes);
    double invertedTotalCost =
        (Math.log10(lhsBytes + 1) + CONSTANT_COST_PER_BYTE) * (rhsBytes + 2 * lhsBytes);
    double perNodeCost = totalCost / lhsNumNodes;
    double invertedPerNodeCost = invertedTotalCost / rhsNumNodes;
    if (LOG.isTraceEnabled()) {
      LOG.trace("isInvertedJoinCheaper() " + TupleId.printIds(joinNode.getTupleIds()));
      LOG.trace("lhsCard " + lhsCard + " lhsBytes " + lhsBytes +
          " lhsNumNodes " + lhsNumNodes);
      LOG.trace("rhsCard " + rhsCard + " rhsBytes " + rhsBytes +
          " rhsNumNodes " + rhsNumNodes);
      LOG.trace("cost " + perNodeCost + " invCost " + invertedPerNodeCost);
      LOG.trace("INVERT? " + (invertedPerNodeCost < perNodeCost));
    }
    return invertedPerNodeCost < perNodeCost;
  }

  /**
   * Converts hash joins to nested-loop joins if the right-side is a SingularRowSrcNode.
   * Does not convert Null Aware Anti Joins because we only support that join op with
   * a hash join.
   * Throws if JoinNode.init() fails on the new nested-loop join node.
   */
  private PlanNode useNljForSingularRowBuilds(PlanNode root, Analyzer analyzer)
      throws ImpalaException {
    for (int i = 0; i < root.getChildren().size(); ++i) {
      root.setChild(i, useNljForSingularRowBuilds(root.getChild(i), analyzer));
    }
    if (!(root instanceof JoinNode)) return root;
    if (root instanceof NestedLoopJoinNode) return root;
    if (!(root.getChild(1) instanceof SingularRowSrcNode)) return root;
    JoinNode joinNode = (JoinNode) root;
    if (joinNode.getJoinOp().isNullAwareLeftAntiJoin()) {
      Preconditions.checkState(joinNode instanceof HashJoinNode);
      return root;
    }
    List<Expr> otherJoinConjuncts = Lists.newArrayList(joinNode.getOtherJoinConjuncts());
    otherJoinConjuncts.addAll(joinNode.getEqJoinConjuncts());
    JoinNode newJoinNode = new NestedLoopJoinNode(joinNode.getChild(0),
        joinNode.getChild(1), joinNode.isStraightJoin(),
        joinNode.getDistributionModeHint(), joinNode.getJoinOp(), otherJoinConjuncts);
    newJoinNode.getConjuncts().addAll(joinNode.getConjuncts());
    newJoinNode.setId(joinNode.getId());
    newJoinNode.init(analyzer);
    return newJoinNode;
  }

  private void checkForSmallQueryOptimization(PlanNode singleNodePlan) {
    MaxRowsProcessedVisitor visitor = new MaxRowsProcessedVisitor();
    singleNodePlan.accept(visitor);
    // TODO: IMPALA-3335: support the optimization for plans with joins.
    if (!visitor.valid() || visitor.foundJoinNode()) return;
    // This optimization executes the plan on a single node so the threshold must
    // be based on the total number of rows processed.
    long maxRowsProcessed = visitor.getMaxRowsProcessed();
    int threshold = ctx_.getQueryOptions().exec_single_node_rows_threshold;
    if (maxRowsProcessed < threshold) {
      // Execute on a single node and disable codegen for small results
      ctx_.getQueryOptions().setNum_nodes(1);
      ctx_.getQueryCtx().disable_codegen_hint = true;
      if (maxRowsProcessed < ctx_.getQueryOptions().batch_size ||
          maxRowsProcessed < 1024 && ctx_.getQueryOptions().batch_size == 0) {
        // Only one scanner thread for small queries
        ctx_.getQueryOptions().setNum_scanner_threads(1);
      }
      // disable runtime filters
      ctx_.getQueryOptions().setRuntime_filter_mode(TRuntimeFilterMode.OFF);
    }
  }

  private void checkForDisableCodegen(PlanNode distributedPlan) {
    MaxRowsProcessedVisitor visitor = new MaxRowsProcessedVisitor();
    distributedPlan.accept(visitor);
    if (!visitor.valid()) return;
    // This heuristic threshold tries to determine if the per-node codegen time will
    // reduce per-node execution time enough to justify the cost of codegen. Per-node
    // execution time is correlated with the number of rows flowing through the plan.
    if (visitor.getMaxRowsProcessedPerNode()
        < ctx_.getQueryOptions().getDisable_codegen_rows_threshold()) {
      ctx_.getQueryCtx().disable_codegen_hint = true;
    }
  }

  /**
   * Insert a sort node on top of the plan, depending on the clustered/noclustered
   * plan hint and on the 'sort.columns' table property. If clustering is enabled in
   * insertStmt or additional columns are specified in the 'sort.columns' table property,
   * then the ordering columns will start with the clustering columns (key columns for
   * Kudu tables), so that partitions can be written sequentially in the table sink. Any
   * additional non-clustering columns specified by the 'sort.columns' property will be
   * added to the ordering columns and after any clustering columns. If no clustering is
   * requested and the table does not contain columns in the 'sort.columns' property, then
   * no sort node will be added to the plan.
   */
  public void createPreInsertSort(InsertStmt insertStmt, PlanFragment inputFragment,
       Analyzer analyzer) throws ImpalaException {
    List<Expr> orderingExprs = Lists.newArrayList();

    boolean partialSort = false;
    if (insertStmt.getTargetTable() instanceof KuduTable) {
      // Always sort if the 'clustered' hint is present. Otherwise, don't sort if either
      // the 'noclustered' hint is present, or this is a single node exec, or if the
      // target table is unpartitioned.
      if (insertStmt.hasClusteredHint() || (!insertStmt.hasNoClusteredHint()
          && !ctx_.isSingleNodeExec() && !insertStmt.getPartitionKeyExprs().isEmpty())) {
        orderingExprs.add(
            KuduUtil.createPartitionExpr(insertStmt, ctx_.getRootAnalyzer()));
        orderingExprs.addAll(insertStmt.getPrimaryKeyExprs());
        partialSort = true;
      }
    } else if (insertStmt.requiresClustering()) {
      orderingExprs.addAll(insertStmt.getPartitionKeyExprs());
    }
    orderingExprs.addAll(insertStmt.getSortExprs());
    // Ignore constants for the sake of clustering.
    Expr.removeConstants(orderingExprs);

    if (orderingExprs.isEmpty()) return;

    // Build sortinfo to sort by the ordering exprs.
    List<Boolean> isAscOrder = Collections.nCopies(orderingExprs.size(), true);
    List<Boolean> nullsFirstParams = Collections.nCopies(orderingExprs.size(), false);
    SortInfo sortInfo = new SortInfo(orderingExprs, isAscOrder, nullsFirstParams);

    ExprSubstitutionMap smap = sortInfo.createSortTupleInfo(
        insertStmt.getResultExprs(), analyzer);
    sortInfo.getSortTupleDescriptor().materializeSlots();

    insertStmt.substituteResultExprs(smap, analyzer);

    PlanNode node = null;
    if (partialSort) {
      node = SortNode.createPartialSortNode(
          ctx_.getNextNodeId(), inputFragment.getPlanRoot(), sortInfo);
    } else {
      node = SortNode.createTotalSortNode(
          ctx_.getNextNodeId(), inputFragment.getPlanRoot(), sortInfo, 0);
    }
    node.init(analyzer);

    inputFragment.setPlanRoot(node);
  }
}
