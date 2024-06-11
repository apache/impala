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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.ColumnLineageGraph;
import org.apache.impala.analysis.ColumnLineageGraph.ColumnLabel;
import org.apache.impala.analysis.DeleteStmt;
import org.apache.impala.analysis.DmlStatementBase;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.InsertStmt;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.analysis.QueryStmt;
import org.apache.impala.analysis.SortInfo;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.QueryConstants;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TMinmaxFilteringLevel;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryExecRequest;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TRuntimeFilterMode;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.KuduUtil;
import org.apache.impala.util.MathUtil;
import org.apache.impala.util.MaxRowsProcessedVisitor;
import org.apache.kudu.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import static org.apache.impala.analysis.ToSqlOptions.SHOW_IMPLICIT_CASTS;

/**
 * Creates an executable plan from an analyzed parse tree and query options.
 */
public class Planner {
  private final static Logger LOG = LoggerFactory.getLogger(Planner.class);

  // Minimum per-host resource requirements to ensure that no plan node set can have
  // estimates of zero, even if the contained PlanNodes have estimates of zero.
  public static final long MIN_PER_HOST_MEM_ESTIMATE_BYTES = 10 * 1024 * 1024;

  // The amount of memory added to a dedicated coordinator's memory estimate to
  // compensate for underestimation. In the general case estimates for exec
  // nodes tend to overestimate and should work fine but for estimates in the
  // 100-500 MB space, underestimates can be a problem. We pick a value of 100MB
  // because it is trivial for large estimates and small enough to not make a
  // huge impact on the coordinator's process memory (which ideally would be
  // large).
  public static final long DEDICATED_COORD_SAFETY_BUFFER_BYTES = 100 * 1024 * 1024;

  public static final ResourceProfile MIN_PER_HOST_RESOURCES =
      new ResourceProfileBuilder()
          .setMemEstimateBytes(MIN_PER_HOST_MEM_ESTIMATE_BYTES)
          .setMinMemReservationBytes(0)
          .build();

  private final PlannerContext ctx_;

  public Planner(AnalysisResult analysisResult, TQueryCtx queryCtx,
      EventSequence timeline) {
    ctx_ = new PlannerContext(analysisResult, queryCtx, timeline);
  }

  public TQueryCtx getQueryCtx() { return ctx_.getQueryCtx(); }
  public PlannerContext getPlannerCtx() { return ctx_; }
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
  private List<PlanFragment> createPlanFragments() throws ImpalaException {
    SingleNodePlanner singleNodePlanner = new SingleNodePlanner(ctx_);
    DistributedPlanner distributedPlanner = new DistributedPlanner(ctx_);
    PlanNode singleNodePlan = singleNodePlanner.createSingleNodePlan();
    ctx_.getTimeline().markEvent("Single node plan created");
    List<PlanFragment> fragments = null;

    checkForSmallQueryOptimization(singleNodePlan);

    // Join rewrites.
    invertJoins(singleNodePlan, ctx_.isSingleNodeExec());
    singleNodePlan = useNljForSingularRowBuilds(singleNodePlan, ctx_.getRootAnalyzer());

    SingleNodePlanner.validatePlan(ctx_, singleNodePlan);

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

      checkAndOverrideMinmaxFilterThresholdAndLevel(ctx_.getQueryOptions());
    }

    rootFragment.verifyTree();
    ExprSubstitutionMap rootNodeSmap = rootFragment.getPlanRoot().getOutputSmap();
    if (ctx_.isInsertOrCtas()) {
      InsertStmt insertStmt = ctx_.getAnalysisResult().getInsertStmt();
      insertStmt.substituteResultExprs(rootNodeSmap, ctx_.getRootAnalyzer());
      if (!ctx_.isSingleNodeExec()) {
        // Repartition on partition keys
        rootFragment = distributedPlanner.createDmlFragment(
            rootFragment, insertStmt, ctx_.getRootAnalyzer(), fragments);
      }
      // Add optional sort node to the plan, based on clustered/noclustered plan hint.
      createPreInsertSort(insertStmt, rootFragment, ctx_.getRootAnalyzer());
      // set up table sink for root fragment
      rootFragment.setSink(insertStmt.createDataSink());
    } else if (ctx_.isUpdate() || ctx_.isDelete() || ctx_.isOptimize()) {
      DmlStatementBase stmt;
      if (ctx_.isUpdate()) {
        stmt = ctx_.getAnalysisResult().getUpdateStmt();
      } else if (ctx_.isDelete()) {
        stmt = ctx_.getAnalysisResult().getDeleteStmt();
      } else {
        stmt = ctx_.getAnalysisResult().getOptimizeStmt();
      }
      Preconditions.checkNotNull(stmt);
      stmt.substituteResultExprs(rootNodeSmap, ctx_.getRootAnalyzer());
      if (stmt.getTargetTable() instanceof FeIcebergTable) {
        rootFragment = createIcebergDmlPlanFragment(
            rootFragment, distributedPlanner, stmt, fragments);
      }
      // Set up update sink for root fragment
      rootFragment.setSink(stmt.createDataSink());
    } else if (ctx_.isQuery()) {
      QueryStmt queryStmt = ctx_.getQueryStmt();
      queryStmt.substituteResultExprs(rootNodeSmap, ctx_.getRootAnalyzer());
      List<Expr> resultExprs = queryStmt.getResultExprs();
      rootFragment.setSink(
          ctx_.getAnalysisResult().getQueryStmt().createDataSink(resultExprs));
    }

    // The check for disabling codegen uses estimates of rows per node so must be done
    // on the distributed plan.
    checkForDisableCodegen(rootFragment.getPlanRoot());

    if (LOG.isTraceEnabled()) {
      LOG.trace("desctbl: " + ctx_.getRootAnalyzer().getDescTbl().debugString());
      LOG.trace("root sink: " + rootFragment.getSink().getExplainString(
            "", "", ctx_.getQueryOptions(), TExplainLevel.VERBOSE));
      LOG.trace("finalize plan fragments");
    }
    for (PlanFragment fragment: fragments) {
      fragment.finalizeExchanges(ctx_.getRootAnalyzer());
    }

    Collections.reverse(fragments);
    ctx_.getTimeline().markEvent("Distributed plan created");
    ctx_.getRootAnalyzer().logCacheStats();

    ColumnLineageGraph graph = ctx_.getRootAnalyzer().getColumnLineageGraph();
    if (BackendConfig.INSTANCE.getComputeLineage() || RuntimeEnv.INSTANCE.isTestEnv()) {
      // Lineage is disabled for UPDATE AND DELETE statements
      if (ctx_.isUpdateOrDelete()) return fragments;
      // Compute the column lineage graph
      if (ctx_.isInsertOrCtas()) {
        InsertStmt insertStmt = ctx_.getAnalysisResult().getInsertStmt();
        FeTable targetTable = insertStmt.getTargetTable();
        Preconditions.checkNotNull(targetTable);
        if (targetTable instanceof FeKuduTable) {
          if (ctx_.isInsert()) {
            // For insert statements on Kudu tables, we only need to consider
            // the labels of columns mentioned in the column list.
            List<String> mentionedColumns = insertStmt.getMentionedColumns();
            Preconditions.checkState(!mentionedColumns.isEmpty());
            List<ColumnLabel> targetColLabels = new ArrayList<>();
            for (String column: mentionedColumns) {
              targetColLabels.add(new ColumnLabel(column, targetTable.getTableName(),
                  ColumnLineageGraph.getTableType(targetTable)));
            }
            graph.addTargetColumnLabels(targetColLabels);
          } else {
            Preconditions.checkState(ctx_.isCtas());
            if (((FeKuduTable)targetTable).hasAutoIncrementingColumn()) {
              // Omit auto-incrementing column for Kudu table since the column is not in
              // expression. The auto-incrementing column is only added to target table
              // for CTAS statement so that the table has same layout as the table
              // created by Kudu engine. We don't need to compute Lineage graph for the
              // column.
              List<ColumnLabel> targetColLabels = new ArrayList<>();
              for (String column: targetTable.getColumnNames()) {
                if (column.equals(Schema.getAutoIncrementingColumnName())) continue;
                targetColLabels.add(new ColumnLabel(column, targetTable.getTableName(),
                    ColumnLineageGraph.getTableType(targetTable)));
              }
              graph.addTargetColumnLabels(targetColLabels);
            } else {
              graph.addTargetColumnLabels(targetTable);
            }
          }
        } else if (targetTable instanceof FeHBaseTable) {
          graph.addTargetColumnLabels(targetTable);
        } else {
          graph.addTargetColumnLabels(targetTable);
        }
      } else {
        graph.addTargetColumnLabels(ctx_.getQueryStmt().getColLabels().stream()
            .map(col -> new ColumnLabel(col))
            .collect(Collectors.toList()));
      }
      List<Expr> outputExprs = new ArrayList<>();
      rootFragment.getSink().collectExprs(outputExprs);
      graph.computeLineageGraph(outputExprs, ctx_.getRootAnalyzer());
      if (LOG.isTraceEnabled()) LOG.trace("lineage: " + graph.debugString());
      ctx_.getTimeline().markEvent("Lineage info computed");
    }

    return fragments;
  }

  public PlanFragment createIcebergDmlPlanFragment(PlanFragment rootFragment,
      DistributedPlanner distributedPlanner, DmlStatementBase stmt,
      List<PlanFragment> fragments) throws ImpalaException {
    if (!ctx_.isSingleNodeExec()) {
      // Repartition on partition keys
      rootFragment = distributedPlanner.createDmlFragment(
          rootFragment, stmt, ctx_.getRootAnalyzer(), fragments);
    }
    // We don't need to add a SORT node for DELETE operations as we are using the
    // IcebergBufferedDeleteSink. UPDATE/MERGE statements will still require to
    // sort their data records.
    if (!(stmt instanceof DeleteStmt)) {
      createPreDmlSort(stmt, rootFragment, ctx_.getRootAnalyzer());
    }
    return rootFragment;
  }

  /**
   * Return a list of plans, each represented by the root of their fragment trees. May
   * return a single-node, distributed, or parallel plan depending on the query and
   * configuration.
   */
  public List<PlanFragment> createPlans() throws ImpalaException {
    List<PlanFragment> distrPlan = createPlanFragments();
    Preconditions.checkNotNull(distrPlan);
    if (useParallelPlan(ctx_)) {
      ParallelPlanner parallelPlanner = new ParallelPlanner(ctx_);
      distrPlan = parallelPlanner.createPlans(distrPlan.get(0));
      ctx_.getTimeline().markEvent("Parallel plans created");
    } else {
      distrPlan = Collections.singletonList(distrPlan.get(0));
    }
    // TupleCachePlanner comes last, because it needs to compute the eligibility of
    // various locations in the PlanNode tree. Runtime filters and other modifications
    // to the tree can change this, so this comes after all those modifications are
    // complete.
    if (useTupleCache(ctx_)) {
      TupleCachePlanner cachePlanner = new TupleCachePlanner(ctx_);
      distrPlan = cachePlanner.createPlans(distrPlan);
      ctx_.getTimeline().markEvent("Tuple caching plan created");
    }
    return distrPlan;
  }

  /**
   * Return true if we should generate a parallel plan for this query, based on the
   * current parallelism mode and whether a single-node plan was chosen.
   */
  public static boolean useParallelPlan(PlannerContext planCtx) {
    return useMTFragment(planCtx.getQueryOptions()) && !planCtx.isSingleNodeExec();
  }

  // Return true if MT_DOP>0 or COMPUTE_PROCESSING_COST=true.
  public static boolean useMTFragment(TQueryOptions queryOptions) {
    Preconditions.checkState(queryOptions.isSetMt_dop());
    return queryOptions.getMt_dop() > 0 || queryOptions.isCompute_processing_cost();
  }

  // Return true if ENABLE_TUPLE_CACHE=true
  public static boolean useTupleCache(PlannerContext planCtx) {
    return planCtx.getQueryOptions().isEnable_tuple_cache();
  }

  /**
   * Return combined explain string for all plan fragments.
   * Includes the estimated resource requirements from the request if set.
   * Uses a default level of EXTENDED, unless overriden by the
   * 'explain_level' query option.
   */
  public String getExplainString(List<PlanFragment> fragments,
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
  public String getExplainString(List<PlanFragment> fragments,
      TQueryExecRequest request, TExplainLevel explainLevel) {
    StringBuilder str = new StringBuilder();
    boolean hasHeader = false;

    // Only some requests (queries, DML, etc) have a resource profile.
    if (request.isSetMax_per_host_min_mem_reservation()) {
      Preconditions.checkState(request.isSetMax_per_host_thread_reservation());
      Preconditions.checkState(request.isSetPer_host_mem_estimate());
      str.append(String.format(
          "Max Per-Host Resource Reservation: Memory=%s Threads=%d\n",
          PrintUtils.printBytes(request.getMax_per_host_min_mem_reservation()),
          request.getMax_per_host_thread_reservation()));
      str.append(String.format("Per-Host Resource Estimates: Memory=%s\n",
          PrintUtils.printBytesRoundedToMb(request.getPer_host_mem_estimate())));
      if (BackendConfig.INSTANCE.useDedicatedCoordinatorEstimates()) {
        str.append(String.format("Dedicated Coordinator Resource Estimate: Memory=%s\n",
            PrintUtils.printBytesRoundedToMb(request.getDedicated_coord_mem_estimate())));
      }
      hasHeader = true;
    }
    // Warn if the planner is running in DEBUG mode.
    if (request.query_ctx.client_request.query_options.planner_testcase_mode) {
      str.append("WARNING: The planner is running in TESTCASE mode. This should only be "
          + "used by developers for debugging.\nTo disable it, do SET " +
          "PLANNER_TESTCASE_MODE=false.\n");
    }
    if (request.query_ctx.disable_codegen_hint) {
      str.append("Codegen disabled by planner\n");
    }

    // IMPALA-1983 In the case of corrupt stats, issue a warning for all queries except
    // child queries of 'compute stats'.
    if (!request.query_ctx.isSetParent_query_id() &&
        request.query_ctx.isSetTables_with_corrupt_stats() &&
        !request.query_ctx.getTables_with_corrupt_stats().isEmpty()) {
      List<String> tableNames = new ArrayList<>();
      for (TTableName tableName: request.query_ctx.getTables_with_corrupt_stats()) {
        tableNames.add(tableName.db_name + "." + tableName.table_name);
      }
      str.append(
          "The row count in one or more partitions in the following tables \n" +
          "is either a) less than -1, or b) 0 but the size of all the files inside \n" +
          "the partition(s) is positive.\n" +
          "The latter case does not necessarily imply the existence of corrupt \n" +
          "statistics when the corresponding tables are transactional.\n" +
          "If it is suspected that there may be corrupt statistics, dropping and \n" +
          "re-computing statistics could resolve this problem.\n" +
          Joiner.on(", ").join(tableNames) + "\n");
      hasHeader = true;
    }

    // Append warning about tables missing stats except for child queries of
    // 'compute stats'. The parent_query_id is only set for compute stats child queries.
    if (!request.query_ctx.isSetParent_query_id() &&
        request.query_ctx.isSetTables_missing_stats() &&
        !request.query_ctx.getTables_missing_stats().isEmpty()) {
      List<String> tableNames = new ArrayList<>();
      for (TTableName tableName: request.query_ctx.getTables_missing_stats()) {
        tableNames.add(tableName.db_name + "." + tableName.table_name);
      }
      str.append("WARNING: The following tables are missing relevant table " +
          "and/or column statistics.\n" + Joiner.on(", ").join(tableNames) + "\n");
      hasHeader = true;
    }

    if (request.query_ctx.isSetTables_missing_diskids()) {
      List<String> tableNames = new ArrayList<>();
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

    if (explainLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      // In extended explain include the analyzed query text showing implicit casts
      String queryText = ctx_.getQueryStmt().toSql(SHOW_IMPLICIT_CASTS);
      String wrappedText = PrintUtils.wrapString("Analyzed query: " + queryText, 80);
      str.append(wrappedText).append("\n");
      hasHeader = true;
    }
    // Note that the analyzed query text must be the last thing in the header.
    // This is to help tests that parse the header.

    // Add the blank line that indicates the end of the header
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
   * Adjust effective parallelism of each plan fragment of query after considering
   * processing cost rate and blocking operator.
   * <p>
   * Only valid after {@link PlanFragment#computeCostingSegment(TQueryOptions)}
   * has been called for all plan fragments in the list.
   */
  private static void computeEffectiveParallelism(List<PlanFragment> postOrderFragments,
      int minThreadPerNode, int maxThreadPerNode, TQueryOptions queryOptions) {
    Preconditions.checkArgument(minThreadPerNode > 0);
    Preconditions.checkArgument(maxThreadPerNode >= minThreadPerNode);
    Preconditions.checkArgument(
        QueryConstants.MAX_FRAGMENT_INSTANCES_PER_NODE >= maxThreadPerNode);
    for (PlanFragment fragment : postOrderFragments) {
      if (!(fragment.getSink() instanceof JoinBuildSink)) {
        // Only adjust parallelism of non-join build fragment.
        // Join build fragment will be adjusted later by fragment hosting the join node.
        fragment.traverseEffectiveParallelism(
            minThreadPerNode, maxThreadPerNode, null, queryOptions);
      }
    }

    for (PlanFragment fragment : postOrderFragments) {
      fragment.setEffectiveNumInstance();
    }
  }

  /**
   * This method returns the effective CPU requirement of a query when considering
   * processing cost rate and blocking operator.
   * <p>
   * Only valid after {@link #computeEffectiveParallelism(List, int, int, TQueryOptions)}
   * has been called over the plan fragment list.
   */
  private static CoreCount computeBlockingAwareCores(
      List<PlanFragment> postOrderFragments, boolean findUnboundedCount) {
    // fragmentCoreState is a mapping between a fragment (through its PlanFragmentId) and
    // its CoreCount. The first element of the pair is the CoreCount of subtree rooted at
    // that fragment. The second element of the pair is the CoreCount of blocking-child
    // subtrees under that fragment. The effective CoreCount of a fragment is derived from
    // the pair through the following formula:
    //   max(Pair.first, sum(Pair.second))
    Map<PlanFragmentId, Pair<CoreCount, List<CoreCount>>> fragmentCoreState =
        Maps.newHashMap();

    for (PlanFragment fragment : postOrderFragments) {
      fragment.computeBlockingAwareCores(fragmentCoreState, findUnboundedCount);
    }

    PlanFragment root = postOrderFragments.get(postOrderFragments.size() - 1);
    Pair<CoreCount, List<CoreCount>> rootCores = fragmentCoreState.get(root.getId());

    CoreCount maxCores = root.maxCore(
        rootCores.first, CoreCount.sum(rootCores.second), findUnboundedCount);
    return maxCores;
  }

  /**
   * Reduce plan node cardinalities based on runtime filter information.
   * Valid to call after runtime filter generation and before processing cost
   * computation.
   */
  public static void reduceCardinalityByRuntimeFilter(
      List<PlanFragment> planRoots, PlannerContext planCtx) {
    double reductionScale = planCtx.getRootAnalyzer()
                                .getQueryOptions()
                                .getRuntime_filter_cardinality_reduction_scale();
    if (reductionScale <= 0) return;
    PlanFragment rootFragment = planRoots.get(0);
    Stack<PlanNode> nodeStack = new Stack<>();
    rootFragment.getPlanRoot().reduceCardinalityByRuntimeFilter(
        nodeStack, reductionScale);
  }

  /**
   * Compute processing cost of each plan fragment in the query plan and adjust each
   * fragment parallelism according to producer-consumer rate between them.
   */
  public static void computeProcessingCost(
      List<PlanFragment> planRoots, TQueryExecRequest request, PlannerContext planCtx) {
    Analyzer rootAnalyzer = planCtx.getRootAnalyzer();
    TQueryOptions queryOptions = rootAnalyzer.getQueryOptions();

    PlanFragment rootFragment = planRoots.get(0);
    List<PlanFragment> postOrderFragments = new ArrayList<>();
    boolean testCostCalculation = queryOptions.isEnable_replan()
        && (RuntimeEnv.INSTANCE.isTestEnv() || queryOptions.isTest_replan());
    if (queryOptions.isCompute_processing_cost() || testCostCalculation) {
      postOrderFragments = rootFragment.getNodesPostOrder();
      for (PlanFragment fragment : postOrderFragments) {
        fragment.computeCostingSegment(queryOptions);
      }
    }

    // Only do parallelism adjustment if COMPUTE_PROCESSING_COST is enabled.
    if (!queryOptions.isCompute_processing_cost()) {
      request.setCores_required(-1);
      return;
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Computing effective parallelism."
          + " numNode=" + rootAnalyzer.numExecutorsForPlanning()
          + " availableCoresPerNode=" + rootAnalyzer.getAvailableCoresPerNode()
          + " minThreads=" + rootAnalyzer.getMinParallelismPerNode()
          + " maxThreads=" + rootAnalyzer.getMaxParallelismPerNode());
    }

    computeEffectiveParallelism(postOrderFragments,
        rootAnalyzer.getMinParallelismPerNode(), rootAnalyzer.getMaxParallelismPerNode(),
        queryOptions);

    // Count bounded core count. This is taken from final instance count from previous
    // step.
    CoreCount boundedCores = computeBlockingAwareCores(postOrderFragments, false);
    Set<PlanFragmentId> dominantFragmentIds =
        new HashSet<>(boundedCores.getUniqueFragmentIds());
    int coresRequired = Math.max(1, boundedCores.totalWithoutCoordinator());
    if (boundedCores.hasCoordinator()) {
      // exclude coordinator fragment from dominantFragmentIds.
      dominantFragmentIds.remove(rootFragment.getId());
    }
    request.setCores_required(coresRequired);
    LOG.info("CoreCount=" + boundedCores + ", coresRequired=" + coresRequired);

    // Mark dominant fragment. This will be used by scheduler in scheduler.cc to count
    // admission slot requirement.
    for (PlanFragment fragment : postOrderFragments) {
      if (dominantFragmentIds.contains(fragment.getId())) fragment.markDominant();
    }

    // Count unbounded core count. This is based on maximum parallelism of each fragment.
    CoreCount unboundedCores = computeBlockingAwareCores(postOrderFragments, true);
    int coresRequiredUnbounded = Math.max(1, unboundedCores.totalWithoutCoordinator());
    request.setCores_required_unbounded(coresRequiredUnbounded);
    LOG.info("CoreCountUnbounded=" + unboundedCores
        + ", coresRequiredUnbounded=" + coresRequiredUnbounded);
  }

  /**
   * Computes the per-host resource profile for the given plans, i.e. the peak resources
   * consumed by all fragment instances belonging to the query per host. Sets the
   * per-host resource values in 'request'.
   */
  public static void computeResourceReqs(List<PlanFragment> planRoots,
      TQueryCtx queryCtx, TQueryExecRequest request, PlannerContext planCtx,
      boolean isQueryStmt) {
    Preconditions.checkState(!planRoots.isEmpty());
    Preconditions.checkNotNull(request);
    TQueryOptions queryOptions = planCtx.getRootAnalyzer().getQueryOptions();

    // Peak per-host peak resources for all plan fragments, assuming that all fragments
    // are scheduled on all nodes. The actual per-host resource requirements are computed
    // after scheduling.
    ResourceProfile maxPerHostPeakResources = ResourceProfile.invalid();
    long totalRuntimeFilterMemBytes = 0;

    // Do a pass over all the fragments to compute resource profiles. Compute the
    // profiles bottom-up since a fragment's profile may depend on its descendants.
    PlanFragment rootFragment = planRoots.get(0);
    List<PlanFragment> allFragments = rootFragment.getNodesPostOrder();
    boolean trivial =
        TrivialQueryChecker.IsTrivial(rootFragment, queryOptions, isQueryStmt);

    for (PlanFragment fragment: allFragments) {
      // Compute the per-node, per-sink and aggregate profiles for the fragment.
      fragment.computeResourceProfile(planCtx.getRootAnalyzer());

      // Different fragments do not synchronize their Open() and Close(), so the backend
      // does not provide strong guarantees about whether one fragment instance releases
      // resources before another acquires them. Conservatively assume that all fragment
      // instances run on all backends with max DOP, and can consume their peak resources
      // at the same time, i.e. that the query-wide peak resources is the sum of the
      // per-fragment-instance peak resources.
      // TODO: IMPALA-9255: take into account parallel plan dependencies.
      maxPerHostPeakResources = maxPerHostPeakResources.sum(
          fragment.getTotalPerBackendResourceProfile(queryOptions));
      // Coordinator has to have a copy of each of the runtime filters to perform filter
      // aggregation. Note that this overestimates because it includes local runtime
      // filters that do not go via the coordinator.
      totalRuntimeFilterMemBytes +=
          fragment.getProducedRuntimeFiltersMemReservationBytes();
    }
    rootFragment.computePipelineMembership();

    Preconditions.checkState(maxPerHostPeakResources.getMemEstimateBytes() >= 0,
        maxPerHostPeakResources.getMemEstimateBytes());
    Preconditions.checkState(maxPerHostPeakResources.getMinMemReservationBytes() >= 0,
        maxPerHostPeakResources.getMinMemReservationBytes());

    maxPerHostPeakResources = MIN_PER_HOST_RESOURCES.max(maxPerHostPeakResources);

    request.setPer_host_mem_estimate(maxPerHostPeakResources.getMemEstimateBytes());
    request.setPlanner_per_host_mem_estimate(
        maxPerHostPeakResources.getMemEstimateBytes());
    request.setIs_trivial_query(trivial);
    request.setMax_per_host_min_mem_reservation(
        maxPerHostPeakResources.getMinMemReservationBytes());
    request.setMax_per_host_thread_reservation(
        maxPerHostPeakResources.getThreadReservation());
    if (isQueryStmt) {
      // Only one instance of root fragment, so don't need to multiply instance resource
      // profile.
      ResourceProfile rootFragmentResourceProfile =
          rootFragment.getPerInstanceResourceProfile().sum(
              rootFragment.getPerBackendResourceProfile());
      request.setDedicated_coord_mem_estimate(
          MathUtil.saturatingAdd(rootFragmentResourceProfile.getMemEstimateBytes(),
              totalRuntimeFilterMemBytes + DEDICATED_COORD_SAFETY_BUFFER_BYTES));
    } else {
      // For queries that don't have a coordinator fragment, estimate a small
      // amount of memory that the query state spwaned on the coordinator can use.
      request.setDedicated_coord_mem_estimate(totalRuntimeFilterMemBytes +
          DEDICATED_COORD_SAFETY_BUFFER_BYTES);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Max per-host min reservation: " +
          maxPerHostPeakResources.getMinMemReservationBytes());
      LOG.trace("Max estimated per-host memory: " +
          maxPerHostPeakResources.getMemEstimateBytes());
      LOG.trace("Max estimated per-host thread reservation: " +
          maxPerHostPeakResources.getThreadReservation());
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
  public static void invertJoins(PlanNode root, boolean isLocalPlan) {
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
      // Re-compute the numNodes and numInstances based on the new input order
      joinNode.recomputeNodes();
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
  public static boolean isInvertedJoinCheaper(JoinNode joinNode, boolean isLocalPlan) {
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

  public static void checkForSmallQueryOptimization(PlanNode singleNodePlan,
      PlannerContext ctx) {
    MaxRowsProcessedVisitor visitor = new MaxRowsProcessedVisitor();
    singleNodePlan.accept(visitor);
    if (!visitor.valid()) return;
    // This optimization executes the plan on a single node so the threshold must
    // be based on the total number of rows processed.
    long maxRowsProcessed = visitor.getMaxRowsProcessed();
    int threshold = ctx.getQueryOptions().exec_single_node_rows_threshold;
    if (maxRowsProcessed < threshold) {
      // Execute on a single node and disable codegen for small results
      LOG.trace("Query is small enough to execute on a single node: maxRowsProcessed = "
          + maxRowsProcessed);
      ctx.getQueryOptions().setNum_nodes(1);
      ctx.getQueryCtx().disable_codegen_hint = true;
      if (maxRowsProcessed < ctx.getQueryOptions().batch_size ||
          maxRowsProcessed < 1024 && ctx.getQueryOptions().batch_size == 0) {
        // Only one scanner thread for small queries
        ctx.getQueryOptions().setNum_scanner_threads(1);
      }
      // disable runtime filters
      ctx.getQueryOptions().setRuntime_filter_mode(TRuntimeFilterMode.OFF);
    }
  }

  private void checkForSmallQueryOptimization(PlanNode singleNodePlan) {
      checkForSmallQueryOptimization(singleNodePlan, ctx_);
  }

  /**
   * The MINMAX_FILTER_SORTED_COLUMNS query option could override the filter threshold
   * (query option MINMAX_FILTER_THRESHOLD) and the filtering level (query option
   * MINMAX_FILTERING_LEVEL) if set and the threshold is 0.0.
   */
  private void checkAndOverrideMinmaxFilterThresholdAndLevel(TQueryOptions queryOptions) {
    if (queryOptions.parquet_read_statistics) {
      if (queryOptions.isMinmax_filter_sorted_columns()) {
        if (queryOptions.getMinmax_filter_threshold() == 0.0) {
          queryOptions.setMinmax_filter_threshold(0.5);
          queryOptions.setMinmax_filtering_level(TMinmaxFilteringLevel.PAGE);
        }
      }
      if (queryOptions.isMinmax_filter_partition_columns()) {
        if (queryOptions.getMinmax_filter_threshold() == 0.0) {
          queryOptions.setMinmax_filter_threshold(0.5);
        }
      }
    }
  }

  public static void checkForDisableCodegen(PlanNode distributedPlan,
      PlannerContext ctx) {
    MaxRowsProcessedVisitor visitor = new MaxRowsProcessedVisitor();
    distributedPlan.accept(visitor);
    if (!visitor.valid()) return;
    // This heuristic threshold tries to determine if the per-node codegen time will
    // reduce per-node execution time enough to justify the cost of codegen. Per-node
    // execution time is correlated with the number of rows flowing through the plan.
    if (visitor.getMaxRowsProcessedPerNode()
        < ctx.getQueryOptions().getDisable_codegen_rows_threshold()) {
      ctx.getQueryCtx().disable_codegen_hint = true;
    }
  }

  private void checkForDisableCodegen(PlanNode distributedPlan) {
      checkForDisableCodegen(distributedPlan, ctx_);
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
    List<Expr> orderingExprs = new ArrayList<>();

    boolean partialSort = false;
    int numPartitionKeys = 0;
    if (insertStmt.getTargetTable() instanceof FeKuduTable) {
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
      List<Expr> partKeys = insertStmt.getPartitionKeyExprs();
      orderingExprs.addAll(partKeys);
      // Ignore constants. Only dynamic partition inserts have non constant keys.
      Expr.removeConstants(orderingExprs);
      numPartitionKeys = orderingExprs.size();
    }
    orderingExprs.addAll(insertStmt.getSortExprs());
    // Ignore constants for the sake of clustering.
    Expr.removeConstants(orderingExprs);

    if (orderingExprs.isEmpty()) return;

    // Build sortinfo to sort by the ordering exprs.
    List<Boolean> isAscOrder = Collections.nCopies(orderingExprs.size(), true);
    List<Boolean> nullsFirstParams = Collections.nCopies(orderingExprs.size(), false);
    SortInfo sortInfo = new SortInfo(orderingExprs, isAscOrder, nullsFirstParams,
        insertStmt.getSortingOrder());
    sortInfo.setNumLexicalKeysInZOrder(numPartitionKeys);
    sortInfo.createSortTupleInfo(insertStmt.getResultExprs(), analyzer);
    sortInfo.getSortTupleDescriptor().materializeSlots();
    insertStmt.substituteResultExprs(sortInfo.getOutputSmap(), analyzer);

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

  public void createPreDmlSort(DmlStatementBase dmlStmt, PlanFragment inputFragment,
      Analyzer analyzer) throws ImpalaException {
    List<Expr> orderingExprs = new ArrayList<>();

    List<Expr> partitionKeyExprs = dmlStmt.getPartitionKeyExprs();
    orderingExprs.addAll(partitionKeyExprs);
    orderingExprs.addAll(dmlStmt.getSortExprs());

    if (orderingExprs.isEmpty()) return;

    // Build sortinfo to sort by the ordering exprs.
    List<Boolean> isAscOrder = Collections.nCopies(orderingExprs.size(), true);
    List<Boolean> nullsFirstParams = Collections.nCopies(orderingExprs.size(), false);
    SortInfo sortInfo = new SortInfo(orderingExprs, isAscOrder, nullsFirstParams,
        dmlStmt.getSortingOrder());
    int numPartitionKeys = partitionKeyExprs.size();
    sortInfo.setNumLexicalKeysInZOrder(numPartitionKeys);
    sortInfo.createSortTupleInfo(dmlStmt.getResultExprs(), analyzer);
    sortInfo.getSortTupleDescriptor().materializeSlots();
    dmlStmt.substituteResultExprs(sortInfo.getOutputSmap(), analyzer);

    PlanNode node = SortNode.createTotalSortNode(
        ctx_.getNextNodeId(), inputFragment.getPlanRoot(), sortInfo, 0);
    node.init(analyzer);

    inputFragment.setPlanRoot(node);
  }
}
