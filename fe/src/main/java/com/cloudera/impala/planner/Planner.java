package com.cloudera.impala.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.ColumnLineageGraph;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.ExprSubstitutionMap;
import com.cloudera.impala.analysis.InsertStmt;
import com.cloudera.impala.analysis.QueryStmt;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.PrintUtils;
import com.cloudera.impala.common.RuntimeEnv;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TRuntimeFilterMode;
import com.cloudera.impala.thrift.TQueryCtx;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TTableName;
import com.cloudera.impala.util.MaxRowsProcessedVisitor;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Creates an executable plan from an analyzed parse tree and query options.
 */
public class Planner {
  private final static Logger LOG = LoggerFactory.getLogger(Planner.class);

  private final PlannerContext ctx_;

  public Planner(AnalysisContext.AnalysisResult analysisResult, TQueryCtx queryCtx) {
    ctx_ = new PlannerContext(analysisResult, queryCtx);
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
    ctx_.getRootAnalyzer().getTimeline().markEvent("Single node plan created");
    ArrayList<PlanFragment> fragments = null;

    // Determine the maximum number of rows processed by any node in the plan tree
    MaxRowsProcessedVisitor visitor = new MaxRowsProcessedVisitor();
    singleNodePlan.accept(visitor);
    long maxRowsProcessed = visitor.get() == -1 ? Long.MAX_VALUE : visitor.get();
    boolean isSmallQuery =
        maxRowsProcessed < ctx_.getQueryOptions().exec_single_node_rows_threshold;
    if (isSmallQuery) {
      // Execute on a single node and disable codegen for small results
      ctx_.getQueryOptions().setNum_nodes(1);
      ctx_.getQueryOptions().setDisable_codegen(true);
      if (maxRowsProcessed < ctx_.getQueryOptions().batch_size ||
          maxRowsProcessed < 1024 && ctx_.getQueryOptions().batch_size == 0) {
        // Only one scanner thread for small queries
        ctx_.getQueryOptions().setNum_scanner_threads(1);
      }
    } else if (
      ctx_.getQueryOptions().getRuntime_filter_mode() != TRuntimeFilterMode.OFF) {
      // Always compute filters, even if the BE won't always use all of them.
      RuntimeFilterGenerator.generateRuntimeFilters(ctx_.getRootAnalyzer(),
          singleNodePlan, ctx_.getQueryOptions().getMax_num_runtime_filters());
      ctx_.getRootAnalyzer().getTimeline().markEvent(
          "Runtime filters computed");
    }

    if (ctx_.isSingleNodeExec()) {
      // create one fragment containing the entire single-node plan tree
      fragments = Lists.newArrayList(new PlanFragment(
          ctx_.getNextFragmentId(), singleNodePlan, DataPartition.UNPARTITIONED));
    } else {
      singleNodePlanner.validatePlan(singleNodePlan);
      // create distributed plan
      fragments = distributedPlanner.createPlanFragments(singleNodePlan);
    }

    PlanFragment rootFragment = fragments.get(fragments.size() - 1);
    ExprSubstitutionMap rootNodeSmap = rootFragment.getPlanRoot().getOutputSmap();
    ColumnLineageGraph graph = ctx_.getRootAnalyzer().getColumnLineageGraph();
    List<Expr> resultExprs = null;
    Table targetTable = null;
    if (ctx_.isInsertOrCtas()) {
      InsertStmt insertStmt = ctx_.getAnalysisResult().getInsertStmt();
      insertStmt.substituteResultExprs(rootNodeSmap, ctx_.getRootAnalyzer());
      resultExprs = insertStmt.getResultExprs();
      targetTable = insertStmt.getTargetTable();
      graph.addTargetColumnLabels(targetTable);
      if (!ctx_.isSingleNodeExec()) {
        // repartition on partition keys
        rootFragment = distributedPlanner.createInsertFragment(
            rootFragment, insertStmt, ctx_.getRootAnalyzer(), fragments);
      }
      // set up table sink for root fragment
      rootFragment.setSink(insertStmt.createDataSink());
    } else {
      QueryStmt queryStmt = ctx_.getQueryStmt();
      queryStmt.substituteResultExprs(rootNodeSmap, ctx_.getRootAnalyzer());
      resultExprs = queryStmt.getResultExprs();
      graph.addTargetColumnLabels(ctx_.getQueryStmt().getColLabels());
    }
    rootFragment.setOutputExprs(resultExprs);

    LOG.debug("desctbl: " + ctx_.getRootAnalyzer().getDescTbl().debugString());
    LOG.debug("resultexprs: " + Expr.debugString(rootFragment.getOutputExprs()));
    LOG.debug("finalize plan fragments");
    for (PlanFragment fragment: fragments) {
      fragment.finalize(ctx_.getRootAnalyzer());
    }

    Collections.reverse(fragments);
    ctx_.getRootAnalyzer().getTimeline().markEvent("Distributed plan created");

    if (RuntimeEnv.INSTANCE.computeLineage() || RuntimeEnv.INSTANCE.isTestEnv()) {
      // Compute the column lineage graph
      if (ctx_.isInsertOrCtas()) {
        Preconditions.checkNotNull(targetTable);
        List<Expr> exprs = Lists.newArrayList();
        if (targetTable instanceof HBaseTable) {
          exprs.addAll(resultExprs);
        } else {
          exprs.addAll(ctx_.getAnalysisResult().getInsertStmt().getPartitionKeyExprs());
          exprs.addAll(resultExprs.subList(0,
              targetTable.getNonClusteringColumns().size()));
        }
        graph.computeLineageGraph(exprs, ctx_.getRootAnalyzer());
      } else {
        graph.computeLineageGraph(resultExprs, ctx_.getRootAnalyzer());
      }
      LOG.trace("lineage: " + graph.debugString());
      ctx_.getRootAnalyzer().getTimeline().markEvent("Lineage info computed");
    }

    return fragments;
  }

  /**
   * Return combined explain string for all plan fragments.
   * Includes the estimated resource requirements from the request if set.
   */
  public String getExplainString(ArrayList<PlanFragment> fragments,
      TQueryExecRequest request, TExplainLevel explainLevel) {
    StringBuilder str = new StringBuilder();
    boolean hasHeader = false;
    if (request.isSetPer_host_mem_req() && request.isSetPer_host_vcores()) {
      str.append(
          String.format("Estimated Per-Host Requirements: Memory=%s VCores=%s\n",
          PrintUtils.printBytes(request.getPer_host_mem_req()),
          request.per_host_vcores));
      hasHeader = true;
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
      str.append("WARNING: The following tables have potentially corrupt table\n" +
          "statistics. Drop and re-compute statistics to resolve this problem.\n" +
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

    if (request.query_ctx.isDisable_spilling()) {
      str.append("WARNING: Spilling is disabled for this query as a safety guard.\n" +
          "Reason: Query option disable_unsafe_spills is set, at least one table\n" +
          "is missing relevant stats, and no plan hints were given.\n");
      hasHeader = true;
    }
    if (hasHeader) str.append("\n");

    if (explainLevel.ordinal() < TExplainLevel.VERBOSE.ordinal()) {
      // Print the non-fragmented parallel plan.
      str.append(fragments.get(0).getExplainString(explainLevel));
    } else {
      // Print the fragmented parallel plan.
      for (int i = 0; i < fragments.size(); ++i) {
        PlanFragment fragment = fragments.get(i);
        str.append(fragment.getExplainString(explainLevel));
        if (explainLevel == TExplainLevel.VERBOSE && i + 1 != fragments.size()) {
          str.append("\n");
        }
      }
    }
    return str.toString();
  }

  /**
   * Estimates the per-host memory and CPU requirements for the given plan fragments,
   * and sets the results in request.
   * Optionally excludes the requirements for unpartitioned fragments.
   * TODO: The LOG.warn() messages should eventually become Preconditions checks
   * once resource estimation is more robust.
   */
  public void computeResourceReqs(List<PlanFragment> fragments,
      boolean excludeUnpartitionedFragments,
      TQueryExecRequest request) {
    Preconditions.checkState(!fragments.isEmpty());
    Preconditions.checkNotNull(request);

    // Compute pipelined plan node sets.
    ArrayList<PipelinedPlanNodeSet> planNodeSets =
        PipelinedPlanNodeSet.computePlanNodeSets(fragments.get(0).getPlanRoot());

    // Compute the max of the per-host mem and vcores requirement.
    // Note that the max mem and vcores may come from different plan node sets.
    long maxPerHostMem = Long.MIN_VALUE;
    int maxPerHostVcores = Integer.MIN_VALUE;
    for (PipelinedPlanNodeSet planNodeSet: planNodeSets) {
      if (!planNodeSet.computeResourceEstimates(
          excludeUnpartitionedFragments, ctx_.getQueryOptions())) {
        continue;
      }
      long perHostMem = planNodeSet.getPerHostMem();
      int perHostVcores = planNodeSet.getPerHostVcores();
      if (perHostMem > maxPerHostMem) maxPerHostMem = perHostMem;
      if (perHostVcores > maxPerHostVcores) maxPerHostVcores = perHostVcores;
    }

    // Do not ask for more cores than are in the RuntimeEnv.
    maxPerHostVcores = Math.min(maxPerHostVcores, RuntimeEnv.INSTANCE.getNumCores());

    // Legitimately set costs to zero if there are only unpartitioned fragments
    // and excludeUnpartitionedFragments is true.
    if (maxPerHostMem == Long.MIN_VALUE || maxPerHostVcores == Integer.MIN_VALUE) {
      boolean allUnpartitioned = true;
      for (PlanFragment fragment: fragments) {
        if (fragment.isPartitioned()) {
          allUnpartitioned = false;
          break;
        }
      }
      if (allUnpartitioned && excludeUnpartitionedFragments) {
        maxPerHostMem = 0;
        maxPerHostVcores = 0;
      }
    }

    if (maxPerHostMem < 0 || maxPerHostMem == Long.MIN_VALUE) {
      LOG.warn("Invalid per-host memory requirement: " + maxPerHostMem);
    }
    if (maxPerHostVcores < 0 || maxPerHostVcores == Integer.MIN_VALUE) {
      LOG.warn("Invalid per-host virtual cores requirement: " + maxPerHostVcores);
    }
    request.setPer_host_mem_req(maxPerHostMem);
    request.setPer_host_vcores((short) maxPerHostVcores);

    LOG.debug("Estimated per-host peak memory requirement: " + maxPerHostMem);
    LOG.debug("Estimated per-host virtual cores requirement: " + maxPerHostVcores);
  }
}
