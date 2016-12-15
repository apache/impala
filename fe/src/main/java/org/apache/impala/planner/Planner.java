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
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.ColumnLineageGraph;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.InsertStmt;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.analysis.QueryStmt;
import org.apache.impala.analysis.SortInfo;
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
import org.apache.impala.thrift.TRuntimeFilterMode;
import org.apache.impala.thrift.TTableName;
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

  private final PlannerContext ctx_;

  public Planner(AnalysisContext.AnalysisResult analysisResult, TQueryCtx queryCtx) {
    ctx_ = new PlannerContext(analysisResult, queryCtx);
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
      ctx_.getQueryCtx().disable_codegen_hint = true;
      if (maxRowsProcessed < ctx_.getQueryOptions().batch_size ||
          maxRowsProcessed < 1024 && ctx_.getQueryOptions().batch_size == 0) {
        // Only one scanner thread for small queries
        ctx_.getQueryOptions().setNum_scanner_threads(1);
      }
      // disable runtime filters
      ctx_.getQueryOptions().setRuntime_filter_mode(TRuntimeFilterMode.OFF);
    }

    // Join rewrites.
    invertJoins(singleNodePlan, ctx_.isSingleNodeExec());
    singleNodePlan = useNljForSingularRowBuilds(singleNodePlan, ctx_.getRootAnalyzer());

    // create runtime filters
    if (ctx_.getQueryOptions().getRuntime_filter_mode() != TRuntimeFilterMode.OFF) {
      // Always compute filters, even if the BE won't always use all of them.
      RuntimeFilterGenerator.generateRuntimeFilters(ctx_.getRootAnalyzer(),
          singleNodePlan, ctx_.getQueryOptions().getMax_num_runtime_filters());
      ctx_.getRootAnalyzer().getTimeline().markEvent(
          "Runtime filters computed");
    }

    singleNodePlanner.validatePlan(singleNodePlan);

    if (ctx_.isSingleNodeExec()) {
      // create one fragment containing the entire single-node plan tree
      fragments = Lists.newArrayList(new PlanFragment(
          ctx_.getNextFragmentId(), singleNodePlan, DataPartition.UNPARTITIONED));
    } else {
      // create distributed plan
      fragments = distributedPlanner.createPlanFragments(singleNodePlan);
    }

    PlanFragment rootFragment = fragments.get(fragments.size() - 1);
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
      createClusteringSort(insertStmt, rootFragment, ctx_.getRootAnalyzer());
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

    if (LOG.isTraceEnabled()) {
      LOG.trace("desctbl: " + ctx_.getRootAnalyzer().getDescTbl().debugString());
      LOG.trace("resultexprs: " + Expr.debugString(rootFragment.getOutputExprs()));
      LOG.trace("finalize plan fragments");
    }
    for (PlanFragment fragment: fragments) {
      fragment.finalize(ctx_.getRootAnalyzer());
    }

    Collections.reverse(fragments);
    ctx_.getRootAnalyzer().getTimeline().markEvent("Distributed plan created");

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
      ctx_.getRootAnalyzer().getTimeline().markEvent("Lineage info computed");
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
    ctx_.getRootAnalyzer().getTimeline().markEvent("Parallel plans created");
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
        if (i < fragments.size() - 1) str.append("\n");
      }
    }
    return str.toString();
  }

  /**
   * Returns true if the fragments are for a trivial, coordinator-only query:
   * Case 1: Only an EmptySetNode, e.g. query has a limit 0.
   * Case 2: Query has only constant exprs.
   */
  private static boolean isTrivialCoordOnlyPlan(List<PlanFragment> fragments) {
    Preconditions.checkNotNull(fragments);
    Preconditions.checkState(!fragments.isEmpty());
    if (fragments.size() > 1) return false;
    PlanNode root = fragments.get(0).getPlanRoot();
    if (root instanceof EmptySetNode) return true;
    if (root instanceof UnionNode && ((UnionNode) root).isConstantUnion()) return true;
    return false;
  }

  /**
   * Estimates the per-host memory and CPU requirements for the given plan fragments,
   * and sets the results in request.
   * Optionally excludes the requirements for unpartitioned fragments.
   * TODO: The LOG.warn() messages should eventually become Preconditions checks
   * once resource estimation is more robust.
   * TODO: Revisit and possibly remove during MT work, particularly references to vcores.
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

    // Special case for some trivial coordinator-only queries (IMPALA-3053, IMPALA-1092).
    if (isTrivialCoordOnlyPlan(fragments)) {
      maxPerHostMem = 1024;
      maxPerHostVcores = 1;
    }

    // Set costs to zero if there are only unpartitioned fragments and
    // excludeUnpartitionedFragments is true.
    // TODO: handle this case with a better indication for unknown, e.g. -1 or not set.
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

    if (LOG.isTraceEnabled()) {
      LOG.trace("Estimated per-host peak memory requirement: " + maxPerHostMem);
      LOG.trace("Estimated per-host virtual cores requirement: " + maxPerHostVcores);
    }
  }

  /**
   * Traverses the plan tree rooted at 'root' and inverts outer and semi joins
   * in the following situations:
   * 1. If the left-hand side is a SingularRowSrcNode then we invert the join because
   *    then the build side is guaranteed to have only a single row.
   * 2. There is no backend support for distributed non-equi right outer/semi joins,
   *    so we invert them (any distributed left semi/outer join is ok).
   * 3. Invert semi/outer joins if the right-hand size is estimated to have a higher
   *    cardinality*avgSerializedSize. Do not invert if relevant stats are missing.
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

      // 1. No inversion allowed due to straight join.
      // 2. The null-aware left anti-join operator is not considered for inversion.
      //    There is no backend support for a null-aware right anti-join because
      //    we cannot execute it efficiently.
      if (joinNode.isStraightJoin() || joinOp.isNullAwareLeftAntiJoin()) {
        // Re-compute tuple ids since their order must correspond to the order of children.
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
      } else {
        // Invert the join if doing so reduces the size of the materialized rhs
        // (may also reduce network costs depending on the join strategy).
        // Only consider this optimization if both the lhs/rhs cardinalities are known.
        long lhsCard = joinNode.getChild(0).getCardinality();
        long rhsCard = joinNode.getChild(1).getCardinality();
        float lhsAvgRowSize = joinNode.getChild(0).getAvgRowSize();
        float rhsAvgRowSize = joinNode.getChild(1).getAvgRowSize();
        if (lhsCard != -1 && rhsCard != -1 &&
            lhsCard * lhsAvgRowSize < rhsCard * rhsAvgRowSize) {
          joinNode.invertJoin();
        }
      }
    }

    // Re-compute tuple ids because the backend assumes that their order corresponds to
    // the order of children.
    root.computeTupleIds();
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

  /**
   * Insert a sort node on top of the plan, depending on the clustered/noclustered plan
   * hint. This will sort the data produced by 'inputFragment' by the clustering columns
   * (key columns for Kudu tables), so that partitions can be written sequentially in the
   * table sink.
   */
  public void createClusteringSort(InsertStmt insertStmt, PlanFragment inputFragment,
       Analyzer analyzer) throws ImpalaException {
    if (!insertStmt.hasClusteredHint()) return;

    List<Expr> orderingExprs;
    if (insertStmt.getTargetTable() instanceof KuduTable) {
      orderingExprs = Lists.newArrayList(insertStmt.getPrimaryKeyExprs());
    } else {
      orderingExprs = Lists.newArrayList(insertStmt.getPartitionKeyExprs());
    }
    // Ignore constants for the sake of clustering.
    Expr.removeConstants(orderingExprs);

    if (orderingExprs.isEmpty()) return;

    // Build sortinfo to sort by the ordering exprs.
    List<Boolean> isAscOrder = Collections.nCopies(orderingExprs.size(), false);
    List<Boolean> nullsFirstParams = Collections.nCopies(orderingExprs.size(), false);
    SortInfo sortInfo = new SortInfo(orderingExprs, isAscOrder, nullsFirstParams);

    ExprSubstitutionMap smap = sortInfo.createSortTupleInfo(
        insertStmt.getResultExprs(), analyzer);
    sortInfo.getSortTupleDescriptor().materializeSlots();

    insertStmt.substituteResultExprs(smap, analyzer);

    SortNode sortNode = new SortNode(ctx_.getNextNodeId(), inputFragment.getPlanRoot(),
        sortInfo, false, 0);
    sortNode.init(analyzer);

    inputFragment.setPlanRoot(sortNode);
  }
}
