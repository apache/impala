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

package org.apache.impala.calcite.service;

import org.apache.impala.calcite.rel.node.NodeWithExprs;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.DataPartition;
import org.apache.impala.planner.DistributedPlanner;
import org.apache.impala.planner.JoinNode;
import org.apache.impala.planner.NestedLoopJoinNode;
import org.apache.impala.planner.ParallelPlanner;
import org.apache.impala.planner.PlanFragment;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.Planner;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.PlanRootSink;
import org.apache.impala.planner.RuntimeFilterGenerator;
import org.apache.impala.planner.SingleNodePlanner;
import org.apache.impala.planner.SingularRowSrcNode;
import org.apache.impala.planner.SubplanNode;
import org.apache.impala.service.Frontend;
import org.apache.impala.service.FrontendProfile;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPlanExecInfo;
import org.apache.impala.thrift.TPlanFragment;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryExecRequest;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TRuntimeFilterMode;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TStmtType;
import org.apache.impala.util.EventSequence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * ExecRequestCreator. Responsible for taking a PlanNode and the output Expr list
 *  from the top level PlanNode and convert it into a TExecRequest thrift object
 * which is needed by the backend executor code. The input PlanNode tree is
 * an optimized logical tree based on the Calcite rules. This class is also
 * responsible for creating physical node optimizations which are located in
 * the SingleNodePlanner and DistributedPlanner.
 *
 * TODO: This class is very similar to the Frontend.createExecRequest method and
 * contains duplicate code. Accordingly, This class and that method should be
 * refactored to prevent this duplication.
 **/
public class ExecRequestCreator implements CompilerStep {
  protected static final Logger LOG =
      LoggerFactory.getLogger(ExecRequestCreator.class.getName());

  private final CalcitePhysPlanCreator physPlanCreator;
  private final CalciteJniFrontend.QueryContext queryCtx;
  private final CalciteMetadataHandler mdHandler;

  public ExecRequestCreator(CalcitePhysPlanCreator physPlanCreator,
      CalciteJniFrontend.QueryContext queryCtx, CalciteMetadataHandler mdHandler) {
    this.physPlanCreator = physPlanCreator;
    this.queryCtx = queryCtx;
    this.mdHandler = mdHandler;
  }

  /**
   * create() is the main public method responsible for taking the NodeWithExprs
   * object containing the PlanNode and output Expr list and returning the TExecRequest.
   */
  public TExecRequest create(NodeWithExprs nodeWithExprs) throws ImpalaException {
    TExecRequest request = createExecRequest(nodeWithExprs.planNode_,
        queryCtx.getTQueryCtx(), physPlanCreator.getPlannerContext(),
        physPlanCreator.getAnalyzer(), nodeWithExprs.outputExprs_,
        mdHandler.getStmtTableCache().tables.values());

    return request;
  }

  /**
   * Create an exec request for Impala to execute based on the supplied plan. This
   * method is similar to the Frontend.createExecRequest method and needs to be
   * refactored.
   */
  private TExecRequest createExecRequest(PlanNode planNodeRoot, TQueryCtx queryCtx,
      PlannerContext plannerContext, Analyzer analyzer, List<Expr> outputExprs,
      Collection<FeTable> tables) throws ImpalaException {
    List<PlanFragment> fragments =
        createPlans(planNodeRoot, analyzer, plannerContext, outputExprs);
    PlanFragment planFragmentRoot = fragments.get(0);

    TQueryExecRequest queryExecRequest = new TQueryExecRequest();
    TExecRequest result = createExecRequest(queryCtx, planFragmentRoot,
        queryExecRequest);
    queryExecRequest.setHost_list(getHostLocations(tables));
    queryExecRequest.setCores_required(-1);

    // compute resource requirements of the final plan
    Planner.computeResourceReqs(fragments, queryCtx, queryExecRequest,
        plannerContext, true /*isQuery*/);

    // create the plan's exec-info and assign fragment idx
    int idx = 0;
    for (PlanFragment planRoot : fragments) {
      TPlanExecInfo tPlanExecInfo = Frontend.createPlanExecInfo(planRoot, queryCtx);
      queryExecRequest.addToPlan_exec_info(tPlanExecInfo);
      for (TPlanFragment fragment : tPlanExecInfo.fragments) {
        fragment.setIdx(idx++);
      }
    }

    // create EXPLAIN output after setting everything else
    queryExecRequest.setQuery_ctx(queryCtx); // needed by getExplainString()

    List<PlanFragment> allFragments = planFragmentRoot.getNodesPreOrder();
    // to mimic the original planner behavior, use EXTENDED mode explain except for
    // EXPLAIN statements.
    // TODO: support explain plans
    // TExplainLevel explainLevel =
    //     isExplain ? plannerContext.getQueryOptions().getExplain_level() :
    //     TExplainLevel.EXTENDED;
    TExplainLevel explainLevel = TExplainLevel.EXTENDED;
    // if (isExplain) {
    //   result.setStmt_type(TStmtType.EXPLAIN);
    // }
    String explainString = getExplainString(allFragments, explainLevel, plannerContext);
    queryExecRequest.setQuery_plan(explainString);


    queryCtx.setDesc_tbl_serialized(
        plannerContext.getRootAnalyzer().getDescTbl().toSerializedThrift());

    plannerContext.getTimeline().markEvent("Execution request created");
    EventSequence eventSequence = plannerContext.getTimeline();
    result.setTimeline(eventSequence.toThrift());

    TRuntimeProfileNode calciteProfile =
        this.queryCtx.getFrontend().createTRuntimeProfileNode(Frontend.PLANNER_PROFILE);
    this.queryCtx.getFrontend().addPlannerToProfile("CalcitePlanner");
    result.setProfile(FrontendProfile.getCurrent().emitAsThrift());
    result.setProfile_children(FrontendProfile.getCurrent().emitChildrenAsThrift());
    return result;
  }

  List<PlanFragment> createPlans(PlanNode planNodeRoot, Analyzer analyzer,
      PlannerContext ctx, List<Expr> outputExprs) throws ImpalaException {
      // Create the values transfer graph in the Analyzer. Note that Calcite plans
      // don't register equijoin predicates in the Analyzer's GlobalState since
      // Calcite should have already done the predicate inferencing analysis.
      // Hence, the GlobalState's registeredValueTransfers will be empty. It is
      // still necessary to instantiate the graph because otherwise
      // RuntimeFilterGenerator tries to de-reference it and encounters NPE.
      analyzer.computeValueTransferGraph();
      Planner.checkForSmallQueryOptimization(planNodeRoot, ctx);

      // Although the Calcite plan creates the relative order among different
      // joins, currently it does not swap left and right inputs if the right
      // input has higher estimated cardinality. Do this through Impala's method
      // since we are using Impala's cardinality estimates in the physical planning.
      invertJoins(planNodeRoot, ctx.isSingleNodeExec(), ctx.getRootAnalyzer());
      SingleNodePlanner.validatePlan(ctx, planNodeRoot);

      List<PlanFragment> fragments =
          createPlanFragments(planNodeRoot, ctx, analyzer, outputExprs);
      PlanFragment planFragmentRoot = fragments.get(0);
      List<PlanFragment> rootFragments;
      if (Planner.useParallelPlan(ctx)) {
        ParallelPlanner parallelPlanner = new ParallelPlanner(ctx);
        // The rootFragmentList contains the 'root' fragments of each of
        // the parallel plans
        rootFragments = parallelPlanner.createPlans(planFragmentRoot);
        ctx.getTimeline().markEvent("Parallel plans created");
      } else {
        rootFragments = new ArrayList(Arrays.asList(planFragmentRoot));
      }
      return rootFragments;
  }

  /**
   * Create one or more plan fragments corresponding to the supplied single node physical
   * plan. This function calls Impala's DistributedPlanner to create the plan fragments
   * and does some post-processing.  It is loosely based on Impala's Planner.createPlan()
   * function.
   */
  private List<PlanFragment> createPlanFragments(PlanNode planNodeRoot,
      PlannerContext ctx, Analyzer analyzer,
      List<Expr> outputExprs) throws ImpalaException {

    DistributedPlanner distributedPlanner = new DistributedPlanner(ctx);
    List<PlanFragment> fragments;

    if (ctx.isSingleNodeExec()) {
      // create one fragment containing the entire single-node plan tree
      fragments = Lists.newArrayList(new PlanFragment(
          ctx.getNextFragmentId(), planNodeRoot, DataPartition.UNPARTITIONED));
    } else {
      fragments = new ArrayList<>();
      // Create distributed plan. For insert/CTAS without limit,
      // isPartitioned should be true.
      // TODO: only query statements are currently supported
      // final boolean isPartitioned = stmtType_ ==
      //     TStmtType.DML && !planNodeRoot.hasLimit();
      boolean isPartitioned = false;
      distributedPlanner.createPlanFragments(planNodeRoot, isPartitioned, fragments);
    }

    PlanFragment rootFragment = fragments.get(fragments.size() - 1);
    // Create runtime filters.
    if (ctx.getQueryOptions().getRuntime_filter_mode() != TRuntimeFilterMode.OFF) {
      RuntimeFilterGenerator.generateRuntimeFilters(ctx, rootFragment.getPlanRoot());
      ctx.getTimeline().markEvent("Runtime filters computed");
    }

    rootFragment.verifyTree();

    List<Expr> resultExprs = outputExprs;
    rootFragment.setSink(new PlanRootSink(resultExprs));

    Planner.checkForDisableCodegen(rootFragment.getPlanRoot(), ctx);
    // finalize exchanges: this ensures that for hash partitioned joins, the partitioning
    // keys on both sides of the join have compatible data types
    for (PlanFragment fragment: fragments) {
      fragment.finalizeExchanges(analyzer);
    }

    Collections.reverse(fragments);
    ctx.getTimeline().markEvent("Distributed plan created");
    return fragments;
  }

  private TExecRequest createExecRequest(TQueryCtx queryCtx,
      PlanFragment planFragmentRoot, TQueryExecRequest queryExecRequest) {
    TExecRequest result = new TExecRequest();
    // NOTE: the below 4 are mandatory fields
    result.setQuery_options(queryCtx.getClient_request().getQuery_options());

    // TODO: Need to populate these 3 fields
    result.setAccess_events(new ArrayList<>());
    result.setAnalysis_warnings(new ArrayList<>());
    result.setUser_has_profile_access(true);

    result.setQuery_exec_request(queryExecRequest);

    // TODO: only query currently supported
    //    result.setStmt_type(stmtType_);
    //    result.getQuery_exec_request().setStmt_type(stmtType_);
    result.setStmt_type(TStmtType.QUERY);
    result.getQuery_exec_request().setStmt_type(TStmtType.QUERY);

    // fill in the metadata using the root fragment's PlanRootSink
    Preconditions.checkState(planFragmentRoot.hasSink());
    List<Expr> outputExprs = new ArrayList<>();

    planFragmentRoot.getSink().collectExprs(outputExprs);
    result.setResult_set_metadata(createQueryResultSetMetadata(outputExprs));

    return result;
  }

  // TODO: Refactor and share Impala's getExplainString()
  private String getExplainString(List<PlanFragment> fragments,
      TExplainLevel explainLevel, PlannerContext ctx) {
    if (explainLevel.ordinal() < TExplainLevel.VERBOSE.ordinal()) {
      // Print the non-fragmented parallel plan.
      return fragments.get(0).getExplainString(ctx.getQueryOptions(), explainLevel);
    }

    StringBuffer sb = new StringBuffer();
    // Print the fragmented parallel plan.
    for (int i = 0; i < fragments.size(); ++i) {
      PlanFragment fragment = fragments.get(i);
      sb.append(fragment.getExplainString(ctx.getQueryOptions(), explainLevel));
      if (i < fragments.size() - 1) {
        sb.append("\n");
      }
    }
    return sb.toString();
  }

  private TResultSetMetadata createQueryResultSetMetadata(List<Expr> outputExprs) {
    TResultSetMetadata metadata = new TResultSetMetadata();
    int colCnt = outputExprs.size();
    for (int i = 0; i < colCnt; ++i) {
      TColumn colDesc = new TColumn(outputExprs.get(i).toString(),
          outputExprs.get(i).getType().toThrift());
      metadata.addToColumns(colDesc);
    }
    return metadata;
  }

  private List<TNetworkAddress> getHostLocations(Collection<FeTable> tables) {
    Set<TNetworkAddress> hostLocations = new HashSet<>();
    for (FeTable table : tables) {
      if (table instanceof HdfsTable) {
        hostLocations.addAll(((HdfsTable) table).getHostIndex().getList());
      }
    }
    return new ArrayList<>(hostLocations);
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
   * Return true if any join in the plan rooted at 'root' was inverted.
   *
   * TODO: This should be replaced once we conclude the changes contained in this method
   *       are safe to be pushed to Planner.invertJoins, i.e., they do not cause any
   *       performance regressions with Impala FE. A couple of differences in this version
   *       of invertJoins include:
   *       - The computeStats is done here. The Calcite planner will eventually have
   *       the computeStats built in during optimization time, but the join stats here
   *       are specific for Impala. So we need an explicit computeStats call only for
   *       Impala here.
   *       - We only need to do the computeStats when the plan changes via inversion, so
   *       the method returns a boolean whenever the inversion happens.
   *       - A Jira has been filed for this: IMPALA-12958. Probably the best fix for this
   *       is to put the inversion inside a Calcite rule.
   *
   **/
  private static boolean invertJoins(PlanNode root, boolean isLocalPlan,
      Analyzer analyzer) {
    boolean inverted = false;
    if (root instanceof SubplanNode) {
      inverted |= invertJoins(root.getChild(0), isLocalPlan, analyzer);
      inverted |= invertJoins(root.getChild(1), true, analyzer);
    } else {
      for (PlanNode child: root.getChildren()) {
        inverted |= invertJoins(child, isLocalPlan, analyzer);
      }
    }

    if (root instanceof JoinNode) {
      JoinNode joinNode = (JoinNode) root;
      JoinOperator joinOp = joinNode.getJoinOp();

      if (!joinNode.isInvertible(isLocalPlan)) {
        if (inverted) {
          // Re-compute tuple ids since their order must correspond to the order
          // of children.
          root.computeTupleIds();
          // Re-compute stats since PK-FK inference and cardinality may have changed after
          // inversion.
          root.computeStats(analyzer);
        }
        return inverted;
      }

      if (joinNode.getChild(0) instanceof SingularRowSrcNode) {
        // Always place a singular row src on the build side because it
        // only produces a single row.
        joinNode.invertJoin();
        inverted = true;
      } else if (!isLocalPlan && joinNode instanceof NestedLoopJoinNode &&
          (joinOp.isRightSemiJoin() || joinOp.isRightOuterJoin())) {
        // The current join is a distributed non-equi right outer or semi join
        // which has no backend support. Invert the join to make it executable.
        joinNode.invertJoin();
        inverted = true;
      } else if (Planner.isInvertedJoinCheaper(joinNode, isLocalPlan)) {
        joinNode.invertJoin();
        inverted = true;
      }
      // Re-compute the numNodes and numInstances based on the new input order
      joinNode.recomputeNodes();
    }

    if (inverted) {
      // Re-compute tuple ids because the backend assumes that their order corresponds to
      // the order of children.
      root.computeTupleIds();
      // Re-compute stats since PK-FK inference and cardinality may have changed after
      // inversion.
      root.computeStats(analyzer);
    }
    return inverted;
  }

  @Override
  public void logDebug(Object resultObject) {
    if (!(resultObject instanceof TExecRequest)) {
      LOG.debug("Finished create exec request step, but unknown result: " + resultObject);
    }
    LOG.debug("Exec request: " + resultObject);
  }
}
