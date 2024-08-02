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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.impala.analysis.AggregateInfo;
import org.apache.impala.analysis.AnalyticInfo;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BaseTableRef;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.analysis.CollectionTableRef;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprId;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.IcebergMetadataTableRef;
import org.apache.impala.analysis.InlineViewRef;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.MultiAggregateInfo.AggPhase;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.Path;
import org.apache.impala.analysis.Path.PathType;
import org.apache.impala.analysis.QueryStmt;
import org.apache.impala.analysis.SelectStmt;
import org.apache.impala.analysis.SetOperationStmt.SetOperand;
import org.apache.impala.analysis.SingularRowSrcTableRef;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.SortInfo;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.analysis.TupleIsNullPredicate;
import org.apache.impala.analysis.UnionStmt;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.FeDataSourceTable;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeSystemTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.iceberg.IcebergMetadataTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.NotImplementedException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.AnalyticEvalNode.LimitPushdownInfo;
import org.apache.impala.planner.JoinNode.DistributionMode;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.AcidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Constructs a non-executable single-node plan from an analyzed parse tree.
 * The single-node plan does not contain data exchanges or data-reduction optimizations
 * such as local aggregations that are important for distributed execution.
 * The single-node plan needs to be wrapped in a plan fragment for it to be executable.
 */
public class SingleNodePlanner {
  // Controls whether a distinct aggregation should be inserted before a join input.
  // If the size of the distinct values after aggregation is less than or equal to
  // the original input size multiplied by this threshold, the distinct agg should be
  // inserted.
  private static final double JOIN_DISTINCT_THRESHOLD = 0.25;

  private final static Logger LOG = LoggerFactory.getLogger(SingleNodePlanner.class);

  private final PlannerContext ctx_;

  // Set to true if single node planning added new value transfers to the
  // value transfer graph in 'analyzer'.
  private boolean valueTransferGraphNeedsUpdate_ = false;

  public SingleNodePlanner(PlannerContext ctx) {
    ctx_ = ctx;
  }

  /**
   * Generates and returns the root of the single-node plan for the analyzed parse tree
   * in the planner context. The planning process recursively walks the parse tree and
   * performs the following actions.
   * In the top-down phase over query statements:
   * - Materialize the slots required for evaluating expressions of that statement.
   * - Migrate conjuncts from parent blocks into inline views and set operands.
   * In the bottom-up phase generate the plan tree for every query statement:
   * - Generate the plan for the FROM-clause of a select statement: The plan trees of
   *   absolute and uncorrelated table refs are connected via JoinNodes. The relative
   *   and correlated table refs are associated with one or more SubplanNodes.
   * - A new SubplanNode is placed on top of an existing plan node whenever the tuples
   *   materialized by that plan node enable evaluation of one or more relative or
   *   correlated table refs, i.e., SubplanNodes are placed at the lowest possible point
   *   in the plan, often right after a ScanNode materializing the (single) parent tuple.
   * - The right-hand side of each SubplanNode is a plan tree generated by joining a
   *   SingularRowSrcTableRef with those applicable relative and correlated refs.
   *   A SingularRowSrcTableRef represents the current row being processed by the
   *   SubplanNode from its input (first child).
   * - Connecting table ref plans via JoinNodes is done in a cost-based fashion
   *   (join-order optimization). All materialized slots, including those of tuples
   *   materialized inside a SubplanNode, must be known for an accurate estimate of row
   *   sizes needed for cost-based join ordering.
   * - The remaining aggregate/analytic/orderby portions of a select statement are added
   *   on top of the FROM-clause plan.
   * - Whenever a new node is added to the plan tree, assign conjuncts that can be
   *   evaluated at that node and compute the stats of that node (cardinality, etc.).
   * - Apply combined expression substitution map of child plan nodes; if a plan node
   *   re-maps its input, set a substitution map to be applied by parents.
   */
  public PlanNode createSingleNodePlan() throws ImpalaException {
    QueryStmt queryStmt = ctx_.getQueryStmt();
    // Use the stmt's analyzer which is not necessarily the root analyzer
    // to detect empty result sets.
    Analyzer analyzer = queryStmt.getAnalyzer();
    analyzer.computeValueTransferGraph();
    ctx_.getTimeline().markEvent("Value transfer graph computed");

    // Mark slots referenced by output exprs as materialized, prior to generating the
    // plan tree.
    // We need to mark the result exprs of the topmost select block as materialized, so
    // that PlanNode.init() can compute the final mem layout of materialized tuples
    // (the byte size of tuples is needed for cost computations).
    // TODO: instead of materializing everything produced by the plan root, derive
    // referenced slots from destination fragment and add a materialization node
    // if not all output is needed by destination fragment
    // TODO 2: should the materialization decision be cost-based?
    if (queryStmt.getBaseTblResultExprs() != null) {
      analyzer.materializeSlots(queryStmt.getBaseTblResultExprs());
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("desctbl: " + analyzer.getDescTbl().debugString());
    }
    PlanNode singleNodePlan = createQueryPlan(queryStmt, analyzer,
        ctx_.getQueryOptions().isDisable_outermost_topn());
    Preconditions.checkNotNull(singleNodePlan);
    // Recompute the graph since we may have added new equivalences.
    if (valueTransferGraphNeedsUpdate_) {
      ctx_.getTimeline().markEvent("Recomputing value transfer graph");
      analyzer.computeValueTransferGraph();
      ctx_.getTimeline().markEvent("Value transfer graph computed");
      valueTransferGraphNeedsUpdate_ = false;
    }

    return singleNodePlan;
  }

  /**
   * Checks that the given single-node plan is executable:
   * - It may not contain right or full outer joins with no equi-join conjuncts that
   *   are not inside the right child of a SubplanNode.
   * Throws a NotImplementedException if plan validation fails.
   */
  public static void validatePlan(PlannerContext planCtx,
    PlanNode planNode) throws NotImplementedException {
    // Any join can run in a single-node plan.
    if (planCtx.isSingleNodeExec()) return;

    if (planNode instanceof NestedLoopJoinNode) {
      JoinNode joinNode = (JoinNode) planNode;
      JoinOperator joinOp = joinNode.getJoinOp();
      if ((joinOp.isRightSemiJoin() || joinOp.isFullOuterJoin()
           || joinOp == JoinOperator.RIGHT_OUTER_JOIN)
          && joinNode.getEqJoinConjuncts().isEmpty()) {
        throw new NotImplementedException(String.format("Error generating a valid " +
            "execution plan for this query. A %s type with no equi-join " +
            "predicates can only be executed with a single node plan.",
            joinOp.toString()));
      }
    }

    if (planNode instanceof SubplanNode) {
      // Right and full outer joins with no equi-join conjuncts are ok in the right
      // child of a SubplanNode.
      validatePlan(planCtx, planNode.getChild(0));
    } else {
      for (PlanNode child: planNode.getChildren()) {
        validatePlan(planCtx, child);
      }
    }
  }

  /**
   * Creates an EmptyNode that 'materializes' the tuples of the given stmt.
   * Marks all collection-typed slots referenced in stmt as non-materialized because
   * they are never unnested, and therefore the corresponding parent scan should not
   * materialize them.
   */
  private PlanNode createEmptyNode(QueryStmt stmt, Analyzer analyzer) {
    List<TupleId> tupleIds = new ArrayList<>();
    stmt.getMaterializedTupleIds(tupleIds);
    if (tupleIds.isEmpty()) {
      // Constant selects do not have materialized tuples at this stage.
      Preconditions.checkState(stmt instanceof SelectStmt,
          "Only constant selects should have no materialized tuples");
      SelectStmt selectStmt = (SelectStmt)stmt;
      Preconditions.checkState(selectStmt.getTableRefs().isEmpty());
      tupleIds.add(createResultTupleDescriptor(selectStmt, "empty", analyzer).getId());
    }
    unmarkCollectionSlots(stmt);
    EmptySetNode node = new EmptySetNode(ctx_.getNextNodeId(), tupleIds);
    node.init(analyzer);
    // Set the output smap to resolve exprs referencing inline views within stmt.
    // Not needed for a SetOperationStmt because it materializes its input operands.
    if (stmt instanceof SelectStmt) {
      node.setOutputSmap(((SelectStmt) stmt).getBaseTblSmap());
    }
    return node;
  }

  /**
   * Mark all collection-typed slots in stmt as non-materialized.
   */
  private void unmarkCollectionSlots(QueryStmt stmt) {
    List<TableRef> tblRefs = new ArrayList<>();
    stmt.collectFromClauseTableRefs(tblRefs);
    for (TableRef ref: tblRefs) {
      if (!ref.isRelative()) continue;
      Preconditions.checkState(ref instanceof CollectionTableRef);
      CollectionTableRef collTblRef = (CollectionTableRef) ref;
      Expr collExpr = collTblRef.getCollectionExpr();
      Preconditions.checkState(collExpr instanceof SlotRef);
      SlotRef collSlotRef = (SlotRef) collExpr;
      collSlotRef.getDesc().setIsMaterialized(false);
      // Re-compute the mem layout if necessary. The tuple may not have a mem layout if
      // no plan has been generated for the TableRef (e.g. due to limit 0 or similar).
      collSlotRef.getDesc().getParent().recomputeMemLayout();
    }
  }

  /**
   * Create plan tree for single-node execution. Generates PlanNodes for the
   * Select/Project/Join/Union [All]/Group by/Having/Order by clauses of the query stmt.
   */
  private PlanNode createQueryPlan(QueryStmt stmt, Analyzer analyzer, boolean disableTopN)
      throws ImpalaException {
    if (analyzer.hasEmptyResultSet()) return createEmptyNode(stmt, analyzer);

    PlanNode root;
    if (stmt instanceof SelectStmt) {
      SelectStmt selectStmt = (SelectStmt) stmt;
      root = createSelectPlan(selectStmt, analyzer);

      // insert possible AnalyticEvalNode before SortNode
      if (((SelectStmt) stmt).getAnalyticInfo() != null) {
        AnalyticInfo analyticInfo = selectStmt.getAnalyticInfo();
        AnalyticPlanner analyticPlanner =
            new AnalyticPlanner(analyticInfo, analyzer, ctx_);
        MultiAggregateInfo multiAggInfo = selectStmt.getMultiAggInfo();
        List<Expr> groupingExprs;
        if (multiAggInfo != null) {
          groupingExprs = multiAggInfo.getSubstGroupingExprs();
          Preconditions.checkState(groupingExprs != null);
        } else {
          groupingExprs = Collections.emptyList();
        }

        List<Expr> inputPartitionExprs = new ArrayList<>();
        root = analyticPlanner.createSingleNodePlan(
            root, groupingExprs, inputPartitionExprs);
        if (multiAggInfo != null && !inputPartitionExprs.isEmpty()
            && multiAggInfo.getMaterializedAggClasses().size() == 1) {
          // analytic computation will benefit from a partition on inputPartitionExprs
          multiAggInfo.getMaterializedAggClass(0).setPartitionExprs(inputPartitionExprs);
        }
      }
    } else {
      Preconditions.checkState(stmt instanceof UnionStmt);
      root = createUnionPlan((UnionStmt) stmt, analyzer);
    }

    // Avoid adding a sort node if the sort tuple has no materialized slots.
    boolean sortHasMaterializedSlots = false;
    if (stmt.evaluateOrderBy()) {
      for (SlotDescriptor sortSlotDesc:
        stmt.getSortInfo().getSortTupleDescriptor().getSlots()) {
        if (sortSlotDesc.isMaterialized()) {
          sortHasMaterializedSlots = true;
          break;
        }
      }
    }

    if (stmt.evaluateOrderBy() && sortHasMaterializedSlots) {
      root = createSortNode(ctx_, analyzer, root, stmt.getSortInfo(), stmt.getLimit(),
          stmt.getOffset(), stmt.hasLimit(), disableTopN);
    } else {
      root.setLimit(stmt.getLimit());
      root.computeStats(analyzer);
      if (root.hasLimit()) {
        checkAndApplyLimitPushdown(root, null, root.getLimit(), analyzer, ctx_);
      }
    }

    return root;
  }

  /**
   * Creates and initializes either a SortNode or a TopNNode depending on various
   * heuristics and configuration parameters.
   */
  public static SortNode createSortNode(PlannerContext planCtx, Analyzer analyzer,
    PlanNode root, SortInfo sortInfo, long limit, long offset, boolean hasLimit,
    boolean disableTopN) throws ImpalaException {
    SortNode sortNode;

    if (hasLimit && offset == 0) {
      checkAndApplyLimitPushdown(root, sortInfo, limit, analyzer, planCtx);
    }

    if (hasLimit && !disableTopN) {
      sortNode = SortNode.createTopNSortNode(planCtx.getQueryOptions(),
          planCtx.getNextNodeId(), root, sortInfo, offset, limit, false);
    } else {
      sortNode =
          SortNode.createTotalSortNode(planCtx.getNextNodeId(), root, sortInfo, offset);
      sortNode.setLimit(limit);
    }
    Preconditions.checkState(sortNode.hasValidStats());
    sortNode.init(analyzer);

    return sortNode;
  }

  /**
   * For certain qualifying conditions, we can push a limit from the top level
   * sort down to the sort associated with an AnalyticEval node.
   */
  private static void checkAndApplyLimitPushdown(PlanNode root, SortInfo sortInfo,
    long limit, Analyzer analyzer, PlannerContext planCtx) {
    LimitPushdownInfo pushdownLimit = null;
    AnalyticEvalNode analyticNode = null;
    List<PlanNode> intermediateNodes = new ArrayList<>();
    SortNode analyticNodeSort = null;
    PlanNode descendant = findDescendantAnalyticNode(root, intermediateNodes);
    if (descendant != null && intermediateNodes.size() <= 1) {
      Preconditions.checkArgument(descendant instanceof AnalyticEvalNode);
      analyticNode = (AnalyticEvalNode) descendant;
      if (!(analyticNode.getChild(0) instanceof SortNode)) {
        // if the over() clause is empty, there won't be a child SortNode
        // so limit pushdown is not applicable
        return;
      }
      analyticNodeSort = (SortNode) analyticNode.getChild(0);
      // Don't attempt conversion if there is already a limit or offset.
      if (analyticNodeSort.hasLimit() || analyticNodeSort.hasOffset()) {
        return;
      }
      int numNodes = intermediateNodes.size();
      if (numNodes > 1 ||
              (numNodes == 1 && !(intermediateNodes.get(0) instanceof SelectNode))) {
        pushdownLimit = null;
      } else if (numNodes == 0) {
        pushdownLimit = analyticNode.checkForLimitPushdown(sortInfo,
            root.getOutputSmap(), null, limit, analyticNodeSort,
            planCtx.getRootAnalyzer());
      } else {
        SelectNode selectNode = (SelectNode) intermediateNodes.get(0);
        pushdownLimit = analyticNode.checkForLimitPushdown(sortInfo,
            root.getOutputSmap(), selectNode, limit, analyticNodeSort,
            planCtx.getRootAnalyzer());
      }
    }

    if (pushdownLimit != null) {
      Preconditions.checkArgument(analyticNode != null);
      Preconditions.checkArgument(analyticNode.getChild(0) instanceof SortNode);
      analyticNodeSort.tryConvertToTopN(limit + pushdownLimit.additionalLimit,
           analyzer, pushdownLimit.includeTies);
      // after the limit is pushed down, update stats for the analytic eval node
      // and intermediate nodes
      analyticNode.computeStats(analyzer);
      for (PlanNode n : intermediateNodes) {
        n.computeStats(analyzer);
      }
    }
  }

  /**
   * Starting from the supplied root PlanNode, traverse the descendants
   * to find the first AnalyticEvalNode.  If a blocking node such as
   * Join, Aggregate, Sort is encountered, return null. The
   * 'intermediateNodes' is populated with the nodes encountered during
   * traversal.
   */
  private static PlanNode findDescendantAnalyticNode(PlanNode root,
    List<PlanNode> intermediateNodes) {
    if (root == null || root instanceof AnalyticEvalNode) {
      return root;
    }
    // If we encounter a blocking operator (sort, aggregate, join), or a Subplan,
    // there's no need to go further. Also, we bail early if we encounter multi-input
    // operator such as union-all.  In the future, we could potentially extend the
    // limit pushdown to both sides of a union-all
    if (root instanceof SortNode || root instanceof AggregationNode ||
          root instanceof JoinNode || root instanceof SubplanNode ||
          root.getChildren().size() > 1) {
      return null;
    }
    intermediateNodes.add(root);
    return findDescendantAnalyticNode(root.getChild(0), intermediateNodes);
  }

  /**
   * If there are unassigned conjuncts that are bound by tupleIds or if there are slot
   * equivalences for tupleIds that have not yet been enforced, add them to the plan
   * tree, returning either the original root or a new root.
   */
  private PlanNode addUnassignedConjuncts(
      Analyzer analyzer, List<TupleId> tupleIds, PlanNode root) {
    // No point in adding SelectNode on top of an EmptyNode.
    if (root instanceof EmptySetNode) return root;
    Preconditions.checkNotNull(root);
    // Gather unassigned conjuncts and generate predicates to enforce
    // slot equivalences for each tuple id.
    List<Expr> conjuncts = analyzer.getUnassignedConjuncts(root);
    return root.addConjunctsToNode(ctx_, analyzer, tupleIds, conjuncts);
  }

  /**
   * Return the cheapest plan that materializes the joins of all TableRefs in
   * parentRefPlans and the subplans of all applicable TableRefs in subplanRefs.
   * Assumes that parentRefPlans are in the order as they originally appeared in
   * the query.
   * For this plan:
   * - the plan is executable, ie, all non-cross joins have equi-join predicates
   * - the leftmost scan is over the largest of the inputs for which we can still
   *   construct an executable plan
   * - from bottom to top, all rhs's are in increasing order of selectivity (percentage
   *   of surviving rows)
   * - outer/cross/semi joins: rhs serialized size is < lhs serialized size;
   *   enforced via join inversion, if necessary
   * - SubplanNodes are placed as low as possible in the plan tree - as soon as the
   *   required tuple ids of one or more TableRefs in subplanRefs are materialized
   * Returns null if we can't create an executable plan.
   */
  private PlanNode createCheapestJoinPlan(Analyzer analyzer,
      List<Pair<TableRef, PlanNode>> parentRefPlans, List<SubplanRef> subplanRefs)
      throws ImpalaException {
    LOG.trace("createCheapestJoinPlan");
    if (parentRefPlans.size() == 1) return parentRefPlans.get(0).second;

    // collect eligible candidates for the leftmost input; list contains
    // (plan, materialized size)
    List<Pair<TableRef, Long>> candidates = new ArrayList<>();
    for (Pair<TableRef, PlanNode> entry: parentRefPlans) {
      TableRef ref = entry.first;
      JoinOperator joinOp = ref.getJoinOp();

      // Avoid reordering outer/semi joins which is generally incorrect.
      // TODO: Allow the rhs of any cross join as the leftmost table. This needs careful
      // consideration of the joinOps that result from such a re-ordering (IMPALA-1281).
      if (joinOp.isOuterJoin() || joinOp.isSemiJoin() || joinOp.isCrossJoin()) continue;

      PlanNode plan = entry.second;
      if (plan.getCardinality() == -1) {
        // use 0 for the size to avoid it becoming the leftmost input
        // TODO: Consider raw size of scanned partitions in the absence of stats.
        candidates.add(new Pair<TableRef, Long>(ref, Long.valueOf(0)));
        if (LOG.isTraceEnabled()) {
          LOG.trace("candidate " + ref.getUniqueAlias() + ": 0");
        }
        continue;
      }
      Preconditions.checkState(ref.isAnalyzed());
      long materializedSize =
          (long) Math.ceil(plan.getAvgRowSize() * (double) plan.getCardinality());
      candidates.add(new Pair<TableRef, Long>(ref, Long.valueOf(materializedSize)));
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "candidate " + ref.getUniqueAlias() + ": " + Long.toString(materializedSize));
      }
    }
    if (candidates.isEmpty()) return null;

    // order candidates by descending materialized size; we want to minimize the memory
    // consumption of the materialized hash tables required for the join sequence
    Collections.sort(candidates,
        new Comparator<Pair<TableRef, Long>>() {
          @Override
          public int compare(Pair<TableRef, Long> a, Pair<TableRef, Long> b) {
            long diff = b.second - a.second;
            return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
          }
        });

    for (Pair<TableRef, Long> candidate: candidates) {
      PlanNode result = createJoinPlan(analyzer, candidate.first, parentRefPlans, subplanRefs);
      if (result != null) return result;
    }
    return null;
  }

  /**
   * Returns a plan with leftmostRef's plan as its leftmost input; the joins
   * are in decreasing order of selectiveness (percentage of rows they eliminate).
   * Creates and adds subplan nodes as soon as the tuple ids required by at least one
   * subplan ref are materialized by a join node added during plan generation.
   */
  private PlanNode createJoinPlan(Analyzer analyzer, TableRef leftmostRef,
      List<Pair<TableRef, PlanNode>> refPlans, List<SubplanRef> subplanRefs)
      throws ImpalaException {

    if (LOG.isTraceEnabled()) {
      LOG.trace("createJoinPlan: " + leftmostRef.getUniqueAlias());
    }
    // the refs that have yet to be joined
    List<Pair<TableRef, PlanNode>> remainingRefs = new ArrayList<>();
    PlanNode root = null;  // root of accumulated join plan
    for (Pair<TableRef, PlanNode> entry: refPlans) {
      if (entry.first == leftmostRef) {
        root = entry.second;
      } else {
        remainingRefs.add(entry);
      }
    }
    Preconditions.checkNotNull(root);

    // Maps from a TableRef in refPlans with an outer/semi join op to the set of
    // TableRefs that precede it refPlans (i.e., in FROM-clause order).
    // The map is used to place outer/semi joins at a fixed position in the plan tree
    // (IMPALA-860), s.t. all the tables appearing to the left/right of an outer/semi
    // join in the original query still remain to the left/right after join ordering.
    // This prevents join re-ordering across outer/semi joins which is generally wrong.
    Map<TableRef, Set<TableRef>> precedingRefs = new HashMap<>();
    List<TableRef> tmpTblRefs = new ArrayList<>();
    for (Pair<TableRef, PlanNode> entry: refPlans) {
      TableRef tblRef = entry.first;
      if (tblRef.getJoinOp().isOuterJoin() || tblRef.getJoinOp().isSemiJoin()) {
        precedingRefs.put(tblRef, Sets.newHashSet(tmpTblRefs));
      }
      tmpTblRefs.add(tblRef);
    }

    // Refs that have been joined. The union of joinedRefs and the refs in remainingRefs
    // are the set of all table refs.
    Set<TableRef> joinedRefs = Sets.newHashSet(leftmostRef);
    long numOps = 0;
    int i = 0;
    while (!remainingRefs.isEmpty()) {
      // We minimize the resulting cardinality at each step in the join chain,
      // which minimizes the total number of hash table lookups.
      PlanNode newRoot = null;
      Pair<TableRef, PlanNode> minEntry = null;
      for (Pair<TableRef, PlanNode> entry: remainingRefs) {
        TableRef ref = entry.first;
        JoinOperator joinOp = ref.getJoinOp();

        // Place outer/semi joins at a fixed position in the plan tree.
        Set<TableRef> requiredRefs = precedingRefs.get(ref);
        if (requiredRefs != null) {
          Preconditions.checkState(joinOp.isOuterJoin() || joinOp.isSemiJoin());
          // If the required table refs have not been placed yet, do not even consider
          // the remaining table refs to prevent incorrect re-ordering of tables across
          // outer/semi joins.
          if (!requiredRefs.equals(joinedRefs)) break;
        }

        analyzer.setAssignedConjuncts(root.getAssignedConjuncts());
        PlanNode candidate = createJoinNode(root, entry.second, ref, analyzer);
        if (candidate == null) continue;
        if (LOG.isTraceEnabled()) {
          LOG.trace("cardinality=" + Long.toString(candidate.getCardinality()));
        }

        // Use 'candidate' as the new root; don't consider any other table refs at this
        // position in the plan.
        if (joinOp.isOuterJoin() || joinOp.isSemiJoin()) {
          newRoot = candidate;
          minEntry = entry;
          break;
        }

        // Always prefer Hash Join over Nested-Loop Join due to limited costing
        // infrastructure.
        if (newRoot == null
            || (candidate.getClass().equals(newRoot.getClass())
                && candidate.getCardinality() < newRoot.getCardinality())
            || (candidate instanceof HashJoinNode
                && newRoot instanceof NestedLoopJoinNode)) {
          newRoot = candidate;
          minEntry = entry;
        }
      }
      if (newRoot == null) {
        // Could not generate a valid plan.
        return null;
      }

      // we need to insert every rhs row into the hash table and then look up
      // every lhs row
      long lhsCardinality = root.getCardinality();
      long rhsCardinality = minEntry.second.getCardinality();
      numOps += lhsCardinality + rhsCardinality;
      if (LOG.isTraceEnabled()) {
        LOG.trace(Integer.toString(i) + " chose " + minEntry.first.getUniqueAlias()
            + " #lhs=" + Long.toString(lhsCardinality)
            + " #rhs=" + Long.toString(rhsCardinality)
            + " #ops=" + Long.toString(numOps));
      }
      remainingRefs.remove(minEntry);
      joinedRefs.add(minEntry.first);
      root = newRoot;
      // Create a Subplan on top of the new root for all the subplan refs that can be
      // evaluated at this point.
      // TODO: Once we have stats on nested collections, we should consider the join
      // order in conjunction with the placement of SubplanNodes, i.e., move the creation
      // of SubplanNodes into the join-ordering loop above.
      root = createSubplan(root, subplanRefs, false, analyzer);
      // assign node ids after running through the possible choices in order to end up
      // with a dense sequence of node ids
      if (root instanceof SubplanNode) root.getChild(0).setId(ctx_.getNextNodeId());
      root.setId(ctx_.getNextNodeId());
      analyzer.setAssignedConjuncts(root.getAssignedConjuncts());
      ++i;
    }

    return root;
  }

  /**
   * Return a plan with joins in the order of parentRefPlans (= FROM clause order).
   * Adds coalesced SubplanNodes based on the FROM-clause order of subplanRefs.
   */
  private PlanNode createFromClauseJoinPlan(Analyzer analyzer,
      List<Pair<TableRef, PlanNode>> parentRefPlans, List<SubplanRef> subplanRefs)
      throws ImpalaException {
    // create left-deep sequence of binary hash joins; assign node ids as we go along
    Preconditions.checkState(!parentRefPlans.isEmpty());
    PlanNode root = parentRefPlans.get(0).second;
    for (int i = 1; i < parentRefPlans.size(); ++i) {
      TableRef innerRef = parentRefPlans.get(i).first;
      PlanNode innerPlan = parentRefPlans.get(i).second;
      root = createJoinNode(root, innerPlan, innerRef, analyzer);
      if (root != null) root = createSubplan(root, subplanRefs, false, analyzer);
      if (root instanceof SubplanNode) root.getChild(0).setId(ctx_.getNextNodeId());
      root.setId(ctx_.getNextNodeId());
    }
    return root;
  }

  /**
   * Create tree of PlanNodes that implements the Select/Project/Join/Group by/Having
   * of the selectStmt query block.
   */
  private PlanNode createSelectPlan(SelectStmt selectStmt, Analyzer analyzer)
      throws ImpalaException {
    // no from clause -> materialize the select's exprs with a UnionNode
    if (selectStmt.getTableRefs().isEmpty()) {
      return createConstantSelectPlan(selectStmt, analyzer);
    }

    // Slot materialization:
    // We need to mark all slots as materialized that are needed during the execution
    // of selectStmt, and we need to do that prior to creating plans for the TableRefs
    // (because createTableRefNode() might end up calling computeMemLayout() on one or
    // more TupleDescriptors, at which point all referenced slots need to be marked).
    //
    // For non-join predicates, slots are marked as follows:
    // - for base table scan predicates, this is done directly by ScanNode.init(), which
    //   can do a better job because it doesn't need to materialize slots that are only
    //   referenced for partition pruning, for instance
    // - for inline views, non-join predicates are pushed down, at which point the
    //   process repeats itself.
    selectStmt.materializeRequiredSlots(analyzer);

    List<TupleId> rowTuples = new ArrayList<>();
    // collect output tuples of subtrees
    for (TableRef tblRef: selectStmt.getTableRefs()) {
      rowTuples.addAll(tblRef.getMaterializedTupleIds());
    }

    // If the selectStmt's select-project-join portion returns an empty result set
    // create a plan that feeds the aggregation of selectStmt with an empty set.
    // Make sure the slots of the aggregation exprs and the tuples that they reference
    // are materialized (see IMPALA-1960). Marks all collection-typed slots referenced
    // in this select stmt as non-materialized because they are never unnested. Note that
    // this creates extra unused space in the tuple since the mem layout has already been
    // computed.
    if (analyzer.hasEmptySpjResultSet()) {
      unmarkCollectionSlots(selectStmt);
      PlanNode emptySetNode = new EmptySetNode(ctx_.getNextNodeId(), rowTuples);
      emptySetNode.init(analyzer);
      emptySetNode.setOutputSmap(selectStmt.getBaseTblSmap());
      return createAggregationPlan(selectStmt, analyzer, emptySetNode);
    }

    // Transform outer join into inner join whenever possible
    if (!analyzer.isStraightJoin() &&
        analyzer.getQueryOptions().isEnable_outer_join_to_inner_transformation() &&
        analyzer.hasOuterJoined(selectStmt.getTableRefs())) {
      Set<TupleId> nonnullableTidList = new HashSet<>();
      if (analyzer.simplifyOuterJoins(selectStmt.getTableRefs(), nonnullableTidList)) {
        // Recompute the graph since we may need to add value-transfer edges based on the
        // registered equi-join conjuncts.
        ctx_.getTimeline().markEvent("Recomputing value transfer graph");
        analyzer.computeValueTransferGraph();
        ctx_.getTimeline().markEvent("Value transfer graph computed");
        valueTransferGraphNeedsUpdate_ = false;
      }
    }

    // Separate table refs into parent refs (uncorrelated or absolute) and
    // subplan refs (correlated or relative), and generate their plan.
    List<TableRef> parentRefs = new ArrayList<>();
    List<SubplanRef> subplanRefs = new ArrayList<>();
    computeParentAndSubplanRefs(
        selectStmt.getTableRefs(), analyzer.isStraightJoin(), parentRefs, subplanRefs);
    MultiAggregateInfo multiAggInfo = selectStmt.getMultiAggInfo();
    PlanNode root = createTableRefsPlan(parentRefs, subplanRefs, multiAggInfo, analyzer);
    // Add aggregation, if any.
    if (multiAggInfo != null) {
      // Apply substitution for optimized scan/agg plan,
      if (multiAggInfo.getMaterializedAggClasses().size() == 1 &&
          (root instanceof HdfsScanNode || root instanceof KuduScanNode)) {
        AggregateInfo scanAggInfo = multiAggInfo.getMaterializedAggClass(0);
        scanAggInfo.substitute(((ScanNode) root).getOptimizedAggSmap(), analyzer);
        scanAggInfo.getMergeAggInfo().substitute(
            ((ScanNode) root).getOptimizedAggSmap(), analyzer);
      }
      root = createAggregationPlan(selectStmt, analyzer, root);
    }

    // All the conjuncts_ should be assigned at this point.
    // TODO: Re-enable this check here and/or elswehere.
    //Preconditions.checkState(!analyzer.hasUnassignedConjuncts());
    return root;
  }

  /**
   * Holds a table ref that must be evaluated inside a subplan (i.e., a relative or
   * correlated ref), along with the materialized tuple ids and table ref ids that
   * are required for this table ref to be correctly evaluated inside a SubplanNode.
   *
   * Required materialized tuple ids:
   * These ensure that the SubplanNode evaluating this table ref is placed only once all
   * root tuples needed by this table ref or relative refs contained in this table ref
   * are materialized.
   *
   * Required table ref ids:
   * These ensure that the SubplanNode evaluating this table ref is placed correctly
   * with respect to join ordering, in particular, that the SubplanNode is not ordered
   * across semi/outer joins.
   */
  private static class SubplanRef {
    // Relative or correlated table ref.
    public final TableRef tblRef;

    // List of tuple ids that must be materialized before 'tblRef' can be
    // correctly evaluated inside a SubplanNode.
    public final List<TupleId> requiredTids;

    // List of table ref ids that a plan tree must contain before 'tblRef'
    // can be correctly evaluated inside a SubplanNode.
    public final List<TupleId> requiredTblRefIds;

    public SubplanRef(TableRef tblRef, List<TupleId> requiredTids,
        List<TupleId> requiredTblRefIds) {
      Preconditions.checkState(tblRef.isRelative() || tblRef.isCorrelated());
      this.tblRef = tblRef;
      this.requiredTids = requiredTids;
      this.requiredTblRefIds = requiredTblRefIds;
    }
  }

  /**
   * Separates tblRefs into the following two lists.
   *
   * parentRefs:
   * Uncorrelated and non-relative table refs. These are the 'regular' table refs whose
   * plans are connected by join nodes, and are not placed inside a Subplan. The returned
   * parentRefs are self-contained with respect to TableRef linking, i.e., each returned
   * TableRef has its left TableRef link set to the TableRef preceding it in parentRefs.
   *
   * subplanRefs:
   * Correlated and relative table refs. The plan of such refs must be put inside a
   * Subplan. See SubplanRef for details. The left TableRef link of the TableRefs in
   * returned SubplanRefs are set to null.
   * If isStraightJoin is true, then the required tuple ids and table ref ids of a
   * correlated or relative ref are simply those of all table refs preceding it in
   * the FROM-clause order.
   *
   * If this function is called when generating the right-hand side of a SubplanNode,
   * then correlated and relative table refs that require only tuples produced by the
   * SubplanNode's input are placed inside parentRefs.
   */
  private void computeParentAndSubplanRefs(List<TableRef> tblRefs,
      boolean isStraightJoin, List<TableRef> parentRefs, List<SubplanRef> subplanRefs) {
    // List of table ref ids materialized so far during plan generation, including those
    // from the subplan context, if any. We append the ids of table refs placed into
    // parentRefs to this list to satisfy the ordering requirement of subsequent
    // table refs that should also be put into parentRefs. Consider this example:
    // FROM t, (SELECT ... FROM t.c1 LEFT JOIN t.c2 ON(...) JOIN t.c3 ON (...)) v
    // Table ref t.c3 has an ordering dependency on t.c2 due to the outer join, but t.c3
    // must be placed into the subplan that materializes t.c1 and t.c2.
    List<TupleId> planTblRefIds = new ArrayList<>();

    // List of materialized tuple ids in the subplan context, if any. This list must
    // remain constant in this function because the subplan context is fixed. Any
    // relative or correlated table ref that requires a materialized tuple id produced
    // by an element in tblRefs should be placed into subplanRefs because it requires
    // a new subplan context. Otherwise, it should be placed into parentRefs.
    List<TupleId> subplanTids = Collections.emptyList();

    if (ctx_.hasSubplan()) {
      // Add all table ref ids from the subplan context.
      planTblRefIds.addAll(ctx_.getSubplan().getChild(0).getTblRefIds());
      subplanTids =
          Collections.unmodifiableList(ctx_.getSubplan().getChild(0).getTupleIds());
    }

    // Table ref representing the last outer or semi join we have seen.
    TableRef lastSemiOrOuterJoin = null;
    for (TableRef ref: tblRefs) {
      boolean isParentRef = true;
      if (ref.isRelative() || ref.isCorrelated()) {
        List<TupleId> requiredTids = new ArrayList<>();
        List<TupleId> requiredTblRefIds = new ArrayList<>();
        if (ref.isCorrelated()) {
          requiredTids.addAll(ref.getCorrelatedTupleIds());
        } else {
          CollectionTableRef collectionTableRef = (CollectionTableRef) ref;
          SlotRef collectionExpr =
              (SlotRef) collectionTableRef.getCollectionExpr();
          if (collectionExpr != null) {
            // If the collection is within a (possibly nested) struct, add the tuple in
            // which the top level struct is located.
            SlotDescriptor desc = collectionExpr.getDesc();
            TupleDescriptor topTuple = desc.getTopEnclosingTupleDesc();
            requiredTids.add(topTuple.getId());
          } else {
            requiredTids.add(collectionTableRef.getResolvedPath().getRootDesc().getId());
          }
        }
        // Add all plan table ref ids as an ordering dependency for straight_join.
        if (isStraightJoin) requiredTblRefIds.addAll(planTblRefIds);
        if (lastSemiOrOuterJoin != null) {
          // Prevent incorrect join re-ordering across outer/semi joins by requiring all
          // table ref ids to the left and including the last outer/semi join.
          // TODO: Think about when we can allow re-ordering across semi/outer joins
          // in subplans.
          requiredTblRefIds.addAll(lastSemiOrOuterJoin.getAllTableRefIds());
        }
        if (!subplanTids.containsAll(requiredTids)) {
          isParentRef = false;
          // Outer and semi joins are placed at a fixed position in the join order.
          // They require that all tables to their left are materialized.
          if (ref.getJoinOp().isOuterJoin() || ref.getJoinOp().isSemiJoin()) {
            requiredTblRefIds.addAll(ref.getAllTableRefIds());
            requiredTblRefIds.remove(ref.getId());
          }
          subplanRefs.add(new SubplanRef(ref, requiredTids, requiredTblRefIds));
        }
      }
      if (isParentRef) {
        parentRefs.add(ref);
        planTblRefIds.add(ref.getId());
      }
      if (ref.getJoinOp().isOuterJoin() || ref.getJoinOp().isSemiJoin()) {
        lastSemiOrOuterJoin = ref;
      }
    }
    Preconditions.checkState(tblRefs.size() == parentRefs.size() + subplanRefs.size());

    // Fix the chain of parent table refs and set the left table of all subplanRefs to
    // null. This step needs to be done outside of the loop above because the left links
    // are required for getAllTupleIds() used for determining the requiredTblRefIds.
    parentRefs.get(0).setLeftTblRef(null);
    for (int i = 1; i < parentRefs.size(); ++i) {
      parentRefs.get(i).setLeftTblRef(parentRefs.get(i - 1));
    }
    for (SubplanRef subplanRef: subplanRefs) subplanRef.tblRef.setLeftTblRef(null);
    if (LOG.isTraceEnabled()) {
      LOG.trace("parentRefs: {}",
          parentRefs.stream().map(TableRef::debugString).reduce(", ", String::concat));
      LOG.trace("subplanRefs: {}", subplanRefs.stream().map(r -> r.tblRef.debugString())
          .reduce(", ", String::concat));
    }
  }

  /**
   * Returns a plan tree for evaluating the given parentRefs and subplanRefs.
   */
  private PlanNode createTableRefsPlan(List<TableRef> parentRefs,
      List<SubplanRef> subplanRefs, MultiAggregateInfo aggInfo, Analyzer analyzer)
      throws ImpalaException {
    // create plans for our table refs; use a list here instead of a map to
    // maintain a deterministic order of traversing the TableRefs during join
    // plan generation (helps with tests)
    List<Pair<TableRef, PlanNode>> parentRefPlans = new ArrayList<>();
    List<CollectionTableRef> unnestCollectionRefs =
        extractZippingUnnestTableRefs(parentRefs);
    reduceUnnestCollectionRefs(parentRefs, unnestCollectionRefs);
    for (TableRef ref: parentRefs) {
      PlanNode root = createTableRefNode(ref, aggInfo, analyzer, unnestCollectionRefs);
      Preconditions.checkNotNull(root);
      root = createSubplan(root, subplanRefs, true, analyzer);
      parentRefPlans.add(new Pair<TableRef, PlanNode>(ref, root));
    }
    // save state of conjunct assignment; needed for join plan generation
    for (Pair<TableRef, PlanNode> entry: parentRefPlans) {
      entry.second.setAssignedConjuncts(analyzer.getAssignedConjuncts());
    }

    PlanNode root = null;
    if (!analyzer.isStraightJoin()) {
      Set<ExprId> assignedConjuncts = analyzer.getAssignedConjuncts();
      root = createCheapestJoinPlan(analyzer, parentRefPlans, subplanRefs);
      // If createCheapestJoinPlan() failed to produce an executable plan, then we need
      // to restore the original state of conjunct assignment for the straight-join plan
      // to not incorrectly miss conjuncts.
      if (root == null) analyzer.setAssignedConjuncts(assignedConjuncts);
    }
    if (analyzer.isStraightJoin() || root == null) {
      // we didn't have enough stats to do a cost-based join plan, or the STRAIGHT_JOIN
      // keyword was in the select list: use the FROM clause order instead
      root = createFromClauseJoinPlan(analyzer, parentRefPlans, subplanRefs);
      Preconditions.checkNotNull(root);
    }
    return root;
  }

  /**
   * Places a SubplanNode on top of 'root' that evaluates all the subplan refs that can
   * be correctly evaluated from 'root's materialized tuple ids. Returns 'root' if there
   * are no applicable subplan refs.
   * Assigns the returned SubplanNode a new node id unless assignId is false.
   *
   * If applicable, the SubplanNode is created as follows:
   * - 'root' is the input of the SubplanNode (first child)
   * - the second child is the plan tree generated from these table refs:
   *   1. a SingularRowSrcTableRef that represents the current row being processed
   *      by the SubplanNode to be joined with
   *   2. all applicable subplan refs
   * - the second child plan tree is generated as usual with createTableRefsPlan()
   * - the plans of the applicable subplan refs are generated as usual, without a
   *   SingularRowSrcTableRef
   * - nested SubplanNodes are generated recursively inside createTableRefsPlan() by
   *   passing in the remaining subplanRefs that are not applicable after 'root'; some
   *   of those subplanRefs may become applicable inside the second child plan tree of
   *   the SubplanNode generated here
   */
  private PlanNode createSubplan(PlanNode root, List<SubplanRef> subplanRefs,
      boolean assignId, Analyzer analyzer) throws ImpalaException {
    Preconditions.checkNotNull(root);
    List<TableRef> applicableRefs = extractApplicableRefs(root, subplanRefs);
    if (applicableRefs.isEmpty()) return root;

    // Prepend a SingularRowSrcTableRef representing the current row being processed
    // by the SubplanNode from its input (first child).
    Preconditions.checkState(applicableRefs.get(0).getLeftTblRef() == null);
    applicableRefs.add(0, new SingularRowSrcTableRef(root));
    applicableRefs.get(1).setLeftTblRef(applicableRefs.get(0));

    // Construct an incomplete SubplanNode that only knows its input so we can push it
    // into the planner context. The subplan is set after the subplan tree has been
    // constructed.
    SubplanNode subplanNode = new SubplanNode(root);
    if (assignId) subplanNode.setId(ctx_.getNextNodeId());

    // Push the SubplanNode such that UnnestNodes and SingularRowSrcNodes can pick up
    // their containing SubplanNode. Also, further plan generation relies on knowing
    // whether we are in a subplan context or not (see computeParentAndSubplanRefs()).
    ctx_.pushSubplan(subplanNode);
    PlanNode subplan = createTableRefsPlan(applicableRefs, subplanRefs, null, analyzer);
    ctx_.popSubplan();
    subplanNode.setSubplan(subplan);
    subplanNode.init(analyzer);
    return subplanNode;
  }

  /**
   * Returns a new list with all table refs from subplanRefs that can be correctly
   * evaluated inside a SubplanNode placed after the given plan root.
   * The returned table refs have their left-table links properly set, and the
   * corresponding SubplanRefs are removed from subplanRefs.
   */
  private List<TableRef> extractApplicableRefs(PlanNode root,
      List<SubplanRef> subplanRefs) {
    // List of table ref ids in 'root' as well as the table ref ids of all table refs
    // placed in 'subplanRefs' so far.
    List<TupleId> tblRefIds = Lists.newArrayList(root.getTblRefIds());
    List<TableRef> result = new ArrayList<>();
    Iterator<SubplanRef> subplanRefIt = subplanRefs.iterator();
    TableRef leftTblRef = null;
    while (subplanRefIt.hasNext()) {
      SubplanRef subplanRef = subplanRefIt.next();
      // Ensure that 'root' materializes all required tuples (first condition), and that
      // correct join ordering is obeyed (second condition).
      if (root.getTupleIds().containsAll(subplanRef.requiredTids) &&
          tblRefIds.containsAll(subplanRef.requiredTblRefIds)) {
        subplanRef.tblRef.setLeftTblRef(leftTblRef);
        result.add(subplanRef.tblRef);
        leftTblRef = subplanRef.tblRef;
        subplanRefIt.remove();
        // Add the table ref id such that other subplan refs that can be evaluated inside
        // the same SubplanNode but only after this ref are returned as well.
        tblRefIds.add(subplanRef.tblRef.getId());
      }
    }
    return result;
  }

  /**
   * This functions gathers and returns all the CollectionTableRefs that are for zipping
   * unnest.
   */
  private List<CollectionTableRef> extractZippingUnnestTableRefs(
      List<TableRef> refs) {
    Preconditions.checkNotNull(refs);
    List<CollectionTableRef> collectionRefs = Lists.newArrayList();
    for (TableRef ref : refs) {
      if (ref instanceof CollectionTableRef && ref.isZippingUnnest()) {
        collectionRefs.add((CollectionTableRef)ref);
      }
    }
    return collectionRefs;
  }

  /**
   * This functions removes the items in 'unnestCollectionRefs' from 'refs' except the
   * first item in 'unnestCollectionRefs'. This is used when the collectionTableRefs are
   * handled by a single UNNEST node for zipping unnest. A single CollectionTableRef item
   * has to remain in 'refs' so that the subplan creation can see that an UNNEST node has
   * to be created.
   */
  private void reduceUnnestCollectionRefs(List<TableRef> refs,
      List<CollectionTableRef> unnestCollectionRefs) {
    Preconditions.checkNotNull(refs);
    Preconditions.checkNotNull(unnestCollectionRefs);
    if (unnestCollectionRefs.size() <= 1) return;
    List<CollectionTableRef> reducedCollectionRefs =
        unnestCollectionRefs.subList(1, unnestCollectionRefs.size());
    refs.removeAll(reducedCollectionRefs);
  }

  /**
   * Returns a new AggregationNode that materializes the aggregation of the given stmt.
   * Assigns conjuncts from the Having clause to the returned node.
   */
  private AggregationNode createAggregationPlan(
      SelectStmt selectStmt, Analyzer analyzer, PlanNode root) throws ImpalaException {
    MultiAggregateInfo multiAggInfo =
        Preconditions.checkNotNull(selectStmt.getMultiAggInfo());
    AggregationNode result = createAggregationPlan(root, multiAggInfo, analyzer);
    ExprSubstitutionMap simplifiedAggSmap = multiAggInfo.getSimplifiedAggSmap();
    if (simplifiedAggSmap == null) return result;

    // Fix up aggregations that simplified to a single class after
    // materializeRequiredSlots().

    // Collect conjuncts and then re-assign them to the top-most aggregation node
    // of the simplified plan.
    AggregationNode dummyAgg = new AggregationNode(
        ctx_.getNextNodeId(), result, multiAggInfo, AggPhase.TRANSPOSE);
    dummyAgg.init(analyzer);
    List<Expr> conjuncts =
        Expr.substituteList(dummyAgg.getConjuncts(), simplifiedAggSmap, analyzer, true);
    // Validate conjuncts after substitution.
    for (Expr c : conjuncts) {
      Preconditions.checkState(c.isBound(result.getTupleIds().get(0)));
    }
    result.getConjuncts().addAll(conjuncts);

    // Apply simplification substitution in ancestors.
    result.setOutputSmap(
        ExprSubstitutionMap.compose(result.getOutputSmap(), simplifiedAggSmap, analyzer));
    return result;
  }

  private AggregationNode createAggregationPlan(PlanNode root,
      MultiAggregateInfo multiAggInfo, Analyzer analyzer) throws InternalException {
    Preconditions.checkNotNull(multiAggInfo);
    AggregationNode agg =
        new AggregationNode(ctx_.getNextNodeId(), root, multiAggInfo, AggPhase.FIRST);
    agg.init(analyzer);
    if (!multiAggInfo.hasSecondPhase() && !multiAggInfo.hasTransposePhase()) {
      return agg;
    }

    agg.setIntermediateTuple();

    if (multiAggInfo.hasSecondPhase()) {
      agg.unsetNeedsFinalize();
      agg = new AggregationNode(
          ctx_.getNextNodeId(), agg, multiAggInfo, AggPhase.SECOND);
      agg.init(analyzer);
    }
    if (multiAggInfo.hasTransposePhase()) {
      agg = new AggregationNode(
          ctx_.getNextNodeId(), agg, multiAggInfo, AggPhase.TRANSPOSE);
      agg.init(analyzer);
    }
    return agg;
  }

 /**
  * Returns a UnionNode that materializes the exprs of the constant selectStmt.
  * Replaces the resultExprs of the selectStmt with SlotRefs into the materialized tuple.
  */
  private PlanNode createConstantSelectPlan(SelectStmt selectStmt, Analyzer analyzer)
      throws InternalException {
    Preconditions.checkState(selectStmt.getTableRefs().isEmpty());
    List<Expr> resultExprs = selectStmt.getResultExprs();
    // Create tuple descriptor for materialized tuple.
    TupleDescriptor tupleDesc = createResultTupleDescriptor(selectStmt, "union", analyzer);
    UnionNode unionNode = new UnionNode(ctx_.getNextNodeId(), tupleDesc.getId());
    // Analysis guarantees that selects without a FROM clause only have constant exprs.
    unionNode.addConstExprList(Lists.newArrayList(resultExprs));

    // Replace the select stmt's resultExprs with SlotRefs into tupleDesc.
    for (int i = 0; i < resultExprs.size(); ++i) {
      SlotRef slotRef = new SlotRef(tupleDesc.getSlots().get(i));
      resultExprs.set(i, slotRef);
    }
    // UnionNode.init() needs tupleDesc to have been initialized
    unionNode.init(analyzer);
    return unionNode;
  }

  /**
   * Create tuple descriptor that can hold the results of the given SelectStmt, with one
   * slot per result expr.
   */
  private TupleDescriptor createResultTupleDescriptor(SelectStmt selectStmt,
      String debugName, Analyzer analyzer) {
    TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor(
        debugName);
    tupleDesc.setIsMaterialized(true);

    List<Expr> resultExprs = selectStmt.getResultExprs();
    List<String> colLabels = selectStmt.getColLabels();
    for (int i = 0; i < resultExprs.size(); ++i) {
      Expr resultExpr = resultExprs.get(i);
      String colLabel = colLabels.get(i);
      SlotDescriptor slotDesc = analyzer.addSlotDescriptor(tupleDesc);
      slotDesc.setLabel(colLabel);
      slotDesc.setSourceExpr(resultExpr);
      slotDesc.setType(resultExpr.getType());
      slotDesc.setStats(ColumnStats.fromExpr(resultExpr));
      slotDesc.setIsMaterialized(true);
    }
    tupleDesc.computeMemLayout();
    return tupleDesc;
  }

  /**
   * Returns plan tree for an inline view ref:
   * - predicates from the enclosing scope that can be evaluated directly within
   *   the inline-view plan are pushed down
   * - predicates that cannot be evaluated directly within the inline-view plan
   *   but only apply to the inline view are evaluated in a SelectNode placed
   *   on top of the inline view plan
   * - all slots that are referenced by predicates from the enclosing scope that cannot
   *   be pushed down are marked as materialized (so that when computeMemLayout() is
   *   called on the base table descriptors materialized by the inline view it has a
   *   complete picture)
   */
  private PlanNode createInlineViewPlan(Analyzer analyzer, InlineViewRef inlineViewRef)
      throws ImpalaException {
    // If possible, "push down" view predicates; this is needed in order to ensure
    // that predicates such as "x + y = 10" are evaluated in the view's plan tree
    // rather than a SelectNode grafted on top of that plan tree.
    // This doesn't prevent predicate propagation, because predicates like
    // "x = 10" that get pushed down are still connected to equivalent slots
    // via the equality predicates created for the view's select list.
    // Include outer join conjuncts here as well because predicates from the
    // On-clause of an outer join may be pushed into the inline view as well.
    migrateConjunctsToInlineView(analyzer, inlineViewRef);

    migrateSimpleLimitToInlineView(analyzer, inlineViewRef);

    // Turn a constant select into a UnionNode that materializes the exprs.
    // TODO: unify this with createConstantSelectPlan(), this is basically the
    // same thing
    QueryStmt viewStmt = inlineViewRef.getViewStmt();
    if (viewStmt instanceof SelectStmt) {
      SelectStmt selectStmt = (SelectStmt) viewStmt;
      if (selectStmt.getTableRefs().isEmpty()) {
        if (inlineViewRef.getAnalyzer().hasEmptyResultSet()) {
          PlanNode emptySetNode = createEmptyNode(viewStmt, inlineViewRef.getAnalyzer());
          // Still substitute exprs in parent nodes with the inline-view's smap to make
          // sure no exprs reference the non-materialized inline view slots. No wrapping
          // with TupleIsNullPredicates is necessary here because we do not migrate
          // conjuncts into outer-joined inline views, so hasEmptyResultSet() cannot be
          // true for an outer-joined inline view that has no table refs.
          Preconditions.checkState(!analyzer.isOuterJoined(inlineViewRef.getId()));
          emptySetNode.setOutputSmap(inlineViewRef.getSmap());
          // The tblRef materialized by this node is still the 'inlineViewRef'.
          emptySetNode.setTblRefIds(Lists.newArrayList(inlineViewRef.getId()));
          return emptySetNode;
        }
        // Analysis should have generated a tuple id into which to materialize the exprs.
        Preconditions.checkState(inlineViewRef.getMaterializedTupleIds().size() == 1);
        // we need to materialize all slots of our inline view tuple
        analyzer.getTupleDesc(inlineViewRef.getId()).materializeSlots();
        UnionNode unionNode = new UnionNode(ctx_.getNextNodeId(),
            inlineViewRef.getMaterializedTupleIds().get(0));
        if (analyzer.hasEmptyResultSet()) return unionNode;
        unionNode.setTblRefIds(Lists.newArrayList(inlineViewRef.getId()));
        unionNode.addConstExprList(selectStmt.getBaseTblResultExprs());
        unionNode.init(analyzer);
        return unionNode;
      }
    }

    PlanNode rootNode =
        createQueryPlan(inlineViewRef.getViewStmt(), inlineViewRef.getAnalyzer(), false);
    // TODO: we should compute the "physical layout" of the view's descriptor, so that
    // the avg row size is available during optimization; however, that means we need to
    // select references to its resultExprs from the enclosing scope(s)
    rootNode.setTblRefIds(Lists.newArrayList(inlineViewRef.getId()));

    // The output smap is the composition of the inline view's smap and the output smap
    // of the inline view's plan root. This ensures that all downstream exprs referencing
    // the inline view are replaced with exprs referencing the physical output of the
    // inline view's plan.
    ExprSubstitutionMap outputSmap = ExprSubstitutionMap.compose(
        inlineViewRef.getSmap(), rootNode.getOutputSmap(), analyzer);
    if (analyzer.isOuterJoined(inlineViewRef.getId())) {
      // Exprs against non-matched rows of an outer join should always return NULL.
      // Make the rhs exprs of the output smap nullable, if necessary. This expr wrapping
      // must be performed on the composed smap, and not on the the inline view's smap,
      // because the rhs exprs must first be resolved against the physical output of
      // 'planRoot' to correctly determine whether wrapping is necessary.
      List<Expr> nullableRhs = TupleIsNullPredicate.wrapExprs(
          outputSmap.getRhs(), rootNode.getTupleIds(), analyzer);
      outputSmap = new ExprSubstitutionMap(outputSmap.getLhs(), nullableRhs);
    }
    // Set output smap of rootNode *before* creating a SelectNode for proper resolution.
    rootNode.setOutputSmap(outputSmap);

    // Add runtime cardinality check if needed
    if (inlineViewRef.getViewStmt().isRuntimeScalar()) {
      rootNode = new CardinalityCheckNode(ctx_.getNextNodeId(), rootNode,
          inlineViewRef.getViewStmt().getOrigSqlString());
      rootNode.setOutputSmap(outputSmap);
      rootNode.init(ctx_.getRootAnalyzer());
    }

    // If the inline view has a LIMIT/OFFSET or unassigned conjuncts due to analytic
    // functions, we may have conjuncts that need to be assigned to a SELECT node on
    // top of the current plan root node.
    //
    // TODO: This check is also repeated in migrateConjunctsToInlineView() because we
    // need to make sure that equivalences are not enforced multiple times. Consolidate
    // the assignment of conjuncts and the enforcement of equivalences into a single
    // place.
    if (!canMigrateConjuncts(inlineViewRef)) {
      rootNode = addUnassignedConjuncts(
          analyzer, inlineViewRef.getDesc().getId().asList(), rootNode);
    }
    // Transfer whether the inline view is for a non-correlated subquery returning at
    // most one value to the root node when it is an AggregationNode.
    if (rootNode instanceof AggregationNode) {
      ((AggregationNode) rootNode)
          .setIsNonCorrelatedScalarSubquery(
              inlineViewRef.isNonCorrelatedScalarSubquery());
    }
    return rootNode;
  }

  /**
   * Get the unassigned conjuncts that can be evaluated in inline view and return them
   * in 'evalInInlineViewPreds'.
   * If a conjunct is not an On-clause predicate and is safe to propagate it inside the
   * inline view, add it to 'evalAfterJoinPreds'.
   */
  private void getConjunctsToInlineView(final Analyzer analyzer, final String alias,
      final List<TupleId> tupleIds, final List<Expr> unassignedConjuncts,
      List<Expr> evalInInlineViewPreds, List<Expr> evalAfterJoinPreds) {
    for (Expr e: unassignedConjuncts) {
      if (!e.isBoundByTupleIds(tupleIds)) continue;
      List<TupleId> tids = new ArrayList<>();
      e.getIds(tids, null);
      if (tids.isEmpty()) {
        evalInInlineViewPreds.add(e);
      } else if (e.isOnClauseConjunct()) {
        if (!analyzer.canEvalOnClauseConjunct(tupleIds, e)) continue;
        evalInInlineViewPreds.add(e);
      } else if (analyzer.isLastOjMaterializedByTupleIds(tupleIds, e)) {
        evalInInlineViewPreds.add(e);
      } else {
        // The predicate belong to an outer-joined tuple, it's correct to duplicate(not
        // migrate) this predicate inside the inline view since it does not evalute to
        // true with null slots
        try {
          if (!analyzer.isTrueWithNullSlots(e)) {
            evalAfterJoinPreds.add(e);
            if (LOG.isTraceEnabled()) {
              LOG.trace("Can propagate {} to inline view {}", e.debugString(), alias);
            }
          }
        } catch (InternalException ex) {
          // Expr evaluation failed in the backend. Skip 'e' since we cannot
          // determine whether propagation is safe.
          LOG.warn("Skipping propagation of conjunct because backend evaluation failed: "
              + e.toSql(), ex);
        }
        continue;
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Can evaluate {} in inline view {}", e.debugString(), alias);
      }
    }
  }

  public void migrateSimpleLimitToInlineView(final Analyzer analyzer,
    final InlineViewRef inlineViewRef) {
    Pair<Boolean, Long> outerStatus = analyzer.getSimpleLimitStatus();
    if (outerStatus == null || !outerStatus.first
        || inlineViewRef.isTableMaskingView()) {
      return;
    }
    Pair<Boolean, Long> viewStatus =
        inlineViewRef.getAnalyzer().getSimpleLimitStatus();
    // if the view already has a limit, we leave it as-is otherwise we
    // apply the outer limit
    if (viewStatus != null && !viewStatus.first) {
      inlineViewRef.getAnalyzer().setSimpleLimitStatus(new Pair<>(Boolean.valueOf(true),
          outerStatus.second));
    }
  }

  /**
   * Migrates unassigned conjuncts into an inline view. Conjuncts are not
   * migrated into the inline view if the view has a LIMIT/OFFSET clause or if the
   * view's stmt computes analytic functions (see IMPALA-1243/IMPALA-1900).
   * The reason is that analytic functions compute aggregates over their entire input,
   * and applying filters from the enclosing scope *before* the aggregate computation
   * would alter the results. This is unlike regular aggregate computation, which only
   * makes the *output* of the computation visible to the enclosing scope, so that
   * filters from the enclosing scope can be safely applied (to the grouping cols, say).
   */
  private void migrateConjunctsToInlineView(final Analyzer analyzer,
      final InlineViewRef inlineViewRef) throws ImpalaException {
    List<TupleId> tids = inlineViewRef.getId().asList();
    if (inlineViewRef.isTableMaskingView()
        && inlineViewRef.getUnMaskedTableRef().exposeNestedColumnsByTableMaskView()) {
      tids.add(inlineViewRef.getUnMaskedTableRef().getId());
    }
    List<Expr> unassignedConjuncts = analyzer.getUnassignedConjuncts(tids, true);
    if (LOG.isTraceEnabled()) {
      LOG.trace("migrateConjunctsToInlineView() unassignedConjuncts: " +
          Expr.debugString(unassignedConjuncts));
    }
    if (!canMigrateConjuncts(inlineViewRef)) {
      // We may be able to migrate some specific analytic conjuncts into the view.
      List<Expr> analyticPreds = findAnalyticConjunctsToMigrate(analyzer, inlineViewRef,
              unassignedConjuncts);
      if (analyticPreds.size() > 0) {
        migrateOrCopyConjunctsToInlineView(analyzer, inlineViewRef, tids,
            analyticPreds, unassignedConjuncts);
      }
    } else {
      migrateOrCopyConjunctsToInlineView(analyzer, inlineViewRef, tids,
          unassignedConjuncts, unassignedConjuncts);
    }

    // mark (fully resolve) slots referenced by remaining unassigned conjuncts as
    // materialized
    List<Expr> substUnassigned = Expr.substituteList(unassignedConjuncts,
        inlineViewRef.getBaseTblSmap(), analyzer, false);
    analyzer.materializeSlots(substUnassigned);
  }

  /**
   * Migrate or copy unassigned conjuncts into an inline view. Parameter evalPreds is
   * analytic predicates when there has specific analytic, equals to unassignedConjuncts
   * when there has no LIMIT/OFFSET or anylitic functions.
   */
  private void migrateOrCopyConjunctsToInlineView(final Analyzer analyzer,
      final InlineViewRef inlineViewRef, final List<TupleId> tids,
      List<Expr> evalPreds, List<Expr> unassignedConjuncts)
      throws ImpalaException {
    List<Expr> evalInInlineViewPreds = new ArrayList<>();
    List<Expr> evalAfterJoinPreds = new ArrayList<>();
    getConjunctsToInlineView(analyzer, inlineViewRef.getExplicitAlias(), tids,
        evalPreds, evalInInlineViewPreds, evalAfterJoinPreds);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Assign predicates for view: migrating {}, coping {}.",
          Expr.debugString(evalInInlineViewPreds), Expr.debugString(evalAfterJoinPreds));
    }
    unassignedConjuncts.removeAll(evalInInlineViewPreds);
    // Migrate the conjuncts by marking the original ones as assigned. They will either
    // be ignored if they are identity predicates (e.g. a = a), or be substituted into
    // new ones. The substituted ones will be re-registered.
    analyzer.markConjunctsAssigned(evalInInlineViewPreds);
    // Propagate the conjuncts evaluating the nullable side of outer-join.
    // Don't mark them as assigned so they can be assigned at the JOIN node.
    evalInInlineViewPreds.addAll(evalAfterJoinPreds);
    addConjunctsIntoInlineView(analyzer, inlineViewRef, evalInInlineViewPreds);
  }

  /**
   * Return any conjuncts in 'conjuncts' that reference analytic exprs in 'inlineViewRef'
   * and can be safely migrated into 'inlineViewRef', even if
   * canMigrateConjuncts(inlineViewRef) is false.
   */
  private List<Expr> findAnalyticConjunctsToMigrate(final Analyzer analyzer,
          final InlineViewRef inlineViewRef, List<Expr> conjuncts) {
    // Not safe to migrate if the inline view has a limit or offset that is applied after
    // analytic function evaluation.
    if (inlineViewRef.getViewStmt().hasLimit() || inlineViewRef.getViewStmt().hasOffset()
            || !(inlineViewRef.getViewStmt() instanceof SelectStmt)) {
      return Collections.emptyList();
    }
    SelectStmt selectStmt = ((SelectStmt) inlineViewRef.getViewStmt());
    if (!selectStmt.hasAnalyticInfo()) return Collections.emptyList();

    // Find conjuncts that reference the (logical) analytic tuple. These conjuncts will
    // only be evaluated after the analytic functions in the subquery so will not migrate
    // to be evaluated earlier in the plan (which could produce incorrect results).
    TupleDescriptor analyticTuple = selectStmt.getAnalyticInfo().getOutputTupleDesc();
    List<Expr> analyticPreds = new ArrayList<>();
    List<TupleId> tupleIds = new ArrayList<>();
    for (Expr pred : conjuncts) {
      Expr viewPred = pred.substitute(inlineViewRef.getSmap(), analyzer, false);
      tupleIds.clear();
      viewPred.getIds(tupleIds, null);
      // Only consider analytic conjuncts which are referencing the analytic tuple
      // and skip the ones which are referencing more than 1 tuple.
      // Example: a = MAX(b) (where MAX(b) is an analytic function). In this case,
      // the left side may not be referencing analytic tuple and it would not
      // be safe to migrate such predicates into the inline view.
      if (viewPred.referencesTuple(analyticTuple.getId()) && tupleIds.size() <= 1) {
        analyticPreds.add(pred);
      }
    }
    return analyticPreds;
  }

  /**
   * Add the provided conjuncts to be evaluated inside 'inlineViewRef'.
   * Does not mark the conjuncts as assigned.
   */
  private void addConjunctsIntoInlineView(final Analyzer analyzer,
          final InlineViewRef inlineViewRef, List<Expr> preds)
                  throws AnalysisException {
    // Generate predicates to enforce equivalences among slots of the inline view
    // tuple. These predicates are also migrated into the inline view.
    analyzer.createEquivConjuncts(inlineViewRef.getId(), preds);

    // Remove unregistered predicates that finally resolved to predicates reference
    // the same slot on both sides (e.g. a = a). Such predicates have been generated from
    // slot equivalences and may incorrectly reject rows with nulls
    // (IMPALA-1412/IMPALA-2643/IMPALA-8386).
    Predicate<Expr> isIdentityPredicate = new Predicate<Expr>() {
      @Override
      public boolean apply(Expr e) {
        if (!org.apache.impala.analysis.Predicate.isEquivalencePredicate(e)
            || !((BinaryPredicate) e).isInferred()) {
          return false;
        }
        try {
          BinaryPredicate finalExpr = (BinaryPredicate) e.trySubstitute(
              inlineViewRef.getBaseTblSmap(), analyzer, false);
          boolean isIdentity = finalExpr.hasIdenticalOperands();

          // Verity that "smap[e1] == smap[e2]" => "baseTblSmap[e1] == baseTblSmap[e2]"
          // in case we have bugs in generating baseTblSmap.
          BinaryPredicate midExpr = (BinaryPredicate) e.trySubstitute(
              inlineViewRef.getSmap(), analyzer, false);
          Preconditions.checkState(!midExpr.hasIdenticalOperands() || isIdentity);

          if (LOG.isTraceEnabled() && isIdentity) {
            LOG.trace("Removed identity predicate: " + finalExpr.debugString());
          }
          return isIdentity;
        } catch (Exception ex) {
          throw new IllegalStateException(
                  "Failed analysis after expr substitution.", ex);
        }
      }
    };
    Iterables.removeIf(preds, isIdentityPredicate);

    // create new predicates against the inline view's unresolved result exprs, not
    // the resolved result exprs, in order to avoid skipping scopes (and ignoring
    // limit clauses on the way)
    List<Expr> viewPredicates =
        Expr.substituteList(preds, inlineViewRef.getSmap(), analyzer, false);

    // perform any post-processing of the predicates before registering
    removeDisqualifyingInferredPreds(inlineViewRef.getAnalyzer(), viewPredicates);

    // Unset the On-clause flag of the migrated conjuncts because the migrated conjuncts
    // apply to the post-join/agg/analytic result of the inline view.
    for (Expr e: viewPredicates) e.setIsOnClauseConjunct(false);
    inlineViewRef.getAnalyzer().registerConjuncts(viewPredicates);
  }

  /**
   * Analyze the predicates in the context of the inline view for certain disqualifying
   * conditions and remove such predicates from the input list. One such condition is
   * the predicate is an inferred predicate AND either its left or right SlotRef
   * references the output of an outer join. Note that although such predicates
   * may have been detected at the time of creating the values transfer graph
   * (in the Analyzer), we do this check here anyways as a safety in case any such
   * predicate 'fell through' to this stage.
   */
  private void removeDisqualifyingInferredPreds(Analyzer analyzer, List<Expr> preds) {
    ListIterator<Expr> iter = preds.listIterator();
    while (iter.hasNext()) {
      Expr e = iter.next();
      if (e instanceof BinaryPredicate && ((BinaryPredicate)e).isInferred()) {
        BinaryPredicate p = (BinaryPredicate)e;
        Pair<SlotId, SlotId> slots = p.getEqSlots();
        if (slots == null) continue;
        TupleId leftParent = analyzer.getTupleId(slots.first);
        TupleId rightParent = analyzer.getTupleId(slots.second);
        // check if either the left parent or right parent is an outer joined tuple
        // Note: strictly, we may be ok to check only for the null producing
        // side but we are being conservative here to check both sides. With
        // additional testing we could potentially relax this.
        if (analyzer.isOuterJoined(leftParent) ||
                analyzer.isOuterJoined(rightParent)) {
          iter.remove();
          LOG.warn("Removed inferred predicate " + p.toSql() + " from the list of " +
                  "predicates considered for inline view because either the left " +
                  "or right side is derived from an outer join output.");
        }
      }
    }
  }

  /**
   * Checks if conjuncts can be migrated into an inline view.
   */
  private boolean canMigrateConjuncts(InlineViewRef inlineViewRef) {
    return !inlineViewRef.getViewStmt().hasLimit()
        && !inlineViewRef.getViewStmt().hasOffset()
        && (!(inlineViewRef.getViewStmt() instanceof SelectStmt)
            || !((SelectStmt) inlineViewRef.getViewStmt()).hasAnalyticInfo());
  }

  /**
   * Create a node to materialize the slots in the given HdfsTblRef.
   *
   * The given 'aggInfo' is used for detecting and applying optimizations that span both
   * the scan and aggregation.
   */
  private PlanNode createHdfsScanPlan(TableRef hdfsTblRef, MultiAggregateInfo aggInfo,
      List<Expr> conjuncts, Analyzer analyzer) throws ImpalaException {
    TupleDescriptor tupleDesc = hdfsTblRef.getDesc();

    // Do partition pruning before deciding which slots to materialize because we might
    // end up removing some predicates.
    HdfsPartitionPruner pruner = new HdfsPartitionPruner(tupleDesc);
    Pair<List<? extends FeFsPartition>, List<Expr>> pair =
        pruner.prunePartitions(analyzer, conjuncts, false, hdfsTblRef);
    List<? extends FeFsPartition> partitions = pair.first;

    // Mark all slots referenced by the remaining conjuncts as materialized.
    analyzer.materializeSlots(conjuncts);

    // TODO: Remove this section, once DATE type is supported across all fileformats.
    // Check if there are any partitions for which DATE is not supported.
    FeFsPartition part = findUnsupportedDateFsPartition(partitions);
    if (part != null) {
      FeFsTable table = (FeFsTable)hdfsTblRef.getTable();
      HdfsFileFormat ff = part.getFileFormat();
      // Throw an exception if tupleDesc contains a non-clustering, materialized
      // DATE slot.
      for (SlotDescriptor slotDesc: tupleDesc.getMaterializedSlots()) {
        if (slotDesc.getColumn() != null
            && !table.isClusteringColumn(slotDesc.getColumn())
            && slotDesc.getType() == ScalarType.DATE) {
          throw new NotImplementedException(
              "Scanning DATE values in table '" + table.getFullName() +
              "' is not supported for fileformat " + ff);
        }
      }
    }

    // For queries which contain partition columns only and where all materialized
    // aggregate expressions have distinct semantics, we only need to return one
    // row per partition. Please see createHdfsScanPlan() for details.
    boolean allAggsDistinct = aggInfo != null && aggInfo.hasAllDistinctAgg();
    boolean isPartitionKeyScan = allAggsDistinct && tupleDesc.hasClusteringColsOnly();

    // If the optimization for partition key scans with metadata is enabled,
    // try evaluating with metadata first. If not, fall back to scanning.
    TQueryOptions queryOpts = analyzer.getQueryCtx().client_request.query_options;
    if (isPartitionKeyScan && queryOpts.optimize_partition_key_scans) {
      Set<List<Expr>> uniqueExprs = new HashSet<>();

      for (FeFsPartition partition : partitions) {
        // Ignore empty partitions to match the behavior of the scan based approach.
        if (partition.getSize() == 0) continue;
        List<Expr> exprs = new ArrayList<>();
        for (SlotDescriptor slotDesc: tupleDesc.getSlots()) {
          // UnionNode.init() will go through all the slots in the tuple descriptor so
          // there needs to be an entry in 'exprs' for each slot. For unmaterialized
          // slots, use dummy null values. UnionNode will filter out unmaterialized slots.
          if (!slotDesc.isMaterialized()) {
            exprs.add(NullLiteral.create(slotDesc.getType()));
          } else {
            int pos = slotDesc.getColumn().getPosition();
            exprs.add(partition.getPartitionValue(pos));
          }
        }
        uniqueExprs.add(exprs);
      }

      // Create a UNION node with all unique partition keys.
      UnionNode unionNode = new UnionNode(ctx_.getNextNodeId(), tupleDesc.getId());
      for (List<Expr> exprList: uniqueExprs) {
        unionNode.addConstExprList(exprList);
      }
      unionNode.init(analyzer);
      return unionNode;
    } else if (addAcidSlotsIfNeeded(analyzer, hdfsTblRef, partitions)) {
      // We are scanning a full ACID table that has delete delta files. Let's create
      // a LEFT ANTI JOIN between the insert deltas and delete deltas.
      return createAcidJoinNode(analyzer, hdfsTblRef, conjuncts, partitions, pair.second);
    } else {
      HdfsScanNode scanNode =
          new HdfsScanNode(ctx_.getNextNodeId(), tupleDesc, conjuncts, partitions,
              hdfsTblRef, aggInfo, pair.second, isPartitionKeyScan);
      scanNode.init(analyzer);
      return scanNode;
    }
  }

  /**
   * Adds partitioning columns and ACID columns to the table. It's needed to do a hash
   * join between insert deltas and delete deltas.
   * Returns true when the slot refs are added successfully and they are needed.
   * Return false means this was a no-op because the slot refs are not needed, e.g.
   * it's a non-ACID table, or there are no delete delta files.
   * Throws exception in case of errors.
   * Purposefully made 'public' for third party usage.
   */
  public static boolean addAcidSlotsIfNeeded(Analyzer analyzer, TableRef hdfsTblRef,
      List<? extends FeFsPartition> partitions) throws AnalysisException {
    FeTable feTable = hdfsTblRef.getTable();
    if (!AcidUtils.isFullAcidTable(feTable.getMetaStoreTable().getParameters())) {
      return false;
    }
    boolean areThereDeletedRows = false;
    for (FeFsPartition partition: partitions) {
      if (partition.genDeleteDeltaPartition() != null) {
        areThereDeletedRows = true;
        break;
      }
    }
    if (!areThereDeletedRows) return false;
    addAcidSlots(analyzer, hdfsTblRef);
    return true;
  }

  /* Purposefully made 'public' for third party usage.*/
  public static void addAcidSlots(Analyzer analyzer, TableRef hdfsTblRef)
      throws AnalysisException {
    FeTable feTable = hdfsTblRef.getTable();
    List<String> rawPath = new ArrayList<>();
    rawPath.add(hdfsTblRef.getUniqueAlias());
    // Add slot refs for the partitioning columns.
    for (Column partCol : feTable.getClusteringColumns()) {
      rawPath.add(partCol.getName());
      addSlotRefToDesc(analyzer, rawPath);
      rawPath.remove(rawPath.size() - 1);
    }
    // Add slot refs for the ACID fields that identify rows.
    rawPath.add("row__id");
    String[] acidFields = {"originaltransaction", "bucket", "rowid"};
    for (String acidField : acidFields) {
      rawPath.add(acidField);
      addSlotRefToDesc(analyzer, rawPath);
      rawPath.remove(rawPath.size() - 1);
    }
  }

  /**
   * Adds a new slot ref with path 'rawPath' to its tuple descriptor. This is a no-op if
   * the tuple descriptor already has a slot ref with the given raw path. Returns the slot
   * descriptor (new or already existing) for 'rawPath'.
   */
  public static SlotDescriptor addSlotRefToDesc(Analyzer analyzer, List<String> rawPath)
      throws AnalysisException {
    Path resolvedPath = null;
    try {
      resolvedPath = analyzer.resolvePath(rawPath, PathType.SLOT_REF);
    } catch (TableLoadingException e) {
      // Should never happen because we only check registered table aliases.
      Preconditions.checkState(false);
    }
    Preconditions.checkNotNull(resolvedPath);
    SlotDescriptor desc = analyzer.registerSlotRef(resolvedPath);
    desc.setIsMaterialized(true);
    return desc;
  }

  /**
   * Takes an 'hdfsTblRef' of an ACID table and creates two scan nodes for it. One for
   * the insert delta files, and one for the delete files. On top of the two scans it
   * adds a LEFT ANTI HASH JOIN with BROADCAST distribution mode. I.e. delete events will
   * be broadcasted to the nodes that scan the insert files.
   */
  private PlanNode createAcidJoinNode(Analyzer analyzer, TableRef hdfsTblRef,
      List<Expr> conjuncts, List<? extends FeFsPartition> partitions,
      List<Expr> partConjuncts)
      throws ImpalaException {
    FeTable feTable = hdfsTblRef.getTable();
    Preconditions.checkState(AcidUtils.isFullAcidTable(
      feTable.getMetaStoreTable().getParameters()));

    // Let's create separate partitions for inserts and deletes.
    List<FeFsPartition> insertDeltaPartitions = new ArrayList<>();
    List<FeFsPartition> deleteDeltaPartitions = new ArrayList<>();
    for (FeFsPartition part : partitions) {
      insertDeltaPartitions.add(part.genInsertDeltaPartition());
      FeFsPartition deleteDeltaPartition = part.genDeleteDeltaPartition();
      if (deleteDeltaPartition != null) deleteDeltaPartitions.add(deleteDeltaPartition);
    }
    // The followings just create separate scan nodes for insert deltas and delete deltas,
    // plus adds a LEFT ANTI HASH JOIN above them.
    TableRef deleteDeltaRef = TableRef.newTableRef(analyzer, hdfsTblRef.getPath(),
        hdfsTblRef.getUniqueAlias() + "-delete-delta");
    addAcidSlots(analyzer, deleteDeltaRef);
    HdfsScanNode deltaScanNode = new HdfsScanNode(ctx_.getNextNodeId(),
        hdfsTblRef.getDesc(), conjuncts, insertDeltaPartitions, hdfsTblRef,
        /*aggInfo=*/null, partConjuncts, /*isPartitionKeyScan=*/false);
    deltaScanNode.init(analyzer);
    HdfsScanNode deleteDeltaScanNode = new HdfsScanNode(ctx_.getNextNodeId(),
        deleteDeltaRef.getDesc(), Collections.emptyList(), deleteDeltaPartitions,
        deleteDeltaRef, /*aggInfo=*/null, partConjuncts, /*isPartitionKeyScan=*/false);
    deleteDeltaScanNode.init(analyzer);
    //TODO: ACID join conjuncts currently contain predicates for all partitioning columns
    // and the ACID fields. So all of those columns will be the inputs of the hash
    // function in the HASH JOIN. Probably we could only include 'originalTransaction' and
    // 'rowid' in the hash predicates, while passing the other conjuncts in
    // 'otherJoinConjuncts'.
    List<BinaryPredicate> acidJoinConjuncts = createAcidJoinConjuncts(analyzer,
        hdfsTblRef.getDesc(), deleteDeltaRef.getDesc());
    JoinNode acidJoin = new HashJoinNode(deltaScanNode, deleteDeltaScanNode,
        /*straight_join=*/true, DistributionMode.BROADCAST, JoinOperator.LEFT_ANTI_JOIN,
        acidJoinConjuncts, /*otherJoinConjuncts=*/Collections.emptyList());
    acidJoin.setId(ctx_.getNextNodeId());
    acidJoin.init(analyzer);
    acidJoin.setIsDeleteRowsJoin();
    return acidJoin;
  }

  /**
   * Creates conjuncts used in the ANTI-JOIN between insert deltas and delete
   * deltas. I.e. it adds equality predicates for the partitioning columns and
   * ACID columns. E.g. [insertDelta.part = deleteDelta.part,
   * insertDelta.row__id.rowid = deleteDelta.row__id.rowid, ...]
   * Purposefully made 'public' for third party usage.
   *
   * @param insertTupleDesc Tuple descriptor of the insert delta scan node
   * @param deleteTupleDesc Tuple descriptor of the delete delta scan node
   */
  public static List<BinaryPredicate> createAcidJoinConjuncts(Analyzer analyzer,
      TupleDescriptor insertTupleDesc, TupleDescriptor deleteTupleDesc)
      throws AnalysisException {
    List<BinaryPredicate> ret = new ArrayList<>();
    // 'deleteTupleDesc' only has slot descriptors for the slots needed in the JOIN, i.e.
    // it only has slot refs for the partitioning columns and ACID columns. Therefore we
    // can just iterate over it and find the corresponding slot refs in the insert tuple
    // descriptor and create an equality predicate between the slot ref pairs.
    for (SlotDescriptor deleteSlotDesc : deleteTupleDesc.getSlots()) {
      boolean foundMatch = false;
      for (SlotDescriptor insertSlotDesc : insertTupleDesc.getSlots()) {
        if (deleteSlotDesc.getMaterializedPath().equals(
            insertSlotDesc.getMaterializedPath())) {
          foundMatch = true;
          BinaryPredicate pred = new BinaryPredicate(
              Operator.EQ, new SlotRef(insertSlotDesc), new SlotRef(deleteSlotDesc));
          pred.analyze(analyzer);
          ret.add(pred);
          break;
        }
      }
      Preconditions.checkState(foundMatch);
    }
    return ret;
  }

  /**
   * Looks for a filesystem-based partition in 'partitions' with no DATE support and
   * returns the first one it finds. Right now, scanning DATE values is supported for
   * TEXT, PARQUET, AVRO and ORC fileformats.
   *
   * Returns null otherwise.
   */
  private FeFsPartition findUnsupportedDateFsPartition(
      List<? extends FeFsPartition> partitions) {
    for (FeFsPartition part: partitions) {
      HdfsFileFormat ff = part.getFileFormat();
      if (!ff.isDateTypeSupported()) return part;
    }
    return null;
  }

  /**
   * Create node for scanning all data files of a particular table.
   *
   * The given 'aggInfo' is used for detecting and applying optimizations that span both
   * the scan and aggregation. Only applicable to HDFS and Kudu table refs.
   *
   * Throws if a PlanNode.init() failed or if planning of the given
   * table ref is not implemented.
   */
  private PlanNode createScanNode(TableRef tblRef, MultiAggregateInfo aggInfo,
      Analyzer analyzer) throws ImpalaException {
    ScanNode scanNode = null;

    // Get all predicates bound by the tuple.
    List<Expr> conjuncts = new ArrayList<>();
    TupleId tid = tblRef.getId();
    conjuncts.addAll(analyzer.getBoundPredicates(tid));

    // Also add remaining unassigned conjuncts
    List<Expr> unassigned = analyzer.getUnassignedConjuncts(tid.asList());
    PlanNode.removeZippingUnnestConjuncts(unassigned, analyzer);

    conjuncts.addAll(unassigned);
    analyzer.markConjunctsAssigned(unassigned);
    analyzer.createEquivConjuncts(tid, conjuncts);

    // Perform constant propagation and optimization if rewriting is enabled
    if (analyzer.getQueryCtx().client_request.query_options.enable_expr_rewrites) {
      if (!Expr.optimizeConjuncts(conjuncts, analyzer)) {
        // Conjuncts implied false; convert to EmptySetNode
        EmptySetNode node = new EmptySetNode(ctx_.getNextNodeId(), tid.asList());
        node.init(analyzer);
        return node;
      }
    } else {
      Expr.removeDuplicates(conjuncts);
    }

    FeTable table = tblRef.getTable();
    if (table instanceof FeFsTable) {
      if (table instanceof FeIcebergTable) {
        IcebergScanPlanner icebergPlanner = new IcebergScanPlanner(analyzer, ctx_, tblRef,
            conjuncts, aggInfo);
        return icebergPlanner.createIcebergScanPlan();
      }
      return createHdfsScanPlan(tblRef, aggInfo, conjuncts, analyzer);
    } else if (table instanceof FeDataSourceTable) {
      scanNode = new DataSourceScanNode(ctx_.getNextNodeId(), tblRef.getDesc(),
          conjuncts);
      scanNode.init(analyzer);
      return scanNode;
    } else if (table instanceof FeHBaseTable) {
      // HBase table
      scanNode = new HBaseScanNode(ctx_.getNextNodeId(), tblRef.getDesc());
      scanNode.addConjuncts(conjuncts);
      scanNode.init(analyzer);
      return scanNode;
    } else if (table instanceof FeKuduTable) {
      scanNode = new KuduScanNode(ctx_.getNextNodeId(), tblRef.getDesc(), conjuncts,
          aggInfo, tblRef);
      scanNode.init(analyzer);
      return scanNode;
    } else if (table instanceof IcebergMetadataTable) {
      return createIcebergMetadataScanNode(tblRef, conjuncts, analyzer);
    } else if (table instanceof FeSystemTable) {
      scanNode = new SystemTableScanNode(ctx_.getNextNodeId(), tblRef.getDesc());
      scanNode.addConjuncts(conjuncts);
      scanNode.init(analyzer);
      return scanNode;
    } else {
      throw new NotImplementedException(
          "Planning not implemented for table class: " + table.getClass());
    }
  }

  private PlanNode createIcebergMetadataScanNode(TableRef tblRef, List<Expr> conjuncts,
      Analyzer analyzer) throws ImpalaException {
    IcebergMetadataScanNode icebergMetadataScanNode =
        new IcebergMetadataScanNode(ctx_.getNextNodeId(), conjuncts, tblRef);
    icebergMetadataScanNode.init(analyzer);
    return icebergMetadataScanNode;
  }

  /**
   * Returns all applicable conjuncts for join between two plan trees 'materializing' the
   * given left-hand and right-hand side table ref ids. The conjuncts either come from
   * the analyzer or are generated based on equivalence classes, if necessary. The
   * returned conjuncts are marked as assigned.
   * The conjuncts can be used for hash table lookups.
   * - for inner joins, those are equi-join predicates in which one side is fully bound
   *   by lhsTblRefIds and the other by rhsTblRefIds
   * - for outer joins: same type of conjuncts as inner joins, but only from the
   *   ON or USING clause
   * Predicates that are redundant based on equivalence classes are intentionally
   * returned by this function because the removal of redundant predicates and the
   * creation of new predicates for enforcing slot equivalences go hand-in-hand
   * (see analyzer.createEquivConjuncts()).
   */
  private List<BinaryPredicate> getHashLookupJoinConjuncts(
      List<TupleId> lhsTblRefIds, List<TupleId> rhsTblRefIds, Analyzer analyzer) {
    List<BinaryPredicate> result = new ArrayList<>();
    List<Expr> candidates = analyzer.getEqJoinConjuncts(lhsTblRefIds, rhsTblRefIds);
    Preconditions.checkNotNull(candidates);
    for (Expr e: candidates) {
      if (!(e instanceof BinaryPredicate)) continue;
      BinaryPredicate normalizedJoinConjunct =
          getNormalizedEqPred(e, lhsTblRefIds, rhsTblRefIds, analyzer);
      if (normalizedJoinConjunct == null) continue;
      analyzer.markConjunctAssigned(e);
      result.add(normalizedJoinConjunct);
    }
    if (!result.isEmpty()) return result;

    // Construct join conjuncts derived from equivalence class membership.
    Set<TupleId> lhsTblRefIdsHs = new HashSet<>(lhsTblRefIds);
    for (TupleId rhsId: rhsTblRefIds) {
      TableRef rhsTblRef = analyzer.getTableRef(rhsId);
      Preconditions.checkNotNull(rhsTblRef);
      for (SlotDescriptor slotDesc: rhsTblRef.getDesc().getSlots()) {
        SlotId rhsSid = slotDesc.getId();
        for (SlotId lhsSid : analyzer.getEquivClass(rhsSid)) {
          if (lhsTblRefIdsHs.contains(analyzer.getTupleId(lhsSid))) {
            result.add(analyzer.createInferredEqPred(lhsSid, rhsSid));
            break;
          }
        }
      }
    }
    return result;
  }

  /**
   * Returns a normalized version of a binary equality predicate 'expr' where the lhs
   * child expr is bound by some tuple in 'lhsTids' and the rhs child expr is bound by
   * some tuple in 'rhsTids'. Returns 'expr' if this predicate is already normalized.
   * Returns null in any of the following cases:
   * 1. It is not an equality predicate
   * 2. One of the operands is a constant
   * 3. Both children of this predicate are the same expr
   * 4. Cannot be normalized
   */
  public static BinaryPredicate getNormalizedEqPred(Expr expr, List<TupleId> lhsTids,
      List<TupleId> rhsTids, Analyzer analyzer) {
    if (!(expr instanceof BinaryPredicate)) return null;
    BinaryPredicate pred = (BinaryPredicate) expr;
    if (!pred.getOp().isEquivalence() && pred.getOp() != Operator.NULL_MATCHING_EQ) {
      return null;
    }
    if (pred.getChild(0).isConstant() || pred.getChild(1).isConstant()) return null;

    Expr lhsExpr = Expr.getFirstBoundChild(pred, lhsTids);
    Expr rhsExpr = Expr.getFirstBoundChild(pred, rhsTids);
    if (lhsExpr == null || rhsExpr == null || lhsExpr == rhsExpr) return null;

    BinaryPredicate result = new BinaryPredicate(pred.getOp(), lhsExpr, rhsExpr);
    result.analyzeNoThrow(analyzer);
    return result;
  }

  /**
   * Similar to getNormalizedEqPred(), except returns a normalized version of a binary
   * single range predicate 'expr'. A single range predicate is defined as one with
   * pred.getOp() being =, <, <=, > or >=.
   */
  public static BinaryPredicate getNormalizedSingleRangePred(
      Expr expr, List<TupleId> lhsTids, List<TupleId> rhsTids, Analyzer analyzer) {
    if (!(expr instanceof BinaryPredicate)) return null;
    BinaryPredicate pred = (BinaryPredicate) expr;
    if (!pred.getOp().isSingleRange()) return null;

    if (pred.getChild(0).isConstant() || pred.getChild(1).isConstant()) return null;

    Expr lhsExpr = Expr.getFirstBoundChild(pred, lhsTids);
    Expr rhsExpr = Expr.getFirstBoundChild(pred, rhsTids);
    if (lhsExpr == null || rhsExpr == null || lhsExpr == rhsExpr) return null;

    BinaryPredicate result = new BinaryPredicate(pred.getOp(), lhsExpr, rhsExpr);
    result.analyzeNoThrow(analyzer);
    return result;
  }

  /**
   * Creates a new node to join outer with inner. Collects and assigns join conjunct
   * as well as regular conjuncts. Calls init() on the new join node.
   * Throws if the JoinNode.init() fails.
   */
  private PlanNode createJoinNode(PlanNode outer, PlanNode inner,
      TableRef innerRef, Analyzer analyzer) throws ImpalaException {
    // get eq join predicates for the TableRefs' ids (not the PlanNodes' ids, which
    // are materialized)
    List<BinaryPredicate> eqJoinConjuncts = getHashLookupJoinConjuncts(
        outer.getTblRefIds(), inner.getTblRefIds(), analyzer);
    // Outer joins should only use On-clause predicates as eqJoinConjuncts.
    if (!innerRef.getJoinOp().isOuterJoin()) {
      analyzer.createEquivConjuncts(outer.getTblRefIds(), inner.getTblRefIds(),
          eqJoinConjuncts);
    }
    if (!eqJoinConjuncts.isEmpty() && innerRef.getJoinOp() == JoinOperator.CROSS_JOIN) {
      innerRef.setJoinOp(JoinOperator.INNER_JOIN);
    }

    List<Expr> otherJoinConjuncts = new ArrayList<>();
    if (innerRef.getJoinOp().isOuterJoin()) {
      // Also assign conjuncts from On clause. All remaining unassigned conjuncts
      // that can be evaluated by this join are assigned in createSelectPlan().
      otherJoinConjuncts = analyzer.getUnassignedOjConjuncts(innerRef);
    } else if (innerRef.getJoinOp().isSemiJoin()) {
      // Unassigned conjuncts bound by the invisible tuple id of a semi join must have
      // come from the join's On-clause, and therefore, must be added to the other join
      // conjuncts to produce correct results.
      // TODO This doesn't handle predicates specified in the On clause which are not
      // bound by any tuple id (e.g. ON (true))
      List<TupleId> tblRefIds = Lists.newArrayList(outer.getTblRefIds());
      tblRefIds.addAll(inner.getTblRefIds());
      otherJoinConjuncts = analyzer.getUnassignedConjuncts(tblRefIds, false);
      if (innerRef.getJoinOp().isNullAwareLeftAntiJoin()) {
        boolean hasNullMatchingEqOperator = false;
        // Keep only the null-matching eq conjunct in the eqJoinConjuncts and move
        // all the others in otherJoinConjuncts. The BE relies on this
        // separation for correct execution of the null-aware left anti join.
        Iterator<BinaryPredicate> it = eqJoinConjuncts.iterator();
        while (it.hasNext()) {
          BinaryPredicate conjunct = it.next();
          if (!conjunct.isNullMatchingEq()) {
            otherJoinConjuncts.add(conjunct);
            it.remove();
          } else {
            // Only one null-matching eq conjunct is allowed
            Preconditions.checkState(!hasNullMatchingEqOperator);
            hasNullMatchingEqOperator = true;
          }
        }
        Preconditions.checkState(hasNullMatchingEqOperator);
      }
    }
    PlanNode.removeZippingUnnestConjuncts(otherJoinConjuncts, analyzer);
    analyzer.markConjunctsAssigned(otherJoinConjuncts);

    if (analyzer.getQueryOptions().isEnable_distinct_semi_join_optimization() &&
            innerRef.getJoinOp().isLeftSemiJoin()) {
      inner =
          addDistinctToJoinInput(inner, analyzer, eqJoinConjuncts, otherJoinConjuncts);
    }

    // Use a nested-loop join if there are no equi-join conjuncts, or if the inner
    // (build side) is a singular row src. A singular row src has a cardinality of 1, so
    // a nested-loop join is certainly cheaper than a hash join.
    JoinNode result = null;
    Preconditions.checkState(!innerRef.getJoinOp().isNullAwareLeftAntiJoin()
        || !(inner instanceof SingularRowSrcNode));
    if (eqJoinConjuncts.isEmpty() || inner instanceof SingularRowSrcNode) {
      otherJoinConjuncts.addAll(eqJoinConjuncts);
      result = new NestedLoopJoinNode(outer, inner, analyzer.isStraightJoin(),
          innerRef.getDistributionMode(), innerRef.getJoinOp(), otherJoinConjuncts);
    } else {
      result = new HashJoinNode(outer, inner, analyzer.isStraightJoin(),
          innerRef.getDistributionMode(), innerRef.getJoinOp(), eqJoinConjuncts,
          otherJoinConjuncts);
    }
    result.init(analyzer);
    return result;
  }

  /**
   * Optionally add a aggregation node on top of 'joinInput' if it is cheaper to project
   * and aggregate the slots needed to evaluate the provided join conjuncts. This
   * is only safe to do if the join's results do not depend on the number of duplicate
   * values and if the join does not need to return any slots from 'joinInput'.  E.g.
   * the inner of a left semi join satisfies both of those conditions.
   * @return the original 'joinInput' or its new AggregationNode parent.
   */
  private PlanNode addDistinctToJoinInput(PlanNode joinInput, Analyzer analyzer,
          List<BinaryPredicate> eqJoinConjuncts, List<Expr> otherJoinConjuncts)
                  throws InternalException, AnalysisException {
    List<Expr> allJoinConjuncts = new ArrayList<>();
    allJoinConjuncts.addAll(eqJoinConjuncts);
    allJoinConjuncts.addAll(otherJoinConjuncts);
    allJoinConjuncts = Expr.substituteList(
            allJoinConjuncts, joinInput.getOutputSmap(), analyzer, true);

    // Identify the unique slots from the inner required by the join conjuncts. Since this
    // is a semi-join, the inner tuple is not returned from the join and we do not need
    // any other slots from the inner.
    List<SlotId> allSlotIds = new ArrayList<>();
    Expr.getIds(allJoinConjuncts, null, allSlotIds);
    List<TupleId> joinInputTupleIds = joinInput.getTupleIds();
    List<Expr> distinctExprs = new ArrayList<>();
    for (SlotDescriptor slot : analyzer.getSlotDescs(allSlotIds)) {
      if (joinInputTupleIds.contains(slot.getParent().getId())) {
        distinctExprs.add(new SlotRef(slot));
      }
    }

    // If there are no join predicates, this can be more efficiently handled by
    // inserting a limit in the plan (since the first row returned from 'joinInput'
    // will satisfy the join predicates).
    if (distinctExprs.isEmpty()) {
      joinInput.setLimit(1);
      return joinInput;
    }
    long numDistinct = AggregationNode.estimateNumGroups(distinctExprs,
            joinInput.getCardinality());
    if (numDistinct < 0 || joinInput.getCardinality() < 0) {
      // Default to not adding the aggregation if stats are missing.
      LOG.trace("addDistinctToJoinInput():: missing stats, will not add agg");
      return joinInput;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("addDistinctToJoinInput(): " + "numDistinct=" + numDistinct +
              " inputCardinality=" + joinInput.getCardinality());
    }

    // Check to see if an aggregation would reduce input by enough to justify inserting
    // it. We factor in the average row size to account for the aggregate projecting
    // out slots. The agg would be ineffective if the input already have 0 or 1 rows.
    if (joinInput.getCardinality() <= 1 ||
        numDistinct > JOIN_DISTINCT_THRESHOLD * joinInput.getCardinality()) {
      return joinInput;
    }

    // Set up an aggregation node to return only distinct slots.
    MultiAggregateInfo distinctAggInfo =
         new MultiAggregateInfo(distinctExprs, Collections.emptyList(), null);
    distinctAggInfo.analyze(analyzer);
    distinctAggInfo.materializeRequiredSlots(analyzer, new ExprSubstitutionMap());
    AggregationNode agg = new AggregationNode(
            ctx_.getNextNodeId(), joinInput, distinctAggInfo, AggPhase.FIRST);
    agg.init(analyzer);
    // Mark the agg as materializing the same table ref. This is required so that other
    // parts of planning, e.g. subplan generation, know that this plan tree materialized
    // the table ref.
    agg.setTblRefIds(joinInput.getTblRefIds());
    // All references to the input slots in join conjuncts must be replaced with
    // references to aggregate slots. The output smap from the aggregate info contains
    // these mappings, so we can add it to the output smap of the agg to ensure that
    // join conjuncts get replaced correctly.
    agg.setOutputSmap(ExprSubstitutionMap.compose(
            agg.getOutputSmap(), distinctAggInfo.getOutputSmap(), analyzer));

    // Add value transfers between original slots and aggregate tuple so that runtime
    // filters can be pushed through the aggregation. We can defer updating the
    // value transfer graph until after the single node plan is constructed because
    // a precondition of calling this function is that the join does not return any
    // of the slots from this plan tree.
    for (int i = 0; i < distinctExprs.size(); ++i) {
      Expr distinctExpr = distinctExprs.get(i);
      SlotDescriptor outputSlot =
              distinctAggInfo.getAggClass(0).getResultTupleDesc().getSlots().get(i);
      analyzer.registerValueTransfer(
              ((SlotRef)distinctExpr).getSlotId(), outputSlot.getId());
      valueTransferGraphNeedsUpdate_ = true;
    }
    return agg;
  }

  /**
   * Create a tree of PlanNodes for the given tblRef, which can be a BaseTableRef,
   * CollectionTableRef or an InlineViewRef.
   *
   * The given 'aggInfo' is used for detecting and applying optimizations that span both
   * the scan and aggregation. Only applicable to HDFS and Kudu table refs.
   *
   * 'collectionRefs' holds all the CollectionTableRefs that serve the purpose of zipping
   * unnest arrays. Unlike the regular CollectionTableRefs, these will be handled by a
   * single UnnestNode.
   *
   * Throws if a PlanNode.init() failed or if planning of the given
   * table ref is not implemented.
   */
  private PlanNode createTableRefNode(TableRef tblRef, MultiAggregateInfo aggInfo,
      Analyzer analyzer, List<CollectionTableRef> collectionRefsToZip)
      throws ImpalaException {
    PlanNode result = null;
    if (tblRef instanceof BaseTableRef) {
      result = createScanNode(tblRef, aggInfo, analyzer);
    } else if (tblRef instanceof CollectionTableRef) {
      if (tblRef.isRelative()) {
        Preconditions.checkState(ctx_.hasSubplan());
        if (collectionRefsToZip != null && collectionRefsToZip.size() > 0) {
          result = new UnnestNode(ctx_.getNextNodeId(), ctx_.getSubplan(),
              collectionRefsToZip);
        } else {
          result = new UnnestNode(ctx_.getNextNodeId(), ctx_.getSubplan(),
              (CollectionTableRef) tblRef);
        }
        result.init(analyzer);
      } else {
        result = createScanNode(tblRef, null, analyzer);
      }
    } else if (tblRef instanceof InlineViewRef) {
      result = createInlineViewPlan(analyzer, (InlineViewRef) tblRef);
    } else if (tblRef instanceof SingularRowSrcTableRef) {
      Preconditions.checkState(ctx_.hasSubplan());
      result = new SingularRowSrcNode(ctx_.getNextNodeId(), ctx_.getSubplan());
      result.init(analyzer);
    } else if (tblRef instanceof IcebergMetadataTableRef) {
      result = createScanNode(tblRef, aggInfo, analyzer);
    } else {
      throw new NotImplementedException(
          "Planning not implemented for table ref class: " + tblRef.getClass());
    }
    return result;
  }

  /**
   * Create a plan tree corresponding to 'unionOperands' for the given unionStmt.
   * The individual operands' plan trees are attached to a single UnionNode.
   * If unionDistinctPlan is not null, it is expected to contain the plan for the
   * distinct portion of the given unionStmt. The unionDistinctPlan is then added
   * as a child of the returned UnionNode.
   */
  private UnionNode createUnionPlan(Analyzer analyzer, UnionStmt unionStmt,
      List<SetOperand> unionOperands, PlanNode unionDistinctPlan) throws ImpalaException {
    UnionNode unionNode = new UnionNode(ctx_.getNextNodeId(), unionStmt.getTupleId(),
        unionStmt.getSetOperationResultExprs(), ctx_.hasSubplan());
    for (SetOperand op : unionOperands) {
      if (op.getAnalyzer().hasEmptyResultSet()) {
        unmarkCollectionSlots(op.getQueryStmt());
        continue;
      }
      QueryStmt queryStmt = op.getQueryStmt();
      if (queryStmt instanceof SelectStmt) {
        SelectStmt selectStmt = (SelectStmt) queryStmt;
        if (selectStmt.getTableRefs().isEmpty()) {
          unionNode.addConstExprList(selectStmt.getResultExprs());
          continue;
        }
      }
      PlanNode opPlan = createQueryPlan(queryStmt, op.getAnalyzer(), false);
      // There may still be unassigned conjuncts if the operand has an order by + limit.
      // Place them into a SelectNode on top of the operand's plan.
      opPlan = addUnassignedConjuncts(analyzer, opPlan.getTupleIds(), opPlan);
      if (opPlan instanceof EmptySetNode) continue;
      unionNode.addChild(opPlan, op.getQueryStmt().getResultExprs());
    }

    if (unionDistinctPlan != null) {
      Preconditions.checkState(unionStmt.hasUnionDistinctOps());
      Preconditions.checkState(unionDistinctPlan instanceof AggregationNode);
      unionNode.addChild(unionDistinctPlan,
          unionStmt.getDistinctAggInfo().getGroupingExprs());
    }
    unionNode.init(analyzer);
    return unionNode;
  }

  /**
   * Returns plan tree for unionStmt:
   * - distinctOperands' plan trees are collected in a single UnionNode
   *   and duplicates removed via distinct aggregation
   * - the output of that plus the allOperands' plan trees are collected in
   *   another UnionNode which materializes the result of unionStmt
   * - if any of the union operands contains analytic exprs, we avoid pushing
   *   predicates directly into the operands and instead evaluate them
   *   *after* the final UnionNode (see createInlineViewPlan() for the reasoning)
   *   TODO: optimize this by still pushing predicates into the union operands
   *   that don't contain analytic exprs and evaluating the conjuncts in Select
   *   directly above the AnalyticEvalNodes
   * TODO: Simplify the plan of unions with empty operands using an empty set node.
   * TODO: Simplify the plan of unions with only a single non-empty operand to not
   *       use a union node (this is tricky because a union materializes a new tuple).
   */
  private PlanNode createUnionPlan(UnionStmt unionStmt, Analyzer analyzer)
      throws ImpalaException {
    List<Expr> conjuncts =
        analyzer.getUnassignedConjuncts(unionStmt.getTupleId().asList(), false);
    if (!unionStmt.hasAnalyticExprs()) {
      // Turn unassigned predicates for unionStmt's tupleId_ into predicates for
      // the individual operands.
      // Do this prior to creating the operands' plan trees so they get a chance to
      // pick up propagated predicates.
      for (SetOperand op : unionStmt.getOperands()) {
        List<Expr> opConjuncts =
            Expr.substituteList(conjuncts, op.getSmap(), analyzer, false);
        op.getAnalyzer().registerConjuncts(opConjuncts);
      }
      analyzer.markConjunctsAssigned(conjuncts);
    } else {
      // mark slots referenced by the yet-unassigned conjuncts
      analyzer.materializeSlots(conjuncts);
    }
    // mark slots after predicate propagation but prior to plan tree generation
    unionStmt.materializeRequiredSlots(analyzer);

    PlanNode result = null;
    // create DISTINCT tree
    if (unionStmt.hasUnionDistinctOps()) {
      result = createUnionPlan(
          analyzer, unionStmt, unionStmt.getUnionDistinctOperands(), null);
      result = new AggregationNode(
          ctx_.getNextNodeId(), result, unionStmt.getDistinctAggInfo(), AggPhase.FIRST);
      result.init(analyzer);
    }
    // create ALL tree
    if (unionStmt.hasUnionAllOps()) {
      result =
          createUnionPlan(analyzer, unionStmt, unionStmt.getUnionAllOperands(), result);
    }

    if (unionStmt.hasAnalyticExprs()) {
      result = addUnassignedConjuncts(
          analyzer, unionStmt.getTupleId().asList(), result);
    }
    return result;
  }
}
