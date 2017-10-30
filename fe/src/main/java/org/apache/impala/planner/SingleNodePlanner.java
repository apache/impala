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
import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.CollectionTableRef;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprId;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.InlineViewRef;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.QueryStmt;
import org.apache.impala.analysis.SelectStmt;
import org.apache.impala.analysis.SingularRowSrcTableRef;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TableSampleClause;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.analysis.TupleIsNullPredicate;
import org.apache.impala.analysis.UnionStmt.UnionOperand;
import org.apache.impala.analysis.UnionStmt;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.catalog.HBaseTable;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.NotImplementedException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.RuntimeEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Constructs a non-executable single-node plan from an analyzed parse tree.
 * The single-node plan does not contain data exchanges or data-reduction optimizations
 * such as local aggregations that are important for distributed execution.
 * The single-node plan needs to be wrapped in a plan fragment for it to be executable.
 */
public class SingleNodePlanner {
  private final static Logger LOG = LoggerFactory.getLogger(SingleNodePlanner.class);

  private final PlannerContext ctx_;

  public SingleNodePlanner(PlannerContext ctx) {
    ctx_ = ctx;
  }

  /**
   * Generates and returns the root of the single-node plan for the analyzed parse tree
   * in the planner context. The planning process recursively walks the parse tree and
   * performs the following actions.
   * In the top-down phase over query statements:
   * - Materialize the slots required for evaluating expressions of that statement.
   * - Migrate conjuncts from parent blocks into inline views and union operands.
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
    analyzer.computeEquivClasses();
    ctx_.getAnalysisResult().getTimeline().markEvent("Equivalence classes computed");

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
    return singleNodePlan;
  }

  /**
   * Checks that the given single-node plan is executable:
   * - It may not contain right or full outer joins with no equi-join conjuncts that
   *   are not inside the right child of a SubplanNode.
   * - MT_DOP > 0 is not supported for plans with base table joins or table sinks.
   * Throws a NotImplementedException if plan validation fails.
   */
  public void validatePlan(PlanNode planNode) throws NotImplementedException {
    if (ctx_.getQueryOptions().isSetMt_dop() && ctx_.getQueryOptions().mt_dop > 0
        && !RuntimeEnv.INSTANCE.isTestEnv()
        && (planNode instanceof JoinNode || ctx_.hasTableSink())) {
      throw new NotImplementedException(
          "MT_DOP not supported for plans with base table joins or table sinks.");
    }

    // As long as MT_DOP is unset or 0 any join can run in a single-node plan.
    if (ctx_.isSingleNodeExec() &&
        (!ctx_.getQueryOptions().isSetMt_dop() || ctx_.getQueryOptions().mt_dop == 0)) {
      return;
    }

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
      validatePlan(planNode.getChild(0));
    } else {
      for (PlanNode child: planNode.getChildren()) {
        validatePlan(child);
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
    ArrayList<TupleId> tupleIds = Lists.newArrayList();
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
    // Not needed for a UnionStmt because it materializes its input operands.
    if (stmt instanceof SelectStmt) {
      node.setOutputSmap(((SelectStmt) stmt).getBaseTblSmap());
    }
    return node;
  }

  /**
   * Mark all collection-typed slots in stmt as non-materialized.
   */
  private void unmarkCollectionSlots(QueryStmt stmt) {
    List<TableRef> tblRefs = Lists.newArrayList();
    stmt.collectTableRefs(tblRefs);
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
        List<Expr> inputPartitionExprs = Lists.newArrayList();
        AggregateInfo aggInfo = selectStmt.getAggInfo();
        root = analyticPlanner.createSingleNodePlan(root,
            aggInfo != null ? aggInfo.getGroupingExprs() : null, inputPartitionExprs);
        if (aggInfo != null && !inputPartitionExprs.isEmpty()) {
          // analytic computation will benefit from a partition on inputPartitionExprs
          aggInfo.setPartitionExprs(inputPartitionExprs);
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
      long limit = stmt.getLimit();
      // TODO: External sort could be used for very large limits
      // not just unlimited order-by
      boolean useTopN = stmt.hasLimit() && !disableTopN;
      if (useTopN) {
        root = SortNode.createTopNSortNode(
            ctx_.getNextNodeId(), root, stmt.getSortInfo(), stmt.getOffset());
      } else {
        root = SortNode.createTotalSortNode(
            ctx_.getNextNodeId(), root, stmt.getSortInfo(), stmt.getOffset());
      }
      Preconditions.checkState(root.hasValidStats());
      root.setLimit(limit);
      root.init(analyzer);
    } else {
      root.setLimit(stmt.getLimit());
      root.computeStats(analyzer);
    }

    return root;
  }

  /**
   * If there are unassigned conjuncts that are bound by tupleIds or if there are slot
   * equivalences for tupleIds that have not yet been enforced, returns a SelectNode on
   * top of root that evaluates those conjuncts; otherwise returns root unchanged.
   * TODO: change this to assign the unassigned conjuncts to root itself, if that is
   * semantically correct
   */
  private PlanNode addUnassignedConjuncts(
      Analyzer analyzer, List<TupleId> tupleIds, PlanNode root) {
    // No point in adding SelectNode on top of an EmptyNode.
    if (root instanceof EmptySetNode) return root;
    Preconditions.checkNotNull(root);
    // Gather unassigned conjuncts and generate predicates to enfore
    // slot equivalences for each tuple id.
    List<Expr> conjuncts = analyzer.getUnassignedConjuncts(root);
    for (TupleId tid: tupleIds) {
      analyzer.createEquivConjuncts(tid, conjuncts);
    }
    if (conjuncts.isEmpty()) return root;
    // evaluate conjuncts in SelectNode
    SelectNode selectNode = new SelectNode(ctx_.getNextNodeId(), root, conjuncts);
    // init() marks conjuncts as assigned
    selectNode.init(analyzer);
    Preconditions.checkState(selectNode.hasValidStats());
    return selectNode;
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
    ArrayList<Pair<TableRef, Long>> candidates = Lists.newArrayList();
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
        candidates.add(new Pair(ref, new Long(0)));
        if (LOG.isTraceEnabled()) {
          LOG.trace("candidate " + ref.getUniqueAlias() + ": 0");
        }
        continue;
      }
      Preconditions.checkState(ref.isAnalyzed());
      long materializedSize =
          (long) Math.ceil(plan.getAvgRowSize() * (double) plan.getCardinality());
      candidates.add(new Pair(ref, new Long(materializedSize)));
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
    List<Pair<TableRef, PlanNode>> remainingRefs = Lists.newArrayList();
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
    Map<TableRef, Set<TableRef>> precedingRefs = Maps.newHashMap();
    List<TableRef> tmpTblRefs = Lists.newArrayList();
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

    ArrayList<TupleId> rowTuples = Lists.newArrayList();
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

    // Separate table refs into parent refs (uncorrelated or absolute) and
    // subplan refs (correlated or relative), and generate their plan.
    List<TableRef> parentRefs = Lists.newArrayList();
    List<SubplanRef> subplanRefs = Lists.newArrayList();
    computeParentAndSubplanRefs(
        selectStmt.getTableRefs(), analyzer.isStraightJoin(), parentRefs, subplanRefs);
    AggregateInfo aggInfo = selectStmt.getAggInfo();
    PlanNode root = createTableRefsPlan(parentRefs, subplanRefs, aggInfo, analyzer);
    // add aggregation, if any
    if (aggInfo != null) {
      if (root instanceof HdfsScanNode) {
        aggInfo.substitute(((HdfsScanNode) root).getOptimizedAggSmap(), analyzer);
        aggInfo.getMergeAggInfo().substitute(((HdfsScanNode) root).getOptimizedAggSmap(), analyzer);
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
    List<TupleId> planTblRefIds = Lists.newArrayList();

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
        List<TupleId> requiredTids = Lists.newArrayList();
        List<TupleId> requiredTblRefIds = Lists.newArrayList();
        if (ref.isCorrelated()) {
          requiredTids.addAll(ref.getCorrelatedTupleIds());
        } else {
          CollectionTableRef collectionTableRef = (CollectionTableRef) ref;
          requiredTids.add(collectionTableRef.getResolvedPath().getRootDesc().getId());
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
  }

  /**
   * Returns a plan tree for evaluating the given parentRefs and subplanRefs.
   */
  private PlanNode createTableRefsPlan(List<TableRef> parentRefs,
      List<SubplanRef> subplanRefs, AggregateInfo aggInfo, Analyzer analyzer)
      throws ImpalaException {
    // create plans for our table refs; use a list here instead of a map to
    // maintain a deterministic order of traversing the TableRefs during join
    // plan generation (helps with tests)
    List<Pair<TableRef, PlanNode>> parentRefPlans = Lists.newArrayList();
    for (TableRef ref: parentRefs) {
      PlanNode root = createTableRefNode(ref, aggInfo, analyzer);
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
    List<TableRef> result = Lists.newArrayList();
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
   * Returns a new AggregationNode that materializes the aggregation of the given stmt.
   * Assigns conjuncts from the Having clause to the returned node.
   */
  private PlanNode createAggregationPlan(SelectStmt selectStmt, Analyzer analyzer,
      PlanNode root) throws ImpalaException {
    Preconditions.checkState(selectStmt.getAggInfo() != null);
    // add aggregation, if required
    AggregateInfo aggInfo = selectStmt.getAggInfo();
    root = new AggregationNode(ctx_.getNextNodeId(), root, aggInfo);
    root.init(analyzer);
    Preconditions.checkState(root.hasValidStats());
    // if we're computing DISTINCT agg fns, the analyzer already created the
    // 2nd phase agginfo
    if (aggInfo.isDistinctAgg()) {
      ((AggregationNode)root).unsetNeedsFinalize();
      // The output of the 1st phase agg is the 1st phase intermediate.
      ((AggregationNode)root).setIntermediateTuple();
      root = new AggregationNode(ctx_.getNextNodeId(), root,
          aggInfo.getSecondPhaseDistinctAggInfo());
      root.init(analyzer);
      Preconditions.checkState(root.hasValidStats());
    }
    // add Having clause
    root.assignConjuncts(analyzer);
    return root;
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
   * Transform '=', '<[=]' and '>[=]' comparisons for given slot into
   * ValueRange. Also removes those predicates which were used for the construction
   * of ValueRange from 'conjuncts_'. Only looks at comparisons w/ string constants
   * (ie, the bounds of the result can be evaluated with Expr::GetValue(NULL)).
   * HBase row key filtering works only if the row key is mapped to a string column and
   * the expression is a string constant expression.
   * If there are multiple competing comparison predicates that could be used
   * to construct a ValueRange, only the first one from each category is chosen.
   */
  private ValueRange createHBaseValueRange(SlotDescriptor d, List<Expr> conjuncts) {
    ListIterator<Expr> i = conjuncts.listIterator();
    ValueRange result = null;
    while (i.hasNext()) {
      Expr e = i.next();
      if (!(e instanceof BinaryPredicate)) continue;
      BinaryPredicate comp = (BinaryPredicate) e;
      if ((comp.getOp() == BinaryPredicate.Operator.NE)
          || (comp.getOp() == BinaryPredicate.Operator.DISTINCT_FROM)
          || (comp.getOp() == BinaryPredicate.Operator.NOT_DISTINCT)) {
        continue;
      }
      Expr slotBinding = comp.getSlotBinding(d.getId());
      if (slotBinding == null || !slotBinding.isConstant() ||
          !slotBinding.getType().equals(Type.STRING)) {
        continue;
      }

      if (comp.getOp() == BinaryPredicate.Operator.EQ) {
        i.remove();
        return ValueRange.createEqRange(slotBinding);
      }

      if (result == null) result = new ValueRange();

      // TODO: do we need copies here?
      if (comp.getOp() == BinaryPredicate.Operator.GT
          || comp.getOp() == BinaryPredicate.Operator.GE) {
        if (result.getLowerBound() == null) {
          result.setLowerBound(slotBinding);
          result.setLowerBoundInclusive(comp.getOp() == BinaryPredicate.Operator.GE);
          i.remove();
        }
      } else {
        if (result.getUpperBound() == null) {
          result.setUpperBound(slotBinding);
          result.setUpperBoundInclusive(comp.getOp() == BinaryPredicate.Operator.LE);
          i.remove();
        }
      }
    }
    return result;
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
    return rootNode;
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
  public void migrateConjunctsToInlineView(Analyzer analyzer,
      InlineViewRef inlineViewRef) throws ImpalaException {
    List<Expr> unassignedConjuncts =
        analyzer.getUnassignedConjuncts(inlineViewRef.getId().asList(), true);
    if (!canMigrateConjuncts(inlineViewRef)) {
      // mark (fully resolve) slots referenced by unassigned conjuncts as
      // materialized
      List<Expr> substUnassigned = Expr.substituteList(unassignedConjuncts,
          inlineViewRef.getBaseTblSmap(), analyzer, false);
      analyzer.materializeSlots(substUnassigned);
      return;
    }

    List<Expr> preds = Lists.newArrayList();
    for (Expr e: unassignedConjuncts) {
      if (analyzer.canEvalPredicate(inlineViewRef.getId().asList(), e)) {
        preds.add(e);
      }
    }
    unassignedConjuncts.removeAll(preds);
    // Generate predicates to enforce equivalences among slots of the inline view
    // tuple. These predicates are also migrated into the inline view.
    analyzer.createEquivConjuncts(inlineViewRef.getId(), preds);

    // create new predicates against the inline view's unresolved result exprs, not
    // the resolved result exprs, in order to avoid skipping scopes (and ignoring
    // limit clauses on the way)
    List<Expr> viewPredicates =
        Expr.substituteList(preds, inlineViewRef.getSmap(), analyzer, false);

    // Remove unregistered predicates that reference the same slot on
    // both sides (e.g. a = a). Such predicates have been generated from slot
    // equivalences and may incorrectly reject rows with nulls (IMPALA-1412/IMPALA-2643).
    Predicate<Expr> isIdentityPredicate = new Predicate<Expr>() {
      @Override
      public boolean apply(Expr expr) {
        return org.apache.impala.analysis.Predicate.isEquivalencePredicate(expr)
            && ((BinaryPredicate) expr).isInferred()
            && expr.getChild(0).equals(expr.getChild(1));
      }
    };
    Iterables.removeIf(viewPredicates, isIdentityPredicate);

    // Migrate the conjuncts by marking the original ones as assigned, and
    // re-registering the substituted ones with new ids.
    analyzer.markConjunctsAssigned(preds);
    // Unset the On-clause flag of the migrated conjuncts because the migrated conjuncts
    // apply to the post-join/agg/analytic result of the inline view.
    for (Expr e: viewPredicates) e.setIsOnClauseConjunct(false);
    inlineViewRef.getAnalyzer().registerConjuncts(viewPredicates);

    // mark (fully resolve) slots referenced by remaining unassigned conjuncts as
    // materialized
    List<Expr> substUnassigned = Expr.substituteList(unassignedConjuncts,
        inlineViewRef.getBaseTblSmap(), analyzer, false);
    analyzer.materializeSlots(substUnassigned);
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
  private PlanNode createHdfsScanPlan(TableRef hdfsTblRef, AggregateInfo aggInfo,
      List<Expr> conjuncts, Analyzer analyzer) throws ImpalaException {
    TupleDescriptor tupleDesc = hdfsTblRef.getDesc();

    // Do partition pruning before deciding which slots to materialize because we might
    // end up removing some predicates.
    HdfsPartitionPruner pruner = new HdfsPartitionPruner(tupleDesc);
    List<HdfsPartition> partitions = pruner.prunePartitions(analyzer, conjuncts, false);

    // Mark all slots referenced by the remaining conjuncts as materialized.
    analyzer.materializeSlots(conjuncts);

    // For queries which contain partition columns only, we may use the metadata instead
    // of table scans. This is only feasible if all materialized aggregate expressions
    // have distinct semantics. Please see createHdfsScanPlan() for details.
    boolean fastPartitionKeyScans =
        analyzer.getQueryCtx().client_request.query_options.optimize_partition_key_scans &&
        aggInfo != null && aggInfo.hasAllDistinctAgg();

    // If the optimization for partition key scans with metadata is enabled,
    // try evaluating with metadata first. If not, fall back to scanning.
    if (fastPartitionKeyScans && tupleDesc.hasClusteringColsOnly()) {
      HashSet<List<Expr>> uniqueExprs = new HashSet<List<Expr>>();

      for (HdfsPartition partition: partitions) {
        // Ignore empty partitions to match the behavior of the scan based approach.
        if (partition.isDefaultPartition() || partition.getSize() == 0) {
          continue;
        }
        List<Expr> exprs = Lists.newArrayList();
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
    } else {
      ScanNode scanNode =
          new HdfsScanNode(ctx_.getNextNodeId(), tupleDesc, conjuncts, partitions,
              hdfsTblRef, aggInfo);
      scanNode.init(analyzer);
      return scanNode;
    }
  }

  /**
   * Create node for scanning all data files of a particular table.
   *
   * The given 'aggInfo' is used for detecting and applying optimizations that span both
   * the scan and aggregation. Only applicable to HDFS table refs.
   *
   * Throws if a PlanNode.init() failed or if planning of the given
   * table ref is not implemented.
   */
  private PlanNode createScanNode(TableRef tblRef, AggregateInfo aggInfo,
      Analyzer analyzer) throws ImpalaException {
    ScanNode scanNode = null;

    // Get all predicates bound by the tuple.
    List<Expr> conjuncts = Lists.newArrayList();
    TupleId tid = tblRef.getId();
    conjuncts.addAll(analyzer.getBoundPredicates(tid));

    // Also add remaining unassigned conjuncts
    List<Expr> unassigned = analyzer.getUnassignedConjuncts(tid.asList());
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

    Table table = tblRef.getTable();
    if (table instanceof HdfsTable) {
      return createHdfsScanPlan(tblRef, aggInfo, conjuncts, analyzer);
    } else if (table instanceof DataSourceTable) {
      scanNode = new DataSourceScanNode(ctx_.getNextNodeId(), tblRef.getDesc(),
          conjuncts);
      scanNode.init(analyzer);
      return scanNode;
    } else if (table instanceof HBaseTable) {
      // HBase table
      scanNode = new HBaseScanNode(ctx_.getNextNodeId(), tblRef.getDesc());
    } else if (tblRef.getTable() instanceof KuduTable) {
      scanNode = new KuduScanNode(ctx_.getNextNodeId(), tblRef.getDesc(), conjuncts);
      scanNode.init(analyzer);
      return scanNode;
    } else {
      throw new NotImplementedException(
          "Planning not implemented for table ref class: " + tblRef.getClass());
    }
    // TODO: move this to HBaseScanNode.init();
    Preconditions.checkState(scanNode instanceof HBaseScanNode);
    List<ValueRange> keyRanges = Lists.newArrayList();
    // determine scan predicates for clustering cols
    for (int i = 0; i < tblRef.getTable().getNumClusteringCols(); ++i) {
      SlotDescriptor slotDesc = analyzer.getColumnSlot(
          tblRef.getDesc(), tblRef.getTable().getColumns().get(i));
      if (slotDesc == null || !slotDesc.getType().isStringType()) {
        // the hbase row key is mapped to a non-string type
        // (since it's stored in ascii it will be lexicographically ordered,
        // and non-string comparisons won't work)
        keyRanges.add(null);
      } else {
        // create ValueRange from conjuncts_ for slot; also removes conjuncts_ that were
        // used as input for filter
        keyRanges.add(createHBaseValueRange(slotDesc, conjuncts));
      }
    }

    ((HBaseScanNode)scanNode).setKeyRanges(keyRanges);
    scanNode.addConjuncts(conjuncts);
    scanNode.init(analyzer);

    return scanNode;
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
   * returneded by this function because the removal of redundant predicates and the
   * creation of new predicates for enforcing slot equivalences go hand-in-hand
   * (see analyzer.createEquivConjuncts()).
   */
  private List<BinaryPredicate> getHashLookupJoinConjuncts(
      List<TupleId> lhsTblRefIds, List<TupleId> rhsTblRefIds, Analyzer analyzer) {
    List<BinaryPredicate> result = Lists.newArrayList();
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
    for (TupleId rhsId: rhsTblRefIds) {
      TableRef rhsTblRef = analyzer.getTableRef(rhsId);
      Preconditions.checkNotNull(rhsTblRef);
      for (SlotDescriptor slotDesc: rhsTblRef.getDesc().getSlots()) {
        SlotId rhsSid = slotDesc.getId();
        // List of slots that participate in a value transfer with rhsSid and are belong
        // to a tuple in lhsTblRefIds. The value transfer is not necessarily mutual.
        List<SlotId> lhsSlotIds = analyzer.getEquivSlots(rhsSid, lhsTblRefIds);
        for (SlotId lhsSid: lhsSlotIds) {
          // A mutual value transfer between lhsSid and rhsSid is required for correctly
          // generating an inferred predicate. Otherwise, the predicate might incorrectly
          // eliminate rows that would have been non-matches of an outer or anti join.
          if (analyzer.hasMutualValueTransfer(lhsSid, rhsSid)) {
            // construct a BinaryPredicates in order to get correct casting;
            // we only do this for one of the equivalent slots, all the other implied
            // equalities are redundant
            BinaryPredicate pred =
                analyzer.createInferredEqPred(lhsSid, rhsSid);
            result.add(pred);
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

    List<Expr> otherJoinConjuncts = Lists.newArrayList();
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
    analyzer.markConjunctsAssigned(otherJoinConjuncts);

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
   * Create a tree of PlanNodes for the given tblRef, which can be a BaseTableRef,
   * CollectionTableRef or an InlineViewRef.
   *
   * The given 'aggInfo' is used for detecting and applying optimizations that span both
   * the scan and aggregation. Only applicable to HDFS table refs.
   *
   * Throws if a PlanNode.init() failed or if planning of the given
   * table ref is not implemented.
   */
  private PlanNode createTableRefNode(TableRef tblRef, AggregateInfo aggInfo,
      Analyzer analyzer) throws ImpalaException {
    PlanNode result = null;
    if (tblRef instanceof BaseTableRef) {
      result = createScanNode(tblRef, aggInfo, analyzer);
    } else if (tblRef instanceof CollectionTableRef) {
      if (tblRef.isRelative()) {
        Preconditions.checkState(ctx_.hasSubplan());
        result = new UnnestNode(ctx_.getNextNodeId(), ctx_.getSubplan(),
            (CollectionTableRef) tblRef);
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
  private UnionNode createUnionPlan(
      Analyzer analyzer, UnionStmt unionStmt, List<UnionOperand> unionOperands,
      PlanNode unionDistinctPlan)
      throws ImpalaException {
    UnionNode unionNode = new UnionNode(ctx_.getNextNodeId(), unionStmt.getTupleId(),
        unionStmt.getUnionResultExprs(), ctx_.hasSubplan());
    for (UnionOperand op: unionOperands) {
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
      Preconditions.checkState(unionStmt.hasDistinctOps());
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
      for (UnionOperand op: unionStmt.getOperands()) {
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
    if (unionStmt.hasDistinctOps()) {
      result = createUnionPlan(
          analyzer, unionStmt, unionStmt.getDistinctOperands(), null);
      result = new AggregationNode(
          ctx_.getNextNodeId(), result, unionStmt.getDistinctAggInfo());
      result.init(analyzer);
    }
    // create ALL tree
    if (unionStmt.hasAllOps()) {
      result = createUnionPlan(analyzer, unionStmt, unionStmt.getAllOperands(), result);
    }

    if (unionStmt.hasAnalyticExprs()) {
      result = addUnassignedConjuncts(
          analyzer, unionStmt.getTupleId().asList(), result);
    }
    return result;
  }
}
