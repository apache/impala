// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AggregateInfo;
import com.cloudera.impala.analysis.AnalyticInfo;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BaseTableRef;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.CollectionTableRef;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.ExprId;
import com.cloudera.impala.analysis.ExprSubstitutionMap;
import com.cloudera.impala.analysis.InlineViewRef;
import com.cloudera.impala.analysis.JoinOperator;
import com.cloudera.impala.analysis.QueryStmt;
import com.cloudera.impala.analysis.SelectStmt;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.TableRef;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.analysis.TupleIsNullPredicate;
import com.cloudera.impala.analysis.UnionStmt;
import com.cloudera.impala.analysis.UnionStmt.UnionOperand;
import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.DataSourceTable;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.common.Pair;
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
   * - materialize the slots required for evaluating expressions of that statement
   * - migrate conjuncts from parent blocks into inline views and union operands
   * In the bottom-up phase generate the plan tree for every query statement:
   * - perform join-order optimization when generating the plan of the FROM
   *   clause of a select statement; requires that all materialized slots are known
   *   for an accurate estimate of row sizes needed for cost-based join ordering
   * - assign conjuncts that can be evaluated at that node and compute the stats
   *   of that node (cardinality, etc.)
   * - apply combined expression substitution map of child plan nodes; if a plan node
   *   re-maps its input, set a substitution map to be applied by parents
   */
  public PlanNode createSingleNodePlan() throws ImpalaException {
    QueryStmt queryStmt = ctx_.getQueryStmt();
    // Use the stmt's analyzer which is not necessarily the root analyzer
    // to detect empty result sets.
    Analyzer analyzer = queryStmt.getAnalyzer();
    analyzer.computeEquivClasses();
    analyzer.getTimeline().markEvent("Equivalence classes computed");

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

    LOG.trace("desctbl: " + analyzer.getDescTbl().debugString());
    PlanNode singleNodePlan = createQueryPlan(queryStmt, analyzer,
        ctx_.getQueryOptions().isDisable_outermost_topn());
    Preconditions.checkNotNull(singleNodePlan);
    return singleNodePlan;
  }

  /**
   * Creates an EmptyNode that 'materializes' the tuples of the given stmt.
   */
  private PlanNode createEmptyNode(QueryStmt stmt, Analyzer analyzer)
      throws InternalException {
    ArrayList<TupleId> tupleIds = Lists.newArrayList();
    stmt.getMaterializedTupleIds(tupleIds);
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
        ArrayList<TupleId> stmtTupleIds = Lists.newArrayList();
        stmt.getMaterializedTupleIds(stmtTupleIds);
        AnalyticPlanner analyticPlanner =
            new AnalyticPlanner(stmtTupleIds, analyticInfo, analyzer, ctx_);
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
      root = new SortNode(ctx_.getNextNodeId(), root, stmt.getSortInfo(),
          useTopN, stmt.getOffset());
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
      Analyzer analyzer, List<TupleId> tupleIds, PlanNode root)
      throws InternalException {
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
   * Return the cheapest plan that materializes the joins of all TblRefs in refPlans.
   * Assumes that refPlans are in the order as they originally appeared in the query.
   * For this plan:
   * - the plan is executable, ie, all non-cross joins have equi-join predicates
   * - the leftmost scan is over the largest of the inputs for which we can still
   *   construct an executable plan
   * - all rhs's are in decreasing order of selectiveness (percentage of rows they
   *   eliminate)
   * - outer/cross/semi joins: rhs serialized size is < lhs serialized size;
   *   enforced via join inversion, if necessary
   * Returns null if we can't create an executable plan.
   */
  private PlanNode createCheapestJoinPlan(
      Analyzer analyzer, List<Pair<TableRef, PlanNode>> refPlans)
      throws ImpalaException {
    LOG.trace("createCheapestJoinPlan");
    if (refPlans.size() == 1) return refPlans.get(0).second;

    // collect eligible candidates for the leftmost input; list contains
    // (plan, materialized size)
    ArrayList<Pair<TableRef, Long>> candidates = Lists.newArrayList();
    for (Pair<TableRef, PlanNode> entry: refPlans) {
      TableRef ref = entry.first;
      JoinOperator joinOp = ref.getJoinOp();

      // The rhs table of an outer/semi join can appear as the left-most input if we
      // invert the lhs/rhs and the join op. However, we may only consider this inversion
      // for the very first join in refPlans, otherwise we could reorder tables/joins
      // across outer/semi joins which is generally incorrect. The null-aware
      // left anti-join operator is never considered for inversion because we can't
      // execute the null-aware right anti-join efficiently.
      // TODO: Allow the rhs of any cross join as the leftmost table. This needs careful
      // consideration of the joinOps that result from such a re-ordering (IMPALA-1281).
      if (((joinOp.isOuterJoin() || joinOp.isSemiJoin() || joinOp.isCrossJoin()) &&
          ref != refPlans.get(1).first) || joinOp.isNullAwareLeftAntiJoin()) {
        // ref cannot appear as the leftmost input
        continue;
      }

      PlanNode plan = entry.second;
      if (plan.getCardinality() == -1) {
        // use 0 for the size to avoid it becoming the leftmost input
        // TODO: Consider raw size of scanned partitions in the absence of stats.
        candidates.add(new Pair(ref, new Long(0)));
        LOG.trace("candidate " + ref.getUniqueAlias() + ": 0");
        continue;
      }
      Preconditions.checkNotNull(ref.getDesc());
      long materializedSize =
          (long) Math.ceil(plan.getAvgRowSize() * (double) plan.getCardinality());
      candidates.add(new Pair(ref, new Long(materializedSize)));
      LOG.trace("candidate " + ref.getUniqueAlias() + ": " + Long.toString(materializedSize));
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
      PlanNode result = createJoinPlan(analyzer, candidate.first, refPlans);
      if (result != null) return result;
    }
    return null;
  }

  /**
   * Returns a plan with leftmostRef's plan as its leftmost input; the joins
   * are in decreasing order of selectiveness (percentage of rows they eliminate).
   * The leftmostRef's join will be inverted if it is an outer/semi/cross join.
   */
  private PlanNode createJoinPlan(
      Analyzer analyzer, TableRef leftmostRef, List<Pair<TableRef, PlanNode>> refPlans)
      throws ImpalaException {

    LOG.trace("createJoinPlan: " + leftmostRef.getUniqueAlias());
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
    // refs that have been joined. The union of joinedRefs and the refs in remainingRefs
    // are the set of all table refs.
    Set<TableRef> joinedRefs = Sets.newHashSet();
    joinedRefs.add(leftmostRef);

    // If the leftmostTblRef is an outer/semi/cross join, we must invert it.
    boolean planHasInvertedJoin = false;
    if (leftmostRef.getJoinOp().isOuterJoin()
        || leftmostRef.getJoinOp().isSemiJoin()
        || leftmostRef.getJoinOp().isCrossJoin()) {
      // TODO: Revisit the interaction of join inversion here and the analysis state
      // that is changed in analyzer.invertOuterJoin(). Changing the analysis state
      // should not be necessary because the semantics of an inverted outer join do
      // not change.
      leftmostRef.invertJoin(refPlans, analyzer);
      planHasInvertedJoin = true;
    }

    long numOps = 0;
    int i = 0;
    while (!remainingRefs.isEmpty()) {
      // we minimize the resulting cardinality at each step in the join chain,
      // which minimizes the total number of hash table lookups
      PlanNode newRoot = null;
      Pair<TableRef, PlanNode> minEntry = null;
      for (Pair<TableRef, PlanNode> entry: remainingRefs) {
        TableRef ref = entry.first;
        LOG.trace(Integer.toString(i) + " considering ref " + ref.getUniqueAlias());

        // Determine whether we can or must consider this join at this point in the plan.
        // Place outer/semi joins at a fixed position in the plan tree (IMPALA-860),
        // s.t. all the tables appearing to the left/right of an outer/semi join in
        // the original query still remain to the left/right after join ordering. This
        // prevents join re-ordering across outer/semi joins which is generally wrong.
        // The checks below relies on remainingRefs being in the order as they originally
        // appeared in the query.
        JoinOperator joinOp = ref.getJoinOp();
        if (joinOp.isOuterJoin() || joinOp.isSemiJoin()) {
          List<TupleId> currentTids = Lists.newArrayList(root.getTblRefIds());
          currentTids.add(ref.getId());
          // Place outer/semi joins at a fixed position in the plan tree. We know that
          // the join resulting from 'ref' must become the new root if the current
          // root materializes exactly those tuple ids corresponding to TableRefs
          // appearing to the left of 'ref' in the original query.
          List<TupleId> tableRefTupleIds = ref.getAllTupleIds();
          if (!currentTids.containsAll(tableRefTupleIds) ||
              !tableRefTupleIds.containsAll(currentTids)) {
            // Do not consider the remaining table refs to prevent incorrect re-ordering
            // of tables across outer/semi/anti joins.
            break;
          }
        } else if (ref.getJoinOp().isCrossJoin()) {
          if (!joinedRefs.contains(ref.getLeftTblRef())) continue;
        }

        PlanNode rhsPlan = entry.second;
        analyzer.setAssignedConjuncts(root.getAssignedConjuncts());

        boolean invertJoin = false;
        if (joinOp.isOuterJoin() || joinOp.isSemiJoin() || joinOp.isCrossJoin()) {
          // Invert the join if doing so reduces the size of build-side hash table
          // (may also reduce network costs depending on the join strategy).
          // Only consider this optimization if both the lhs/rhs cardinalities are known.
          // The null-aware left anti-join operator is never considered for inversion
          // because we can't execute the null-aware right anti-join efficiently.
          long lhsCard = root.getCardinality();
          long rhsCard = rhsPlan.getCardinality();
          if (lhsCard != -1 && rhsCard != -1 &&
              lhsCard * root.getAvgRowSize() < rhsCard * rhsPlan.getAvgRowSize() &&
              !joinOp.isNullAwareLeftAntiJoin()) {
            invertJoin = true;
          }
        }
        PlanNode candidate = null;
        if (invertJoin) {
          ref.setJoinOp(ref.getJoinOp().invert());
          candidate = createJoinNode(analyzer, rhsPlan, root, ref, null);
          planHasInvertedJoin = true;
        } else {
          candidate = createJoinNode(analyzer, root, rhsPlan, null, ref);
        }
        if (candidate == null) continue;
        LOG.trace("cardinality=" + Long.toString(candidate.getCardinality()));

        // Use 'candidate' as the new root; don't consider any other table refs at this
        // position in the plan.
        if (joinOp.isOuterJoin() || joinOp.isSemiJoin()) {
          newRoot = candidate;
          minEntry = entry;
          break;
        }

        // Always prefer Hash Join over Cross Join due to limited costing infrastructure
        if (newRoot == null
            || (candidate.getClass().equals(newRoot.getClass())
                && candidate.getCardinality() < newRoot.getCardinality())
            || (candidate instanceof HashJoinNode && newRoot instanceof CrossJoinNode)) {
          newRoot = candidate;
          minEntry = entry;
        }
      }
      if (newRoot == null) {
        // Currently, it should not be possible to invert a join for a plan that turns
        // out to be non-executable because (1) the joins we consider for inversion are
        // barriers in the join order, and (2) the caller of this function only considers
        // other leftmost table refs if a plan turns out to be non-executable.
        // TODO: This preconditions check will need to be changed to undo the in-place
        // modifications made to table refs for join inversion, if the caller decides to
        // explore more leftmost table refs.
        Preconditions.checkState(!planHasInvertedJoin);
        return null;
      }

      // we need to insert every rhs row into the hash table and then look up
      // every lhs row
      long lhsCardinality = root.getCardinality();
      long rhsCardinality = minEntry.second.getCardinality();
      numOps += lhsCardinality + rhsCardinality;
      LOG.debug(Integer.toString(i) + " chose " + minEntry.first.getUniqueAlias()
          + " #lhs=" + Long.toString(lhsCardinality)
          + " #rhs=" + Long.toString(rhsCardinality)
          + " #ops=" + Long.toString(numOps));
      remainingRefs.remove(minEntry);
      joinedRefs.add(minEntry.first);
      root = newRoot;
      // assign id_ after running through the possible choices in order to end up
      // with a dense sequence of node ids
      root.setId(ctx_.getNextNodeId());
      analyzer.setAssignedConjuncts(root.getAssignedConjuncts());
      ++i;
    }

    return root;
  }

  /**
   * Return a plan with joins in the order of refPlans (= FROM clause order).
   */
  private PlanNode createFromClauseJoinPlan(
      Analyzer analyzer, List<Pair<TableRef, PlanNode>> refPlans)
      throws ImpalaException {
    // create left-deep sequence of binary hash joins; assign node ids as we go along
    Preconditions.checkState(!refPlans.isEmpty());
    PlanNode root = refPlans.get(0).second;
    for (int i = 1; i < refPlans.size(); ++i) {
      TableRef innerRef = refPlans.get(i).first;
      PlanNode innerPlan = refPlans.get(i).second;
      root = createJoinNode(analyzer, root, innerPlan, null, innerRef);
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
    // are materialized (see IMPALA-1960).
    if (analyzer.hasEmptySpjResultSet()) {
      PlanNode emptySetNode = new EmptySetNode(ctx_.getNextNodeId(), rowTuples);
      emptySetNode.init(analyzer);
      emptySetNode.setOutputSmap(selectStmt.getBaseTblSmap());
      return createAggregationPlan(selectStmt, analyzer, emptySetNode);
    }

    // create plans for our table refs; use a list here instead of a map to
    // maintain a deterministic order of traversing the TableRefs during join
    // plan generation (helps with tests)
    List<Pair<TableRef, PlanNode>> refPlans = Lists.newArrayList();
    for (TableRef ref: selectStmt.getTableRefs()) {
      PlanNode plan = createTableRefNode(analyzer, ref);
      Preconditions.checkState(plan != null);
      refPlans.add(new Pair(ref, plan));
    }
    // save state of conjunct assignment; needed for join plan generation
    for (Pair<TableRef, PlanNode> entry: refPlans) {
      entry.second.setAssignedConjuncts(analyzer.getAssignedConjuncts());
    }

    PlanNode root = null;
    if (!selectStmt.getSelectList().isStraightJoin()) {
      Set<ExprId> assignedConjuncts = analyzer.getAssignedConjuncts();
      root = createCheapestJoinPlan(analyzer, refPlans);
      if (root == null) analyzer.setAssignedConjuncts(assignedConjuncts);
    }
    if (selectStmt.getSelectList().isStraightJoin() || root == null) {
      // we didn't have enough stats to do a cost-based join plan, or the STRAIGHT_JOIN
      // keyword was in the select list: use the FROM clause order instead
      root = createFromClauseJoinPlan(analyzer, refPlans);
      Preconditions.checkNotNull(root);
    }

    // add aggregation, if any
    if (selectStmt.getAggInfo() != null) {
      root = createAggregationPlan(selectStmt, analyzer, root);
    }

    // All the conjuncts_ should be assigned at this point.
    // TODO: Re-enable this check here and/or elswehere.
    //Preconditions.checkState(!analyzer.hasUnassignedConjuncts());
    return root;
  }

  /**
   * Returns a new AggregationNode that materializes the aggregation of the given stmt.
   * Assigns conjuncts from the Having clause to the returned node.
   */
  private PlanNode createAggregationPlan(SelectStmt selectStmt, Analyzer analyzer,
      PlanNode root) throws InternalException {
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
    ArrayList<Expr> resultExprs = selectStmt.getResultExprs();
    ArrayList<String> colLabels = selectStmt.getColLabels();
    // Create tuple descriptor for materialized tuple.
    TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor("union");
    tupleDesc.setIsMaterialized(true);
    UnionNode unionNode = new UnionNode(ctx_.getNextNodeId(), tupleDesc.getId());

    // Analysis guarantees that selects without a FROM clause only have constant exprs.
    unionNode.addConstExprList(Lists.newArrayList(resultExprs));

    // Replace the select stmt's resultExprs with SlotRefs into tupleDesc.
    for (int i = 0; i < resultExprs.size(); ++i) {
      SlotDescriptor slotDesc = analyzer.addSlotDescriptor(tupleDesc);
      slotDesc.setLabel(colLabels.get(i));
      slotDesc.setSourceExpr(resultExprs.get(i));
      slotDesc.setType(resultExprs.get(i).getType());
      slotDesc.setStats(ColumnStats.fromExpr(resultExprs.get(i)));
      slotDesc.setIsMaterialized(true);
      SlotRef slotRef = new SlotRef(slotDesc);
      resultExprs.set(i, slotRef);
    }
    tupleDesc.computeMemLayout();
    // UnionNode.init() needs tupleDesc to have been initialized
    unionNode.init(analyzer);
    return unionNode;
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
      if (comp.getOp() == BinaryPredicate.Operator.NE) continue;
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
          return createEmptyNode(viewStmt, inlineViewRef.getAnalyzer());
        }
        // Analysis should have generated a tuple id_ into which to materialize the exprs.
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
    // the avg row size is availble during optimization; however, that means we need to
    // select references to its resultExprs from the enclosing scope(s)
    rootNode.setTblRefIds(Lists.newArrayList(inlineViewRef.getId()));

    ExprSubstitutionMap inlineViewSmap = inlineViewRef.getSmap();
    if (analyzer.isOuterJoined(inlineViewRef.getId())) {
      // Exprs against non-matched rows of an outer join should always return NULL.
      // Make the rhs exprs of the inline view's smap nullable, if necessary.
      List<Expr> nullableRhs = TupleIsNullPredicate.wrapExprs(
          inlineViewSmap.getRhs(), rootNode.getTupleIds(), analyzer);
      inlineViewSmap = new ExprSubstitutionMap(inlineViewSmap.getLhs(), nullableRhs);
    }
    // Set output smap of rootNode *before* creating a SelectNode for proper resolution.
    // The output smap is the composition of the inline view's smap and the output smap
    // of the inline view's plan root. This ensures that all downstream exprs referencing
    // the inline view are replaced with exprs referencing the physical output of
    // the inline view's plan.
    ExprSubstitutionMap composedSmap = ExprSubstitutionMap.compose(inlineViewSmap,
        rootNode.getOutputSmap(), analyzer);
    rootNode.setOutputSmap(composedSmap);

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
    // equivalences and may incorrectly reject rows with nulls (IMPALA-1412).
    Predicate<Expr> isIdentityPredicate = new Predicate<Expr>() {
      @Override
      public boolean apply(Expr expr) {
        if (!(expr instanceof BinaryPredicate)
            || ((BinaryPredicate) expr).getOp() != BinaryPredicate.Operator.EQ) {
          return false;
        }
        if (!expr.isRegisteredPredicate()
            && expr.getChild(0) instanceof SlotRef
            && expr.getChild(1) instanceof SlotRef
            && (((SlotRef) expr.getChild(0)).getSlotId() ==
               ((SlotRef) expr.getChild(1)).getSlotId())) {
          return true;
        }
        return false;
      }
    };
    Iterables.removeIf(viewPredicates, isIdentityPredicate);

    // "migrate" conjuncts_ by marking them as assigned and re-registering them with
    // new ids.
    // Mark pre-substitution conjuncts as assigned, since the ids of the new exprs may
    // have changed.
    analyzer.markConjunctsAssigned(preds);
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
   * Create node for scanning all data files of a particular table.
   */
  private PlanNode createScanNode(Analyzer analyzer, TableRef tblRef)
      throws InternalException {
    ScanNode scanNode = null;
    if (tblRef.getTable() instanceof HdfsTable) {
      scanNode = new HdfsScanNode(ctx_.getNextNodeId(), tblRef.getDesc(),
          (HdfsTable)tblRef.getTable());
      scanNode.init(analyzer);
      return scanNode;
    } else if (tblRef.getTable() instanceof DataSourceTable) {
      scanNode = new DataSourceScanNode(ctx_.getNextNodeId(), tblRef.getDesc());
      scanNode.init(analyzer);
      return scanNode;
    } else if (tblRef.getTable() instanceof HBaseTable) {
      // HBase table
      scanNode = new HBaseScanNode(ctx_.getNextNodeId(), tblRef.getDesc());
    } else {
      throw new InternalException("Invalid table ref class: " + tblRef.getClass());
    }
    // TODO: move this to HBaseScanNode.init();
    Preconditions.checkState(scanNode instanceof HBaseScanNode);

    List<Expr> conjuncts = analyzer.getUnassignedConjuncts(scanNode);
    // mark conjuncts_ assigned here; they will either end up inside a
    // ValueRange or will be evaluated directly by the node
    analyzer.markConjunctsAssigned(conjuncts);
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
      // Ignore predicate if one of its children is a constant.
      if (e.getChild(0).isConstant() || e.getChild(1).isConstant()) continue;

      Expr rhsExpr = null;
      if (e.getChild(0).isBoundByTupleIds(rhsTblRefIds)) {
        rhsExpr = e.getChild(0);
      } else {
        Preconditions.checkState(e.getChild(1).isBoundByTupleIds(rhsTblRefIds));
        rhsExpr = e.getChild(1);
      }

      Expr lhsExpr = null;
      if (e.getChild(1).isBoundByTupleIds(lhsTblRefIds)) {
        lhsExpr = e.getChild(1);
      } else if (e.getChild(0).isBoundByTupleIds(lhsTblRefIds)) {
        lhsExpr = e.getChild(0);
      } else {
        // not an equi-join condition between the lhs and rhs ids
        continue;
      }

      Preconditions.checkState(lhsExpr != rhsExpr);
      BinaryPredicate joinConjunct =
          new BinaryPredicate(((BinaryPredicate)e).getOp(), lhsExpr, rhsExpr);
      analyzer.markConjunctAssigned(e);
      joinConjunct.analyzeNoThrow(analyzer);
      result.add(joinConjunct);
    }
    if (!result.isEmpty()) return result;

    // Construct join conjuncts derived from equivalence class membership.
    for (TupleId rhsId: rhsTblRefIds) {
      TableRef rhsTblRef = analyzer.getTableRef(rhsId);
      Preconditions.checkNotNull(rhsTblRef);
      for (SlotDescriptor slotDesc: rhsTblRef.getDesc().getSlots()) {
        List<SlotId> lhsSlotIds = analyzer.getEquivSlots(slotDesc.getId(), lhsTblRefIds);
        if (!lhsSlotIds.isEmpty()) {
          // construct a BinaryPredicates in order to get correct casting;
          // we only do this for one of the equivalent slots, all the other implied
          // equalities are redundant
          BinaryPredicate pred =
              analyzer.createEqPredicate(lhsSlotIds.get(0), slotDesc.getId());
          result.add(pred);
        }
      }
    }
    return result;
  }

  /**
   * Create a node to join outer with inner. Either the outer or the inner may be a plan
   * created from a table ref (but not both), and the corresponding outer/innerRef
   * should be non-null.
   */
  private PlanNode createJoinNode(
      Analyzer analyzer, PlanNode outer, PlanNode inner, TableRef outerRef,
      TableRef innerRef) throws ImpalaException {
    Preconditions.checkState(innerRef != null ^ outerRef != null);
    TableRef tblRef = (innerRef != null) ? innerRef : outerRef;

    // get eq join predicates for the TableRefs' ids (not the PlanNodes' ids, which
    // are materialized)
    List<BinaryPredicate> eqJoinConjuncts = Collections.emptyList();
    if (innerRef != null) {
      eqJoinConjuncts = getHashLookupJoinConjuncts(
          outer.getTblRefIds(), inner.getTblRefIds(), analyzer);
      // Outer joins should only use On-clause predicates as eqJoinConjuncts.
      if (!innerRef.getJoinOp().isOuterJoin()) {
        analyzer.createEquivConjuncts(outer.getTblRefIds(), inner.getTblRefIds(),
            eqJoinConjuncts);
      }
    } else {
      eqJoinConjuncts = getHashLookupJoinConjuncts(
          inner.getTblRefIds(), outer.getTblRefIds(), analyzer);
      // Outer joins should only use On-clause predicates as eqJoinConjuncts.
      if (!outerRef.getJoinOp().isOuterJoin()) {
        analyzer.createEquivConjuncts(inner.getTblRefIds(), outer.getTblRefIds(),
            eqJoinConjuncts);
      }
      // Reverse the lhs/rhs of the join conjuncts.
      for (BinaryPredicate eqJoinConjunct: eqJoinConjuncts) {
        Expr swapTmp = eqJoinConjunct.getChild(0);
        eqJoinConjunct.setChild(0, eqJoinConjunct.getChild(1));
        eqJoinConjunct.setChild(1, swapTmp);
      }
    }

    // Handle implicit cross joins
    if (eqJoinConjuncts.isEmpty()) {
      // Since our only implementation of semi and outer joins is hash-based, and we do
      // not re-order semi and outer joins, we must have eqJoinConjuncts here to execute
      // this query.
      // TODO: Revisit when we add more semi/join implementations. Pick up and pass in
      // the otherJoinConjuncts.
      if (tblRef.getJoinOp().isOuterJoin() ||
          tblRef.getJoinOp().isSemiJoin()) {
        throw new NotImplementedException(
            String.format("%s join with '%s' without equi-join " +
            "conjuncts is not supported.",
            tblRef.getJoinOp().isOuterJoin() ? "Outer" : "Semi",
            innerRef.getUniqueAlias()));
      }
      CrossJoinNode result =
          new CrossJoinNode(outer, inner, tblRef, Collections.<Expr>emptyList());
      result.init(analyzer);
      return result;
    }

    // Handle explicit cross joins with equi join conditions
    if (tblRef.getJoinOp() == JoinOperator.CROSS_JOIN) {
      tblRef.setJoinOp(JoinOperator.INNER_JOIN);
    }

    List<Expr> otherJoinConjuncts = Lists.newArrayList();
    if (tblRef.getJoinOp().isOuterJoin()) {
      // Also assign conjuncts from On clause. All remaining unassigned conjuncts
      // that can be evaluated by this join are assigned in createSelectPlan().
      otherJoinConjuncts = analyzer.getUnassignedOjConjuncts(tblRef);
    } else if (tblRef.getJoinOp().isSemiJoin()) {
      // Unassigned conjuncts bound by the invisible tuple id of a semi join must have
      // come from the join's On-clause, and therefore, must be added to the other join
      // conjuncts to produce correct results.
      otherJoinConjuncts =
          analyzer.getUnassignedConjuncts(tblRef.getAllTupleIds(), false);
      if (tblRef.getJoinOp().isNullAwareLeftAntiJoin()) {
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

    HashJoinNode result =
        new HashJoinNode(outer, inner, tblRef, eqJoinConjuncts, otherJoinConjuncts);
    result.init(analyzer);
    return result;
  }

  /**
   * Create a tree of PlanNodes for the given tblRef, which can be a BaseTableRef,
   * CollectionTableRef or an InlineViewRef.
   */
  private PlanNode createTableRefNode(Analyzer analyzer, TableRef tblRef)
      throws ImpalaException {
    if (tblRef instanceof BaseTableRef || tblRef instanceof CollectionTableRef) {
      return createScanNode(analyzer, tblRef);
    } else if (tblRef instanceof InlineViewRef) {
      return createInlineViewPlan(analyzer, (InlineViewRef) tblRef);
    }
    throw new InternalException(
        "Unknown TableRef node: " + tblRef.getClass().getSimpleName());
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
    UnionNode unionNode = new UnionNode(ctx_.getNextNodeId(), unionStmt.getTupleId());
    for (UnionOperand op: unionOperands) {
      if (op.getAnalyzer().hasEmptyResultSet()) continue;
      QueryStmt queryStmt = op.getQueryStmt();
      if (queryStmt instanceof SelectStmt) {
        SelectStmt selectStmt = (SelectStmt) queryStmt;
        if (selectStmt.getTableRefs().isEmpty()) {
          unionNode.addConstExprList(selectStmt.getBaseTblResultExprs());
          continue;
        }
      }
      PlanNode opPlan = createQueryPlan(queryStmt, op.getAnalyzer(), false);
      if (opPlan instanceof EmptySetNode) continue;
      unionNode.addChild(opPlan, op.getQueryStmt().getBaseTblResultExprs());
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
