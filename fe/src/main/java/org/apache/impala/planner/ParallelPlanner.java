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
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.impala.common.IdGenerator;
import com.google.common.base.Preconditions;

/**
 * The parallel planner is responsible for breaking up a single distributed plan
 * (= tree of PlanFragments) into a (logical) tree of distributed plans. The root
 * of that tree produces the query result, all the other ones produce intermediate
 * join build sides. All plans that produce intermediate join build sides (one per join
 * node in the recipient) for a single recipient plan are grouped together into a
 * cohort. Since each plan may only produce a build side for at most one recipient
 * plan, each plan belongs to exactly one cohort.
 *
 * TODO: if the input to the JoinBuildSink is the result of a grouping aggregation
 * on the join keys, the AggregationNode should materialize the final hash table
 * directly (instead of reading the hash table content and feeding it into a
 * JoinBuildSink to build another hash table)
 *
 * TODO: instead of cohort ids, create a Plan class that is a subclass of TreeNode?
 */
public class ParallelPlanner {
  private final static Logger LOG = LoggerFactory.getLogger(ParallelPlanner.class);

  private final IdGenerator<JoinTableId> joinTableIdGenerator_ =
      JoinTableId.createGenerator();
  private final IdGenerator<PlanId> planIdGenerator_ = PlanId.createGenerator();
  private final IdGenerator<CohortId> cohortIdGenerator_ = CohortId.createGenerator();
  private final PlannerContext ctx_;

  private List<PlanFragment> planRoots_ = new ArrayList<>();

  public ParallelPlanner(PlannerContext ctx) { ctx_ = ctx; }

  /**
   * Given a distributed plan, return list of plans ready for parallel execution. Each
   * returned fragment has a PlanRootSink or JoinBuildSink as their sink. The first
   * plan in the list materializes the query result and subsequent plans materialize
   * the build sides of joins. Each plan appears before its dependencies in the list.
   * Assigns cohortId and planId for all fragments.
   * TODO: create class DistributedPlan with a PlanFragment member, so we don't
   * need to distinguish PlanFragment and Plan through comments?
   */
  public List<PlanFragment> createPlans(PlanFragment root) {
    root.setPlanId(planIdGenerator_.getNextId());
    root.setCohortId(cohortIdGenerator_.getNextId());
    planRoots_.add(root);
    createBuildPlans(root, null);
    return planRoots_;
  }

  /**
   * Recursively traverse tree of fragments of 'plan' from top to bottom and
   * move all build inputs of joins into separate plans. 'buildCohortId' is the
   * cohort id of the build plans of 'fragment' and may be null if the plan
   * to which 'fragment' belongs has so far not required any build plans.
   * Assign fragment's plan id and cohort id to children.
   */
  private void createBuildPlans(PlanFragment fragment, CohortId buildCohortId) {
    List<JoinNode> joins = new ArrayList<>();
    collectJoins(fragment.getPlanRoot(), joins);
    if (!joins.isEmpty()) {
      if (buildCohortId == null) buildCohortId = cohortIdGenerator_.getNextId();
      for (JoinNode join: joins) createBuildPlan(join, buildCohortId);
    }

    for (PlanFragment child: fragment.getChildren()) {
      // We already recursed on the join build fragment in createBuildPlan().
      if (child.getSink() instanceof JoinBuildSink) continue;
      // Propagate the plan and cohort IDs to children that are part of the same plan.
      child.setPlanId(fragment.getPlanId());
      child.setCohortId(fragment.getCohortId());
      createBuildPlans(child, buildCohortId);
    }
  }

  /**
   * Collect all JoinNodes that aren't themselves the build side of a join node
   * in this fragment or the rhs of a SubplanNode.
   */
  private void collectJoins(PlanNode node, List<JoinNode> result) {
    if (node instanceof JoinNode) {
      result.add((JoinNode)node);
      // for joins, only descend through the probe side;
      // we're recursively traversing the build side when constructing the build plan
      // in createBuildPlan()
      collectJoins(node.getChild(0), result);
      return;
    }
    if (node instanceof ExchangeNode) return;
    if (node instanceof SubplanNode) {
      collectJoins(node.getChild(0), result);
      return;
    }
    for (PlanNode child: node.getChildren()) collectJoins(child, result);
  }

  /**
   * Collect all ExchangeNodes in this fragment.
   */
  private void collectExchangeNodes(PlanNode node, List<ExchangeNode> result) {
    if (node instanceof ExchangeNode) {
      result.add((ExchangeNode)node);
      return;
    }
    for (PlanNode child: node.getChildren()) collectExchangeNodes(child, result);
  }

  /**
   * Create new plan that materializes build input of 'join' and assign it 'cohortId'.
   * In the process, moves all fragments required for this materialization from tree
   * rooted at 'join's fragment into the new plan.
   * Also assigns the new plan a plan id.
   */
  private void createBuildPlan(JoinNode join, CohortId cohortId) {
    Preconditions.checkNotNull(cohortId);
    // collect all ExchangeNodes on the build side and their corresponding input
    // fragments
    final List<ExchangeNode> exchNodes = new ArrayList<>();
    collectExchangeNodes(join.getChild(1), exchNodes);

    com.google.common.base.Predicate<PlanFragment> isInputFragment =
        new com.google.common.base.Predicate<PlanFragment>() {
          @Override
          public boolean apply(PlanFragment f) {
            // we're starting with the fragment containing the join, which might
            // be terminal
            if (f.getDestNode() == null) return false;
            for (ExchangeNode exch: exchNodes) {
              if (exch.getId() == f.getDestNode().getId()) return true;
            }
            return false;
          }
        };
    List<PlanFragment> inputFragments = new ArrayList<>();
    join.getFragment().collect(isInputFragment, inputFragments);
    Preconditions.checkState(exchNodes.size() == inputFragments.size());

    // Create new fragment with JoinBuildSink that consumes the output of the
    // join's rhs input (the one that materializes the build side).
    // The new fragment has the same data partition as the join node's fragment.
    JoinBuildSink buildSink =
        new JoinBuildSink(joinTableIdGenerator_.getNextId(), join);
    join.setJoinTableId(buildSink.getJoinTableId());
    // Filters will be produced by the build sink. Remove them from the join node since
    // it now not responsible for producing them.
    join.getRuntimeFilters().clear();
    // c'tor fixes up PlanNode.fragment_
    PlanFragment buildFragment = new PlanFragment(ctx_.getNextFragmentId(),
        join.getChild(1), join.getFragment().getDataPartition());
    buildFragment.setSink(buildSink);

    // Fix up the child/parent relationships in the PlanFragment tree.
    for (int i = 0; i < exchNodes.size(); ++i) {
      Preconditions.checkState(exchNodes.get(i).getFragment() == buildFragment);
      join.getFragment().removeChild(inputFragments.get(i));
      buildFragment.getChildren().add(inputFragments.get(i));
    }
    buildFragment.setDestination(join);

    // assign plan and cohort id
    buildFragment.setPlanId(planIdGenerator_.getNextId());
    buildFragment.setCohortId(cohortId);

    planRoots_.add(buildFragment);
    if (LOG.isTraceEnabled()) {
      LOG.trace("new build fragment " + buildFragment.getId().toString());
      LOG.trace("in cohort " + buildFragment.getCohortId().toString());
      LOG.trace("for join node " + join.getId().toString());
    }
    createBuildPlans(buildFragment, null);
  }

}
