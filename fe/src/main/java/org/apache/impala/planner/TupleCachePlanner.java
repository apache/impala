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

import java.util.List;

import org.apache.impala.common.ImpalaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * The TupleCachePlanner adds TupleCacheNodes to an existing distributed plan tree.
 * Calculating eligibility and cache keys for locations in the plan tree requires that
 * the plan tree be in a stable form that won't later change. That means that this is
 * designed to run as the last step in planning.
 *
 * The current algorithm is to add a TupleCacheNode at every eligible location. This will
 * need to be refined with cost calculations later.
 */
public class TupleCachePlanner {
  private final static Logger LOG = LoggerFactory.getLogger(TupleCachePlanner.class);

  private final PlannerContext ctx_;

  public TupleCachePlanner(PlannerContext ctx) {
    ctx_ = ctx;
  }

  /**
   * This takes an existing distributed plan, computes the eligibility and cache keys,
   * then adds TupleCacheNodes at eligible locations.
   */
  public List<PlanFragment> createPlans(List<PlanFragment> plan) throws ImpalaException {

    // Start at the root of the PlanNode tree
    PlanNode root = plan.get(0).getPlanRoot();
    // Step 1: Compute the TupleCacheInfo for all PlanNodes
    root.computeTupleCacheInfo(ctx_.getRootAnalyzer().getDescTbl());

    // Step 2: Build up the new PlanNode tree with TupleCacheNodes added
    PlanNode newRoot = buildCachingPlan(root);
    // Since buildCachingPlan is modifying things in place, verify that the top-most plan
    // fragment's plan root matches with the newRoot returned.
    Preconditions.checkState(plan.get(0).getPlanRoot() == newRoot);

    // We may add some extra PlanNodes in the tree, but nothing we do will impact the
    // number of fragments or the shape of the plan. We are modifying the PlanFragments
    // in place and can just return the same list.
    return plan;
  }

  /**
   * Add TupleCacheNodes at every eligible location via a bottom-up traversal of the tree.
   */
  private PlanNode buildCachingPlan(PlanNode node) throws ImpalaException {
    // Recurse through the children applying the caching policy
    for (int i = 0; i < node.getChildCount(); i++) {
      node.setChild(i, buildCachingPlan(node.getChild(i)));
    }

    // If this node is not eligible, then we are done
    if (!node.getTupleCacheInfo().isEligible()) {
      return node;
    }

    // Should we cache above this node?
    // Simplest policy: always cache if eligible
    // TODO: Make this more complicated (e.g. cost calculations)
    if (LOG.isTraceEnabled()) {
      LOG.trace("Adding TupleCacheNode above node " + node.getId().toString());
    }
    // Allocate TupleCacheNode
    TupleCacheNode tupleCacheNode = new TupleCacheNode(ctx_.getNextNodeId(), node);
    tupleCacheNode.init(ctx_.getRootAnalyzer());
    PlanFragment curFragment = node.getFragment();
    if (node == curFragment.getPlanRoot()) {
      // If this is the top of a fragment, update the fragment plan root
      curFragment.addPlanRoot(tupleCacheNode);
      return tupleCacheNode;
    } else {
      // If this is not the top of a fragment, then set the fragment
      tupleCacheNode.setFragment(curFragment);
      return tupleCacheNode;
    }
  }
}
