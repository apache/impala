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

import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.catalog.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Check whether a query is trivial or not. A trivial query is allowed for an immediate
 * admission even in the case that the admission pool runs out of the memory or hits the
 * limit of the request number.
 *
 */
public class TrivialQueryChecker {
  private final static Logger LOG = LoggerFactory.getLogger(TrivialQueryChecker.class);

  /**
   * Check whether query meets the general requirements that a trivial query must have.
   */
  private static boolean PassedMustHave(PlanFragment rootFragment) {
    List<PlanFragment> allFragments = rootFragment.getNodesPostOrder();
    if (allFragments.size() != 1) return false;
    if (!(rootFragment.getSink() instanceof PlanRootSink)) return false;
    PlanNode planRoot = rootFragment.getPlanRoot();
    if (planRoot.numNodes() != 1) return false;
    if (planRoot instanceof UnionNode) {
      // The trivial query would return 0 or 1 row, otherwise return false.
      if (((UnionNode) planRoot).constExprLists_.size() > 1
          || ((UnionNode) planRoot).resultExprLists_.size() > 0) {
        return false;
      }
    } else if (!(rootFragment.getPlanRoot() instanceof EmptySetNode)) {
      return false;
    }
    return true;
  }

  /**
   * A helper method to check whether there is a sleep function expression inside the
   * expression list. Used by PassedSpecialCheck() only.
   */
  private static boolean HasFunctionSleep(List<Expr> exprList) {
    if (exprList == null) return false;
    List<FunctionCallExpr> sleepFuncList = new ArrayList<>();
    for (Expr expr : exprList) {
      if (expr == null) continue;
      expr.collectAll(Expr.IS_FN_SLEEP, sleepFuncList);
      if (sleepFuncList.size() > 0) return true;
    }
    return false;
  }

  /**
   * Check whether the query meets the special requirements of a trivial query.
   * Should only be called after passing PassedMustHave().
   */
  private static boolean PassedSpecialCheck(PlanFragment rootFragment) {
    // If contains sleep function, we don't consider it to be trivial, because it can
    // sleep for a long time and doesn't meet the original idea of the setting of
    // trivial queries.
    // Also a lot of testcases use sleep for testing, it is better to treat it as a
    // normal query.
    PlanNode planRoot = rootFragment.getPlanRoot();
    Preconditions.checkArgument(planRoot.numNodes() == 1);
    if (planRoot instanceof UnionNode) {
      UnionNode unionNode = (UnionNode) planRoot;
      Preconditions.checkArgument(unionNode.resultExprLists_.size() == 0);
      for (List<Expr> constList : unionNode.constExprLists_) {
        if (HasFunctionSleep(constList)) return false;
      }
    } else {
      // Must be an EmptySetNode if it is not a UnionNode.
      Preconditions.checkArgument(rootFragment.getPlanRoot() instanceof EmptySetNode);
    }
    return true;
  }

  /**
   * Returns whether the query is trivial. Used for admission controller.
   */
  public static boolean IsTrivial(
      PlanFragment rootFragment, TQueryOptions queryOptions, boolean isQueryStmt) {
    if (!queryOptions.isEnable_trivial_query_for_admission() || !isQueryStmt) {
      return false;
    }
    if (!PassedMustHave(rootFragment)) return false;
    return PassedSpecialCheck(rootFragment);
  }
}
