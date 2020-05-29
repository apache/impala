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

package org.apache.impala.analysis;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.rewrite.ExprRewriter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Select list items plus optional distinct clause and optional plan hints.
 */
public class SelectList {
  private List<PlanHint> planHints_ = new ArrayList<>();
  private boolean isDistinct_;

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  private final List<SelectListItem> items_;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  public SelectList(List<SelectListItem> items) {
    isDistinct_ = false;
    items_ = items;
  }

  public SelectList() {
    isDistinct_ = false;
    items_ = new ArrayList<>();
  }

  public SelectList(List<SelectListItem> items, boolean isDistinct,
      List<PlanHint> planHints) {
    isDistinct_ = isDistinct;
    items_ = items;
    planHints_ = planHints;
  }

  /**
   * C'tor for cloning.
   */
  public SelectList(SelectList other) {
    planHints_ = Lists.newArrayList(other.planHints_);
    items_ = new ArrayList<>();
    for (SelectListItem item: other.items_) {
      items_.add(item.clone());
    }
    isDistinct_ = other.isDistinct_;
  }

  public List<SelectListItem> getItems() { return items_; }

  public void setPlanHints(List<PlanHint> planHints) {
    Preconditions.checkNotNull(planHints);
    planHints_ = planHints;
  }

  public List<PlanHint> getPlanHints() { return planHints_; }
  public boolean isDistinct() { return isDistinct_; }
  public void setIsDistinct(boolean value) { isDistinct_ = value; }
  public boolean hasPlanHints() { return !planHints_.isEmpty(); }

  public void analyzePlanHints(Analyzer analyzer) {
    for (PlanHint hint : planHints_) {
      if (hint.is("straight_join")) {
        analyzer.setIsStraightJoin();
      } else {
        analyzer.addWarning("PLAN hint not recognized: " + hint);
      }
    }
  }

  public void rewriteExprs(ExprRewriter rewriter, Analyzer analyzer)
      throws AnalysisException {
    List<Subquery> subqueryExprs = new ArrayList<>();
    for (SelectListItem item: items_) {
      if (item.isStar()) continue;
      item.setExpr(rewriter.rewrite(item.getExpr(), analyzer));
      item.getExpr().collect(Subquery.class, subqueryExprs);
    }
    for (Subquery s : subqueryExprs) {
      s.getStatement().rewriteExprs(rewriter);
    }
  }

  @Override
  public SelectList clone() { return new SelectList(this); }

  public void reset() {
    for (SelectListItem item: items_) {
      if (!item.isStar()) item.setExpr(item.getExpr().reset());
    }
  }
}
