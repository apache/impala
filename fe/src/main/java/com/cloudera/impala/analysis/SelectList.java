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

package com.cloudera.impala.analysis;

import java.util.List;

import com.google.common.collect.Lists;

/**
 * Select list items plus optional distinct clause and optional plan hints.
 */
public class SelectList {
  private List<String> planHints_;
  private boolean isDistinct_;

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  private final List<SelectListItem> items_;

  // Set in analyzePlanHints() based on planHints_.
  private boolean isStraightJoin_;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  public SelectList(List<SelectListItem> items) {
    isDistinct_ = false;
    items_ = items;
    isStraightJoin_ = false;
  }

  public SelectList() {
    isDistinct_ = false;
    items_ = Lists.newArrayList();
    isStraightJoin_ = false;
  }

  public SelectList(List<SelectListItem> items, boolean isDistinct,
      List<String> planHints) {
    isDistinct_ = isDistinct;
    isStraightJoin_ = false;
    items_ = items;
    planHints_ = planHints;
  }

  /**
   * C'tor for cloning.
   */
  public SelectList(SelectList other) {
    planHints_ =
        (other.planHints_ != null) ? Lists.newArrayList(other.planHints_) : null;
    items_ = Lists.newArrayList();
    for (SelectListItem item: other.items_) {
      items_.add(item.clone());
    }
    isDistinct_ = other.isDistinct_;
    isStraightJoin_ = other.isStraightJoin_;
  }

  public List<SelectListItem> getItems() { return items_; }
  public void setPlanHints(List<String> planHints) { planHints_ = planHints; }
  public List<String> getPlanHints() { return planHints_; }
  public boolean isDistinct() { return isDistinct_; }
  public void setIsDistinct(boolean value) { isDistinct_ = value; }
  public boolean isStraightJoin() { return isStraightJoin_; }
  public boolean hasPlanHints() { return planHints_ != null; }

  public void analyzePlanHints(Analyzer analyzer) {
    if (planHints_ == null) return;
    for (String hint: planHints_) {
      if (!hint.equalsIgnoreCase("straight_join")) {
        analyzer.addWarning("PLAN hint not recognized: " + hint);
      }
      isStraightJoin_ = true;
      analyzer.setHasPlanHints();
    }
  }

  @Override
  public SelectList clone() { return new SelectList(this); }

  public void reset() {
    for (SelectListItem item: items_) {
      if (!item.isStar()) item.getExpr().reset();
    }
    isStraightJoin_ = false;
  }
}
