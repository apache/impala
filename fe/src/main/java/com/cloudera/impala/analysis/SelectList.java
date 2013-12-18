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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * Select list items plus distinct clause.
 *
 */
public class SelectList {
  private final ArrayList<SelectListItem> items_ = Lists.newArrayList();
  private boolean isDistinct_;
  private boolean isStraightJoin_;

  public SelectList() {
    this.isDistinct_ = false;
    this.isStraightJoin_ = false;
  }

  public SelectList(List<SelectListItem> items) {
    isDistinct_ = false;
    isStraightJoin_ = false;
    this.items_.addAll(items);
  }

  public ArrayList<SelectListItem> getItems() { return items_; }
  public boolean isDistinct() { return isDistinct_; }
  public void setIsDistinct(boolean value) { isDistinct_ = value; }
  public boolean isStraightJoin() { return isStraightJoin_; }
  public void setIsStraightJoin(boolean value) { isStraightJoin_ = value; }

  @Override
  public SelectList clone() {
    SelectList clone = new SelectList();
    for (SelectListItem item: items_) {
      clone.items_.add(item.clone());
    }
    clone.setIsDistinct(isDistinct_);
    clone.setIsStraightJoin(isStraightJoin_);
    return clone;
  }
}
