// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import com.google.common.collect.Lists;

/**
 * Select list items plus distinct clause.
 *
 */
class SelectList {
  private final ArrayList<SelectListItem> items = Lists.newArrayList();
  private boolean isDistinct;

  public SelectList() {
    super();
    this.isDistinct = false;
  }

  public ArrayList<SelectListItem> getItems() {
    return items;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  public void setIsDistinct(boolean value) {
    isDistinct = value;
  }
}
