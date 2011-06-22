// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

public class SlotId extends Id {
  public SlotId() {
    super();
  }

  public SlotId(int id) {
    super(id);
  }

  public List<SlotId> asList() {
    ArrayList<SlotId> list = Lists.newArrayList();
    list.add(this);
    return list;
  }
}
