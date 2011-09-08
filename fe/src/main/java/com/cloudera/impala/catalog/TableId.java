// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.common.Id;
import com.google.common.collect.Lists;

public class TableId extends Id {
  public TableId() {
    super();
  }

  public TableId(int id) {
    super(id);
  }

  public List<TableId> asList() {
    ArrayList<TableId> list = Lists.newArrayList();
    list.add(this);
    return list;
  }
}
