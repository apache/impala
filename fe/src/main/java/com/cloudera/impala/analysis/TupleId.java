// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.ArrayList;

import com.google.common.collect.Lists;

public class TupleId extends Id {
  public TupleId() {
    super();
  }

  public TupleId(int id) {
    super(id);
  }

  public ArrayList<TupleId> asList() {
    ArrayList<TupleId> list = Lists.newArrayList();
    list.add(this);
    return list;
  }
}
