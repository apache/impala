// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

public class TupleId extends Id {
  public TupleId() {
    super();
  }

  public TupleId(int id) {
    super(id);
  }

  public List<TupleId> asList() {
    ArrayList<TupleId> list = Lists.newArrayList();
    list.add(this);
    return list;
  }
}
