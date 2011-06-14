// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.catalog.Table;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class TupleDescriptor {
  private final TupleId id;
  private final ArrayList<SlotDescriptor> slots;
  private Table table;  // underlying table, if there is one

  TupleDescriptor(int id) {
    this.id = new TupleId(id);
    this.slots = new ArrayList<SlotDescriptor>();
  }

  public void addSlot(SlotDescriptor desc) {
    slots.add(desc);
  }

  public TupleId getId() {
    return id;
  }

  public ArrayList<SlotDescriptor> getSlots() {
    return slots;
  }

  public Table getTable() {
    return table;
  }

  public void setTable(Table tbl) {
    table = tbl;
  }

  public String debugString() {
    StringBuilder output = new StringBuilder("[tuple_id=");
    output.append(id.getId());
    output.append(" tbl=" + (table == null ? "null" : table.getFullName()));
    output.append(" slots=(");
    List<String> strings = Lists.newArrayList();
    for (SlotDescriptor slot: slots) {
      strings.add(slot.debugString());
    }
    output.append(Joiner.on(", ").join(strings) + ")]");
    return output.toString();
  }
}
