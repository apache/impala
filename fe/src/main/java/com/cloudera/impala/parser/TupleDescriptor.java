// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import java.util.ArrayList;

import com.cloudera.impala.catalog.Table;

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
}
