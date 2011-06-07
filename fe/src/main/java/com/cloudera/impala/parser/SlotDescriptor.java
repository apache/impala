// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.PrimitiveType;

public class SlotDescriptor {
  private SlotId id;
  private TupleDescriptor parent;
  private PrimitiveType type;
  private Column column;  // underlying column, if there is one

  SlotDescriptor(int id, TupleDescriptor parent) {
    this.id = new SlotId(id);
    this.parent = parent;
  }

  public SlotId getId() {
    return id;
  }

  public TupleDescriptor getParent() {
    return parent;
  }

  public PrimitiveType getType() {
    return type;
  }

  public void setType(PrimitiveType type) {
    this.type = type;
  }

  public Column getColumn() {
    return column;
  }

  public void setColumn(Column column) {
    this.column = column;
    this.type = column.getType();
  }
}
