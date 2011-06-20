// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.PrimitiveType;
import com.google.common.base.Objects;

public class SlotDescriptor {
  private final SlotId id;
  private final TupleDescriptor parent;
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

  public String debugString() {
    String colStr = (column == null ? "null" : column.getName());
    String typeStr = (type == null ? "null" : type.toString());
    return Objects.toStringHelper(this)
        .add("id", id.getId())
        .add("col", colStr)
        .add("type", typeStr)
        .toString();
  }
}
