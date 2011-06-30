// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.PrimitiveType;
import com.google.common.base.Objects;

public class SlotDescriptor {
  private final SlotId id;
  private final TupleDescriptor parent;
  private PrimitiveType type;
  private Column column;  // underlying column, if there is one

  // physical layout parameters
  private int byteSize;
  private int byteOffset;  // within tuple
  private int nullIndicatorByte;  // index into byte array
  private int nullIndicatorBit; // index within byte

  SlotDescriptor(int id, TupleDescriptor parent) {
    this.id = new SlotId(id);
    this.parent = parent;
  }

  public int getNullIndicatorByte() {
    return nullIndicatorByte;
  }

  public void setNullIndicatorByte(int nullIndicatorByte) {
    this.nullIndicatorByte = nullIndicatorByte;
  }

  public int getNullIndicatorBit() {
    return nullIndicatorBit;
  }

  public void setNullIndicatorBit(int nullIndicatorBit) {
    this.nullIndicatorBit = nullIndicatorBit;
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

  public int getByteSize() {
    return byteSize;
  }

  public void setByteSize(int byteSize) {
    this.byteSize = byteSize;
  }

  public int getByteOffset() {
    return byteOffset;
  }

  public void setByteOffset(int byteOffset) {
    this.byteOffset = byteOffset;
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
