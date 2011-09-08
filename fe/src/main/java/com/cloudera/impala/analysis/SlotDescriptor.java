// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.thrift.TSlotDescriptor;
import com.google.common.base.Objects;

public class SlotDescriptor {
  private final SlotId id;
  private final TupleDescriptor parent;
  private PrimitiveType type;
  private Column column;  // underlying column, if there is one

  // if false, this slot doesn't need to be materialized in parent tuple
  // (and physical layout parameters are invalid)
  private boolean isMaterialized; 

  // physical layout parameters
  private int byteSize;
  private int byteOffset;  // within tuple
  private int nullIndicatorByte;  // index into byte array
  private int nullIndicatorBit; // index within byte

  SlotDescriptor(int id, TupleDescriptor parent) {
    this.id = new SlotId(id);
    this.parent = parent;
    this.byteOffset = -1;  // invalid
    this.isMaterialized = true;
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

  public boolean getIsMaterialized() {
    return isMaterialized;
  }

  public void setIsMaterialized(boolean value) {
    isMaterialized = value;
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

  public TSlotDescriptor toThrift() {
    return new TSlotDescriptor(
        id.asInt(), parent.getId().asInt(), type.toThrift(),
        ((column != null) ? column.getPosition() : -1),
        byteOffset, nullIndicatorByte, nullIndicatorBit, isMaterialized);
  }

  public String debugString() {
    String colStr = (column == null ? "null" : column.getName());
    String typeStr = (type == null ? "null" : type.toString());
    return Objects.toStringHelper(this)
        .add("id", id.asInt())
        .add("col", colStr)
        .add("type", typeStr)
        .add("materialized", isMaterialized)
        .add("byteSize", byteSize)
        .add("byteOffset", byteOffset)
        .add("nullIndicatorByte", nullIndicatorByte)
        .add("nullIndicatorBit", nullIndicatorBit)
        .toString();
  }
}
