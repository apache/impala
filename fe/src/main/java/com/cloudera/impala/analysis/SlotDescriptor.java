// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.thrift.TSlotDescriptor;
import com.google.common.base.Objects;

public class SlotDescriptor {
  private final SlotId id;
  private final TupleDescriptor parent;
  private PrimitiveType type;
  private Column column;  // underlying column, if there is one
  private String label;  // for SlotRef.toSql() in absence of column name

  // if false, this slot doesn't need to be materialized in parent tuple
  // (and physical layout parameters are invalid)
  private boolean isMaterialized;

  // if false, this slot cannot be NULL
  private boolean isNullable;

  // physical layout parameters
  private int byteSize;
  private int byteOffset;  // within tuple
  private int nullIndicatorByte;  // index into byte array
  private int nullIndicatorBit; // index within byte
  private int slotIdx;          // index within tuple struct

  private ColumnStats stats;  // only set if 'column' isn't set

  SlotDescriptor(int id, TupleDescriptor parent) {
    this.id = new SlotId(id);
    this.parent = parent;
    this.byteOffset = -1;  // invalid
    this.isMaterialized = false;
    this.isNullable = true;
  }

  public int getNullIndicatorByte() { return nullIndicatorByte; }
  public void setNullIndicatorByte(int nullIndicatorByte) {
    this.nullIndicatorByte = nullIndicatorByte;
  }
  public int getNullIndicatorBit() { return nullIndicatorBit; }
  public void setNullIndicatorBit(int nullIndicatorBit) {
    this.nullIndicatorBit = nullIndicatorBit;
  }
  public SlotId getId() { return id; }
  public TupleDescriptor getParent() { return parent; }
  public PrimitiveType getType() { return type; }
  public void setType(PrimitiveType type) { this.type = type; }
  public Column getColumn() { return column; }
  public void setColumn(Column column) {
    this.column = column;
    this.type = column.getType();
  }
  public boolean getIsMaterialized() { return isMaterialized; }
  public void setIsMaterialized(boolean value) { isMaterialized = value; }
  public boolean getIsNullable() { return isNullable; }
  public void setIsNullable(boolean value) { isNullable = value; }
  public int getByteSize() { return byteSize; }
  public void setByteSize(int byteSize) { this.byteSize = byteSize; }
  public int getByteOffset() { return byteOffset; }
  public void setByteOffset(int byteOffset) { this.byteOffset = byteOffset; }
  public void setSlotIdx(int slotIdx) { this.slotIdx = slotIdx; }
  public String getLabel() { return label; }
  public void setLabel(String label) { this.label = label; }
  public void setStats(ColumnStats stats) { this.stats = stats; }

  public ColumnStats getStats() {
    if (stats == null) {
      if (column != null) {
        stats = column.getStats();
      } else {
        stats = new ColumnStats(type);
      }
    }
    return stats;
  }

  public TSlotDescriptor toThrift() {
    return new TSlotDescriptor(
        id.asInt(), parent.getId().asInt(), type.toThrift(),
        ((column != null) ? column.getPosition() : -1),
        byteOffset, nullIndicatorByte, nullIndicatorBit,
        slotIdx, isMaterialized);
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
        .add("slotIdx", slotIdx)
        .toString();
  }
}
