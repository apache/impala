// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.thrift.TTupleDescriptor;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class TupleDescriptor {
  private final TupleId id;
  private final ArrayList<SlotDescriptor> slots;
  private Table table;  // underlying table, if there is one

  private int byteSize;  // of all slots plus null indicators

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

  public int getByteSize() {
    return byteSize;
  }

  public String debugString() {
    String tblStr = (table == null ? "null" : table.getFullName());
    List<String> slotStrings = Lists.newArrayList();
    for (SlotDescriptor slot : slots) {
      slotStrings.add(slot.debugString());
    }
    return Objects.toStringHelper(this)
        .add("tuple_id", id.asInt())
        .add("tbl", tblStr)
        .add("slots", "[" + Joiner.on(", ").join(slotStrings) + "]")
        .toString();
  }

  public TTupleDescriptor toThrift() {
    TTupleDescriptor ttupleDesc = new TTupleDescriptor(id.asInt(), byteSize);
    if (table != null) {
      ttupleDesc.setTableId(table.getId().asInt());
    }
    return ttupleDesc;
  }

  protected void computeMemLayout() {
    // sort slots by size
    List<List<SlotDescriptor>> slotsBySize =
        Lists.newArrayListWithCapacity(PrimitiveType.getMaxSlotSize());
    for (int i = 0; i <= PrimitiveType.getMaxSlotSize(); ++i) {
      slotsBySize.add(new ArrayList<SlotDescriptor>());
    }

    int numNullableSlots = 0;
    for (SlotDescriptor d: slots) {
      if (d.getIsMaterialized()) {
        slotsBySize.get(d.getType().getSlotSize()).add(d);
        if (d.getIsNullable()) {
          ++numNullableSlots;
        }
      }
    }
    // we shouldn't have anything of size 0
    Preconditions.checkState(slotsBySize.get(0).isEmpty());

    // assign offsets to slots in order of ascending size
    int numNullIndicatorBytes = (numNullableSlots + 7) / 8;
    int offset = numNullIndicatorBytes;
    int nullIndicatorByte = 0;
    int nullIndicatorBit = 0;
    for (int slotSize = 1; slotSize <= PrimitiveType.getMaxSlotSize(); ++slotSize) {
      if (slotsBySize.get(slotSize).isEmpty()) {
        continue;
      }
      if (slotSize > 1) {
        // insert padding
        offset = (offset + slotSize - 1) / slotSize * slotSize;
      }

      for (SlotDescriptor d: slotsBySize.get(slotSize)) {
        d.setByteSize(slotSize);
        d.setByteOffset(offset);
        offset += slotSize;

        // assign null indicator
        if (d.getIsNullable()) {
          d.setNullIndicatorByte(nullIndicatorByte);
          d.setNullIndicatorBit(nullIndicatorBit);
          nullIndicatorBit = (nullIndicatorBit + 1) % 8;
          if (nullIndicatorBit == 0) {
            ++nullIndicatorByte;
          }
        } else {
          // Non-nullable slots will have 0 for the byte offset and -1 for the bit mask
          d.setNullIndicatorBit(-1);
          d.setNullIndicatorByte(0);
        }
      }
    }

    this.byteSize = offset;
  }
}
