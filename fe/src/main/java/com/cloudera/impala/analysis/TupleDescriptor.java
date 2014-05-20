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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.thrift.TTupleDescriptor;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class TupleDescriptor {
  private final static Logger LOG = LoggerFactory.getLogger(TupleDescriptor.class);
  private final TupleId id_;
  private String alias_;
  private final ArrayList<SlotDescriptor> slots_;
  private Table table_;  // underlying table, if there is one

  // if false, this tuple doesn't need to be materialized
  private boolean isMaterialized_ = true;

  private int byteSize_;  // of all slots plus null indicators
  private int numNullBytes_;

  // if true, computeMemLayout() has been called and we can't add any additional
  // slots
  private boolean hasMemLayout_ = false;

  private float avgSerializedSize_;  // in bytes; includes serialization overhead

  TupleDescriptor(TupleId id) {
    this.id_ = id;
    this.slots_ = new ArrayList<SlotDescriptor>();
  }

  public void addSlot(SlotDescriptor desc) {
    Preconditions.checkState(!hasMemLayout_);
    slots_.add(desc);
  }

  public TupleId getId() { return id_; }
  public ArrayList<SlotDescriptor> getSlots() { return slots_; }
  public Table getTable() { return table_; }
  public TableName getTableName() {
    if (table_ == null) return null;
    return new TableName(
        table_.getDb() != null ? table_.getDb().getName() : null, table_.getName());
  }
  public void setTable(Table tbl) { table_ = tbl; }
  public int getByteSize() { return byteSize_; }
  public float getAvgSerializedSize() { return avgSerializedSize_; }
  public boolean getIsMaterialized() { return isMaterialized_; }
  public void setIsMaterialized(boolean value) { isMaterialized_ = value; }
  public String getAlias() { return alias_; }
  public void setAlias(String alias) { alias_ = alias; }

  public String debugString() {
    String tblStr = (table_ == null ? "null" : table_.getFullName());
    List<String> slotStrings = Lists.newArrayList();
    for (SlotDescriptor slot : slots_) {
      slotStrings.add(slot.debugString());
    }
    return Objects.toStringHelper(this)
        .add("id", id_.asInt())
        .add("tbl", tblStr)
        .add("byte_size", byteSize_)
        .add("is_materialized", isMaterialized_)
        .add("slots", "[" + Joiner.on(", ").join(slotStrings) + "]")
        .toString();
  }

  /**
   * Materialize all slots.
   */
  public void materializeSlots() {
    for (SlotDescriptor slot: slots_) {
      slot.setIsMaterialized(true);
    }
  }

  public TTupleDescriptor toThrift() {
    TTupleDescriptor ttupleDesc =
        new TTupleDescriptor(id_.asInt(), byteSize_, numNullBytes_);
    // do not set the table id for virtual tables such as views and inline views
    if (table_ != null && !table_.isVirtualTable()) {
      ttupleDesc.setTableId(table_.getId().asInt());
    }
    return ttupleDesc;
  }

  public void computeMemLayout() {
    if (hasMemLayout_) return;
    hasMemLayout_ = true;

    // sort slots by size
    List<List<SlotDescriptor>> slotsBySize =
        Lists.newArrayListWithCapacity(PrimitiveType.getMaxSlotSize());
    for (int i = 0; i <= PrimitiveType.getMaxSlotSize(); ++i) {
      slotsBySize.add(new ArrayList<SlotDescriptor>());
    }

    // populate slotsBySize; also compute avgSerializedSize
    int numNullableSlots = 0;
    for (SlotDescriptor d: slots_) {
      if (!d.isMaterialized()) continue;
      ColumnStats stats = d.getStats();
      if (stats.hasAvgSerializedSize()) {
        avgSerializedSize_ += d.getStats().getAvgSerializedSize();
      } else {
        // TODO: for computed slots, try to come up with stats estimates
        avgSerializedSize_ += d.getType().getSlotSize();
      }
      slotsBySize.get(d.getType().getSlotSize()).add(d);
      if (d.getIsNullable()) ++numNullableSlots;
    }
    // we shouldn't have anything of size 0
    Preconditions.checkState(slotsBySize.get(0).isEmpty());

    // assign offsets to slots in order of ascending size
    numNullBytes_ = (numNullableSlots + 7) / 8;
    int offset = numNullBytes_;
    int nullIndicatorByte = 0;
    int nullIndicatorBit = 0;
    // slotIdx is the index into the resulting tuple struct.  The first (smallest) field
    // is 0, next is 1, etc.
    int slotIdx = 0;
    for (int slotSize = 1; slotSize <= PrimitiveType.getMaxSlotSize(); ++slotSize) {
      if (slotsBySize.get(slotSize).isEmpty()) continue;
      if (slotSize > 1) {
        // insert padding
        int alignTo = Math.min(slotSize, 8);
        offset = (offset + alignTo - 1) / alignTo * alignTo;
      }

      for (SlotDescriptor d: slotsBySize.get(slotSize)) {
        Preconditions.checkState(d.isMaterialized());
        d.setByteSize(slotSize);
        d.setByteOffset(offset);
        d.setSlotIdx(slotIdx++);
        offset += slotSize;

        // assign null indicator
        if (d.getIsNullable()) {
          d.setNullIndicatorByte(nullIndicatorByte);
          d.setNullIndicatorBit(nullIndicatorBit);
          nullIndicatorBit = (nullIndicatorBit + 1) % 8;
          if (nullIndicatorBit == 0) ++nullIndicatorByte;
        } else {
          // Non-nullable slots will have 0 for the byte offset and -1 for the bit mask
          d.setNullIndicatorBit(-1);
          d.setNullIndicatorByte(0);
        }
      }
    }

    this.byteSize_ = offset;
  }

  /**
   * Returns true if tuples of type 'this' can be assigned to tuples of type 'desc'
   * (checks that both have the same number of slots and that slots are of the same type)
   */
  public boolean isCompatible(TupleDescriptor desc) {
    if (slots_.size() != desc.slots_.size()) return false;
    for (int i = 0; i < slots_.size(); ++i) {
      if (!slots_.get(i).getType().equals(desc.slots_.get(i).getType())) return false;
    }
    return true;
  }
}
