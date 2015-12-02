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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.StructType;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.thrift.TTupleDescriptor;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A collection of slots that are organized in a CPU-friendly memory layout. A slot is
 * a typed placeholder for a single value operated on at runtime. A slot can be named or
 * anonymous. A named slot corresponds directly to a column or field that can be directly
 * referenced in a query by its name. An anonymous slot represents an intermediate value
 * produced during query execution, e.g., aggregation output.
 * A tuple descriptor has an associated type and a list of slots. Its type is a struct
 * that contains as its fields the list of all named slots covered by this tuple.
 * The list of slots tracks the named slots that are actually referenced in a query, as
 * well as all anonymous slots. Although not required, a tuple descriptor typically
 * only has named or anonymous slots and not a mix of both.
 *
 * For example, every table reference has a corresponding tuple descriptor. The columns
 * of the table are represented by the tuple descriptor's type (struct type with one
 * field per column). The list of slots tracks which of the table's columns are actually
 * referenced. A similar explanation applies for collection references.
 *
 * A tuple descriptor may be materialized or non-materialized. A non-materialized tuple
 * descriptor acts as a placeholder for 'virtual' table references such as inline views,
 * and must not be materialized at runtime.
 */
public class TupleDescriptor {
  private final TupleId id_;
  private final String debugName_;  // debug-only
  private final ArrayList<SlotDescriptor> slots_ = Lists.newArrayList();

  // Resolved path to the collection corresponding to this tuple descriptor, if any,
  // Only set for materialized tuples.
  private Path path_;

  // Type of this tuple descriptor. Used for slot/table resolution in analysis.
  private StructType type_;

  // All legal aliases of this tuple.
  private String[] aliases_;

  // If true, requires that aliases_.length() == 1. However, aliases_.length() == 1
  // does not imply an explicit alias because nested collection refs have only a
  // single implicit alias.
  private boolean hasExplicitAlias_;

  // If false, this tuple doesn't need to be materialized.
  private boolean isMaterialized_ = true;

  // If true, computeMemLayout() has been called and we can't add any additional slots.
  private boolean hasMemLayout_ = false;

  private int byteSize_;  // of all slots plus null indicators
  private int numNullBytes_;
  private float avgSerializedSize_;  // in bytes; includes serialization overhead

  public TupleDescriptor(TupleId id, String debugName) {
    id_ = id;
    path_ = null;
    debugName_ = debugName;
  }

  public void addSlot(SlotDescriptor desc) {
    Preconditions.checkState(!hasMemLayout_);
    slots_.add(desc);
  }

  public TupleId getId() { return id_; }
  public ArrayList<SlotDescriptor> getSlots() { return slots_; }
  public Table getTable() {
    if (path_ == null) return null;
    return path_.getRootTable();
  }
  public TableName getTableName() {
    Table t = getTable();
    return (t == null) ? null : t.getTableName();
  }
  public void setPath(Path p) {
    Preconditions.checkNotNull(p);
    Preconditions.checkState(p.isResolved());
    Preconditions.checkState(p.destType().isCollectionType());
    path_ = p;
    if (p.destTable() != null) {
      // Do not use Path.getTypeAsStruct() to only allow implicit path resolutions,
      // because this tuple desc belongs to a base table ref.
      type_ = (StructType) p.destTable().getType().getItemType();
    } else {
      // Also allow explicit path resolutions.
      type_ = Path.getTypeAsStruct(p.destType());
    }
  }
  public Path getPath() { return path_; }
  public void setType(StructType type) { type_ = type; }
  public StructType getType() { return type_; }
  public int getByteSize() { return byteSize_; }
  public float getAvgSerializedSize() { return avgSerializedSize_; }
  public boolean isMaterialized() { return isMaterialized_; }
  public void setIsMaterialized(boolean value) { isMaterialized_ = value; }
  public boolean hasMemLayout() { return hasMemLayout_; }
  public void setAliases(String[] aliases, boolean hasExplicitAlias) {
    aliases_ = aliases;
    hasExplicitAlias_ = hasExplicitAlias;
  }
  public boolean hasExplicitAlias() { return hasExplicitAlias_; }
  public String getAlias() { return (aliases_ != null) ? aliases_[0] : null; }
  public TableName getAliasAsName() {
    return (aliases_ != null) ? new TableName(null, aliases_[0]) : null;
  }

  public TupleDescriptor getRootDesc() {
    if (path_ == null) return null;
    return path_.getRootDesc();
  }

  public String debugString() {
    String tblStr = (getTable() == null ? "null" : getTable().getFullName());
    List<String> slotStrings = Lists.newArrayList();
    for (SlotDescriptor slot : slots_) {
      slotStrings.add(slot.debugString());
    }
    return Objects.toStringHelper(this)
        .add("id", id_.asInt())
        .add("name", debugName_)
        .add("tbl", tblStr)
        .add("byte_size", byteSize_)
        .add("is_materialized", isMaterialized_)
        .add("slots", "[" + Joiner.on(", ").join(slotStrings) + "]")
        .toString();
  }

  /**
   * Checks that this tuple is materialized and has a mem layout. Throws if this tuple
   * is not executable, i.e., if one of those conditions is not met.
   */
  public void checkIsExecutable() {
    Preconditions.checkState(isMaterialized_, String.format(
        "Illegal reference to non-materialized tuple: debugname=%s alias=%s tid=%s",
        debugName_, StringUtils.defaultIfEmpty(getAlias(), "n/a"), id_));
    Preconditions.checkState(hasMemLayout_, String.format(
        "Missing memory layout for tuple: debugname=%s alias=%s tid=%s",
        debugName_, StringUtils.defaultIfEmpty(getAlias(), "n/a"), id_));
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
    // do not set the table id or tuple path for views
    if (getTable() != null && !(getTable() instanceof View)) {
      ttupleDesc.setTableId(getTable().getId().asInt());
      Preconditions.checkNotNull(path_);
      ttupleDesc.setTuplePath(path_.getAbsolutePath());
    }
    return ttupleDesc;
  }

  public void computeMemLayout() {
    if (hasMemLayout_) return;
    hasMemLayout_ = true;

    // sort slots by size
    Map<Integer, List<SlotDescriptor>> slotsBySize =
        new HashMap<Integer, List<SlotDescriptor>>();

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
      if (!slotsBySize.containsKey(d.getType().getSlotSize())) {
        slotsBySize.put(d.getType().getSlotSize(), new ArrayList<SlotDescriptor>());
      }
      slotsBySize.get(d.getType().getSlotSize()).add(d);
      if (d.getIsNullable()) ++numNullableSlots;
    }
    // we shouldn't have anything of size <= 0
    Preconditions.checkState(!slotsBySize.containsKey(0));
    Preconditions.checkState(!slotsBySize.containsKey(-1));

    // assign offsets to slots in order of ascending size
    numNullBytes_ = (numNullableSlots + 7) / 8;
    int offset = numNullBytes_;
    int nullIndicatorByte = 0;
    int nullIndicatorBit = 0;
    // slotIdx is the index into the resulting tuple struct.  The first (smallest) field
    // is 0, next is 1, etc.
    int slotIdx = 0;
    List<Integer> sortedSizes = new ArrayList<Integer>(slotsBySize.keySet());
    Collections.sort(sortedSizes);
    for (int slotSize: sortedSizes) {
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
   * Return true if the slots being materialized are all partition columns.
   */
  public boolean hasClusteringColsOnly() {
    Table table = getTable();
    if (!(table instanceof HdfsTable) || table.getNumClusteringCols() == 0) return false;

    HdfsTable hdfsTable = (HdfsTable)table;
    for (SlotDescriptor slotDesc: getSlots()) {
      if (!slotDesc.isMaterialized()) continue;
      if (slotDesc.getColumn() == null ||
          slotDesc.getColumn().getPosition() >= hdfsTable.getNumClusteringCols()) {
        return false;
      }
    }
    return true;
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
