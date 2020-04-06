// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.StructType;
import org.apache.impala.thrift.TTupleDescriptor;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

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
 * Each tuple and slot descriptor has an associated unique id (within the scope of a
 * query). A given slot descriptor is owned by exactly one tuple descriptor.
 *
 * For example, every table reference has a corresponding tuple descriptor. The columns
 * of the table are represented by the tuple descriptor's type (struct type with one
 * field per column). The list of slots tracks which of the table's columns are actually
 * referenced. A similar explanation applies for collection references.
 *
 * A tuple descriptor may be materialized or non-materialized. A non-materialized tuple
 * descriptor acts as a placeholder for 'virtual' table references such as inline views,
 * and must not be materialized at runtime.
 *
 * Memory Layout
 * Slots are placed in descending order by size with trailing bytes to store null flags.
 * Null flags are omitted for non-nullable slots, with the following exceptions for Kudu
 * scan tuples to match Kudu's client row format: If there is at least one nullable Kudu
 * scan slot, then all slots (even non-nullable ones) get a null flag. If there are no
 * nullable Kudu scan slots, then there are also no null flags.
 * There is no padding between tuples when stored back-to-back in a row batch.
 *
 * Example: select bool_col, int_col, string_col, smallint_col from functional.alltypes
 * Slots:   string_col|int_col|smallint_col|bool_col|null_byte
 * Offsets: 0          12      16           18       19
 */
public class TupleDescriptor {
  // Padding size in bytes for Kudu string slots.
  private static final int KUDU_STRING_PADDING = 4;

  private final TupleId id_;
  private final String debugName_;  // debug-only
  private final List<SlotDescriptor> slots_ = new ArrayList<>();

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

  // Underlying masked table if this is the tuple of a table masking view.
  private BaseTableRef maskedTable_ = null;
  // Tuple of the table masking view that masks this tuple's table.
  private TupleDescriptor maskedByTuple_ = null;

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
  public List<SlotDescriptor> getSlots() { return slots_; }

  public List<SlotDescriptor> getMaterializedSlots() {
    List<SlotDescriptor> result = new ArrayList<>();
    for (SlotDescriptor slot: slots_) {
      if (slot.isMaterialized()) result.add(slot);
    }
    return result;
  }

  /**
   * Returns all materialized slots ordered by their offset. Valid to call after the
   * mem layout has been computed.
   */
  public List<SlotDescriptor> getSlotsOrderedByOffset() {
    Preconditions.checkState(hasMemLayout_);
    List<SlotDescriptor> result = getMaterializedSlots();
    Collections.sort(result, new Comparator<SlotDescriptor> () {
      @Override
      public int compare(SlotDescriptor a, SlotDescriptor b) {
        return Integer.compare(a.getByteOffset(), b.getByteOffset());
      }
    });
    return result;
  }

  public FeTable getTable() {
    if (path_ == null) return null;
    return path_.getRootTable();
  }

  public TableName getTableName() {
    FeTable t = getTable();
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

  public TupleDescriptor getMaskedByTuple() { return maskedByTuple_; }
  public BaseTableRef getMaskedTable() { return maskedTable_; }
  public void setMaskedTable(BaseTableRef table) {
    Preconditions.checkState(maskedTable_ == null);
    maskedTable_ = table;
    table.getDesc().maskedByTuple_ = this;
  }

  public String debugString() {
    String tblStr = (getTable() == null ? "null" : getTable().getFullName());
    List<String> slotStrings = new ArrayList<>();
    for (SlotDescriptor slot : slots_) {
      slotStrings.add(slot.debugString());
    }
    MoreObjects.ToStringHelper toStrHelper = MoreObjects.toStringHelper(this)
        .add("id", id_.asInt())
        .add("name", debugName_)
        .add("tbl", tblStr)
        .add("byte_size", byteSize_)
        .add("is_materialized", isMaterialized_)
        .add("slots", "[" + Joiner.on(", ").join(slotStrings) + "]");
    if (maskedTable_ != null) toStrHelper.add("masks", maskedTable_.getId());
    return toStrHelper.toString();
  }

  @Override
  public String toString() { return debugString(); }

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
    for (SlotDescriptor slot: slots_) slot.setIsMaterialized(true);
  }

  public TTupleDescriptor toThrift(Integer tableId) {
    TTupleDescriptor ttupleDesc =
        new TTupleDescriptor(id_.asInt(), byteSize_, numNullBytes_);
    if (tableId == null) return ttupleDesc;
    ttupleDesc.setTableId(tableId);
    Preconditions.checkNotNull(path_);
    ttupleDesc.setTuplePath(path_.getAbsolutePath());
    return ttupleDesc;
  }

  /**
   * In some cases changes are made to a tuple after the memory layout has been computed.
   * This function allows us to recompute the memory layout, if necessary. No-op if this
   * tuple does not have an existing mem layout.
   */
  public void recomputeMemLayout() {
    if (!hasMemLayout_) return;
    hasMemLayout_ = false;
    computeMemLayout();
  }

  public void computeMemLayout() {
    if (hasMemLayout_) return;
    hasMemLayout_ = true;

    boolean alwaysAddNullBit = hasNullableKuduScanSlots();
    avgSerializedSize_ = 0;

    // maps from slot size to slot descriptors with that size
    Map<Integer, List<SlotDescriptor>> slotsBySize = new HashMap<>();

    // populate slotsBySize
    int numNullBits = 0;
    int totalSlotSize = 0;
    for (SlotDescriptor d: slots_) {
      if (!d.isMaterialized()) continue;
      ColumnStats stats = d.getStats();
      int slotSize = d.getType().getSlotSize();

      if (stats.hasAvgSize()) {
        avgSerializedSize_ += d.getStats().getAvgSerializedSize();
      } else {
        // TODO: for computed slots, try to come up with stats estimates
        avgSerializedSize_ += slotSize;
      }
      // Add padding for a KUDU string slot.
      if (d.isKuduStringSlot()) {
        slotSize += KUDU_STRING_PADDING;
        avgSerializedSize_ += KUDU_STRING_PADDING;
      }
      if (!slotsBySize.containsKey(slotSize)) {
        slotsBySize.put(slotSize, new ArrayList<>());
      }
      totalSlotSize += slotSize;
      slotsBySize.get(slotSize).add(d);
      if (d.getIsNullable() || alwaysAddNullBit) ++numNullBits;
    }
    // we shouldn't have anything of size <= 0
    Preconditions.checkState(!slotsBySize.containsKey(0));
    Preconditions.checkState(!slotsBySize.containsKey(-1));

    // assign offsets to slots in order of descending size
    numNullBytes_ = (numNullBits + 7) / 8;
    int slotOffset = 0;
    int nullIndicatorByte = totalSlotSize;
    int nullIndicatorBit = 0;
    // slotIdx is the index into the resulting tuple struct.  The first (largest) field
    // is 0, next is 1, etc.
    int slotIdx = 0;
    // sort slots in descending order of size
    List<Integer> sortedSizes = new ArrayList<>(slotsBySize.keySet());
    Collections.sort(sortedSizes, Collections.reverseOrder());
    for (int slotSize: sortedSizes) {
      if (slotsBySize.get(slotSize).isEmpty()) continue;
      for (SlotDescriptor d: slotsBySize.get(slotSize)) {
        Preconditions.checkState(d.isMaterialized());
        d.setByteSize(slotSize);
        d.setByteOffset(slotOffset);
        d.setSlotIdx(slotIdx++);
        slotOffset += slotSize;

        // assign null indicator
        if (d.getIsNullable() || alwaysAddNullBit) {
          d.setNullIndicatorByte(nullIndicatorByte);
          d.setNullIndicatorBit(nullIndicatorBit);
          nullIndicatorBit = (nullIndicatorBit + 1) % 8;
          if (nullIndicatorBit == 0) ++nullIndicatorByte;
        }
        // non-nullable slots have 0 for the byte offset and -1 for the bit mask
        // to make sure IS NULL always evaluates to false in the BE without having
        // to check nullability explicitly
        if (!d.getIsNullable()) {
          d.setNullIndicatorBit(-1);
          d.setNullIndicatorByte(0);
        }
      }
    }
    Preconditions.checkState(slotOffset == totalSlotSize);

    byteSize_ = totalSlotSize + numNullBytes_;
  }

  /**
   * Returns true if this tuple has at least one materialized nullable Kudu scan slot.
   */
  private boolean hasNullableKuduScanSlots() {
    if (!(getTable() instanceof FeKuduTable)) return false;
    for (SlotDescriptor d: slots_) {
      if (d.isMaterialized() && d.getIsNullable()) return true;
    }
    return false;
  }

  /**
   * Return true if the slots being materialized are all partition columns.
   */
  public boolean hasClusteringColsOnly() {
    FeTable table = getTable();
    if (!(table instanceof FeFsTable) || table.getNumClusteringCols() == 0) return false;

    FeFsTable hdfsTable = (FeFsTable)table;
    for (SlotDescriptor slotDesc: getSlots()) {
      if (!slotDesc.isMaterialized()) continue;
      if (slotDesc.getColumn() == null ||
          !hdfsTable.isClusteringColumn(slotDesc.getColumn())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns a list of slot ids that correspond to partition columns.
   */
  public List<SlotId> getPartitionSlots() {
    List<SlotId> partitionSlots = new ArrayList<>();
    for (SlotDescriptor slotDesc: getSlots()) {
      if (slotDesc.getColumn() == null) continue;
      if (slotDesc.getColumn().getPosition() < getTable().getNumClusteringCols()) {
        partitionSlots.add(slotDesc.getId());
      }
    }
    return partitionSlots;
  }

  /**
   * Returns true if the tuple has any variable-length slots.
   */
  public boolean hasVarLenSlots() {
    for (SlotDescriptor slot: slots_) {
      if (!slot.getType().isFixedLengthType()) return true;
    }
    return false;
  }
}
