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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TSlotDescriptor;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class SlotDescriptor {
  private final SlotId id_;
  private final TupleDescriptor parent_;

  // Resolved path to the column/field corresponding to this slot descriptor, if any,
  // Only set for slots that represent a column/field materialized in a scan.
  private Path path_;
  private Type type_;

  // Tuple descriptor for collection items. Only set if type_ is an array or map.
  private TupleDescriptor itemTupleDesc_;

  // for SlotRef.toSql() in the absence of a path
  private String label_;

  // Expr(s) materialized into this slot; multiple exprs for unions. Should be empty if
  // path_ is set.
  private List<Expr> sourceExprs_ = new ArrayList<>();

  // if false, this slot doesn't need to be materialized in parent tuple
  // (and physical layout parameters are invalid)
  private boolean isMaterialized_ = false;

  // if false, this slot cannot be NULL
  // Note: it is still possible that a SlotRef pointing to this descriptor could have a
  // NULL value if the entire tuple is NULL, for example as the result of an outer join.
  private boolean isNullable_ = true;

  // physical layout parameters
  private int byteSize_;
  private int byteOffset_;  // within tuple
  private int nullIndicatorByte_;  // index into byte array
  private int nullIndicatorBit_; // index within byte
  private int slotIdx_;          // index within tuple struct

  private ColumnStats stats_;  // only set if 'column' isn't set

  SlotDescriptor(SlotId id, TupleDescriptor parent) {
    Preconditions.checkNotNull(id);
    Preconditions.checkNotNull(parent);
    id_ = id;
    parent_ = parent;
    byteOffset_ = -1;  // invalid
  }

  SlotDescriptor(SlotId id, TupleDescriptor parent, SlotDescriptor src) {
    Preconditions.checkNotNull(id);
    Preconditions.checkNotNull(parent);
    id_ = id;
    parent_ = parent;
    type_ = src.type_;
    itemTupleDesc_ = src.itemTupleDesc_;
    path_ = src.path_;
    label_ = src.label_;
    sourceExprs_ = src.sourceExprs_;
    isMaterialized_ = src.isMaterialized_;
    isNullable_ = src.isNullable_;
    byteSize_ = src.byteSize_;
    byteOffset_ = src.byteOffset_;
    nullIndicatorByte_ = src.nullIndicatorByte_;
    nullIndicatorBit_ = src.nullIndicatorBit_;
    slotIdx_ = src.slotIdx_;
    stats_ = src.stats_;
  }

  public int getNullIndicatorByte() { return nullIndicatorByte_; }
  public void setNullIndicatorByte(int nullIndicatorByte) {
    this.nullIndicatorByte_ = nullIndicatorByte;
  }
  public int getNullIndicatorBit() { return nullIndicatorBit_; }
  public void setNullIndicatorBit(int nullIndicatorBit) {
    this.nullIndicatorBit_ = nullIndicatorBit;
  }
  public SlotId getId() { return id_; }
  public TupleDescriptor getParent() { return parent_; }
  public Type getType() { return type_; }
  public void setType(Type type) { type_ = type; }
  public TupleDescriptor getItemTupleDesc() { return itemTupleDesc_; }
  public void setItemTupleDesc(TupleDescriptor t) {
    Preconditions.checkState(
        itemTupleDesc_ == null, "Item tuple descriptor already set.");
    itemTupleDesc_ = t;
  }
  public boolean isMaterialized() { return isMaterialized_; }
  public void setIsMaterialized(boolean value) { isMaterialized_ = value; }
  public boolean getIsNullable() { return isNullable_; }
  public void setIsNullable(boolean value) { isNullable_ = value; }
  public int getByteSize() { return byteSize_; }
  public void setByteSize(int byteSize) { this.byteSize_ = byteSize; }
  public int getByteOffset() { return byteOffset_; }
  public void setByteOffset(int byteOffset) { this.byteOffset_ = byteOffset; }
  public void setSlotIdx(int slotIdx) { this.slotIdx_ = slotIdx; }
  public String getLabel() { return label_; }
  public void setLabel(String label) { label_ = label; }
  public void setSourceExprs(List<Expr> exprs) { sourceExprs_ = exprs; }
  public void setSourceExpr(Expr expr) { sourceExprs_ = Collections.singletonList(expr); }
  public void addSourceExpr(Expr expr) { sourceExprs_.add(expr); }
  public List<Expr> getSourceExprs() { return sourceExprs_; }
  public void setStats(ColumnStats stats) { this.stats_ = stats; }

  public void setPath(Path path) {
    Preconditions.checkNotNull(path);
    Preconditions.checkState(path.isRootedAtTuple());
    Preconditions.checkState(path.getRootDesc() == parent_);
    path_ = path;
    type_ = path_.destType();
    label_ = Joiner.on(".").join(path.getRawPath());

    // Set nullability, if this refers to a KuduColumn.
    if (path_.destColumn() instanceof KuduColumn) {
      KuduColumn kuduColumn = (KuduColumn)path_.destColumn();
      isNullable_ = kuduColumn.isNullable();
    }
  }

  public Path getPath() { return path_; }
  public boolean isScanSlot() { return path_ != null && path_.isRootedAtTable(); }
  public Column getColumn() { return !isScanSlot() ? null : path_.destColumn(); }

  public ColumnStats getStats() {
    if (stats_ == null) {
      Column c = getColumn();
      if (c != null) {
        stats_ = c.getStats();
      } else {
        stats_ = new ColumnStats(type_);
      }
    }
    return stats_;
  }

  /**
   * Checks if this descriptor describes  an array "pos" pseudo-column.
   *
   * Note: checking whether the column is null distinguishes between top-level columns
   * and nested types. This check more specifically looks just for a reference to the
   * "pos" field of an array type.
   */
  public boolean isArrayPosRef() {
    if (parent_ == null) return false;
    Type parentType = parent_.getType();
    if (parentType instanceof CollectionStructType) {
      if (((CollectionStructType)parentType).isArrayStruct() &&
          label_.equals(Path.ARRAY_POS_FIELD_NAME)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if this slot is of STRING type in a kudu table.
   */
  public boolean isKuduStringSlot() {
    if (getParent() == null) return false;
    if (!(getParent().getTable() instanceof FeKuduTable)) return false;
    return getType().isStringType();
  }

  /**
   * Assembles the absolute materialized path to this slot starting from the schema
   * root. The materialized path points to the first non-struct schema element along the
   * path starting from the parent's tuple path to this slot's path.
   *
   * The materialized path is used to determine when a new tuple (containing a new
   * instance of this slot) should be created. A tuple is emitted for every data item
   * pointed to by the materialized path. For scalar slots this trivially means that every
   * data item goes into a different tuple. For collection slots, the materialized path
   * specifies how many data items go into a single collection value.
   *
   * For scalar slots, the materialized path is the same as its path. However, for
   * collection slots, the materialized path may be different than path_. This happens
   * when the query materializes a "flattened" collection composed of concatenated nested
   * collections.
   *
   * For example, given the table:
   *   CREATE TABLE tbl (id bigint, outer_array array<array<int>>);
   *
   * And the query:
   *   select id, inner_array.item from tbl t, t.outer_array.item inner_array
   *
   * The path 't.outer_array.item' corresponds to the absolute path [1,0]. However, the
   * 'inner_array' slot appears in the table-level tuple, with tuplePath [] (i.e. one
   * tuple materialized per table row). There is a single array materialized per
   * 'outer_array', not per 'inner_array'. Thus the materializedPath for this slot will be
   * [1], not [1,0].
   */
  public List<Integer> getMaterializedPath() {
    Preconditions.checkNotNull(parent_);
    // A slot descriptor typically only has a path if the parent also has one.
    // However, we sometimes materialize inline-view tuples when generating plan trees
    // with EmptySetNode portions. In that case, a slot descriptor could have a non-empty
    // path pointing into the inline-view tuple (which has no path).
    if (!isScanSlot() || parent_.getPath() == null) return Collections.emptyList();
    Preconditions.checkState(path_.isResolved());

    List<Integer> materializedPath = Lists.newArrayList(path_.getAbsolutePath());
    // For scalar types, the materialized path is the same as path_
    if (type_.isScalarType()) return materializedPath;
    Preconditions.checkState(type_.isCollectionType());
    Preconditions.checkState(path_.getFirstCollectionIndex() != -1);
    // Truncate materializedPath after first collection element
    // 'offset' adjusts for the index returned by path_.getFirstCollectionIndex() being
    // relative to path_.getRootDesc()
    int offset = !path_.isRootedAtTuple() ? 0 :
        path_.getRootDesc().getPath().getAbsolutePath().size();
    materializedPath.subList(
        offset + path_.getFirstCollectionIndex() + 1, materializedPath.size()).clear();
    return materializedPath;
  }

  /**
   * Initializes a slot by setting its source expression information
   */
  public void initFromExpr(Expr expr) {
    setLabel(expr.toSql());
    Preconditions.checkState(sourceExprs_.isEmpty());
    setSourceExpr(expr);
    setStats(ColumnStats.fromExpr(expr));
    Preconditions.checkState(expr.getType().isValid());
    setType(expr.getType());
  }

  /**
   * Return true if the physical layout of this descriptor matches the physical layout
   * of the other descriptor, but not necessarily ids.
   */
  public boolean LayoutEquals(SlotDescriptor other) {
    if (!getType().equals(other.getType())) return false;
    if (isNullable_ != other.isNullable_) return false;
    if (getByteSize() != other.getByteSize()) return false;
    if (getByteOffset() != other.getByteOffset()) return false;
    if (getNullIndicatorByte() != other.getNullIndicatorByte()) return false;
    if (getNullIndicatorBit() != other.getNullIndicatorBit()) return false;
    return true;
  }

  public TSlotDescriptor toThrift() {
    Preconditions.checkState(isMaterialized_);
    List<Integer> materializedPath = getMaterializedPath();
    TSlotDescriptor result = new TSlotDescriptor(
        id_.asInt(), parent_.getId().asInt(), type_.toThrift(),
        materializedPath, byteOffset_, nullIndicatorByte_, nullIndicatorBit_,
        slotIdx_);
    if (itemTupleDesc_ != null) {
      // Check for recursive or otherwise invalid item tuple descriptors. Since we assign
      // tuple ids globally in increasing order, the id of an item tuple descriptor must
      // always have been generated after the parent tuple id if the tuple/slot belong
      // to a base table. For example, tuples/slots introduced during planning do not
      // have such a guarantee.
      Preconditions.checkState(!isScanSlot() ||
          itemTupleDesc_.getId().asInt() > parent_.getId().asInt());
      result.setItemTupleId(itemTupleDesc_.getId().asInt());
    }
    return result;
  }

  public static String debugString(Collection<SlotDescriptor> slots) {
    if (slots == null || slots.isEmpty()) return "";
    List<String> strings = new ArrayList<>();
    for (SlotDescriptor slot: slots) {
      strings.add(slot.debugString());
    }
    return Joiner.on("\n").join(strings);
  }

  public String debugString() {
    String pathStr = (path_ == null) ? "null" : path_.toString();
    String typeStr = (type_ == null ? "null" : type_.toString());
    return Objects.toStringHelper(this)
        .add("id", id_.asInt())
        .add("path", pathStr)
        .add("label", label_)
        .add("type", typeStr)
        .add("materialized", isMaterialized_)
        .add("byteSize", byteSize_)
        .add("byteOffset", byteOffset_)
        .add("nullable", isNullable_)
        .add("nullIndicatorByte", nullIndicatorByte_)
        .add("nullIndicatorBit", nullIndicatorBit_)
        .add("slotIdx", slotIdx_)
        .add("stats", stats_)
        .toString();
  }

  @Override
  public String toString() { return debugString(); }
}
