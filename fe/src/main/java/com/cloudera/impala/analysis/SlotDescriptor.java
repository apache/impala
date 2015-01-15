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

import java.util.Collections;
import java.util.List;

import jline.internal.Preconditions;

import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.thrift.TSlotDescriptor;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

public class SlotDescriptor {
  private final SlotId id_;
  private final TupleDescriptor parent_;
  // Path of slot relative to parent_. Only set for slots that represent a column/field.
  private List<Integer> path_;
  private Type type_;
  private Column column_;  // underlying column, if there is one
  private String label_; // for SlotRef.toSql() in absence of column name
  // Expr(s) materialized into this slot; multiple exprs for unions. Should be empty if
  // column_ is set.
  private List<Expr> sourceExprs_ = Lists.newArrayList();

  // if false, this slot doesn't need to be materialized in parent tuple
  // (and physical layout parameters are invalid)
  private boolean isMaterialized_ = false;

  // if false, this slot cannot be NULL
  private boolean isNullable_ = true;

  // physical layout parameters
  private int byteSize_;
  private int byteOffset_;  // within tuple
  private int nullIndicatorByte_;  // index into byte array
  private int nullIndicatorBit_; // index within byte
  private int slotIdx_;          // index within tuple struct

  private ColumnStats stats_;  // only set if 'column' isn't set

  SlotDescriptor(SlotId id, TupleDescriptor parent) {
    id_ = id;
    parent_ = parent;
    byteOffset_ = -1;  // invalid
  }

  SlotDescriptor(SlotId id, TupleDescriptor parent, SlotDescriptor src) {
    id_ = id;
    parent_ = parent;
    type_ = src.type_;
    column_ = src.column_;
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
  public void setPath(List<Integer> path) { path_ = path; }
  public Type getType() { return type_; }
  public void setType(Type type) { type_ = type; }
  public Column getColumn() { return column_; }
  public void setColumn(Column column) { column_ = column; }
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

  public ColumnStats getStats() {
    if (stats_ == null) {
      if (column_ != null) {
        stats_ = column_.getStats();
      } else {
        stats_ = new ColumnStats(type_);
      }
    }
    return stats_;
  }

  /**
   * Assembles the absolute physical path to this slot starting from the schema root.
   */
  public List<Integer> getAbsolutePath() {
    Preconditions.checkNotNull(parent_);
    if (parent_.getPath() == null) return Collections.emptyList();
    List<Integer> result = Lists.newArrayList(parent_.getPath().getAbsolutePath());
    result.addAll(path_);
    return result;
  }

  public TSlotDescriptor toThrift() {
    List<Integer> slotPath = getAbsolutePath();
    TSlotDescriptor result = new TSlotDescriptor(
        id_.asInt(), parent_.getId().asInt(), type_.toThrift(),
        slotPath, byteOffset_, nullIndicatorByte_, nullIndicatorBit_,
        slotIdx_, isMaterialized_);
    return result;
  }

  public String debugString() {
    String colStr = (column_ == null ? "null" : column_.getName());
    String typeStr = (type_ == null ? "null" : type_.toString());
    String pathStr = (path_ == null) ? "null" : Joiner.on(".").join(path_);
    return Objects.toStringHelper(this)
        .add("id", id_.asInt())
        .add("col", colStr)
        .add("path", pathStr)
        .add("type", typeStr)
        .add("materialized", isMaterialized_)
        .add("byteSize", byteSize_)
        .add("byteOffset", byteOffset_)
        .add("nullIndicatorByte", nullIndicatorByte_)
        .add("nullIndicatorBit", nullIndicatorBit_)
        .add("slotIdx", slotIdx_)
        .add("stats", stats_)
        .toString();
  }
}
