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

import java.util.List;
import java.util.Set;

import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TSlotRef;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class SlotRef extends Expr {
  private final TableName tblName_;
  private final String col_;
  private final String label_;  // printed in toSql()

  // results of analysis
  private SlotDescriptor desc_;

  public SlotDescriptor getDesc() {
    Preconditions.checkState(isAnalyzed_);
    Preconditions.checkNotNull(desc_);
    return desc_;
  }

  public SlotId getSlotId() {
    Preconditions.checkState(isAnalyzed_);
    Preconditions.checkNotNull(desc_);
    return desc_.getId();
  }

  public SlotRef(TableName tblName, String col) {
    super();
    this.tblName_ = tblName;
    this.col_ = col;
    this.label_ = ToSqlUtils.getIdentSql(col);
  }

  // C'tor for a "pre-analyzed" ref to a slot
  public SlotRef(SlotDescriptor desc) {
    super();
    this.tblName_ = null;
    if (desc.getColumn() != null) {
      this.col_ = desc.getColumn().getName();
    } else {
      this.col_ = null;
    }
    this.isAnalyzed_ = true;
    this.desc_ = desc;
    this.type_ = desc.getType();
    String alias = desc.getParent().getAlias();
    this.label_ = (alias != null ? alias + "." : "") + desc.getLabel();
    this.numDistinctValues_ = desc.getStats().getNumDistinctValues();
  }

  /**
   * C'tor for cloning.
   */
  private SlotRef(SlotRef other) {
    super(other);
    tblName_ = other.tblName_;
    col_ = other.col_;
    label_ = other.label_;
    desc_ = other.desc_;
    type_ = other.type_;
    isAnalyzed_ = other.isAnalyzed_;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    desc_ = analyzer.registerColumnRef(tblName_, col_);
    type_ = desc_.getType();
    if (!type_.isSupported()) {
      throw new AnalysisException("Unsupported type '"
          + type_.toString() + "' in '" + toSql() + "'.");
    }
    if (type_.isInvalid()) {
      // In this case, the metastore contained a string we can't parse at all
      // e.g. map. We could report a better error if we stored the original
      // HMS string.
      throw new AnalysisException("Unsupported type in '" + toSql() + "'.");
    }
    numDistinctValues_ = desc_.getStats().getNumDistinctValues();
    if (type_.isBoolean()) selectivity_ = DEFAULT_SELECTIVITY;
    isAnalyzed_ = true;
  }

  @Override
  public String toSqlImpl() {
    if (tblName_ != null) {
      Preconditions.checkNotNull(label_);
      return tblName_.toSql() + "." + label_;
    } else if (label_ == null) {
      return "<slot " + Integer.toString(desc_.getId().asInt()) + ">";
    } else {
      return label_;
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.SLOT_REF;
    msg.slot_ref = new TSlotRef(desc_.getId().asInt());
    Preconditions.checkState(desc_.getParent().isMaterialized(),
        String.format("Illegal reference to non-materialized tuple: tid=%s",
            desc_.getParent().getId()));
    // we shouldn't be sending exprs over non-materialized slots
    Preconditions.checkState(desc_.isMaterialized(),
        String.format("Illegal reference to non-materialized slot: tid=%s sid=%s",
            desc_.getParent().getId(), desc_.getId()));
    // we also shouldn't have forgotten to compute the mem layout
    Preconditions.checkState(desc_.getByteOffset() != -1,
        String.format("Missing memory layout for tuple with tid=%s",
            desc_.getParent().getId()));
  }

  @Override
  public String debugString() {
    Objects.ToStringHelper toStrHelper = Objects.toStringHelper(this);
    String tblNameStr = (tblName_ == null ? "null" : tblName_.toString());
    toStrHelper.add("tblName", tblNameStr);
    toStrHelper.add("type", type_);
    toStrHelper.add("col", col_);
    String idStr = (desc_ == null ? "null" : Integer.toString(desc_.getId().asInt()));
    toStrHelper.add("id", idStr);
    return toStrHelper.toString();
  }

  @Override
  public int hashCode() {
    if (desc_ != null) return desc_.getId().hashCode();
    return Objects.hashCode(tblName_, (col_ == null) ? null : col_.toLowerCase());
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    SlotRef other = (SlotRef) obj;
    // check slot ids first; if they're both set we only need to compare those
    // (regardless of how the ref was constructed)
    if (desc_ != null && other.desc_ != null) {
      return desc_.getId().equals(other.desc_.getId());
    }
    if ((tblName_ == null) != (other.tblName_ == null)) return false;
    if (tblName_ != null && !tblName_.equals(other.tblName_)) return false;
    if ((col_ == null) != (other.col_ == null)) return false;
    if (col_ != null && !col_.toLowerCase().equals(other.col_.toLowerCase())) return false;
    return true;
  }

  @Override
  public boolean isBoundByTupleIds(List<TupleId> tids) {
    Preconditions.checkState(desc_ != null);
    for (TupleId tid: tids) {
      if (tid.equals(desc_.getParent().getId())) return true;
    }
    return false;
  }

  @Override
  public boolean isBoundBySlotIds(List<SlotId> slotIds) {
    Preconditions.checkState(isAnalyzed_);
    return slotIds.contains(desc_.getId());
  }

  @Override
  public void getIdsHelper(Set<TupleId> tupleIds, Set<SlotId> slotIds) {
    Preconditions.checkState(type_.isValid());
    Preconditions.checkState(desc_ != null);
    if (slotIds != null) slotIds.add(desc_.getId());
    if (tupleIds != null) tupleIds.add(desc_.getParent().getId());
  }

  public String getColumnName() { return col_; }

  @Override
  public Expr clone() { return new SlotRef(this); }

  @Override
  public String toString() {
    if (desc_ != null) {
      return "tid=" + desc_.getParent().getId() + " sid=" + desc_.getId();
    }
    return "no desc set";
  }

  @Override
  protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
    if (type_.isNull()) {
      // Hack to prevent null SlotRefs in the BE
      return NullLiteral.create(targetType);
    } else {
      return super.uncheckedCastTo(targetType);
    }
  }
}
