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

import java.util.List;
import java.util.Set;

import org.apache.impala.analysis.Path.PathType;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.UnsupportedFeatureException;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;
import org.apache.impala.thrift.TSlotRef;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class SlotRef extends Expr {
  private final List<String> rawPath_;
  private final String label_;  // printed in toSql()

  // Results of analysis.
  private SlotDescriptor desc_;

  public SlotRef(List<String> rawPath) {
    super();
    rawPath_ = rawPath;
    label_ = ToSqlUtils.getPathSql(rawPath_);
  }

  /**
   * C'tor for a "dummy" SlotRef used in substitution maps.
   */
  public SlotRef(String alias) {
    super();
    rawPath_ = null;
    // Relies on the label_ being compared in equals().
    label_ = ToSqlUtils.getIdentSql(alias.toLowerCase());
  }

  /**
   * C'tor for a "pre-analyzed" ref to a slot.
   */
  public SlotRef(SlotDescriptor desc) {
    super();
    if (desc.isScanSlot()) {
      rawPath_ = desc.getPath().getRawPath();
    } else {
      rawPath_ = null;
    }
    desc_ = desc;
    type_ = desc.getType();
    evalCost_ = SLOT_REF_COST;
    String alias = desc.getParent().getAlias();
    label_ = (alias != null ? alias + "." : "") + desc.getLabel();
    numDistinctValues_ = desc.getStats().getNumDistinctValues();
    analysisDone();
  }

  /**
   * C'tor for cloning.
   */
  private SlotRef(SlotRef other) {
    super(other);
    rawPath_ = other.rawPath_;
    label_ = other.label_;
    desc_ = other.desc_;
    type_ = other.type_;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    // TODO: derived slot refs (e.g., star-expanded) will not have rawPath set.
    // Change construction to properly handle such cases.
    Preconditions.checkState(rawPath_ != null);
    Path resolvedPath = null;
    try {
      resolvedPath = analyzer.resolvePath(rawPath_, PathType.SLOT_REF);
    } catch (TableLoadingException e) {
      // Should never happen because we only check registered table aliases.
      Preconditions.checkState(false);
    }
    Preconditions.checkNotNull(resolvedPath);
    desc_ = analyzer.registerSlotRef(resolvedPath);
    type_ = desc_.getType();
    if (!type_.isSupported()) {
      throw new UnsupportedFeatureException("Unsupported type '"
          + type_.toSql() + "' in '" + toSql() + "'.");
    }
    if (type_.isInvalid()) {
      // In this case, the metastore contained a string we can't parse at all
      // e.g. map. We could report a better error if we stored the original
      // HMS string.
      throw new UnsupportedFeatureException("Unsupported type in '" + toSql() + "'.");
    }

    numDistinctValues_ = desc_.getStats().getNumDistinctValues();
    FeTable rootTable = resolvedPath.getRootTable();
    if (rootTable != null && rootTable.getNumRows() > 0) {
      // The NDV cannot exceed the #rows in the table.
      numDistinctValues_ = Math.min(numDistinctValues_, rootTable.getNumRows());
    }
  }

  @Override
  protected float computeEvalCost() {
    return SLOT_REF_COST;
  }

  @Override
  protected boolean isConstantImpl() { return false; }

  public SlotDescriptor getDesc() {
    Preconditions.checkState(isAnalyzed());
    Preconditions.checkNotNull(desc_);
    return desc_;
  }

  public SlotId getSlotId() {
    Preconditions.checkState(isAnalyzed());
    Preconditions.checkNotNull(desc_);
    return desc_.getId();
  }

  public Path getResolvedPath() {
    Preconditions.checkState(isAnalyzed());
    return desc_.getPath();
  }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    if (label_ != null) return label_;
    if (rawPath_ != null) return ToSqlUtils.getPathSql(rawPath_);
    return "<slot " + Integer.toString(desc_.getId().asInt()) + ">";
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.SLOT_REF;
    msg.slot_ref = new TSlotRef(desc_.getId().asInt());
    // we shouldn't be sending exprs over non-materialized slots
    Preconditions.checkState(desc_.isMaterialized(), String.format(
        "Illegal reference to non-materialized slot: tid=%s sid=%s",
        desc_.getParent().getId(), desc_.getId()));
    // check that the tuples associated with this slot are executable
    desc_.getParent().checkIsExecutable();
    if (desc_.getItemTupleDesc() != null) desc_.getItemTupleDesc().checkIsExecutable();
  }

  @Override
  public String debugString() {
    Objects.ToStringHelper toStrHelper = Objects.toStringHelper(this);
    if (label_ != null) toStrHelper.add("label", label_);
    if (rawPath_ != null) toStrHelper.add("path", Joiner.on('.').join(rawPath_));
    toStrHelper.add("type", type_.toSql());
    String idStr = (desc_ == null ? "null" : Integer.toString(desc_.getId().asInt()));
    toStrHelper.add("id", idStr);
    return toStrHelper.toString();
  }

  @Override
  public int hashCode() {
    if (desc_ != null) return desc_.getId().hashCode();
    return Objects.hashCode(Joiner.on('.').join(rawPath_).toLowerCase());
  }

  @Override
  public boolean localEquals(Expr that) {
    if (!super.localEquals(that)) return false;
    SlotRef other = (SlotRef) that;
    // check slot ids first; if they're both set we only need to compare those
    // (regardless of how the ref was constructed)
    if (desc_ != null && other.desc_ != null) {
      return desc_.getId().equals(other.desc_.getId());
    }
    return label_ == null ? other.label_ == null : label_.equalsIgnoreCase(other.label_);
  }

  /** Used for {@link Expr#matches(Expr, Comparator)} */
  interface Comparator {
    boolean matches(SlotRef a, SlotRef b);
  }

  /**
   * A wrapper around localEquals() used for {@link #Expr#matches(Expr, Comparator)}.
   */
  static final Comparator SLOTREF_EQ_CMP = new Comparator() {
    @Override
    public boolean matches(SlotRef a, SlotRef b) { return a.localEquals(b); }
  };

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
    Preconditions.checkState(isAnalyzed());
    return slotIds.contains(desc_.getId());
  }

  @Override
  public void getIdsHelper(Set<TupleId> tupleIds, Set<SlotId> slotIds) {
    Preconditions.checkState(type_.isValid());
    Preconditions.checkState(desc_ != null);
    if (slotIds != null) slotIds.add(desc_.getId());
    if (tupleIds != null) tupleIds.add(desc_.getParent().getId());
  }

  @Override
  public Expr clone() { return new SlotRef(this); }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    if (rawPath_ != null) {
      buf.append(String.join(".", rawPath_));
    } else if (label_ != null) {
      buf.append(label_);
    }
    boolean closeParen = buf.length() > 0;
    if (closeParen) buf.append(" (");
    if (desc_ != null) {
      buf.append("tid=")
        .append(desc_.getParent().getId())
        .append(" sid=")
        .append(desc_.getId());
    } else {
      buf.append("no desc set");
    }
    if (closeParen) buf.append(")");
    return buf.toString();
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
