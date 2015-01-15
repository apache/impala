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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Path.PathType;
import com.cloudera.impala.catalog.TableLoadingException;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TSlotRef;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class SlotRef extends Expr {
  private final static Logger LOG = LoggerFactory.getLogger(SlotRef.class);

  private final List<String> rawPath_;
  private final String label_;  // printed in toSql()

  // Results of analysis.
  private Path resolvedPath_;
  // Dot-separated path to a column or struct field; excludes db and table
  private String matchedPath_;
  private SlotDescriptor desc_;

  public SlotRef(ArrayList<String> rawPath) {
    super();
    rawPath_ = rawPath;
    matchedPath_ = null;
    label_ = ToSqlUtils.getPathSql(rawPath_);
  }

  /**
   * C'tor for a SlotRef on a resolved path.
   */
  public SlotRef(Path resolvedPath) {
    super();
    Preconditions.checkState(resolvedPath.isResolved());
    rawPath_ = resolvedPath.getFullyQualifiedRawPath();
    matchedPath_ = null;
    label_ = ToSqlUtils.getPathSql(rawPath_);
    resolvedPath_ = resolvedPath;
  }

  // C'tor for a "dummy" SlotRef used in substitution maps.
  public SlotRef(String alias) {
    super();
    rawPath_ = null;
    matchedPath_ = alias;
    label_ = ToSqlUtils.getIdentSql(alias.toLowerCase());
  }

  // C'tor for a "pre-analyzed" ref to a slot.
  public SlotRef(SlotDescriptor desc) {
    super();
    rawPath_ = null;
    // TODO: Add support for referencing a field within a complex type, possibly
    // by introducing a desc.getPath().
    if (desc.getColumn() != null) {
      matchedPath_ = desc.getColumn().getName();
    } else {
      matchedPath_ = null;
    }
    isAnalyzed_ = true;
    desc_ = desc;
    type_ = desc.getType();
    String alias = desc.getParent().getAlias();
    label_ = (alias != null ? alias + "." : "") + desc.getLabel();
    numDistinctValues_ = desc.getStats().getNumDistinctValues();
  }

  /**
   * C'tor for cloning.
   */
  private SlotRef(SlotRef other) {
    super(other);
    rawPath_ = other.rawPath_;
    label_ = other.label_;
    resolvedPath_ = other.resolvedPath_;
    matchedPath_ = other.matchedPath_;
    desc_ = other.desc_;
    type_ = other.type_;
    isAnalyzed_ = other.isAnalyzed_;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);

    if (resolvedPath_ == null) {
      try {
        resolvedPath_ = analyzer.resolvePath(rawPath_, PathType.SLOT_REF);
      } catch (TableLoadingException e) {
        // Should never happen because we only check registered table aliases.
        Preconditions.checkState(false);
      }
    }
    Preconditions.checkNotNull(resolvedPath_);
    matchedPath_ = Joiner.on(".").join(resolvedPath_.getRawPath());
    desc_ = analyzer.registerSlotRef(resolvedPath_);
    type_ = desc_.getType();
    if (!type_.isSupported()) {
      throw new AnalysisException("Unsupported type '"
          + type_.toSql() + "' in '" + toSql() + "'.");
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

  public Path getResolvedPath() {
    Preconditions.checkState(isAnalyzed_);
    return resolvedPath_;
  }

  @Override
  public String toSqlImpl() {
    if (label_ != null) return label_;
    if (rawPath_ != null) return ToSqlUtils.getPathSql(rawPath_);
    return "<slot " + Integer.toString(desc_.getId().asInt()) + ">";
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
    if (rawPath_ != null) toStrHelper.add("path", Joiner.on('.').join(rawPath_));
    toStrHelper.add("colName", matchedPath_);
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
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    SlotRef other = (SlotRef) obj;
    // check slot ids first; if they're both set we only need to compare those
    // (regardless of how the ref was constructed)
    if (desc_ != null && other.desc_ != null) {
      return desc_.getId().equals(other.desc_.getId());
    }
    if (matchedPath_ != null && other.matchedPath_ != null) {
      return matchedPath_.equals(other.matchedPath_);
    }
    if ((label_ == null) != (other.label_ == null)) return false;
    if (!label_.equalsIgnoreCase(other.label_)) return false;
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

  public String getMatchedPath() { return matchedPath_; }

  @Override
  public void resetAnalysisState() {
    super.resetAnalysisState();
    resolvedPath_ = null;
  }
}
