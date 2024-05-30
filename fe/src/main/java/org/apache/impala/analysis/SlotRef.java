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
import java.util.List;
import java.util.Set;

import org.apache.impala.analysis.Path.PathType;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.TypeCompatibility;
import org.apache.impala.catalog.iceberg.IcebergMetadataTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ThriftSerializationCtx;
import org.apache.impala.common.UnsupportedFeatureException;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;
import org.apache.impala.thrift.TSlotRef;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

public class SlotRef extends Expr {
  protected List<String> rawPath_;
  protected final String label_;  // printed in toSql()

  // Results of analysis.
  protected SlotDescriptor desc_;

  // The resolved path after resolving 'rawPath_'.
  protected Path resolvedPath_ = null;

  // Indicates if this SlotRef is coming from zipping unnest where the unnest is given in
  // the FROM clause. Note, when the unnest is in the select list then an UnnestExpr
  // would be used instead of a SlotRef.
  protected boolean isZippingUnnest_ = false;

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
      resolvedPath_ = desc.getPath();
      rawPath_ = resolvedPath_.getRawPath();
    } else {
      rawPath_ = null;
    }
    desc_ = desc;
    type_ = desc.getType();
    evalCost_ = SLOT_REF_COST;
    String alias = desc.getParent().getAlias();
    label_ = (alias != null ? alias + "." : "") + desc.getLabel();
    numDistinctValues_ = adjustNumDistinctValues();

    if (type_.isStructType()) addStructChildrenAsSlotRefs();

    analysisDone();
  }

  /**
   * C'tor for cloning.
   */
  protected SlotRef(SlotRef other) {
    super(other);
    resolvedPath_ = other.resolvedPath_;
    if (other.rawPath_ != null) {
      // Instead of using the reference of 'other.rawPath_' clone its values into another
      // list.
      rawPath_ = new ArrayList<>();
      rawPath_.addAll(other.rawPath_);
    } else {
      rawPath_ = null;
    }
    label_ = other.label_;
    desc_ = other.desc_;
    isZippingUnnest_ = other.isZippingUnnest_;
  }

  /**
   * Applies an adjustment to an ndv of zero with nulls. NULLs aren't accounted for in the
   * ndv during stats computation. When computing cardinality in the cases where ndv is
   * zero and the slot is nullable we set the ndv to one to prevent the cardinalities from
   * zeroing out and leading to bad plans. Addressing IMPALA-7310 would include an extra
   * ndv for whenever nulls are present in general, not just in the case of a zero ndv.
   */
  private long adjustNumDistinctValues() {
    Preconditions.checkNotNull(desc_);
    Preconditions.checkNotNull(desc_.getStats());

    long numDistinctValues = desc_.getStats().getNumDistinctValues();
    // Adjust an ndv of zero to 1 if stats indicate there are null values.
    if (numDistinctValues == 0 && desc_.getIsNullable() &&
        (desc_.getStats().hasNulls() || !desc_.getStats().hasNullsStats())) {
      numDistinctValues = 1;
    }
    return numDistinctValues;
  }

  /**
   * Resetting a struct SlotRef remove its children as an analyzeImpl() on this
   * particular SlotRef will create the children again.
   */
  @Override
  public SlotRef reset() {
    if (type_.isStructType()) clearChildren();
    super.reset();
    return this;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    // TODO: derived slot refs (e.g., star-expanded) will not have rawPath set.
    // Change construction to properly handle such cases.
    Preconditions.checkState(rawPath_ != null);
    try {
      resolvedPath_ = analyzer.resolvePathWithMasking(rawPath_, PathType.SLOT_REF);
    } catch (TableLoadingException e) {
      // Should never happen because we only check registered table aliases.
      Preconditions.checkState(false);
    }

    Preconditions.checkNotNull(resolvedPath_);
    desc_ = analyzer.registerSlotRef(resolvedPath_, false /*duplicateIfCollections*/);
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
    // Register columns of a catalog table for column masking.
    if (!resolvedPath_.getMatchedTypes().isEmpty()) {
      analyzer.registerColumnForMasking(desc_);
    }

    numDistinctValues_ = adjustNumDistinctValues();
    FeTable rootTable = resolvedPath_.getRootTable();
    if (rootTable != null && rootTable.getNumRows() > 0) {
      // The NDV cannot exceed the #rows in the table.
      numDistinctValues_ = Math.min(numDistinctValues_, rootTable.getNumRows());
    }

    if (type_.isStructType()) {
      addStructChildrenAsSlotRefs();
      checkForUnsupportedStructFeatures();
    }
  }

  /**
   * Re-expands the struct: recreates the item tuple descriptor of 'desc_' and the child
   * 'SlotRef's, recursively.
   * Expects this 'SlotRef' to be a struct.
   */
  public void reExpandStruct(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(type_ != null && type_.isStructType());
    desc_.clearItemTupleDesc();
    children_.clear();

    analyzer.createStructTuplesAndSlotDescs(desc_);
    addStructChildrenAsSlotRefs();
    checkForUnsupportedStructFeatures();
  }

  // Throws an AnalysisException if any of the struct fields, recursively, of this SlotRef
  // is a collection or unsupported type or has any other unsupported feature.
  // Should only be used if this is a struct.
  public void checkForUnsupportedStructFeatures() throws AnalysisException {
    Preconditions.checkState(type_ instanceof StructType);
    for (Expr child : getChildren()) {
      final Type fieldType = child.getType();
      if (!fieldType.isSupported()) {
        throw new AnalysisException("Unsupported type '"
            + fieldType.toSql() + "' in '" + toSql() + "'.");
      }

      if (fieldType.isStructType()) {
        Preconditions.checkState(child instanceof SlotRef);
        ((SlotRef) child).checkForUnsupportedStructFeatures();
      }
    }
    if (resolvedPath_ != null) {
      FeTable rootTable = resolvedPath_.getRootTable();
      if (rootTable != null) {
        checkTableTypeSupportsStruct(rootTable);
        if (rootTable instanceof FeFsTable) {
          checkFileFormatSupportsStruct((FeFsTable)rootTable);
        }
      }
    }
  }

  private void checkTableTypeSupportsStruct(FeTable feTable) throws AnalysisException {
    if (!(feTable instanceof FeFsTable) &&
        !(feTable instanceof IcebergMetadataTable)) {
      throw new AnalysisException(
          String.format("%s is not supported when querying STRUCT type %s",
              feTable, type_.toSql()));
    }
  }

  // Throws exception if the given HdfsFileFormat does not support querying STRUCT types.
  // Iceberg tables also have ICEBERG as HdfsFileFormat. In case of Iceberg there is no
  // need to throw exception because the data file formats in the Iceberg table will be
  // also tested separately.
  private void checkFileFormatSupportsStruct(FeFsTable feFsTable)
      throws AnalysisException {
    for (HdfsFileFormat format : feFsTable.getFileFormats()) {
      if (! (format == HdfsFileFormat.PARQUET ||
           format == HdfsFileFormat.ORC ||
           format == HdfsFileFormat.ICEBERG)) {
        throw new AnalysisException("Querying STRUCT is only supported for ORC and "
            + "Parquet file formats.");
      }
    }
  }

  // Assumes this 'SlotRef' is a struct and that desc_.itemTupleDesc_ has already been
  // filled. Creates the children 'SlotRef's for the struct recursively.
  private void addStructChildrenAsSlotRefs() {
    Preconditions.checkState(desc_.getType().isStructType());
    TupleDescriptor structTuple = desc_.getItemTupleDesc();
    Preconditions.checkState(structTuple != null);
    for (SlotDescriptor childSlot : structTuple.getSlots()) {
      // If 'childSlot' is also a struct, the constructor will call this method on it.
      SlotRef childSlotRef = new SlotRef(childSlot);
      children_.add(childSlotRef);
    }
  }

  /**
   * The TreeNode.collect() function shouldn't iterate the children of this SlotRef if
   * this is a struct SlotRef. The desired functionality is to collect the struct
   * SlotRefs but not their children.
   */
  @Override
  protected boolean shouldCollectRecursively() {
    if (desc_ != null && desc_.getType().isStructType()) return false;
    return true;
  }

  @Override
  protected float computeEvalCost() {
    return SLOT_REF_COST;
  }

  @Override
  protected boolean isConstantImpl() { return false; }

  public boolean hasDesc() { return desc_ != null; }

  public SlotDescriptor getDesc() {
    Preconditions.checkState(isAnalyzed());
    Preconditions.checkNotNull(desc_);
    return desc_;
  }

  public List<String> getRawPath() { return rawPath_; }

  public SlotId getSlotId() {
    Preconditions.checkState(isAnalyzed());
    Preconditions.checkNotNull(desc_);
    return desc_.getId();
  }

  public Path getResolvedPath() {
    Preconditions.checkState(isAnalyzed());
    return desc_.getPath();
  }

  public void setIsZippingUnnest(boolean b) { isZippingUnnest_ = b; }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    if (label_ != null) return label_;
    if (rawPath_ != null) return ToSqlUtils.getPathSql(rawPath_);
    return "<slot " + Integer.toString(desc_.getId().asInt()) + ">";
  }

  @Override
  protected void toThrift(TExprNode msg) {
    Preconditions.checkState(false, "Unexpected use of old toThrift() signature");
  }

  @Override
  protected void toThrift(TExprNode msg, ThriftSerializationCtx serialCtx) {
    msg.node_type = TExprNodeType.SLOT_REF;
    msg.slot_ref = new TSlotRef(serialCtx.translateSlotId(desc_.getId()).asInt());
    // we shouldn't be sending exprs over non-materialized slots
    Preconditions.checkState(desc_.isMaterialized(), String.format(
        "Illegal reference to non-materialized slot: tid=%s sid=%s",
        desc_.getParent().getId(), desc_.getId()));
    Preconditions.checkState(desc_.getByteOffset() >= 0);
    // check that the tuples associated with this slot are executable
    desc_.getParent().checkIsExecutable();
    if (desc_.getItemTupleDesc() != null) desc_.getItemTupleDesc().checkIsExecutable();
  }

  @Override
  public String debugString() {
    MoreObjects.ToStringHelper toStrHelper = MoreObjects.toStringHelper(this);
    if (label_ != null) toStrHelper.add("label", label_);
    if (rawPath_ != null) toStrHelper.add("path", Joiner.on('.').join(rawPath_));
    toStrHelper.add("type", type_.toSql());
    String idStr = (desc_ == null ? "null" : Integer.toString(desc_.getId().asInt()));
    toStrHelper.add("id", idStr);
    return toStrHelper.toString();
  }

  /**
   * Overrides hashCode to ignore children, as done in
   * {@link Expr#matches(Expr, Comparator)}.
   */
  @Override
  public int hashCode() {
    if (desc_ != null) return desc_.getId().hashCode();
    if (label_ != null) return label_.toLowerCase().hashCode();
    return super.localHash();
  }

  @Override
  protected boolean localEquals(Expr that) {
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
  protected Expr substituteImpl(ExprSubstitutionMap smap, Analyzer analyzer) {
    if (smap != null) {
      Expr substExpr = smap.get(this);
      if (substExpr != null) return substExpr.clone();
    }

    // SlotRefs must remain analyzed to support substitution across query blocks,
    // therefore we do not call resetAnalysisState().
    substituteImplOnChildren(smap, analyzer);
    return this;
  }

  @Override
  public boolean isBoundByTupleIds(List<TupleId> tids) {
    Preconditions.checkState(desc_ != null);
    // If this SlotRef is coming from zipping unnest then try to do a similar check as
    // UnnestExpr does.
    if (isZippingUnnest_ && desc_.getParent() != null &&
        desc_.getParent().getRootDesc() != null) {
      TupleId parentId = desc_.getParent().getRootDesc().getId();
      for (TupleId tid: tids) {
        if (tid.equals(parentId)) return true;
      }
    }
    for (TupleDescriptor enclosingTupleDesc : desc_.getEnclosingTupleDescs()) {
      if (tids.contains(enclosingTupleDesc.getId())) return true;
    }
    return false;
  }

  @Override
  public boolean isBoundBySlotIds(List<SlotId> slotIds) {
    Preconditions.checkState(isAnalyzed());
    if (slotIds.contains(desc_.getId())) return true;
    for (SlotDescriptor enclosingSlotDesc : desc_.getEnclosingStructSlotDescs()) {
      if (slotIds.contains(enclosingSlotDesc.getId())) return true;
    }
    return false;
  }

  @Override
  public void getIdsHelper(Set<TupleId> tupleIds, Set<SlotId> slotIds) {
    Preconditions.checkState(type_.isValid());
    Preconditions.checkState(desc_ != null);
    if (slotIds != null) slotIds.add(desc_.getId());
    if (tupleIds != null) tupleIds.add(desc_.getParent().getId());

    // If we are a struct, we need to add all fields recursively so they are materialised.
    if (desc_.getType().isStructType() && slotIds != null) {
      TupleDescriptor itemTupleDesc = desc_.getItemTupleDesc();
      Preconditions.checkState(itemTupleDesc != null);
      itemTupleDesc.getSlotsRecursively().stream()
        .forEach(slotDesc -> slotIds.add(slotDesc.getId()));
    }
  }

  @Override
  public boolean referencesTuple(TupleId tid) {
    Preconditions.checkState(type_.isValid());
    Preconditions.checkState(desc_ != null);
    return desc_.getParent().getId() == tid;
  }

  @Override
  public Expr clone() {
    return new SlotRef(this);
  }

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
  protected Expr uncheckedCastTo(Type targetType, TypeCompatibility compatibility)
      throws AnalysisException {
    if (type_.isNull()) {
      // Hack to prevent null SlotRefs in the BE
      return NullLiteral.create(targetType);
    } else {
      return super.uncheckedCastTo(targetType, compatibility);
    }
  }

  // Return true since SlotRefs should be easy to access.
  @Override
  public boolean shouldConvertToCNF() { return true; }
}
