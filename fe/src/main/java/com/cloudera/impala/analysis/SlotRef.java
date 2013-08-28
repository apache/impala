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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TSlotRef;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class SlotRef extends Expr {
  private final static Logger LOG = LoggerFactory.getLogger(SlotRef.class);

  private final TableName tblName;
  private final String col;
  private final String label;  // printed in toSql()

  // results of analysis
  private SlotDescriptor desc;

  public SlotDescriptor getDesc() {
    Preconditions.checkState(isAnalyzed);
    Preconditions.checkNotNull(desc);
    return desc;
  }

  public SlotId getSlotId() {
    Preconditions.checkState(isAnalyzed);
    Preconditions.checkNotNull(desc);
    return desc.getId();
  }

  public SlotRef(TableName tblName, String col) {
    super();
    this.tblName = tblName;
    this.col = col;
    this.label = ToSqlUtils.getHiveIdentSql(col);
  }

  // C'tor for a "pre-analyzed" ref to a slot
  public SlotRef(SlotDescriptor desc) {
    super();
    this.tblName = null;
    if (desc.getColumn() != null) {
      this.col = desc.getColumn().getName();
    } else {
      this.col = null;
    }
    this.isAnalyzed = true;
    this.desc = desc;
    this.type = desc.getType();
    String alias = desc.getParent().getAlias();
    //this.label =  desc.getLabel();
    // TODO: should this simply be the SlotDescriptor's label?
    this.label = (alias != null ? alias + "." : "") + desc.getLabel();
    this.numDistinctValues = desc.getStats().getNumDistinctValues();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (isAnalyzed) return;
    super.analyze(analyzer);
    desc = analyzer.registerColumnRef(tblName, col);
    type = desc.getType();
    if (!type.isSupported()) {
      throw new AnalysisException("Unsupported type '"
          + type.toString() + "' in '" + toSql() + "'.");
    }
    numDistinctValues = desc.getStats().getNumDistinctValues();
    if (type == PrimitiveType.BOOLEAN) selectivity = defaultSelectivity;
  }

  @Override
  public String toSqlImpl() {
    if (tblName != null) {
      Preconditions.checkNotNull(label);
      return tblName.toSql() + "." + label;
    } else if (label == null) {
      return "<slot " + Integer.toString(desc.getId().asInt()) + ">";
    } else {
      return label;
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.SLOT_REF;
    msg.slot_ref = new TSlotRef(desc.getId().asInt());
    // we shouldn't be sending exprs over non-materialized slots
    Preconditions.checkState(desc.isMaterialized());
    // we also shouldn't have forgotten to compute the mem layout
    Preconditions.checkState(desc.getByteOffset() != -1);
  }

  @Override
  public String debugString() {
    Objects.ToStringHelper toStrHelper = Objects.toStringHelper(this);
    String tblNameStr = (tblName == null ? "null" : tblName.toString());
    toStrHelper.add("tblName", tblNameStr);
    toStrHelper.add("col", col);
    String idStr = (desc == null ? "null" : Integer.toString(desc.getId().asInt()));
    toStrHelper.add("id", idStr);
    return toStrHelper.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    SlotRef other = (SlotRef) obj;
    // check slot ids first; if they're both set we only need to compare those
    // (regardless of how the ref was constructed)
    if (desc != null && other.desc != null) {
      return desc.getId().equals(other.desc.getId());
    }
    if ((tblName == null) != (other.tblName == null)) return false;
    if (tblName != null && !tblName.equals(other.tblName)) return false;
    if ((col == null) != (other.col == null)) return false;
    if (col != null && !col.toLowerCase().equals(other.col.toLowerCase())) return false;
    return true;
  }

  @Override
  public boolean isBoundByTupleIds(List<TupleId> tids) {
    Preconditions.checkState(desc != null);
    for (TupleId tid: tids) {
      if (tid.equals(desc.getParent().getId())) return true;
    }
    return false;
  }

  @Override
  public boolean isBoundBySlotIds(List<SlotId> slotIds) {
    Preconditions.checkState(isAnalyzed);
    return slotIds.contains(desc.getId());
  }

  @Override
  public void getIdsHelper(Set<TupleId> tupleIds, Set<SlotId> slotIds) {
    Preconditions.checkState(type != PrimitiveType.INVALID_TYPE);
    Preconditions.checkState(desc != null);
    if (slotIds != null) slotIds.add(desc.getId());
    if (tupleIds != null) tupleIds.add(desc.getParent().getId());
  }

  public String getColumnName() {
    return col;
  }
}
