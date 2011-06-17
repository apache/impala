// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import java.util.List;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class SlotRef extends Expr {
  private final TableName tblName;
  private final String col;

  // results of analysis
  private SlotDescriptor desc;

  public SlotRef(TableName tblName, String col) {
    super();
    this.tblName = tblName;
    this.col = col;
  }

  // C'tor for a "pre-analyzed" ref to slot that doesn't correspond to
  // a table's column.
  public SlotRef(SlotDescriptor desc) {
    super();
    this.tblName = null;
    this.col = null;
    this.desc = desc;
    this.type = desc.getType();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    desc = analyzer.registerColumnRef(tblName, col);
    type = desc.getType();
  }

  @Override
  public String toSql() {
    if (tblName != null) {
      return tblName.toString() + "." + col;
    } else if (col != null) {
      return col;
    } else {
      return "<slot " + Integer.toString(desc.getId().getId()) + ">";
    }
  }

  @Override
  public String debugString() {
    List<String> output = Lists.newArrayList();
    if (tblName != null) {
      output.add("tblname=" + tblName.toString());
    }
    if (col != null) {
      output.add("col=" + col);
    }
    if (desc != null) {
      output.add("id=" + Integer.toString(desc.getId().getId()));
    }
    return "slotref[" + Joiner.on(" ").join(output) + "]";
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    SlotRef other = (SlotRef) obj;
    // check slot ids first; if they're both set we only need to compare those
    // (regardless of how the ref was constructed)
    if (desc != null && other.desc != null) {
      return desc.getId().equals(other.desc.getId());
    }
    if ((tblName == null) != (other.tblName == null)) {
      return false;
    }
    if (tblName != null && !tblName.equals(other.tblName)) {
      return false;
    }
    if ((col == null) != (other.col == null)) {
      return false;
    }
    if (col != null && !col.equals(other.col)) {
      return false;
    }
    return true;
  }

  @Override
  public boolean isBound(List<TupleId> tids) {
    Preconditions.checkState(desc != null);
    for (TupleId tid: tids) {
      if (tid.equals(desc.getParent().getId())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void getIds(List<TupleId> tupleIds, List<SlotId> slotIds) {
    Preconditions.checkState(type != PrimitiveType.INVALID_TYPE);
    Preconditions.checkState(desc != null);
    slotIds.add(desc.getId());
    tupleIds.add(desc.getParent().getId());
  }
}
