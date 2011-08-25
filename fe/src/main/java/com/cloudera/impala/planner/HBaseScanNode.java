// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.THBaseScanNode;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.google.common.base.Objects;

public class HBaseScanNode extends ScanNode {
  private final TupleDescriptor desc;

  public HBaseScanNode(TupleDescriptor desc) {
    super(desc);
    this.desc = desc;
  }

  @Override
  public void finalize(Analyzer analyzer) throws InternalException {
  }


  @Override
  protected String debugString() {
    HBaseTable tbl = (HBaseTable) desc.getTable();
    return Objects.toStringHelper(this)
        .add("tid", desc.getId().asInt())
        .add("hiveTblName", tbl.getFullName())
        .add("hbaseTblName", tbl.getHBaseTableName())
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.HBASE_SCAN_NODE;
    HBaseTable tbl = (HBaseTable) desc.getTable();
    msg.hbase_scan_node = new THBaseScanNode(desc.getId().asInt(), tbl.getHBaseTableName());
  }

  @Override
  protected String getExplainString(String prefix) {
    HBaseTable tbl = (HBaseTable) desc.getTable();
    StringBuilder output = new StringBuilder();
    output.append(prefix + "SCAN HBASE table=" + tbl.getName() + "\n");
    output.append(prefix + "  PREDICATES: " + getExplainString(conjuncts) + "\n");
    return output.toString();
  }

}
