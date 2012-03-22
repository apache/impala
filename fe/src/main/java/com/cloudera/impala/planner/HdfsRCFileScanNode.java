// Copyright (c) 2011 Cloudera, Inc. All rights reserved.
package com.cloudera.impala.planner;

import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;

/**
 * HdfsRCFileScanNode.
 *
 */
public class HdfsRCFileScanNode extends HdfsScanNode {

  public HdfsRCFileScanNode(int id, TupleDescriptor desc, HdfsTable tbl) {
    super(id, desc, tbl);
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    super.toThrift(msg);
    msg.node_type = TPlanNodeType.HDFS_RCFILE_SCAN_NODE;
  }
}
