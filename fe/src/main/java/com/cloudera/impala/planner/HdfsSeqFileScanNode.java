// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
package com.cloudera.impala.planner;

import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;

/**
 * HdfsSeqFileScanNode.
 *
 */
public class HdfsSeqFileScanNode extends HdfsScanNode {

  public HdfsSeqFileScanNode(int id, TupleDescriptor desc, HdfsTable tbl) {
    super(id, desc, tbl);
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    super.toThrift(msg);
    msg.node_type = TPlanNodeType.HDFS_SEQFILE_SCAN_NODE;
  }
}
