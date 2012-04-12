// Copyright (c) 2011 Cloudera, Inc. All rights reserved.
package com.cloudera.impala.catalog;

import java.util.List;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.planner.DataSink;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.cloudera.impala.thrift.TTableType;

/**
 * Sequence Table.
 *
 */
public class HdfsSeqFileTable extends HdfsTable {

  // Input format class for Sequence tables read by Hive.
  private static final String sequenceFileInputFormat =
      "org.apache.hadoop.mapred.SequenceFileInputFormat";

  protected HdfsSeqFileTable(TableId id, Db db, String name, String owner) {
    super(id, db, name, owner);
  }

  @Override
  public TTableDescriptor toThrift() {
    TTableDescriptor tTable = super.toThrift();
    tTable.setTableType(TTableType.HDFS_SEQFILE_TABLE);
    return tTable;
  }

  public static boolean isSeqFileTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return msTbl.getSd().getInputFormat().equals(sequenceFileInputFormat);
  }

  @Override
  public DataSink createDataSink(List<Expr> partitionKeyExprs, boolean overwrite) {
    throw new UnsupportedOperationException("HdfsSeqFile Output Sink not implemented.");
  }
}

