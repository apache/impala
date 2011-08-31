// Copyright (c) 2011 Cloudera, Inc. All rights reserved.
package com.cloudera.impala.catalog;

import com.cloudera.impala.thrift.TTable;
import com.cloudera.impala.thrift.TTableType;

/**
 * RCFileTable.
 *
 */
public class HdfsRCFileTable extends HdfsTable {

  private static final String rcfileInputFormat =
      "org.apache.hadoop.hive.ql.io.RCFileInputFormat";

  protected HdfsRCFileTable(Db db, String name, String owner) {
    super(db, name, owner);
  }

  @Override
  public TTable toThrift() {
    TTable ttable = super.toThrift();
    ttable.setTableType(TTableType.HDFS_RCFILE_TABLE);
    return ttable;
  }

  public static boolean isRCFileTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return msTbl.getSd().getInputFormat().equals(rcfileInputFormat);
  }

}
