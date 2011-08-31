// Copyright (c) 2011 Cloudera, Inc. All rights reserved.
package com.cloudera.impala.catalog;

import com.cloudera.impala.thrift.TTable;
import com.cloudera.impala.thrift.TTableType;

/**
 * TextTable.
 *
 */
public class HdfsTextTable extends HdfsTable {

  // Input format class for Text tables read by Hive.
  private static final String textInputFormat = "org.apache.hadoop.mapred.TextInputFormat";

  protected HdfsTextTable(Db db, String name, String owner) {
    super(db, name, owner);
  }

  @Override
  public TTable toThrift() {
    TTable ttable = super.toThrift();
    ttable.setTableType(TTableType.HDFS_TEXT_TABLE);
    return ttable;
  }

  public static boolean isTextTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return msTbl.getSd().getInputFormat().equals(textInputFormat);
  }

}
