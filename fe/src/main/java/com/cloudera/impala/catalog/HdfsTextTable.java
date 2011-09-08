// Copyright (c) 2011 Cloudera, Inc. All rights reserved.
package com.cloudera.impala.catalog;

import com.cloudera.impala.thrift.TTableDescriptor;
import com.cloudera.impala.thrift.TTableType;

/**
 * TextTable.
 *
 */
public class HdfsTextTable extends HdfsTable {

  // Input format class for Text tables read by Hive.
  private static final String textInputFormat = "org.apache.hadoop.mapred.TextInputFormat";

  protected HdfsTextTable(TableId id, Db db, String name, String owner) {
    super(id, db, name, owner);
  }

  @Override
  public TTableDescriptor toThrift() {
    TTableDescriptor tTable = super.toThrift();
    tTable.setTableType(TTableType.HDFS_TEXT_TABLE);
    return tTable;
  }

  public static boolean isTextTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return msTbl.getSd().getInputFormat().equals(textInputFormat);
  }

}
