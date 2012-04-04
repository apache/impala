// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.thrift.TDataSink;
import com.cloudera.impala.thrift.TDataSinkType;
import com.cloudera.impala.thrift.THdfsTextTableSink;
import com.cloudera.impala.thrift.TTableSink;

/**
 * Data sink for writing delimited text file(s) to Hdfs.
 *
 */
public class HdfsTextTableSink extends HdfsTableSink {
  public HdfsTextTableSink(Table targetTable, List<Expr> partitionKeyExprs,
      boolean overwrite) {
    super(targetTable, partitionKeyExprs, overwrite);
  }

  @Override
  public String getExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "WRITE TO HDFS table=" + targetTable.getFullName() + "\n");
    output.append(prefix + "  OVERWRITE=" + (overwrite ? "true" : "false") + "\n");
    if (!partitionKeyExprs.isEmpty()) {
      output.append(prefix + "  PARTITIONS: ");
      for (Expr expr : partitionKeyExprs) {
        output.append(expr.toSql() + ",");
      }
    }
    output.deleteCharAt(output.length() - 1);
    output.append("\n");
    return output.toString();
  }

  @Override
  protected TDataSink toThrift() {
    TDataSink dataSink = new TDataSink(TDataSinkType.TABLE_SINK);
    THdfsTextTableSink hdfsTextTableSink =
        new THdfsTextTableSink(Expr.treesToThrift(partitionKeyExprs), overwrite);
    dataSink.tableSink = new TTableSink(targetTable.getId().asInt(),
        hdfsTextTableSink);
    return dataSink;
  }
}
