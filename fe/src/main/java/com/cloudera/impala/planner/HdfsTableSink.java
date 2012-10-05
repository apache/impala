// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.thrift.TDataSink;
import com.cloudera.impala.thrift.TDataSink2;
import com.cloudera.impala.thrift.TDataSinkType;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.THdfsFileFormat;
import com.cloudera.impala.thrift.THdfsTableSink;
import com.cloudera.impala.thrift.TTableSink;

/**
 * Base class for Hdfs data sinks such as HdfsTextTableSink.
 *
 */
public class HdfsTableSink extends TableSink {
  // Exprs for computing the output partition(s).
  protected final List<Expr> partitionKeyExprs;
  // Whether to overwrite the existing partition(s).
  protected final boolean overwrite;
  protected THdfsFileFormat format;

  public HdfsTableSink(Table targetTable,
      List<Expr> partitionKeyExprs, boolean overwrite) {
    super(targetTable);
    this.partitionKeyExprs = partitionKeyExprs;
    this.overwrite = overwrite;
  }

  @Override
  public String getExplainString(String prefix, TExplainLevel explainLevel) {
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
    THdfsTableSink hdfsTableSink =
        new THdfsTableSink(Expr.treesToThrift(partitionKeyExprs), overwrite);
    dataSink.tableSink = new TTableSink(targetTable.getId().asInt(),
        hdfsTableSink);
    return dataSink;
  }

  @Override
  protected TDataSink2 toThrift2() {
    TDataSink2 result = new TDataSink2(TDataSinkType.TABLE_SINK);
    THdfsTableSink hdfsTableSink =
        new THdfsTableSink(Expr.treesToThrift(partitionKeyExprs), overwrite);
    result.table_sink = new TTableSink(targetTable.getId().asInt(),
        hdfsTableSink);
    return result;
  }
}
