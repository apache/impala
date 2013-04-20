// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.thrift.TDataSink;
import com.cloudera.impala.thrift.TDataSinkType;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.THdfsFileFormat;
import com.cloudera.impala.thrift.THdfsTableSink;
import com.cloudera.impala.thrift.TTableSink;
import com.cloudera.impala.thrift.TTableSinkType;

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
    output.append(prefix + "WRITE TO HDFS table=" + targetTable.getFullName()
        + "\n");
    output.append(prefix + "  overwrite=" + (overwrite ? "true" : "false") + "\n");
    if (!partitionKeyExprs.isEmpty()) {
      output.append(prefix + "  partitions: ");
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
    TDataSink result = new TDataSink(TDataSinkType.TABLE_SINK);
    THdfsTableSink hdfsTableSink =
        new THdfsTableSink(Expr.treesToThrift(partitionKeyExprs), overwrite);
    TTableSink tTableSink = new TTableSink(targetTable.getId().asInt(),
        TTableSinkType.HDFS);
    tTableSink.hdfs_table_sink = hdfsTableSink;
    result.table_sink = tTableSink;

    return result;
  }
}
