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
import com.cloudera.impala.catalog.HdfsFileFormat;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.PrintUtils;
import com.cloudera.impala.thrift.TDataSink;
import com.cloudera.impala.thrift.TDataSinkType;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.THdfsTableSink;
import com.cloudera.impala.thrift.TTableSink;
import com.cloudera.impala.thrift.TTableSinkType;
import com.google.common.base.Preconditions;

/**
 * Base class for Hdfs data sinks such as HdfsTextTableSink.
 *
 */
public class HdfsTableSink extends TableSink {
  // Default number of partitions used for computeCosts() in the absence of column stats.
  protected final long DEFAULT_NUM_PARTITIONS = 10;

  // Exprs for computing the output partition(s).
  protected final List<Expr> partitionKeyExprs_;
  // Whether to overwrite the existing partition(s).
  protected final boolean overwrite_;

  public HdfsTableSink(Table targetTable, List<Expr> partitionKeyExprs,
      boolean overwrite) {
    super(targetTable);
    Preconditions.checkState(targetTable instanceof HdfsTable);
    partitionKeyExprs_ = partitionKeyExprs;
    overwrite_ = overwrite;
  }

  @Override
  public void computeCosts() {
    HdfsTable table = (HdfsTable) targetTable;
    // TODO: Estimate the memory requirements more accurately by partition type.
    HdfsFileFormat format = table.getMajorityFormat();
    PlanNode inputNode = fragment.getPlanRoot();
    int numNodes = fragment.getNumNodes();
    // Compute the per-host number of partitions, taking the number of nodes
    // and the data partition of the fragment executing this sink into account.
    long numPartitions = fragment.getNumDistinctValues(partitionKeyExprs_);
    if (numPartitions == -1) numPartitions = DEFAULT_NUM_PARTITIONS;
    long perPartitionMemReq = getPerPartitionMemReq(format);

    // The estimate is based purely on the per-partition mem req if the input cardinality
    // or the avg row size is unknown.
    if (inputNode.getCardinality() == -1 || inputNode.getAvgRowSize() == -1) {
      perHostMemCost = numPartitions * perPartitionMemReq;
      return;
    }

    // The per-partition estimate may be higher than the memory required to buffer
    // the entire input data.
    long perHostInputCardinality = Math.max(1L, inputNode.getCardinality() / numNodes);
    long perHostInputBytes =
        (long) Math.ceil(perHostInputCardinality * inputNode.getAvgRowSize());
    perHostMemCost = Math.min(perHostInputBytes, numPartitions * perPartitionMemReq);
  }

  /**
   * Returns the per-partition memory requirement for inserting into the given
   * file format.
   */
  private long getPerPartitionMemReq(HdfsFileFormat format) {
    switch (format) {
      // Writing to a Parquet table requires up to 1GB of buffer per partition.
      // TODO: The per-partition memory requirement is configurable in the QueryOptions.
      case PARQUET: return 1024L * 1024L * 1024L;
      case TEXT: return 100L * 1024L;
      default:
        Preconditions.checkState(false, "Unsupported TableSink format " +
            format.toString());
    }
    return 0;
  }

  @Override
  public String getExplainString(String prefix, TExplainLevel explainLevel) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "WRITE TO HDFS table=" + targetTable.getFullName()
        + "\n");
    output.append(prefix + "  overwrite=" + (overwrite_ ? "true" : "false") + "\n");
    if (!partitionKeyExprs_.isEmpty()) {
      output.append(prefix + "  partition keys: ");
      for (Expr expr: partitionKeyExprs_) {
        output.append(expr.toSql() + ",");
      }
    }
    output.deleteCharAt(output.length() - 1);

    // Report the total number of partitions, independent of the number of nodes
    // and the data partition of the fragment executing this sink.
    if (explainLevel == TExplainLevel.VERBOSE) {
      long totalNumPartitions = Expr.getNumDistinctValues(partitionKeyExprs_);
      if (totalNumPartitions == -1) {
        output.append("  #partitions: unavailable");
      } else {
        output.append("  #partitions: " + totalNumPartitions);
      }
      output.append("\n");
      output.append(PrintUtils.printMemCost(prefix + "  ", perHostMemCost));
    }
    output.append("\n");
    return output.toString();
  }

  @Override
  protected TDataSink toThrift() {
    TDataSink result = new TDataSink(TDataSinkType.TABLE_SINK);
    THdfsTableSink hdfsTableSink = new THdfsTableSink(
        Expr.treesToThrift(partitionKeyExprs_), overwrite_);
    TTableSink tTableSink = new TTableSink(targetTable.getId().asInt(),
        TTableSinkType.HDFS);
    tTableSink.hdfs_table_sink = hdfsTableSink;
    result.table_sink = tTableSink;
    return result;
  }
}
